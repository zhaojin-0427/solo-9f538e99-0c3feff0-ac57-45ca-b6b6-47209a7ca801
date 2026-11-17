"""素材制包端到端：校验原因、train/val 收纳、停供、并发、重启。"""
import hashlib
import io
import zipfile

import pytest

from app.db import get_db
from app.service import Service
from tests.conftest import make_csv, make_dup


ROWS = [
    ("I1", "P1", "A"), ("I2", "P2", "A"), ("I3", "P3", "A"),
    ("I4", "P4", "B"), ("I5", "P5", "B"),
]
QUOTA = {"A": 2, "B": 1}


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_source(files: dict[str, bytes], rows: list[tuple] | None = None,
                header: str = "图像号,ZIP 内路径,SHA-256") -> bytes:
    """构造素材 ZIP：files 为 {zip 路径: 内容}，rows 为 (图像号, 路径, 摘要)。"""
    if rows is None:
        rows = [(name[len("images/"):].split(".")[0], name, _sha(data))
                for name, data in files.items()]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("files.csv", "\n".join(
            [header] + [",".join(r) for r in rows]) + "\n")
        for name, data in files.items():
            zf.writestr(name, data)
    return buf.getvalue()


def make_all_source(payloads: dict[str, bytes] | None = None) -> bytes:
    """覆盖全部清单图像的合法素材包。"""
    payloads = payloads or {}
    files = {}
    rows = []
    for image_no, *_ in ROWS:
        name = f"images/{image_no}.png"
        data = payloads.get(image_no, f"pixels-{image_no}".encode())
        files[name] = data
        rows.append((image_no, name, _sha(data)))
    return make_source(files, rows)


def published_service(tmp_path) -> Service:
    svc = Service(str(tmp_path / "pkg.db"), synchronous=True)
    svc.create_version(make_csv(ROWS), make_dup([]), QUOTA)
    svc.start_partition()
    assert svc.current_manifest() is not None
    return svc


def upload_package(client, source: bytes, name: str = "materials.zip"):
    return client.post(
        "/packages",
        data={"source_zip": (io.BytesIO(source), name)},
        content_type="multipart/form-data",
        follow_redirects=True,
    )


class _Stream:
    def __init__(self, data: bytes):
        self._io = io.BytesIO(data)

    def read(self, n=-1):
        return self._io.read(n)


def test_package_groups_originals_by_side_and_includes_manifest(tmp_path):
    svc = published_service(tmp_path)
    payloads = {image_no: f"raw-{image_no}".encode() * 100 for image_no, *_ in ROWS}
    job_id = svc.start_package("materials.zip", _Stream(make_all_source(payloads)))
    job = svc.get_package_job(job_id)
    assert job["status"] == "succeeded", job["reason"]
    package = svc.current_package()
    assert package is not None and package["entry_count"] == len(ROWS)

    entries = {e["image_no"]: e for e in svc.get_manifest_entries(
        svc.current_manifest()["id"])}
    with zipfile.ZipFile(package["file_path"]) as zf:
        names = set(zf.namelist())
        for image_no, entry in entries.items():
            expected = f"{entry['side']}/images/{image_no}.png"
            assert expected in names
            assert zf.read(expected) == payloads[image_no]
        assert "manifest.csv" in names
        manifest_text = zf.read("manifest.csv").decode("utf-8")
        lines = manifest_text.strip().split("\n")
        assert lines[0] == "图像号,患者号,类别,侧别,文件路径,SHA-256"
        by_image = {line.split(",")[0]: line.split(",") for line in lines[1:]}
        assert set(by_image) == set(entries)
        for image_no, entry in entries.items():
            row = by_image[image_no]
            assert row[1] == entry["patient_no"]
            assert row[2] == entry["class_name"]
            assert row[3] == entry["side"]
            assert row[4].startswith(entry["side"] + "/images/")
            assert row[5] == _sha(payloads[image_no])
    # 包摘要与文件内容一致
    assert hashlib.sha256(open(package["file_path"], "rb").read()).hexdigest() == package["sha256"]
    svc.close()


def test_missing_file_reports_reason_no_package(tmp_path):
    svc = published_service(tmp_path)
    source = make_all_source()
    # 删除 I3 的映射（相当于缺件）
    files = {f"images/{i}.png": f"pixels-{i}".encode() for i, *_ in ROWS}
    rows = [(i, f"images/{i}.png", _sha(files[f"images/{i}.png"]))
            for i, *_ in ROWS if i != "I3"]
    job_id = svc.start_package("bad.zip", _Stream(make_source(files, rows)))
    job = svc.get_package_job(job_id)
    assert job["status"] == "failed"
    assert job["fail_stage"] == "missing"
    assert "I3" in job["reason"] and "缺件" in job["reason"]
    assert svc.current_package() is None
    assert list(svc.package_dir.glob("materials_*.zip")) == []
    svc.close()


def test_extra_mapping_reports_reason(tmp_path):
    svc = published_service(tmp_path)
    files = {f"images/{i}.png": f"pixels-{i}".encode() for i, *_ in ROWS}
    files["images/EXTRA.png"] = b"extra"
    rows = [(i, f"images/{i}.png", _sha(files[f"images/{i}.png"])) for i, *_ in ROWS]
    rows.append(("EXTRA", "images/EXTRA.png", _sha(b"extra")))
    job_id = svc.start_package("bad.zip", _Stream(make_source(files, rows)))
    job = svc.get_package_job(job_id)
    assert job["status"] == "failed"
    assert job["fail_stage"] == "mapping"
    assert "EXTRA" in job["reason"]
    assert svc.current_package() is None
    svc.close()


def test_duplicate_mapping_reports_reason(tmp_path):
    svc = published_service(tmp_path)
    files = {f"images/{i}.png": f"pixels-{i}".encode() for i, *_ in ROWS}
    rows = [(i, f"images/{i}.png", _sha(files[f"images/{i}.png"])) for i, *_ in ROWS]
    rows.append(("I1", "images/I2.png", _sha(files["images/I2.png"])))
    job_id = svc.start_package("bad.zip", _Stream(make_source(files, rows)))
    job = svc.get_package_job(job_id)
    assert job["status"] == "failed"
    assert job["fail_stage"] == "mapping"
    assert "重复映射" in job["reason"]
    svc.close()


def test_same_path_for_two_images_reports_reason(tmp_path):
    svc = published_service(tmp_path)
    files = {f"images/{i}.png": f"pixels-{i}".encode() for i, *_ in ROWS}
    rows = [(i, f"images/{i}.png", _sha(files[f"images/{i}.png"])) for i, *_ in ROWS]
    # I1 与 I2 都映射到同一份文件（I1 原路径仍在 ZIP 中，但不再被映射）
    files["images/shared.png"] = files["images/I1.png"]
    rows = [(i, "images/shared.png" if i in ("I1", "I2") else p, d)
            for i, p, d in rows]
    job_id = svc.start_package("bad.zip", _Stream(make_source(files, rows)))
    job = svc.get_package_job(job_id)
    assert job["status"] == "failed"
    assert job["fail_stage"] == "mapping"
    assert "重复映射" in job["reason"]
    svc.close()


def test_digest_mismatch_reports_reason(tmp_path):
    svc = published_service(tmp_path)
    files = {f"images/{i}.png": f"pixels-{i}".encode() for i, *_ in ROWS}
    rows = [(i, f"images/{i}.png", _sha(files[f"images/{i}.png"])) for i, *_ in ROWS]
    rows[0] = ("I1", "images/I1.png", "0" * 64)
    job_id = svc.start_package("bad.zip", _Stream(make_source(files, rows)))
    job = svc.get_package_job(job_id)
    assert job["status"] == "failed"
    assert job["fail_stage"] == "digest"
    assert "摘要不符" in job["reason"] and "I1" in job["reason"]
    assert svc.current_package() is None
    assert list(svc.package_dir.glob("materials_*.zip")) == []
    svc.close()


@pytest.mark.parametrize(
    "bad_path,stage",
    [
        ("../escape.png", "path"),
        ("/abs/images/I1.png", "path"),
        ("images/../../I1.png", "path"),
        ("other/I1.png", "path"),
    ],
)
def test_out_of_bounds_paths_rejected(tmp_path, bad_path, stage):
    svc = published_service(tmp_path)
    files = {f"images/{i}.png": f"pixels-{i}".encode() for i, *_ in ROWS}
    rows = [(i, f"images/{i}.png", _sha(files[f"images/{i}.png"])) for i, *_ in ROWS]
    rows[0] = ("I1", bad_path, _sha(files["images/I1.png"]))
    files[bad_path] = files["images/I1.png"]
    job_id = svc.start_package("bad.zip", _Stream(make_source(files, rows)))
    job = svc.get_package_job(job_id)
    assert job["status"] == "failed"
    assert job["fail_stage"] == stage
    assert "不合法" in job["reason"]
    svc.close()


def test_oversize_single_file_rejected(tmp_path):
    svc = published_service(tmp_path)
    files = {f"images/{i}.png": f"pixels-{i}".encode() for i, *_ in ROWS}
    big = b"x" * (5 * 1024 * 1024 + 1)
    files["images/I1.png"] = big
    rows = [(i, f"images/{i}.png", _sha(files[f"images/{i}.png"])) for i, *_ in ROWS]
    job_id = svc.start_package("bad.zip", _Stream(make_source(files, rows)))
    job = svc.get_package_job(job_id)
    assert job["status"] == "failed"
    assert job["fail_stage"] == "size"
    assert "5 MiB" in job["reason"]
    assert svc.current_package() is None
    svc.close()


def test_exactly_5mib_file_allowed(tmp_path):
    svc = published_service(tmp_path)
    files = {f"images/{i}.png": f"pixels-{i}".encode() for i, *_ in ROWS}
    files["images/I1.png"] = b"y" * (5 * 1024 * 1024)
    rows = [(i, f"images/{i}.png", _sha(files[f"images/{i}.png"])) for i, *_ in ROWS]
    job_id = svc.start_package("ok.zip", _Stream(make_source(files, rows)))
    job = svc.get_package_job(job_id)
    assert job["status"] == "succeeded", job["reason"]
    svc.close()


def test_missing_files_csv_reports_format_reason(tmp_path):
    svc = published_service(tmp_path)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("images/I1.png", b"x")
    job_id = svc.start_package("bad.zip", _Stream(buf.getvalue()))
    job = svc.get_package_job(job_id)
    assert job["status"] == "failed"
    assert job["fail_stage"] == "format"
    assert "files.csv" in job["reason"]
    svc.close()


def test_non_zip_upload_reports_format_reason(tmp_path):
    svc = published_service(tmp_path)
    job_id = svc.start_package("bad.zip", _Stream(b"not a zip at all"))
    job = svc.get_package_job(job_id)
    assert job["status"] == "failed"
    assert job["fail_stage"] == "format"
    svc.close()


def test_rejects_non_zip_filename(tmp_path):
    svc = published_service(tmp_path)
    from app.parser import InputError

    with pytest.raises(InputError):
        svc.start_package("materials.tar", _Stream(make_all_source()))
    svc.close()


def test_requires_published_manifest(tmp_path):
    from app.parser import InputError

    svc = Service(str(tmp_path / "empty.db"), synchronous=True)
    with pytest.raises(InputError):
        svc.start_package("materials.zip", _Stream(make_all_source()))
    svc.close()


def test_uploaded_source_and_tmp_cleaned_after_job(tmp_path):
    svc = published_service(tmp_path)
    job_id = svc.start_package("materials.zip", _Stream(make_all_source()))
    assert svc.get_package_job(job_id)["status"] == "succeeded"
    assert list(svc.package_tmp_dir.iterdir()) == []
    svc.close()


def test_correction_after_publish_stops_package_download(tmp_path):
    svc = published_service(tmp_path)
    svc.start_package("materials.zip", _Stream(make_all_source()))
    package = svc.current_package()
    assert svc.package_downloadable(package["id"]) is not None
    # 发布后更正输入：旧包立即停供、文件删除
    svc.create_version(make_csv(ROWS), make_dup([]), {"A": 1, "B": 1})
    assert svc.package_downloadable(package["id"]) is None
    assert svc.current_package() is None
    assert not (svc.package_dir / package["file_name"]).exists()
    svc.close()


def test_correction_during_packaging_never_becomes_current(tmp_path):
    db = str(tmp_path / "race.db")
    svc = Service(db, synchronous=False, package_delay=0.6)
    svc.create_version(make_csv(ROWS), make_dup([]), QUOTA)
    svc.start_partition()
    deadline = _wait_partition(svc)
    assert deadline
    job_id = svc.start_package("materials.zip", _Stream(make_all_source()))
    import time

    time.sleep(0.2)
    svc.create_version(make_csv(ROWS), make_dup([]), {"A": 1, "B": 1})
    end = time.time() + 10
    while time.time() < end:
        if svc.get_package_job(job_id)["status"] in ("succeeded", "failed"):
            break
        time.sleep(0.1)
    job = svc.get_package_job(job_id)
    assert job["status"] == "failed"
    assert job["fail_stage"] == "stale"
    assert svc.current_package() is None
    assert list(svc.package_dir.glob("materials_*.zip")) == []
    svc.close()


def test_concurrent_packaging_single_current_package(tmp_path):
    db = str(tmp_path / "conc.db")
    svc = Service(db, synchronous=False)
    svc.create_version(make_csv(ROWS), make_dup([]), QUOTA)
    svc.start_partition()
    assert _wait_partition(svc)
    source = make_all_source()

    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=5) as pool:
        ids = list(pool.map(lambda _: svc.start_package("m.zip", _Stream(source)), range(5)))
    assert len(set(ids)) == 1  # 同清单并发（内容相同）只登记一个作业
    end = __import__("time").time() + 10
    while __import__("time").time() < end:
        if svc.get_package_job(ids[0])["status"] in ("succeeded", "failed"):
            break
        __import__("time").sleep(0.1)
    assert svc.get_package_job(ids[0])["status"] == "succeeded"
    conn = get_db(db)
    try:
        n = conn.execute(
            "SELECT COUNT(*) c FROM material_packages WHERE status='published'"
        ).fetchone()["c"]
    finally:
        conn.close()
    assert n == 1
    assert len(list(svc.package_dir.glob("materials_*.zip"))) == 1
    svc.close()


def test_restart_interrupts_package_without_half_package(tmp_path):
    db = str(tmp_path / "restart.db")
    svc = Service(db, synchronous=False, package_delay=1.0)
    svc.create_version(make_csv(ROWS), make_dup([]), QUOTA)
    svc.start_partition()
    assert _wait_partition(svc)
    job_id = svc.start_package("m.zip", _Stream(make_all_source()))
    import time

    time.sleep(0.3)
    svc2 = Service(db, synchronous=True)
    job = svc2.get_package_job(job_id)
    assert job["status"] == "failed"
    assert job["fail_stage"] == "restart"
    assert svc2.current_package() is None
    assert list(svc2.package_dir.glob("materials_*.zip")) == []
    assert list(svc2.package_tmp_dir.iterdir()) == []
    # 重启后可重新上传出包
    job2 = svc2.start_package("m.zip", _Stream(make_all_source()))
    assert svc2.get_package_job(job2)["status"] == "succeeded"
    svc.close()
    svc2.close()


def test_repack_replaces_previous_current_package(tmp_path):
    svc = published_service(tmp_path)
    j1 = svc.start_package("a.zip", _Stream(make_all_source()))
    p1 = svc.current_package()
    assert svc.get_package_job(j1)["status"] == "succeeded"
    # 不同内容重新上传：旧包停供、文件删除，只有新包可下载
    payloads = {image_no: f"rev2-{image_no}".encode() for image_no, *_ in ROWS}
    j2 = svc.start_package("b.zip", _Stream(make_all_source(payloads)))
    p2 = svc.current_package()
    assert svc.get_package_job(j2)["status"] == "succeeded"
    assert p2["id"] != p1["id"]
    assert svc.package_downloadable(p1["id"]) is None
    assert svc.package_downloadable(p2["id"]) is not None
    assert not (svc.package_dir / p1["file_name"]).exists()
    svc.close()


def test_identical_repack_is_idempotent(tmp_path):
    svc = published_service(tmp_path)
    source = make_all_source()
    j1 = svc.start_package("a.zip", _Stream(source))
    p1 = svc.current_package()
    j2 = svc.start_package("a.zip", _Stream(source))
    assert j2 == j1
    assert svc.current_package()["id"] == p1["id"]
    assert len(list(svc.package_dir.glob("materials_*.zip"))) == 1
    svc.close()


def _wait_partition(svc, timeout=10):
    import time

    end = time.time() + timeout
    while time.time() < end:
        manifest = svc.current_manifest()
        if manifest is not None:
            return True
        jobs = svc.list_jobs()
        if jobs and jobs[0]["status"] == "failed":
            return False
        time.sleep(0.05)
    return False
