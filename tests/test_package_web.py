"""素材包网页流程：上传、失败原因页、下载、更正后停供。"""
import io
import zipfile

from app.db import get_db
from tests.conftest import upload
from tests.test_packaging import (
    QUOTA,
    ROWS,
    _Stream,
    make_all_source,
    upload_package,
)


def _publish(client, svc):
    upload(client, ROWS, QUOTA)
    client.post("/partition", follow_redirects=True)
    assert svc.current_manifest() is not None


def test_web_upload_download_and_zip_contents(client):
    c, svc = client
    _publish(c, svc)
    resp = upload_package(c, make_all_source())
    assert resp.status_code == 200
    package = svc.current_package()
    assert package is not None
    page = resp.data.decode("utf-8")
    assert f"素材包 #{package['id']}" in page
    assert "下载素材包" in page

    dl = c.get(f"/packages/{package['id']}/download")
    assert dl.status_code == 200
    assert dl.headers["Content-Type"] == "application/zip"
    assert "attachment" in dl.headers["Content-Disposition"]
    with zipfile.ZipFile(io.BytesIO(dl.data)) as zf:
        names = zf.namelist()
        assert "manifest.csv" in names
        assert any(n.startswith("train/images/") for n in names)
        assert any(n.startswith("val/images/") for n in names)


def test_web_rejection_shows_reason_and_no_package(client):
    c, svc = client
    _publish(c, svc)
    source = make_all_source()
    files = {f"images/{i}.png": f"pixels-{i}".encode() for i, *_ in ROWS}
    # 摘要错误
    import hashlib

    rows = [(i, f"images/{i}.png", hashlib.sha256(files[f"images/{i}.png"]).hexdigest())
            for i, *_ in ROWS]
    rows[1] = ("I2", "images/I2.png", "f" * 64)
    from tests.test_packaging import make_source

    resp = upload_package(c, make_source(files, rows))
    page = resp.data.decode("utf-8")
    assert "未生成素材包" in page
    assert "摘要不符" in page
    assert "I2" in page
    assert svc.current_package() is None
    assert svc.list_packages() == []
    # 首页仍可正常打开，且无当前包
    assert "当前清单尚无可用素材包" in c.get("/").data.decode("utf-8")


def test_web_upload_requires_published_manifest(client):
    c, _svc = client
    resp = upload_package(c, make_all_source())
    assert "没有已发布清单" in resp.data.decode("utf-8")


def test_web_correction_stops_package_download(client):
    c, svc = client
    _publish(c, svc)
    upload_package(c, make_all_source())
    package = svc.current_package()
    assert c.get(f"/packages/{package['id']}/download").status_code == 200
    # 更正输入后旧包停供
    upload(c, [("J1", "Q1", "A"), ("J2", "Q2", "A")], {"A": 1})
    resp = c.get(f"/packages/{package['id']}/download", follow_redirects=False)
    assert resp.status_code == 302
    page = c.get(f"/packages/{package['id']}/download", follow_redirects=True)
    assert "不可下载" in page.data.decode("utf-8")
    assert svc.package_downloadable(package["id"]) is None
    hist = c.get("/history").data.decode("utf-8")
    assert "已停供" in hist


def test_web_history_lists_package_jobs(client):
    c, svc = client
    _publish(c, svc)
    upload_package(c, make_all_source())
    page = c.get("/history").data.decode("utf-8")
    assert "素材包与制包作业" in page
    assert "materials_v1" in page


def test_web_non_zip_filename_rejected(client):
    c, svc = client
    _publish(c, svc)
    resp = upload_package(c, make_all_source(), name="materials.tar")
    assert "请上传 .zip" in resp.data.decode("utf-8")
    assert svc.current_package() is None
