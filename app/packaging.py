"""素材 ZIP 制包：校验根目录 files.csv，核对清单映射，按 train/val 收纳原件。

输入 ZIP 约定（不落盘解压，全部流式读取）：

    files.csv             根目录，列出 图像号、ZIP 内路径、SHA-256
    images/...            清单图像对应的原件

生成包约定：

    manifest.csv          图像号、患者号、类别、侧别、文件路径、SHA-256
    train/<原 images/ 下相对路径>
    val/<原 images/ 下相对路径>

任何缺件、重复映射、摘要不符、越界路径、单件超限都会汇总成可读原因，
并且不产生输出文件（调用方应在失败时删除临时输出）。
"""
from __future__ import annotations

import csv
import hashlib
import io
import os
import re
import stat
import zipfile
from dataclasses import asdict, dataclass, field
from pathlib import Path

MAX_FILE_BYTES = 5 * 1024 * 1024           # 单件原件上限 5 MiB
MAX_FILES_CSV_BYTES = 1024 * 1024          # files.csv 自身上限
MAX_SOURCE_BYTES = 320 * 1024 * 1024       # 上传素材 ZIP 体积上限（防无界写入）
CHUNK = 1024 * 1024

_HEX64 = re.compile(r"^[0-9a-fA-F]{64}$")
_STAGE_ORDER = ("format", "path", "mapping", "missing", "size", "digest")
_STAGE_LABEL = {
    "format": "ZIP 格式",
    "path": "路径越界",
    "mapping": "清单映射",
    "missing": "缺件",
    "size": "单件超限",
    "digest": "摘要校验",
}

_IMAGE_HEADERS = {"图像号", "imageno", "image", "imageid", "图像编号"}
_PATH_HEADERS = {
    "zip内路径", "zippath", "路径", "文件路径", "path", "filepath", "zip路径",
}
_SHA_HEADERS = {"sha256", "摘要", "digest", "sha"}


class PackageError(Exception):
    """素材包无法生成：reason 面向用户，detail 为结构化问题清单。"""

    def __init__(self, stage: str, reason: str, detail: dict | None = None):
        super().__init__(reason)
        self.stage = stage
        self.reason = reason
        self.detail = detail or {}


@dataclass
class Problem:
    stage: str
    message: str
    image_no: str | None = None
    path: str | None = None
    line: int | None = None


@dataclass
class PackageResult:
    entry_count: int
    train_count: int
    val_count: int
    size_bytes: int
    sha256: str
    unmapped_files: list[str] = field(default_factory=list)


def _decode(raw: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "gbk"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _norm_header(text: str) -> str:
    return re.sub(r"[\s_\-]+", "", text.strip().lower())


def _header_indices(row: list[str]) -> list[int] | None:
    """按表头名定位三列；全部识别才返回，否则返回 None。"""
    seen: dict[str, int] = {}
    for i, cell in enumerate(row):
        seen.setdefault(_norm_header(cell), i)
    groups = (_IMAGE_HEADERS, _PATH_HEADERS, _SHA_HEADERS)
    if all(any(key in seen for key in group) for group in groups):
        return [next(seen[key] for key in group if key in seen) for group in groups]
    return None


def _looks_like_header(row: list[str]) -> bool:
    matched = 0
    for cell in row:
        key = _norm_header(cell)
        if any(key in group for group in (_IMAGE_HEADERS, _PATH_HEADERS, _SHA_HEADERS)):
            matched += 1
    return matched >= 2


def _fmt_line(n: int) -> str:
    return f"files.csv 第 {n} 行"


def _validate_image_path(raw: str) -> tuple[str | None, str | None]:
    """返回 (images/ 下相对路径, 错误原因)。只接受 images/ 下的普通相对路径。"""
    s = (raw or "").strip()
    if not s:
        return None, "路径为空"
    if "\x00" in s or any(ord(ch) < 0x20 for ch in s):
        return None, "路径包含控制字符"
    if "\\" in s:
        return None, "路径包含反斜杠（ZIP 内应使用 / 分隔）"
    if s.startswith("/"):
        return None, "路径不能是绝对路径"
    if re.match(r"^[A-Za-z]:", s):
        return None, "路径不能包含盘符"
    if not s.startswith("images/"):
        return None, "路径必须位于 images/ 目录下"
    rel = s[len("images/"):]
    if not rel or rel.endswith("/"):
        return None, "路径必须是 images/ 下的具体文件，不能是目录"
    segments = rel.split("/")
    if any(seg in ("", ".", "..") for seg in segments):
        return None, "路径不能包含空段、. 或 ..（不能越出 images/）"
    return rel, None


def _raise_problems(problems: list[Problem], extra_detail: dict | None = None) -> None:
    order = {stage: i for i, stage in enumerate(_STAGE_ORDER)}
    ordered = sorted(problems, key=lambda p: order.get(p.stage, len(order)))
    stage = ordered[0].stage

    messages: list[str] = []
    for p in ordered:
        if p.message not in messages:
            messages.append(p.message)
    shown = messages[:60]
    reason = "素材校验失败，未生成素材包：" + "；".join(shown)
    if len(messages) > len(shown):
        reason += f"；……另有 {len(messages) - len(shown)} 项问题"
    detail = {
        "stage": stage,
        "stage_label": _STAGE_LABEL.get(stage, stage),
        "problem_count": len(messages),
        "problems": [asdict(p) for p in ordered[:200]],
    }
    if extra_detail:
        detail.update(extra_detail)
    raise PackageError(stage, reason, detail)


def _open_source(source_path: str | Path) -> zipfile.ZipFile:
    try:
        return zipfile.ZipFile(source_path)
    except (zipfile.BadZipFile, OSError) as exc:
        raise PackageError(
            "format",
            f"上传文件不是可读取的 ZIP（{exc}），未生成素材包。",
            {"stage": "format", "problems": [{"stage": "format", "message": str(exc)}]},
        )


def _find_files_csv(zf: zipfile.ZipFile) -> zipfile.ZipInfo:
    candidates = [
        info
        for info in zf.infolist()
        if "/" not in info.filename and info.filename.lower() == "files.csv"
    ]
    if not candidates:
        raise PackageError(
            "format",
            "素材 ZIP 根目录缺少 files.csv，未生成素材包。",
            {"stage": "format", "problems": [{"stage": "format", "message": "根目录缺少 files.csv"}]},
        )
    if len(candidates) > 1:
        raise PackageError(
            "format",
            "素材 ZIP 根目录存在多个 files.csv，映射不唯一，未生成素材包。",
            {"stage": "format", "problems": [{"stage": "format", "message": "根目录存在多个 files.csv"}]},
        )
    info = candidates[0]
    if info.is_dir():
        raise PackageError(
            "format",
            "素材 ZIP 根目录的 files.csv 是目录而不是文件，未生成素材包。",
            {"stage": "format", "problems": [{"stage": "format", "message": "files.csv 不是文件"}]},
        )
    if info.flag_bits & 0x1:
        raise PackageError(
            "format",
            "素材 ZIP 的 files.csv 被加密，无法校验，未生成素材包。",
            {"stage": "format", "problems": [{"stage": "format", "message": "files.csv 被加密"}]},
        )
    if info.file_size > MAX_FILES_CSV_BYTES:
        raise PackageError(
            "format",
            f"files.csv 超过 {MAX_FILES_CSV_BYTES // 1024} KiB 上限，未生成素材包。",
            {"stage": "format", "problems": [{"stage": "format", "message": "files.csv 过大"}]},
        )
    return info


def _parse_files_csv(zf: zipfile.ZipFile, info: zipfile.ZipInfo) -> list[tuple[int, str, str, str]]:
    # 有界读取：不信任 ZIP 头声明的解压后大小，防止压缩炸弹
    try:
        with zf.open(info) as fh:
            raw = fh.read(MAX_FILES_CSV_BYTES + 1)
    except (zipfile.BadZipFile, RuntimeError, NotImplementedError, OSError) as exc:
        raise PackageError(
            "format",
            f"无法读取根目录 files.csv（{exc}），未生成素材包。",
            {"stage": "format", "problems": [{"stage": "format", "message": str(exc)}]},
        )
    if len(raw) > MAX_FILES_CSV_BYTES:
        raise PackageError(
            "format",
            f"files.csv 超过 {MAX_FILES_CSV_BYTES // 1024} KiB 上限，未生成素材包。",
            {"stage": "format", "problems": [{"stage": "format", "message": "files.csv 过大"}]},
        )
    text = _decode(raw)
    try:
        reader = csv.reader(io.StringIO(text.lstrip("﻿")))
        rows = [[cell.strip() for cell in row] for row in reader]
    except csv.Error as exc:
        raise PackageError(
            "format",
            f"files.csv 不是有效的 CSV（{exc}），未生成素材包。",
            {"stage": "format", "problems": [{"stage": "format", "message": str(exc)}]},
        )
    rows = [row for row in rows if any(cell for cell in row)]
    if not rows:
        raise PackageError(
            "format",
            "files.csv 为空，未生成素材包。",
            {"stage": "format", "problems": [{"stage": "format", "message": "files.csv 为空"}]},
        )

    indices = _header_indices(rows[0])
    if indices is not None:
        data_rows = rows[1:]
        first_line = 2
    elif _looks_like_header(rows[0]):
        raise PackageError(
            "format",
            "files.csv 表头无法识别，需要包含 图像号、ZIP 内路径、SHA-256 三列，未生成素材包。",
            {"stage": "format", "problems": [{"stage": "format", "message": "files.csv 表头无法识别"}]},
        )
    else:
        indices = [0, 1, 2]
        data_rows = rows
        first_line = 1

    out: list[tuple[int, str, str, str]] = []
    for offset, row in enumerate(data_rows):
        line = first_line + offset
        if len(row) < 3:
            raise PackageError(
                "format",
                f"{_fmt_line(line)}列数不足，需要 3 列（图像号、ZIP 内路径、SHA-256），未生成素材包。",
                {"stage": "format", "problems": [{"stage": "format", "message": f"{_fmt_line(line)}列数不足", "line": line}]},
            )
        image_no = row[indices[0]]
        zip_path = row[indices[1]]
        digest = row[indices[2]]
        if not image_no or not zip_path or not digest:
            raise PackageError(
                "format",
                f"{_fmt_line(line)}存在空值（图像号、ZIP 内路径、SHA-256 均必填），未生成素材包。",
                {"stage": "format", "problems": [{"stage": "format", "message": f"{_fmt_line(line)}存在空值", "line": line}]},
            )
        out.append((line, image_no, zip_path, digest))
    return out


def build_package(
    source_path: str | Path,
    entries: list[dict],
    out_path: str | Path,
) -> PackageResult:
    """校验素材 ZIP 并生成训练/验证素材包；失败时不保留输出文件。"""
    if not entries:
        raise PackageError(
            "format",
            "当前清单没有任何图像，无法制包。",
            {"stage": "format", "problems": [{"stage": "format", "message": "清单为空"}]},
        )

    out_path = Path(out_path)
    zf = _open_source(source_path)
    try:
        files_info = _find_files_csv(zf)
        rows = _parse_files_csv(zf, files_info)

        all_infos = zf.infolist()
        infos_by_name: dict[str, list[zipfile.ZipInfo]] = {}
        for info in all_infos:
            infos_by_name.setdefault(info.filename, []).append(info)

        manifest_images = [e["image_no"] for e in entries]
        manifest_set = set(manifest_images)
        problems: list[Problem] = []

        # ---- 静态校验：映射、路径、缺件、单件大小 -----------------------------
        rows_by_image: dict[str, tuple[int, str, str]] = {}
        for line, image_no, zip_path, digest in rows:
            if image_no not in manifest_set:
                problems.append(
                    Problem(
                        "mapping",
                        f"{_fmt_line(line)}的图像号 {image_no} 不在当前清单中（多余映射）",
                        image_no=image_no,
                        line=line,
                    )
                )
            if image_no in rows_by_image:
                problems.append(
                    Problem(
                        "mapping",
                        f"重复映射：图像 {image_no} 在 files.csv 中出现多次",
                        image_no=image_no,
                        line=line,
                    )
                )
                continue
            rows_by_image[image_no] = (line, zip_path, digest)

            if not _HEX64.match(digest):
                problems.append(
                    Problem(
                        "digest",
                        f"{_fmt_line(line)}中图像 {image_no} 的 SHA-256 格式不正确（需 64 位十六进制）",
                        image_no=image_no,
                        line=line,
                    )
                )

            _rel, path_err = _validate_image_path(zip_path)
            if path_err:
                problems.append(
                    Problem(
                        "path",
                        f"{_fmt_line(line)}中图像 {image_no} 的路径 {zip_path!r} 不合法：{path_err}",
                        image_no=image_no,
                        path=zip_path,
                        line=line,
                    )
                )
                continue

            infos = infos_by_name.get(zip_path)
            if not infos:
                problems.append(
                    Problem(
                        "missing",
                        f"缺件：图像 {image_no} 映射的 ZIP 内文件 {zip_path} 不存在",
                        image_no=image_no,
                        path=zip_path,
                        line=line,
                    )
                )
                continue
            if len(infos) > 1:
                problems.append(
                    Problem(
                        "mapping",
                        f"重复映射：ZIP 内路径 {zip_path} 存在 {len(infos)} 个同名条目",
                        image_no=image_no,
                        path=zip_path,
                        line=line,
                    )
                )
                continue
            info = infos[0]
            if info.is_dir():
                problems.append(
                    Problem(
                        "missing",
                        f"缺件：图像 {image_no} 映射的 {zip_path} 是目录而不是文件",
                        image_no=image_no,
                        path=zip_path,
                        line=line,
                    )
                )
                continue
            if info.flag_bits & 0x1:
                problems.append(
                    Problem(
                        "path",
                        f"图像 {image_no} 的文件 {zip_path} 被加密，无法读取",
                        image_no=image_no,
                        path=zip_path,
                        line=line,
                    )
                )
                continue
            mode = info.external_attr >> 16
            if stat.S_ISLNK(mode):
                problems.append(
                    Problem(
                        "path",
                        f"图像 {image_no} 的 {zip_path} 是符号链接，不允许放入素材包",
                        image_no=image_no,
                        path=zip_path,
                        line=line,
                    )
                )
                continue
            if info.file_size > MAX_FILE_BYTES:
                problems.append(
                    Problem(
                        "size",
                        f"单件超限：图像 {image_no} 的 {zip_path} 为 {info.file_size} 字节，"
                        f"超过 5 MiB（{MAX_FILE_BYTES} 字节）",
                        image_no=image_no,
                        path=zip_path,
                        line=line,
                    )
                )
                continue

        # 清单图像必须逐张有映射
        for image_no in manifest_images:
            if image_no not in rows_by_image:
                problems.append(
                    Problem(
                        "missing",
                        f"缺件：清单图像 {image_no} 未在 files.csv 中映射任何文件",
                        image_no=image_no,
                    )
                )

        # ZIP 内路径不能被两张图共用（即使其它映射字段不同，也算重复映射）
        by_path: dict[str, str] = {}
        for image_no, (line, zip_path, _digest) in rows_by_image.items():
            if image_no not in manifest_set:
                continue
            if zip_path in by_path and by_path[zip_path] != image_no:
                problems.append(
                    Problem(
                        "mapping",
                        f"重复映射：图像 {by_path[zip_path]} 与 {image_no} 都映射到 {zip_path}",
                        image_no=image_no,
                        path=zip_path,
                        line=line,
                    )
                )
            else:
                by_path[zip_path] = image_no

        mapped_paths = {row[1] for row in rows_by_image.values()}
        unmapped_files = sorted(
            info.filename
            for info in all_infos
            if info.filename.startswith("images/")
            and not info.is_dir()
            and info.filename not in mapped_paths
        )

        if problems:
            detail = {
                "missing_images": [
                    image_no
                    for image_no in manifest_images
                    if image_no not in rows_by_image
                ],
                "extra_images": sorted(
                    image_no for image_no in rows_by_image if image_no not in manifest_set
                ),
                "unmapped_files": unmapped_files[:50],
            }
            _raise_problems(problems, detail)

        # ---- 逐件读取、校验摘要、写入输出 ZIP ---------------------------------
        ordered_entries = sorted(
            entries,
            key=lambda e: (e["side"], e["class_name"], e["image_no"]),
        )
        manifest_rows: list[list[str]] = []
        val_count = train_count = 0
        digest_problems: list[Problem] = []
        written_paths: set[str] = set()

        out_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with zipfile.ZipFile(
                out_path, "w", compression=zipfile.ZIP_STORED, allowZip64=True
            ) as zout:
                for entry in ordered_entries:
                    image_no = entry["image_no"]
                    _line, zip_path, declared = rows_by_image[image_no]
                    _rel, _err = _validate_image_path(zip_path)
                    assert _rel is not None
                    out_name = f"{entry['side']}/{zip_path}"
                    if out_name in written_paths:  # 防御：极端情况下仍不覆盖
                        digest_problems.append(
                            Problem(
                                "mapping",
                                f"重复映射：输出路径 {out_name} 与已有图像冲突",
                                image_no=image_no,
                                path=zip_path,
                            )
                        )
                        break
                    written_paths.add(out_name)

                    info = infos_by_name[zip_path][0]
                    zi = zipfile.ZipInfo(out_name, date_time=_now_tuple())
                    zi.compress_type = zipfile.ZIP_STORED
                    zi.external_attr = 0o644 << 16
                    hasher = hashlib.sha256()
                    written = 0
                    try:
                        with zf.open(info) as src, zout.open(zi, "w", force_zip64=True) as dst:
                            while True:
                                chunk = src.read(CHUNK)
                                if not chunk:
                                    break
                                written += len(chunk)
                                if written > MAX_FILE_BYTES:
                                    raise _TooLarge()
                                hasher.update(chunk)
                                dst.write(chunk)
                    except _TooLarge:
                        digest_problems.append(
                            Problem(
                                "size",
                                f"单件超限：图像 {image_no} 的 {zip_path} 实际解压后超过 5 MiB",
                                image_no=image_no,
                                path=zip_path,
                            )
                        )
                        break
                    except (zipfile.BadZipFile, RuntimeError, NotImplementedError, OSError) as exc:
                        digest_problems.append(
                            Problem(
                                "digest",
                                f"摘要校验失败：图像 {image_no} 的 {zip_path} 无法完整读取"
                                f"（ZIP 校验和或压缩格式错误：{exc}）",
                                image_no=image_no,
                                path=zip_path,
                            )
                        )
                        break

                    actual = hasher.hexdigest()
                    if actual.lower() != declared.lower():
                        digest_problems.append(
                            Problem(
                                "digest",
                                f"摘要不符：图像 {image_no} 的 {zip_path} 实际 SHA-256 为 "
                                f"{actual}，files.csv 声明为 {declared}",
                                image_no=image_no,
                                path=zip_path,
                            )
                        )
                    manifest_rows.append(
                        [
                            image_no,
                            entry["patient_no"],
                            entry["class_name"],
                            entry["side"],
                            out_name,
                            actual,
                        ]
                    )
                    if entry["side"] == "val":
                        val_count += 1
                    else:
                        train_count += 1

                if digest_problems:
                    raise _AbortBuild()

                # manifest.csv 只在全部原件通过校验后写入
                buf = io.StringIO(newline="")
                writer = csv.writer(buf, lineterminator="\n")
                writer.writerow(
                    ["图像号", "患者号", "类别", "侧别", "文件路径", "SHA-256"]
                )
                writer.writerows(manifest_rows)
                zi = zipfile.ZipInfo("manifest.csv", date_time=_now_tuple())
                zi.compress_type = zipfile.ZIP_DEFLATED
                zi.external_attr = 0o644 << 16
                zout.writestr(zi, buf.getvalue().encode("utf-8"))
        except _AbortBuild:
            _remove_quietly(out_path)
            detail = {
                "checked_images": len(manifest_rows),
                "unmapped_files": unmapped_files[:50],
            }
            _raise_problems(digest_problems, detail)
        except Exception:
            _remove_quietly(out_path)
            raise

        # 输出文件落盘并计算包摘要
        _fsync_file(out_path)
        size_bytes, sha256 = _file_size_and_sha(out_path)
        return PackageResult(
            entry_count=len(manifest_rows),
            train_count=train_count,
            val_count=val_count,
            size_bytes=size_bytes,
            sha256=sha256,
            unmapped_files=unmapped_files,
        )
    finally:
        zf.close()


class _TooLarge(Exception):
    pass


class _AbortBuild(Exception):
    pass


def _now_tuple() -> tuple[int, int, int, int, int, int]:
    import time

    return time.localtime()[:6]


def _remove_quietly(path: str | Path) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


def _fsync_file(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def _file_size_and_sha(path: Path) -> tuple[int, str]:
    hasher = hashlib.sha256()
    total = 0
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(CHUNK)
            if not chunk:
                break
            total += len(chunk)
            hasher.update(chunk)
    return total, hasher.hexdigest()
