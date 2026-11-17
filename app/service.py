"""业务服务层：输入版本、作业调度、原子发布、历史与下载。"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

from . import db as dbmod
from .db import get_db
from .packaging import (
    MAX_SOURCE_BYTES,
    PackageError,
    build_package,
)
from .parser import (
    InputError,
    parse_dups_csv,
    parse_images_csv,
    parse_quota,
    validate_and_build,
)
from .solver import SolveError, solve


class Service:
    def __init__(
        self,
        db_path: str,
        synchronous: bool = False,
        solve_delay: float = 0.0,
        package_dir: str | Path | None = None,
        package_delay: float = 0.0,
    ):
        self.db_path = db_path
        # synchronous=True：作业在请求线程内立刻执行，便于自测确定性
        self.synchronous = synchronous
        self.solve_delay = solve_delay
        self.package_delay = package_delay
        base = Path(db_path).parent
        env_dir = os.environ.get("PACKAGE_DIR")
        self.package_dir = (
            Path(package_dir) if package_dir else Path(env_dir) if env_dir else base / "packages"
        )
        self.package_tmp_dir = self.package_dir / "_tmp"
        self.package_dir.mkdir(parents=True, exist_ok=True)
        self.package_tmp_dir.mkdir(parents=True, exist_ok=True)
        # 上次进程中断遗留的临时上传/半成品包：从不出现在数据库，直接清掉
        _clean_dir(self.package_tmp_dir)
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="partition")
        self._package_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="package")
        self._lock = threading.Lock()
        self._package_lock = threading.Lock()
        dbmod.init_db(db_path)
        # 重启恢复：作废非当前清单的包登记，清理临时/半包文件（数据库里 published 的才保留）
        self.supersede_packages_except(self._active_version_id())
        self.sweep_orphan_packages()

    def _active_version_id(self) -> int | None:
        conn = get_db(self.db_path)
        try:
            row = conn.execute(
                "SELECT id FROM input_versions WHERE status='active' "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        return row["id"] if row else None

    def close(self) -> None:
        """关闭后台线程池，避免多实例下线程跨库争用作业。"""
        self._pool.shutdown(wait=False, cancel_futures=True)
        self._package_pool.shutdown(wait=False, cancel_futures=True)

    # ------------------------------------------------------------------ 版本
    def create_version(
        self, image_csv: str, dup_csv: str, quota_raw: dict
    ) -> tuple[int, list[str]]:
        """解析+校验全部输入，通过后整体写入新版本；旧版本立即失效。

        返回 (version_id, 警告信息列表)。任何一步失败都不写库。
        """
        images = parse_images_csv(image_csv)
        dup_edges = parse_dups_csv(dup_csv or "")
        components, classes = validate_and_build(images, dup_edges)
        quota = parse_quota(quota_raw, classes)
        total_imgs = len(images)
        total_quota = sum(quota.values())
        if total_quota > total_imgs:
            raise InputError(
                f"验证配额合计 {total_quota} 张，超过全量图像 {total_imgs} 张"
            )

        conn = get_db(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "INSERT INTO input_versions(image_csv, dup_csv, image_count, "
                "class_names, quotas, note) VALUES (?,?,?,?,?,?)",
                (
                    image_csv,
                    dup_csv or "",
                    total_imgs,
                    ",".join(classes),
                    json.dumps(quota, ensure_ascii=False),
                    f"用户上传于 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                ),
            )
            vid = cur.lastrowid
            conn.executemany(
                "INSERT INTO images(version_id, image_no, patient_no, class_name) "
                "VALUES (?,?,?,?)",
                [(vid, r.image_no, r.patient_no, r.class_name) for r in images],
            )
            conn.executemany(
                "INSERT INTO dup_edges(version_id, image_a, image_b) VALUES (?,?,?)",
                [(vid, a, b) for a, b in dup_edges],
            )
            # 新版本生效的同一事务里，旧版本全部转为历史
            conn.execute(
                "UPDATE input_versions SET status='superseded' WHERE id<>?", (vid,)
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        # 更正输入即旧包失效：先作废登记（下载立即停供），再删除旧文件。
        self.supersede_packages_except(vid)
        return vid, classes

    def current_version(self) -> sqlite3.Row | None:
        conn = get_db(self.db_path)
        try:
            return conn.execute(
                "SELECT * FROM input_versions WHERE status='active' "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()

    def get_version(self, vid: int) -> sqlite3.Row | None:
        conn = get_db(self.db_path)
        try:
            return conn.execute(
                "SELECT * FROM input_versions WHERE id=?", (vid,)
            ).fetchone()
        finally:
            conn.close()

    def list_versions(self) -> list[sqlite3.Row]:
        conn = get_db(self.db_path)
        try:
            return list(
                conn.execute("SELECT * FROM input_versions ORDER BY id DESC").fetchall()
            )
        finally:
            conn.close()

    # ------------------------------------------------------------------ 作业
    def start_partition(self) -> int:
        version = self.current_version()
        if version is None:
            raise InputError("尚未上传任何输入数据，请先上传 CSV 并填写配额")
        # 部分唯一索引 uq_job_active 保证同版本至多一个进行中/成功的作业。
        # - 并发发起：INSERT OR IGNORE 撞约束，复用同一作业；
        # - 失败作业不占约束位，允许重新发起（新作业行，历史失败仍保留）。
        with self._lock:
            conn = get_db(self.db_path)
            try:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO partition_jobs(version_id, status) "
                    "VALUES (?,'queued')",
                    (version["id"],),
                )
                if cur.rowcount == 1:
                    job_id = cur.lastrowid
                    is_new = True
                else:
                    row = conn.execute(
                        "SELECT id FROM partition_jobs WHERE version_id=? "
                        "AND status IN ('queued','running','succeeded') ORDER BY id LIMIT 1",
                        (version["id"],),
                    ).fetchone()
                    job_id = row["id"]
                    is_new = False
            finally:
                conn.close()

        # 只有新插入的作业需要投递；已有作业（含运行中/已成功）直接返回其 id
        if is_new:
            if self.synchronous:
                self.run_job(job_id)
            else:
                self._pool.submit(self.run_job, job_id)
        return job_id

    def get_job(self, job_id: int) -> sqlite3.Row | None:
        conn = get_db(self.db_path)
        try:
            return conn.execute(
                "SELECT * FROM partition_jobs WHERE id=?", (job_id,)
            ).fetchone()
        finally:
            conn.close()

    def list_jobs(self, limit: int = 20) -> list[sqlite3.Row]:
        conn = get_db(self.db_path)
        try:
            return list(
                conn.execute(
                    "SELECT j.*, v.status AS version_status FROM partition_jobs j "
                    "JOIN input_versions v ON v.id=j.version_id "
                    "ORDER BY j.id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            )
        finally:
            conn.close()

    def run_job(self, job_id: int) -> None:
        """作业主体：求解 + 原子发布。整个发布在单个写事务内，失败不留半成品。"""
        conn = get_db(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            job = conn.execute(
                "SELECT * FROM partition_jobs WHERE id=?", (job_id,)
            ).fetchone()
            if job is None or job["status"] not in ("queued", "running"):
                conn.execute("ROLLBACK")
                return
            version = conn.execute(
                "SELECT * FROM input_versions WHERE id=?", (job["version_id"],)
            ).fetchone()
            # 过期依据不得发布：求解期间输入被更正则版本已 superseded
            if version["status"] != "active":
                conn.execute(
                    "UPDATE partition_jobs SET status='failed', fail_stage='stale', "
                    "finished_at=datetime('now'), reason=? WHERE id=?",
                    (
                        "求解期间输入或配额已被更正，该版本已成为历史，过期依据不得发布。请基于当前版本重新划分。",
                        job_id,
                    ),
                )
                conn.execute("COMMIT")
                return
            # 并发保护：同版本已有成功清单则直接复用，不产生第二份
            existing = conn.execute(
                "SELECT id FROM manifests WHERE version_id=? AND status='published'",
                (version["id"],),
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE partition_jobs SET status='succeeded', "
                    "finished_at=datetime('now'), manifest_id=? WHERE id=?",
                    (existing["id"], job_id),
                )
                conn.execute("COMMIT")
                return

            conn.execute(
                "UPDATE partition_jobs SET status='running', "
                "started_at=datetime('now') WHERE id=?",
                (job_id,),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

        # ---- 事务外求解（CPU 工作，不持有写锁；发布前再次校验版本新鲜度）----
        try:
            result_data = self._compute(version)
        except SolveError as exc:
            self._fail_job(job_id, exc.stage, exc.reason, exc.detail)
            return
        except InputError as exc:
            self._fail_job(job_id, "validation", str(exc), {})
            return
        except Exception as exc:  # 兜底：任何意外都收敛为失败作业，绝不悬挂
            self._fail_job(job_id, "error", f"求解发生未预期错误：{exc}", {})
            return

        self._publish(job_id, version["id"], result_data)

    def _compute(self, version: sqlite3.Row) -> dict:
        images = parse_images_csv(version["image_csv"])
        dup_edges = parse_dups_csv(version["dup_csv"])
        components, classes = validate_and_build(images, dup_edges)
        quota = json.loads(version["quotas"])
        conn = get_db(self.db_path)
        try:
            registry = {
                r["image_no"]: r["side"]
                for r in conn.execute("SELECT image_no, side FROM side_registry").fetchall()
            }
        finally:
            conn.close()
        if self.solve_delay:
            import time

            time.sleep(self.solve_delay)
        result = solve(components, classes, quota, registry)
        img_index = {r.image_no: r for r in images}
        entries = []
        for comp in components:
            side = result.assignment[comp.cid]
            for img in comp.image_nos:
                row = img_index[img]
                entries.append(
                    {
                        "image_no": img,
                        "patient_no": row.patient_no,
                        "class_name": row.class_name,
                        "component_id": comp.cid,
                        "side": side,
                    }
                )
        return {
            "entries": entries,
            "classes": classes,
            "quota": quota,
            "locked": [
                {
                    "component_id": lc.component.cid,
                    "side": lc.side,
                    "locked_images": lc.locked_images,
                }
                for lc in result.locked
            ],
        }

    def _publish(self, job_id: int, version_id: int, data: dict) -> None:
        """原子发布：版本新鲜度复查 → 清单 → 侧别登记 → 作业成功，同一事务。"""
        conn = get_db(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            version = conn.execute(
                "SELECT status FROM input_versions WHERE id=?", (version_id,)
            ).fetchone()
            if version["status"] != "active":
                conn.execute(
                    "UPDATE partition_jobs SET status='failed', fail_stage='stale', "
                    "finished_at=datetime('now'), reason=? WHERE id=?",
                    ("求解期间输入或配额已被更正，过期依据不得发布。", job_id),
                )
                conn.execute("COMMIT")
                return
            dup = conn.execute(
                "SELECT id FROM manifests WHERE version_id=? AND status='published'",
                (version_id,),
            ).fetchone()
            if dup:
                conn.execute(
                    "UPDATE partition_jobs SET status='succeeded', "
                    "finished_at=datetime('now'), manifest_id=? WHERE id=?",
                    (dup["id"], job_id),
                )
                conn.execute("COMMIT")
                return

            # 登记侧别前再次核对：永久登记与本次分配必须一致，冲突则整体失败
            for e in data["entries"]:
                row = conn.execute(
                    "SELECT side FROM side_registry WHERE image_no=?",
                    (e["image_no"],),
                ).fetchone()
                if row is not None and row["side"] != e["side"]:
                    conn.execute(
                        "UPDATE partition_jobs SET status='failed', fail_stage='conflict', "
                        "finished_at=datetime('now'), reason=? WHERE id=?",
                        (
                            f"关联冲突：图像 {e['image_no']} 曾发布且侧别永久固定为 "
                            f"{row['side']}，本次求解结果为 {e['side']}，已中止发布，未写入任何分配。",
                            job_id,
                        ),
                    )
                    conn.execute("COMMIT")
                    return

            cur = conn.execute(
                "INSERT INTO manifests(version_id, status, reason) "
                "VALUES (?,'published', NULL)",
                (version_id,),
            )
            manifest_id = cur.lastrowid
            conn.executemany(
                "INSERT INTO manifest_entries(manifest_id, image_no, patient_no, "
                "class_name, component_id, side) VALUES (?,?,?,?,?,?)",
                [
                    (
                        manifest_id,
                        e["image_no"],
                        e["patient_no"],
                        e["class_name"],
                        e["component_id"],
                        e["side"],
                    )
                    for e in data["entries"]
                ],
            )
            # 永久侧别登记：已存在的不动（INSERT OR IGNORE 配合上面的冲突复查）
            conn.executemany(
                "INSERT OR IGNORE INTO side_registry(image_no, side, first_manifest_id) "
                "VALUES (?,?,?)",
                [
                    (e["image_no"], e["side"], manifest_id) for e in data["entries"]
                ],
            )
            conn.execute(
                "UPDATE partition_jobs SET status='succeeded', finished_at=datetime('now'), "
                "manifest_id=? WHERE id=?",
                (manifest_id, job_id),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            # 事务回滚后把作业标记失败（独立事务），避免悬挂
            try:
                self._fail_job(job_id, "error", "发布时发生数据库错误，事务已整体回滚，未留下半次分配。", {})
            except Exception:
                pass
            raise
        finally:
            conn.close()

    def _fail_job(self, job_id: int, stage: str, reason: str, detail: dict) -> None:
        # 失败只登记在 partition_jobs（历史/原因页展示），绝不产生半成品 manifest。
        # reason 存人类可读说明，detail 单独成列存结构化诊断。
        conn = get_db(self.db_path)
        try:
            conn.execute(
                "UPDATE partition_jobs SET status='failed', fail_stage=?, reason=?, "
                "detail=?, finished_at=datetime('now') WHERE id=?",
                (stage, reason, json.dumps(detail, ensure_ascii=False) if detail else None, job_id),
            )
        finally:
            conn.close()

    # ------------------------------------------------------------------ 清单
    def current_manifest(self) -> sqlite3.Row | None:
        version = self.current_version()
        if version is None:
            return None
        conn = get_db(self.db_path)
        try:
            return conn.execute(
                "SELECT * FROM manifests WHERE version_id=? AND status='published'",
                (version["id"],),
            ).fetchone()
        finally:
            conn.close()

    def get_manifest_entries(self, manifest_id: int) -> list[sqlite3.Row]:
        conn = get_db(self.db_path)
        try:
            return list(
                conn.execute(
                    "SELECT * FROM manifest_entries WHERE manifest_id=? "
                    "ORDER BY side DESC, class_name, image_no",
                    (manifest_id,),
                ).fetchall()
            )
        finally:
            conn.close()

    def list_manifests(self) -> list[sqlite3.Row]:
        conn = get_db(self.db_path)
        try:
            return list(
                conn.execute(
                    "SELECT m.*, v.status AS version_status, v.image_count "
                    "FROM manifests m JOIN input_versions v ON v.id=m.version_id "
                    "ORDER BY m.id DESC"
                ).fetchall()
            )
        finally:
            conn.close()

    def registry_snapshot(self) -> list[sqlite3.Row]:
        conn = get_db(self.db_path)
        try:
            return list(
                conn.execute(
                    "SELECT * FROM side_registry ORDER BY image_no"
                ).fetchall()
            )
        finally:
            conn.close()

    # ------------------------------------------------------------ 素材制包
    def start_package(self, source_name: str, source_bytes) -> int:
        """登记一次素材制包作业。

        - 只服务当前已发布清单；最近一次上传若仍在执行，直接复用该作业（并发只留一个）；
        - 每次上传的 ZIP 体积受 MAX_SOURCE_BYTES 限制，超限不落盘、不建作业；
        - 上传内容先写入临时目录，作业结束（成功/失败/重启）都会清理。
        """
        manifest, _version = self._current_publish()
        if manifest is None:
            raise InputError(
                "当前没有已发布清单，暂时不能制作素材包。请先完成划分并成功发布清单。"
            )
        if source_name and not source_name.lower().endswith(".zip"):
            raise InputError("请上传 .zip 格式的素材文件。")

        saved = self._spool_upload(source_bytes)
        job_id: int | None = None
        is_new = False
        failed = False
        try:
            with self._package_lock:
                conn = get_db(self.db_path)
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    running = conn.execute(
                        "SELECT id FROM package_jobs WHERE manifest_id=? AND "
                        "status IN ('queued','running') ORDER BY id DESC LIMIT 1",
                        (manifest["id"],),
                    ).fetchone()
                    if running is not None:
                        # 同一清单正在制包：只保留这一份作业，本次上传不重复起包
                        job_id = running["id"]
                    else:
                        same = conn.execute(
                            "SELECT j.id FROM package_jobs j "
                            "JOIN material_packages p ON p.job_id=j.id "
                            "WHERE j.manifest_id=? AND j.status='succeeded' "
                            "AND j.source_sha256=? AND p.status='published' "
                            "ORDER BY j.id DESC LIMIT 1",
                            (manifest["id"], saved["sha256"]),
                        ).fetchone()
                        if same is not None:
                            # 完全相同的素材重复上传：结果幂等，直接复用已发布的包
                            job_id = same["id"]
                        else:
                            cur = conn.execute(
                                "INSERT INTO package_jobs("
                                "version_id, manifest_id, source_name, source_path, "
                                "source_size, source_sha256, status) "
                                "VALUES (?,?,?,?,?,?,'queued')",
                                (
                                    manifest["version_id"],
                                    manifest["id"],
                                    (source_name or "upload.zip")[:200],
                                    str(saved["path"]),
                                    saved["size"],
                                    saved["sha256"],
                                ),
                            )
                            job_id = cur.lastrowid
                            is_new = True
                    conn.execute("COMMIT")
                except sqlite3.IntegrityError:
                    # 并发下另一个请求已登记成功：复用那份进行中的作业，本次上传丢弃
                    conn.execute("ROLLBACK")
                    row = conn.execute(
                        "SELECT id FROM package_jobs WHERE manifest_id=? AND "
                        "status IN ('queued','running') ORDER BY id LIMIT 1",
                        (manifest["id"],),
                    ).fetchone()
                    job_id = row["id"] if row else None
                    if job_id is None:
                        raise
                except Exception:
                    conn.execute("ROLLBACK")
                    failed = True
                    raise
                finally:
                    conn.close()
        finally:
            if failed or not is_new:
                # 登记失败或本次上传未被采用：立即清理临时文件
                _remove_file(saved["path"])
        if job_id is None:  # pragma: no cover - 防御
            raise InputError("无法登记制包作业，请稍后重试。")
        if not is_new and self.get_package_job(job_id) is None:
            raise InputError("制包作业不存在，请重新上传素材 ZIP。")

        if is_new:
            if self.synchronous:
                self.run_package_job(job_id)
            else:
                self._package_pool.submit(self.run_package_job, job_id)
        return job_id

    def _spool_upload(self, source_bytes) -> dict:
        """把上传流写入临时文件（限制体积），返回 path/size/sha256。"""
        path = self.package_tmp_dir / f"src_{uuid.uuid4().hex}.zip"
        hasher = hashlib.sha256()
        total = 0
        try:
            with open(path, "wb") as fh:
                while True:
                    chunk = source_bytes.read(1024 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > MAX_SOURCE_BYTES:
                        raise InputError(
                            f"素材 ZIP 超过 {MAX_SOURCE_BYTES // (1024 * 1024)} MiB 上传上限，"
                            "未生成素材包。"
                        )
                    hasher.update(chunk)
                    fh.write(chunk)
                fh.flush()
                os.fsync(fh.fileno())
        except Exception:
            _remove_file(path)
            raise
        if total == 0:
            _remove_file(path)
            raise InputError("上传的素材 ZIP 为空，未生成素材包。")
        return {"path": path, "size": total, "sha256": hasher.hexdigest()}

    def get_package_job(self, job_id: int) -> sqlite3.Row | None:
        conn = get_db(self.db_path)
        try:
            return conn.execute(
                "SELECT * FROM package_jobs WHERE id=?", (job_id,)
            ).fetchone()
        finally:
            conn.close()

    def list_package_jobs(self, limit: int = 20) -> list[sqlite3.Row]:
        """只返回当前已发布清单的制包作业：同清单并发只保留一份，历史清单的不在此列。"""
        manifest, _version = self._current_publish()
        if manifest is None:
            return []
        conn = get_db(self.db_path)
        try:
            return list(
                conn.execute(
                    "SELECT * FROM package_jobs WHERE manifest_id=? ORDER BY id DESC LIMIT ?",
                    (manifest["id"], limit),
                ).fetchall()
            )
        finally:
            conn.close()

    def list_package_jobs_all(self, limit: int = 100) -> list[sqlite3.Row]:
        conn = get_db(self.db_path)
        try:
            return list(
                conn.execute(
                    "SELECT j.*, v.status AS version_status "
                    "FROM package_jobs j JOIN input_versions v ON v.id=j.version_id "
                    "ORDER BY j.id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            )
        finally:
            conn.close()

    def list_packages(self) -> list[sqlite3.Row]:
        conn = get_db(self.db_path)
        try:
            return list(
                conn.execute(
                    "SELECT * FROM material_packages ORDER BY id DESC"
                ).fetchall()
            )
        finally:
            conn.close()

    def get_package(self, package_id: int) -> sqlite3.Row | None:
        conn = get_db(self.db_path)
        try:
            return conn.execute(
                "SELECT * FROM material_packages WHERE id=?", (package_id,)
            ).fetchone()
        finally:
            conn.close()

    def current_package(self) -> sqlite3.Row | None:
        """当前可下载素材包：所属清单仍为当前已发布清单，且包状态为 published。"""
        manifest, _version = self._current_publish()
        if manifest is None:
            return None
        conn = get_db(self.db_path)
        try:
            return conn.execute(
                "SELECT * FROM material_packages WHERE manifest_id=? AND status='published'",
                (manifest["id"],),
            ).fetchone()
        finally:
            conn.close()

    def _current_publish(self) -> tuple[sqlite3.Row | None, sqlite3.Row | None]:
        version = self.current_version()
        if version is None:
            return None, None
        conn = get_db(self.db_path)
        try:
            manifest = conn.execute(
                "SELECT * FROM manifests WHERE version_id=? AND status='published'",
                (version["id"],),
            ).fetchone()
        finally:
            conn.close()
        return manifest, version

    def package_downloadable(self, package_id: int) -> sqlite3.Row | None:
        """返回可下载的包行；历史版本、被替换、文件缺失一律返回 None。"""
        conn = get_db(self.db_path)
        try:
            row = conn.execute(
                "SELECT p.*, v.status AS version_status, m.status AS manifest_status "
                "FROM material_packages p "
                "JOIN input_versions v ON v.id=p.version_id "
                "JOIN manifests m ON m.id=p.manifest_id "
                "WHERE p.id=?",
                (package_id,),
            ).fetchone()
        finally:
            conn.close()
        if (
            row is None
            or row["status"] != "published"
            or row["version_status"] != "active"
            or row["manifest_status"] != "published"
        ):
            return None
        if not Path(row["file_path"]).is_file():
            return None
        return row

    def run_package_job(self, job_id: int) -> None:
        """制包作业：登记 running → 事务外制包 → 原子发布登记当前包。"""
        conn = get_db(self.db_path)
        source_path = None
        try:
            conn.execute("BEGIN IMMEDIATE")
            job = conn.execute(
                "SELECT * FROM package_jobs WHERE id=?", (job_id,)
            ).fetchone()
            if job is None or job["status"] != "queued":
                conn.execute("ROLLBACK")
                return
            manifest = conn.execute(
                "SELECT m.*, v.status AS version_status FROM manifests m "
                "JOIN input_versions v ON v.id=m.version_id WHERE m.id=?",
                (job["manifest_id"],),
            ).fetchone()
            if manifest is None or manifest["status"] != "published":
                conn.execute("ROLLBACK")
                self._fail_package_job(
                    job_id, "stale", "清单已失效，素材包未生成。", {}
                )
                return
            if manifest["version_status"] != "active":
                conn.execute("ROLLBACK")
                self._fail_package_job(
                    job_id,
                    "stale",
                    "着手制包时清单所属输入版本已被更正，过期依据不得制包。请基于当前清单重新上传素材。",
                    {},
                )
                return
            conn.execute(
                "UPDATE package_jobs SET status='running', started_at=datetime('now') "
                "WHERE id=?",
                (job_id,),
            )
            conn.execute("COMMIT")
            source_path = job["source_path"]
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

        entries = [
            dict(e)
            for e in self.get_manifest_entries(
                self.get_package_job(job_id)["manifest_id"]
            )
        ]
        out_tmp = self.package_tmp_dir / f"pkg_{job_id}_{uuid.uuid4().hex}.zip"
        try:
            if self.package_delay:
                time.sleep(self.package_delay)
            result = build_package(source_path, entries, out_tmp)
        except PackageError as exc:
            _remove_file(out_tmp)
            self._fail_package_job(job_id, exc.stage, exc.reason, exc.detail)
            _remove_file(source_path)
            return
        except Exception as exc:  # 兜底：任何意外都收敛为失败作业，绝不悬挂
            _remove_file(out_tmp)
            self._fail_package_job(
                job_id, "error", f"制包时发生未预期错误：{exc}", {}
            )
            _remove_file(source_path)
            return

        try:
            self._publish_package(job_id, result, out_tmp)
        finally:
            _remove_file(source_path)

    def _publish_package(self, job_id: int, result, out_tmp: Path) -> None:
        """原子发布：复查最新清单 → 落盘改名 → 旧包废止 → 新包登记，同一事务。"""
        conn = get_db(self.db_path)
        moved_path: Path | None = None
        try:
            conn.execute("BEGIN IMMEDIATE")
            job = conn.execute(
                "SELECT * FROM package_jobs WHERE id=?", (job_id,)
            ).fetchone()
            if job is None or job["status"] != "running":
                # 作业已被判失败（如重启恢复/其它收尾）：本次结果一律作废
                conn.execute("ROLLBACK")
                _remove_file(out_tmp)
                return
            manifest = conn.execute(
                "SELECT m.*, v.status AS version_status FROM manifests m "
                "JOIN input_versions v ON v.id=m.version_id WHERE m.id=?",
                (job["manifest_id"],),
            ).fetchone()
            current = conn.execute(
                "SELECT id FROM manifests WHERE version_id=? AND status='published'",
                (manifest["version_id"],),
            ).fetchone() if manifest else None
            if (
                manifest is None
                or manifest["version_status"] != "active"
                or current is None
                or current["id"] != manifest["id"]
            ):
                conn.execute("ROLLBACK")
                _remove_file(out_tmp)
                self._fail_package_job(
                    job_id,
                    "stale",
                    "制包期间输入已被更正或清单已失效，该素材包不得成为当前包，未登记下载。",
                    {},
                )
                return

            # 同一清单重新制包：先在同一事务里废止旧包，再登记新包，
            # 保证任何时刻至多一份 published（部分唯一索引兜底）。
            final_name = (
                f"materials_v{manifest['version_id']}_m{manifest['id']}_{job_id}.zip"
            )
            final_path = self.package_dir / final_name
            try:
                os.replace(out_tmp, final_path)
                _fsync_dir(self.package_dir)
                moved_path = final_path
            except OSError as exc:
                conn.execute("ROLLBACK")
                _remove_file(out_tmp)
                self._fail_package_job(
                    job_id, "error", f"素材包落盘失败：{exc}", {}
                )
                return

            prev = conn.execute(
                "SELECT id, file_path FROM material_packages WHERE manifest_id=? "
                "AND status='published'",
                (manifest["id"],),
            ).fetchall()
            for old in prev:
                conn.execute(
                    "UPDATE material_packages SET status='superseded', "
                    "fail_reason='已被同一清单的新素材包取代，停止下载' WHERE id=?",
                    (old["id"],),
                )
            cur = conn.execute(
                "INSERT INTO material_packages(version_id, manifest_id, job_id, "
                "file_name, file_path, size_bytes, sha256, entry_count, status) "
                "VALUES (?,?,?,?,?,?,?,?,'published')",
                (
                    manifest["version_id"],
                    manifest["id"],
                    job_id,
                    final_name,
                    str(final_path),
                    result.size_bytes,
                    result.sha256,
                    result.entry_count,
                ),
            )
            package_id = cur.lastrowid
            extra = (
                {"unmapped_files": result.unmapped_files[:50]}
                if result.unmapped_files
                else None
            )
            conn.execute(
                "UPDATE package_jobs SET status='succeeded', finished_at=datetime('now'), "
                "package_id=?, detail=?, source_path=NULL WHERE id=?",
                (
                    package_id,
                    json.dumps(extra, ensure_ascii=False) if extra else None,
                    job_id,
                ),
            )
            conn.execute("COMMIT")
            for old in prev:
                if old["file_path"] != str(final_path):
                    _remove_file(old["file_path"])
        except Exception:
            conn.execute("ROLLBACK")
            _remove_file(out_tmp)
            # 事务回滚后新包文件也一并删除，避免登记失败却残留可被误取的文件
            if moved_path is not None:
                _remove_file(moved_path)
            try:
                self._fail_package_job(
                    job_id,
                    "error",
                    "发布素材包时发生数据库错误，登记已整体回滚，未留下可下载的半包。",
                    {},
                )
            except Exception:
                pass
            raise
        finally:
            conn.close()

    def _fail_package_job(
        self, job_id: int, stage: str, reason: str, detail: dict
    ) -> None:
        conn = get_db(self.db_path)
        try:
            conn.execute(
                "UPDATE package_jobs SET status='failed', fail_stage=?, reason=?, "
                "detail=?, finished_at=datetime('now'), source_path=NULL "
                "WHERE id=? AND status IN ('queued','running')",
                (
                    stage,
                    reason,
                    json.dumps(detail, ensure_ascii=False) if detail else None,
                    job_id,
                ),
            )
        finally:
            conn.close()

    def supersede_packages_for_version(self, version_id: int) -> int:
        """输入更正后：该版本下所有已发布素材包立即作废（下载停供、文件删除）。"""
        return self.supersede_packages_except(None, only_version_id=version_id)

    def supersede_packages_except(
        self, keep_version_id: int | None, only_version_id: int | None = None
    ) -> int:
        """作废素材包登记后再删文件：数据库状态先失效，任何下载请求即刻停供。

        作废时写入 fail_reason，历史页可解释停供原因。
        """
        sql = "SELECT id, file_path FROM material_packages WHERE status='published'"
        params: list[int] = []
        reason = "所属输入版本已被更正，素材包停止下载"
        if keep_version_id is not None:
            sql += " AND version_id<>?"
            params.append(keep_version_id)
        elif only_version_id is not None:
            sql += " AND version_id=?"
            params.append(only_version_id)
            reason = "输入已被更正，该素材包停止下载"
        conn = get_db(self.db_path)
        try:
            rows = conn.execute(sql, params).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE material_packages SET status='superseded', fail_reason=? "
                    "WHERE id=?",
                    (reason, row["id"]),
                )
        finally:
            conn.close()
        for row in rows:
            _remove_file(row["file_path"])
        return len(rows)

    def sweep_orphan_packages(self) -> None:
        """只保留数据库登记为 published 的包文件；重启中断留下的临时/半包全部删除。"""
        conn = get_db(self.db_path)
        try:
            live = {
                row["file_path"]
                for row in conn.execute(
                    "SELECT file_path FROM material_packages WHERE status='published'"
                ).fetchall()
            }
        finally:
            conn.close()
        for path in self.package_dir.glob("materials_*.zip"):
            if str(path) not in live:
                _remove_file(path)
        _clean_dir(self.package_tmp_dir)


def _remove_file(path: str | Path | None) -> None:
    if not path:
        return
    try:
        os.remove(path)
    except OSError:
        pass


def _clean_dir(path: Path) -> None:
    try:
        for child in path.iterdir():
            if child.is_file():
                _remove_file(child)
            else:
                shutil.rmtree(child, ignore_errors=True)
    except OSError:
        pass


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass
