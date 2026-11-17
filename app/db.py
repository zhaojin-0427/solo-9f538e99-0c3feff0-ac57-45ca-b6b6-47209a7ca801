"""SQLite 访问层：建表、连接管理、常量。

所有需要原子性的发布动作都在单个写事务中完成，并配合部分唯一索引保证：
- 同一输入版本最多一份已发布清单；
- 同一图像的侧别登记全局唯一（永久固定，不被后续分配改写）。
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

MAX_IMAGES = 40
MAX_CLASSES = 4
VALID = "val"
TRAIN = "train"
SIDES = (TRAIN, VALID)


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS input_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    status TEXT NOT NULL DEFAULT 'active',          -- active | superseded
    image_csv TEXT NOT NULL,                        -- 原始 CSV 全文，留档
    dup_csv TEXT NOT NULL,
    image_count INTEGER NOT NULL,
    class_names TEXT NOT NULL,                      -- 逗号分隔，固定 4 类
    quotas TEXT NOT NULL,                           -- JSON: {类别: 验证张数}
    note TEXT
);

CREATE TABLE IF NOT EXISTS images (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id INTEGER NOT NULL REFERENCES input_versions(id),
    image_no TEXT NOT NULL,
    patient_no TEXT NOT NULL,
    class_name TEXT NOT NULL,
    UNIQUE(version_id, image_no)
);

CREATE TABLE IF NOT EXISTS dup_edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id INTEGER NOT NULL REFERENCES input_versions(id),
    image_a TEXT NOT NULL,
    image_b TEXT NOT NULL
);

-- 侧别登记：永久记录，图像号全局唯一。模型一旦在某侧“见过”，永不更改。
CREATE TABLE IF NOT EXISTS side_registry (
    image_no TEXT PRIMARY KEY,
    side TEXT NOT NULL CHECK (side IN ('train', 'val')),
    first_manifest_id INTEGER,
    locked_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS manifests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id INTEGER NOT NULL REFERENCES input_versions(id),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    status TEXT NOT NULL CHECK (status IN ('published', 'failed')),
    reason TEXT
);

CREATE TABLE IF NOT EXISTS manifest_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_id INTEGER NOT NULL REFERENCES manifests(id),
    image_no TEXT NOT NULL,
    patient_no TEXT NOT NULL,
    class_name TEXT NOT NULL,
    component_id INTEGER NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('train', 'val')),
    UNIQUE(manifest_id, image_no)
);

CREATE TABLE IF NOT EXISTS partition_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id INTEGER NOT NULL REFERENCES input_versions(id),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT,
    finished_at TEXT,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    fail_stage TEXT,                               -- validation | conflict | quota | stale | restart
    reason TEXT,
    detail TEXT,                                   -- 结构化诊断 JSON
    manifest_id INTEGER
);

-- 素材制包作业：每个作业对应一次上传的素材 ZIP，只服务当前已发布清单。
CREATE TABLE IF NOT EXISTS package_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id INTEGER NOT NULL REFERENCES input_versions(id),
    manifest_id INTEGER NOT NULL REFERENCES manifests(id),
    source_name TEXT,                              -- 用户上传文件名（仅展示）
    source_path TEXT,                              -- 服务端临时文件，作业结束即清理
    source_size INTEGER,
    source_sha256 TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT,
    finished_at TEXT,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    fail_stage TEXT,                               -- format | mapping | path | missing | digest | size | stale | restart | error
    reason TEXT,
    detail TEXT,
    package_id INTEGER
);

-- 已发布素材包：manifest_id 唯一的“当前包”由部分唯一索引保证。
CREATE TABLE IF NOT EXISTS material_packages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id INTEGER NOT NULL REFERENCES input_versions(id),
    manifest_id INTEGER NOT NULL REFERENCES manifests(id),
    job_id INTEGER,
    file_name TEXT NOT NULL,
    file_path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    entry_count INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('published', 'superseded')),
    fail_reason TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 同一输入版本至多一份已发布清单（失败记录可多条，用于历史/原因查看）。
CREATE UNIQUE INDEX IF NOT EXISTS uq_manifest_published
    ON manifests(version_id) WHERE status = 'published';

-- 同一输入版本至多一个“进行中/成功”的划分作业（数据库级硬约束，跨线程/进程有效）。
-- 失败作业不占该约束位，允许更正后或针对同一输入重新发起重试。
CREATE UNIQUE INDEX IF NOT EXISTS uq_job_active
    ON partition_jobs(version_id) WHERE status IN ('queued', 'running', 'succeeded');

-- 同一清单至多一个正在执行的制包作业（顺序重传可替换旧包，旧 job 不占位），
-- 以及至多一份已发布素材包。并发上传只有第一个能登记/发布，可下载的包始终只有一份。
CREATE UNIQUE INDEX IF NOT EXISTS uq_package_job_active
    ON package_jobs(manifest_id) WHERE status IN ('queued', 'running');
CREATE UNIQUE INDEX IF NOT EXISTS uq_package_published
    ON material_packages(manifest_id) WHERE status = 'published';
"""


def get_db(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(db_path: str | Path) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = get_db(db_path)
    try:
        conn.executescript(SCHEMA)
        # 重启恢复：进程内线程池不持久，残留 queued/running 任务一律判失败，
        # 绝不允许“半次分配”——它们从未写入 manifest/registry，直接收尾。
        conn.execute(
            "UPDATE partition_jobs SET status='failed', fail_stage='restart', "
            "finished_at=datetime('now'), "
            "reason='服务重启，该任务未完成，未产生任何分配，请重新发起划分' "
            "WHERE status IN ('queued','running')"
        )
        # 制包作业同理：中断的作业从未登记 material_packages，不会留下可下载半包。
        conn.execute(
            "UPDATE package_jobs SET status='failed', fail_stage='restart', "
            "finished_at=datetime('now'), source_path=NULL, "
            "reason='服务重启，该素材包未完成，未登记下载，请重新上传素材 ZIP' "
            "WHERE status IN ('queued','running')"
        )
    finally:
        conn.close()
