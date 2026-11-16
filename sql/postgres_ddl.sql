-- PostgreSQL 血缘追加存储表结构（纯追加写 Append-Only）
-- 每次任务解析生成唯一单调递增的 batch_id，不执行 UPDATE / DELETE / TRUNCATE；
-- 查询时默认通过 (job_name, MAX(batch_id)) 取各任务最新批次生效，亦可回溯历史全量批次。

CREATE TABLE IF NOT EXISTS hl_job_batches (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    batch_id BIGINT NOT NULL,
    dag_name VARCHAR(255) NOT NULL DEFAULT '',
    task_name VARCHAR(255) NOT NULL DEFAULT '',
    dialect VARCHAR(32) NOT NULL DEFAULT 'hive',
    default_db VARCHAR(128) NOT NULL DEFAULT '',
    files_json TEXT NOT NULL DEFAULT '[]',
    statement_count INTEGER NOT NULL DEFAULT 0,
    parsed_count INTEGER NOT NULL DEFAULT 0,
    fallback_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    table_count INTEGER NOT NULL DEFAULT 0,
    table_edge_count INTEGER NOT NULL DEFAULT 0,
    column_edge_count INTEGER NOT NULL DEFAULT 0,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hl_job_batches_job_batch
    ON hl_job_batches (job_name, batch_id DESC);
CREATE INDEX IF NOT EXISTS idx_hl_job_batches_batch_id
    ON hl_job_batches (batch_id);

CREATE TABLE IF NOT EXISTS hl_job_tables (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    batch_id BIGINT NOT NULL,
    db_name VARCHAR(128) NOT NULL DEFAULT '',
    table_name VARCHAR(255) NOT NULL,
    full_table_name VARCHAR(384) NOT NULL,
    table_kind VARCHAR(32) NOT NULL DEFAULT 'table',
    is_source BOOLEAN NOT NULL DEFAULT FALSE,
    is_target BOOLEAN NOT NULL DEFAULT FALSE,
    columns_json TEXT NOT NULL DEFAULT '[]',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hl_job_tables_job_batch
    ON hl_job_tables (job_name, batch_id);
CREATE INDEX IF NOT EXISTS idx_hl_job_tables_full_name
    ON hl_job_tables (full_table_name, is_target, is_source);

CREATE TABLE IF NOT EXISTS hl_table_edges (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    batch_id BIGINT NOT NULL,
    source_db VARCHAR(128) NOT NULL DEFAULT '',
    source_table VARCHAR(255) NOT NULL,
    source_full_name VARCHAR(384) NOT NULL,
    target_db VARCHAR(128) NOT NULL DEFAULT '',
    target_table VARCHAR(255) NOT NULL,
    target_full_name VARCHAR(384) NOT NULL,
    stmt_indices_json TEXT NOT NULL DEFAULT '[]',
    sql_preview TEXT NOT NULL DEFAULT '',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hl_table_edges_job_batch
    ON hl_table_edges (job_name, batch_id);
CREATE INDEX IF NOT EXISTS idx_hl_table_edges_target
    ON hl_table_edges (target_full_name);
CREATE INDEX IF NOT EXISTS idx_hl_table_edges_source
    ON hl_table_edges (source_full_name);

CREATE TABLE IF NOT EXISTS hl_column_edges (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    batch_id BIGINT NOT NULL,
    source_db VARCHAR(128) NOT NULL DEFAULT '',
    source_table VARCHAR(255) NOT NULL,
    source_full_name VARCHAR(384) NOT NULL,
    source_column VARCHAR(255) NOT NULL,
    target_db VARCHAR(128) NOT NULL DEFAULT '',
    target_table VARCHAR(255) NOT NULL,
    target_full_name VARCHAR(384) NOT NULL,
    target_column VARCHAR(255) NOT NULL,
    edge_kind VARCHAR(32) NOT NULL DEFAULT 'direct',
    stmt_indices_json TEXT NOT NULL DEFAULT '[]',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hl_column_edges_job_batch
    ON hl_column_edges (job_name, batch_id);
CREATE INDEX IF NOT EXISTS idx_hl_column_edges_target
    ON hl_column_edges (target_full_name, target_column);
CREATE INDEX IF NOT EXISTS idx_hl_column_edges_source
    ON hl_column_edges (source_full_name, source_column);
