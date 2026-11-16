//! PostgreSQL 纯追加存储与 `WITH RECURSIVE` 深度图递归查询实现

use crate::db::schema::{mask_db_url, postgres_ddl};
use crate::db::{
    BatchPayload, BatchRow, BoxFut, ColumnEdgeRow, JobSearchItem, JobTableRow, LineageBackend,
    LineageDirection, QueryScope, RawColumnSubgraph, RawTableSubgraph, RawTaskSubgraph,
    TableEdgeRow, TableSearchItem,
};
use anyhow::Context;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::collections::VecDeque;
use tokio::sync::Mutex;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls};

const INSERT_CHUNK_SIZE: usize = 200;
const SHALLOW_HOP_THRESHOLD: u32 = 6;
const MAX_UNLIMITED_HOPS: u32 = 64;

pub struct PgBackend {
    url: String,
    client: Mutex<Option<Client>>,
}

impl PgBackend {
    pub async fn connect(url: impl Into<String>) -> anyhow::Result<Self> {
        let url = url.into();
        let backend = Self {
            url,
            client: Mutex::new(None),
        };
        let mut guard = backend.client.lock().await;
        *guard = Some(Self::open_client(&backend.url).await?);
        drop(guard);
        Ok(backend)
    }

    async fn open_client(url: &str) -> anyhow::Result<Client> {
        let masked = mask_db_url(url);
        let (client, connection) = tokio_postgres::connect(url, NoTls)
            .await
            .with_context(|| format!("连接 PostgreSQL 失败 ({masked})"))?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("[postgres] 连接后台通道断开: {e}");
            }
        });
        Ok(client)
    }

    async fn with_client(&self) -> anyhow::Result<tokio::sync::MutexGuard<'_, Option<Client>>> {
        let mut guard = self.client.lock().await;
        let need_reconnect = match guard.as_ref() {
            Some(c) => c.is_closed(),
            None => true,
        };
        if need_reconnect {
            *guard = Some(Self::open_client(&self.url).await?);
        }
        Ok(guard)
    }
}

/// 生成各组件查询中用于筛选有效批次的前置 CTE 片段
pub(crate) fn active_scope_cte(scope: QueryScope) -> &'static str {
    match scope {
        QueryScope::LatestBatch => {
            "latest_batches AS (
                SELECT DISTINCT ON (job_name) job_name, batch_id
                FROM hl_job_batches
                ORDER BY job_name, batch_id DESC
            )"
        }
        QueryScope::AllHistory => {
            "latest_batches AS (
                SELECT job_name, batch_id FROM hl_job_batches
            )"
        }
    }
}

fn assemble_payloads(
    batches: Vec<BatchRow>,
    job_tables: Vec<JobTableRow>,
    table_edges: Vec<TableEdgeRow>,
    col_edges: Vec<ColumnEdgeRow>,
) -> Vec<BatchPayload> {
    let mut map: HashMap<(String, i64), BatchPayload> = HashMap::default();
    let mut order = Vec::with_capacity(batches.len());
    for b in batches {
        let key = (b.job_name.clone(), b.batch_id);
        order.push(key.clone());
        map.insert(
            key,
            BatchPayload {
                batch: b,
                job_tables: Vec::new(),
                table_edges: Vec::new(),
                column_edges: Vec::new(),
            },
        );
    }
    for jt in job_tables {
        let key = (jt.job_name.clone(), jt.batch_id);
        if let Some(p) = map.get_mut(&key) {
            p.job_tables.push(jt);
        }
    }
    for te in table_edges {
        let key = (te.job_name.clone(), te.batch_id);
        if let Some(p) = map.get_mut(&key) {
            p.table_edges.push(te);
        }
    }
    for ce in col_edges {
        let key = (ce.job_name.clone(), ce.batch_id);
        if let Some(p) = map.get_mut(&key) {
            p.column_edges.push(ce);
        }
    }
    order.into_iter().filter_map(|k| map.remove(&k)).collect()
}

async fn load_batches_by_filter(
    client: &Client,
    where_clause: &str,
    params: &[&(dyn ToSql + Sync)],
) -> anyhow::Result<Vec<BatchPayload>> {
    let batch_sql = format!(
        "WITH target_b AS ({where_clause})
         SELECT b.job_name, b.batch_id, b.dag_name, b.task_name, b.dialect, b.default_db,
                b.files_json, b.statement_count, b.parsed_count, b.fallback_count,
                b.failed_count, b.table_count, b.table_edge_count, b.column_edge_count,
                (EXTRACT(EPOCH FROM b.ingested_at) * 1000)::bigint AS ingested_ms
         FROM hl_job_batches b
         INNER JOIN target_b tb ON b.job_name = tb.job_name AND b.batch_id = tb.batch_id
         ORDER BY b.batch_id ASC"
    );
    let b_rows = client.query(&batch_sql, params).await?;
    if b_rows.is_empty() {
        return Ok(Vec::new());
    }
    let batches: Vec<BatchRow> = b_rows
        .into_iter()
        .map(|r| BatchRow {
            job_name: r.get(0),
            batch_id: r.get(1),
            dag_name: r.get(2),
            task_name: r.get(3),
            dialect: r.get(4),
            default_db: r.get(5),
            files_json: r.get(6),
            statement_count: r.get(7),
            parsed_count: r.get(8),
            fallback_count: r.get(9),
            failed_count: r.get(10),
            table_count: r.get(11),
            table_edge_count: r.get(12),
            column_edge_count: r.get(13),
            ingested_at_ms: r.get(14),
        })
        .collect();

    let jt_sql = format!(
        "WITH target_b AS ({where_clause})
         SELECT t.job_name, t.batch_id, t.db_name, t.table_name, t.full_table_name,
                t.table_kind, t.is_source, t.is_target, t.columns_json
         FROM hl_job_tables t
         INNER JOIN target_b tb ON t.job_name = tb.job_name AND t.batch_id = tb.batch_id"
    );
    let jt_rows = client.query(&jt_sql, params).await?;
    let job_tables: Vec<JobTableRow> = jt_rows
        .into_iter()
        .map(|r| {
            let cols_json: String = r.get(8);
            let columns: Vec<String> = serde_json::from_str(&cols_json).unwrap_or_default();
            JobTableRow {
                job_name: r.get(0),
                batch_id: r.get(1),
                db_name: r.get(2),
                table_name: r.get(3),
                full_table_name: r.get(4),
                table_kind: r.get(5),
                is_source: r.get(6),
                is_target: r.get(7),
                columns,
            }
        })
        .collect();

    let te_sql = format!(
        "WITH target_b AS ({where_clause})
         SELECT e.job_name, e.batch_id, e.source_db, e.source_table, e.source_full_name,
                e.target_db, e.target_table, e.target_full_name, e.stmt_indices_json, e.sql_preview
         FROM hl_table_edges e
         INNER JOIN target_b tb ON e.job_name = tb.job_name AND e.batch_id = tb.batch_id"
    );
    let te_rows = client.query(&te_sql, params).await?;
    let table_edges: Vec<TableEdgeRow> = te_rows
        .into_iter()
        .map(|r| {
            let stmts_json: String = r.get(8);
            let stmt_indices: Vec<usize> = serde_json::from_str(&stmts_json).unwrap_or_default();
            TableEdgeRow {
                job_name: r.get(0),
                batch_id: r.get(1),
                source_db: r.get(2),
                source_table: r.get(3),
                source_full_name: r.get(4),
                target_db: r.get(5),
                target_table: r.get(6),
                target_full_name: r.get(7),
                stmt_indices,
                sql_preview: r.get(9),
            }
        })
        .collect();

    let ce_sql = format!(
        "WITH target_b AS ({where_clause})
         SELECT c.job_name, c.batch_id, c.source_db, c.source_table, c.source_full_name,
                c.source_column, c.target_db, c.target_table, c.target_full_name,
                c.target_column, c.edge_kind, c.stmt_indices_json
         FROM hl_column_edges c
         INNER JOIN target_b tb ON c.job_name = tb.job_name AND c.batch_id = tb.batch_id"
    );
    let ce_rows = client.query(&ce_sql, params).await?;
    let col_edges: Vec<ColumnEdgeRow> = ce_rows
        .into_iter()
        .map(|r| {
            let stmts_json: String = r.get(11);
            let stmt_indices: Vec<usize> = serde_json::from_str(&stmts_json).unwrap_or_default();
            ColumnEdgeRow {
                job_name: r.get(0),
                batch_id: r.get(1),
                source_db: r.get(2),
                source_table: r.get(3),
                source_full_name: r.get(4),
                source_column: r.get(5),
                target_db: r.get(6),
                target_table: r.get(7),
                target_full_name: r.get(8),
                target_column: r.get(9),
                edge_kind: r.get(10),
                stmt_indices,
            }
        })
        .collect();

    Ok(assemble_payloads(
        batches,
        job_tables,
        table_edges,
        col_edges,
    ))
}

/// 在返回的连通边集合上计算精确最短跳数并按 `cap` 裁剪，消除超深菱形 DAG 在 SQL 层的路径爆炸
fn compute_directed_distances<K, F>(
    start: K,
    cap: u32,
    adj: &HashMap<K, Vec<K>>,
    mut eq_key: F,
) -> HashMap<K, u32>
where
    K: Eq + std::hash::Hash + Clone,
    F: FnMut(&K, &K) -> bool,
{
    let mut dist: HashMap<K, u32> = HashMap::default();
    let mut q: VecDeque<(K, u32)> = VecDeque::new();
    q.push_back((start.clone(), 0));
    while let Some((u, d)) = q.pop_front() {
        if d >= cap {
            continue;
        }
        if let Some(nxts) = adj.get(&u) {
            for v in nxts {
                if eq_key(v, &start) || dist.contains_key(v) {
                    continue;
                }
                dist.insert(v.clone(), d + 1);
                q.push_back((v.clone(), d + 1));
            }
        }
    }
    dist
}

impl LineageBackend for PgBackend {
    fn backend_name(&self) -> &'static str {
        "postgres"
    }

    fn ping(&self) -> BoxFut<'_, ()> {
        Box::pin(async move {
            let guard = self.with_client().await?;
            let client = guard.as_ref().unwrap();
            client.simple_query("SELECT 1").await?;
            Ok(())
        })
    }

    fn ensure_schema(&self) -> BoxFut<'_, ()> {
        Box::pin(async move {
            let guard = self.with_client().await?;
            let client = guard.as_ref().unwrap();
            client
                .batch_execute(postgres_ddl())
                .await
                .context("初始化 PostgreSQL 血缘表结构失败")?;
            Ok(())
        })
    }

    fn append_batch<'a>(&'a self, payload: &'a BatchPayload) -> BoxFut<'a, ()> {
        Box::pin(async move {
            let mut guard = self.with_client().await?;
            let client = guard.as_mut().unwrap();
            let tx = client
                .transaction()
                .await
                .context("开启 PostgreSQL 追加写事务失败")?;

            let b = &payload.batch;
            let ingested_sec = (b.ingested_at_ms as f64) / 1000.0;
            tx.execute(
                "INSERT INTO hl_job_batches (
                    job_name, batch_id, dag_name, task_name, dialect, default_db,
                    files_json, statement_count, parsed_count, fallback_count,
                    failed_count, table_count, table_edge_count, column_edge_count, ingested_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, $8, $9, $10,
                    $11, $12, $13, $14, to_timestamp($15)
                )",
                &[
                    &b.job_name,
                    &b.batch_id,
                    &b.dag_name,
                    &b.task_name,
                    &b.dialect,
                    &b.default_db,
                    &b.files_json,
                    &b.statement_count,
                    &b.parsed_count,
                    &b.fallback_count,
                    &b.failed_count,
                    &b.table_count,
                    &b.table_edge_count,
                    &b.column_edge_count,
                    &ingested_sec,
                ],
            )
            .await
            .context("追加写入 hl_job_batches 失败")?;

            // 批量追加 hl_job_tables
            for chunk in payload.job_tables.chunks(INSERT_CHUNK_SIZE) {
                let cols_jsons: Vec<String> = chunk
                    .iter()
                    .map(|r| serde_json::to_string(&r.columns).unwrap_or_else(|_| "[]".into()))
                    .collect();
                let mut sql = String::from(
                    "INSERT INTO hl_job_tables (
                        job_name, batch_id, db_name, table_name, full_table_name,
                        table_kind, is_source, is_target, columns_json, ingested_at
                    ) VALUES ",
                );
                let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(chunk.len() * 9 + 1);
                params.push(&ingested_sec);
                for (i, r) in chunk.iter().enumerate() {
                    if i > 0 {
                        sql.push(',');
                    }
                    let base = i * 9 + 2;
                    sql.push_str(&format!(
                        "(${},${},${},${},${},${},${},${},${},to_timestamp($1))",
                        base,
                        base + 1,
                        base + 2,
                        base + 3,
                        base + 4,
                        base + 5,
                        base + 6,
                        base + 7,
                        base + 8
                    ));
                    params.push(&r.job_name);
                    params.push(&r.batch_id);
                    params.push(&r.db_name);
                    params.push(&r.table_name);
                    params.push(&r.full_table_name);
                    params.push(&r.table_kind);
                    params.push(&r.is_source);
                    params.push(&r.is_target);
                    params.push(&cols_jsons[i]);
                }
                tx.execute(&sql, &params)
                    .await
                    .context("批量追加 hl_job_tables 失败")?;
            }

            // 批量追加 hl_table_edges
            for chunk in payload.table_edges.chunks(INSERT_CHUNK_SIZE) {
                let stmts_jsons: Vec<String> = chunk
                    .iter()
                    .map(|r| serde_json::to_string(&r.stmt_indices).unwrap_or_else(|_| "[]".into()))
                    .collect();
                let mut sql = String::from(
                    "INSERT INTO hl_table_edges (
                        job_name, batch_id, source_db, source_table, source_full_name,
                        target_db, target_table, target_full_name, stmt_indices_json, sql_preview, ingested_at
                    ) VALUES ",
                );
                let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(chunk.len() * 10 + 1);
                params.push(&ingested_sec);
                for (i, r) in chunk.iter().enumerate() {
                    if i > 0 {
                        sql.push(',');
                    }
                    let base = i * 10 + 2;
                    sql.push_str(&format!(
                        "(${},${},${},${},${},${},${},${},${},${},to_timestamp($1))",
                        base,
                        base + 1,
                        base + 2,
                        base + 3,
                        base + 4,
                        base + 5,
                        base + 6,
                        base + 7,
                        base + 8,
                        base + 9
                    ));
                    params.push(&r.job_name);
                    params.push(&r.batch_id);
                    params.push(&r.source_db);
                    params.push(&r.source_table);
                    params.push(&r.source_full_name);
                    params.push(&r.target_db);
                    params.push(&r.target_table);
                    params.push(&r.target_full_name);
                    params.push(&stmts_jsons[i]);
                    params.push(&r.sql_preview);
                }
                tx.execute(&sql, &params)
                    .await
                    .context("批量追加 hl_table_edges 失败")?;
            }

            // 批量追加 hl_column_edges
            for chunk in payload.column_edges.chunks(INSERT_CHUNK_SIZE) {
                let stmts_jsons: Vec<String> = chunk
                    .iter()
                    .map(|r| serde_json::to_string(&r.stmt_indices).unwrap_or_else(|_| "[]".into()))
                    .collect();
                let mut sql = String::from(
                    "INSERT INTO hl_column_edges (
                        job_name, batch_id, source_db, source_table, source_full_name, source_column,
                        target_db, target_table, target_full_name, target_column, edge_kind, stmt_indices_json, ingested_at
                    ) VALUES ",
                );
                let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(chunk.len() * 12 + 1);
                params.push(&ingested_sec);
                for (i, r) in chunk.iter().enumerate() {
                    if i > 0 {
                        sql.push(',');
                    }
                    let base = i * 12 + 2;
                    sql.push_str(&format!(
                        "(${},${},${},${},${},${},${},${},${},${},${},${},to_timestamp($1))",
                        base,
                        base + 1,
                        base + 2,
                        base + 3,
                        base + 4,
                        base + 5,
                        base + 6,
                        base + 7,
                        base + 8,
                        base + 9,
                        base + 10,
                        base + 11
                    ));
                    params.push(&r.job_name);
                    params.push(&r.batch_id);
                    params.push(&r.source_db);
                    params.push(&r.source_table);
                    params.push(&r.source_full_name);
                    params.push(&r.source_column);
                    params.push(&r.target_db);
                    params.push(&r.target_table);
                    params.push(&r.target_full_name);
                    params.push(&r.target_column);
                    params.push(&r.edge_kind);
                    params.push(&stmts_jsons[i]);
                }
                tx.execute(&sql, &params)
                    .await
                    .context("批量追加 hl_column_edges 失败")?;
            }

            tx.commit().await.context("提交 PostgreSQL 追加事务失败")?;
            Ok(())
        })
    }

    fn load_latest_snapshot(&self) -> BoxFut<'_, Vec<BatchPayload>> {
        Box::pin(async move {
            let guard = self.with_client().await?;
            let client = guard.as_ref().unwrap();
            load_batches_by_filter(
                client,
                "SELECT DISTINCT ON (job_name) job_name, batch_id
                 FROM hl_job_batches
                 ORDER BY job_name, batch_id DESC",
                &[],
            )
            .await
        })
    }

    fn fetch_batches_since(&self, after_batch_id: i64) -> BoxFut<'_, Vec<BatchPayload>> {
        Box::pin(async move {
            let guard = self.with_client().await?;
            let client = guard.as_ref().unwrap();
            load_batches_by_filter(
                client,
                "SELECT job_name, batch_id FROM hl_job_batches WHERE batch_id > $1",
                &[&after_batch_id],
            )
            .await
        })
    }

    fn search_jobs<'a>(
        &'a self,
        keyword: &'a str,
        limit: usize,
        scope: QueryScope,
    ) -> BoxFut<'a, Vec<JobSearchItem>> {
        Box::pin(async move {
            let guard = self.with_client().await?;
            let client = guard.as_ref().unwrap();
            let pattern = format!("%{}%", keyword.trim().to_lowercase());
            let lim = limit.clamp(1, 500) as i64;
            let cte = active_scope_cte(scope);
            let sql = format!(
                "WITH {cte},
                 matched_jobs AS (
                     SELECT DISTINCT ON (b.job_name)
                            b.job_name, b.dag_name, b.task_name, b.batch_id,
                            (EXTRACT(EPOCH FROM b.ingested_at) * 1000)::bigint AS ingested_ms
                     FROM hl_job_batches b
                     INNER JOIN latest_batches lb ON b.job_name = lb.job_name AND b.batch_id = lb.batch_id
                     WHERE LOWER(b.job_name) LIKE $1
                     ORDER BY b.job_name, b.batch_id DESC
                 )
                 SELECT mj.job_name, mj.dag_name, mj.task_name, mj.batch_id, mj.ingested_ms,
                        COALESCE(ARRAY_AGG(DISTINCT jt.full_table_name)
                            FILTER (WHERE jt.is_source AND jt.table_kind NOT IN ('temp','result')), ARRAY[]::varchar[]),
                        COALESCE(ARRAY_AGG(DISTINCT jt.full_table_name)
                            FILTER (WHERE jt.is_target AND jt.table_kind NOT IN ('temp','result')), ARRAY[]::varchar[])
                 FROM matched_jobs mj
                 LEFT JOIN latest_batches lb2 ON lb2.job_name = mj.job_name
                 LEFT JOIN hl_job_tables jt ON jt.job_name = lb2.job_name AND jt.batch_id = lb2.batch_id
                 GROUP BY mj.job_name, mj.dag_name, mj.task_name, mj.batch_id, mj.ingested_ms
                 ORDER BY mj.batch_id DESC, mj.job_name ASC
                 LIMIT $2"
            );
            let rows = client.query(&sql, &[&pattern, &lim]).await?;
            Ok(rows
                .into_iter()
                .map(|r| JobSearchItem {
                    job_name: r.get(0),
                    dag_name: r.get(1),
                    task_name: r.get(2),
                    latest_batch_id: r.get(3),
                    ingested_at_ms: r.get(4),
                    source_tables: r.get(5),
                    target_tables: r.get(6),
                })
                .collect())
        })
    }

    fn search_tables<'a>(
        &'a self,
        keyword: &'a str,
        limit: usize,
        scope: QueryScope,
    ) -> BoxFut<'a, Vec<TableSearchItem>> {
        Box::pin(async move {
            let guard = self.with_client().await?;
            let client = guard.as_ref().unwrap();
            let pattern = format!("%{}%", keyword.trim().to_lowercase());
            let lim = limit.clamp(1, 500) as i64;
            let cte = active_scope_cte(scope);
            let sql = format!(
                "WITH {cte}
                 SELECT jt.full_table_name,
                        MAX(jt.db_name) AS db_name,
                        MAX(jt.table_name) AS table_name,
                        MAX(jt.table_kind) AS table_kind,
                        COALESCE(ARRAY_AGG(DISTINCT jt.job_name) FILTER (WHERE jt.is_target), ARRAY[]::varchar[]),
                        COALESCE(ARRAY_AGG(DISTINCT jt.job_name) FILTER (WHERE jt.is_source), ARRAY[]::varchar[])
                 FROM hl_job_tables jt
                 INNER JOIN latest_batches lb ON jt.job_name = lb.job_name AND jt.batch_id = lb.batch_id
                 WHERE jt.table_kind NOT IN ('temp', 'result')
                   AND jt.full_table_name LIKE $1
                 GROUP BY jt.full_table_name
                 ORDER BY jt.full_table_name ASC
                 LIMIT $2"
            );
            let rows = client.query(&sql, &[&pattern, &lim]).await?;
            Ok(rows
                .into_iter()
                .map(|r| TableSearchItem {
                    full_table_name: r.get(0),
                    db_name: r.get(1),
                    table_name: r.get(2),
                    table_kind: r.get(3),
                    producer_jobs: r.get(4),
                    consumer_jobs: r.get(5),
                })
                .collect())
        })
    }

    fn query_table_subgraph<'a>(
        &'a self,
        table: &'a str,
        dir: LineageDirection,
        max_hops: u32,
        scope: QueryScope,
    ) -> BoxFut<'a, RawTableSubgraph> {
        Box::pin(async move {
            let guard = self.with_client().await?;
            let client = guard.as_ref().unwrap();
            let cap = if max_hops == 0 {
                MAX_UNLIMITED_HOPS
            } else {
                max_hops
            };
            let cte = active_scope_cte(scope);
            let shallow = max_hops > 0 && max_hops <= SHALLOW_HOP_THRESHOLD;
            let hop_i32 = cap as i32;

            let mut all_edges: Vec<(String, String, String, String)> = Vec::new();
            let mut seen_e: HashSet<(String, String, String)> = HashSet::default();
            let mut up_adj: HashMap<String, Vec<String>> = HashMap::default();
            let mut down_adj: HashMap<String, Vec<String>> = HashMap::default();

            if dir.want_up() {
                let rows = if shallow {
                    let sql = format!(
                        "WITH RECURSIVE {cte},
                         active_e AS (
                             SELECT e.source_full_name, e.target_full_name, e.job_name, e.sql_preview
                             FROM hl_table_edges e
                             INNER JOIN latest_batches lb ON e.job_name = lb.job_name AND e.batch_id = lb.batch_id
                         ),
                         walk(src, dst, job_name, sql_preview, depth, visited) AS (
                             SELECT e.source_full_name, e.target_full_name, e.job_name, e.sql_preview, 1,
                                    ARRAY[$1::varchar, e.source_full_name]
                             FROM active_e e
                             WHERE e.target_full_name = $1
                             UNION ALL
                             SELECT e.source_full_name, e.target_full_name, e.job_name, e.sql_preview, w.depth + 1,
                                    w.visited || e.source_full_name
                             FROM active_e e
                             INNER JOIN walk w ON e.target_full_name = w.src
                             WHERE w.depth < $2 AND NOT (e.source_full_name = ANY(w.visited))
                         )
                         SELECT DISTINCT src, dst, job_name, sql_preview FROM walk"
                    );
                    client.query(&sql, &[&table, &hop_i32]).await?
                } else {
                    let sql = format!(
                        "WITH RECURSIVE {cte},
                         active_e AS (
                             SELECT e.source_full_name, e.target_full_name, e.job_name, e.sql_preview
                             FROM hl_table_edges e
                             INNER JOIN latest_batches lb ON e.job_name = lb.job_name AND e.batch_id = lb.batch_id
                         ),
                         walk(src, dst, job_name, sql_preview) AS (
                             SELECT e.source_full_name, e.target_full_name, e.job_name, e.sql_preview
                             FROM active_e e
                             WHERE e.target_full_name = $1
                             UNION
                             SELECT e.source_full_name, e.target_full_name, e.job_name, e.sql_preview
                             FROM active_e e
                             INNER JOIN walk w ON e.target_full_name = w.src
                         )
                         SELECT src, dst, job_name, sql_preview FROM walk"
                    );
                    client.query(&sql, &[&table]).await?
                };
                for r in rows {
                    let s: String = r.get(0);
                    let d: String = r.get(1);
                    let j: String = r.get(2);
                    let p: String = r.get(3);
                    up_adj.entry(d.clone()).or_default().push(s.clone());
                    all_edges.push((s, d, j, p));
                }
            }

            if dir.want_down() {
                let rows = if shallow {
                    let sql = format!(
                        "WITH RECURSIVE {cte},
                         active_e AS (
                             SELECT e.source_full_name, e.target_full_name, e.job_name, e.sql_preview
                             FROM hl_table_edges e
                             INNER JOIN latest_batches lb ON e.job_name = lb.job_name AND e.batch_id = lb.batch_id
                         ),
                         walk(src, dst, job_name, sql_preview, depth, visited) AS (
                             SELECT e.source_full_name, e.target_full_name, e.job_name, e.sql_preview, 1,
                                    ARRAY[$1::varchar, e.target_full_name]
                             FROM active_e e
                             WHERE e.source_full_name = $1
                             UNION ALL
                             SELECT e.source_full_name, e.target_full_name, e.job_name, e.sql_preview, w.depth + 1,
                                    w.visited || e.target_full_name
                             FROM active_e e
                             INNER JOIN walk w ON e.source_full_name = w.dst
                             WHERE w.depth < $2 AND NOT (e.target_full_name = ANY(w.visited))
                         )
                         SELECT DISTINCT src, dst, job_name, sql_preview FROM walk"
                    );
                    client.query(&sql, &[&table, &hop_i32]).await?
                } else {
                    let sql = format!(
                        "WITH RECURSIVE {cte},
                         active_e AS (
                             SELECT e.source_full_name, e.target_full_name, e.job_name, e.sql_preview
                             FROM hl_table_edges e
                             INNER JOIN latest_batches lb ON e.job_name = lb.job_name AND e.batch_id = lb.batch_id
                         ),
                         walk(src, dst, job_name, sql_preview) AS (
                             SELECT e.source_full_name, e.target_full_name, e.job_name, e.sql_preview
                             FROM active_e e
                             WHERE e.source_full_name = $1
                             UNION
                             SELECT e.source_full_name, e.target_full_name, e.job_name, e.sql_preview
                             FROM active_e e
                             INNER JOIN walk w ON e.source_full_name = w.dst
                         )
                         SELECT src, dst, job_name, sql_preview FROM walk"
                    );
                    client.query(&sql, &[&table]).await?
                };
                for r in rows {
                    let s: String = r.get(0);
                    let d: String = r.get(1);
                    let j: String = r.get(2);
                    let p: String = r.get(3);
                    down_adj.entry(s.clone()).or_default().push(d.clone());
                    all_edges.push((s, d, j, p));
                }
            }

            let up_dist =
                compute_directed_distances(table.to_string(), cap, &up_adj, |a, b| a == b);
            let down_dist =
                compute_directed_distances(table.to_string(), cap, &down_adj, |a, b| a == b);

            let mut valid_nodes: HashSet<String> = HashSet::default();
            valid_nodes.insert(table.to_string());
            valid_nodes.extend(up_dist.keys().cloned());
            valid_nodes.extend(down_dist.keys().cloned());

            let filtered_edges: Vec<(String, String, String, String)> = all_edges
                .into_iter()
                .filter(|(s, d, j, _)| {
                    valid_nodes.contains(s)
                        && valid_nodes.contains(d)
                        && seen_e.insert((s.clone(), d.clone(), j.clone()))
                })
                .collect();

            let tbl_vec: Vec<String> = valid_nodes.into_iter().collect();
            let jt_sql = format!(
                "WITH {cte}
                 SELECT jt.job_name, jt.batch_id, jt.db_name, jt.table_name, jt.full_table_name,
                        jt.table_kind, jt.is_source, jt.is_target, jt.columns_json
                 FROM hl_job_tables jt
                 INNER JOIN latest_batches lb ON jt.job_name = lb.job_name AND jt.batch_id = lb.batch_id
                 WHERE jt.full_table_name = ANY($1)"
            );
            let jt_rows = client.query(&jt_sql, &[&tbl_vec]).await?;
            let job_tables: Vec<JobTableRow> = jt_rows
                .into_iter()
                .map(|r| {
                    let cj: String = r.get(8);
                    JobTableRow {
                        job_name: r.get(0),
                        batch_id: r.get(1),
                        db_name: r.get(2),
                        table_name: r.get(3),
                        full_table_name: r.get(4),
                        table_kind: r.get(5),
                        is_source: r.get(6),
                        is_target: r.get(7),
                        columns: serde_json::from_str(&cj).unwrap_or_default(),
                    }
                })
                .collect();

            Ok(RawTableSubgraph {
                edges: filtered_edges,
                job_tables,
                up_distances: up_dist.into_iter().collect(),
                down_distances: down_dist.into_iter().collect(),
            })
        })
    }

    fn query_table_task_subgraph<'a>(
        &'a self,
        table: &'a str,
        dir: LineageDirection,
        max_hops: u32,
        scope: QueryScope,
    ) -> BoxFut<'a, RawTaskSubgraph> {
        Box::pin(async move {
            let guard = self.with_client().await?;
            let client = guard.as_ref().unwrap();
            let cte = active_scope_cte(scope);

            // 先查出当前表的直接产出任务和直接消费任务
            let seed_sql = format!(
                "WITH {cte}
                 SELECT DISTINCT jt.job_name, jt.is_target, jt.is_source
                 FROM hl_job_tables jt
                 INNER JOIN latest_batches lb ON jt.job_name = lb.job_name AND jt.batch_id = lb.batch_id
                 WHERE jt.full_table_name = $1 AND jt.table_kind NOT IN ('temp', 'result')"
            );
            let seed_rows = client.query(&seed_sql, &[&table]).await?;
            let mut producers = Vec::new();
            let mut consumers = Vec::new();
            for r in seed_rows {
                let j: String = r.get(0);
                let is_t: bool = r.get(1);
                let is_s: bool = r.get(2);
                if is_t && dir.want_up() && !producers.contains(&j) {
                    producers.push(j.clone());
                }
                if is_s && dir.want_down() && !consumers.contains(&j) {
                    consumers.push(j);
                }
            }
            producers.sort();
            consumers.sort();
            drop(guard);

            self.walk_pg_tasks(&producers, &consumers, Some(table), dir, max_hops, scope, 1)
                .await
        })
    }

    fn query_task_subgraph<'a>(
        &'a self,
        job_name: &'a str,
        dir: LineageDirection,
        max_hops: u32,
        scope: QueryScope,
    ) -> BoxFut<'a, RawTaskSubgraph> {
        Box::pin(async move {
            let guard = self.with_client().await?;
            let client = guard.as_ref().unwrap();
            let cte = active_scope_cte(scope);
            let find_sql = format!(
                "WITH {cte}
                 SELECT b.job_name FROM hl_job_batches b
                 INNER JOIN latest_batches lb ON b.job_name = lb.job_name AND b.batch_id = lb.batch_id
                 WHERE LOWER(b.job_name) = LOWER($1)
                 LIMIT 1"
            );
            let row_opt = client.query_opt(&find_sql, &[&job_name.trim()]).await?;
            drop(guard);
            let Some(row) = row_opt else {
                return Ok(RawTaskSubgraph::default());
            };
            let canonical: String = row.get(0);
            let seeds = vec![canonical];
            self.walk_pg_tasks(&seeds, &seeds, None, dir, max_hops, scope, 0)
                .await
        })
    }

    fn query_column_subgraph<'a>(
        &'a self,
        full_table: &'a str,
        column: &'a str,
        dir: LineageDirection,
        max_hops: u32,
        scope: QueryScope,
    ) -> BoxFut<'a, RawColumnSubgraph> {
        Box::pin(async move {
            let guard = self.with_client().await?;
            let client = guard.as_ref().unwrap();
            let cap = if max_hops == 0 {
                MAX_UNLIMITED_HOPS
            } else {
                max_hops
            };
            let cte = active_scope_cte(scope);

            let mut raw_edges: Vec<ColumnEdgeRow> = Vec::new();
            let mut up_adj: HashMap<(String, String), Vec<(String, String)>> = HashMap::default();
            let mut down_adj: HashMap<(String, String), Vec<(String, String)>> = HashMap::default();

            if dir.want_up() {
                let sql = format!(
                    "WITH RECURSIVE {cte},
                     active_c AS (
                         SELECT c.job_name, c.batch_id, c.source_db, c.source_table, c.source_full_name,
                                c.source_column, c.target_db, c.target_table, c.target_full_name,
                                c.target_column, c.edge_kind, c.stmt_indices_json
                         FROM hl_column_edges c
                         INNER JOIN latest_batches lb ON c.job_name = lb.job_name AND c.batch_id = lb.batch_id
                     ),
                     walk AS (
                         SELECT * FROM active_c
                         WHERE target_full_name = $1 AND (target_column = $2 OR target_column = '*')
                         UNION
                         SELECT c.* FROM active_c c
                         INNER JOIN walk w
                             ON c.target_full_name = w.source_full_name
                            AND (c.target_column = w.source_column OR c.target_column = '*' OR w.source_column = '*')
                     )
                     SELECT job_name, batch_id, source_db, source_table, source_full_name, source_column,
                            target_db, target_table, target_full_name, target_column, edge_kind, stmt_indices_json
                     FROM walk"
                );
                let rows = client.query(&sql, &[&full_table, &column]).await?;
                for r in rows {
                    let sj: String = r.get(11);
                    let ce = ColumnEdgeRow {
                        job_name: r.get(0),
                        batch_id: r.get(1),
                        source_db: r.get(2),
                        source_table: r.get(3),
                        source_full_name: r.get(4),
                        source_column: r.get(5),
                        target_db: r.get(6),
                        target_table: r.get(7),
                        target_full_name: r.get(8),
                        target_column: r.get(9),
                        edge_kind: r.get(10),
                        stmt_indices: serde_json::from_str(&sj).unwrap_or_default(),
                    };
                    up_adj
                        .entry((ce.target_full_name.clone(), ce.target_column.clone()))
                        .or_default()
                        .push((ce.source_full_name.clone(), ce.source_column.clone()));
                    if ce.target_column == "*" {
                        up_adj
                            .entry((ce.target_full_name.clone(), column.to_string()))
                            .or_default()
                            .push((ce.source_full_name.clone(), ce.source_column.clone()));
                    }
                    raw_edges.push(ce);
                }
            }

            if dir.want_down() {
                let sql = format!(
                    "WITH RECURSIVE {cte},
                     active_c AS (
                         SELECT c.job_name, c.batch_id, c.source_db, c.source_table, c.source_full_name,
                                c.source_column, c.target_db, c.target_table, c.target_full_name,
                                c.target_column, c.edge_kind, c.stmt_indices_json
                         FROM hl_column_edges c
                         INNER JOIN latest_batches lb ON c.job_name = lb.job_name AND c.batch_id = lb.batch_id
                     ),
                     walk AS (
                         SELECT * FROM active_c
                         WHERE source_full_name = $1 AND (source_column = $2 OR source_column = '*')
                         UNION
                         SELECT c.* FROM active_c c
                         INNER JOIN walk w
                             ON c.source_full_name = w.target_full_name
                            AND (c.source_column = w.target_column OR c.source_column = '*' OR w.target_column = '*')
                     )
                     SELECT job_name, batch_id, source_db, source_table, source_full_name, source_column,
                            target_db, target_table, target_full_name, target_column, edge_kind, stmt_indices_json
                     FROM walk"
                );
                let rows = client.query(&sql, &[&full_table, &column]).await?;
                for r in rows {
                    let sj: String = r.get(11);
                    let ce = ColumnEdgeRow {
                        job_name: r.get(0),
                        batch_id: r.get(1),
                        source_db: r.get(2),
                        source_table: r.get(3),
                        source_full_name: r.get(4),
                        source_column: r.get(5),
                        target_db: r.get(6),
                        target_table: r.get(7),
                        target_full_name: r.get(8),
                        target_column: r.get(9),
                        edge_kind: r.get(10),
                        stmt_indices: serde_json::from_str(&sj).unwrap_or_default(),
                    };
                    down_adj
                        .entry((ce.source_full_name.clone(), ce.source_column.clone()))
                        .or_default()
                        .push((ce.target_full_name.clone(), ce.target_column.clone()));
                    if ce.source_column == "*" {
                        down_adj
                            .entry((ce.source_full_name.clone(), column.to_string()))
                            .or_default()
                            .push((ce.target_full_name.clone(), ce.target_column.clone()));
                    }
                    raw_edges.push(ce);
                }
            }

            let start = (full_table.to_string(), column.to_string());
            let up_dist = compute_directed_distances(start.clone(), cap, &up_adj, |a, b| a == b);
            let down_dist =
                compute_directed_distances(start.clone(), cap, &down_adj, |a, b| a == b);

            let mut valid_cols: HashSet<(String, String)> = HashSet::default();
            valid_cols.insert(start);
            valid_cols.insert((full_table.to_string(), "*".to_string()));
            valid_cols.extend(up_dist.keys().cloned());
            valid_cols.extend(down_dist.keys().cloned());

            let mut seen_ce: HashSet<(String, String, String, String, String)> = HashSet::default();
            let mut all_tbls: HashSet<String> = HashSet::default();
            all_tbls.insert(full_table.to_string());

            let edges: Vec<ColumnEdgeRow> = raw_edges
                .into_iter()
                .filter(|ce| {
                    let s_ok = valid_cols
                        .contains(&(ce.source_full_name.clone(), ce.source_column.clone()));
                    let t_ok = valid_cols
                        .contains(&(ce.target_full_name.clone(), ce.target_column.clone()));
                    if s_ok
                        && t_ok
                        && seen_ce.insert((
                            ce.source_full_name.clone(),
                            ce.source_column.clone(),
                            ce.target_full_name.clone(),
                            ce.target_column.clone(),
                            ce.job_name.clone(),
                        ))
                    {
                        all_tbls.insert(ce.source_full_name.clone());
                        all_tbls.insert(ce.target_full_name.clone());
                        true
                    } else {
                        false
                    }
                })
                .collect();

            let tbl_vec: Vec<String> = all_tbls.into_iter().collect();
            let jt_sql = format!(
                "WITH {cte}
                 SELECT jt.job_name, jt.batch_id, jt.db_name, jt.table_name, jt.full_table_name,
                        jt.table_kind, jt.is_source, jt.is_target, jt.columns_json
                 FROM hl_job_tables jt
                 INNER JOIN latest_batches lb ON jt.job_name = lb.job_name AND jt.batch_id = lb.batch_id
                 WHERE jt.full_table_name = ANY($1)"
            );
            let jt_rows = client.query(&jt_sql, &[&tbl_vec]).await?;
            let job_tables: Vec<JobTableRow> = jt_rows
                .into_iter()
                .map(|r| {
                    let cj: String = r.get(8);
                    JobTableRow {
                        job_name: r.get(0),
                        batch_id: r.get(1),
                        db_name: r.get(2),
                        table_name: r.get(3),
                        full_table_name: r.get(4),
                        table_kind: r.get(5),
                        is_source: r.get(6),
                        is_target: r.get(7),
                        columns: serde_json::from_str(&cj).unwrap_or_default(),
                    }
                })
                .collect();

            Ok(RawColumnSubgraph {
                edges,
                job_tables,
                up_distances: up_dist.into_iter().collect(),
                down_distances: down_dist.into_iter().collect(),
            })
        })
    }
}

impl PgBackend {
    #[allow(clippy::too_many_arguments)]
    async fn walk_pg_tasks(
        &self,
        up_seeds: &[String],
        down_seeds: &[String],
        anchor_table: Option<&str>,
        dir: LineageDirection,
        max_hops: u32,
        scope: QueryScope,
        seed_base_dist: u32,
    ) -> anyhow::Result<RawTaskSubgraph> {
        let guard = self.with_client().await?;
        let client = guard.as_ref().unwrap();
        let cap = if max_hops == 0 {
            MAX_UNLIMITED_HOPS
        } else {
            max_hops
        };
        let cte = active_scope_cte(scope);

        let mut raw_edges: Vec<(String, String, String)> = Vec::new();
        let mut up_adj: HashMap<String, Vec<String>> = HashMap::default();
        let mut down_adj: HashMap<String, Vec<String>> = HashMap::default();

        if dir.want_up() && !up_seeds.is_empty() {
            let sql = format!(
                "WITH RECURSIVE {cte},
                 active_jt AS (
                     SELECT jt.job_name, jt.full_table_name, jt.is_source, jt.is_target
                     FROM hl_job_tables jt
                     INNER JOIN latest_batches lb ON jt.job_name = lb.job_name AND jt.batch_id = lb.batch_id
                     WHERE jt.table_kind NOT IN ('temp', 'result')
                 ),
                 task_links AS (
                     SELECT DISTINCT w.job_name AS up_job, r.job_name AS dn_job, w.full_table_name AS via_tbl
                     FROM active_jt w
                     INNER JOIN active_jt r
                         ON w.full_table_name = r.full_table_name
                        AND w.is_target = TRUE
                        AND r.is_source = TRUE
                        AND w.job_name <> r.job_name
                 ),
                 walk AS (
                     SELECT up_job, dn_job, via_tbl FROM task_links WHERE dn_job = ANY($1)
                     UNION
                     SELECT tl.up_job, tl.dn_job, tl.via_tbl
                     FROM task_links tl
                     INNER JOIN walk w ON tl.dn_job = w.up_job
                 )
                 SELECT up_job, dn_job, via_tbl FROM walk"
            );
            let rows = client.query(&sql, &[&up_seeds]).await?;
            for r in rows {
                let u: String = r.get(0);
                let d: String = r.get(1);
                let v: String = r.get(2);
                up_adj.entry(d.clone()).or_default().push(u.clone());
                raw_edges.push((u, d, v));
            }
        }

        if dir.want_down() && !down_seeds.is_empty() {
            let sql = format!(
                "WITH RECURSIVE {cte},
                 active_jt AS (
                     SELECT jt.job_name, jt.full_table_name, jt.is_source, jt.is_target
                     FROM hl_job_tables jt
                     INNER JOIN latest_batches lb ON jt.job_name = lb.job_name AND jt.batch_id = lb.batch_id
                     WHERE jt.table_kind NOT IN ('temp', 'result')
                 ),
                 task_links AS (
                     SELECT DISTINCT w.job_name AS up_job, r.job_name AS dn_job, w.full_table_name AS via_tbl
                     FROM active_jt w
                     INNER JOIN active_jt r
                         ON w.full_table_name = r.full_table_name
                        AND w.is_target = TRUE
                        AND r.is_source = TRUE
                        AND w.job_name <> r.job_name
                 ),
                 walk AS (
                     SELECT up_job, dn_job, via_tbl FROM task_links WHERE up_job = ANY($1)
                     UNION
                     SELECT tl.up_job, tl.dn_job, tl.via_tbl
                     FROM task_links tl
                     INNER JOIN walk w ON tl.up_job = w.dn_job
                 )
                 SELECT up_job, dn_job, via_tbl FROM walk"
            );
            let rows = client.query(&sql, &[&down_seeds]).await?;
            for r in rows {
                let u: String = r.get(0);
                let d: String = r.get(1);
                let v: String = r.get(2);
                down_adj.entry(u.clone()).or_default().push(d.clone());
                raw_edges.push((u, d, v));
            }
        }

        // 多起点 BFS 计算精确跳数
        let mut up_dist: HashMap<String, u32> = HashMap::default();
        if dir.want_up() {
            let mut q: VecDeque<(String, u32)> = VecDeque::new();
            for s in up_seeds {
                if seed_base_dist > 0 {
                    up_dist.insert(s.clone(), seed_base_dist);
                }
                q.push_back((s.clone(), seed_base_dist));
            }
            let seed_set: HashSet<&str> = up_seeds.iter().map(String::as_str).collect();
            while let Some((cur, d)) = q.pop_front() {
                if d >= cap {
                    continue;
                }
                if let Some(nxts) = up_adj.get(&cur) {
                    for u in nxts {
                        if (seed_base_dist == 0 && seed_set.contains(u.as_str()))
                            || up_dist.contains_key(u)
                        {
                            continue;
                        }
                        up_dist.insert(u.clone(), d + 1);
                        q.push_back((u.clone(), d + 1));
                    }
                }
            }
        }

        let mut down_dist: HashMap<String, u32> = HashMap::default();
        if dir.want_down() {
            let mut q: VecDeque<(String, u32)> = VecDeque::new();
            for s in down_seeds {
                if seed_base_dist > 0 {
                    down_dist.insert(s.clone(), seed_base_dist);
                }
                q.push_back((s.clone(), seed_base_dist));
            }
            let seed_set: HashSet<&str> = down_seeds.iter().map(String::as_str).collect();
            while let Some((cur, d)) = q.pop_front() {
                if d >= cap {
                    continue;
                }
                if let Some(nxts) = down_adj.get(&cur) {
                    for dn in nxts {
                        if (seed_base_dist == 0 && seed_set.contains(dn.as_str()))
                            || down_dist.contains_key(dn)
                        {
                            continue;
                        }
                        down_dist.insert(dn.clone(), d + 1);
                        q.push_back((dn.clone(), d + 1));
                    }
                }
            }
        }

        let mut valid_jobs: HashSet<String> = HashSet::default();
        valid_jobs.extend(up_seeds.iter().cloned());
        valid_jobs.extend(down_seeds.iter().cloned());
        valid_jobs.extend(up_dist.keys().cloned());
        valid_jobs.extend(down_dist.keys().cloned());

        let mut seen_te: HashSet<(String, String, String)> = HashSet::default();
        let mut task_edges: Vec<(String, String, String)> = raw_edges
            .into_iter()
            .filter(|(u, d, v)| {
                valid_jobs.contains(u)
                    && valid_jobs.contains(d)
                    && seen_te.insert((u.clone(), d.clone(), v.clone()))
            })
            .collect();

        if let Some(tbl) = anchor_table {
            for pj in up_seeds {
                for cj in down_seeds {
                    if pj != cj && seen_te.insert((pj.clone(), cj.clone(), tbl.to_string())) {
                        task_edges.push((pj.clone(), cj.clone(), tbl.to_string()));
                    }
                }
            }
        }

        let job_vec: Vec<String> = valid_jobs.into_iter().collect();
        let jobs = if job_vec.is_empty() {
            Vec::new()
        } else {
            let j_sql = format!(
                "WITH {cte},
                 mj AS (
                     SELECT DISTINCT ON (b.job_name)
                            b.job_name, b.dag_name, b.task_name, b.batch_id,
                            (EXTRACT(EPOCH FROM b.ingested_at) * 1000)::bigint AS ingested_ms
                     FROM hl_job_batches b
                     INNER JOIN latest_batches lb ON b.job_name = lb.job_name AND b.batch_id = lb.batch_id
                     WHERE b.job_name = ANY($1)
                     ORDER BY b.job_name, b.batch_id DESC
                 )
                 SELECT mj.job_name, mj.dag_name, mj.task_name, mj.batch_id, mj.ingested_ms,
                        COALESCE(ARRAY_AGG(DISTINCT jt.full_table_name)
                            FILTER (WHERE jt.is_source AND jt.table_kind NOT IN ('temp','result')), ARRAY[]::varchar[]),
                        COALESCE(ARRAY_AGG(DISTINCT jt.full_table_name)
                            FILTER (WHERE jt.is_target AND jt.table_kind NOT IN ('temp','result')), ARRAY[]::varchar[])
                 FROM mj
                 LEFT JOIN latest_batches lb2 ON lb2.job_name = mj.job_name
                 LEFT JOIN hl_job_tables jt ON jt.job_name = lb2.job_name AND jt.batch_id = lb2.batch_id
                 GROUP BY mj.job_name, mj.dag_name, mj.task_name, mj.batch_id, mj.ingested_ms
                 ORDER BY mj.job_name ASC"
            );
            let rows = client.query(&j_sql, &[&job_vec]).await?;
            rows.into_iter()
                .map(|r| JobSearchItem {
                    job_name: r.get(0),
                    dag_name: r.get(1),
                    task_name: r.get(2),
                    latest_batch_id: r.get(3),
                    ingested_at_ms: r.get(4),
                    source_tables: r.get(5),
                    target_tables: r.get(6),
                })
                .collect()
        };

        Ok(RawTaskSubgraph {
            task_edges,
            direct_producers: if anchor_table.is_some() {
                up_seeds.to_vec()
            } else {
                Vec::new()
            },
            direct_consumers: if anchor_table.is_some() {
                down_seeds.to_vec()
            } else {
                Vec::new()
            },
            jobs,
            up_distances: up_dist.into_iter().collect(),
            down_distances: down_dist.into_iter().collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_cte_differs_between_latest_and_history() {
        assert!(active_scope_cte(QueryScope::LatestBatch).contains("DISTINCT ON (job_name)"));
        assert!(!active_scope_cte(QueryScope::AllHistory).contains("DISTINCT ON"));
    }

    #[test]
    fn bfs_distances_respect_cycles_and_diamonds() {
        let mut adj: HashMap<String, Vec<String>> = HashMap::default();
        adj.insert("d".into(), vec!["b".into(), "c".into()]);
        adj.insert("b".into(), vec!["a".into()]);
        adj.insert("c".into(), vec!["a".into(), "d".into()]);
        adj.insert("a".into(), vec!["z".into()]);
        let d = compute_directed_distances("d".to_string(), 2, &adj, |x, y| x == y);
        assert_eq!(d.get("b"), Some(&1));
        assert_eq!(d.get("c"), Some(&1));
        assert_eq!(d.get("a"), Some(&2));
        assert!(!d.contains_key("z"));
    }
}
