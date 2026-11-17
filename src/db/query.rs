//! 全局血缘查询门面（`LineageService`）：协调内存热图索引与存储后端，输出四类血缘结构与 WebUI 图视图

use crate::db::extract::{
    extract_batch_payload, format_qualified_column, parse_qualified_column, split_db_table,
    split_job_name,
};
use crate::db::hot_index::HotGraphIndex;
use crate::db::memory::MemoryBackend;
use crate::db::postgres::PgBackend;
use crate::db::schema::mask_db_url;
use crate::db::{
    DbConfig, JobSearchItem, LineageBackend, LineageDirection, QueryScope, RawColumnSubgraph,
    RawTableSubgraph, RawTaskSubgraph, SaveSummary, TableSearchItem,
};
use crate::{AnalyzedWorkspace, SqlDialect};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

const SYNC_INTERVAL_MS: i64 = 500;
const MAX_CHAINS: usize = 128;
const MAX_CHAIN_LEN: usize = 64;

#[derive(Debug, Clone, Serialize)]
pub struct TableNodeOut {
    pub full_table_name: String,
    pub db_name: String,
    pub table_name: String,
    pub table_kind: String,
    pub distance: u32,
    pub producer_jobs: Vec<String>,
    pub consumer_jobs: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableEdgeOut {
    pub source: String,
    pub target: String,
    pub jobs: Vec<String>,
    pub sql_preview: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableLineageResponse {
    pub query_table: String,
    pub scope: &'static str,
    pub direction: &'static str,
    pub hops: u32,
    pub upstream_tables: Vec<TableNodeOut>,
    pub downstream_tables: Vec<TableNodeOut>,
    pub edges: Vec<TableEdgeOut>,
    pub graph: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskNodeOut {
    pub job_name: String,
    pub dag_name: String,
    pub task_name: String,
    pub distance: u32,
    pub latest_batch_id: i64,
    pub ingested_at_ms: i64,
    pub source_tables: Vec<String>,
    pub target_tables: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskEdgeOut {
    pub source_job: String,
    pub target_job: String,
    pub via_tables: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableTaskLineageResponse {
    pub query_table: String,
    pub scope: &'static str,
    pub direction: &'static str,
    pub hops: u32,
    pub producer_jobs: Vec<String>,
    pub consumer_jobs: Vec<String>,
    pub upstream_jobs: Vec<TaskNodeOut>,
    pub downstream_jobs: Vec<TaskNodeOut>,
    pub task_edges: Vec<TaskEdgeOut>,
    pub graph: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskLineageResponse {
    pub query_job: String,
    pub matched_job: Option<String>,
    pub scope: &'static str,
    pub direction: &'static str,
    pub hops: u32,
    pub job_detail: Option<JobSearchItem>,
    pub candidates: Vec<JobSearchItem>,
    pub upstream_jobs: Vec<TaskNodeOut>,
    pub downstream_jobs: Vec<TaskNodeOut>,
    pub task_edges: Vec<TaskEdgeOut>,
    pub chains: Vec<Vec<String>>,
    pub graph: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnNodeOut {
    /// `库名.表名.字段名`
    pub qualified_column: String,
    pub db_name: String,
    pub table_name: String,
    pub full_table_name: String,
    pub column_name: String,
    pub distance: u32,
    pub edge_kinds: Vec<String>,
    pub jobs: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnEdgeOut {
    pub source: String,
    pub target: String,
    pub source_table: String,
    pub source_column: String,
    pub target_table: String,
    pub target_column: String,
    pub kind: String,
    pub jobs: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnTableOut {
    pub full_table_name: String,
    pub db_name: String,
    pub table_name: String,
    pub distance: u32,
    pub columns: Vec<String>,
    pub jobs: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnJobOut {
    pub job_name: String,
    pub dag_name: String,
    pub task_name: String,
    pub distance: u32,
    pub via_tables: Vec<String>,
    pub via_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnLineageResponse {
    /// `库名.表名.字段名`
    pub query_column: String,
    pub db_name: String,
    pub table_name: String,
    pub full_table_name: String,
    pub column_name: String,
    pub scope: &'static str,
    pub direction: &'static str,
    pub hops: u32,
    pub upstream_columns: Vec<ColumnNodeOut>,
    pub downstream_columns: Vec<ColumnNodeOut>,
    pub upstream_tables: Vec<ColumnTableOut>,
    pub downstream_tables: Vec<ColumnTableOut>,
    pub upstream_jobs: Vec<ColumnJobOut>,
    pub downstream_jobs: Vec<ColumnJobOut>,
    pub edges: Vec<ColumnEdgeOut>,
    /// 完整字段级血缘路径（每项为 `["ods.a.x", "dwd.b.x", "dws.c.y"]`）
    pub chains: Vec<Vec<String>>,
    pub graph: Value,
}

pub struct LineageService {
    cfg: DbConfig,
    backend: Arc<dyn LineageBackend>,
    hot_index: Option<RwLock<HotGraphIndex>>,
    last_sync_ms: AtomicI64,
    sync_mutex: tokio::sync::Mutex<()>,
}

impl LineageService {
    pub async fn connect(cfg: DbConfig) -> anyhow::Result<Self> {
        let trimmed = cfg.url.trim();
        if trimmed.is_empty() {
            anyhow::bail!("数据库连接地址不能为空");
        }
        let backend: Arc<dyn LineageBackend> =
            if trimmed == "memory://" || trimmed.starts_with("memory://") {
                Arc::new(MemoryBackend::new())
            } else {
                Arc::new(PgBackend::connect(trimmed).await?)
            };
        Self::from_backend(cfg, backend).await
    }

    pub async fn from_backend(
        cfg: DbConfig,
        backend: Arc<dyn LineageBackend>,
    ) -> anyhow::Result<Self> {
        if cfg.auto_create_table {
            backend.ensure_schema().await?;
        } else {
            backend.ping().await?;
        }
        let hot_index = if cfg.use_hot_index {
            let initial = backend.load_latest_snapshot().await?;
            Some(RwLock::new(HotGraphIndex::from_batches(&initial, false)))
        } else {
            None
        };
        Ok(Self {
            cfg,
            backend,
            hot_index,
            last_sync_ms: AtomicI64::new(now_ms()),
            sync_mutex: tokio::sync::Mutex::new(()),
        })
    }

    pub fn masked_url(&self) -> String {
        mask_db_url(&self.cfg.url)
    }

    pub fn backend_name(&self) -> &'static str {
        self.backend.backend_name()
    }

    pub fn hot_index_enabled(&self) -> bool {
        self.hot_index.is_some()
    }

    pub async fn ping(&self) -> anyhow::Result<()> {
        self.backend.ping().await
    }

    async fn sync_hot_index_if_needed(&self) -> anyhow::Result<()> {
        let Some(lock) = &self.hot_index else {
            return Ok(());
        };
        let cur = now_ms();
        if cur - self.last_sync_ms.load(Ordering::Relaxed) < SYNC_INTERVAL_MS {
            return Ok(());
        }
        let _sync_guard = self.sync_mutex.lock().await;
        if now_ms() - self.last_sync_ms.load(Ordering::Relaxed) < SYNC_INTERVAL_MS {
            return Ok(());
        }
        let watermark = lock.read().await.watermark_batch_id();
        let delta = self.backend.fetch_batches_since(watermark).await?;
        if !delta.is_empty() {
            lock.write().await.apply_batches(&delta);
        }
        self.last_sync_ms.store(now_ms(), Ordering::Relaxed);
        Ok(())
    }

    /// 将单次 SQL 解析结果追加落库，并实时同步更新内存热图索引
    pub async fn save_workspace(
        &self,
        job_name: &str,
        dialect: SqlDialect,
        default_db: Option<&str>,
        ws: &AnalyzedWorkspace,
    ) -> anyhow::Result<SaveSummary> {
        let payload = extract_batch_payload(job_name, dialect, default_db, ws)?;
        self.backend.append_batch(&payload).await?;
        if let Some(lock) = &self.hot_index {
            lock.write()
                .await
                .apply_batches(std::slice::from_ref(&payload));
            self.last_sync_ms.store(now_ms(), Ordering::Relaxed);
        }
        Ok(SaveSummary {
            job_name: payload.batch.job_name,
            dag_name: payload.batch.dag_name,
            task_name: payload.batch.task_name,
            batch_id: payload.batch.batch_id,
            tables_written: payload.job_tables.len(),
            table_edges_written: payload.table_edges.len(),
            column_edges_written: payload.column_edges.len(),
        })
    }

    pub async fn search_jobs(
        &self,
        keyword: &str,
        limit: usize,
        scope: QueryScope,
    ) -> anyhow::Result<Vec<JobSearchItem>> {
        if scope == QueryScope::LatestBatch {
            if let Some(lock) = &self.hot_index {
                self.sync_hot_index_if_needed().await?;
                return Ok(lock.read().await.search_jobs(keyword, limit));
            }
        }
        self.backend.search_jobs(keyword, limit, scope).await
    }

    pub async fn search_tables(
        &self,
        keyword: &str,
        limit: usize,
        scope: QueryScope,
    ) -> anyhow::Result<Vec<TableSearchItem>> {
        if scope == QueryScope::LatestBatch {
            if let Some(lock) = &self.hot_index {
                self.sync_hot_index_if_needed().await?;
                return Ok(lock.read().await.search_tables(keyword, limit));
            }
        }
        self.backend.search_tables(keyword, limit, scope).await
    }

    /// 查询 1：输入 `库名.表名` 查询表级别的上下游加工血缘
    pub async fn query_table_lineage(
        &self,
        raw_table: &str,
        dir: LineageDirection,
        hops: u32,
        scope: QueryScope,
    ) -> anyhow::Result<TableLineageResponse> {
        let (_, _, full_table) = split_db_table(raw_table);
        if full_table.is_empty() {
            anyhow::bail!("请输入有效的 库名.表名");
        }
        let sub = if scope == QueryScope::LatestBatch {
            if let Some(lock) = &self.hot_index {
                self.sync_hot_index_if_needed().await?;
                lock.read()
                    .await
                    .query_table_subgraph(&full_table, dir, hops)
            } else {
                self.backend
                    .query_table_subgraph(&full_table, dir, hops, scope)
                    .await?
            }
        } else {
            self.backend
                .query_table_subgraph(&full_table, dir, hops, scope)
                .await?
        };
        Ok(build_table_lineage_response(
            &full_table,
            dir,
            hops,
            scope,
            sub,
        ))
    }

    /// 查询 2：输入 `库名.表名` 查询任务级别的上下游血缘
    pub async fn query_table_task_lineage(
        &self,
        raw_table: &str,
        dir: LineageDirection,
        hops: u32,
        scope: QueryScope,
    ) -> anyhow::Result<TableTaskLineageResponse> {
        let (_, _, full_table) = split_db_table(raw_table);
        if full_table.is_empty() {
            anyhow::bail!("请输入有效的 库名.表名");
        }
        let sub = if scope == QueryScope::LatestBatch {
            if let Some(lock) = &self.hot_index {
                self.sync_hot_index_if_needed().await?;
                lock.read()
                    .await
                    .query_table_task_subgraph(&full_table, dir, hops)
            } else {
                self.backend
                    .query_table_task_subgraph(&full_table, dir, hops, scope)
                    .await?
            }
        } else {
            self.backend
                .query_table_task_subgraph(&full_table, dir, hops, scope)
                .await?
        };
        Ok(build_table_task_response(
            &full_table,
            dir,
            hops,
            scope,
            sub,
        ))
    }

    /// 查询 3：按照任务名搜索上、下游任务链路
    pub async fn query_task_lineage(
        &self,
        job_query: &str,
        dir: LineageDirection,
        hops: u32,
        scope: QueryScope,
    ) -> anyhow::Result<TaskLineageResponse> {
        let trimmed = job_query.trim();
        if trimmed.is_empty() {
            anyhow::bail!("请输入要查询的任务名（如 dag.task）");
        }
        let candidates = self.search_jobs(trimmed, 30, scope).await?;
        let exact = candidates
            .iter()
            .find(|c| c.job_name.eq_ignore_ascii_case(trimmed))
            .cloned()
            .or_else(|| candidates.first().cloned());

        let Some(chosen) = exact else {
            return Ok(TaskLineageResponse {
                query_job: trimmed.to_string(),
                matched_job: None,
                scope: scope.as_str(),
                direction: dir.as_str(),
                hops,
                job_detail: None,
                candidates: Vec::new(),
                upstream_jobs: Vec::new(),
                downstream_jobs: Vec::new(),
                task_edges: Vec::new(),
                chains: Vec::new(),
                graph: json!({"mode": "task", "tables": [], "edges": [], "col_edges": []}),
            });
        };

        let target_name = chosen.job_name.clone();
        let sub = if scope == QueryScope::LatestBatch {
            if let Some(lock) = &self.hot_index {
                lock.read()
                    .await
                    .query_task_subgraph(&target_name, dir, hops)
            } else {
                self.backend
                    .query_task_subgraph(&target_name, dir, hops, scope)
                    .await?
            }
        } else {
            self.backend
                .query_task_subgraph(&target_name, dir, hops, scope)
                .await?
        };

        Ok(build_task_lineage_response(
            trimmed, chosen, candidates, dir, hops, scope, sub,
        ))
    }

    /// 查询 4：按照 `库名`、`表名`、`字段名` 输出上下游 `库名.表名.字段名` 的字段级别血缘链路
    #[allow(clippy::too_many_arguments)]
    pub async fn query_column_lineage(
        &self,
        db: Option<&str>,
        table: Option<&str>,
        column: Option<&str>,
        combined_field: Option<&str>,
        dir: LineageDirection,
        hops: u32,
        scope: QueryScope,
    ) -> anyhow::Result<ColumnLineageResponse> {
        let (db_name, table_name, full_table, col_name) =
            parse_qualified_column(db, table, column, combined_field)?;
        let sub = if scope == QueryScope::LatestBatch {
            if let Some(lock) = &self.hot_index {
                self.sync_hot_index_if_needed().await?;
                lock.read()
                    .await
                    .query_column_subgraph(&full_table, &col_name, dir, hops)
            } else {
                self.backend
                    .query_column_subgraph(&full_table, &col_name, dir, hops, scope)
                    .await?
            }
        } else {
            self.backend
                .query_column_subgraph(&full_table, &col_name, dir, hops, scope)
                .await?
        };

        Ok(build_column_lineage_response(
            &db_name,
            &table_name,
            &full_table,
            &col_name,
            dir,
            hops,
            scope,
            sub,
        ))
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Kahn 拓扑分层（支持成环图优雅降级，确保 DAG 从左向右有序排布）
fn compute_dag_levels(n: usize, edges: &[(usize, usize)]) -> Vec<u32> {
    if n == 0 {
        return Vec::new();
    }
    let mut up: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut down: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut indeg: Vec<u32> = vec![0; n];
    let mut seen: HashSet<(usize, usize)> = HashSet::default();
    for &(s, t) in edges {
        if s < n && t < n && s != t && seen.insert((s, t)) {
            down[s].push(t);
            up[t].push(s);
            indeg[t] += 1;
        }
    }
    let mut levels = vec![u32::MAX; n];
    let mut q: VecDeque<usize> = VecDeque::new();
    for (i, &d) in indeg.iter().enumerate() {
        if d == 0 {
            levels[i] = 0;
            q.push_back(i);
        }
    }
    let mut processed = vec![false; n];
    while let Some(u) = q.pop_front() {
        processed[u] = true;
        for &v in &down[u] {
            let next_lvl = levels[u] + 1;
            if levels[v] == u32::MAX || levels[v] < next_lvl {
                levels[v] = next_lvl;
            }
            indeg[v] = indeg[v].saturating_sub(1);
            if indeg[v] == 0 {
                q.push_back(v);
            }
        }
    }
    for i in 0..n {
        if !processed[i] {
            let mut best = 0u32;
            for &p in &up[i] {
                if processed[p] && levels[p] != u32::MAX {
                    best = best.max(levels[p] + 1);
                }
            }
            levels[i] = best;
        }
    }
    levels
}

/// 从有向边集提取从源头到末端的完整链路列表（环路安全、数量有界）
fn extract_directed_chains(nodes: &[String], edges: &[(String, String)]) -> Vec<Vec<String>> {
    if edges.is_empty() {
        return Vec::new();
    }
    let mut out_adj: HashMap<&str, Vec<&str>> = HashMap::default();
    let mut in_deg: HashMap<&str, usize> = HashMap::default();
    for n in nodes {
        in_deg.entry(n.as_str()).or_insert(0);
    }
    for (s, t) in edges {
        if s == t {
            continue;
        }
        let v = out_adj.entry(s.as_str()).or_default();
        if !v.contains(&t.as_str()) {
            v.push(t.as_str());
            *in_deg.entry(t.as_str()).or_insert(0) += 1;
            in_deg.entry(s.as_str()).or_insert(0);
        }
    }
    for v in out_adj.values_mut() {
        v.sort_unstable();
    }

    let mut roots: Vec<&str> = in_deg
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(&k, _)| k)
        .collect();
    if roots.is_empty() {
        roots = nodes.iter().map(String::as_str).collect();
    }
    roots.sort_unstable();

    let mut chains: Vec<Vec<String>> = Vec::new();
    let mut path: Vec<String> = Vec::new();
    let mut on_path: HashSet<String> = HashSet::default();
    let mut ever_visited: HashSet<String> = HashSet::default();

    #[allow(clippy::too_many_arguments)]
    fn dfs(
        cur: &str,
        out_adj: &HashMap<&str, Vec<&str>>,
        path: &mut Vec<String>,
        on_path: &mut HashSet<String>,
        ever_visited: &mut HashSet<String>,
        chains: &mut Vec<Vec<String>>,
    ) {
        if chains.len() >= MAX_CHAINS {
            return;
        }
        path.push(cur.to_string());
        on_path.insert(cur.to_string());
        ever_visited.insert(cur.to_string());

        let mut extended = false;
        if path.len() < MAX_CHAIN_LEN {
            if let Some(nxts) = out_adj.get(cur) {
                for &nxt in nxts {
                    if !on_path.contains(nxt) {
                        extended = true;
                        dfs(nxt, out_adj, path, on_path, ever_visited, chains);
                        if chains.len() >= MAX_CHAINS {
                            break;
                        }
                    }
                }
            }
        }
        if !extended && path.len() >= 2 {
            chains.push(path.clone());
        }

        on_path.remove(cur);
        path.pop();
    }

    for r in roots {
        dfs(
            r,
            &out_adj,
            &mut path,
            &mut on_path,
            &mut ever_visited,
            &mut chains,
        );
        if chains.len() >= MAX_CHAINS {
            break;
        }
    }
    if chains.len() < MAX_CHAINS {
        let mut sorted_nodes: Vec<&str> = nodes.iter().map(String::as_str).collect();
        sorted_nodes.sort_unstable();
        for n in sorted_nodes {
            if !ever_visited.contains(n) && out_adj.contains_key(n) {
                dfs(
                    n,
                    &out_adj,
                    &mut path,
                    &mut on_path,
                    &mut ever_visited,
                    &mut chains,
                );
                if chains.len() >= MAX_CHAINS {
                    break;
                }
            }
        }
    }
    chains
}

fn build_table_lineage_response(
    focus_table: &str,
    dir: LineageDirection,
    hops: u32,
    scope: QueryScope,
    sub: RawTableSubgraph,
) -> TableLineageResponse {
    // 聚合各物理表元数据与产出/消费任务
    #[derive(Default)]
    struct TMeta {
        kind: String,
        producers: Vec<String>,
        consumers: Vec<String>,
        columns: Vec<String>,
    }

    let mut meta_map: HashMap<String, TMeta> = HashMap::default();
    for jt in &sub.job_tables {
        let entry = meta_map.entry(jt.full_table_name.clone()).or_default();
        if entry.kind.is_empty() || entry.kind == "table" {
            entry.kind = jt.table_kind.clone();
        }
        if jt.is_target && !entry.producers.contains(&jt.job_name) {
            entry.producers.push(jt.job_name.clone());
        }
        if jt.is_source && !entry.consumers.contains(&jt.job_name) {
            entry.consumers.push(jt.job_name.clone());
        }
        for c in &jt.columns {
            if !entry.columns.contains(c) {
                entry.columns.push(c.clone());
            }
        }
    }

    // 聚合边 (src, dst) -> (jobs, preview)
    let mut edge_map: HashMap<(String, String), (Vec<String>, String)> = HashMap::default();
    let mut all_names: HashSet<String> = HashSet::default();
    if !meta_map.is_empty() || !sub.edges.is_empty() {
        all_names.insert(focus_table.to_string());
    }
    for k in meta_map.keys() {
        all_names.insert(k.clone());
    }
    for (s, d, j, p) in sub.edges {
        all_names.insert(s.clone());
        all_names.insert(d.clone());
        let entry = edge_map.entry((s, d)).or_default();
        if !entry.0.contains(&j) {
            entry.0.push(j);
        }
        if entry.1.is_empty() && !p.is_empty() {
            entry.1 = p;
        }
    }

    let mut up_nodes: Vec<TableNodeOut> = sub
        .up_distances
        .iter()
        .map(|(name, dist)| {
            let (db, tbl, _) = split_db_table(name);
            let m = meta_map.get(name);
            let mut pj = m.map(|x| x.producers.clone()).unwrap_or_default();
            let mut cj = m.map(|x| x.consumers.clone()).unwrap_or_default();
            pj.sort();
            cj.sort();
            TableNodeOut {
                full_table_name: name.clone(),
                db_name: db,
                table_name: tbl,
                table_kind: m
                    .map(|x| x.kind.clone())
                    .filter(|k| !k.is_empty())
                    .unwrap_or_else(|| "table".into()),
                distance: *dist,
                producer_jobs: pj,
                consumer_jobs: cj,
            }
        })
        .collect();
    up_nodes.sort_by(|a, b| {
        a.distance
            .cmp(&b.distance)
            .then_with(|| a.full_table_name.cmp(&b.full_table_name))
    });

    let mut down_nodes: Vec<TableNodeOut> = sub
        .down_distances
        .iter()
        .map(|(name, dist)| {
            let (db, tbl, _) = split_db_table(name);
            let m = meta_map.get(name);
            let mut pj = m.map(|x| x.producers.clone()).unwrap_or_default();
            let mut cj = m.map(|x| x.consumers.clone()).unwrap_or_default();
            pj.sort();
            cj.sort();
            TableNodeOut {
                full_table_name: name.clone(),
                db_name: db,
                table_name: tbl,
                table_kind: m
                    .map(|x| x.kind.clone())
                    .filter(|k| !k.is_empty())
                    .unwrap_or_else(|| "table".into()),
                distance: *dist,
                producer_jobs: pj,
                consumer_jobs: cj,
            }
        })
        .collect();
    down_nodes.sort_by(|a, b| {
        a.distance
            .cmp(&b.distance)
            .then_with(|| a.full_table_name.cmp(&b.full_table_name))
    });

    let mut edges_out: Vec<TableEdgeOut> = edge_map
        .into_iter()
        .map(|((source, target), (mut jobs, sql_preview))| {
            jobs.sort();
            TableEdgeOut {
                source,
                target,
                jobs,
                sql_preview,
            }
        })
        .collect();
    edges_out.sort_by(|a, b| {
        a.source
            .cmp(&b.source)
            .then_with(|| a.target.cmp(&b.target))
    });

    // 构建 WebUI 图结构
    let mut sorted_names: Vec<String> = all_names.into_iter().collect();
    sorted_names.sort();
    let name_to_idx: HashMap<&str, usize> = sorted_names
        .iter()
        .enumerate()
        .map(|(i, s)| (s.as_str(), i))
        .collect();
    let idx_edges: Vec<(usize, usize)> = edges_out
        .iter()
        .filter_map(|e| {
            Some((
                *name_to_idx.get(e.source.as_str())?,
                *name_to_idx.get(e.target.as_str())?,
            ))
        })
        .collect();
    let levels = compute_dag_levels(sorted_names.len(), &idx_edges);
    let mut in_deg = vec![0usize; sorted_names.len()];
    let mut out_deg = vec![0usize; sorted_names.len()];
    for &(s, t) in &idx_edges {
        out_deg[s] += 1;
        in_deg[t] += 1;
    }

    let focus_id = name_to_idx.get(focus_table).copied();
    let tables_json: Vec<Value> = sorted_names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let m = meta_map.get(name);
            let cols = m.map(|x| x.columns.clone()).unwrap_or_default();
            let mut pj = m.map(|x| x.producers.clone()).unwrap_or_default();
            let mut cj = m.map(|x| x.consumers.clone()).unwrap_or_default();
            pj.sort();
            cj.sort();
            let kind = m
                .map(|x| x.kind.as_str())
                .filter(|k| !k.is_empty())
                .unwrap_or("table");
            json!({
                "id": i,
                "name": name,
                "kind": kind,
                "is_source": out_deg[i] > 0 || !cj.is_empty(),
                "is_target": in_deg[i] > 0 || !pj.is_empty(),
                "is_focus": Some(i) == focus_id,
                "ncols": cols.len(),
                "in_degree": in_deg[i],
                "out_degree": out_deg[i],
                "level": levels[i],
                "columns": cols,
                "producer_jobs": pj,
                "consumer_jobs": cj,
                "cols": {"lineage": [], "other": cols, "other_total": cols.len()},
            })
        })
        .collect();

    let graph_edges: Vec<Value> = edges_out
        .iter()
        .filter_map(|e| {
            let s = *name_to_idx.get(e.source.as_str())?;
            let t = *name_to_idx.get(e.target.as_str())?;
            let label = if e.jobs.len() <= 2 {
                e.jobs.join(", ")
            } else {
                format!("{} 等{}任务", e.jobs[0], e.jobs.len())
            };
            Some(json!({
                "s": s,
                "t": t,
                "label": label,
                "jobs": e.jobs,
                "sql_preview": e.sql_preview,
                "stmts": [],
                "n_stmts": e.jobs.len(),
            }))
        })
        .collect();

    TableLineageResponse {
        query_table: focus_table.to_string(),
        scope: scope.as_str(),
        direction: dir.as_str(),
        hops,
        upstream_tables: up_nodes,
        downstream_tables: down_nodes,
        edges: edges_out,
        graph: json!({
            "mode": "table",
            "focus_id": focus_id,
            "focus_label": focus_table,
            "tables": tables_json,
            "edges": graph_edges,
            "col_edges": [],
        }),
    }
}

fn merge_task_edges(raw: &[(String, String, String)]) -> Vec<TaskEdgeOut> {
    let mut map: HashMap<(String, String), Vec<String>> = HashMap::default();
    for (u, d, via) in raw {
        let v = map.entry((u.clone(), d.clone())).or_default();
        if !v.contains(via) {
            v.push(via.clone());
        }
    }
    let mut out: Vec<TaskEdgeOut> = map
        .into_iter()
        .map(|((source_job, target_job), mut via_tables)| {
            via_tables.sort();
            TaskEdgeOut {
                source_job,
                target_job,
                via_tables,
            }
        })
        .collect();
    out.sort_by(|a, b| {
        a.source_job
            .cmp(&b.source_job)
            .then_with(|| a.target_job.cmp(&b.target_job))
    });
    out
}

fn to_task_node_out(j: &JobSearchItem, dist: u32) -> TaskNodeOut {
    TaskNodeOut {
        job_name: j.job_name.clone(),
        dag_name: j.dag_name.clone(),
        task_name: j.task_name.clone(),
        distance: dist,
        latest_batch_id: j.latest_batch_id,
        ingested_at_ms: j.ingested_at_ms,
        source_tables: j.source_tables.clone(),
        target_tables: j.target_tables.clone(),
    }
}

fn build_table_task_response(
    focus_table: &str,
    dir: LineageDirection,
    hops: u32,
    scope: QueryScope,
    sub: RawTaskSubgraph,
) -> TableTaskLineageResponse {
    let job_map: HashMap<&str, &JobSearchItem> =
        sub.jobs.iter().map(|j| (j.job_name.as_str(), j)).collect();

    let mut upstream_jobs: Vec<TaskNodeOut> = sub
        .up_distances
        .iter()
        .filter_map(|(jn, d)| job_map.get(jn.as_str()).map(|j| to_task_node_out(j, *d)))
        .collect();
    upstream_jobs.sort_by(|a, b| {
        a.distance
            .cmp(&b.distance)
            .then_with(|| a.job_name.cmp(&b.job_name))
    });

    let mut downstream_jobs: Vec<TaskNodeOut> = sub
        .down_distances
        .iter()
        .filter_map(|(jn, d)| job_map.get(jn.as_str()).map(|j| to_task_node_out(j, *d)))
        .collect();
    downstream_jobs.sort_by(|a, b| {
        a.distance
            .cmp(&b.distance)
            .then_with(|| a.job_name.cmp(&b.job_name))
    });

    let task_edges = merge_task_edges(&sub.task_edges);

    // 构建以目标表为中心锚点、两侧串联任务节点的血缘图：
    // upstream_job --[via]--> producer_job --> [focus_table] --> consumer_job --[via]--> downstream_job
    let has_any = !sub.jobs.is_empty();
    if !has_any {
        return TableTaskLineageResponse {
            query_table: focus_table.to_string(),
            scope: scope.as_str(),
            direction: dir.as_str(),
            hops,
            producer_jobs: sub.direct_producers,
            consumer_jobs: sub.direct_consumers,
            upstream_jobs,
            downstream_jobs,
            task_edges,
            graph: json!({"mode": "task", "tables": [], "edges": [], "col_edges": []}),
        };
    }

    // 节点 0 为锚点物理表，1..=M 为任务节点
    let mut job_names: Vec<String> = sub.jobs.iter().map(|j| j.job_name.clone()).collect();
    job_names.sort();
    let job_to_id: HashMap<&str, usize> = job_names
        .iter()
        .enumerate()
        .map(|(i, n)| (n.as_str(), i + 1))
        .collect();

    let p_set: HashSet<&str> = sub.direct_producers.iter().map(String::as_str).collect();
    let c_set: HashSet<&str> = sub.direct_consumers.iter().map(String::as_str).collect();

    let mut graph_edge_tuples: Vec<(usize, usize, String, Vec<String>)> = Vec::new();
    for pj in &sub.direct_producers {
        if let Some(&pid) = job_to_id.get(pj.as_str()) {
            graph_edge_tuples.push((pid, 0, "产出写入".into(), vec![focus_table.to_string()]));
        }
    }
    for cj in &sub.direct_consumers {
        if let Some(&cid) = job_to_id.get(cj.as_str()) {
            graph_edge_tuples.push((0, cid, "读取消费".into(), vec![focus_table.to_string()]));
        }
    }
    for te in &task_edges {
        // 若是 producer -> consumer 且仅通过当前锚点表相连，已由锚点节点串联，无需再画跨锚点旁路边
        if p_set.contains(te.source_job.as_str())
            && c_set.contains(te.target_job.as_str())
            && te.via_tables.len() == 1
            && te.via_tables[0] == focus_table
        {
            continue;
        }
        if let (Some(&sid), Some(&tid)) = (
            job_to_id.get(te.source_job.as_str()),
            job_to_id.get(te.target_job.as_str()),
        ) {
            let lbl = if te.via_tables.len() <= 2 {
                te.via_tables.join(", ")
            } else {
                format!("{} 等{}表", te.via_tables[0], te.via_tables.len())
            };
            graph_edge_tuples.push((sid, tid, lbl, te.via_tables.clone()));
        }
    }

    let total_nodes = 1 + job_names.len();
    let raw_pairs: Vec<(usize, usize)> = graph_edge_tuples
        .iter()
        .map(|&(s, t, _, _)| (s, t))
        .collect();
    let levels = compute_dag_levels(total_nodes, &raw_pairs);
    let mut in_deg = vec![0usize; total_nodes];
    let mut out_deg = vec![0usize; total_nodes];
    for &(s, t) in &raw_pairs {
        out_deg[s] += 1;
        in_deg[t] += 1;
    }

    let mut nodes_json = Vec::with_capacity(total_nodes);
    nodes_json.push(json!({
        "id": 0,
        "name": focus_table,
        "kind": "anchor_table",
        "is_source": out_deg[0] > 0,
        "is_target": in_deg[0] > 0,
        "is_focus": true,
        "ncols": 0,
        "in_degree": in_deg[0],
        "out_degree": out_deg[0],
        "level": levels[0],
        "producer_jobs": sub.direct_producers,
        "consumer_jobs": sub.direct_consumers,
        "cols": {"lineage": [], "other": [], "other_total": 0},
    }));

    for (idx, jn) in job_names.iter().enumerate() {
        let nid = idx + 1;
        let info = job_map.get(jn.as_str());
        let (dag, task) = info
            .map(|x| (x.dag_name.clone(), x.task_name.clone()))
            .unwrap_or_else(|| {
                split_job_name(jn)
                    .map(|(_, d, t)| (d, t))
                    .unwrap_or_default()
            });
        let srcs = info.map(|x| x.source_tables.clone()).unwrap_or_default();
        let tgts = info.map(|x| x.target_tables.clone()).unwrap_or_default();
        nodes_json.push(json!({
            "id": nid,
            "name": jn,
            "kind": "job",
            "dag_name": dag,
            "task_name": task,
            "is_source": out_deg[nid] > 0,
            "is_target": in_deg[nid] > 0,
            "is_focus": false,
            "ncols": tgts.len(),
            "in_degree": in_deg[nid],
            "out_degree": out_deg[nid],
            "level": levels[nid],
            "source_tables": srcs,
            "target_tables": tgts,
            "latest_batch_id": info.map(|x| x.latest_batch_id).unwrap_or(0),
            "cols": {"lineage": [], "other": [], "other_total": 0},
        }));
    }

    let edges_json: Vec<Value> = graph_edge_tuples
        .into_iter()
        .map(|(s, t, label, via)| {
            json!({
                "s": s,
                "t": t,
                "label": label,
                "via_tables": via,
                "stmts": [],
                "n_stmts": via.len(),
            })
        })
        .collect();

    TableTaskLineageResponse {
        query_table: focus_table.to_string(),
        scope: scope.as_str(),
        direction: dir.as_str(),
        hops,
        producer_jobs: sub.direct_producers,
        consumer_jobs: sub.direct_consumers,
        upstream_jobs,
        downstream_jobs,
        task_edges,
        graph: json!({
            "mode": "task",
            "focus_id": 0,
            "focus_label": focus_table,
            "tables": nodes_json,
            "edges": edges_json,
            "col_edges": [],
        }),
    }
}

fn build_task_lineage_response(
    query_job: &str,
    chosen: JobSearchItem,
    candidates: Vec<JobSearchItem>,
    dir: LineageDirection,
    hops: u32,
    scope: QueryScope,
    sub: RawTaskSubgraph,
) -> TaskLineageResponse {
    let focus_name = chosen.job_name.clone();
    let job_map: HashMap<&str, &JobSearchItem> =
        sub.jobs.iter().map(|j| (j.job_name.as_str(), j)).collect();

    let mut upstream_jobs: Vec<TaskNodeOut> = sub
        .up_distances
        .iter()
        .filter(|(jn, _)| jn != &focus_name)
        .filter_map(|(jn, d)| job_map.get(jn.as_str()).map(|j| to_task_node_out(j, *d)))
        .collect();
    upstream_jobs.sort_by(|a, b| {
        a.distance
            .cmp(&b.distance)
            .then_with(|| a.job_name.cmp(&b.job_name))
    });

    let mut downstream_jobs: Vec<TaskNodeOut> = sub
        .down_distances
        .iter()
        .filter(|(jn, _)| jn != &focus_name)
        .filter_map(|(jn, d)| job_map.get(jn.as_str()).map(|j| to_task_node_out(j, *d)))
        .collect();
    downstream_jobs.sort_by(|a, b| {
        a.distance
            .cmp(&b.distance)
            .then_with(|| a.job_name.cmp(&b.job_name))
    });

    let task_edges = merge_task_edges(&sub.task_edges);

    let mut job_names: Vec<String> = sub.jobs.iter().map(|j| j.job_name.clone()).collect();
    if !job_names.contains(&focus_name) {
        job_names.push(focus_name.clone());
    }
    job_names.sort();

    let edge_pairs_str: Vec<(String, String)> = task_edges
        .iter()
        .map(|e| (e.source_job.clone(), e.target_job.clone()))
        .collect();
    let chains = extract_directed_chains(&job_names, &edge_pairs_str);

    let job_to_id: HashMap<&str, usize> = job_names
        .iter()
        .enumerate()
        .map(|(i, n)| (n.as_str(), i))
        .collect();
    let idx_edges: Vec<(usize, usize)> = task_edges
        .iter()
        .filter_map(|e| {
            Some((
                *job_to_id.get(e.source_job.as_str())?,
                *job_to_id.get(e.target_job.as_str())?,
            ))
        })
        .collect();
    let levels = compute_dag_levels(job_names.len(), &idx_edges);
    let mut in_deg = vec![0usize; job_names.len()];
    let mut out_deg = vec![0usize; job_names.len()];
    for &(s, t) in &idx_edges {
        out_deg[s] += 1;
        in_deg[t] += 1;
    }

    let focus_id = job_to_id.get(focus_name.as_str()).copied();
    let nodes_json: Vec<Value> = job_names
        .iter()
        .enumerate()
        .map(|(i, jn)| {
            let info = job_map.get(jn.as_str()).copied().or_else(|| {
                if jn == &chosen.job_name {
                    Some(&chosen)
                } else {
                    None
                }
            });
            let srcs = info.map(|x| x.source_tables.clone()).unwrap_or_default();
            let tgts = info.map(|x| x.target_tables.clone()).unwrap_or_default();
            json!({
                "id": i,
                "name": jn,
                "kind": "job",
                "dag_name": info.map(|x| x.dag_name.as_str()).unwrap_or(""),
                "task_name": info.map(|x| x.task_name.as_str()).unwrap_or(jn.as_str()),
                "is_source": out_deg[i] > 0,
                "is_target": in_deg[i] > 0,
                "is_focus": Some(i) == focus_id,
                "ncols": tgts.len(),
                "in_degree": in_deg[i],
                "out_degree": out_deg[i],
                "level": levels[i],
                "source_tables": srcs,
                "target_tables": tgts,
                "latest_batch_id": info.map(|x| x.latest_batch_id).unwrap_or(0),
                "cols": {"lineage": [], "other": [], "other_total": 0},
            })
        })
        .collect();

    let edges_json: Vec<Value> = task_edges
        .iter()
        .filter_map(|e| {
            let s = *job_to_id.get(e.source_job.as_str())?;
            let t = *job_to_id.get(e.target_job.as_str())?;
            let lbl = if e.via_tables.len() <= 2 {
                e.via_tables.join(", ")
            } else {
                format!("{} 等{}表", e.via_tables[0], e.via_tables.len())
            };
            Some(json!({
                "s": s,
                "t": t,
                "label": lbl,
                "via_tables": e.via_tables,
                "stmts": [],
                "n_stmts": e.via_tables.len(),
            }))
        })
        .collect();

    TaskLineageResponse {
        query_job: query_job.to_string(),
        matched_job: Some(focus_name.clone()),
        scope: scope.as_str(),
        direction: dir.as_str(),
        hops,
        job_detail: Some(chosen),
        candidates,
        upstream_jobs,
        downstream_jobs,
        task_edges,
        chains,
        graph: json!({
            "mode": "task",
            "focus_id": focus_id,
            "focus_label": focus_name,
            "tables": nodes_json,
            "edges": edges_json,
            "col_edges": [],
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_column_lineage_response(
    db_name: &str,
    table_name: &str,
    full_table: &str,
    col_name: &str,
    dir: LineageDirection,
    hops: u32,
    scope: QueryScope,
    sub: RawColumnSubgraph,
) -> ColumnLineageResponse {
    let query_qualified = format_qualified_column(full_table, col_name);

    // 聚合字段边 (src_tbl, src_col, dst_tbl, dst_col) -> (best_kind, jobs)
    type ColEdgeMap = HashMap<(String, String, String, String), (String, Vec<String>)>;
    let mut ce_map: ColEdgeMap = HashMap::default();
    let mut col_kinds: HashMap<(String, String), Vec<String>> = HashMap::default();
    let mut col_jobs: HashMap<(String, String), Vec<String>> = HashMap::default();
    let mut all_tbls: HashSet<String> = HashSet::default();

    if !sub.edges.is_empty() || !sub.job_tables.is_empty() {
        all_tbls.insert(full_table.to_string());
    }

    for ce in &sub.edges {
        all_tbls.insert(ce.source_full_name.clone());
        all_tbls.insert(ce.target_full_name.clone());
        let key = (
            ce.source_full_name.clone(),
            ce.source_column.clone(),
            ce.target_full_name.clone(),
            ce.target_column.clone(),
        );
        let entry = ce_map
            .entry(key)
            .or_insert_with(|| (ce.edge_kind.clone(), Vec::new()));
        if edge_rank(&ce.edge_kind) > edge_rank(&entry.0) {
            entry.0 = ce.edge_kind.clone();
        }
        if !entry.1.contains(&ce.job_name) {
            entry.1.push(ce.job_name.clone());
        }

        for endpoint in [
            (ce.source_full_name.clone(), ce.source_column.clone()),
            (ce.target_full_name.clone(), ce.target_column.clone()),
        ] {
            let ks = col_kinds.entry(endpoint.clone()).or_default();
            if !ks.contains(&ce.edge_kind) {
                ks.push(ce.edge_kind.clone());
            }
            let js = col_jobs.entry(endpoint).or_default();
            if !js.contains(&ce.job_name) {
                js.push(ce.job_name.clone());
            }
        }
    }

    let make_col_node = |(t, c): &(String, String), dist: u32| -> ColumnNodeOut {
        let (d, tbl, _) = split_db_table(t);
        let mut ks = col_kinds
            .get(&(t.clone(), c.clone()))
            .cloned()
            .unwrap_or_default();
        let mut js = col_jobs
            .get(&(t.clone(), c.clone()))
            .cloned()
            .unwrap_or_default();
        ks.sort();
        js.sort();
        ColumnNodeOut {
            qualified_column: format_qualified_column(t, c),
            db_name: d,
            table_name: tbl,
            full_table_name: t.clone(),
            column_name: c.clone(),
            distance: dist,
            edge_kinds: ks,
            jobs: js,
        }
    };

    let mut upstream_columns: Vec<ColumnNodeOut> = sub
        .up_distances
        .iter()
        .filter(|((t, c), _)| !(t == full_table && c == col_name))
        .map(|(tc, d)| make_col_node(tc, *d))
        .collect();
    upstream_columns.sort_by(|a, b| {
        a.distance
            .cmp(&b.distance)
            .then_with(|| a.qualified_column.cmp(&b.qualified_column))
    });

    let mut downstream_columns: Vec<ColumnNodeOut> = sub
        .down_distances
        .iter()
        .filter(|((t, c), _)| !(t == full_table && c == col_name))
        .map(|(tc, d)| make_col_node(tc, *d))
        .collect();
    downstream_columns.sort_by(|a, b| {
        a.distance
            .cmp(&b.distance)
            .then_with(|| a.qualified_column.cmp(&b.qualified_column))
    });

    let mut edges_out: Vec<ColumnEdgeOut> = ce_map
        .into_iter()
        .map(|((st, sc, tt, tc), (kind, mut jobs))| {
            jobs.sort();
            ColumnEdgeOut {
                source: format_qualified_column(&st, &sc),
                target: format_qualified_column(&tt, &tc),
                source_table: st,
                source_column: sc,
                target_table: tt,
                target_column: tc,
                kind,
                jobs,
            }
        })
        .collect();
    edges_out.sort_by(|a, b| {
        a.source
            .cmp(&b.source)
            .then_with(|| a.target.cmp(&b.target))
    });

    let aggregate_col_tables = |cols: &[ColumnNodeOut]| -> Vec<ColumnTableOut> {
        let mut tmap: HashMap<String, ColumnTableOut> = HashMap::default();
        for c in cols {
            let entry = tmap
                .entry(c.full_table_name.clone())
                .or_insert_with(|| ColumnTableOut {
                    full_table_name: c.full_table_name.clone(),
                    db_name: c.db_name.clone(),
                    table_name: c.table_name.clone(),
                    distance: c.distance,
                    columns: Vec::new(),
                    jobs: Vec::new(),
                });
            entry.distance = entry.distance.min(c.distance);
            if !entry.columns.contains(&c.column_name) {
                entry.columns.push(c.column_name.clone());
            }
            for j in &c.jobs {
                if !entry.jobs.contains(j) {
                    entry.jobs.push(j.clone());
                }
            }
        }
        let mut out: Vec<ColumnTableOut> = tmap.into_values().collect();
        for item in &mut out {
            item.columns.sort();
            item.jobs.sort();
        }
        out.sort_by(|a, b| {
            a.distance
                .cmp(&b.distance)
                .then_with(|| a.full_table_name.cmp(&b.full_table_name))
        });
        out
    };
    let upstream_tables = aggregate_col_tables(&upstream_columns);
    let downstream_tables = aggregate_col_tables(&downstream_columns);

    let mut up_dist_map: HashMap<(&str, &str), u32> = HashMap::default();
    up_dist_map.insert((full_table, col_name), 0);
    up_dist_map.insert((full_table, "*"), 0);
    for ((t, c), d) in &sub.up_distances {
        up_dist_map.entry((t.as_str(), c.as_str())).or_insert(*d);
    }
    let mut down_dist_map: HashMap<(&str, &str), u32> = HashMap::default();
    down_dist_map.insert((full_table, col_name), 0);
    down_dist_map.insert((full_table, "*"), 0);
    for ((t, c), d) in &sub.down_distances {
        down_dist_map.entry((t.as_str(), c.as_str())).or_insert(*d);
    }

    let mut up_job_map: HashMap<String, ColumnJobOut> = HashMap::default();
    let mut down_job_map: HashMap<String, ColumnJobOut> = HashMap::default();
    for ce in &edges_out {
        let sk = (ce.source_table.as_str(), ce.source_column.as_str());
        let tk = (ce.target_table.as_str(), ce.target_column.as_str());
        if let (Some(_), Some(&td)) = (up_dist_map.get(&sk), up_dist_map.get(&tk)) {
            for jn in &ce.jobs {
                let (_, dag, task) = split_job_name(jn).unwrap_or_default();
                let entry = up_job_map
                    .entry(jn.clone())
                    .or_insert_with(|| ColumnJobOut {
                        job_name: jn.clone(),
                        dag_name: dag,
                        task_name: task,
                        distance: td + 1,
                        via_tables: Vec::new(),
                        via_columns: Vec::new(),
                    });
                entry.distance = entry.distance.min(td + 1);
                for tbl in [&ce.source_table, &ce.target_table] {
                    if !entry.via_tables.contains(tbl) {
                        entry.via_tables.push(tbl.clone());
                    }
                }
                for qc in [&ce.source, &ce.target] {
                    if !entry.via_columns.contains(qc) {
                        entry.via_columns.push(qc.clone());
                    }
                }
            }
        }
        if let (Some(&sd), Some(_)) = (down_dist_map.get(&sk), down_dist_map.get(&tk)) {
            for jn in &ce.jobs {
                let (_, dag, task) = split_job_name(jn).unwrap_or_default();
                let entry = down_job_map
                    .entry(jn.clone())
                    .or_insert_with(|| ColumnJobOut {
                        job_name: jn.clone(),
                        dag_name: dag,
                        task_name: task,
                        distance: sd + 1,
                        via_tables: Vec::new(),
                        via_columns: Vec::new(),
                    });
                entry.distance = entry.distance.min(sd + 1);
                for tbl in [&ce.source_table, &ce.target_table] {
                    if !entry.via_tables.contains(tbl) {
                        entry.via_tables.push(tbl.clone());
                    }
                }
                for qc in [&ce.source, &ce.target] {
                    if !entry.via_columns.contains(qc) {
                        entry.via_columns.push(qc.clone());
                    }
                }
            }
        }
    }
    let finish_col_jobs = |map: HashMap<String, ColumnJobOut>| -> Vec<ColumnJobOut> {
        let mut out: Vec<ColumnJobOut> = map.into_values().collect();
        for item in &mut out {
            item.via_tables.sort();
            item.via_columns.sort();
        }
        out.sort_by(|a, b| {
            a.distance
                .cmp(&b.distance)
                .then_with(|| a.job_name.cmp(&b.job_name))
        });
        out
    };
    let upstream_jobs = finish_col_jobs(up_job_map);
    let downstream_jobs = finish_col_jobs(down_job_map);

    // 提取 `库名.表名.字段名` 完整血缘路径链
    let mut all_qcols: Vec<String> = Vec::new();
    let mut qcol_seen: HashSet<String> = HashSet::default();
    let mut col_str_edges: Vec<(String, String)> = Vec::with_capacity(edges_out.len());
    for e in &edges_out {
        if qcol_seen.insert(e.source.clone()) {
            all_qcols.push(e.source.clone());
        }
        if qcol_seen.insert(e.target.clone()) {
            all_qcols.push(e.target.clone());
        }
        col_str_edges.push((e.source.clone(), e.target.clone()));
    }
    let chains = extract_directed_chains(&all_qcols, &col_str_edges);

    // 构建 WebUI 字段级视图（兼容现有 colAnchor / edgeSvg 渲染器）
    let mut tbl_cols_all: HashMap<String, Vec<String>> = HashMap::default();
    let mut tbl_kind_map: HashMap<String, String> = HashMap::default();
    for jt in &sub.job_tables {
        tbl_kind_map
            .entry(jt.full_table_name.clone())
            .or_insert_with(|| jt.table_kind.clone());
        let entry = tbl_cols_all.entry(jt.full_table_name.clone()).or_default();
        for c in &jt.columns {
            if !entry.contains(c) {
                entry.push(c.clone());
            }
        }
    }

    let mut sorted_tbls: Vec<String> = all_tbls.into_iter().collect();
    sorted_tbls.sort();
    let tbl_to_id: HashMap<&str, usize> = sorted_tbls
        .iter()
        .enumerate()
        .map(|(i, s)| (s.as_str(), i))
        .collect();

    let mut tbl_edge_jobs: HashMap<(usize, usize), Vec<String>> = HashMap::default();
    let mut col_deg_in: HashMap<(usize, String), usize> = HashMap::default();
    let mut col_deg_out: HashMap<(usize, String), usize> = HashMap::default();
    let mut tbl_lineage_cols: Vec<Vec<String>> = vec![Vec::new(); sorted_tbls.len()];

    if let Some(&ftid) = tbl_to_id.get(full_table) {
        tbl_lineage_cols[ftid].push(col_name.to_string());
    }

    let mut col_edges_json: Vec<Value> = Vec::with_capacity(edges_out.len());
    for (ei, ce) in edges_out.iter().enumerate() {
        let (Some(&stid), Some(&ttid)) = (
            tbl_to_id.get(ce.source_table.as_str()),
            tbl_to_id.get(ce.target_table.as_str()),
        ) else {
            continue;
        };
        if !tbl_lineage_cols[stid].contains(&ce.source_column) {
            tbl_lineage_cols[stid].push(ce.source_column.clone());
        }
        if !tbl_lineage_cols[ttid].contains(&ce.target_column) {
            tbl_lineage_cols[ttid].push(ce.target_column.clone());
        }
        *col_deg_out
            .entry((stid, ce.source_column.clone()))
            .or_default() += 1;
        *col_deg_in
            .entry((ttid, ce.target_column.clone()))
            .or_default() += 1;
        let tej = tbl_edge_jobs.entry((stid, ttid)).or_default();
        for j in &ce.jobs {
            if !tej.contains(j) {
                tej.push(j.clone());
            }
        }
        col_edges_json.push(json!({
            "i": ei,
            "st": stid,
            "sc": ce.source_column,
            "tt": ttid,
            "tc": ce.target_column,
            "kind": ce.kind,
            "jobs": ce.jobs,
            "stmts": [],
            "n_stmts": ce.jobs.len(),
        }));
    }

    let mut tbl_pairs: Vec<(usize, usize)> = tbl_edge_jobs.keys().copied().collect();
    tbl_pairs.sort_unstable();
    let levels = compute_dag_levels(sorted_tbls.len(), &tbl_pairs);
    let mut in_deg = vec![0usize; sorted_tbls.len()];
    let mut out_deg = vec![0usize; sorted_tbls.len()];
    for &(s, t) in &tbl_pairs {
        out_deg[s] += 1;
        in_deg[t] += 1;
    }

    let focus_tid = tbl_to_id.get(full_table).copied();
    let tables_json: Vec<Value> = sorted_tbls
        .iter()
        .enumerate()
        .map(|(tid, tname)| {
            let lcols = &tbl_lineage_cols[tid];
            let lcols_json: Vec<Value> = lcols
                .iter()
                .map(|c| {
                    let din = col_deg_in.get(&(tid, c.clone())).copied().unwrap_or(0);
                    let dout = col_deg_out.get(&(tid, c.clone())).copied().unwrap_or(0);
                    json!({"name": c, "in": din, "out": dout})
                })
                .collect();
            let all_c = tbl_cols_all.get(tname).cloned().unwrap_or_default();
            let other: Vec<String> = all_c
                .iter()
                .filter(|c| !lcols.contains(c))
                .cloned()
                .collect();
            json!({
                "id": tid,
                "name": tname,
                "kind": tbl_kind_map.get(tname).map(String::as_str).unwrap_or("table"),
                "is_source": out_deg[tid] > 0,
                "is_target": in_deg[tid] > 0,
                "is_focus": Some(tid) == focus_tid,
                "ncols": all_c.len().max(lcols.len()),
                "in_degree": in_deg[tid],
                "out_degree": out_deg[tid],
                "level": levels[tid],
                "columns": all_c,
                "cols": {
                    "lineage": lcols_json,
                    "other": other,
                    "other_total": other.len(),
                }
            })
        })
        .collect();

    let edges_json: Vec<Value> = tbl_pairs
        .into_iter()
        .map(|(s, t)| {
            let mut js = tbl_edge_jobs.remove(&(s, t)).unwrap_or_default();
            js.sort();
            let lbl = if js.len() <= 2 {
                js.join(", ")
            } else {
                format!("{} 等{}任务", js[0], js.len())
            };
            json!({
                "s": s,
                "t": t,
                "label": lbl,
                "jobs": js,
                "stmts": [],
                "n_stmts": js.len(),
            })
        })
        .collect();

    ColumnLineageResponse {
        query_column: query_qualified.clone(),
        db_name: db_name.to_string(),
        table_name: table_name.to_string(),
        full_table_name: full_table.to_string(),
        column_name: col_name.to_string(),
        scope: scope.as_str(),
        direction: dir.as_str(),
        hops,
        upstream_columns,
        downstream_columns,
        upstream_tables,
        downstream_tables,
        upstream_jobs,
        downstream_jobs,
        edges: edges_out,
        chains,
        graph: json!({
            "mode": "column",
            "focus_id": focus_tid,
            "focus_col": col_name,
            "focus_label": query_qualified,
            "tables": tables_json,
            "edges": edges_json,
            "col_edges": col_edges_json,
        }),
    }
}

fn edge_rank(k: &str) -> u8 {
    match k {
        "aggregate" => 3,
        "derived" => 2,
        _ => 1,
    }
}
