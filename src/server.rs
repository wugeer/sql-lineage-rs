//! 血缘可视化 Web 服务：会话内存图索引 + 大文件上传 + 按需子图裁剪

use crate::db::query::LineageService;
use crate::db::schema::postgres_ddl;
use crate::db::{DbConfig, LineageDirection, QueryScope, SaveSummary};
use crate::{analyze_with_sources, AnalyzedWorkspace, InputFile, Options, SqlDialect};
use axum::extract::{DefaultBodyLimit, Multipart, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::Router;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use serde::Deserialize;
use serde_json::json;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const INDEX_HTML: &str = include_str!("web/index.html");
const MAX_SESSIONS: usize = 32;
const MAX_STMT_PER_EDGE: usize = 64;
const MAX_COLS_PER_NODE: usize = 256;

/// 一次分析会话：解析结果 + 血缘索引
pub struct SessionGraph {
    ws: AnalyzedWorkspace,
    dialect: SqlDialect,
    default_db: Option<String>,
    /// 表级上下游邻接（去重，按表 id）
    up: Vec<Vec<u32>>,
    down: Vec<Vec<u32>>,
    /// 拓扑分层（供前端 DAG 布局；成环节点降级为近似层）
    levels: Vec<u32>,
    /// 列级索引：(table, col) -> col_edge 下标集合
    col_out: HashMap<(u32, Box<str>), Vec<u32>>,
    col_in: HashMap<(u32, Box<str>), Vec<u32>>,
    /// 表级出边列索引：source_table -> col_edge 下标集合（用于快速裁剪子图列边）
    tbl_col_out: Vec<Vec<u32>>,
    name_to_id: HashMap<String, u32>,
    /// 库名 -> 表数量（按出现顺序）
    dbs: Vec<(String, u32)>,
    /// 每条列边涉及的列端点
    col_deg_out: HashMap<(u32, Box<str>), u32>,
    col_deg_in: HashMap<(u32, Box<str>), u32>,
}

impl SessionGraph {
    fn build(ws: AnalyzedWorkspace) -> Self {
        let r = &ws.result;
        let n = r.tables.len();
        let mut up: Vec<Vec<u32>> = vec![Vec::new(); n];
        let mut down: Vec<Vec<u32>> = vec![Vec::new(); n];
        let mut seen: HashSet<(u32, u32)> = HashSet::default();
        for e in &r.table_edges {
            if seen.insert((e.source as u32, e.target as u32)) {
                up[e.target].push(e.source as u32);
                down[e.source].push(e.target as u32);
            }
        }

        // 拓扑层：Kahn BFS；残余（环）按已处理前驱的最大层 +1
        let mut indeg: Vec<u32> = vec![0; n];
        for outs in &down {
            for &t in outs {
                indeg[t as usize] += 1;
            }
        }
        let mut levels = vec![u32::MAX; n];
        let mut queue: VecDeque<u32> = VecDeque::new();
        for (i, &d) in indeg.iter().enumerate() {
            if d == 0 {
                levels[i] = 0;
                queue.push_back(i as u32);
            }
        }
        let mut processed = vec![false; n];
        while let Some(u) = queue.pop_front() {
            processed[u as usize] = true;
            for &v in &down[u as usize] {
                if levels[v as usize] < levels[u as usize] + 1 {
                    levels[v as usize] = levels[u as usize] + 1;
                }
                indeg[v as usize] = indeg[v as usize].saturating_sub(1);
                if indeg[v as usize] == 0 {
                    queue.push_back(v);
                }
            }
        }
        // 环节点：取其在已处理前驱中的最大层 +1（无则 0）
        for i in 0..n {
            if !processed[i] {
                let mut best = 0u32;
                for &p in &up[i] {
                    if processed[p as usize] && levels[p as usize] != u32::MAX {
                        best = best.max(levels[p as usize] + 1);
                    }
                }
                levels[i] = best;
            }
        }

        let mut col_out: HashMap<(u32, Box<str>), Vec<u32>> = HashMap::default();
        let mut col_in: HashMap<(u32, Box<str>), Vec<u32>> = HashMap::default();
        let mut col_deg_out: HashMap<(u32, Box<str>), u32> = HashMap::default();
        let mut col_deg_in: HashMap<(u32, Box<str>), u32> = HashMap::default();
        let mut tbl_col_out: Vec<Vec<u32>> = vec![Vec::new(); n];
        for (i, e) in r.column_edges.iter().enumerate() {
            let sk = (e.source_table as u32, e.source_column.as_str().into());
            let tk = (e.target_table as u32, e.target_column.as_str().into());
            col_out.entry(sk.clone()).or_default().push(i as u32);
            col_in.entry(tk.clone()).or_default().push(i as u32);
            if e.source_table < n {
                tbl_col_out[e.source_table].push(i as u32);
            }
            *col_deg_out.entry(sk).or_default() += 1;
            *col_deg_in.entry(tk).or_default() += 1;
        }

        let mut name_to_id = HashMap::default();
        let mut dbs: Vec<(String, u32)> = Vec::new();
        let mut db_idx: HashMap<String, usize> = HashMap::default();
        for t in &r.tables {
            name_to_id.insert(t.name.clone(), t.id as u32);
            let db = match t.name.rfind('.') {
                Some(i) => &t.name[..i],
                None => "",
            };
            let k = *db_idx.entry(db.to_string()).or_insert_with(|| {
                dbs.push((db.to_string(), 0));
                dbs.len() - 1
            });
            dbs[k].1 += 1;
        }
        dbs.sort_by_key(|a| std::cmp::Reverse(a.1));

        SessionGraph {
            ws,
            dialect: SqlDialect::Hive,
            default_db: None,
            up,
            down,
            levels,
            col_out,
            col_in,
            tbl_col_out,
            name_to_id,
            dbs,
            col_deg_out,
            col_deg_in,
        }
    }

    fn table_json(&self, tid: usize) -> serde_json::Value {
        let t = &self.ws.result.tables[tid];
        json!({
            "id": t.id,
            "name": t.name,
            "kind": t.kind,
            "is_source": t.is_source,
            "is_target": t.is_target,
            "ncols": t.columns.len(),
            "in_degree": self.up[tid].len(),
            "out_degree": self.down[tid].len(),
            "level": self.levels[tid],
        })
    }

    /// 节点列信息：先列出血缘列（带度信息），其余列折叠
    fn node_cols_json(&self, tid: usize, edge_set: &HashSet<u32>) -> serde_json::Value {
        let t = &self.ws.result.tables[tid];
        let mut lineage_cols: HashSet<&str> = HashSet::default();
        for &ei in edge_set {
            let e = &self.ws.result.column_edges[ei as usize];
            if e.source_table == tid {
                lineage_cols.insert(e.source_column.as_str());
            }
            if e.target_table == tid {
                lineage_cols.insert(e.target_column.as_str());
            }
        }
        let mut cols: Vec<serde_json::Value> = Vec::new();
        let mut used: HashSet<&str> = HashSet::default();
        for c in &lineage_cols {
            if !used.insert(c) {
                continue;
            }
            let k = (tid as u32, (*c).into());
            let din = self.col_deg_in.get(&k).copied().unwrap_or(0);
            let dout = self.col_deg_out.get(&k).copied().unwrap_or(0);
            cols.push(json!({"name": c, "in": din, "out": dout}));
        }
        cols.sort_by(|a, b| {
            (b["in"].as_u64().unwrap_or(0) + b["out"].as_u64().unwrap_or(0))
                .cmp(&(a["in"].as_u64().unwrap_or(0) + a["out"].as_u64().unwrap_or(0)))
        });
        let other: Vec<&str> = t
            .columns
            .iter()
            .map(|s| s.as_str())
            .filter(|c| !lineage_cols.contains(c))
            .take(MAX_COLS_PER_NODE)
            .collect();
        json!({
            "lineage": cols,
            "other": other,
            "other_total": t.columns.len().saturating_sub(lineage_cols.len()),
        })
    }

    fn edge_json(&self, s: usize, t: usize, stmts: &[usize]) -> serde_json::Value {
        let total = stmts.len();
        let shown: Vec<usize> = stmts.iter().copied().take(MAX_STMT_PER_EDGE).collect();
        json!({"s": s, "t": t, "stmts": shown, "n_stmts": total})
    }

    fn col_edge_json(&self, ei: u32) -> serde_json::Value {
        let e = &self.ws.result.column_edges[ei as usize];
        json!({
            "i": ei,
            "st": e.source_table, "sc": e.source_column,
            "tt": e.target_table, "tc": e.target_column,
            "kind": e.kind,
            "stmts": e.statements.iter().copied().take(MAX_STMT_PER_EDGE).collect::<Vec<_>>(),
            "n_stmts": e.statements.len(),
        })
    }

    /// 以 focus 表为中心按跳数/方向裁剪表级子图
    fn table_subgraph(
        &self,
        focus: usize,
        hops: u32,
        direction: &str,
    ) -> (HashSet<usize>, HashSet<(usize, usize)>) {
        let mut nodes: HashSet<usize> = HashSet::default();
        let mut depth: HashMap<usize, u32> = HashMap::default();
        let mut q: VecDeque<(usize, u32)> = VecDeque::new();
        nodes.insert(focus);
        depth.insert(focus, 0);
        q.push_back((focus, 0));
        let want_up = direction != "downstream";
        let want_down = direction != "upstream";
        let unlimited = hops == 0;
        while let Some((u, d)) = q.pop_front() {
            if !unlimited && d >= hops {
                continue;
            }
            if want_up {
                for &p in &self.up[u] {
                    let p = p as usize;
                    if nodes.insert(p) {
                        depth.insert(p, d + 1);
                        q.push_back((p, d + 1));
                    }
                }
            }
            if want_down {
                for &v in &self.down[u] {
                    let v = v as usize;
                    if nodes.insert(v) {
                        depth.insert(v, d + 1);
                        q.push_back((v, d + 1));
                    }
                }
            }
        }
        let mut edges: HashSet<(usize, usize)> = HashSet::default();
        for &s in &nodes {
            for &t in &self.down[s] {
                let t = t as usize;
                if nodes.contains(&t) {
                    edges.insert((s, t));
                }
            }
        }
        (nodes, edges)
    }

    fn col_edges_for_nodes(&self, nodes: &HashSet<usize>) -> HashSet<u32> {
        let mut set: HashSet<u32> = HashSet::default();
        for &tid in nodes {
            if let Some(eis) = self.tbl_col_out.get(tid) {
                for &ei in eis {
                    let e = &self.ws.result.column_edges[ei as usize];
                    if nodes.contains(&e.target_table) {
                        set.insert(ei);
                    }
                }
            }
        }
        set
    }

    /// 列级全链路追踪（上游 + 下游）
    fn column_trace(&self, tid: usize, col: &str) -> (HashSet<u32>, HashSet<usize>) {
        let mut edges: HashSet<u32> = HashSet::default();
        let mut tables: HashSet<usize> = HashSet::default();
        // 上游：(st, sc) -> (tid, col)
        let mut up_q: VecDeque<(u32, String)> = VecDeque::new();
        up_q.push_back((tid as u32, col.to_string()));
        let mut seen_up: HashSet<(u32, String)> = HashSet::default();
        seen_up.insert((tid as u32, col.to_string()));
        while let Some((t, c)) = up_q.pop_front() {
            tables.insert(t as usize);
            let key = (t, c.as_str().into());
            if let Some(eis) = self.col_in.get(&key) {
                for &ei in eis {
                    if !edges.insert(ei) {
                        continue;
                    }
                    let e = &self.ws.result.column_edges[ei as usize];
                    let nk = (e.source_table as u32, e.source_column.clone());
                    if seen_up.insert(nk.clone()) {
                        up_q.push_back(nk);
                    }
                }
            }
        }
        // 下游
        let mut down_q: VecDeque<(u32, String)> = VecDeque::new();
        down_q.push_back((tid as u32, col.to_string()));
        let mut seen_down: HashSet<(u32, String)> = HashSet::default();
        seen_down.insert((tid as u32, col.to_string()));
        while let Some((t, c)) = down_q.pop_front() {
            tables.insert(t as usize);
            let key = (t, c.as_str().into());
            if let Some(eis) = self.col_out.get(&key) {
                for &ei in eis {
                    if !edges.insert(ei) {
                        continue;
                    }
                    let e = &self.ws.result.column_edges[ei as usize];
                    let nk = (e.target_table as u32, e.target_column.clone());
                    if seen_down.insert(nk.clone()) {
                        down_q.push_back(nk);
                    }
                }
            }
        }
        (edges, tables)
    }
}

struct AppState {
    sessions: Mutex<HashMap<String, Arc<SessionGraph>>>,
    seq: AtomicU64,
    default_dialect: SqlDialect,
    db: tokio::sync::RwLock<Option<Arc<LineageService>>>,
}

fn err(status: StatusCode, msg: impl Into<String>) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(json!({"error": msg.into()})))
}

fn get_session(
    state: &AppState,
    id: &str,
) -> Result<Arc<SessionGraph>, (StatusCode, Json<serde_json::Value>)> {
    state
        .sessions
        .lock()
        .unwrap()
        .get(id)
        .cloned()
        .ok_or_else(|| err(StatusCode::NOT_FOUND, format!("会话 {id} 不存在或已过期")))
}

fn store_session(state: &AppState, g: SessionGraph) -> String {
    let mut map = state.sessions.lock().unwrap();
    if map.len() >= MAX_SESSIONS {
        // 按会话序号数值淘汰最老 session（避免 "s10" < "s2" 的字典序误淘汰）
        if let Some(oldest) = map
            .keys()
            .min_by_key(|k| {
                k.strip_prefix('s')
                    .and_then(|n| n.parse::<u64>().ok())
                    .unwrap_or(u64::MAX)
            })
            .cloned()
        {
            map.remove(&oldest);
        }
    }
    let id = format!("s{}", state.seq.fetch_add(1, Ordering::Relaxed) + 1);
    map.insert(id.clone(), Arc::new(g));
    id
}

// ---------- handlers ----------

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({"ok": true}))
}

#[derive(Deserialize)]
struct TextAnalyzeBody {
    sql: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    default_db: Option<String>,
    /// "auto" | "yes" | "no"
    #[serde(default)]
    analyze_select: Option<String>,
    #[serde(default)]
    dialect: Option<String>,
    #[serde(default)]
    job_name: Option<String>,
    #[serde(default)]
    save_db: Option<bool>,
}

fn parse_select_flag(v: &Option<String>) -> Option<bool> {
    match v
        .as_deref()
        .map(|s| s.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("yes" | "true" | "on" | "all") => Some(true),
        Some("no" | "false" | "off" | "none") => Some(false),
        _ => None,
    }
}

fn parse_dialect(value: Option<&str>, default: SqlDialect) -> Result<SqlDialect, String> {
    value
        .map(str::parse)
        .transpose()
        .map(|v| v.unwrap_or(default))
}

fn run_analysis(
    files: Vec<InputFile>,
    default_db: Option<String>,
    analyze_select: Option<bool>,
    dialect: SqlDialect,
) -> Result<SessionGraph, String> {
    run_analysis_with_job(files, default_db, analyze_select, dialect, None)
}

fn run_analysis_with_job(
    files: Vec<InputFile>,
    default_db: Option<String>,
    analyze_select: Option<bool>,
    dialect: SqlDialect,
    job_name: Option<String>,
) -> Result<SessionGraph, String> {
    if files.is_empty() {
        return Err("未提供 SQL 内容".to_string());
    }
    let clean_db = default_db.filter(|s| !s.trim().is_empty());
    let clean_job = job_name
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let opts = Options {
        dialect,
        default_db: clean_db.clone(),
        analyze_select,
        job_name: clean_job,
    };
    let t = Instant::now();
    let ws = analyze_with_sources(files, &opts);
    let mut g = SessionGraph::build(ws);
    g.dialect = dialect;
    g.default_db = clean_db;
    eprintln!(
        "[analyze] files={} bytes={} stmt={} tables={} tedges={} cedges={} total={:?}",
        g.ws.result.stats.files,
        g.ws.result.stats.bytes,
        g.ws.result.stats.statements,
        g.ws.result.tables.len(),
        g.ws.result.table_edges.len(),
        g.ws.result.column_edges.len(),
        t.elapsed()
    );
    Ok(g)
}

fn analyze_response_with_save(
    state: &Arc<AppState>,
    g: SessionGraph,
    saved: Option<SaveSummary>,
) -> Response {
    let job_name = g.ws.result.job_name.clone();
    let stats = serde_json::to_value(&g.ws.result.stats).unwrap_or_else(|_| json!({}));
    let files: Vec<serde_json::Value> =
        g.ws.result
            .files
            .iter()
            .map(|f| json!({"name": f.name, "encoding": f.encoding}))
            .collect();
    let dbs: Vec<serde_json::Value> = g
        .dbs
        .iter()
        .map(|(d, n)| json!({"db": d, "tables": n}))
        .collect();
    // 初始视图：表数 <= 60 直接全图；否则聚焦写入扇入最高的目标表（多跳子图）
    let n = g.ws.result.tables.len();
    let small = n <= 60;
    let mut focus_table: Option<usize> = None;
    let (nodes, edges) = if small {
        let nodes: HashSet<usize> = (0..n).collect();
        let edges: HashSet<(usize, usize)> =
            g.ws.result
                .table_edges
                .iter()
                .map(|e| (e.source, e.target))
                .collect();
        (nodes, edges)
    } else {
        // 选择下游影响最大的目标表作为默认焦点
        let focus = (0..n)
            .filter(|&i| g.ws.result.tables[i].is_target)
            .max_by_key(|&i| g.up[i].len() + g.down[i].len())
            .unwrap_or(0);
        focus_table = Some(focus);
        g.table_subgraph(focus, 1, "both")
    };
    let col_edge_set = g.col_edges_for_nodes(&nodes);
    let tables_json: Vec<serde_json::Value> = {
        let mut v: Vec<serde_json::Value> = nodes
            .iter()
            .map(|&tid| {
                let mut t = g.table_json(tid);
                t["cols"] = g.node_cols_json(tid, &col_edge_set);
                t
            })
            .collect();
        v.sort_by_key(|t| t["id"].as_u64().unwrap_or(0));
        v
    };
    let edges_json: Vec<serde_json::Value> = edges
        .iter()
        .map(|&(s, t)| {
            let stmts =
                g.ws.result
                    .table_edges
                    .iter()
                    .find(|e| e.source == s && e.target == t)
                    .map(|e| e.statements.as_slice())
                    .unwrap_or(&[]);
            g.edge_json(s, t, stmts)
        })
        .collect();
    let cedges_json: Vec<serde_json::Value> =
        col_edge_set.iter().map(|&ei| g.col_edge_json(ei)).collect();
    let n_table_edges = g.ws.result.table_edges.len();
    let n_col_edges = g.ws.result.column_edges.len();
    let sid = store_session(state, g);
    Json(json!({
        "session": sid,
        "job_name": job_name,
        "saved": saved,
        "stats": stats,
        "files": files,
        "dbs": dbs,
        "initial": {
            "full": small,
            "focus_table": focus_table,
            "tables": tables_json,
            "edges": edges_json,
            "col_edges": cedges_json,
        },
        "counts": {
            "tables": n,
            "table_edges": n_table_edges,
            "column_edges": n_col_edges,
        }
    }))
    .into_response()
}

async fn maybe_save_on_analyze(
    state: &AppState,
    save_db: bool,
    job_name: Option<&str>,
    g: &SessionGraph,
) -> Result<Option<SaveSummary>, (StatusCode, Json<serde_json::Value>)> {
    if !save_db {
        return Ok(None);
    }
    let jn = job_name
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            err(
                StatusCode::BAD_REQUEST,
                "开启追加落库时必须填写 job_name（如 dag.task）",
            )
        })?;
    let svc = state.db.read().await.clone().ok_or_else(|| {
        err(
            StatusCode::BAD_REQUEST,
            "尚未配置数据库连接，请先配置 PostgreSQL 连接",
        )
    })?;
    svc.save_workspace(jn, g.dialect, g.default_db.as_deref(), &g.ws)
        .await
        .map(Some)
        .map_err(|e| {
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("追加落库失败: {e}"),
            )
        })
}

async fn api_analyze_text(
    State(state): State<Arc<AppState>>,
    Json(body): Json<TextAnalyzeBody>,
) -> Response {
    let dialect = match parse_dialect(body.dialect.as_deref(), state.default_dialect) {
        Ok(dialect) => dialect,
        Err(e) => return err(StatusCode::BAD_REQUEST, e).into_response(),
    };
    let save_db = body.save_db.unwrap_or(false);
    let job_name = body.job_name.clone();
    if save_db && job_name.as_deref().map(str::trim).unwrap_or("").is_empty() {
        return err(
            StatusCode::BAD_REQUEST,
            "开启追加落库时必须填写 job_name（如 dag.task）",
        )
        .into_response();
    }
    let files = vec![InputFile {
        name: body.name.unwrap_or_else(|| "editor.sql".to_string()),
        bytes: body.sql.into_bytes(),
    }];
    let select_flag = parse_select_flag(&body.analyze_select);
    let state2 = state.clone();
    let jn_for_parse = job_name.clone();
    match tokio::task::spawn_blocking(move || {
        run_analysis_with_job(
            files,
            body.default_db.clone(),
            select_flag,
            dialect,
            jn_for_parse,
        )
    })
    .await
    {
        Ok(Ok(g)) => match maybe_save_on_analyze(&state2, save_db, job_name.as_deref(), &g).await {
            Ok(saved) => analyze_response_with_save(&state2, g, saved),
            Err(e) => e.into_response(),
        },
        Ok(Err(e)) => err(StatusCode::BAD_REQUEST, e).into_response(),
        Err(e) => err(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("分析任务失败: {e}"),
        )
        .into_response(),
    }
}

async fn api_analyze_upload(State(state): State<Arc<AppState>>, mut mp: Multipart) -> Response {
    let mut files: Vec<InputFile> = Vec::new();
    let mut default_db: Option<String> = None;
    let mut select_flag: Option<bool> = None;
    let mut dialect = state.default_dialect;
    let mut job_name: Option<String> = None;
    let mut save_db = false;
    loop {
        match mp.next_field().await {
            Ok(Some(field)) => {
                let name = field.name().unwrap_or("").to_string();
                match name.as_str() {
                    "default_db" => {
                        if let Ok(v) = field.text().await {
                            if !v.trim().is_empty() {
                                default_db = Some(v);
                            }
                        }
                    }
                    "analyze_select" => {
                        if let Ok(v) = field.text().await {
                            select_flag = parse_select_flag(&Some(v));
                        }
                    }
                    "job_name" => {
                        if let Ok(v) = field.text().await {
                            if !v.trim().is_empty() {
                                job_name = Some(v.trim().to_string());
                            }
                        }
                    }
                    "save_db" => {
                        if let Ok(v) = field.text().await {
                            save_db = matches!(
                                v.trim().to_ascii_lowercase().as_str(),
                                "1" | "true" | "yes" | "on"
                            );
                        }
                    }
                    "dialect" => {
                        let value = match field.text().await {
                            Ok(value) => value,
                            Err(e) => {
                                return err(e.status(), format!("读取方言失败: {}", e.body_text()))
                                    .into_response()
                            }
                        };
                        dialect = match parse_dialect(Some(&value), dialect) {
                            Ok(dialect) => dialect,
                            Err(e) => return err(StatusCode::BAD_REQUEST, e).into_response(),
                        };
                    }
                    _ => {
                        let fname = field
                            .file_name()
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| format!("{name}.sql"));
                        match field.bytes().await {
                            Ok(b) if !b.is_empty() => {
                                files.push(InputFile {
                                    name: fname,
                                    bytes: b.to_vec(),
                                });
                            }
                            Ok(_) => {}
                            Err(e) => {
                                return err(
                                    e.status(),
                                    format!("读取上传文件失败: {}", e.body_text()),
                                )
                                .into_response();
                            }
                        }
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                return err(e.status(), format!("解析上传数据失败: {}", e.body_text()))
                    .into_response();
            }
        }
    }
    let state2 = state.clone();
    let jn_for_parse = job_name.clone();
    match tokio::task::spawn_blocking(move || {
        run_analysis_with_job(files, default_db, select_flag, dialect, jn_for_parse)
    })
    .await
    {
        Ok(Ok(g)) => match maybe_save_on_analyze(&state2, save_db, job_name.as_deref(), &g).await {
            Ok(saved) => analyze_response_with_save(&state2, g, saved),
            Err(e) => e.into_response(),
        },
        Ok(Err(e)) => err(StatusCode::BAD_REQUEST, e).into_response(),
        Err(e) => err(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("分析任务失败: {e}"),
        )
        .into_response(),
    }
}

async fn api_overview(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let g = get_session(&state, &id)?;
    let r = &g.ws.result;
    let mut kind_counts: HashMap<&str, u32> = HashMap::default();
    for s in &r.statements {
        *kind_counts.entry(s.kind).or_default() += 1;
    }
    Ok(Json(json!({
        "stats": r.stats,
        "files": r.files,
        "dbs": g.dbs.iter().map(|(d,n)| json!({"db":d,"tables":n})).collect::<Vec<_>>(),
        "stmt_kinds": kind_counts,
        "counts": {
            "tables": r.tables.len(),
            "table_edges": r.table_edges.len(),
            "column_edges": r.column_edges.len(),
        }
    })))
}

#[derive(Deserialize)]
struct TableQuery {
    q: Option<String>,
    role: Option<String>,
    db: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,
}

async fn api_tables(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(p): Query<TableQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let g = get_session(&state, &id)?;
    let r = &g.ws.result;
    let q = p.q.as_deref().unwrap_or("").to_lowercase();
    let db = p.db.as_deref().unwrap_or("");
    let role = p.role.as_deref().unwrap_or("all");
    let matches = |t: &crate::model::TableInfo| -> bool {
        if !q.is_empty() && !t.name.contains(&q) && !t.columns.iter().any(|c| c.contains(&q)) {
            return false;
        }
        if !db.is_empty() {
            let tdb = t.name.rfind('.').map(|i| &t.name[..i]).unwrap_or("");
            if tdb != db {
                return false;
            }
        }
        match role {
            "target" => t.is_target && !t.is_source,
            "source" => t.is_source && !t.is_target,
            "mid" => t.is_source && t.is_target,
            "result" => t.kind == "result",
            _ => true,
        }
    };
    let offset = p.offset.unwrap_or(0);
    let limit = p.limit.unwrap_or(80).min(500);
    let mut items: Vec<serde_json::Value> = Vec::new();
    let mut total = 0usize;
    for t in r.tables.iter().filter(|t| matches(t)) {
        if total >= offset && items.len() < limit {
            items.push(g.table_json(t.id));
        }
        total += 1;
    }
    Ok(Json(json!({"total": total, "items": items})))
}

#[derive(Deserialize)]
struct NamesQuery {
    ids: Option<String>,
}

async fn api_table_names(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(p): Query<NamesQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let g = get_session(&state, &id)?;
    let mut out = serde_json::Map::new();
    if let Some(ids) = &p.ids {
        for x in ids
            .split(',')
            .filter_map(|x| x.trim().parse::<usize>().ok())
        {
            if let Some(t) = g.ws.result.tables.get(x) {
                out.insert(x.to_string(), json!(t.name));
            }
        }
    }
    Ok(Json(json!({"names": out})))
}

#[derive(Deserialize)]
struct SubgraphQuery {
    table: usize,
    hops: Option<u32>,
    direction: Option<String>,
    col: Option<String>,
}

async fn api_subgraph(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(p): Query<SubgraphQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let g = get_session(&state, &id)?;
    let r = &g.ws.result;
    if p.table >= r.tables.len() {
        return Err(err(StatusCode::BAD_REQUEST, "表 id 超出范围"));
    }
    let direction = p.direction.as_deref().unwrap_or("both");
    // 指定列 -> 列级全链路
    if let Some(col) = p.col.as_deref().filter(|c| !c.is_empty()) {
        let (cedges, tables) = g.column_trace(p.table, col);
        let nodes: Vec<serde_json::Value> = {
            let mut v: Vec<serde_json::Value> = tables
                .iter()
                .map(|&tid| {
                    let mut t = g.table_json(tid);
                    t["cols"] = g.node_cols_json(tid, &cedges);
                    t
                })
                .collect();
            v.sort_by_key(|t| t["id"].as_u64().unwrap_or(0));
            v
        };
        let table_edges: Vec<serde_json::Value> = r
            .table_edges
            .iter()
            .filter(|e| tables.contains(&e.source) && tables.contains(&e.target))
            .map(|e| g.edge_json(e.source, e.target, &e.statements))
            .collect();
        let cedges_json: Vec<serde_json::Value> =
            cedges.iter().map(|&ei| g.col_edge_json(ei)).collect();
        return Ok(Json(json!({
            "full": false,
            "focus": {"table": p.table, "col": col},
            "tables": nodes,
            "edges": table_edges,
            "col_edges": cedges_json,
        })));
    }
    let (nodes, edges) = g.table_subgraph(p.table, p.hops.unwrap_or(1), direction);
    let col_edge_set = g.col_edges_for_nodes(&nodes);
    let mut nodes_json: Vec<serde_json::Value> = nodes
        .iter()
        .map(|&tid| {
            let mut t = g.table_json(tid);
            t["cols"] = g.node_cols_json(tid, &col_edge_set);
            t
        })
        .collect();
    nodes_json.sort_by_key(|t| t["id"].as_u64().unwrap_or(0));
    let edges_json: Vec<serde_json::Value> = edges
        .iter()
        .map(|&(s, t)| {
            let stmts = r
                .table_edges
                .iter()
                .find(|e| e.source == s && e.target == t)
                .map(|e| e.statements.as_slice())
                .unwrap_or(&[]);
            g.edge_json(s, t, stmts)
        })
        .collect();
    let cedges_json: Vec<serde_json::Value> =
        col_edge_set.iter().map(|&ei| g.col_edge_json(ei)).collect();
    Ok(Json(json!({
        "full": false,
        "focus": {"table": p.table},
        "tables": nodes_json,
        "edges": edges_json,
        "col_edges": cedges_json,
    })))
}

#[derive(Deserialize)]
struct StmtQuery {
    ids: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,
    status: Option<String>,
    kind: Option<String>,
}

async fn api_statements(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(p): Query<StmtQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let g = get_session(&state, &id)?;
    let r = &g.ws.result;
    let get_sql = |s: &crate::model::StatementInfo| -> String {
        g.ws.sources
            .get(s.file)
            .map(|t| t.get(s.start..s.end).unwrap_or(""))
            .unwrap_or("")
            .to_string()
    };
    if let Some(ids) = &p.ids {
        let idxs: Vec<usize> = ids
            .split(',')
            .filter_map(|x| x.trim().parse().ok())
            .collect();
        let mut items = Vec::new();
        for i in idxs {
            if let Some(s) = r.statements.get(i) {
                items.push(json!({
                    "index": s.index, "file": r.files.get(s.file).map(|f| f.name.as_str()).unwrap_or(""),
                    "line": s.line, "kind": s.kind, "status": s.status, "error": s.error,
                    "sql": get_sql(s),
                    "targets": s.targets, "sources": s.sources,
                }));
            }
        }
        return Ok(Json(json!({"items": items})));
    }
    let status = p.status.as_deref().unwrap_or("");
    let kind = p.kind.as_deref().unwrap_or("");
    let offset = p.offset.unwrap_or(0);
    let limit = p.limit.unwrap_or(50).min(200);
    let mut items = Vec::new();
    let mut matched = 0usize;
    for s in &r.statements {
        if !status.is_empty() && s.status != status {
            continue;
        }
        if !kind.is_empty() && s.kind != kind {
            continue;
        }
        if matched >= offset && items.len() < limit {
            let sql = get_sql(s);
            let preview: String = sql.chars().take(200).collect();
            items.push(json!({
                "index": s.index, "file": r.files.get(s.file).map(|f| f.name.as_str()).unwrap_or(""),
                "line": s.line, "kind": s.kind, "status": s.status, "error": s.error,
                "preview": preview, "truncated": sql.chars().count() > 200,
                "targets": s.targets, "sources": s.sources,
            }));
        }
        matched += 1;
    }
    Ok(Json(json!({"total": matched, "items": items})))
}

async fn api_table_detail(
    State(state): State<Arc<AppState>>,
    Path((id, tid)): Path<(String, usize)>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let g = get_session(&state, &id)?;
    let r = &g.ws.result;
    if tid >= r.tables.len() {
        return Err(err(StatusCode::NOT_FOUND, "表不存在"));
    }
    let mut t = g.table_json(tid);
    let all_edges: HashSet<u32> = (0..r.column_edges.len() as u32)
        .filter(|&ei| {
            let e = &r.column_edges[ei as usize];
            e.source_table == tid || e.target_table == tid
        })
        .collect();
    t["cols"] = g.node_cols_json(tid, &all_edges);
    t["columns"] = json!(r.tables[tid].columns);
    let ups: Vec<serde_json::Value> = g.up[tid]
        .iter()
        .map(|&u| g.table_json(u as usize))
        .collect();
    let downs: Vec<serde_json::Value> = g.down[tid]
        .iter()
        .map(|&u| g.table_json(u as usize))
        .collect();
    Ok(Json(json!({
        "table": t,
        "up": ups,
        "down": downs,
    })))
}

async fn api_export(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    let g = get_session(&state, &id)?;
    let body = serde_json::to_vec(&g.ws.result).map_err(|e| {
        err(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("序列化失败: {e}"),
        )
    })?;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "application/json; charset=utf-8".parse().unwrap(),
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        format!("attachment; filename=\"lineage_{id}.json\"")
            .parse()
            .unwrap(),
    );
    Ok((headers, body).into_response())
}

async fn api_lookup_table(
    State(state): State<Arc<AppState>>,
    Path((id, name)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let g = get_session(&state, &id)?;
    let found = g
        .name_to_id
        .get(&name)
        .or_else(|| g.name_to_id.get(&name.trim().to_ascii_lowercase()));
    match found {
        Some(&tid) => Ok(Json(g.table_json(tid as usize))),
        None => Err(err(StatusCode::NOT_FOUND, "表不存在")),
    }
}

// ---------- 数据库配置、追加落库与全局血缘查询 API ----------

async fn require_db(
    state: &AppState,
) -> Result<Arc<LineageService>, (StatusCode, Json<serde_json::Value>)> {
    state.db.read().await.clone().ok_or_else(|| {
        err(
            StatusCode::BAD_REQUEST,
            "尚未配置 PostgreSQL 数据库连接，请先在顶栏点击“数据库配置”完成连接",
        )
    })
}

async fn api_db_status(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let svc_opt = state.db.read().await.clone();
    match svc_opt {
        None => Json(json!({"configured": false})),
        Some(svc) => match svc.ping().await {
            Ok(()) => Json(json!({
                "configured": true,
                "ok": true,
                "backend": svc.backend_name(),
                "masked_url": svc.masked_url(),
                "hot_index": svc.hot_index_enabled(),
            })),
            Err(e) => Json(json!({
                "configured": true,
                "ok": false,
                "backend": svc.backend_name(),
                "masked_url": svc.masked_url(),
                "hot_index": svc.hot_index_enabled(),
                "error": e.to_string(),
            })),
        },
    }
}

#[derive(Deserialize)]
struct DbConfigBody {
    url: String,
    #[serde(default)]
    auto_create_table: Option<bool>,
    #[serde(default)]
    use_hot_index: Option<bool>,
}

async fn api_db_config(
    State(state): State<Arc<AppState>>,
    Json(body): Json<DbConfigBody>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let cfg = DbConfig {
        url: body.url,
        auto_create_table: body.auto_create_table.unwrap_or(true),
        use_hot_index: body.use_hot_index.unwrap_or(true),
    };
    let svc = LineageService::connect(cfg)
        .await
        .map_err(|e| err(StatusCode::BAD_REQUEST, format!("数据库连接失败: {e}")))?;
    let resp = json!({
        "configured": true,
        "ok": true,
        "backend": svc.backend_name(),
        "masked_url": svc.masked_url(),
        "hot_index": svc.hot_index_enabled(),
    });
    *state.db.write().await = Some(Arc::new(svc));
    Ok(Json(resp))
}

async fn api_db_ddl() -> Json<serde_json::Value> {
    Json(json!({"ddl": postgres_ddl()}))
}

#[derive(Deserialize)]
struct SaveSessionBody {
    job_name: String,
}

async fn api_save_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SaveSessionBody>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let g = get_session(&state, &id)?;
    let svc = require_db(&state).await?;
    let summary = svc
        .save_workspace(&body.job_name, g.dialect, g.default_db.as_deref(), &g.ws)
        .await
        .map_err(|e| err(StatusCode::BAD_REQUEST, format!("追加落库失败: {e}")))?;
    Ok(Json(json!({"saved": summary})))
}

#[derive(Deserialize)]
struct DbSearchQuery {
    q: Option<String>,
    limit: Option<usize>,
    all_history: Option<bool>,
}

async fn api_db_jobs(
    State(state): State<Arc<AppState>>,
    Query(p): Query<DbSearchQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let svc = require_db(&state).await?;
    let scope = QueryScope::from_all_history(p.all_history.unwrap_or(false));
    let items = svc
        .search_jobs(p.q.as_deref().unwrap_or(""), p.limit.unwrap_or(50), scope)
        .await
        .map_err(|e| {
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("检索任务失败: {e}"),
            )
        })?;
    Ok(Json(json!({"items": items})))
}

async fn api_db_tables(
    State(state): State<Arc<AppState>>,
    Query(p): Query<DbSearchQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let svc = require_db(&state).await?;
    let scope = QueryScope::from_all_history(p.all_history.unwrap_or(false));
    let items = svc
        .search_tables(p.q.as_deref().unwrap_or(""), p.limit.unwrap_or(50), scope)
        .await
        .map_err(|e| {
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("检索表失败: {e}"),
            )
        })?;
    Ok(Json(json!({"items": items})))
}

fn parse_dir_param(
    raw: Option<&str>,
) -> Result<LineageDirection, (StatusCode, Json<serde_json::Value>)> {
    raw.unwrap_or("both")
        .parse()
        .map_err(|e| err(StatusCode::BAD_REQUEST, e))
}

#[derive(Deserialize)]
struct DbTableLineageQuery {
    table: String,
    direction: Option<String>,
    hops: Option<u32>,
    all_history: Option<bool>,
}

async fn api_db_lineage_table(
    State(state): State<Arc<AppState>>,
    Query(p): Query<DbTableLineageQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let svc = require_db(&state).await?;
    let dir = parse_dir_param(p.direction.as_deref())?;
    let scope = QueryScope::from_all_history(p.all_history.unwrap_or(false));
    let res = svc
        .query_table_lineage(&p.table, dir, p.hops.unwrap_or(5), scope)
        .await
        .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(
        serde_json::to_value(res).unwrap_or_else(|_| json!({})),
    ))
}

async fn api_db_lineage_table_tasks(
    State(state): State<Arc<AppState>>,
    Query(p): Query<DbTableLineageQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let svc = require_db(&state).await?;
    let dir = parse_dir_param(p.direction.as_deref())?;
    let scope = QueryScope::from_all_history(p.all_history.unwrap_or(false));
    let res = svc
        .query_table_task_lineage(&p.table, dir, p.hops.unwrap_or(5), scope)
        .await
        .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(
        serde_json::to_value(res).unwrap_or_else(|_| json!({})),
    ))
}

#[derive(Deserialize)]
struct DbTaskLineageQuery {
    job: String,
    direction: Option<String>,
    hops: Option<u32>,
    all_history: Option<bool>,
}

async fn api_db_lineage_task(
    State(state): State<Arc<AppState>>,
    Query(p): Query<DbTaskLineageQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let svc = require_db(&state).await?;
    let dir = parse_dir_param(p.direction.as_deref())?;
    let scope = QueryScope::from_all_history(p.all_history.unwrap_or(false));
    let res = svc
        .query_task_lineage(&p.job, dir, p.hops.unwrap_or(5), scope)
        .await
        .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(
        serde_json::to_value(res).unwrap_or_else(|_| json!({})),
    ))
}

#[derive(Deserialize)]
struct DbColumnLineageQuery {
    db: Option<String>,
    table: Option<String>,
    column: Option<String>,
    field: Option<String>,
    direction: Option<String>,
    hops: Option<u32>,
    all_history: Option<bool>,
}

async fn api_db_lineage_column(
    State(state): State<Arc<AppState>>,
    Query(p): Query<DbColumnLineageQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let svc = require_db(&state).await?;
    let dir = parse_dir_param(p.direction.as_deref())?;
    let scope = QueryScope::from_all_history(p.all_history.unwrap_or(false));
    let res = svc
        .query_column_lineage(
            p.db.as_deref(),
            p.table.as_deref(),
            p.column.as_deref(),
            p.field.as_deref(),
            dir,
            p.hops.unwrap_or(5),
            scope,
        )
        .await
        .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(
        serde_json::to_value(res).unwrap_or_else(|_| json!({})),
    ))
}

pub async fn run(
    host: &str,
    port: u16,
    preload: Vec<InputFile>,
    default_db: Option<String>,
) -> anyhow::Result<()> {
    run_with_dialect(host, port, preload, default_db, SqlDialect::Hive).await
}

pub async fn run_with_dialect(
    host: &str,
    port: u16,
    preload: Vec<InputFile>,
    default_db: Option<String>,
    dialect: SqlDialect,
) -> anyhow::Result<()> {
    run_with_options(host, port, preload, default_db, dialect, None).await
}

pub async fn run_with_options(
    host: &str,
    port: u16,
    preload: Vec<InputFile>,
    default_db: Option<String>,
    dialect: SqlDialect,
    db_cfg: Option<DbConfig>,
) -> anyhow::Result<()> {
    let initial_db = match db_cfg {
        Some(cfg) => {
            let svc = LineageService::connect(cfg).await?;
            eprintln!(
                "[db] 已连接 {} ({}) hot_index={}",
                svc.backend_name(),
                svc.masked_url(),
                svc.hot_index_enabled()
            );
            Some(Arc::new(svc))
        }
        None => None,
    };
    let state = Arc::new(AppState {
        sessions: Mutex::new(HashMap::default()),
        seq: AtomicU64::new(0),
        default_dialect: dialect,
        db: tokio::sync::RwLock::new(initial_db),
    });
    if !preload.is_empty() {
        let names: Vec<String> = preload.iter().map(|f| f.name.clone()).collect();
        let state2 = state.clone();
        let db = default_db.clone();
        tokio::task::spawn_blocking(move || match run_analysis(preload, db, None, dialect) {
            Ok(g) => {
                let sid = store_session(&state2, g);
                eprintln!("[preload] session={sid} files={names:?}");
            }
            Err(e) => eprintln!("[preload] 失败: {e}"),
        });
    }
    let app = Router::new()
        .route("/", get(index))
        .route("/api/health", get(health))
        .route("/api/analyze", post(api_analyze_upload))
        .route("/api/analyze_text", post(api_analyze_text))
        .route("/api/s/{id}/overview", get(api_overview))
        .route("/api/s/{id}/tables", get(api_tables))
        .route("/api/s/{id}/subgraph", get(api_subgraph))
        .route("/api/s/{id}/statements", get(api_statements))
        .route("/api/s/{id}/names", get(api_table_names))
        .route("/api/s/{id}/table/{tid}", get(api_table_detail))
        .route("/api/s/{id}/table_by_name/{name}", get(api_lookup_table))
        .route("/api/s/{id}/export", get(api_export))
        .route("/api/s/{id}/save", post(api_save_session))
        .route("/api/db/status", get(api_db_status))
        .route("/api/db/config", post(api_db_config))
        .route("/api/db/ddl", get(api_db_ddl))
        .route("/api/db/jobs", get(api_db_jobs))
        .route("/api/db/tables", get(api_db_tables))
        .route("/api/db/lineage/table", get(api_db_lineage_table))
        .route(
            "/api/db/lineage/table_tasks",
            get(api_db_lineage_table_tasks),
        )
        .route("/api/db/lineage/task", get(api_db_lineage_task))
        .route("/api/db/lineage/column", get(api_db_lineage_column))
        .layer(DefaultBodyLimit::max(1024 * 1024 * 1024))
        .with_state(state);

    let addr: SocketAddr = format!("{host}:{port}").parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("hive-lineage serve 已启动: http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::FromRequest;
    use axum::http::Request;

    const SQL: &str = "CREATE TABLE dw.s (x int, y string);\n\
        INSERT INTO dw.t SELECT x, upper(y) AS u FROM dw.s;\n\
        bad stmt here;\n\
        SELECT x FROM dw.s;";

    fn st_with_dialect(dialect: SqlDialect) -> Arc<AppState> {
        Arc::new(AppState {
            sessions: Mutex::new(HashMap::default()),
            seq: AtomicU64::new(0),
            default_dialect: dialect,
            db: tokio::sync::RwLock::new(None),
        })
    }

    fn st() -> Arc<AppState> {
        st_with_dialect(SqlDialect::Hive)
    }

    fn g_of(sql: &str) -> SessionGraph {
        run_analysis(
            vec![InputFile {
                name: "t.sql".into(),
                bytes: sql.as_bytes().to_vec(),
            }],
            Some("dw".into()),
            Some(true),
            SqlDialect::Hive,
        )
        .unwrap()
    }

    async fn body_json(resp: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn select_flag() {
        assert_eq!(parse_select_flag(&Some("yes".into())), Some(true));
        assert_eq!(parse_select_flag(&Some("none".into())), Some(false));
        assert_eq!(parse_select_flag(&Some("auto".into())), None);
        assert_eq!(parse_select_flag(&None), None);
    }

    #[tokio::test]
    async fn session_handlers() {
        let state = st();
        let sid = store_session(&state, g_of(SQL));
        assert_eq!(sid, "s1");

        // overview：统计 / 文件 / 库 / 语句类型计数
        let ov = api_overview(State(state.clone()), Path(sid.clone()))
            .await
            .unwrap()
            .0;
        assert_eq!(ov["stats"]["statements"].as_u64().unwrap(), 4);
        assert_eq!(ov["files"][0]["name"], "t.sql");
        assert!(ov["dbs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["db"] == "dw"));
        assert_eq!(ov["stmt_kinds"]["insert"].as_u64().unwrap(), 1);
        assert_eq!(ov["counts"]["tables"].as_u64().unwrap(), 3);

        // tables：全量 + db/role/q 过滤 + 分页
        let q = |p: TableQuery| Query(p);
        let all = api_tables(
            State(state.clone()),
            Path(sid.clone()),
            q(TableQuery {
                q: None,
                role: None,
                db: None,
                offset: None,
                limit: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(all["total"].as_u64().unwrap(), 3); // dw.s, dw.t, query_result
        let filt = api_tables(
            State(state.clone()),
            Path(sid.clone()),
            q(TableQuery {
                q: Some("dw.t".into()),
                role: None,
                db: Some("dw".into()),
                offset: None,
                limit: Some(1),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(filt["items"].as_array().unwrap().len(), 1);
        let mid = api_tables(
            State(state.clone()),
            Path(sid.clone()),
            q(TableQuery {
                q: None,
                role: Some("target".into()),
                db: None,
                offset: None,
                limit: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(mid["total"].as_u64().unwrap() >= 1);

        // names 反查
        let names = api_table_names(
            State(state.clone()),
            Path(sid.clone()),
            Query(NamesQuery {
                ids: Some("0,1".into()),
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(names["names"]["0"].is_string());

        // subgraph：表跳子图（双向/上游）+ 列级追踪
        let sg = api_subgraph(
            State(state.clone()),
            Path(sid.clone()),
            Query(SubgraphQuery {
                table: 1,
                hops: Some(2),
                direction: Some("both".into()),
                col: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(sg["tables"].as_array().unwrap().len() >= 2);
        assert!(!sg["edges"].as_array().unwrap().is_empty());
        let sg_up = api_subgraph(
            State(state.clone()),
            Path(sid.clone()),
            Query(SubgraphQuery {
                table: 1,
                hops: None,
                direction: Some("up".into()),
                col: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(!sg_up["tables"].as_array().unwrap().is_empty());
        let trace = api_subgraph(
            State(state.clone()),
            Path(sid.clone()),
            Query(SubgraphQuery {
                table: 1,
                hops: None,
                direction: None,
                col: Some("x".into()),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(trace["focus"]["col"], "x");
        assert!(!trace["col_edges"].as_array().unwrap().is_empty());

        // statements：ids 直取 / 条件过滤 / 分页
        let got = api_statements(
            State(state.clone()),
            Path(sid.clone()),
            Query(StmtQuery {
                ids: Some("1,2".into()),
                offset: None,
                limit: None,
                status: None,
                kind: None,
            }),
        )
        .await
        .unwrap()
        .0;
        let items = got["items"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        assert!(items[0]["sql"].as_str().unwrap().starts_with("INSERT"));
        let errs = api_statements(
            State(state.clone()),
            Path(sid.clone()),
            Query(StmtQuery {
                ids: None,
                offset: Some(0),
                limit: Some(10),
                status: Some("error".into()),
                kind: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(errs["total"].as_u64().unwrap(), 1);

        // table_detail：含上下游离接表
        let det = api_table_detail(State(state.clone()), Path((sid.clone(), 1)))
            .await
            .unwrap()
            .0;
        assert!(det["up"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["name"] == "dw.s"));
        assert!(
            api_table_detail(State(state.clone()), Path((sid.clone(), 99)))
                .await
                .is_err()
        );

        // table_by_name
        let lk = api_lookup_table(State(state.clone()), Path((sid.clone(), "dw.t".into())))
            .await
            .unwrap()
            .0;
        assert_eq!(lk["name"], "dw.t");
        assert!(
            api_lookup_table(State(state.clone()), Path((sid.clone(), "nope".into())),)
                .await
                .is_err()
        );

        // export：Content-Disposition + 完整 JSON
        let resp = api_export(State(state.clone()), Path(sid.clone()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let cd = resp.headers()[header::CONTENT_DISPOSITION]
            .to_str()
            .unwrap()
            .to_string();
        assert!(cd.contains("lineage_s1.json"));
        let v = body_json(resp).await;
        assert!(v["stats"]["statements"].as_u64().unwrap() == 4);

        // 不存在的会话
        let e = api_overview(State(state.clone()), Path("s404".into())).await;
        assert!(e.is_err());

        // subgraph 表 id 越界
        let e2 = api_subgraph(
            State(state.clone()),
            Path(sid.clone()),
            Query(SubgraphQuery {
                table: 999,
                hops: None,
                direction: None,
                col: None,
            }),
        )
        .await;
        assert!(e2.is_err());
    }

    #[tokio::test]
    async fn analyze_endpoints() {
        let state = st();
        // 文本分析
        let resp = api_analyze_text(
            State(state.clone()),
            Json(TextAnalyzeBody {
                sql: SQL.into(),
                name: None,
                default_db: Some("dw".into()),
                analyze_select: Some("yes".into()),
                dialect: None,
                job_name: None,
                save_db: None,
            }),
        )
        .await;
        let v = body_json(resp).await;
        let sid = v["session"].as_str().unwrap().to_string();
        assert_eq!(v["counts"]["tables"].as_u64().unwrap(), 3);
        // 小图全量 initial
        assert_eq!(v["initial"]["full"], true);
        assert!(v["initial"]["focus_table"].is_null());

        // 全空白 SQL：合法空分析（无语句、无表）
        let resp = api_analyze_text(
            State(state.clone()),
            Json(TextAnalyzeBody {
                sql: "   ".into(),
                name: None,
                default_db: None,
                analyze_select: None,
                dialect: None,
                job_name: None,
                save_db: None,
            }),
        )
        .await;
        let v = body_json(resp).await;
        assert_eq!(v["counts"]["tables"], 0);

        // 无任何文件字段的 multipart -> 400 "未提供 SQL 内容"
        let boundary = "EMPTYBND";
        let mp_body = format!("--{boundary}--\r\n");
        let req = Request::builder()
            .method("POST")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(mp_body))
            .unwrap();
        let mp = Multipart::from_request(req, &()).await.unwrap();
        let resp = api_analyze_upload(State(state.clone()), mp).await;
        let v = body_json(resp).await;
        assert!(v["error"].is_string());

        // multipart 上传
        let boundary = "TESTBND";
        let mp_body = format!(
            "--{b}\r\nContent-Disposition: form-data; name=\"default_db\"\r\n\r\ndw\r\n\
             --{b}\r\nContent-Disposition: form-data; name=\"analyze_select\"\r\n\r\nyes\r\n\
             --{b}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a.sql\"\r\n\
             Content-Type: text/plain\r\n\r\n{sql}\r\n--{b}--\r\n",
            b = boundary,
            sql = SQL
        );
        let req = Request::builder()
            .method("POST")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(mp_body))
            .unwrap();
        let mp = Multipart::from_request(req, &()).await.unwrap();
        let resp = api_analyze_upload(State(state.clone()), mp).await;
        let v = body_json(resp).await;
        assert!(v["session"].is_string());
        assert_ne!(v["session"], sid); // 新会话

        // 大图（>60 表）initial 聚焦写入扇入最高目标
        let mut big = String::new();
        for i in 0..65 {
            big.push_str(&format!("INSERT INTO dw.t{i} SELECT x, y FROM ods.s{i};\n"));
        }
        let resp = api_analyze_text(
            State(state.clone()),
            Json(TextAnalyzeBody {
                sql: big,
                name: Some("big.sql".into()),
                default_db: None,
                analyze_select: None,
                dialect: None,
                job_name: None,
                save_db: None,
            }),
        )
        .await;
        let v = body_json(resp).await;
        assert_eq!(v["initial"]["full"], false);
        assert!(v["initial"]["focus_table"].is_number());

        // index / health
        let Html(h) = index().await;
        assert!(h.contains('<'));
        let Json(hj) = health().await;
        assert_eq!(hj["ok"], true);
    }

    #[tokio::test]
    async fn session_eviction() {
        let state = st();
        // 存满 MAX_SESSIONS(32) 后再存 1 个 -> 最早的 s1 被淘汰
        for _ in 0..33 {
            let sid = store_session(&state, g_of("SELECT x FROM ods.a;"));
            let _ = sid;
        }
        assert!(get_session(&state, "s1").is_err());
        assert!(get_session(&state, "s33").is_ok());
        assert_eq!(state.sessions.lock().unwrap().len(), 32);
    }

    #[tokio::test]
    async fn run_server() {
        // 正常路径：port=0 由系统分配，spawn 后中止（serve 永久等待）
        let h = tokio::spawn(run("127.0.0.1", 0, vec![], None));
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        h.abort();
        let _ = h.await;

        // 预加载路径：正常文件 + 会产生错误也不 panic
        let preload = vec![
            InputFile {
                name: "ok.sql".into(),
                bytes: b"INSERT INTO dw.t SELECT a FROM dw.s".to_vec(),
            },
            InputFile {
                name: "empty.sql".into(),
                bytes: vec![],
            },
        ];
        let h = tokio::spawn(run("127.0.0.1", 0, preload, Some("dw".into())));
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        h.abort();
        let _ = h.await;

        // 端口被占用 -> bind 失败返回 Err
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let r = run("127.0.0.1", port, vec![], None).await;
        assert!(r.is_err());
        drop(listener);

        // 非法地址 -> SocketAddr 解析失败
        let r = run("not a host!!", 0, vec![], None).await;
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn graph_cycles_and_diamond() {
        // 环：x->a->b->a，a 的已处理前驱 x 贡献 best 层（SessionGraph::build 环分支）
        let g = g_of(
            "INSERT INTO a SELECT FROM x; INSERT INTO b SELECT FROM a; INSERT INTO a SELECT FROM b;\n\
             INSERT INTO p SELECT FROM a; INSERT INTO q SELECT FROM a; INSERT INTO d SELECT FROM p JOIN q ON p.id=q.id",
        );
        let state = st();
        let sid = store_session(&state, g);
        // 下游菱形去重：p/q 均指到 d，d 二次入队命中 continue
        let a_tid = {
            let s = state.sessions.lock().unwrap();
            let g = s.get(&sid).unwrap();
            g.ws.result
                .tables
                .iter()
                .find(|t| t.name == "dw.a")
                .unwrap()
                .id
        };
        let sub = api_subgraph(
            State(state.clone()),
            Path(sid.clone()),
            Query(SubgraphQuery {
                table: a_tid,
                hops: Some(3),
                direction: Some("down".into()),
                col: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(sub["tables"].as_array().unwrap().len() >= 4);
        // up 方向
        let sub = api_subgraph(
            State(state.clone()),
            Path(sid.clone()),
            Query(SubgraphQuery {
                table: a_tid,
                hops: Some(3),
                direction: Some("up".into()),
                col: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(sub["tables"].as_array().unwrap().len() >= 3);
    }

    #[tokio::test]
    async fn column_trace_down() {
        // 列级下游追踪：从源列 (dw.s, x) 向下走到 dw.t
        let g = g_of("INSERT INTO dw.t SELECT x AS cx, upper(x) AS ux FROM dw.s");
        let state = st();
        let sid = store_session(&state, g);
        let s_tid = {
            let s = state.sessions.lock().unwrap();
            let g = s.get(&sid).unwrap();
            g.ws.result
                .tables
                .iter()
                .find(|t| t.name == "dw.s")
                .unwrap()
                .id
        };
        let sub = api_subgraph(
            State(state.clone()),
            Path(sid.clone()),
            Query(SubgraphQuery {
                table: s_tid,
                hops: None,
                direction: None,
                col: Some("x".into()),
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(sub["col_edges"].as_array().unwrap().len() >= 2);
    }

    #[tokio::test]
    async fn table_and_stmt_filters() {
        let g = g_of(SQL);
        let state = st();
        let sid = store_session(&state, g);
        let q = |p: TableQuery| Query(p);
        // q 不匹配 + role=result + 非法 role
        for (qq, role) in [
            (Some("nomatch".to_string()), None),
            (None, Some("result".to_string())),
            (None, Some("bogus".to_string())),
            (None, Some("mid".to_string())),
        ] {
            let _ = api_tables(
                State(state.clone()),
                Path(sid.clone()),
                q(TableQuery {
                    q: qq,
                    role,
                    db: None,
                    offset: None,
                    limit: None,
                }),
            )
            .await
            .unwrap()
            .0;
        }
        // statements：status/kind 过滤命中 continue
        for (st_, k) in [
            (Some("error".to_string()), None),
            (None, Some("set".to_string())),
            (Some("skipped".to_string()), Some("query".to_string())),
        ] {
            let _ = api_statements(
                State(state.clone()),
                Path(sid.clone()),
                Query(StmtQuery {
                    ids: None,
                    status: st_,
                    kind: k,
                    offset: Some(1),
                    limit: Some(1),
                }),
            )
            .await
            .unwrap()
            .0;
        }
    }

    #[tokio::test]
    async fn table_detail_source() {
        // 源表 detail：other 列列表分支
        let g = g_of("INSERT INTO dw.t SELECT x, upper(y) AS u FROM dw.s");
        let state = st();
        let sid = store_session(&state, g);
        let s_tid = {
            let s = state.sessions.lock().unwrap();
            let g = s.get(&sid).unwrap();
            g.ws.result
                .tables
                .iter()
                .find(|t| t.name == "dw.s")
                .unwrap()
                .id
        };
        let d = api_table_detail(State(state.clone()), Path((sid.clone(), s_tid)))
            .await
            .unwrap()
            .0;
        assert_eq!(d["table"]["name"], "dw.s");
    }

    #[tokio::test]
    async fn column_trace_self_edge() {
        // 自引用列边 t.a->t.a：up 遍历已入 edges，down 遍历命中 `!edges.insert` continue 臂
        let g = g_of("INSERT INTO dw.t SELECT a AS a FROM dw.t");
        let state = st();
        let sid = store_session(&state, g);
        let t_tid = {
            let s = state.sessions.lock().unwrap();
            let g = s.get(&sid).unwrap();
            g.ws.result
                .tables
                .iter()
                .find(|t| t.name == "dw.t")
                .unwrap()
                .id
        };
        let sub = api_subgraph(
            State(state.clone()),
            Path(sid.clone()),
            Query(SubgraphQuery {
                table: t_tid,
                hops: None,
                direction: None,
                col: Some("a".into()),
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(!sub["col_edges"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn table_detail_other_cols() {
        // 表已登记列但仅部分参与血缘 -> node_cols_json 的 other 列列表分支
        let g =
            g_of("CREATE TABLE dw.t (a INT, b INT, c INT); INSERT INTO dw.o SELECT a FROM dw.t");
        let state = st();
        let sid = store_session(&state, g);
        let t_tid = {
            let s = state.sessions.lock().unwrap();
            let g = s.get(&sid).unwrap();
            g.ws.result
                .tables
                .iter()
                .find(|t| t.name == "dw.t")
                .unwrap()
                .id
        };
        let d = api_table_detail(State(state.clone()), Path((sid.clone(), t_tid)))
            .await
            .unwrap()
            .0;
        assert_eq!(d["table"]["name"], "dw.t");
        assert_eq!(d["table"]["columns"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn upload_errors() {
        let state = st();
        // 空 multipart：files 为空 -> run_analysis Err -> BAD_REQUEST
        let body =
            "--b\r\nContent-Disposition: form-data; name=\"default_db\"\r\n\r\ndw\r\n--b--\r\n";
        let req = Request::builder()
            .header("content-type", "multipart/form-data; boundary=b")
            .body(Body::from(body))
            .unwrap();
        let mp = Multipart::from_request(req, &()).await.unwrap();
        let resp = api_analyze_upload(State(state.clone()), mp).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        // 文件名空 + 空文件字段（Ok(_) 跳过臂 + `_` 字段名臂）
        let body = "--b\r\nContent-Disposition: form-data; name=\"junk\"\r\n\r\n\r\n--b--\r\n";
        let req = Request::builder()
            .header("content-type", "multipart/form-data; boundary=b")
            .body(Body::from(body))
            .unwrap();
        let mp = Multipart::from_request(req, &()).await.unwrap();
        let resp = api_analyze_upload(State(state.clone()), mp).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        // 坏 boundary -> next_field Err
        let body = "--b\r\nContent-Disposition: form-data; name=\"f\"\r\n\r\nSELECT 1\r\n";
        let req = Request::builder()
            .header("content-type", "multipart/form-data; boundary=b")
            .body(Body::from(body))
            .unwrap();
        let mp = Multipart::from_request(req, &()).await.unwrap();
        let resp = api_analyze_upload(State(state.clone()), mp).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    const SPARK_CTAS: &str = "CREATE TABLE dw.t USING PARQUET AS SELECT id FROM ods.s";

    fn assert_spark_ctas_edge(g: &SessionGraph) {
        let r = &g.ws.result;
        let s = r.tables.iter().find(|t| t.name == "ods.s").unwrap().id;
        let t = r.tables.iter().find(|t| t.name == "dw.t").unwrap().id;
        assert!(r.column_edges.iter().any(|e| e.source_table == s
            && e.source_column == "id"
            && e.target_table == t
            && e.target_column == "id"
            && e.kind == "direct"));
    }

    fn dialect_cases() -> [(SqlDialect, Option<&'static str>, usize); 6] {
        [
            (SqlDialect::Hive, None, 0),
            (SqlDialect::Hive, Some("hive"), 0),
            (SqlDialect::Hive, Some("spark"), 1),
            (SqlDialect::Spark, None, 1),
            (SqlDialect::Spark, Some("hive"), 0),
            (SqlDialect::Spark, Some("spark"), 1),
        ]
    }

    #[tokio::test]
    async fn analyze_text_dialect_selection() {
        let missing: TextAnalyzeBody = serde_json::from_value(json!({"sql": SPARK_CTAS})).unwrap();
        assert!(missing.dialect.is_none());
        for (default, requested, expected) in dialect_cases() {
            let state = st_with_dialect(default);
            let body: TextAnalyzeBody =
                serde_json::from_value(json!({"sql": SPARK_CTAS, "dialect": requested})).unwrap();
            let resp = api_analyze_text(State(state.clone()), Json(body)).await;
            assert_eq!(resp.status(), StatusCode::OK);
            let v = body_json(resp).await;
            let sid = v["session"].as_str().unwrap().to_string();
            let g = get_session(&state, &sid).unwrap();
            assert_eq!(
                g.ws.result.stats.parsed, expected,
                "{default:?}/{requested:?}"
            );
            assert_eq!(g.ws.result.stats.fallback, 1 - expected);
            if expected == 1 {
                assert_spark_ctas_edge(&g);
            }
        }
    }

    #[tokio::test]
    async fn analyze_upload_dialect_selection() {
        for (default, requested, expected) in dialect_cases() {
            let state = st_with_dialect(default);
            let boundary = "sql-dialect-test";
            let mut mp_body = format!(
                "--{b}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"t.sql\"\r\n\
                 Content-Type: text/plain\r\n\r\n{sql}\r\n",
                b = boundary,
                sql = SPARK_CTAS
            );
            if let Some(d) = requested {
                mp_body.push_str(&format!(
                    "--{b}\r\nContent-Disposition: form-data; name=\"dialect\"\r\n\r\n{d}\r\n",
                    b = boundary
                ));
            }
            mp_body.push_str(&format!("--{boundary}--\r\n"));
            let req = Request::builder()
                .method("POST")
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(mp_body))
                .unwrap();
            let mp = Multipart::from_request(req, &()).await.unwrap();
            let resp = api_analyze_upload(State(state.clone()), mp).await;
            assert_eq!(resp.status(), StatusCode::OK);
            let v = body_json(resp).await;
            let sid = v["session"].as_str().unwrap().to_string();
            let g = get_session(&state, &sid).unwrap();
            assert_eq!(g.ws.result.stats.files, 1);
            assert_eq!(
                g.ws.result.stats.parsed, expected,
                "{default:?}/{requested:?}"
            );
            assert_eq!(g.ws.result.stats.fallback, 1 - expected);
            if expected == 1 {
                assert_spark_ctas_edge(&g);
            }
        }
    }

    #[tokio::test]
    async fn dialect_invalid_rejected() {
        for bad in ["", "mysql", "sparkk"] {
            let state = st();
            let body: TextAnalyzeBody =
                serde_json::from_value(json!({"sql": SPARK_CTAS, "dialect": bad})).unwrap();
            let resp = api_analyze_text(State(state.clone()), Json(body)).await;
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            let v = body_json(resp).await;
            let e = v["error"].as_str().unwrap();
            assert!(e.contains("hive") && e.contains("spark"));
            assert!(state.sessions.lock().unwrap().is_empty());

            let boundary = "sql-dialect-test";
            let mp_body = format!(
                "--{b}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"t.sql\"\r\n\
                 Content-Type: text/plain\r\n\r\n{sql}\r\n\
                 --{b}\r\nContent-Disposition: form-data; name=\"dialect\"\r\n\r\n{bad}\r\n\
                 --{b}--\r\n",
                b = boundary,
                sql = SPARK_CTAS
            );
            let req = Request::builder()
                .method("POST")
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(mp_body))
                .unwrap();
            let mp = Multipart::from_request(req, &()).await.unwrap();
            let resp = api_analyze_upload(State(state.clone()), mp).await;
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            let v = body_json(resp).await;
            let e = v["error"].as_str().unwrap();
            assert!(e.contains("hive") && e.contains("spark"));
            assert!(state.sessions.lock().unwrap().is_empty());
        }
    }

    // ---------- 数据库配置 / 落库 / 全局血缘查询 API ----------

    async fn st_with_mem_db() -> Arc<AppState> {
        let state = st();
        let svc = LineageService::connect(DbConfig {
            url: "memory://".into(),
            auto_create_table: true,
            use_hot_index: true,
        })
        .await
        .unwrap();
        *state.db.write().await = Some(Arc::new(svc));
        state
    }

    #[tokio::test]
    async fn db_status_and_config() {
        let state = st();
        // 未配置
        let Json(v) = api_db_status(State(state.clone())).await;
        assert_eq!(v["configured"], false);

        // 配置 memory://
        let Json(v) = api_db_config(
            State(state.clone()),
            Json(DbConfigBody {
                url: "memory://".into(),
                auto_create_table: None,
                use_hot_index: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(v["configured"], true);
        assert_eq!(v["backend"], "memory");
        assert_eq!(v["hot_index"], true);

        let Json(v) = api_db_status(State(state.clone())).await;
        assert_eq!(v["configured"], true);
        assert_eq!(v["ok"], true);

        // 空 url -> 400
        let r = api_db_config(
            State(st()),
            Json(DbConfigBody {
                url: "   ".into(),
                auto_create_table: None,
                use_hot_index: None,
            }),
        )
        .await;
        assert_eq!(r.unwrap_err().0, StatusCode::BAD_REQUEST);

        // ddl 输出
        let Json(v) = api_db_ddl().await;
        assert!(v["ddl"].as_str().unwrap().contains("hl_job_batches"));
    }

    #[tokio::test]
    async fn db_save_and_lineage_apis() {
        let state = st_with_mem_db().await;

        // 未配置 DB 时查询 -> 400
        let r = api_db_lineage_table(
            State(st()),
            Query(DbTableLineageQuery {
                table: "dwd.t".into(),
                direction: None,
                hops: None,
                all_history: None,
            }),
        )
        .await;
        assert_eq!(r.unwrap_err().0, StatusCode::BAD_REQUEST);

        // 保存两个任务的解析结果：上游 ods_load -> 下游 dws_agg 读 ods.orders
        let g1 = run_analysis_with_job(
            vec![InputFile {
                name: "a.sql".into(),
                bytes: b"INSERT INTO ods.orders SELECT id, amt FROM ext.raw_orders;".to_vec(),
            }],
            None,
            None,
            SqlDialect::Hive,
            Some("dag.ods_load".into()),
        )
        .unwrap();
        let sid1 = store_session(&state, g1);
        let Json(v) = api_save_session(
            State(state.clone()),
            Path(sid1.clone()),
            Json(SaveSessionBody {
                job_name: "dag.ods_load".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(v["saved"]["job_name"], "dag.ods_load");

        let g2 = run_analysis_with_job(
            vec![InputFile {
                name: "b.sql".into(),
                bytes: b"INSERT INTO dws.daily SELECT cust_id, sum(amt) AS gmv FROM ods.orders GROUP BY cust_id;".to_vec(),
            }],
            None,
            None,
            SqlDialect::Hive,
            Some("dag.dws_agg".into()),
        )
        .unwrap();
        let sid2 = store_session(&state, g2);
        let Json(_) = api_save_session(
            State(state.clone()),
            Path(sid2),
            Json(SaveSessionBody {
                job_name: "dag.dws_agg".into(),
            }),
        )
        .await
        .unwrap();

        // 空白 job_name -> 400
        let r = api_save_session(
            State(state.clone()),
            Path(sid1),
            Json(SaveSessionBody {
                job_name: "  ".into(),
            }),
        )
        .await;
        assert_eq!(r.unwrap_err().0, StatusCode::BAD_REQUEST);

        // jobs / tables 搜索
        let Json(v) = api_db_jobs(
            State(state.clone()),
            Query(DbSearchQuery {
                q: Some("dag".into()),
                limit: None,
                all_history: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(v["items"].as_array().unwrap().len(), 2);
        let Json(v) = api_db_tables(
            State(state.clone()),
            Query(DbSearchQuery {
                q: Some("ods".into()),
                limit: None,
                all_history: None,
            }),
        )
        .await
        .unwrap();
        assert!(v["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["full_table_name"] == "ods.orders"));

        // 表级血缘：dws.daily 上游含 ods.orders 与 ext.raw_orders
        let Json(v) = api_db_lineage_table(
            State(state.clone()),
            Query(DbTableLineageQuery {
                table: "dws.daily".into(),
                direction: Some("upstream".into()),
                hops: Some(5),
                all_history: None,
            }),
        )
        .await
        .unwrap();
        let ups: Vec<&str> = v["upstream_tables"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|t| t["full_table_name"].as_str())
            .collect();
        assert!(ups.contains(&"ods.orders"));
        assert!(ups.contains(&"ext.raw_orders"));
        assert_eq!(v["graph"]["mode"], "table");

        // 表的任务级血缘：ods.orders 产出 dag.ods_load、消费 dag.dws_agg
        let Json(v) = api_db_lineage_table_tasks(
            State(state.clone()),
            Query(DbTableLineageQuery {
                table: "ods.orders".into(),
                direction: None,
                hops: None,
                all_history: None,
            }),
        )
        .await
        .unwrap();
        assert!(v["producer_jobs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|j| j == "dag.ods_load"));
        assert!(v["consumer_jobs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|j| j == "dag.dws_agg"));
        assert_eq!(v["graph"]["mode"], "task");

        // 任务链路：dag.ods_load 下游含 dag.dws_agg
        let Json(v) = api_db_lineage_task(
            State(state.clone()),
            Query(DbTaskLineageQuery {
                job: "dag.ods_load".into(),
                direction: None,
                hops: None,
                all_history: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(v["matched_job"], "dag.ods_load");
        assert!(v["downstream_jobs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|j| j["job_name"] == "dag.dws_agg"));

        // 字段级血缘：dws.daily.gmv 上游含 ods.orders.amt（sum -> aggregate）
        let Json(v) = api_db_lineage_column(
            State(state.clone()),
            Query(DbColumnLineageQuery {
                db: None,
                table: None,
                column: None,
                field: Some("dws.daily.gmv".into()),
                direction: Some("upstream".into()),
                hops: None,
                all_history: None,
            }),
        )
        .await
        .unwrap();
        let ups: Vec<&str> = v["upstream_columns"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|c| c["qualified_column"].as_str())
            .collect();
        assert!(ups.contains(&"ods.orders.amt"));

        // 非法 direction -> 400
        let r = api_db_lineage_table(
            State(state.clone()),
            Query(DbTableLineageQuery {
                table: "dws.daily".into(),
                direction: Some("sideways".into()),
                hops: None,
                all_history: None,
            }),
        )
        .await;
        assert_eq!(r.unwrap_err().0, StatusCode::BAD_REQUEST);

        // 空字段 -> 400
        let r = api_db_lineage_column(
            State(state.clone()),
            Query(DbColumnLineageQuery {
                db: None,
                table: None,
                column: None,
                field: None,
                direction: None,
                hops: None,
                all_history: None,
            }),
        )
        .await;
        assert_eq!(r.unwrap_err().0, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn analyze_text_with_save_db() {
        let state = st_with_mem_db().await;

        // save_db=true 但无 job_name -> 400
        let resp = api_analyze_text(
            State(state.clone()),
            Json(TextAnalyzeBody {
                sql: "INSERT INTO dw.t SELECT a FROM dw.s".into(),
                name: None,
                default_db: None,
                analyze_select: None,
                dialect: None,
                job_name: None,
                save_db: Some(true),
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // save_db + job_name -> saved 摘要
        let resp = api_analyze_text(
            State(state.clone()),
            Json(TextAnalyzeBody {
                sql: "INSERT INTO dw.t SELECT a FROM dw.s".into(),
                name: None,
                default_db: None,
                analyze_select: None,
                dialect: None,
                job_name: Some("etl.task_x".into()),
                save_db: Some(true),
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let v = body_json(resp).await;
        assert_eq!(v["job_name"], "etl.task_x");
        assert_eq!(v["saved"]["job_name"], "etl.task_x");
        assert_eq!(v["saved"]["dag_name"], "etl");

        // 未配置 DB + save_db -> 400
        let bare = st();
        let resp = api_analyze_text(
            State(bare),
            Json(TextAnalyzeBody {
                sql: "INSERT INTO dw.t SELECT a FROM dw.s".into(),
                name: None,
                default_db: None,
                analyze_select: None,
                dialect: None,
                job_name: Some("etl.x".into()),
                save_db: Some(true),
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn server_edge_cases() {
        assert_eq!(parse_select_flag(&Some(" YES ".into())), Some(true));
        assert_eq!(parse_select_flag(&Some("False".into())), Some(false));

        // 超过 MAX_SESSIONS 时按数字序号淘汰 s1/s2/s3，保留 s10
        let state = st();
        for _ in 0..(MAX_SESSIONS + 3) {
            store_session(&state, g_of("SELECT a FROM dw.s"));
        }
        {
            let map = state.sessions.lock().unwrap();
            assert_eq!(map.len(), MAX_SESSIONS);
            assert!(!map.contains_key("s1"));
            assert!(!map.contains_key("s2"));
            assert!(!map.contains_key("s3"));
            assert!(map.contains_key("s10"));
            assert!(map.contains_key(&format!("s{}", MAX_SESSIONS + 3)));
        }

        // 中文语句超过 200 字节但不足 200 字符 -> truncated=false；且大小写查表命中
        let zh = "中".repeat(80);
        let sql_zh = format!("INSERT INTO dw.t SELECT '{zh}' AS note FROM dw.s");
        assert!(sql_zh.len() > 200 && sql_zh.chars().count() < 200);
        let sid = store_session(&state, g_of(&sql_zh));
        let stmts = api_statements(
            State(state.clone()),
            Path(sid.clone()),
            Query(StmtQuery {
                ids: None,
                offset: None,
                limit: None,
                status: None,
                kind: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(stmts["items"][0]["truncated"], false);

        let looked = api_lookup_table(State(state), Path((sid, "DW.T".into())))
            .await
            .unwrap()
            .0;
        assert_eq!(looked["name"], "dw.t");
    }
}
