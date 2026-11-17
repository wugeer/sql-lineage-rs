//! 入库批次数据抽取器：规范化库表/任务名、分配单调递增批次号、消解任务内临时表伪边

use crate::db::{BatchPayload, BatchRow, ColumnEdgeRow, JobTableRow, TableEdgeRow};
use crate::{AnalyzedWorkspace, SqlDialect};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static LAST_BATCH_ID: AtomicI64 = AtomicI64::new(0);

/// 生成进程内严格单调递增的微秒级 `batch_id`
pub fn next_batch_id() -> i64 {
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(1);
    let mut prev = LAST_BATCH_ID.load(Ordering::Relaxed);
    loop {
        let candidate = now_us.max(prev + 1);
        match LAST_BATCH_ID.compare_exchange_weak(
            prev,
            candidate,
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) {
            Ok(_) => return candidate,
            Err(actual) => prev = actual,
        }
    }
}

/// 清洗单个标识符段（去除空白与反引号/引号并转小写）
pub fn clean_segment(raw: &str) -> String {
    raw.trim()
        .trim_matches(|c| c == '`' || c == '"' || c == '\'')
        .trim()
        .to_lowercase()
}

/// 拆分并规范化 `库名.表名` -> `(db_name, table_name, full_table_name)`
pub fn split_db_table(raw: &str) -> (String, String, String) {
    let trimmed = raw.trim();
    if trimmed.starts_with("dir:") || trimmed.starts_with("result:") {
        return (String::new(), trimmed.to_string(), trimmed.to_string());
    }
    let segs: Vec<String> = trimmed
        .split('.')
        .map(clean_segment)
        .filter(|s| !s.is_empty())
        .collect();
    if segs.is_empty() {
        return (String::new(), String::new(), String::new());
    }
    if segs.len() == 1 {
        let tbl = segs[0].clone();
        return (String::new(), tbl.clone(), tbl);
    }
    let full = segs.join(".");
    let idx = full.rfind('.').unwrap();
    (full[..idx].to_string(), full[idx + 1..].to_string(), full)
}

/// 校验并拆分 `job_name`（如 `dag.task`）-> `(normalized_job_name, dag_name, task_name)`
pub fn split_job_name(raw: &str) -> anyhow::Result<(String, String, String)> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("job_name 不能为空（示例：dag.task）");
    }
    if let Some(idx) = trimmed.rfind('.') {
        let dag = trimmed[..idx].trim();
        let task = trimmed[idx + 1..].trim();
        if task.is_empty() {
            anyhow::bail!("job_name '{trimmed}' 格式非法，任务名部分不能为空");
        }
        let norm = if dag.is_empty() {
            task.to_string()
        } else {
            format!("{dag}.{task}")
        };
        Ok((norm, dag.to_string(), task.to_string()))
    } else {
        Ok((trimmed.to_string(), String::new(), trimmed.to_string()))
    }
}

/// 解析三段式字段输入（支持独立 `db` + `table` + `column` 或合并的 `库名.表名.字段名`）
/// 返回 `(db_name, table_name, full_table_name, column_name)`
pub fn parse_qualified_column(
    db: Option<&str>,
    table: Option<&str>,
    column: Option<&str>,
    combined: Option<&str>,
) -> anyhow::Result<(String, String, String, String)> {
    if let Some(raw_field) = combined.map(str::trim).filter(|s| !s.is_empty()) {
        let segs: Vec<String> = raw_field
            .split('.')
            .map(clean_segment)
            .filter(|s| !s.is_empty())
            .collect();
        if segs.len() < 2 {
            anyhow::bail!("字段标识 '{raw_field}' 格式非法，需为 库名.表名.字段名 或 表名.字段名");
        }
        let col = segs.last().unwrap().clone();
        let tbl_raw = segs[..segs.len() - 1].join(".");
        let (d, t, full) = split_db_table(&tbl_raw);
        return Ok((d, t, full, col));
    }

    let col = clean_segment(column.unwrap_or(""));
    if col.is_empty() {
        anyhow::bail!("字段名（column）不能为空");
    }
    let tbl_in = table.unwrap_or("").trim();
    if tbl_in.is_empty() {
        anyhow::bail!("表名（table）不能为空");
    }
    let db_in = clean_segment(db.unwrap_or(""));
    let combined_tbl = if !db_in.is_empty() && !tbl_in.contains('.') {
        format!("{db_in}.{tbl_in}")
    } else {
        tbl_in.to_string()
    };
    let (d, t, full) = split_db_table(&combined_tbl);
    if full.is_empty() {
        anyhow::bail!("表名不能为空");
    }
    Ok((d, t, full, col))
}

/// 格式化 `库名.表名.字段名`
pub fn format_qualified_column(full_table_name: &str, column_name: &str) -> String {
    format!("{full_table_name}.{column_name}")
}

fn compose_edge_kind(a: &str, b: &str) -> &'static str {
    if a == "aggregate" || b == "aggregate" {
        "aggregate"
    } else if a == "derived" || b == "derived" {
        "derived"
    } else {
        "direct"
    }
}

fn kind_rank(k: &str) -> u8 {
    match k {
        "aggregate" => 3,
        "derived" => 2,
        _ => 1,
    }
}

fn clamp_chars(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        s.chars().take(max_chars).collect()
    }
}

fn dialect_str(d: SqlDialect) -> &'static str {
    match d {
        SqlDialect::Hive => "hive",
        SqlDialect::Spark => "spark",
    }
}

fn build_sql_preview(ws: &AnalyzedWorkspace, stmt_indices: &[usize]) -> String {
    const MAX_PREVIEW_CHARS: usize = 500;
    let mut out = String::new();
    for &idx in stmt_indices.iter().take(3) {
        if let Some(st) = ws.result.statements.get(idx) {
            if let Some(src) = ws.sources.get(st.file) {
                if let Some(seg) = src.get(st.start..st.end) {
                    let trimmed = seg.trim();
                    if !trimmed.is_empty() {
                        if !out.is_empty() {
                            out.push_str(" ;\n");
                        }
                        out.push_str(trimmed);
                    }
                }
            }
        }
    }
    if out.chars().count() > MAX_PREVIEW_CHARS {
        let mut truncated: String = out.chars().take(MAX_PREVIEW_CHARS).collect();
        truncated.push_str("...");
        truncated
    } else {
        out
    }
}

/// 将单次 SQL 解析产物转换为可纯追加落库的 `BatchPayload`
///
/// 关键处理：
/// 1. 临时表（`kind == "temp"`）仅在当前脚本会话内有效，穿透折叠为物理表对物理表的表边与字段边，避免跨任务同名临时表污染。
/// 2. 独立查询结果虚拟表（`kind == "result"`）限定在 `result:{job_name}:{name}` 命名空间下，不参与跨任务表连接。
pub fn extract_batch_payload(
    job_name: &str,
    dialect: SqlDialect,
    default_db: Option<&str>,
    ws: &AnalyzedWorkspace,
) -> anyhow::Result<BatchPayload> {
    let (norm_job, dag_name, task_name) = split_job_name(job_name)?;
    let batch_id = next_batch_id();
    let ingested_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(batch_id / 1000);

    let r = &ws.result;
    let n = r.tables.len();
    let is_temp: Vec<bool> = r.tables.iter().map(|t| t.kind == "temp").collect();

    // 每个表 ID 映射为入库规范表元组 (db, tbl, full_name)
    let mapped_names: Vec<(String, String, String)> = r
        .tables
        .iter()
        .map(|t| {
            if t.kind == "result" {
                let full = format!("result:{norm_job}:{}", t.name);
                (String::new(), t.name.clone(), full)
            } else {
                split_db_table(&t.name)
            }
        })
        .collect();

    // 1. 构建物理/持久化表列表（剔除仅脚本内存活的 temp 表）
    // 同时在折叠临时表后修正源/目标角色标记
    let mut collapsed_table_edges: HashMap<(usize, usize), Vec<usize>> = HashMap::default();
    let mut t_adj: Vec<Vec<(usize, &[usize])>> = vec![Vec::new(); n];
    for e in &r.table_edges {
        if e.source < n && e.target < n {
            t_adj[e.source].push((e.target, e.statements.as_slice()));
        }
    }

    for src in 0..n {
        if is_temp[src] {
            continue;
        }
        // 直接物理边 + 经一个或多个 temp 节点中转到达的物理目标表
        let mut q: VecDeque<(usize, Vec<usize>)> = VecDeque::new();
        let mut visited_temp: HashSet<usize> = HashSet::default();
        for &(nxt, stmts) in &t_adj[src] {
            if is_temp[nxt] {
                if visited_temp.insert(nxt) {
                    q.push_back((nxt, stmts.to_vec()));
                }
            } else {
                let entry = collapsed_table_edges.entry((src, nxt)).or_default();
                for &si in stmts {
                    if !entry.contains(&si) {
                        entry.push(si);
                    }
                }
            }
        }
        while let Some((cur_tmp, acc_stmts)) = q.pop_front() {
            for &(nxt, stmts) in &t_adj[cur_tmp] {
                let mut merged = acc_stmts.clone();
                for &si in stmts {
                    if !merged.contains(&si) {
                        merged.push(si);
                    }
                }
                if is_temp[nxt] {
                    if visited_temp.insert(nxt) {
                        q.push_back((nxt, merged));
                    }
                } else {
                    let entry = collapsed_table_edges.entry((src, nxt)).or_default();
                    for si in merged {
                        if !entry.contains(&si) {
                            entry.push(si);
                        }
                    }
                }
            }
        }
    }

    // 2. 穿透 temp 节点折叠字段级血缘边
    // key: (src_tid, src_col, dst_tid, dst_col) -> (best_kind, stmts)
    type ColEdgeKey = (usize, String, usize, String);
    let mut collapsed_col_edges: HashMap<ColEdgeKey, (&'static str, Vec<usize>)> =
        HashMap::default();
    let mut c_adj: HashMap<usize, Vec<&crate::model::ColumnEdge>> = HashMap::default();
    for ce in &r.column_edges {
        c_adj.entry(ce.source_table).or_default().push(ce);
    }

    for ce in &r.column_edges {
        if ce.source_table >= n || ce.target_table >= n || is_temp[ce.source_table] {
            continue;
        }
        let base_kind = compose_edge_kind(ce.kind, "direct");
        if !is_temp[ce.target_table] {
            let key = (
                ce.source_table,
                ce.source_column.clone(),
                ce.target_table,
                ce.target_column.clone(),
            );
            let entry = collapsed_col_edges
                .entry(key)
                .or_insert((base_kind, Vec::new()));
            if kind_rank(base_kind) > kind_rank(entry.0) {
                entry.0 = base_kind;
            }
            for &si in &ce.statements {
                if !entry.1.contains(&si) {
                    entry.1.push(si);
                }
            }
            continue;
        }

        // 目标是 temp 表：继续沿字段匹配（含 `*` 通配）向下游穿透
        let mut cq: VecDeque<(usize, String, &'static str, Vec<usize>)> = VecDeque::new();
        let mut seen_tmp_col: HashSet<(usize, String)> = HashSet::default();
        seen_tmp_col.insert((ce.target_table, ce.target_column.clone()));
        cq.push_back((
            ce.target_table,
            ce.target_column.clone(),
            base_kind,
            ce.statements.clone(),
        ));

        while let Some((tmp_tid, tmp_col, cur_kind, cur_stmts)) = cq.pop_front() {
            let Some(outs) = c_adj.get(&tmp_tid) else {
                continue;
            };
            for next_ce in outs {
                if next_ce.source_column != tmp_col
                    && next_ce.source_column != "*"
                    && tmp_col != "*"
                {
                    continue;
                }
                let next_kind = compose_edge_kind(cur_kind, next_ce.kind);
                let mut merged_stmts = cur_stmts.clone();
                for &si in &next_ce.statements {
                    if !merged_stmts.contains(&si) {
                        merged_stmts.push(si);
                    }
                }
                if is_temp[next_ce.target_table] {
                    if seen_tmp_col.insert((next_ce.target_table, next_ce.target_column.clone())) {
                        cq.push_back((
                            next_ce.target_table,
                            next_ce.target_column.clone(),
                            next_kind,
                            merged_stmts,
                        ));
                    }
                } else if next_ce.target_table != ce.source_table {
                    let key = (
                        ce.source_table,
                        ce.source_column.clone(),
                        next_ce.target_table,
                        next_ce.target_column.clone(),
                    );
                    let entry = collapsed_col_edges
                        .entry(key)
                        .or_insert((next_kind, Vec::new()));
                    if kind_rank(next_kind) > kind_rank(entry.0) {
                        entry.0 = next_kind;
                    }
                    for si in merged_stmts {
                        if !entry.1.contains(&si) {
                            entry.1.push(si);
                        }
                    }
                }
            }
        }
    }

    // 3. 构造有序去重的表列表、表边列表与字段边列表
    let mut eff_src: Vec<bool> = r.tables.iter().map(|t| t.is_source).collect();
    let mut eff_tgt: Vec<bool> = r.tables.iter().map(|t| t.is_target).collect();
    for &(s, d) in collapsed_table_edges.keys() {
        eff_src[s] = true;
        eff_tgt[d] = true;
    }

    let mut job_tables = Vec::new();
    for (tid, t) in r.tables.iter().enumerate() {
        if is_temp[tid] {
            continue;
        }
        let (db_name, table_name, full_table_name) = &mapped_names[tid];
        if full_table_name.is_empty() {
            continue;
        }
        job_tables.push(JobTableRow {
            job_name: clamp_chars(&norm_job, 255),
            batch_id,
            db_name: clamp_chars(db_name, 128),
            table_name: clamp_chars(table_name, 255),
            full_table_name: clamp_chars(full_table_name, 384),
            table_kind: t.kind.to_string(),
            is_source: eff_src[tid],
            is_target: eff_tgt[tid],
            columns: t.columns.clone(),
        });
    }

    let mut sorted_tedges: Vec<((usize, usize), Vec<usize>)> =
        collapsed_table_edges.into_iter().collect();
    sorted_tedges.sort_by_key(|&((s, d), _)| (s, d));

    let mut table_edges = Vec::with_capacity(sorted_tedges.len());
    for ((s, d), mut stmts) in sorted_tedges {
        stmts.sort_unstable();
        let (sdb, stbl, sfull) = &mapped_names[s];
        let (tdb, ttbl, tfull) = &mapped_names[d];
        if sfull.is_empty() || tfull.is_empty() {
            continue;
        }
        let sql_preview = build_sql_preview(ws, &stmts);
        table_edges.push(TableEdgeRow {
            job_name: clamp_chars(&norm_job, 255),
            batch_id,
            source_db: clamp_chars(sdb, 128),
            source_table: clamp_chars(stbl, 255),
            source_full_name: clamp_chars(sfull, 384),
            target_db: clamp_chars(tdb, 128),
            target_table: clamp_chars(ttbl, 255),
            target_full_name: clamp_chars(tfull, 384),
            stmt_indices: stmts,
            sql_preview,
        });
    }

    let mut sorted_cedges: Vec<_> = collapsed_col_edges.into_iter().collect();
    sorted_cedges.sort_by(|a, b| a.0.cmp(&b.0));

    let mut column_edges = Vec::with_capacity(sorted_cedges.len());
    for ((s, scol, d, dcol), (ekind, mut stmts)) in sorted_cedges {
        stmts.sort_unstable();
        let (sdb, stbl, sfull) = &mapped_names[s];
        let (tdb, ttbl, tfull) = &mapped_names[d];
        if sfull.is_empty() || tfull.is_empty() {
            continue;
        }
        column_edges.push(ColumnEdgeRow {
            job_name: clamp_chars(&norm_job, 255),
            batch_id,
            source_db: clamp_chars(sdb, 128),
            source_table: clamp_chars(stbl, 255),
            source_full_name: clamp_chars(sfull, 384),
            source_column: clamp_chars(&scol, 255),
            target_db: clamp_chars(tdb, 128),
            target_table: clamp_chars(ttbl, 255),
            target_full_name: clamp_chars(tfull, 384),
            target_column: clamp_chars(&dcol, 255),
            edge_kind: ekind.to_string(),
            stmt_indices: stmts,
        });
    }

    let files_json = serde_json::to_string(&r.files.iter().map(|f| &f.name).collect::<Vec<_>>())
        .unwrap_or_else(|_| "[]".to_string());

    let batch = BatchRow {
        job_name: clamp_chars(&norm_job, 255),
        batch_id,
        dag_name: clamp_chars(&dag_name, 255),
        task_name: clamp_chars(&task_name, 255),
        dialect: dialect_str(dialect).to_string(),
        default_db: clamp_chars(default_db.unwrap_or("").trim(), 128).to_lowercase(),
        files_json,
        statement_count: r.stats.statements as i32,
        parsed_count: r.stats.parsed as i32,
        fallback_count: r.stats.fallback as i32,
        failed_count: r.stats.failed as i32,
        table_count: job_tables.len() as i32,
        table_edge_count: table_edges.len() as i32,
        column_edge_count: column_edges.len() as i32,
        ingested_at_ms,
    };

    Ok(BatchPayload {
        batch,
        job_tables,
        table_edges,
        column_edges,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{analyze_with_sources, InputFile, Options};

    #[test]
    fn split_helpers_work() {
        assert_eq!(
            split_db_table("`ODS`.`Orders`"),
            ("ods".into(), "orders".into(), "ods.orders".into())
        );
        assert_eq!(
            split_db_table("raw_tbl"),
            ("".into(), "raw_tbl".into(), "raw_tbl".into())
        );
        assert_eq!(
            split_job_name("dw_dag.task_order").unwrap(),
            (
                "dw_dag.task_order".into(),
                "dw_dag".into(),
                "task_order".into()
            )
        );
        assert!(split_job_name("   ").is_err());
        assert_eq!(
            parse_qualified_column(Some("DW"), Some("Order_D"), Some("Amt"), None).unwrap(),
            (
                "dw".into(),
                "order_d".into(),
                "dw.order_d".into(),
                "amt".into()
            )
        );
        assert_eq!(
            parse_qualified_column(None, None, None, Some("ods.orders.cust_id")).unwrap(),
            (
                "ods".into(),
                "orders".into(),
                "ods.orders".into(),
                "cust_id".into()
            )
        );
    }

    #[test]
    fn temp_tables_are_collapsed_into_physical_edges() {
        let sql = "
            CREATE TEMPORARY TABLE tmp_agg AS
            SELECT cust_id, sum(amt) AS total_amt FROM ods.orders GROUP BY cust_id;
            INSERT OVERWRITE TABLE dws.cust_summary
            SELECT cust_id, total_amt + 10 AS final_amt FROM tmp_agg;
        ";
        let ws = analyze_with_sources(
            vec![InputFile {
                name: "job.sql".into(),
                bytes: sql.as_bytes().to_vec(),
            }],
            &Options {
                job_name: Some("etl_dag.cust_summary".into()),
                ..Options::default()
            },
        );
        let payload =
            extract_batch_payload("etl_dag.cust_summary", SqlDialect::Hive, None, &ws).unwrap();
        assert_eq!(payload.batch.dag_name, "etl_dag");
        assert_eq!(payload.batch.task_name, "cust_summary");
        assert!(payload
            .job_tables
            .iter()
            .all(|t| t.full_table_name != "tmp_agg"));
        assert!(payload.table_edges.iter().any(|e| {
            e.source_full_name == "ods.orders" && e.target_full_name == "dws.cust_summary"
        }));
        assert!(payload.column_edges.iter().any(|ce| {
            ce.source_full_name == "ods.orders"
                && ce.source_column == "amt"
                && ce.target_full_name == "dws.cust_summary"
                && ce.target_column == "final_amt"
                && ce.edge_kind == "aggregate"
        }));
    }

    #[test]
    fn extract_edge_cases_and_multihop_temp() {
        assert_eq!(
            split_db_table("dir:/tmp/out"),
            ("".into(), "dir:/tmp/out".into(), "dir:/tmp/out".into())
        );
        assert_eq!(split_db_table("   "), ("".into(), "".into(), "".into()));
        assert!(parse_qualified_column(None, Some("t"), None, None).is_err());
        assert!(parse_qualified_column(None, None, Some("c"), None).is_err());
        assert!(parse_qualified_column(None, Some("."), Some("c"), None).is_err());
        assert!(parse_qualified_column(None, None, None, Some("single")).is_err());
        assert_eq!(clamp_chars("abcdef", 3), "abc");

        // 多跳临时表 tmp1 -> tmp2 + 长 SQL 预览截断 + 独立查询 result 命名空间
        let pad = "x".repeat(600);
        let sql = format!(
            "CREATE TEMPORARY TABLE tmp1 AS SELECT a, '{pad}' AS pad FROM ods.src;\n\
             CREATE TEMPORARY TABLE tmp2 AS SELECT a FROM tmp1;\n\
             INSERT INTO dwd.dst SELECT a FROM tmp2;\n\
             SELECT a FROM ods.src;"
        );
        let ws = analyze_with_sources(
            vec![InputFile {
                name: "job.sql".into(),
                bytes: sql.as_bytes().to_vec(),
            }],
            &Options {
                job_name: Some("dag.multi_tmp".into()),
                analyze_select: Some(true),
                ..Options::default()
            },
        );
        let p = extract_batch_payload("dag.multi_tmp", SqlDialect::Spark, None, &ws).unwrap();
        assert_eq!(p.batch.dialect, "spark");
        assert!(p.table_edges.iter().any(|e| e.source_full_name == "ods.src"
            && e.target_full_name == "dwd.dst"
            && e.sql_preview.ends_with("...")));
        assert!(p
            .job_tables
            .iter()
            .any(|t| t.full_table_name.starts_with("result:dag.multi_tmp:")));
    }
}
