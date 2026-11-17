//! Rust 服务端内存热图增量水位线索引（`HotGraphIndex`）
//!
//! 面向 10~50+ 跳超深血缘查询设计：
//! - 将 `job_name`、`库名.表名`、`字段名` 全部驻留为紧凑的 `u32` 整数符号 ID
//! - 按 `job_name` 维护最新 `batch_id` 快照，支持按 `batch_id > watermark` 增量合并新入库批次
//! - 提供表级、任务级、字段级双向邻接数组，单次多跳广度优先图遍历耗时 < 1ms

use crate::db::{
    BatchPayload, ColumnEdgeRow, JobSearchItem, JobTableRow, LineageDirection, RawColumnSubgraph,
    RawTableSubgraph, RawTaskSubgraph, TableSearchItem,
};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::collections::VecDeque;

const MAX_UNLIMITED_HOPS: u32 = 64;

/// `walk_task_chains` 返回类型：(全部相关任务, 任务依赖边, 上游跳数, 下游跳数)
type TaskWalkResult = (
    HashSet<u32>,
    Vec<(String, String, String)>,
    HashMap<u32, u32>,
    HashMap<u32, u32>,
);

#[derive(Default)]
struct SymInterner {
    strs: Vec<String>,
    ids: HashMap<String, u32>,
}

impl SymInterner {
    fn new_with_star() -> Self {
        let mut s = Self::default();
        s.intern("*");
        s
    }

    #[inline]
    fn intern(&mut self, text: &str) -> u32 {
        if let Some(&id) = self.ids.get(text) {
            return id;
        }
        let id = self.strs.len() as u32;
        let owned = text.to_string();
        self.strs.push(owned.clone());
        self.ids.insert(owned, id);
        id
    }

    #[inline]
    fn lookup(&self, text: &str) -> Option<u32> {
        self.ids.get(text).copied()
    }

    #[inline]
    fn resolve(&self, id: u32) -> &str {
        &self.strs[id as usize]
    }
}

#[derive(Clone)]
struct CompactTableEdge {
    src_tbl: u32,
    dst_tbl: u32,
    sql_preview: String,
}

#[derive(Clone)]
struct CompactColEdge {
    src_tbl: u32,
    src_col: u32,
    dst_tbl: u32,
    dst_col: u32,
    kind: String,
    stmt_indices: Vec<usize>,
}

#[derive(Clone)]
struct CompactJobTable {
    tbl_id: u32,
    db_name: String,
    table_name: String,
    table_kind: String,
    is_source: bool,
    is_target: bool,
    columns: Vec<String>,
}

#[derive(Clone)]
struct JobEntry {
    dag_name: String,
    task_name: String,
    batch_id: i64,
    ingested_at_ms: i64,
    tables: Vec<CompactJobTable>,
    table_edges: Vec<CompactTableEdge>,
    col_edges: Vec<CompactColEdge>,
}

#[derive(Clone, Copy)]
struct TblEdgeRef {
    peer_tbl: u32,
    job_id: u32,
    batch_idx: u32,
    edge_idx: u32,
}

#[derive(Clone, Copy)]
struct ColEdgeRef {
    peer_tbl: u32,
    peer_col: u32,
    job_id: u32,
    batch_idx: u32,
    edge_idx: u32,
}

pub struct HotGraphIndex {
    job_syms: SymInterner,
    tbl_syms: SymInterner,
    col_syms: SymInterner,
    watermark_batch_id: i64,
    /// `job_id -> Vec<JobEntry>`（LatestBatch 模式下每个 job_id 仅保留 1 个最新批次；AllHistory 临时构建时可保留多批次）
    jobs: HashMap<u32, Vec<JobEntry>>,
    keep_all_history: bool,

    tbl_up: HashMap<u32, Vec<TblEdgeRef>>,
    tbl_down: HashMap<u32, Vec<TblEdgeRef>>,
    tbl_writers: HashMap<u32, Vec<u32>>,
    tbl_readers: HashMap<u32, Vec<u32>>,
    tbl_job_refs: HashMap<u32, Vec<(u32, usize, usize)>>,
    col_in: HashMap<(u32, u32), Vec<ColEdgeRef>>,
    col_out: HashMap<(u32, u32), Vec<ColEdgeRef>>,
    tbl_col_in_cols: HashMap<u32, Vec<u32>>,
    tbl_col_out_cols: HashMap<u32, Vec<u32>>,
}

impl Default for HotGraphIndex {
    fn default() -> Self {
        Self::new(false)
    }
}

impl HotGraphIndex {
    pub fn new(keep_all_history: bool) -> Self {
        Self {
            job_syms: SymInterner::default(),
            tbl_syms: SymInterner::default(),
            col_syms: SymInterner::new_with_star(),
            watermark_batch_id: 0,
            jobs: HashMap::default(),
            keep_all_history,
            tbl_up: HashMap::default(),
            tbl_down: HashMap::default(),
            tbl_writers: HashMap::default(),
            tbl_readers: HashMap::default(),
            tbl_job_refs: HashMap::default(),
            col_in: HashMap::default(),
            col_out: HashMap::default(),
            tbl_col_in_cols: HashMap::default(),
            tbl_col_out_cols: HashMap::default(),
        }
    }

    pub fn from_batches(batches: &[BatchPayload], keep_all_history: bool) -> Self {
        let mut idx = Self::new(keep_all_history);
        idx.apply_batches(batches);
        idx
    }

    pub fn watermark_batch_id(&self) -> i64 {
        self.watermark_batch_id
    }

    /// 增量合并一批新入库 `BatchPayload`，仅对发生变更的 `job_id` 局部更新邻接索引
    pub fn apply_batches(&mut self, batches: &[BatchPayload]) -> bool {
        if batches.is_empty() {
            return false;
        }
        let mut dirty_jobs: HashSet<u32> = HashSet::default();
        for bp in batches {
            if bp.batch.batch_id > self.watermark_batch_id {
                self.watermark_batch_id = bp.batch.batch_id;
            }
            let job_id = self.job_syms.intern(&bp.batch.job_name);
            if !self.keep_all_history {
                if let Some(existing) = self.jobs.get(&job_id).and_then(|v| v.first()) {
                    if existing.batch_id >= bp.batch.batch_id {
                        continue;
                    }
                }
            } else if let Some(slot) = self.jobs.get(&job_id) {
                if slot.iter().any(|x| x.batch_id == bp.batch.batch_id) {
                    continue;
                }
            }
            if dirty_jobs.insert(job_id) && self.jobs.contains_key(&job_id) {
                self.remove_job_adjacency(job_id);
            }
            let entry = self.compact_payload(bp);
            if self.keep_all_history {
                let slot = self.jobs.entry(job_id).or_default();
                slot.push(entry);
                slot.sort_by_key(|x| x.batch_id);
            } else {
                self.jobs.insert(job_id, vec![entry]);
            }
        }
        if dirty_jobs.is_empty() {
            return false;
        }
        let mut sorted: Vec<u32> = dirty_jobs.into_iter().collect();
        sorted.sort_unstable();
        for jid in sorted {
            self.add_job_adjacency(jid);
        }
        true
    }

    fn compact_payload(&mut self, bp: &BatchPayload) -> JobEntry {
        let tables = bp
            .job_tables
            .iter()
            .map(|jt| CompactJobTable {
                tbl_id: self.tbl_syms.intern(&jt.full_table_name),
                db_name: jt.db_name.clone(),
                table_name: jt.table_name.clone(),
                table_kind: jt.table_kind.clone(),
                is_source: jt.is_source,
                is_target: jt.is_target,
                columns: jt.columns.clone(),
            })
            .collect();
        let table_edges = bp
            .table_edges
            .iter()
            .map(|te| CompactTableEdge {
                src_tbl: self.tbl_syms.intern(&te.source_full_name),
                dst_tbl: self.tbl_syms.intern(&te.target_full_name),
                sql_preview: te.sql_preview.clone(),
            })
            .collect();
        let col_edges = bp
            .column_edges
            .iter()
            .map(|ce| CompactColEdge {
                src_tbl: self.tbl_syms.intern(&ce.source_full_name),
                src_col: self.col_syms.intern(&ce.source_column),
                dst_tbl: self.tbl_syms.intern(&ce.target_full_name),
                dst_col: self.col_syms.intern(&ce.target_column),
                kind: ce.edge_kind.clone(),
                stmt_indices: ce.stmt_indices.clone(),
            })
            .collect();
        JobEntry {
            dag_name: bp.batch.dag_name.clone(),
            task_name: bp.batch.task_name.clone(),
            batch_id: bp.batch.batch_id,
            ingested_at_ms: bp.batch.ingested_at_ms,
            tables,
            table_edges,
            col_edges,
        }
    }

    fn remove_job_adjacency(&mut self, jid: u32) {
        let Some(batches) = self.jobs.get(&jid) else {
            return;
        };
        let mut touched_tbls: HashSet<u32> = HashSet::default();
        let mut touched_up: HashSet<u32> = HashSet::default();
        let mut touched_down: HashSet<u32> = HashSet::default();
        let mut touched_cin: HashSet<(u32, u32)> = HashSet::default();
        let mut touched_cout: HashSet<(u32, u32)> = HashSet::default();
        for je in batches {
            for jt in &je.tables {
                touched_tbls.insert(jt.tbl_id);
            }
            for te in &je.table_edges {
                touched_up.insert(te.dst_tbl);
                touched_down.insert(te.src_tbl);
            }
            for ce in &je.col_edges {
                touched_cin.insert((ce.dst_tbl, ce.dst_col));
                touched_cout.insert((ce.src_tbl, ce.src_col));
            }
        }
        for tid in touched_tbls {
            if let Some(v) = self.tbl_job_refs.get_mut(&tid) {
                v.retain(|&(j, _, _)| j != jid);
                if v.is_empty() {
                    self.tbl_job_refs.remove(&tid);
                }
            }
            if let Some(w) = self.tbl_writers.get_mut(&tid) {
                w.retain(|&j| j != jid);
                if w.is_empty() {
                    self.tbl_writers.remove(&tid);
                }
            }
            if let Some(r) = self.tbl_readers.get_mut(&tid) {
                r.retain(|&j| j != jid);
                if r.is_empty() {
                    self.tbl_readers.remove(&tid);
                }
            }
        }
        for tid in touched_up {
            if let Some(v) = self.tbl_up.get_mut(&tid) {
                v.retain(|e| e.job_id != jid);
            }
        }
        for tid in touched_down {
            if let Some(v) = self.tbl_down.get_mut(&tid) {
                v.retain(|e| e.job_id != jid);
            }
        }
        for k in touched_cin {
            if let Some(v) = self.col_in.get_mut(&k) {
                v.retain(|e| e.job_id != jid);
            }
        }
        for k in touched_cout {
            if let Some(v) = self.col_out.get_mut(&k) {
                v.retain(|e| e.job_id != jid);
            }
        }
    }

    fn add_job_adjacency(&mut self, jid: u32) {
        let Some(batches) = self.jobs.get(&jid) else {
            return;
        };
        for (bi, je) in batches.iter().enumerate() {
            for (ti, jt) in je.tables.iter().enumerate() {
                self.tbl_job_refs
                    .entry(jt.tbl_id)
                    .or_default()
                    .push((jid, bi, ti));
                if jt.table_kind != "temp" && jt.table_kind != "result" {
                    if jt.is_target {
                        let w = self.tbl_writers.entry(jt.tbl_id).or_default();
                        if !w.contains(&jid) {
                            w.push(jid);
                        }
                    }
                    if jt.is_source {
                        let r = self.tbl_readers.entry(jt.tbl_id).or_default();
                        if !r.contains(&jid) {
                            r.push(jid);
                        }
                    }
                }
            }
            for (ei, te) in je.table_edges.iter().enumerate() {
                let er_up = TblEdgeRef {
                    peer_tbl: te.src_tbl,
                    job_id: jid,
                    batch_idx: bi as u32,
                    edge_idx: ei as u32,
                };
                self.tbl_up.entry(te.dst_tbl).or_default().push(er_up);
                self.tbl_down
                    .entry(te.src_tbl)
                    .or_default()
                    .push(TblEdgeRef {
                        peer_tbl: te.dst_tbl,
                        job_id: jid,
                        batch_idx: bi as u32,
                        edge_idx: ei as u32,
                    });
            }
            for (ci, ce) in je.col_edges.iter().enumerate() {
                self.col_in
                    .entry((ce.dst_tbl, ce.dst_col))
                    .or_default()
                    .push(ColEdgeRef {
                        peer_tbl: ce.src_tbl,
                        peer_col: ce.src_col,
                        job_id: jid,
                        batch_idx: bi as u32,
                        edge_idx: ci as u32,
                    });
                let cin_cols = self.tbl_col_in_cols.entry(ce.dst_tbl).or_default();
                if !cin_cols.contains(&ce.dst_col) {
                    cin_cols.push(ce.dst_col);
                }
                self.col_out
                    .entry((ce.src_tbl, ce.src_col))
                    .or_default()
                    .push(ColEdgeRef {
                        peer_tbl: ce.dst_tbl,
                        peer_col: ce.dst_col,
                        job_id: jid,
                        batch_idx: bi as u32,
                        edge_idx: ci as u32,
                    });
                let cout_cols = self.tbl_col_out_cols.entry(ce.src_tbl).or_default();
                if !cout_cols.contains(&ce.src_col) {
                    cout_cols.push(ce.src_col);
                }
            }
        }
    }

    fn job_summary(&self, jid: u32) -> Option<JobSearchItem> {
        let batches = self.jobs.get(&jid)?;
        let latest = batches.last()?;
        let mut srcs = Vec::new();
        let mut tgts = Vec::new();
        for je in batches {
            for jt in &je.tables {
                if jt.table_kind == "temp" || jt.table_kind == "result" {
                    continue;
                }
                let tname = self.tbl_syms.resolve(jt.tbl_id).to_string();
                if jt.is_source && !srcs.contains(&tname) {
                    srcs.push(tname.clone());
                }
                if jt.is_target && !tgts.contains(&tname) {
                    tgts.push(tname);
                }
            }
        }
        srcs.sort();
        tgts.sort();
        Some(JobSearchItem {
            job_name: self.job_syms.resolve(jid).to_string(),
            dag_name: latest.dag_name.clone(),
            task_name: latest.task_name.clone(),
            latest_batch_id: latest.batch_id,
            ingested_at_ms: latest.ingested_at_ms,
            source_tables: srcs,
            target_tables: tgts,
        })
    }

    fn collect_job_tables(&self, tbl_ids: &HashSet<u32>) -> Vec<JobTableRow> {
        let mut rows = Vec::new();
        for &tid in tbl_ids {
            let full_name = self.tbl_syms.resolve(tid).to_string();
            if let Some(refs) = self.tbl_job_refs.get(&tid) {
                for &(jid, bi, ti) in refs {
                    if let Some(je) = self.jobs.get(&jid).and_then(|v| v.get(bi)) {
                        let jt = &je.tables[ti];
                        rows.push(JobTableRow {
                            job_name: self.job_syms.resolve(jid).to_string(),
                            batch_id: je.batch_id,
                            db_name: jt.db_name.clone(),
                            table_name: jt.table_name.clone(),
                            full_table_name: full_name.clone(),
                            table_kind: jt.table_kind.clone(),
                            is_source: jt.is_source,
                            is_target: jt.is_target,
                            columns: jt.columns.clone(),
                        });
                    }
                }
            }
        }
        rows
    }

    pub fn search_jobs(&self, keyword: &str, limit: usize) -> Vec<JobSearchItem> {
        let kw = keyword.trim().to_lowercase();
        let cap = limit.clamp(1, 500);
        let mut matched: Vec<(i64, &str, u32)> = self
            .jobs
            .iter()
            .filter_map(|(&jid, batches)| {
                let latest = batches.last()?;
                let name = self.job_syms.resolve(jid);
                if !kw.is_empty() && !name.to_lowercase().contains(&kw) {
                    return None;
                }
                Some((latest.batch_id, name, jid))
            })
            .collect();
        matched.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
        matched.truncate(cap);
        matched
            .into_iter()
            .filter_map(|(_, _, jid)| self.job_summary(jid))
            .collect()
    }

    pub fn search_tables(&self, keyword: &str, limit: usize) -> Vec<TableSearchItem> {
        let kw = keyword.trim().to_lowercase();
        let cap = limit.clamp(1, 500);
        let mut matched: Vec<(&str, u32)> = self
            .tbl_job_refs
            .keys()
            .filter_map(|&tid| {
                let full = self.tbl_syms.resolve(tid);
                if full.starts_with("result:") {
                    return None;
                }
                if !kw.is_empty() && !full.to_lowercase().contains(&kw) {
                    return None;
                }
                Some((full, tid))
            })
            .collect();
        matched.sort_by(|a, b| a.0.cmp(b.0));
        matched.truncate(cap);
        let mut out = Vec::with_capacity(matched.len());
        for (full, tid) in matched {
            let Some(refs) = self.tbl_job_refs.get(&tid) else {
                continue;
            };
            let Some(&(jid0, bi0, ti0)) = refs.first() else {
                continue;
            };
            let jt0 = &self.jobs[&jid0][bi0].tables[ti0];
            let mut producers: Vec<String> = self
                .tbl_writers
                .get(&tid)
                .map(|v| {
                    v.iter()
                        .map(|&j| self.job_syms.resolve(j).to_string())
                        .collect()
                })
                .unwrap_or_default();
            let mut consumers: Vec<String> = self
                .tbl_readers
                .get(&tid)
                .map(|v| {
                    v.iter()
                        .map(|&j| self.job_syms.resolve(j).to_string())
                        .collect()
                })
                .unwrap_or_default();
            producers.sort();
            consumers.sort();
            out.push(TableSearchItem {
                full_table_name: full.to_string(),
                db_name: jt0.db_name.clone(),
                table_name: jt0.table_name.clone(),
                table_kind: jt0.table_kind.clone(),
                producer_jobs: producers,
                consumer_jobs: consumers,
            });
        }
        out
    }

    pub fn query_table_subgraph(
        &self,
        table: &str,
        dir: LineageDirection,
        max_hops: u32,
    ) -> RawTableSubgraph {
        let Some(focus_id) = self.tbl_syms.lookup(table) else {
            return RawTableSubgraph::default();
        };
        let cap = if max_hops == 0 {
            MAX_UNLIMITED_HOPS
        } else {
            max_hops
        };

        let mut all_tbls: HashSet<u32> = HashSet::default();
        all_tbls.insert(focus_id);
        let mut seen_edges: HashSet<(u32, u32, u32)> = HashSet::default();
        let mut edges = Vec::new();
        let mut up_dist: HashMap<u32, u32> = HashMap::default();
        let mut down_dist: HashMap<u32, u32> = HashMap::default();

        if dir.want_up() {
            let mut q: VecDeque<(u32, u32)> = VecDeque::new();
            q.push_back((focus_id, 0));
            let mut visited: HashSet<u32> = HashSet::default();
            visited.insert(focus_id);
            while let Some((cur, d)) = q.pop_front() {
                if d >= cap {
                    continue;
                }
                if let Some(ins) = self.tbl_up.get(&cur) {
                    for er in ins {
                        all_tbls.insert(er.peer_tbl);
                        up_dist.entry(er.peer_tbl).or_insert(d + 1);
                        if seen_edges.insert((er.peer_tbl, cur, er.job_id)) {
                            let bi = er.batch_idx as usize;
                            let ei = er.edge_idx as usize;
                            let preview = self
                                .jobs
                                .get(&er.job_id)
                                .and_then(|v| v.get(bi))
                                .and_then(|je| je.table_edges.get(ei))
                                .map(|e| e.sql_preview.clone())
                                .unwrap_or_default();
                            edges.push((
                                self.tbl_syms.resolve(er.peer_tbl).to_string(),
                                self.tbl_syms.resolve(cur).to_string(),
                                self.job_syms.resolve(er.job_id).to_string(),
                                preview,
                            ));
                        }
                        if visited.insert(er.peer_tbl) {
                            q.push_back((er.peer_tbl, d + 1));
                        }
                    }
                }
            }
        }

        if dir.want_down() {
            let mut q: VecDeque<(u32, u32)> = VecDeque::new();
            q.push_back((focus_id, 0));
            let mut visited: HashSet<u32> = HashSet::default();
            visited.insert(focus_id);
            while let Some((cur, d)) = q.pop_front() {
                if d >= cap {
                    continue;
                }
                if let Some(outs) = self.tbl_down.get(&cur) {
                    for er in outs {
                        all_tbls.insert(er.peer_tbl);
                        down_dist.entry(er.peer_tbl).or_insert(d + 1);
                        if seen_edges.insert((cur, er.peer_tbl, er.job_id)) {
                            let bi = er.batch_idx as usize;
                            let ei = er.edge_idx as usize;
                            let preview = self
                                .jobs
                                .get(&er.job_id)
                                .and_then(|v| v.get(bi))
                                .and_then(|je| je.table_edges.get(ei))
                                .map(|e| e.sql_preview.clone())
                                .unwrap_or_default();
                            edges.push((
                                self.tbl_syms.resolve(cur).to_string(),
                                self.tbl_syms.resolve(er.peer_tbl).to_string(),
                                self.job_syms.resolve(er.job_id).to_string(),
                                preview,
                            ));
                        }
                        if visited.insert(er.peer_tbl) {
                            q.push_back((er.peer_tbl, d + 1));
                        }
                    }
                }
            }
        }

        RawTableSubgraph {
            edges,
            job_tables: self.collect_job_tables(&all_tbls),
            up_distances: up_dist
                .into_iter()
                .map(|(t, d)| (self.tbl_syms.resolve(t).to_string(), d))
                .collect(),
            down_distances: down_dist
                .into_iter()
                .map(|(t, d)| (self.tbl_syms.resolve(t).to_string(), d))
                .collect(),
        }
    }

    /// 辅助：给定一组起始上游任务与起始下游任务，扩展跨任务链路
    fn walk_task_chains(
        &self,
        up_seeds: &[u32],
        down_seeds: &[u32],
        dir: LineageDirection,
        max_hops: u32,
        seed_base_dist: u32,
    ) -> TaskWalkResult {
        let cap = if max_hops == 0 {
            MAX_UNLIMITED_HOPS
        } else {
            max_hops
        };
        let mut all_jobs: HashSet<u32> = HashSet::default();
        let mut seen_edges: HashSet<(u32, u32, u32)> = HashSet::default();
        let mut task_edges = Vec::new();
        let mut up_dist: HashMap<u32, u32> = HashMap::default();
        let mut down_dist: HashMap<u32, u32> = HashMap::default();

        if dir.want_up() {
            let mut q: VecDeque<(u32, u32)> = VecDeque::new();
            let mut visited: HashSet<u32> = HashSet::default();
            for &jid in up_seeds {
                all_jobs.insert(jid);
                if seed_base_dist > 0 {
                    up_dist.entry(jid).or_insert(seed_base_dist);
                }
                if visited.insert(jid) {
                    q.push_back((jid, seed_base_dist));
                }
            }
            while let Some((cur_job, d)) = q.pop_front() {
                if d >= cap {
                    continue;
                }
                let Some(batches) = self.jobs.get(&cur_job) else {
                    continue;
                };
                for je in batches {
                    for jt in &je.tables {
                        if !jt.is_source || jt.table_kind == "temp" || jt.table_kind == "result" {
                            continue;
                        }
                        if let Some(writers) = self.tbl_writers.get(&jt.tbl_id) {
                            for &up_job in writers {
                                if up_job == cur_job {
                                    continue;
                                }
                                all_jobs.insert(up_job);
                                up_dist.entry(up_job).or_insert(d + 1);
                                if seen_edges.insert((up_job, cur_job, jt.tbl_id)) {
                                    task_edges.push((
                                        self.job_syms.resolve(up_job).to_string(),
                                        self.job_syms.resolve(cur_job).to_string(),
                                        self.tbl_syms.resolve(jt.tbl_id).to_string(),
                                    ));
                                }
                                if visited.insert(up_job) {
                                    q.push_back((up_job, d + 1));
                                }
                            }
                        }
                    }
                }
            }
        }

        if dir.want_down() {
            let mut q: VecDeque<(u32, u32)> = VecDeque::new();
            let mut visited: HashSet<u32> = HashSet::default();
            for &jid in down_seeds {
                all_jobs.insert(jid);
                if seed_base_dist > 0 {
                    down_dist.entry(jid).or_insert(seed_base_dist);
                }
                if visited.insert(jid) {
                    q.push_back((jid, seed_base_dist));
                }
            }
            while let Some((cur_job, d)) = q.pop_front() {
                if d >= cap {
                    continue;
                }
                let Some(batches) = self.jobs.get(&cur_job) else {
                    continue;
                };
                for je in batches {
                    for jt in &je.tables {
                        if !jt.is_target || jt.table_kind == "temp" || jt.table_kind == "result" {
                            continue;
                        }
                        if let Some(readers) = self.tbl_readers.get(&jt.tbl_id) {
                            for &dn_job in readers {
                                if dn_job == cur_job {
                                    continue;
                                }
                                all_jobs.insert(dn_job);
                                down_dist.entry(dn_job).or_insert(d + 1);
                                if seen_edges.insert((cur_job, dn_job, jt.tbl_id)) {
                                    task_edges.push((
                                        self.job_syms.resolve(cur_job).to_string(),
                                        self.job_syms.resolve(dn_job).to_string(),
                                        self.tbl_syms.resolve(jt.tbl_id).to_string(),
                                    ));
                                }
                                if visited.insert(dn_job) {
                                    q.push_back((dn_job, d + 1));
                                }
                            }
                        }
                    }
                }
            }
        }

        (all_jobs, task_edges, up_dist, down_dist)
    }

    pub fn query_table_task_subgraph(
        &self,
        table: &str,
        dir: LineageDirection,
        max_hops: u32,
    ) -> RawTaskSubgraph {
        let Some(tid) = self.tbl_syms.lookup(table) else {
            return RawTaskSubgraph::default();
        };
        let producers: Vec<u32> = if dir.want_up() {
            self.tbl_writers.get(&tid).cloned().unwrap_or_default()
        } else {
            Vec::new()
        };
        let consumers: Vec<u32> = if dir.want_down() {
            self.tbl_readers.get(&tid).cloned().unwrap_or_default()
        } else {
            Vec::new()
        };

        let (all_jobs, mut task_edges, up_dist, down_dist) =
            self.walk_task_chains(&producers, &consumers, dir, max_hops, 1);

        // 同时记录直接产出任务 -> 直接消费任务经由当前表形成的连接
        let p_set: HashSet<u32> = producers.iter().copied().collect();
        let c_set: HashSet<u32> = consumers.iter().copied().collect();
        let tbl_name = self.tbl_syms.resolve(tid).to_string();
        for &pj in &p_set {
            for &cj in &c_set {
                if pj != cj {
                    let p_name = self.job_syms.resolve(pj).to_string();
                    let c_name = self.job_syms.resolve(cj).to_string();
                    let tup = (p_name, c_name, tbl_name.clone());
                    if !task_edges.contains(&tup) {
                        task_edges.push(tup);
                    }
                }
            }
        }

        let mut jobs: Vec<JobSearchItem> = all_jobs
            .into_iter()
            .filter_map(|jid| self.job_summary(jid))
            .collect();
        jobs.sort_by(|a, b| a.job_name.cmp(&b.job_name));

        let mut direct_producers: Vec<String> = producers
            .into_iter()
            .map(|j| self.job_syms.resolve(j).to_string())
            .collect();
        direct_producers.sort();
        let mut direct_consumers: Vec<String> = consumers
            .into_iter()
            .map(|j| self.job_syms.resolve(j).to_string())
            .collect();
        direct_consumers.sort();

        RawTaskSubgraph {
            task_edges,
            direct_producers,
            direct_consumers,
            jobs,
            up_distances: up_dist
                .into_iter()
                .map(|(j, d)| (self.job_syms.resolve(j).to_string(), d))
                .collect(),
            down_distances: down_dist
                .into_iter()
                .map(|(j, d)| (self.job_syms.resolve(j).to_string(), d))
                .collect(),
        }
    }

    pub fn query_task_subgraph(
        &self,
        job_name: &str,
        dir: LineageDirection,
        max_hops: u32,
    ) -> RawTaskSubgraph {
        // 先尝试精确匹配，再尝试不区分大小写精确匹配
        let target_jid = self.job_syms.lookup(job_name.trim()).or_else(|| {
            let lower = job_name.trim().to_lowercase();
            self.jobs
                .keys()
                .copied()
                .find(|&jid| self.job_syms.resolve(jid).to_lowercase() == lower)
        });
        let Some(focus_jid) = target_jid else {
            return RawTaskSubgraph::default();
        };

        let seeds = [focus_jid];
        let (mut all_jobs, task_edges, up_dist, down_dist) =
            self.walk_task_chains(&seeds, &seeds, dir, max_hops, 0);
        all_jobs.insert(focus_jid);

        let mut jobs: Vec<JobSearchItem> = all_jobs
            .into_iter()
            .filter_map(|jid| self.job_summary(jid))
            .collect();
        jobs.sort_by(|a, b| a.job_name.cmp(&b.job_name));

        RawTaskSubgraph {
            task_edges,
            direct_producers: Vec::new(),
            direct_consumers: Vec::new(),
            jobs,
            up_distances: up_dist
                .into_iter()
                .map(|(j, d)| (self.job_syms.resolve(j).to_string(), d))
                .collect(),
            down_distances: down_dist
                .into_iter()
                .map(|(j, d)| (self.job_syms.resolve(j).to_string(), d))
                .collect(),
        }
    }

    pub fn query_column_subgraph(
        &self,
        full_table: &str,
        column: &str,
        dir: LineageDirection,
        max_hops: u32,
    ) -> RawColumnSubgraph {
        let Some(focus_tid) = self.tbl_syms.lookup(full_table) else {
            return RawColumnSubgraph::default();
        };
        let star_sym = self.col_syms.lookup("*").unwrap_or(0);
        let focus_cid = match self.col_syms.lookup(column) {
            Some(cid) => cid,
            None => {
                // 若该列未出现在边表但存在 `*` 边，允许从 `*` 探测
                if !self.col_in.contains_key(&(focus_tid, star_sym))
                    && !self.col_out.contains_key(&(focus_tid, star_sym))
                {
                    return RawColumnSubgraph {
                        edges: Vec::new(),
                        job_tables: self.collect_job_tables(&HashSet::from_iter([focus_tid])),
                        up_distances: Vec::new(),
                        down_distances: Vec::new(),
                    };
                }
                star_sym
            }
        };

        let cap = if max_hops == 0 {
            MAX_UNLIMITED_HOPS
        } else {
            max_hops
        };
        let mut all_tbls: HashSet<u32> = HashSet::default();
        all_tbls.insert(focus_tid);
        let mut seen_edge_keys: HashSet<(u32, u32, u32, u32, u32)> = HashSet::default();
        let mut edges = Vec::new();
        let mut up_dist: HashMap<(u32, u32), u32> = HashMap::default();
        let mut down_dist: HashMap<(u32, u32), u32> = HashMap::default();

        let push_edge = |edges: &mut Vec<ColumnEdgeRow>,
                         seen: &mut HashSet<(u32, u32, u32, u32, u32)>,
                         er: ColEdgeRef| {
            let bi = er.batch_idx as usize;
            let ci = er.edge_idx as usize;
            let jid = er.job_id;
            let Some(je) = self.jobs.get(&jid).and_then(|v| v.get(bi)) else {
                return;
            };
            let Some(ce) = je.col_edges.get(ci) else {
                return;
            };
            if !seen.insert((ce.src_tbl, ce.src_col, ce.dst_tbl, ce.dst_col, jid)) {
                return;
            }
            let sfull = self.tbl_syms.resolve(ce.src_tbl);
            let dfull = self.tbl_syms.resolve(ce.dst_tbl);
            let (sdb, stbl, _) = crate::db::extract::split_db_table(sfull);
            let (ddb, dtbl, _) = crate::db::extract::split_db_table(dfull);
            edges.push(ColumnEdgeRow {
                job_name: self.job_syms.resolve(jid).to_string(),
                batch_id: je.batch_id,
                source_db: sdb,
                source_table: stbl,
                source_full_name: sfull.to_string(),
                source_column: self.col_syms.resolve(ce.src_col).to_string(),
                target_db: ddb,
                target_table: dtbl,
                target_full_name: dfull.to_string(),
                target_column: self.col_syms.resolve(ce.dst_col).to_string(),
                edge_kind: ce.kind.clone(),
                stmt_indices: ce.stmt_indices.clone(),
            });
        };

        if dir.want_up() {
            let mut q: VecDeque<(u32, u32, u32)> = VecDeque::new();
            let mut visited: HashSet<(u32, u32)> = HashSet::default();
            visited.insert((focus_tid, focus_cid));
            q.push_back((focus_tid, focus_cid, 0));
            while let Some((t, c, d)) = q.pop_front() {
                if d >= cap {
                    continue;
                }
                if c == star_sym {
                    if let Some(cols) = self.tbl_col_in_cols.get(&t) {
                        for &cid in cols {
                            if let Some(v) = self.col_in.get(&(t, cid)) {
                                for &er in v {
                                    all_tbls.insert(er.peer_tbl);
                                    up_dist.entry((er.peer_tbl, er.peer_col)).or_insert(d + 1);
                                    push_edge(&mut edges, &mut seen_edge_keys, er);
                                    if visited.insert((er.peer_tbl, er.peer_col)) {
                                        q.push_back((er.peer_tbl, er.peer_col, d + 1));
                                    }
                                }
                            }
                        }
                    }
                } else {
                    for lookup_col in [c, star_sym] {
                        if let Some(v) = self.col_in.get(&(t, lookup_col)) {
                            for &er in v {
                                all_tbls.insert(er.peer_tbl);
                                up_dist.entry((er.peer_tbl, er.peer_col)).or_insert(d + 1);
                                push_edge(&mut edges, &mut seen_edge_keys, er);
                                if visited.insert((er.peer_tbl, er.peer_col)) {
                                    q.push_back((er.peer_tbl, er.peer_col, d + 1));
                                }
                            }
                        }
                    }
                }
            }
        }

        if dir.want_down() {
            let mut q: VecDeque<(u32, u32, u32)> = VecDeque::new();
            let mut visited: HashSet<(u32, u32)> = HashSet::default();
            visited.insert((focus_tid, focus_cid));
            q.push_back((focus_tid, focus_cid, 0));
            while let Some((t, c, d)) = q.pop_front() {
                if d >= cap {
                    continue;
                }
                if c == star_sym {
                    if let Some(cols) = self.tbl_col_out_cols.get(&t) {
                        for &cid in cols {
                            if let Some(v) = self.col_out.get(&(t, cid)) {
                                for &er in v {
                                    all_tbls.insert(er.peer_tbl);
                                    down_dist.entry((er.peer_tbl, er.peer_col)).or_insert(d + 1);
                                    push_edge(&mut edges, &mut seen_edge_keys, er);
                                    if visited.insert((er.peer_tbl, er.peer_col)) {
                                        q.push_back((er.peer_tbl, er.peer_col, d + 1));
                                    }
                                }
                            }
                        }
                    }
                } else {
                    for lookup_col in [c, star_sym] {
                        if let Some(v) = self.col_out.get(&(t, lookup_col)) {
                            for &er in v {
                                all_tbls.insert(er.peer_tbl);
                                down_dist.entry((er.peer_tbl, er.peer_col)).or_insert(d + 1);
                                push_edge(&mut edges, &mut seen_edge_keys, er);
                                if visited.insert((er.peer_tbl, er.peer_col)) {
                                    q.push_back((er.peer_tbl, er.peer_col, d + 1));
                                }
                            }
                        }
                    }
                }
            }
        }

        RawColumnSubgraph {
            edges,
            job_tables: self.collect_job_tables(&all_tbls),
            up_distances: up_dist
                .into_iter()
                .map(|((t, c), d)| {
                    (
                        (
                            self.tbl_syms.resolve(t).to_string(),
                            self.col_syms.resolve(c).to_string(),
                        ),
                        d,
                    )
                })
                .collect(),
            down_distances: down_dist
                .into_iter()
                .map(|((t, c), d)| {
                    (
                        (
                            self.tbl_syms.resolve(t).to_string(),
                            self.col_syms.resolve(c).to_string(),
                        ),
                        d,
                    )
                })
                .collect(),
        }
    }
}
