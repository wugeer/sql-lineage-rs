//! AST -> 血缘分析器（按语句顺序执行，catalog 随之演进，列名整数符号驻留）

use crate::catalog::{Catalog, ColId, SymPool, STAR_COL};
use crate::model::{ColumnEdge, TableEdge, TableInfo};
use crate::preprocess::restore_placeholders;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use sqlparser::ast::*;
use std::borrow::Cow;
use std::rc::Rc;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Kind {
    Direct,
    Derived,
    Aggregate,
}

impl Kind {
    #[inline]
    fn as_str(self) -> &'static str {
        match self {
            Kind::Direct => "direct",
            Kind::Derived => "derived",
            Kind::Aggregate => "aggregate",
        }
    }

    /// 外层包装级别 w 与本级别合成：direct 透传，aggregate 一旦沾上永远是 aggregate
    #[inline]
    fn wrap(self, w: Kind) -> Kind {
        match (self, w) {
            (Kind::Aggregate, _) | (_, Kind::Aggregate) => Kind::Aggregate,
            (k, Kind::Direct) => k,
            _ => Kind::Derived,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ColSrc {
    pub table: usize,
    pub col: ColId,
    pub kind: Kind,
}

/// 表达式来源列集合：内联容量 4 覆盖绝大多数单/双列引用场景，避免高频小堆分配
type Srcs = smallvec::SmallVec<[ColSrc; 4]>;

#[derive(Clone)]
pub struct ColOut {
    pub name: ColId,
    pub srcs: Rc<Srcs>,
}

/// 一个关系的输出列集合（子查询 / CTE / 表函数 / lateral view）
#[derive(Clone, Default)]
pub struct RelCols {
    pub cols: Vec<ColOut>,
    /// 存在无法展开的 `*`（来源表列未知）
    pub unknown: bool,
}

#[derive(Clone)]
struct CteEntry {
    cols: Rc<Vec<ColOut>>,
    unknown: bool,
}

struct Rel {
    /// 可匹配的名字：别名 / 表全名 / 表末段名
    names: smallvec::SmallVec<[String; 3]>,
    /// 物理表 intern id
    base: Option<usize>,
    cols: Rc<Vec<ColOut>>,
    /// 列集合是否完整已知
    known: bool,
}

#[derive(Default)]
struct Scope<'a> {
    rels: Vec<Rel>,
    /// WINDOW w AS (...) 命名窗口
    nwin: HashMap<String, &'a WindowSpec>,
}

#[inline]
fn lower_cow(s: &str) -> Cow<'_, str> {
    if s.bytes().all(|b| !b.is_ascii_uppercase() && b < 0x80) {
        Cow::Borrowed(s)
    } else if s.is_ascii() {
        Cow::Owned(s.to_ascii_lowercase())
    } else {
        Cow::Owned(s.to_lowercase())
    }
}

#[inline]
fn is_placeholder(s: &str) -> bool {
    s.starts_with("hzv") && s.ends_with("hz") && s.len() > 5
}

fn is_aggregate(name: &str) -> bool {
    matches!(
        name,
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "collect_set"
            | "collect_list"
            | "percentile"
            | "percentile_approx"
            | "variance"
            | "var_samp"
            | "var_pop"
            | "stddev"
            | "stddev_pop"
            | "stddev_samp"
            | "std"
            | "covar_pop"
            | "covar_samp"
            | "corr"
            | "histogram_numeric"
            | "approx_count_distinct"
            | "grouping"
            | "bit_and"
            | "bit_or"
            | "bit_xor"
            | "bool_and"
            | "bool_or"
            | "every"
            | "regr_avgx"
            | "regr_avgy"
            | "regr_count"
            | "regr_intercept"
            | "regr_r2"
            | "regr_slope"
            | "regr_sxx"
            | "regr_sxy"
            | "regr_syy"
            | "kurtosis"
            | "skewness"
    )
}

#[inline]
fn part_lower(p: &ObjectNamePart) -> Cow<'_, str> {
    match p {
        ObjectNamePart::Identifier(i) => lower_cow(&i.value),
        ObjectNamePart::Function(f) => Cow::Owned(f.to_string().to_lowercase()),
    }
}

fn name_parts(n: &ObjectName) -> Vec<String> {
    n.0.iter().map(|p| part_lower(p).into_owned()).collect()
}

/// 未命名 select item 的输出名（列引用取列名，否则 None -> _c{i}）
fn item_name(e: &Expr) -> Option<Cow<'_, str>> {
    match e {
        Expr::Identifier(i) => Some(lower_cow(&i.value)),
        Expr::CompoundIdentifier(ids) => ids.last().map(|i| lower_cow(&i.value)),
        Expr::Nested(x) => item_name(x),
        Expr::Cast { expr: x, .. } => item_name(x),
        _ => None,
    }
}

#[inline]
fn wrap(mut srcs: Srcs, w: Kind) -> Srcs {
    if w == Kind::Direct {
        return srcs;
    }
    for s in &mut srcs {
        s.kind = s.kind.wrap(w);
    }
    srcs
}

pub struct Analyzer {
    pub catalog: Catalog,
    pub syms: SymPool,
    pub current_db: Option<String>,
    default_db: Option<String>,
    /// hzv{n}hz -> ${orig}
    restore: Vec<(String, String)>,
    /// 是否将纯 SELECT 语句映射为虚拟结果表
    standalone_query_mode: bool,
    total_standalone_queries: usize,
    query_result_seq: usize,

    names: Vec<String>,
    ids: HashMap<String, usize>,
    kinds: Vec<&'static str>,
    is_src: Vec<bool>,
    is_tgt: Vec<bool>,
    /// CREATE TEMPORARY TABLE 名（不加库前缀）
    temp: HashSet<String>,
    ctes: HashMap<String, CteEntry>,
    empty_cols: Rc<Vec<ColOut>>,
    rel_cache: Vec<Option<(u32, Rc<Vec<ColOut>>)>>,

    stmt_idx: usize,
    cur_targets: Vec<usize>,
    cur_tgt_seen: HashSet<usize>,
    cur_sources: Vec<usize>,
    cur_src_seen: HashSet<usize>,
    cur_edges: Vec<(usize, ColId, usize, ColId, Kind)>,
    cur_edge_seen: HashSet<(usize, ColId, usize, ColId, Kind)>,
    /// 血缘中出现的列，语句结束后并入 catalog
    pending_cols: Vec<(usize, ColId)>,

    table_edge_map: HashMap<(usize, usize), usize>,
    table_edges: Vec<TableEdge>,
    col_edge_map: HashMap<(usize, ColId, usize, ColId, &'static str), usize>,
    column_edges: Vec<ColumnEdge>,
}

impl Analyzer {
    pub fn new(
        default_db: Option<String>,
        restore: Vec<(String, String)>,
        standalone_query_mode: bool,
        total_standalone_queries: usize,
    ) -> Self {
        Analyzer {
            catalog: Catalog::default(),
            syms: SymPool::new(),
            current_db: None,
            default_db,
            restore,
            standalone_query_mode,
            total_standalone_queries,
            query_result_seq: 0,
            names: Vec::new(),
            ids: HashMap::default(),
            kinds: Vec::new(),
            is_src: Vec::new(),
            is_tgt: Vec::new(),
            temp: HashSet::default(),
            ctes: HashMap::default(),
            empty_cols: Rc::new(Vec::new()),
            rel_cache: Vec::new(),
            stmt_idx: 0,
            cur_targets: Vec::new(),
            cur_tgt_seen: HashSet::default(),
            cur_sources: Vec::new(),
            cur_src_seen: HashSet::default(),
            cur_edges: Vec::new(),
            cur_edge_seen: HashSet::default(),
            pending_cols: Vec::new(),
            table_edge_map: HashMap::default(),
            table_edges: Vec::new(),
            col_edge_map: HashMap::default(),
            column_edges: Vec::new(),
        }
    }

    #[inline]
    pub fn intern_col(&mut self, raw: &str) -> ColId {
        let restored = restore_placeholders(raw, &self.restore);
        self.syms.intern(&restored)
    }

    pub fn register_columns(&mut self, tid: usize, cols: &[String]) {
        let ids: Vec<ColId> = cols.iter().map(|c| self.intern_col(c)).collect();
        self.catalog.register(tid, ids);
    }

    pub fn copy_columns_from_name(&mut self, dst_tid: usize, src_qname: &str) {
        let restored = restore_placeholders(src_qname, &self.restore);
        if let Some(&src_tid) = self.ids.get(restored.as_ref()) {
            self.catalog.copy_from(dst_tid, src_tid);
        }
    }

    /// 规范名分段 -> 库限定后的全名（CTE/temp/变量占位不加前缀）
    pub fn qualify(&self, parts: &[String]) -> String {
        if parts.len() == 1 {
            let p = &parts[0];
            if self.ctes.contains_key(p) || self.temp.contains(p) || is_placeholder(p) {
                return p.clone();
            }
            if let Some(d) = self.current_db.as_ref().or(self.default_db.as_ref()) {
                let mut s = String::with_capacity(d.len() + 1 + p.len());
                s.push_str(d);
                s.push('.');
                s.push_str(p);
                return s;
            }
            return p.clone();
        }
        parts.join(".")
    }

    pub fn intern(&mut self, name: &str) -> usize {
        let n = restore_placeholders(name, &self.restore);
        if let Some(&i) = self.ids.get(n.as_ref()) {
            return i;
        }
        let i = self.names.len();
        let owned = n.into_owned();
        self.names.push(owned.clone());
        self.ids.insert(owned, i);
        self.kinds.push("table");
        self.is_src.push(false);
        self.is_tgt.push(false);
        i
    }

    pub fn set_kind(&mut self, id: usize, kind: &'static str) {
        self.kinds[id] = kind;
    }

    /// 登记 TEMPORARY 表名（后续引用不加库前缀）
    pub fn add_temp(&mut self, name: &str) {
        self.temp.insert(name.to_string());
    }

    pub fn mark_target(&mut self, id: usize) {
        self.is_tgt[id] = true;
        if self.cur_tgt_seen.insert(id) {
            self.cur_targets.push(id);
        }
    }

    pub fn mark_source(&mut self, id: usize) {
        self.is_src[id] = true;
        if self.cur_src_seen.insert(id) {
            self.cur_sources.push(id);
        }
    }

    /// fallback 用：分段名 -> 限定 -> 登记为目标
    pub fn intern_target(&mut self, parts: &[String]) -> usize {
        let qn = self.qualify(parts);
        let id = self.intern(&qn);
        self.mark_target(id);
        id
    }

    pub fn intern_source(&mut self, parts: &[String]) -> usize {
        let qn = self.qualify(parts);
        let id = self.intern(&qn);
        self.mark_source(id);
        id
    }

    pub fn begin_stmt(&mut self, idx: usize) {
        self.stmt_idx = idx;
        self.cur_targets.clear();
        self.cur_tgt_seen.clear();
        self.cur_sources.clear();
        self.cur_src_seen.clear();
        self.cur_edges.clear();
        self.cur_edge_seen.clear();
        self.pending_cols.clear();
    }

    /// 收尾：笛卡尔积表级边、列边并入全局、pending 列并入 catalog
    pub fn finish_stmt(&mut self) -> (Vec<usize>, Vec<usize>) {
        let allow_self = self.cur_targets.len() == 1
            && self.cur_sources.len() == 1
            && self.cur_targets[0] == self.cur_sources[0];
        for &t in &self.cur_targets {
            for &s in &self.cur_sources {
                if s == t && !allow_self {
                    // 若确实存在自读自写（mark_source 已标记），保留边；此处均已被显式 mark_source
                }
                let k = (s, t);
                match self.table_edge_map.get(&k) {
                    Some(&i) => {
                        let st = &mut self.table_edges[i].statements;
                        if st.last() != Some(&self.stmt_idx) {
                            st.push(self.stmt_idx);
                        }
                    }
                    None => {
                        self.table_edge_map.insert(k, self.table_edges.len());
                        self.table_edges.push(TableEdge {
                            source: s,
                            target: t,
                            statements: vec![self.stmt_idx],
                        });
                    }
                }
            }
        }
        for &(st, sc, tt, tc, kind) in &self.cur_edges {
            let k = (st, sc, tt, tc, kind.as_str());
            match self.col_edge_map.get(&k) {
                Some(&i) => {
                    let stv = &mut self.column_edges[i].statements;
                    if stv.last() != Some(&self.stmt_idx) {
                        stv.push(self.stmt_idx);
                    }
                }
                None => {
                    self.col_edge_map.insert(k, self.column_edges.len());
                    self.column_edges.push(ColumnEdge {
                        source_table: st,
                        source_column: self.syms.resolve(sc).to_string(),
                        target_table: tt,
                        target_column: self.syms.resolve(tc).to_string(),
                        kind: kind.as_str(),
                        statements: vec![self.stmt_idx],
                    });
                }
            }
        }
        self.cur_edges.clear();
        for &(tid, col) in &self.pending_cols {
            self.catalog.add_column(tid, col);
        }
        self.pending_cols.clear();
        (
            std::mem::take(&mut self.cur_targets),
            std::mem::take(&mut self.cur_sources),
        )
    }

    pub fn analyze(&mut self, stmt: &Statement) -> &'static str {
        match stmt {
            Statement::Insert(ins) => {
                self.do_insert(ins);
                "insert"
            }
            Statement::Directory { path, source, .. } => {
                let id = self.intern(&format!("dir:{path}"));
                if self.kinds[id] == "table" {
                    self.kinds[id] = "directory";
                }
                self.mark_target(id);
                self.query(source);
                "insert"
            }
            Statement::Query(q) => {
                let body_kind = match &*q.body {
                    SetExpr::Insert(_) => "insert",
                    SetExpr::Update(_) => "update",
                    SetExpr::Merge(_) => "merge",
                    _ => "",
                };
                let out = self.query(q);
                if !body_kind.is_empty() {
                    body_kind
                } else {
                    if self.standalone_query_mode && !self.cur_sources.is_empty() {
                        self.query_result_seq += 1;
                        let rname = if self.total_standalone_queries <= 1 {
                            "query_result".to_string()
                        } else {
                            format!("query_result_{}", self.query_result_seq)
                        };
                        let id = self.intern(&rname);
                        self.kinds[id] = "result";
                        self.mark_target(id);
                        let cols: Vec<ColId> = out.cols.iter().map(|c| c.name).collect();
                        self.catalog.register(id, cols.iter().copied());
                        self.map_target_ids(&out, id, &cols);
                    }
                    "query"
                }
            }
            Statement::CreateTable(ct) => self.do_create_table(ct),
            Statement::CreateView(cv) => self.do_create_view(cv),
            Statement::Merge(m) => {
                self.do_merge(m);
                "merge"
            }
            Statement::Update(u) => {
                self.do_update(u);
                "update"
            }
            _ => "other",
        }
    }

    /// 全部结束后导出表信息与边
    pub fn finish(self) -> (Vec<TableInfo>, Vec<TableEdge>, Vec<ColumnEdge>) {
        let syms = &self.syms;
        let tables = (0..self.names.len())
            .map(|i| {
                let columns = self
                    .catalog
                    .get(i)
                    .map(|cs| cs.iter().map(|&c| syms.resolve(c).to_string()).collect())
                    .unwrap_or_default();
                TableInfo {
                    id: i,
                    name: self.names[i].clone(),
                    kind: self.kinds[i],
                    is_source: self.is_src[i],
                    is_target: self.is_tgt[i],
                    columns,
                }
            })
            .collect();
        (tables, self.table_edges, self.column_edges)
    }

    #[inline]
    fn edge(&mut self, st: usize, sc: ColId, tt: usize, tc: ColId, kind: Kind) {
        let k = (st, sc, tt, tc, kind);
        if self.cur_edge_seen.insert(k) {
            self.cur_edges.push(k);
        }
    }

    // ---------- 语句级 ----------

    fn do_insert(&mut self, ins: &Insert) {
        let mut target: Option<usize> = None;
        match &ins.table {
            TableObject::TableName(n) => {
                let qn = self.qualify(&name_parts(n));
                let id = self.intern(&qn);
                self.mark_target(id);
                target = Some(id);
            }
            TableObject::TableQuery(q) => {
                self.query(q);
            }
            TableObject::TableFunction(_) => {}
        }
        // PARTITION(dt='x') 静态分区列 / PARTITION(dt) 动态列
        let mut static_p: HashSet<ColId> = HashSet::default();
        let mut dyn_p: Vec<ColId> = Vec::new();
        if let Some(parts) = &ins.partitioned {
            for e in parts {
                match e {
                    Expr::BinaryOp {
                        left,
                        op: BinaryOperator::Eq,
                        ..
                    } => {
                        let col = match left.as_ref() {
                            Expr::Identifier(id) => Some(lower_cow(&id.value)),
                            Expr::CompoundIdentifier(ids) => {
                                ids.last().map(|i| lower_cow(&i.value))
                            }
                            _ => None,
                        };
                        if let Some(c) = col {
                            let cid = self.intern_col(&c);
                            static_p.insert(cid);
                        }
                    }
                    Expr::Identifier(id) => {
                        let c = lower_cow(&id.value);
                        dyn_p.push(self.intern_col(&c));
                    }
                    Expr::CompoundIdentifier(ids) => {
                        if let Some(l) = ids.last() {
                            let c = lower_cow(&l.value);
                            dyn_p.push(self.intern_col(&c));
                        }
                    }
                    _ => {}
                }
            }
        }
        let mut explicit: Vec<ColId> = Vec::new();
        for o in &ins.columns {
            if let Some(p) = o.0.last() {
                let c = part_lower(p);
                explicit.push(self.intern_col(&c));
            }
        }
        for i in &ins.after_columns {
            let c = lower_cow(&i.value);
            explicit.push(self.intern_col(&c));
        }
        let out = match &ins.source {
            Some(q) => self.query(q),
            None => RelCols::default(),
        };
        let Some(tid) = target else { return };
        let col_ids: Vec<ColId> = if !explicit.is_empty() {
            explicit
        } else if let Some(cols) = self.catalog.get(tid) {
            let mut np: Vec<ColId> = cols
                .iter()
                .copied()
                .filter(|c| !static_p.contains(c) && !dyn_p.contains(c))
                .collect();
            np.extend(dyn_p.iter().copied());
            np
        } else {
            out.cols.iter().map(|c| c.name).collect()
        };
        self.map_target_ids(&out, tid, &col_ids);
    }

    fn do_create_table(&mut self, ct: &CreateTable) -> &'static str {
        let parts = name_parts(&ct.name);
        let qn = if ct.temporary {
            parts.join(".")
        } else {
            self.qualify(&parts)
        };
        let id = self.intern(&qn);
        self.mark_target(id);
        if ct.temporary {
            self.kinds[id] = "temp";
            if let Some(last) = parts.last() {
                self.temp.insert(last.clone());
            }
        }
        let mut cols: Vec<ColId> = ct
            .columns
            .iter()
            .map(|c| {
                let lc = lower_cow(&c.name.value);
                self.intern_col(&lc)
            })
            .collect();
        if let HiveDistributionStyle::PARTITIONED { columns } = &ct.hive_distribution {
            for c in columns {
                let lc = lower_cow(&c.name.value);
                cols.push(self.intern_col(&lc));
            }
        }
        if !cols.is_empty() {
            self.catalog.register(id, cols);
        }
        if let Some(like) = &ct.like {
            let lname = match like {
                CreateTableLikeKind::Parenthesized(l) | CreateTableLikeKind::Plain(l) => &l.name,
            };
            let lq = self.qualify(&name_parts(lname));
            self.copy_columns_from_name(id, &lq);
        }
        if let Some(cl) = &ct.clone {
            let cq = self.qualify(&name_parts(cl));
            self.copy_columns_from_name(id, &cq);
        }
        if let Some(q) = &ct.query {
            let out = self.query(q);
            let col_ids: Vec<ColId> = if !ct.columns.is_empty() {
                ct.columns
                    .iter()
                    .map(|c| {
                        let lc = lower_cow(&c.name.value);
                        self.intern_col(&lc)
                    })
                    .collect()
            } else {
                out.cols.iter().map(|c| c.name).collect()
            };
            self.catalog.register(id, col_ids.iter().copied());
            self.map_target_ids(&out, id, &col_ids);
            return "ctas";
        }
        "create_table"
    }

    fn do_create_view(&mut self, cv: &CreateView) -> &'static str {
        let parts = name_parts(&cv.name);
        let qn = if cv.temporary {
            parts.join(".")
        } else {
            self.qualify(&parts)
        };
        let id = self.intern(&qn);
        self.mark_target(id);
        self.kinds[id] = "view";
        if cv.temporary {
            if let Some(last) = parts.last() {
                self.temp.insert(last.clone());
            }
        }
        let out = self.query(&cv.query);
        let col_ids: Vec<ColId> = if !cv.columns.is_empty() {
            cv.columns
                .iter()
                .map(|c| {
                    let lc = lower_cow(&c.name.value);
                    self.intern_col(&lc)
                })
                .collect()
        } else {
            out.cols.iter().map(|c| c.name).collect()
        };
        self.catalog.register(id, col_ids.iter().copied());
        self.map_target_ids(&out, id, &col_ids);
        "create_view"
    }

    fn do_merge(&mut self, merge: &Merge) {
        let mut target: Option<usize> = None;
        if let TableFactor::Table { name, .. } = &merge.table {
            let qn = self.qualify(&name_parts(name));
            let id = self.intern(&qn);
            self.mark_target(id);
            target = Some(id);
        }
        let mut sc = Scope::default();
        self.add_factor_ext(&merge.source, &mut sc, true);
        let src_rel_count = sc.rels.len();
        // 将目标表加入作用域用于限定名消歧，但不主动标为语句源表以免制造虚假自环
        self.add_factor_ext(&merge.table, &mut sc, false);
        let _ = self.expr_srcs(&merge.on, &sc);

        let allow_self = target
            .map(|tid| self.cur_src_seen.contains(&tid))
            .unwrap_or(false);

        for clause in &merge.clauses {
            if let Some(pred) = &clause.predicate {
                let _ = self.expr_srcs(pred, &sc);
            }
            match &clause.action {
                MergeAction::Update(upd) => {
                    if let Some(p) = &upd.update_predicate {
                        let _ = self.expr_srcs(p, &sc);
                    }
                    if let Some(p) = &upd.delete_predicate {
                        let _ = self.expr_srcs(p, &sc);
                    }
                    match &upd.kind {
                        MergeUpdateKind::Set(assigns) => {
                            self.apply_assignments(assigns, &sc, target, allow_self);
                        }
                        MergeUpdateKind::Wildcard => {
                            if let Some(tid) = target {
                                let mut out = RelCols::default();
                                for r in &sc.rels[..src_rel_count] {
                                    expand_rel(r, &mut out);
                                }
                                let cols: Vec<ColId> = out.cols.iter().map(|c| c.name).collect();
                                self.map_target_ids_filtered(&out, tid, &cols, allow_self);
                            }
                        }
                    }
                }
                MergeAction::Insert(ins) => {
                    if let Some(p) = &ins.insert_predicate {
                        let _ = self.expr_srcs(p, &sc);
                    }
                    let Some(tid) = target else { continue };
                    match &ins.kind {
                        MergeInsertKind::Values(vals) => {
                            let explicit: Vec<ColId> = ins
                                .columns
                                .iter()
                                .filter_map(|o| {
                                    o.0.last().map(|p| {
                                        let c = part_lower(p);
                                        self.intern_col(&c)
                                    })
                                })
                                .collect();
                            let base_cols: Option<Vec<ColId>> = if !explicit.is_empty() {
                                Some(explicit)
                            } else {
                                self.catalog.get(tid).map(|s| s.to_vec())
                            };
                            for row in &vals.rows {
                                let tcols: Vec<ColId> = match &base_cols {
                                    Some(c) => c.clone(),
                                    None => row
                                        .iter()
                                        .enumerate()
                                        .map(|(idx, ex)| match item_name(ex) {
                                            Some(n) => self.intern_col(&n),
                                            None => self.intern_col(&format!("_c{idx}")),
                                        })
                                        .collect(),
                                };
                                for &tc in &tcols {
                                    if tc != STAR_COL {
                                        self.pending_cols.push((tid, tc));
                                    }
                                }
                                let n = tcols.len().min(row.len());
                                for i in 0..n {
                                    let srcs = self.expr_srcs(&row[i], &sc);
                                    for s in srcs {
                                        if allow_self || s.table != tid {
                                            self.edge(s.table, s.col, tid, tcols[i], s.kind);
                                        }
                                    }
                                }
                            }
                        }
                        MergeInsertKind::Wildcard | MergeInsertKind::Row => {
                            let mut out = RelCols::default();
                            for r in &sc.rels[..src_rel_count] {
                                expand_rel(r, &mut out);
                            }
                            let cols: Vec<ColId> = if !ins.columns.is_empty() {
                                ins.columns
                                    .iter()
                                    .filter_map(|o| {
                                        o.0.last().map(|p| {
                                            let c = part_lower(p);
                                            self.intern_col(&c)
                                        })
                                    })
                                    .collect()
                            } else {
                                out.cols.iter().map(|c| c.name).collect()
                            };
                            self.map_target_ids_filtered(&out, tid, &cols, allow_self);
                        }
                    }
                }
                MergeAction::Delete { .. } | MergeAction::DoNothing { .. } => {}
            }
        }
    }

    fn do_update(&mut self, upd: &Update) {
        let mut target: Option<usize> = None;
        if let TableFactor::Table { name, .. } = &upd.table.relation {
            let qn = self.qualify(&name_parts(name));
            let id = self.intern(&qn);
            self.mark_target(id);
            target = Some(id);
        }
        let mut sc = Scope::default();
        self.add_factor_ext(&upd.table.relation, &mut sc, false);
        for j in &upd.table.joins {
            self.add_factor_ext(&j.relation, &mut sc, true);
            if let Some(JoinConstraint::On(e)) = join_constraint(&j.join_operator) {
                let _ = self.expr_srcs(e, &sc);
            }
        }
        if let Some(from_kind) = &upd.from {
            let twjs = match from_kind {
                UpdateTableFromKind::BeforeSet(v) | UpdateTableFromKind::AfterSet(v) => v,
            };
            for twj in twjs {
                self.add_twj(twj, &mut sc);
            }
        }
        if let Some(sel) = &upd.selection {
            let _ = self.expr_srcs(sel, &sc);
        }
        let allow_self = target
            .map(|tid| self.cur_src_seen.contains(&tid))
            .unwrap_or(false);
        self.apply_assignments(&upd.assignments, &sc, target, allow_self);
    }

    fn apply_assignments(
        &mut self,
        assigns: &[Assignment],
        sc: &Scope,
        target: Option<usize>,
        allow_self: bool,
    ) {
        for a in assigns {
            let srcs = self.expr_srcs(&a.value, sc);
            let Some(tid) = target else { continue };
            match &a.target {
                AssignmentTarget::ColumnName(on) => {
                    if let Some(last) = on.0.last() {
                        let c = part_lower(last);
                        let tcol = self.intern_col(&c);
                        if tcol != STAR_COL {
                            self.pending_cols.push((tid, tcol));
                        }
                        for s in &srcs {
                            if allow_self || s.table != tid {
                                self.edge(s.table, s.col, tid, tcol, s.kind);
                            }
                        }
                    }
                }
                AssignmentTarget::Tuple(ons) => {
                    for on in ons {
                        if let Some(last) = on.0.last() {
                            let c = part_lower(last);
                            let tcol = self.intern_col(&c);
                            if tcol != STAR_COL {
                                self.pending_cols.push((tid, tcol));
                            }
                            for s in &srcs {
                                if allow_self || s.table != tid {
                                    self.edge(s.table, s.col, tid, tcol, s.kind);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// 按位置把 select 输出列映射到目标列并发射列边
    fn map_target_ids(&mut self, out: &RelCols, tid: usize, col_ids: &[ColId]) {
        self.map_target_ids_filtered(out, tid, col_ids, true);
    }

    fn map_target_ids_filtered(
        &mut self,
        out: &RelCols,
        tid: usize,
        col_ids: &[ColId],
        allow_self: bool,
    ) {
        for &cid in col_ids {
            if cid != STAR_COL {
                self.pending_cols.push((tid, cid));
            }
        }
        let n = col_ids.len().min(out.cols.len());
        for (i, &tc) in col_ids.iter().enumerate().take(n) {
            let srcs = out.cols[i].srcs.clone();
            for s in srcs.iter() {
                if allow_self || s.table != tid {
                    self.edge(s.table, s.col, tid, tc, s.kind);
                }
            }
        }
    }

    // ---------- 查询 ----------

    pub fn query(&mut self, q: &Query) -> RelCols {
        let mut saved: Vec<(String, Option<CteEntry>)> = Vec::new();
        if let Some(w) = &q.with {
            saved.reserve(w.cte_tables.len());
            for cte in &w.cte_tables {
                let key = lower_cow(&cte.alias.name.value).into_owned();
                let prev = self.ctes.get(&key).cloned();
                if w.recursive {
                    self.ctes.insert(
                        key.clone(),
                        CteEntry {
                            cols: self.empty_cols.clone(),
                            unknown: false,
                        },
                    );
                }
                let mut rc = self.query(&cte.query);
                if !cte.alias.columns.is_empty() {
                    for (i, cd) in cte.alias.columns.iter().enumerate() {
                        if i < rc.cols.len() {
                            let lc = lower_cow(&cd.name.value);
                            rc.cols[i].name = self.intern_col(&lc);
                        }
                    }
                }
                let entry = CteEntry {
                    cols: Rc::new(rc.cols),
                    unknown: rc.unknown,
                };
                let old = self.ctes.insert(key.clone(), entry);
                // 仅保存进入本 WITH 块之前的原始映射
                if !saved.iter().any(|(k, _)| k == &key) {
                    saved.push((key, if w.recursive { prev } else { old }));
                }
            }
        }
        let out = self.set_expr(&q.body);
        if let Some(ob) = &q.order_by {
            if let OrderByKind::Expressions(es) = &ob.kind {
                let sc = Scope::default();
                for e in es {
                    self.expr_srcs(&e.expr, &sc);
                }
            }
        }
        for (k, prev) in saved {
            match prev {
                Some(v) => {
                    self.ctes.insert(k, v);
                }
                None => {
                    self.ctes.remove(&k);
                }
            }
        }
        out
    }

    fn set_expr(&mut self, se: &SetExpr) -> RelCols {
        match se {
            SetExpr::Select(s) => self.select(s),
            SetExpr::Query(q) => self.query(q),
            SetExpr::SetOperation { left, right, .. } => {
                let l = self.set_expr(left);
                let r = self.set_expr(right);
                union_merge(l, r)
            }
            SetExpr::Values(v) => {
                let n = v.rows.first().map(|r| r.len()).unwrap_or(0);
                let cols = (0..n)
                    .map(|i| ColOut {
                        name: self.intern_col(&format!("_c{i}")),
                        srcs: Rc::new(Srcs::new()),
                    })
                    .collect();
                RelCols {
                    cols,
                    unknown: false,
                }
            }
            SetExpr::Insert(s) => {
                if let Statement::Insert(ins) = s {
                    self.do_insert(ins);
                }
                RelCols::default()
            }
            SetExpr::Update(s) => {
                if let Statement::Update(u) = s {
                    self.do_update(u);
                }
                RelCols::default()
            }
            SetExpr::Merge(s) => {
                if let Statement::Merge(m) = s {
                    self.do_merge(m);
                }
                RelCols::default()
            }
            _ => RelCols::default(),
        }
    }

    fn select(&mut self, sel: &Select) -> RelCols {
        let mut sc = Scope::default();
        for NamedWindowDefinition(ident, def) in &sel.named_window {
            if let NamedWindowExpr::WindowSpec(ws) = def {
                sc.nwin.insert(lower_cow(&ident.value).into_owned(), ws);
            }
        }
        for twj in &sel.from {
            self.add_twj(twj, &mut sc);
        }
        for lv in &sel.lateral_views {
            self.add_lateral(lv, &mut sc);
        }
        let mut out = RelCols::with_capacity(sel.projection.len());
        for item in &sel.projection {
            match item {
                SelectItem::Wildcard(_) => self.expand_all(&sc, &mut out),
                SelectItem::QualifiedWildcard(kind, _) => {
                    if let SelectItemQualifiedWildcardKind::ObjectName(on) = kind {
                        let q = name_parts(on).join(".");
                        if let Some(r) = sc.rels.iter().find(|r| r.names.iter().any(|n| n == &q)) {
                            expand_rel(r, &mut out);
                        } else {
                            out.unknown = true;
                        }
                    } else {
                        out.unknown = true;
                    }
                }
                SelectItem::UnnamedExpr(e) => {
                    let srcs = self.expr_srcs(e, &sc);
                    let pos = out.cols.len();
                    let name_id = match item_name(e) {
                        Some(n) => self.intern_col(&n),
                        None => self.intern_col(&format!("_c{pos}")),
                    };
                    out.cols.push(ColOut {
                        name: name_id,
                        srcs: Rc::new(srcs),
                    });
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let srcs = self.expr_srcs(expr, &sc);
                    let lc = lower_cow(&alias.value);
                    let name_id = self.intern_col(&lc);
                    out.cols.push(ColOut {
                        name: name_id,
                        srcs: Rc::new(srcs),
                    });
                }
                SelectItem::ExprWithAliases { expr, aliases } => {
                    let srcs = Rc::new(self.expr_srcs(expr, &sc));
                    for a in aliases {
                        let lc = lower_cow(&a.value);
                        let name_id = self.intern_col(&lc);
                        out.cols.push(ColOut {
                            name: name_id,
                            srcs: srcs.clone(),
                        });
                    }
                }
            }
        }
        for e in [&sel.selection, &sel.prewhere, &sel.having, &sel.qualify]
            .into_iter()
            .flatten()
        {
            self.expr_srcs(e, &sc);
        }
        if let GroupByExpr::Expressions(exprs, _) = &sel.group_by {
            for e in exprs {
                self.expr_srcs(e, &sc);
            }
        }
        for e in sel
            .cluster_by
            .iter()
            .chain(sel.distribute_by.iter())
            .chain(sel.sort_by.iter().map(|o| &o.expr))
        {
            self.expr_srcs(e, &sc);
        }
        out
    }

    // ---------- FROM 作用域 ----------

    fn add_twj(&mut self, twj: &TableWithJoins, sc: &mut Scope) {
        self.add_factor_ext(&twj.relation, sc, true);
        for j in &twj.joins {
            self.add_factor_ext(&j.relation, sc, true);
            if let JoinOperator::AsOf {
                match_condition,
                constraint,
            } = &j.join_operator
            {
                self.expr_srcs(match_condition, sc);
                if let JoinConstraint::On(e) = constraint {
                    self.expr_srcs(e, sc);
                }
            } else if let Some(JoinConstraint::On(e)) = join_constraint(&j.join_operator) {
                self.expr_srcs(e, sc);
            }
        }
    }

    fn add_factor_ext(&mut self, tf: &TableFactor, sc: &mut Scope, mark_src: bool) {
        match tf {
            TableFactor::Table { name, alias, .. } => {
                let parts = name_parts(name);
                if parts.len() == 1 {
                    if let Some(rc) = self.ctes.get(&parts[0]) {
                        let an = alias
                            .as_ref()
                            .map(|a| lower_cow(&a.name.value).into_owned())
                            .unwrap_or_else(|| parts[0].clone());
                        sc.rels.push(Rel {
                            names: smallvec::smallvec![an],
                            base: None,
                            cols: rc.cols.clone(),
                            known: !rc.unknown,
                        });
                        return;
                    }
                }
                let qn = self.qualify(&parts);
                let id = self.intern(&qn);
                if mark_src {
                    self.mark_source(id);
                }
                let mut names: smallvec::SmallVec<[String; 3]> = smallvec::SmallVec::new();
                if let Some(a) = alias {
                    names.push(lower_cow(&a.name.value).into_owned());
                }
                names.push(parts.join("."));
                if let Some(last) = parts.last() {
                    if names.last().map(String::as_str) != Some(last.as_str()) {
                        names.push(last.clone());
                    }
                }
                let (cols, known) = match self.catalog.get(id) {
                    Some(cl) => {
                        let ver = self.catalog.version(id);
                        if self.rel_cache.len() <= id {
                            self.rel_cache.resize(id + 1, None);
                        }
                        let cached = match &self.rel_cache[id] {
                            Some((cv, rc)) if *cv == ver => rc.clone(),
                            _ => {
                                let built: Rc<Vec<ColOut>> = Rc::new(
                                    cl.iter()
                                        .map(|&c| ColOut {
                                            name: c,
                                            srcs: Rc::new(smallvec::smallvec![ColSrc {
                                                table: id,
                                                col: c,
                                                kind: Kind::Direct,
                                            }]),
                                        })
                                        .collect(),
                                );
                                self.rel_cache[id] = Some((ver, built.clone()));
                                built
                            }
                        };
                        (cached, true)
                    }
                    None => (self.empty_cols.clone(), false),
                };
                sc.rels.push(Rel {
                    names,
                    base: Some(id),
                    cols,
                    known,
                });
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let mut rc = self.query(subquery);
                let mut names: smallvec::SmallVec<[String; 3]> = smallvec::SmallVec::new();
                if let Some(a) = alias {
                    names.push(lower_cow(&a.name.value).into_owned());
                    if !a.columns.is_empty() {
                        for (i, cd) in a.columns.iter().enumerate() {
                            if i < rc.cols.len() {
                                let lc = lower_cow(&cd.name.value);
                                rc.cols[i].name = self.intern_col(&lc);
                            }
                        }
                    }
                }
                sc.rels.push(Rel {
                    names,
                    base: None,
                    cols: Rc::new(rc.cols),
                    known: !rc.unknown,
                });
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.add_twj(table_with_joins, sc);
            }
            TableFactor::Pivot { table, .. }
            | TableFactor::Unpivot { table, .. }
            | TableFactor::MatchRecognize { table, .. } => {
                self.add_factor_ext(table, sc, mark_src);
            }
            TableFactor::Function {
                args, alias, name, ..
            } => {
                let mut star = Srcs::new();
                for a in args {
                    if let Some(e) = func_arg_expr(a) {
                        star.extend(self.expr_srcs(e, sc));
                    }
                }
                let n = alias
                    .as_ref()
                    .map(|a| lower_cow(&a.name.value).into_owned())
                    .unwrap_or_else(|| {
                        name.0
                            .last()
                            .map(|p| part_lower(p).into_owned())
                            .unwrap_or_default()
                    });
                sc.rels.push(Rel {
                    names: smallvec::smallvec![n],
                    base: None,
                    cols: Rc::new(vec![ColOut {
                        name: STAR_COL,
                        srcs: Rc::new(star),
                    }]),
                    known: false,
                });
            }
            TableFactor::UNNEST {
                array_exprs, alias, ..
            } => {
                let mut star = Srcs::new();
                for e in array_exprs {
                    star.extend(self.expr_srcs(e, sc));
                }
                let n = alias
                    .as_ref()
                    .map(|a| lower_cow(&a.name.value).into_owned())
                    .unwrap_or_default();
                sc.rels.push(Rel {
                    names: smallvec::smallvec![n],
                    base: None,
                    cols: Rc::new(vec![ColOut {
                        name: STAR_COL,
                        srcs: Rc::new(star),
                    }]),
                    known: false,
                });
            }
            TableFactor::TableFunction { expr, alias } => {
                let star = self.expr_srcs(expr, sc);
                let n = alias
                    .as_ref()
                    .map(|a| lower_cow(&a.name.value).into_owned())
                    .unwrap_or_default();
                sc.rels.push(Rel {
                    names: smallvec::smallvec![n],
                    base: None,
                    cols: Rc::new(vec![ColOut {
                        name: STAR_COL,
                        srcs: Rc::new(star),
                    }]),
                    known: false,
                });
            }
            TableFactor::JsonTable {
                json_expr, alias, ..
            }
            | TableFactor::OpenJsonTable {
                json_expr, alias, ..
            } => {
                let star = self.expr_srcs(json_expr, sc);
                let n = alias
                    .as_ref()
                    .map(|a| lower_cow(&a.name.value).into_owned())
                    .unwrap_or_default();
                sc.rels.push(Rel {
                    names: smallvec::smallvec![n],
                    base: None,
                    cols: Rc::new(vec![ColOut {
                        name: STAR_COL,
                        srcs: Rc::new(star),
                    }]),
                    known: false,
                });
            }
            _ => {}
        }
    }

    fn add_lateral(&mut self, lv: &LateralView, sc: &mut Scope) {
        let srcs = wrap(self.expr_srcs(&lv.lateral_view, sc), Kind::Derived);
        let shared = Rc::new(srcs);
        let name = lv
            .lateral_view_name
            .0
            .last()
            .map(|p| part_lower(p).into_owned())
            .unwrap_or_default();
        let col_ids: Vec<ColId> = if !lv.lateral_col_alias.is_empty() {
            lv.lateral_col_alias
                .iter()
                .map(|i| {
                    let lc = lower_cow(&i.value);
                    self.intern_col(&lc)
                })
                .collect()
        } else {
            let is_posexplode = match &lv.lateral_view {
                Expr::Function(f) => f
                    .name
                    .0
                    .last()
                    .map(|p| part_lower(p).eq_ignore_ascii_case("posexplode"))
                    .unwrap_or(false),
                _ => false,
            };
            if is_posexplode {
                vec![self.intern_col("pos"), self.intern_col("val")]
            } else {
                vec![self.intern_col("col")]
            }
        };
        sc.rels.push(Rel {
            names: smallvec::smallvec![name],
            base: None,
            cols: Rc::new(
                col_ids
                    .into_iter()
                    .map(|cid| ColOut {
                        name: cid,
                        srcs: shared.clone(),
                    })
                    .collect(),
            ),
            known: true,
        });
    }

    fn expand_all(&mut self, sc: &Scope, out: &mut RelCols) {
        for r in &sc.rels {
            expand_rel(r, out);
        }
    }

    // ---------- 列解析 ----------

    fn resolve_in(&mut self, r: &Rel, col_id: ColId) -> Srcs {
        if let Some(id) = r.base {
            if col_id != STAR_COL {
                self.pending_cols.push((id, col_id));
            }
            return smallvec::smallvec![ColSrc {
                table: id,
                col: col_id,
                kind: Kind::Direct,
            }];
        }
        if let Some(c) = r.cols.iter().find(|c| c.name == col_id) {
            return (*c.srcs).clone();
        }
        if let Some(c) = r.cols.iter().find(|c| c.name == STAR_COL) {
            return (*c.srcs).clone();
        }
        Srcs::new()
    }

    fn resolve_unq(&mut self, col_id: ColId, sc: &Scope) -> Srcs {
        if sc.rels.len() == 1 {
            return self.resolve_in(&sc.rels[0], col_id);
        }
        let mut cand: Option<usize> = None;
        let mut multi = false;
        for (i, r) in sc.rels.iter().enumerate() {
            if r.known && r.cols.iter().any(|c| c.name == col_id) {
                if cand.is_some() {
                    multi = true;
                    break;
                }
                cand = Some(i);
            }
        }
        if multi {
            return Srcs::new();
        }
        if let Some(i) = cand {
            return self.resolve_in(&sc.rels[i], col_id);
        }
        let mut unk_idx: Option<usize> = None;
        for (i, r) in sc.rels.iter().enumerate() {
            if !r.known {
                if unk_idx.is_some() {
                    return Srcs::new();
                }
                unk_idx = Some(i);
            }
        }
        if let Some(idx) = unk_idx {
            return self.resolve_in(&sc.rels[idx], col_id);
        }
        Srcs::new()
    }

    fn resolve_q(&mut self, qualifier: &str, col_id: ColId, sc: &Scope) -> Srcs {
        for r in &sc.rels {
            if r.names.iter().any(|n| n == qualifier) {
                return self.resolve_in(r, col_id);
            }
        }
        Srcs::new()
    }

    // ---------- 表达式 ----------

    fn func_srcs(&mut self, f: &Function, sc: &Scope) -> Srcs {
        let wrapk = f
            .name
            .0
            .last()
            .map(|p| {
                let lc = part_lower(p);
                if is_aggregate(&lc) {
                    Kind::Aggregate
                } else {
                    Kind::Derived
                }
            })
            .unwrap_or(Kind::Derived);
        let mut v = Srcs::new();
        for arglist in [&f.args, &f.parameters] {
            match arglist {
                FunctionArguments::List(l) => {
                    for a in &l.args {
                        if let Some(e) = func_arg_expr(a) {
                            v.extend(self.expr_srcs(e, sc));
                        }
                    }
                    for c in &l.clauses {
                        match c {
                            FunctionArgumentClause::OrderBy(obs) => {
                                for o in obs {
                                    v.extend(self.expr_srcs(&o.expr, sc));
                                }
                            }
                            FunctionArgumentClause::Where(e) | FunctionArgumentClause::Limit(e) => {
                                v.extend(self.expr_srcs(e, sc));
                            }
                            _ => {}
                        }
                    }
                }
                FunctionArguments::Subquery(q) => {
                    let rc = self.query(q);
                    if let Some(c) = rc.cols.first() {
                        v.extend((*c.srcs).clone());
                    }
                }
                FunctionArguments::None => {}
            }
        }
        for w in &f.within_group {
            v.extend(self.expr_srcs(&w.expr, sc));
        }
        if let Some(fe) = &f.filter {
            v.extend(self.expr_srcs(fe, sc));
        }
        let mut v = wrap(v, wrapk);
        let mut win = Srcs::new();
        match &f.over {
            Some(WindowType::WindowSpec(ws)) => self.window_srcs(ws, sc, &mut win),
            Some(WindowType::NamedWindow(id)) => {
                let lc = lower_cow(&id.value);
                if let Some(&ws) = sc.nwin.get(lc.as_ref()) {
                    self.window_srcs(ws, sc, &mut win);
                }
            }
            None => {}
        }
        v.extend(wrap(win, Kind::Derived));
        v
    }

    fn window_srcs(&mut self, ws: &WindowSpec, sc: &Scope, out: &mut Srcs) {
        for e in &ws.partition_by {
            out.extend(self.expr_srcs(e, sc));
        }
        for o in &ws.order_by {
            out.extend(self.expr_srcs(&o.expr, sc));
        }
    }

    fn expr_srcs(&mut self, e: &Expr, sc: &Scope) -> Srcs {
        match e {
            Expr::Identifier(id) => {
                let v = lower_cow(&id.value);
                if is_placeholder(&v) {
                    Srcs::new()
                } else {
                    let cid = self.intern_col(&v);
                    self.resolve_unq(cid, sc)
                }
            }
            Expr::CompoundIdentifier(ids) => match ids.as_slice() {
                [] => Srcs::new(),
                [single] => {
                    let v = lower_cow(&single.value);
                    if is_placeholder(&v) {
                        Srcs::new()
                    } else {
                        let cid = self.intern_col(&v);
                        self.resolve_unq(cid, sc)
                    }
                }
                [qual_single, col_ident] => {
                    let col = lower_cow(&col_ident.value);
                    let cid = self.intern_col(&col);
                    let q = lower_cow(&qual_single.value);
                    self.resolve_q(&q, cid, sc)
                }
                many => {
                    let col = lower_cow(&many.last().unwrap().value);
                    let cid = self.intern_col(&col);
                    let mut q = String::new();
                    for (idx, item) in many[..many.len() - 1].iter().enumerate() {
                        if idx > 0 {
                            q.push('.');
                        }
                        q.push_str(&lower_cow(&item.value));
                    }
                    self.resolve_q(&q, cid, sc)
                }
            },
            Expr::CompoundFieldAccess { root, access_chain } => {
                let mut v = self.expr_srcs(root, sc);
                for a in access_chain {
                    match a {
                        AccessExpr::Dot(x) => v.extend(self.expr_srcs(x, sc)),
                        AccessExpr::Subscript(Subscript::Index { index }) => {
                            v.extend(self.expr_srcs(index, sc))
                        }
                        _ => {}
                    }
                }
                wrap(v, Kind::Derived)
            }
            Expr::JsonAccess { value, .. } => wrap(self.expr_srcs(value, sc), Kind::Derived),
            Expr::Nested(x) => self.expr_srcs(x, sc),
            Expr::Cast { expr: x, .. }
            | Expr::Collate { expr: x, .. }
            | Expr::Prior(x)
            | Expr::OuterJoin(x)
            | Expr::Named { expr: x, .. }
            | Expr::Prefixed { value: x, .. } => self.expr_srcs(x, sc),
            Expr::Value(_)
            | Expr::Wildcard(_)
            | Expr::QualifiedWildcard(..)
            | Expr::TypedString(_) => Srcs::new(),
            Expr::Function(f) => self.func_srcs(f, sc),
            Expr::Subquery(q) => {
                let rc = self.query(q);
                wrap(
                    rc.cols
                        .first()
                        .map(|c| (*c.srcs).clone())
                        .unwrap_or_default(),
                    Kind::Derived,
                )
            }
            Expr::Exists { subquery, .. } => {
                self.query(subquery);
                Srcs::new()
            }
            Expr::InSubquery { expr, subquery, .. } => {
                self.query(subquery);
                wrap(self.expr_srcs(expr, sc), Kind::Derived)
            }
            Expr::InList { expr, list, .. } => {
                let mut v = self.expr_srcs(expr, sc);
                for x in list {
                    v.extend(self.expr_srcs(x, sc));
                }
                wrap(v, Kind::Derived)
            }
            Expr::InUnnest {
                expr, array_expr, ..
            } => {
                let mut v = self.expr_srcs(expr, sc);
                v.extend(self.expr_srcs(array_expr, sc));
                wrap(v, Kind::Derived)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                let mut v = self.expr_srcs(expr, sc);
                v.extend(self.expr_srcs(low, sc));
                v.extend(self.expr_srcs(high, sc));
                wrap(v, Kind::Derived)
            }
            Expr::BinaryOp { left, right, .. } => {
                let mut v = self.expr_srcs(left, sc);
                v.extend(self.expr_srcs(right, sc));
                wrap(v, Kind::Derived)
            }
            Expr::UnaryOp { expr, .. } => wrap(self.expr_srcs(expr, sc), Kind::Derived),
            Expr::Like {
                expr,
                pattern,
                escape_char,
                ..
            }
            | Expr::ILike {
                expr,
                pattern,
                escape_char,
                ..
            }
            | Expr::SimilarTo {
                expr,
                pattern,
                escape_char,
                ..
            } => {
                let mut v = self.expr_srcs(expr, sc);
                v.extend(self.expr_srcs(pattern, sc));
                if let Some(e) = escape_char {
                    v.extend(self.expr_srcs(e, sc));
                }
                wrap(v, Kind::Derived)
            }
            Expr::RLike { expr, pattern, .. } => {
                let mut v = self.expr_srcs(expr, sc);
                v.extend(self.expr_srcs(pattern, sc));
                wrap(v, Kind::Derived)
            }
            Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
                let mut v = self.expr_srcs(left, sc);
                v.extend(self.expr_srcs(right, sc));
                wrap(v, Kind::Derived)
            }
            Expr::IsFalse(x)
            | Expr::IsNotFalse(x)
            | Expr::IsTrue(x)
            | Expr::IsNotTrue(x)
            | Expr::IsNull(x)
            | Expr::IsNotNull(x)
            | Expr::IsUnknown(x)
            | Expr::IsNotUnknown(x) => wrap(self.expr_srcs(x, sc), Kind::Derived),
            Expr::IsDistinctFrom(a, b) | Expr::IsNotDistinctFrom(a, b) => {
                let mut v = self.expr_srcs(a, sc);
                v.extend(self.expr_srcs(b, sc));
                wrap(v, Kind::Derived)
            }
            Expr::IsJson { expr, .. } | Expr::IsNormalized { expr, .. } => {
                wrap(self.expr_srcs(expr, sc), Kind::Derived)
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                let mut v = Srcs::new();
                if let Some(o) = operand {
                    v.extend(self.expr_srcs(o, sc));
                }
                for c in conditions {
                    v.extend(self.expr_srcs(&c.condition, sc));
                    v.extend(self.expr_srcs(&c.result, sc));
                }
                if let Some(el) = else_result {
                    v.extend(self.expr_srcs(el, sc));
                }
                wrap(v, Kind::Derived)
            }
            Expr::Convert { expr, styles, .. } => {
                let mut v = self.expr_srcs(expr, sc);
                for s in styles {
                    v.extend(self.expr_srcs(s, sc));
                }
                wrap(v, Kind::Derived)
            }
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => {
                let mut v = self.expr_srcs(timestamp, sc);
                v.extend(self.expr_srcs(time_zone, sc));
                wrap(v, Kind::Derived)
            }
            Expr::Extract { expr, .. } | Expr::Ceil { expr, .. } | Expr::Floor { expr, .. } => {
                wrap(self.expr_srcs(expr, sc), Kind::Derived)
            }
            Expr::Position { expr, r#in } => {
                let mut v = self.expr_srcs(expr, sc);
                v.extend(self.expr_srcs(r#in, sc));
                wrap(v, Kind::Derived)
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let mut v = self.expr_srcs(expr, sc);
                for o in [substring_from, substring_for].into_iter().flatten() {
                    v.extend(self.expr_srcs(o, sc));
                }
                wrap(v, Kind::Derived)
            }
            Expr::Trim {
                expr,
                trim_what,
                trim_characters,
                ..
            } => {
                let mut v = self.expr_srcs(expr, sc);
                if let Some(w) = trim_what {
                    v.extend(self.expr_srcs(w, sc));
                }
                for c in trim_characters.iter().flatten() {
                    v.extend(self.expr_srcs(c, sc));
                }
                wrap(v, Kind::Derived)
            }
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                let mut v = self.expr_srcs(expr, sc);
                v.extend(self.expr_srcs(overlay_what, sc));
                v.extend(self.expr_srcs(overlay_from, sc));
                if let Some(o) = overlay_for {
                    v.extend(self.expr_srcs(o, sc));
                }
                wrap(v, Kind::Derived)
            }
            Expr::Tuple(list) => {
                let mut v = Srcs::new();
                for x in list {
                    v.extend(self.expr_srcs(x, sc));
                }
                wrap(v, Kind::Derived)
            }
            Expr::GroupingSets(sets) | Expr::Cube(sets) | Expr::Rollup(sets) => {
                let mut v = Srcs::new();
                for s in sets {
                    for x in s {
                        v.extend(self.expr_srcs(x, sc));
                    }
                }
                wrap(v, Kind::Derived)
            }
            Expr::Struct { values, .. } => {
                let mut v = Srcs::new();
                for x in values {
                    v.extend(self.expr_srcs(x, sc));
                }
                wrap(v, Kind::Derived)
            }
            Expr::Dictionary(fields) => {
                let mut v = Srcs::new();
                for f2 in fields {
                    v.extend(self.expr_srcs(&f2.value, sc));
                }
                wrap(v, Kind::Derived)
            }
            Expr::Map(m) => {
                let mut v = Srcs::new();
                for en in &m.entries {
                    v.extend(self.expr_srcs(&en.key, sc));
                    v.extend(self.expr_srcs(&en.value, sc));
                }
                wrap(v, Kind::Derived)
            }
            Expr::Array(a) => {
                let mut v = Srcs::new();
                for x in &a.elem {
                    v.extend(self.expr_srcs(x, sc));
                }
                wrap(v, Kind::Derived)
            }
            Expr::Interval(i) => wrap(self.expr_srcs(&i.value, sc), Kind::Derived),
            Expr::Lambda(l) => wrap(self.expr_srcs(&l.body, sc), Kind::Derived),
            Expr::MemberOf(m) => {
                let mut v = self.expr_srcs(&m.value, sc);
                v.extend(self.expr_srcs(&m.array, sc));
                wrap(v, Kind::Derived)
            }
            Expr::MatchAgainst { columns, .. } => {
                let mut v = Srcs::new();
                for c in columns {
                    let parts = name_parts(c);
                    if let Some(col) = parts.last() {
                        let cid = self.intern_col(col);
                        let q = parts[..parts.len() - 1].join(".");
                        v.extend(self.resolve_q(&q, cid, sc));
                    }
                }
                wrap(v, Kind::Derived)
            }
        }
    }
}

impl RelCols {
    #[inline]
    fn with_capacity(cap: usize) -> Self {
        RelCols {
            cols: Vec::with_capacity(cap),
            unknown: false,
        }
    }
}

fn join_constraint(op: &JoinOperator) -> Option<&JoinConstraint> {
    match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c)
        | JoinOperator::Semi(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::Anti(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c)
        | JoinOperator::StraightJoin(c) => Some(c),
        _ => None,
    }
}

fn func_arg_expr(a: &FunctionArg) -> Option<&Expr> {
    match a {
        FunctionArg::Named { arg, .. }
        | FunctionArg::ExprNamed { arg, .. }
        | FunctionArg::Unnamed(arg) => match arg {
            FunctionArgExpr::Expr(e) => Some(e),
            _ => None,
        },
    }
}

fn expand_rel(r: &Rel, out: &mut RelCols) {
    if let Some(id) = r.base {
        if r.known {
            out.cols.extend(r.cols.iter().cloned());
        } else {
            out.cols.push(ColOut {
                name: STAR_COL,
                srcs: Rc::new(smallvec::smallvec![ColSrc {
                    table: id,
                    col: STAR_COL,
                    kind: Kind::Direct,
                }]),
            });
            out.unknown = true;
        }
    } else {
        if !r.known {
            out.unknown = true;
        }
        out.cols.extend(r.cols.iter().cloned());
    }
}

/// UNION 等集合操作按位置合并（名字取左分支）
fn union_merge(mut l: RelCols, mut r: RelCols) -> RelCols {
    let n = l.cols.len().min(r.cols.len());
    for i in 0..n {
        let mut srcs = (*l.cols[i].srcs).clone();
        for s in r.cols[i].srcs.iter() {
            if !srcs.contains(s) {
                srcs.push(*s);
            }
        }
        l.cols[i].srcs = Rc::new(srcs);
    }
    if r.cols.len() > l.cols.len() {
        let l_star = l
            .cols
            .iter()
            .find(|c| c.name == STAR_COL)
            .map(|c| c.srcs.clone());
        for mut c in r.cols.drain(n..) {
            if let Some(ref star_srcs) = l_star {
                let mut srcs = (*c.srcs).clone();
                for s in star_srcs.iter() {
                    if !srcs.contains(s) {
                        srcs.push(*s);
                    }
                }
                c.srcs = Rc::new(srcs);
            }
            l.cols.push(c);
        }
    } else if l.cols.len() > r.cols.len() {
        if let Some(r_star) = r.cols.iter().find(|c| c.name == STAR_COL) {
            for c in &mut l.cols[n..] {
                let mut srcs = (*c.srcs).clone();
                for s in r_star.srcs.iter() {
                    if !srcs.contains(s) {
                        srcs.push(*s);
                    }
                }
                c.srcs = Rc::new(srcs);
            }
        }
    }
    l.unknown |= r.unknown;
    l
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::{ClickHouseDialect, Dialect, GenericDialect};
    use sqlparser::parser::Parser;

    /// 用指定方言解析并驱动 Analyzer（覆盖 Hive 方言不会产生的 AST 变体）
    fn run_dialect(
        d: &dyn Dialect,
        sql: &str,
    ) -> (Vec<TableInfo>, Vec<TableEdge>, Vec<ColumnEdge>) {
        let stmts = Parser::parse_sql(d, sql).unwrap();
        let mut an = Analyzer::new(None, vec![], false, 0);
        for (i, s) in stmts.iter().enumerate() {
            an.begin_stmt(i);
            an.analyze(s);
            an.finish_stmt();
        }
        an.finish()
    }

    fn run(sql: &str) -> (Vec<TableInfo>, Vec<TableEdge>, Vec<ColumnEdge>) {
        run_dialect(&GenericDialect {}, sql)
    }

    fn tid(tables: &[TableInfo], name: &str) -> usize {
        tables
            .iter()
            .find(|t| t.name == name)
            .unwrap_or_else(|| panic!("table {name} missing"))
            .id
    }

    fn cedge<'a>(
        edges: &'a [ColumnEdge],
        tables: &[TableInfo],
        st: &str,
        sc: &str,
        tt: &str,
        tc: &str,
        kind: &str,
    ) -> Option<&'a ColumnEdge> {
        let (si, ti) = (tid(tables, st), tid(tables, tt));
        edges.iter().find(|e| {
            e.source_table == si
                && e.source_column == sc
                && e.target_table == ti
                && e.target_column == tc
                && e.kind == kind
        })
    }

    #[test]
    fn generic_factors() {
        let (tables, tedges, cedges) = run(
            "INSERT INTO dw.t SELECT u.x FROM dw.s, UNNEST(dw.s.arr) u;\n\
             INSERT INTO dw.t2 SELECT e.v FROM dw.s, LATERAL explode(dw.s.arr) e;\n\
             INSERT INTO dw.t3 SELECT * FROM dw.t2 UNPIVOT (v FOR k IN (a, b)) u;\n\
             INSERT INTO dw.t4 SELECT * FROM OPENJSON(dw.s.j) WITH (c INT '$.c') o",
        );
        // UNNEST 臂：u 只有 * 列，u.x 落到 STAR 兜底
        assert!(cedge(&cedges, &tables, "dw.s", "arr", "dw.t", "x", "direct").is_some());
        // LATERAL fn() 臂：同上
        assert!(cedge(&cedges, &tables, "dw.s", "arr", "dw.t2", "v", "direct").is_some());
        // UNPIVOT/OpenJsonTable 臂：内层表递归成为源
        let s = tid(&tables, "dw.t2");
        let t = tid(&tables, "dw.t3");
        assert!(tedges.iter().any(|e| e.source == s && e.target == t));
        assert!(tables.iter().any(|t| t.name == "dw.t4"));
    }

    #[test]
    fn generic_items() {
        let (tables, _, cedges) = run(
            "INSERT INTO dw.t SELECT x AS (a, b), MAP {'k': y}, STRUCT(y, z), (y, z) \
             FROM dw.s WHERE MATCH(m, n) AGAINST ('kw')",
        );
        // ExprWithAliases：多个别名共享同一组源
        assert!(cedge(&cedges, &tables, "dw.s", "x", "dw.t", "a", "direct").is_some());
        assert!(cedge(&cedges, &tables, "dw.s", "x", "dw.t", "b", "direct").is_some());
        // Map / Struct / Tuple -> derived
        assert!(cedge(&cedges, &tables, "dw.s", "y", "dw.t", "_c2", "derived").is_some());
        assert!(cedge(&cedges, &tables, "dw.s", "z", "dw.t", "_c3", "derived").is_some());
        assert!(cedge(&cedges, &tables, "dw.s", "y", "dw.t", "_c3", "derived").is_some());
    }

    #[test]
    fn generic_paren_dml() {
        // 括号包裹的 DML -> SetExpr::Insert/Update/Merge 臂
        let (tables, _, _) = run("(INSERT INTO dw.t VALUES (1));\n\
             (UPDATE dw.t SET a = 1);\n\
             (MERGE INTO dw.t USING dw.s ON t.id = s.id WHEN MATCHED THEN UPDATE SET a = 1)");
        assert!(tables.iter().any(|t| t.name == "dw.t"));
        assert!(tables.iter().any(|t| t.name == "dw.s"));
    }

    #[test]
    fn clickhouse_prewhere() {
        let (tables, _, _) =
            run_dialect(&ClickHouseDialect {}, "SELECT a FROM dw.t PREWHERE x > 0");
        assert!(tables.iter().any(|t| t.name == "dw.t"));
    }

    #[test]
    fn manual_expr_variants() {
        // Hive/Generic 均不产生的 Expr 臂：Prior/Named/Dictionary/OuterJoin/Prefixed
        let mut stmts = Parser::parse_sql(&GenericDialect {}, "SELECT x FROM dw.t").unwrap();
        let ident = |s: &str| Expr::Identifier(Ident::new(s));
        let tuple = Expr::Tuple(vec![
            Expr::Prior(Box::new(ident("a"))),
            Expr::Named {
                expr: Box::new(ident("b")),
                name: Ident::new("n"),
            },
            Expr::Dictionary(vec![DictionaryField {
                key: Ident::new("k"),
                value: Box::new(ident("c")),
            }]),
            Expr::OuterJoin(Box::new(ident("d"))),
            Expr::Prefixed {
                prefix: Ident::new("p"),
                value: Box::new(ident("e")),
            },
        ]);
        if let Statement::Query(q) = &mut stmts[0] {
            if let SetExpr::Select(s) = &mut *q.body {
                s.projection[0] = SelectItem::UnnamedExpr(tuple);
            }
        }
        let mut an = Analyzer::new(None, vec![], false, 0);
        an.begin_stmt(0);
        let out = match &stmts[0] {
            Statement::Query(q) => an.query(q),
            _ => unreachable!(),
        };
        let srcs = &out.cols[0].srcs;
        for col in ["a", "b", "c", "d", "e"] {
            assert!(
                srcs.iter().any(|s| s.col == an.syms.intern(col)),
                "missing src col {col}"
            );
        }
    }

    #[test]
    fn manual_fn_subquery() {
        // FunctionArguments::Subquery 臂：f(SELECT a FROM s)
        let mut stmts = Parser::parse_sql(&GenericDialect {}, "SELECT f(1) FROM dw.t").unwrap();
        let inner = Parser::parse_sql(&GenericDialect {}, "SELECT a FROM dw.s").unwrap();
        let inner_q = match &inner[0] {
            Statement::Query(q) => q.clone(),
            _ => unreachable!(),
        };
        if let Statement::Query(q) = &mut stmts[0] {
            if let SetExpr::Select(s) = &mut *q.body {
                if let SelectItem::UnnamedExpr(Expr::Function(f)) = &mut s.projection[0] {
                    f.args = FunctionArguments::Subquery(inner_q);
                }
            }
        }
        let mut an = Analyzer::new(None, vec![], false, 0);
        an.begin_stmt(0);
        let out = match &stmts[0] {
            Statement::Query(q) => an.query(q),
            _ => unreachable!(),
        };
        let a = an.syms.intern("a");
        assert!(out.cols[0].srcs.iter().any(|s| s.col == a));
    }

    #[test]
    fn manual_asof_join() {
        // JoinOperator::AsOf 臂
        let mut stmts = Parser::parse_sql(
            &GenericDialect {},
            "INSERT INTO dw.o SELECT a FROM dw.t JOIN dw.s ON t.id = s.id",
        )
        .unwrap();
        let mc = Parser::parse_sql(&GenericDialect {}, "SELECT t.ts <= s.ts").unwrap();
        let mc_expr = match &mc[0] {
            Statement::Query(q) => match &*q.body {
                SetExpr::Select(s) => match &s.projection[0] {
                    SelectItem::UnnamedExpr(e) => e.clone(),
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };
        if let Statement::Insert(ins) = &mut stmts[0] {
            if let Some(q) = &mut ins.source {
                if let SetExpr::Select(s) = &mut *q.body {
                    let j = &mut s.from[0].joins[0];
                    let c = match &j.join_operator {
                        JoinOperator::Join(c) => c.clone(),
                        _ => unreachable!(),
                    };
                    j.join_operator = JoinOperator::AsOf {
                        match_condition: mc_expr,
                        constraint: c,
                    };
                }
            }
        }
        let mut an = Analyzer::new(None, vec![], false, 0);
        an.begin_stmt(0);
        an.analyze(&stmts[0]);
        an.finish_stmt();
        let (tables, tedges, _) = an.finish();
        let (s, o) = (tid(&tables, "dw.s"), tid(&tables, "dw.o"));
        assert!(tedges.iter().any(|e| e.source == s && e.target == o));
    }

    #[test]
    fn manual_table_object() {
        // TableObject::TableQuery / TableFunction 臂（方言门控，手工构造）
        let mut stmts = Parser::parse_sql(&GenericDialect {}, "INSERT INTO dw.t SELECT 1").unwrap();
        let inner = Parser::parse_sql(&GenericDialect {}, "SELECT a FROM dw.s").unwrap();
        let inner_q = match &inner[0] {
            Statement::Query(q) => q.clone(),
            _ => unreachable!(),
        };
        if let Statement::Insert(ins) = &mut stmts[0] {
            ins.table = TableObject::TableQuery(inner_q);
        }
        let mut an = Analyzer::new(None, vec![], false, 0);
        an.begin_stmt(0);
        an.analyze(&stmts[0]);
        let (tables, _, _) = an.finish();
        assert!(tables.iter().any(|t| t.name == "dw.s"));
    }

    #[test]
    fn expr_scalars() {
        // TypedString / IntroducerString / 常数等杂项
        let (tables, _, _) = run("INSERT INTO dw.t SELECT DATE '2020-01-01', x FROM dw.s");
        assert!(tables.iter().any(|t| t.name == "dw.t"));
    }

    #[test]
    fn resolve_unknown_qualifier() {
        // resolve_q 未命中 -> Srcs::new；unk_idx 唯一未知 rel -> STAR 兜底
        let (tables, _, cedges) = run("CREATE TABLE dw.s (x INT);\n\
             INSERT INTO dw.t SELECT w.x, y FROM dw.s, explode(x) e");
        assert!(tables.iter().any(|t| t.name == "dw.t"));
        let _ = cedges;
    }

    #[test]
    fn generic_ddl() {
        // do_create_table 非 CTAS 分支（Hive 分类把普通 DDL 交给 fallback，经 Analyzer 直接喂 AST 覆盖）
        let (tables, tedges, _) = run("CREATE TEMPORARY TABLE tp (a INT);\n\
             CREATE TABLE pt (a INT) PARTITIONED BY (d STRING);\n\
             CREATE TABLE lk LIKE pt;\n\
             CREATE TABLE cl CLONE pt;\n\
             CREATE TABLE ct AS SELECT x, y FROM dw.s");
        let kinds: Vec<_> = tables.iter().map(|t| (t.name.as_str(), t.kind)).collect();
        assert!(kinds.contains(&("tp", "temp")));
        assert!(kinds.contains(&("pt", "table")));
        // PARTITIONED 列登记进目录
        assert!(tables
            .iter()
            .find(|t| t.name == "pt")
            .unwrap()
            .columns
            .iter()
            .any(|c| c == "d"));
        // LIKE/CLONE 拷贝列目录
        for n in ["lk", "cl"] {
            assert!(tables
                .iter()
                .find(|t| t.name == n)
                .unwrap()
                .columns
                .iter()
                .any(|c| c == "a"));
        }
        let (s, t) = (tid(&tables, "dw.s"), tid(&tables, "ct"));
        assert!(tedges.iter().any(|e| e.source == s && e.target == t));
    }

    #[test]
    fn generic_other_statement() {
        // 能解析但无对应分析臂的语句 -> "other"
        let mut an = Analyzer::new(None, vec![], false, 0);
        for sql in [
            "DELETE FROM dw.t WHERE a = 1",
            "VACUUM dw.t",
            "ANALYZE TABLE dw.t",
        ] {
            let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
            an.begin_stmt(0);
            assert_eq!(an.analyze(&stmts[0]), "other");
            an.finish_stmt();
        }
    }

    #[test]
    fn generic_join_operators() {
        // join_constraint 其余算子臂 + `_ => None`（CROSS/OUTER APPLY）
        let (tables, tedges, _) = run(
            "INSERT INTO dw.o1 SELECT * FROM dw.a SEMI JOIN dw.b ON a.id = b.id;\n\
             INSERT INTO dw.o2 SELECT * FROM dw.a ANTI JOIN dw.b ON a.id = b.id;\n\
             INSERT INTO dw.o3 SELECT * FROM dw.a LEFT ANTI JOIN dw.b ON a.id = b.id;\n\
             INSERT INTO dw.o4 SELECT * FROM dw.a RIGHT SEMI JOIN dw.b ON a.id = b.id;\n\
             INSERT INTO dw.o5 SELECT * FROM dw.a RIGHT ANTI JOIN dw.b ON a.id = b.id;\n\
             INSERT INTO dw.o6 SELECT * FROM dw.a CROSS APPLY dw.b;\n\
             INSERT INTO dw.o7 SELECT * FROM dw.a OUTER APPLY dw.b",
        );
        for n in [
            "dw.o1", "dw.o2", "dw.o3", "dw.o4", "dw.o5", "dw.o6", "dw.o7",
        ] {
            let (s, t) = (tid(&tables, "dw.a"), tid(&tables, n));
            assert!(tedges.iter().any(|e| e.source == s && e.target == t), "{n}");
        }
    }

    #[test]
    fn generic_expr_arms() {
        // JsonAccess / Lambda / FunctionArgumentClause / TableFactor::Function 无名
        let (tables, _, cedges) = run(
            "INSERT INTO dw.t SELECT a -> 'k', transform(a, x -> x + 1), func(a LIMIT 1) FROM dw.s;\n\
             INSERT INTO dw.t2 SELECT v FROM dw.s, LATERAL explode(dw.s.arr)",
        );
        assert!(cedge(&cedges, &tables, "dw.s", "a", "dw.t", "_c0", "derived").is_some());
        assert!(cedge(&cedges, &tables, "dw.s", "a", "dw.t", "_c1", "derived").is_some());
        let _ = tid(&tables, "dw.t2");
    }

    #[test]
    fn generic_grouping_exprs() {
        // Expr::GroupingSets / Cube / Rollup 臂（group_by 元素走 expr_srcs）
        let (tables, _, cedges) =
            run("INSERT INTO dw.t SELECT x, count(a) FROM dw.s GROUP BY GROUPING SETS ((x), (y));");
        assert!(cedge(&cedges, &tables, "dw.s", "x", "dw.t", "x", "direct").is_some());
    }

    #[test]
    fn generic_merge_row() {
        // MergeInsertKind::Row + 显式列 -> ins.columns 显式列收集臂
        let (tables, tedges, _) = run("MERGE INTO dw.t USING dw.s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (a, b) ROW");
        let (s, t) = (tid(&tables, "dw.s"), tid(&tables, "dw.t"));
        assert!(tedges.iter().any(|e| e.source == s && e.target == t));
    }

    #[test]
    fn generic_catalog_rel() {
        // catalog.get -> Some(cl) 臂：CREATE 登记列后 rel 按目录展开
        let (tables, _, cedges) = run("CREATE TABLE dw.s (x INT, y INT);\n\
             INSERT INTO dw.t SELECT s.* FROM dw.s s;\n\
             INSERT INTO dw.t2 SELECT x FROM dw.s");
        assert!(cedge(&cedges, &tables, "dw.s", "x", "dw.t", "x", "direct").is_some());
        assert!(cedge(&cedges, &tables, "dw.s", "x", "dw.t2", "x", "direct").is_some());
    }

    #[test]
    fn manual_insert_variants() {
        // TableObject::TableFunction / ins.source None / after_columns 空臂
        let f = Parser::parse_sql(&GenericDialect {}, "SELECT f(1)").unwrap();
        let func = match &f[0] {
            Statement::Query(q) => match &*q.body {
                SetExpr::Select(s) => match &s.projection[0] {
                    SelectItem::UnnamedExpr(Expr::Function(f)) => f.clone(),
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };
        let mut stmts = Parser::parse_sql(&GenericDialect {}, "INSERT INTO dw.t SELECT 1").unwrap();
        if let Statement::Insert(ins) = &mut stmts[0] {
            ins.table = TableObject::TableFunction(func);
            ins.source = None;
        }
        let mut an = Analyzer::new(None, vec![], false, 0);
        an.begin_stmt(0);
        an.analyze(&stmts[0]);
        let (tables, _, _) = an.finish();
        let _ = tables;
    }

    #[test]
    fn manual_ctas_columns() {
        // CTAS 显式列臂：CREATE TABLE e (a, b) AS SELECT（方言均不解析，手工构造）
        let mut stmts = Parser::parse_sql(
            &GenericDialect {},
            "CREATE TABLE dw.e AS SELECT x, y FROM dw.s",
        )
        .unwrap();
        if let Statement::CreateTable(ct) = &mut stmts[0] {
            ct.columns = vec![
                ColumnDef {
                    name: Ident::new("a"),
                    data_type: DataType::Int(None),
                    options: vec![],
                },
                ColumnDef {
                    name: Ident::new("b"),
                    data_type: DataType::Int(None),
                    options: vec![],
                },
            ];
        }
        let mut an = Analyzer::new(None, vec![], false, 0);
        an.begin_stmt(0);
        an.analyze(&stmts[0]);
        an.finish_stmt();
        let (tables, tedges, _) = an.finish();
        let (s, t) = (tid(&tables, "dw.s"), tid(&tables, "dw.e"));
        assert!(tedges.iter().any(|e| e.source == s && e.target == t));
    }

    #[test]
    fn manual_single_compound() {
        // CompoundIdentifier 仅 1 段的退化形态
        let mut stmts =
            Parser::parse_sql(&GenericDialect {}, "INSERT INTO dw.t SELECT x FROM dw.s").unwrap();
        if let Statement::Insert(ins) = &mut stmts[0] {
            if let Some(q) = &mut ins.source {
                if let SetExpr::Select(s) = &mut *q.body {
                    s.projection[0] =
                        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![Ident::new("only")]));
                }
            }
        }
        let mut an = Analyzer::new(None, vec![], false, 0);
        an.begin_stmt(0);
        an.analyze(&stmts[0]);
        let (tables, _, _) = an.finish();
        assert!(tables.iter().any(|t| t.name == "dw.t"));
    }

    #[test]
    fn manual_fn_clauses() {
        // FunctionArgumentClause::Where/Limit/`_` 与 Function.parameters 臂
        let mut stmts = Parser::parse_sql(
            &GenericDialect {},
            "INSERT INTO dw.t SELECT count(a) FROM dw.s",
        )
        .unwrap();
        if let Statement::Insert(ins) = &mut stmts[0] {
            if let Some(q) = &mut ins.source {
                if let SetExpr::Select(s) = &mut *q.body {
                    if let SelectItem::UnnamedExpr(Expr::Function(f)) = &mut s.projection[0] {
                        if let FunctionArguments::List(l) = &mut f.args {
                            l.clauses = vec![
                                FunctionArgumentClause::Where(Expr::Identifier(Ident::new("w"))),
                                FunctionArgumentClause::Limit(Expr::Identifier(Ident::new("l"))),
                                FunctionArgumentClause::IgnoreOrRespectNulls(
                                    NullTreatment::IgnoreNulls,
                                ),
                            ];
                        }
                        f.parameters = f.args.clone();
                    }
                }
            }
        }
        let mut an = Analyzer::new(None, vec![], false, 0);
        an.begin_stmt(0);
        an.analyze(&stmts[0]);
        an.finish_stmt();
        let (tables, _, _) = an.finish();
        let _ = tables;
    }

    /// 用一段 SELECT 的投影做 AST 替换实验场
    fn patch_projection<F: FnMut(&mut SelectItem)>(sql: &str, mut f: F) -> Statement {
        let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        if let Statement::Insert(ins) = &mut stmts[0] {
            if let Some(q) = &mut ins.source {
                if let SetExpr::Select(s) = &mut *q.body {
                    for item in &mut s.projection {
                        f(item);
                    }
                }
            }
        }
        stmts.remove(0)
    }

    #[test]
    fn manual_expr_arms() {
        // JsonAccess / Lambda / Convert.styles / trim_characters / ExprNamed / 占位符 CompoundIdentifier
        let mut exprs = vec![
            Expr::JsonAccess {
                value: Box::new(Expr::Identifier(Ident::new("j"))),
                path: JsonPath {
                    path: vec![JsonPathElem::Dot {
                        key: "k".into(),
                        quoted: false,
                    }],
                },
            },
            Expr::Lambda(LambdaFunction {
                params: OneOrManyWithParens::One(LambdaFunctionParameter {
                    name: Ident::new("x"),
                    data_type: None,
                }),
                body: Box::new(Expr::Identifier(Ident::new("b"))),
                syntax: LambdaSyntax::Arrow,
            }),
            Expr::Convert {
                is_try: false,
                expr: Box::new(Expr::Identifier(Ident::new("a"))),
                data_type: None,
                charset: None,
                target_before_value: false,
                styles: vec![Expr::Identifier(Ident::new("st"))],
            },
            Expr::Trim {
                expr: Box::new(Expr::Identifier(Ident::new("a"))),
                trim_where: None,
                trim_what: None,
                trim_characters: Some(vec![Expr::Identifier(Ident::new("tc"))]),
            },
            Expr::CompoundIdentifier(vec![Ident::new("hzv0hz")]),
        ];
        let stmt = patch_projection("INSERT INTO dw.t SELECT a FROM dw.s", |item| {
            if !exprs.is_empty() {
                *item = SelectItem::UnnamedExpr(exprs.remove(0));
            }
        });
        let mut an = Analyzer::new(None, vec![], false, 0);
        an.begin_stmt(0);
        an.analyze(&stmt);
        let (tables, _, _) = an.finish();
        assert!(tables.iter().any(|t| t.name == "dw.t"));
    }

    #[test]
    fn manual_expr_named_arg() {
        // FunctionArg::ExprNamed（`f(a => b)`/`f(a := b)` 均解析为 Named，手工构造）
        let stmt = patch_projection("INSERT INTO dw.t SELECT f(a) FROM dw.s", |item| {
            if let SelectItem::UnnamedExpr(Expr::Function(f)) = item {
                if let FunctionArguments::List(l) = &mut f.args {
                    l.args = vec![FunctionArg::ExprNamed {
                        name: Expr::Identifier(Ident::new("n")),
                        arg: FunctionArgExpr::Expr(Expr::Identifier(Ident::new("v"))),
                        operator: FunctionArgOperator::Assignment,
                    }];
                }
            }
        });
        let mut an = Analyzer::new(None, vec![], false, 0);
        an.begin_stmt(0);
        an.analyze(&stmt);
        let (tables, _, _) = an.finish();
        assert!(tables.iter().any(|t| t.name == "dw.t"));
    }

    #[test]
    fn manual_wildcard_expr() {
        // SelectItemQualifiedWildcardKind::Expr -> out.unknown 臂
        let stmt = patch_projection("INSERT INTO dw.t SELECT a FROM dw.s", |item| {
            *item = SelectItem::QualifiedWildcard(
                SelectItemQualifiedWildcardKind::Expr(Expr::Identifier(Ident::new("x"))),
                WildcardAdditionalOptions::default(),
            );
        });
        let mut an = Analyzer::new(None, vec![], false, 0);
        an.begin_stmt(0);
        an.analyze(&stmt);
        let (tables, _, _) = an.finish();
        assert!(tables.iter().any(|t| t.name == "dw.t"));
    }

    #[test]
    fn manual_setexpr_table() {
        // SetExpr::Table -> set_expr `_` 臂
        let mut stmts =
            Parser::parse_sql(&GenericDialect {}, "INSERT INTO dw.t SELECT a FROM dw.s").unwrap();
        if let Statement::Insert(ins) = &mut stmts[0] {
            if let Some(q) = &mut ins.source {
                *q.body = SetExpr::Table(Box::new(Table {
                    table_name: Some("x".into()),
                    schema_name: None,
                }));
            }
        }
        let mut an = Analyzer::new(None, vec![], false, 0);
        an.begin_stmt(0);
        an.analyze(&stmts[0]);
        let (tables, _, _) = an.finish();
        assert!(tables.iter().any(|t| t.name == "dw.t"));
    }
}
