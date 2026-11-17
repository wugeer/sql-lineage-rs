//! hive_sql_parse: Hive / 数仓 SQL 血缘分析核心库
//! analyze(files, opts) -> LineageResult，多文件按顺序视作同一脚本

mod analyzer;
mod catalog;
pub mod db;
mod fallback;
mod lexer;
pub mod model;
mod preprocess;
pub mod server;

/// AST 构建/释放为分配密集型负载，mimalloc 显著优于默认分配器
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use analyzer::Analyzer;
use lexer::{split_statements, RawStatement};
use model::{FileInfo, LineageResult, StatementInfo};
use preprocess::{classify, substitute, Prep};
use rayon::prelude::*;
use rustc_hash::FxHashMap as HashMap;
use sqlparser::ast::Statement;
use sqlparser::dialect::{Dialect, HiveDialect, SparkSqlDialect};
use sqlparser::parser::Parser;
use std::borrow::Cow;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

pub struct InputFile {
    pub name: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum SqlDialect {
    #[default]
    Hive,
    Spark,
}

impl std::str::FromStr for SqlDialect {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "hive" => Ok(Self::Hive),
            "spark" => Ok(Self::Spark),
            _ => Err("不支持的 SQL 方言，仅支持 hive 或 spark".to_string()),
        }
    }
}

#[derive(Default, Clone)]
pub struct Options {
    pub dialect: SqlDialect,
    pub default_db: Option<String>,
    /// None: 自动模式（脚本无任何写入目标语句时自动解析独立 SELECT 为虚拟结果表）
    /// Some(true): 始终解析独立 SELECT 查询
    /// Some(false): 跳过独立 SELECT 查询
    pub analyze_select: Option<bool>,
    /// 任务标识（如 dag.task）
    pub job_name: Option<String>,
}

/// 携带解码后源码文本的分析产物（供服务端 O(1) 按偏移提取语句源码）
pub struct AnalyzedWorkspace {
    pub result: LineageResult,
    pub sources: Vec<String>,
}

struct Work<'a> {
    idx: usize,
    kind: &'static str,
    segments: Vec<Cow<'a, str>>,
    query_seg: Option<Cow<'a, str>>,
    use_db: Option<Option<String>>,
    ddl: Option<Cow<'a, str>>,
    results: Vec<Result<Vec<Statement>, String>>,
}

/// 由分类结果构建 Work（返回值附带 SET 变量赋值，由调用方决定是否写入 vars）
fn build_work<'a>(i: usize, p: Prep<'a>) -> (Work<'a>, Option<(String, String)>) {
    let mut w = Work {
        idx: i,
        kind: "other",
        segments: Vec::new(),
        query_seg: None,
        use_db: None,
        ddl: None,
        results: Vec::new(),
    };
    let mut kv = None;
    match p {
        Prep::Skip { kind } => w.kind = kind,
        Prep::Set { kv: k } => {
            w.kind = "set";
            kv = k;
        }
        Prep::Use { db } => {
            w.kind = "use";
            w.use_db = Some(db);
        }
        Prep::Query { segment } => {
            w.kind = "query";
            w.query_seg = Some(segment);
        }
        Prep::Parse { kind, segments } => {
            w.kind = kind;
            w.segments = segments;
        }
        Prep::Ddl { kind, text } => {
            w.kind = kind;
            w.ddl = Some(text);
        }
    }
    (w, kv)
}

const PARSE_CHUNK_SIZE: usize = 256;

fn global_parse_pool() -> &'static rayon::ThreadPool {
    static POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();
    POOL.get_or_init(|| {
        rayon::ThreadPoolBuilder::new()
            .stack_size(16 * 1024 * 1024)
            .build()
            .unwrap_or_else(|_| rayon::ThreadPoolBuilder::new().build().unwrap())
    })
}

/// 解析线程池引用（HL_THREADS 自定义线程数时持有池所有权，否则借用全局池）
enum ParsePool {
    Owned(rayon::ThreadPool),
    Global,
}

impl ParsePool {
    fn get(&self) -> &rayon::ThreadPool {
        match self {
            Self::Owned(p) => p,
            Self::Global => global_parse_pool(),
        }
    }
}

fn parse_pool() -> ParsePool {
    if let Some(n) = std::env::var("HL_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
    {
        if let Ok(custom) = rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .stack_size(16 * 1024 * 1024)
            .build()
        {
            return ParsePool::Owned(custom);
        }
    }
    ParsePool::Global
}

fn parse_seg(sql: &str, dialect: SqlDialect) -> Result<Vec<Statement>, String> {
    let dialect: &dyn Dialect = match dialect {
        SqlDialect::Hive => &HiveDialect {},
        SqlDialect::Spark => &SparkSqlDialect {},
    };
    let mut p = Parser::new(dialect)
        .with_recursion_limit(256)
        .try_with_sql(sql)
        .map_err(|e| e.to_string())?;
    p.parse_statements().map_err(|e| e.to_string())
}

/// 单文件字节流解码（UTF-8 校验失败按 GBK 解码）
fn decode(bytes: Vec<u8>) -> (String, &'static str) {
    match String::from_utf8(bytes) {
        Ok(s) => (s, "utf-8"),
        Err(err) => {
            let bytes = err.into_bytes();
            let (cow, _, _) = encoding_rs::GBK.decode(&bytes);
            (cow.into_owned(), "gbk")
        }
    }
}

pub fn analyze(files: Vec<InputFile>, opts: &Options) -> LineageResult {
    analyze_with_sources(files, opts).result
}

pub fn analyze_with_sources(files: Vec<InputFile>, opts: &Options) -> AnalyzedWorkspace {
    let t0 = Instant::now();
    let prof = std::env::var_os("HL_PROFILE").is_some();
    let mut res = LineageResult {
        job_name: opts
            .job_name
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        ..LineageResult::default()
    };
    res.stats.files = files.len();
    res.stats.bytes = files.iter().map(|f| f.bytes.len()).sum();

    // 1) 零多余拷贝解码 + 字节级语句切分
    let mut raws: Vec<(usize, RawStatement)> = Vec::new();
    let mut texts: Vec<String> = Vec::with_capacity(files.len());
    for (fi, f) in files.into_iter().enumerate() {
        let (text, enc) = decode(f.bytes);
        res.files.push(FileInfo {
            name: f.name,
            encoding: enc,
        });
        for s in split_statements(&text) {
            raws.push((fi, s));
        }
        texts.push(text);
    }
    res.stats.statements = raws.len();

    if prof {
        eprintln!("[profile] decode+split: {:?}", t0.elapsed());
    }

    let texts = texts;
    {
        let t_prep = Instant::now();
        // 2) 预处理（变量代入 / USE / 首 token 短路分类 / 多路 INSERT 改写）。
        //    全文无 ${} 变量时代入必为借用、逐语句分类无状态，works 构建可完全并行；
        //    含变量时回退顺序处理以维护 SET 变量环境与占位符还原表。
        let mut restore: Vec<(String, String)> = Vec::new();
        let any_vars = raws
            .par_iter()
            .any(|(fi, r)| texts[*fi][r.start..r.end].contains("${"));

        let mut works: Vec<Work> = if any_vars {
            let mut vars: HashMap<String, String> = HashMap::default();
            let mut intern: HashMap<String, usize> = HashMap::default();
            // 先顺序处理变量替换与 SET 语句，其余语句留给 rayon 并行 tokenize/classify
            let mut staged: Vec<Result<Work, Cow<'_, str>>> = Vec::with_capacity(raws.len());
            for (i, (fi, raw)) in raws.iter().enumerate() {
                let text = &texts[*fi][raw.start..raw.end];
                let sub = substitute(text, &vars, &mut intern, &mut restore);
                if let Some(t0) = lexer::first_token(&sub) {
                    if t0.kind == lexer::TokKind::Word && t0.slice(&sub).eq_ignore_ascii_case("set")
                    {
                        let (w, kv) = build_work(i, classify(sub));
                        if let Some((k, v)) = kv {
                            vars.insert(k, v);
                        }
                        staged.push(Ok(w));
                        continue;
                    }
                }
                staged.push(Err(sub));
            }
            staged
                .into_par_iter()
                .enumerate()
                .map(|(i, item)| match item {
                    Ok(w) => w,
                    Err(sub) => build_work(i, classify(sub)).0,
                })
                .collect()
        } else {
            raws.par_iter()
                .enumerate()
                .map(|(i, (fi, raw))| {
                    let text = &texts[*fi][raw.start..raw.end];
                    build_work(i, classify(Cow::Borrowed(text))).0
                })
                .collect()
        };

        let has_write_stmt = works
            .iter()
            .any(|w| !matches!(w.kind, "other" | "set" | "use" | "query"));
        let query_stmt_count = works.iter().filter(|w| w.kind == "query").count();

        let enable_standalone_query = match opts.analyze_select {
            Some(flag) => flag,
            None => !has_write_stmt && query_stmt_count > 0,
        };
        if enable_standalone_query {
            for w in &mut works {
                if let Some(qseg) = w.query_seg.take() {
                    w.segments.push(qseg);
                }
            }
        }

        if prof {
            eprintln!("[profile] preprocess: {:?}", t_prep.elapsed());
        }

        let pool = parse_pool();

        if std::env::var_os("HL_PARSE_ONLY").is_some() {
            pool.get().install(|| {
                for chunk in works.chunks_mut(PARSE_CHUNK_SIZE) {
                    chunk.par_iter_mut().for_each(|w| {
                        let parsed = w
                            .segments
                            .iter()
                            .map(|s| parse_seg(s, opts.dialect))
                            .collect::<Vec<_>>();
                        drop(parsed);
                    });
                }
            });
            res.stats.elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
            return AnalyzedWorkspace {
                result: res,
                sources: texts,
            };
        }

        // 3) 三级流水线：解析生产 -> 主线程顺序分析 -> 并行释放。
        //    主线程分析 chunk i 时，worker 池并行解析 chunk i+1 并释放 chunk i-1，
        //    消除三阶段串行空转；sync_channel 背压保证在途 AST 有界（<=4 chunk）。
        //    Analyzer 含 Rc 不可跨线程，故分析仍固定在主线程，AST 经 channel 转移所有权。
        let mut t_parse = Duration::ZERO;
        let mut t_analyze = Duration::ZERO;
        let mut t_fin = Duration::ZERO;
        let mut t_drop = Duration::ZERO;

        let mut an = Analyzer::new(
            opts.default_db.clone(),
            restore,
            enable_standalone_query,
            query_stmt_count,
        );
        let mut stmt_infos: Vec<StatementInfo> = Vec::with_capacity(works.len());

        let mut chunks: Vec<Vec<Work>> = Vec::with_capacity(works.len() / PARSE_CHUNK_SIZE + 1);
        let mut it = works.into_iter();
        loop {
            let c: Vec<Work> = it.by_ref().take(PARSE_CHUNK_SIZE).collect();
            if c.is_empty() {
                break;
            }
            chunks.push(c);
        }

        let (parsed_tx, parsed_rx) = std::sync::mpsc::sync_channel::<Vec<Work>>(1);
        let (drop_tx, drop_rx) = std::sync::mpsc::sync_channel::<Vec<Work>>(1);

        std::thread::scope(|ts| {
            let pool = pool.get();
            let parser_h = ts.spawn(move || {
                let mut acc = Duration::ZERO;
                for mut c in chunks {
                    let tp = Instant::now();
                    pool.install(|| {
                        c.par_iter_mut().for_each(|w| {
                            if !w.segments.is_empty() {
                                w.results = w
                                    .segments
                                    .iter()
                                    .map(|s| parse_seg(s, opts.dialect))
                                    .collect();
                            }
                        });
                    });
                    acc += tp.elapsed();
                    if parsed_tx.send(c).is_err() {
                        break;
                    }
                }
                acc
            });
            let dropper_h = ts.spawn(move || {
                let mut acc = Duration::ZERO;
                while let Ok(mut c) = drop_rx.recv() {
                    let td = Instant::now();
                    pool.install(|| {
                        c.par_iter_mut().for_each(|w| {
                            w.results = Vec::new();
                            w.segments = Vec::new();
                        });
                    });
                    acc += td.elapsed();
                }
                acc
            });

            while let Ok(mut c) = parsed_rx.recv() {
                for w in c.iter_mut() {
                    let (fi, raw) = &raws[w.idx];
                    let mut info = StatementInfo {
                        index: w.idx,
                        file: *fi,
                        line: raw.line,
                        start: raw.start,
                        end: raw.end,
                        kind: w.kind,
                        status: "skipped",
                        error: None,
                        targets: Vec::new(),
                        sources: Vec::new(),
                    };
                    if let Some(db) = &w.use_db {
                        an.current_db = db.clone();
                    } else if let Some(ddl) = &w.ddl {
                        an.begin_stmt(w.idx);
                        fallback::extract(ddl, &mut an);
                        let (targets, sources) = an.finish_stmt();
                        info.targets = targets;
                        info.sources = sources;
                        info.status = "ok";
                    } else if !w.segments.is_empty() {
                        an.begin_stmt(w.idx);
                        let mut kind = w.kind;
                        let mut first_err: Option<String> = None;
                        for (si, r) in w.results.iter().enumerate() {
                            match r {
                                Ok(stmts) => {
                                    for s in stmts {
                                        let ta = Instant::now();
                                        let k = an.analyze(s);
                                        t_analyze += ta.elapsed();
                                        if k != "other" {
                                            kind = k;
                                        }
                                    }
                                }
                                Err(e) => {
                                    if first_err.is_none() {
                                        first_err = Some(e.clone());
                                    }
                                    fallback::extract(&w.segments[si], &mut an);
                                }
                            }
                        }
                        let tf = Instant::now();
                        let (targets, sources) = an.finish_stmt();
                        t_fin += tf.elapsed();
                        info.kind = kind;
                        info.targets = targets;
                        info.sources = sources;
                        info.error = first_err;
                        info.status = if info.error.is_none() {
                            "ok"
                        } else if !info.targets.is_empty() || !info.sources.is_empty() {
                            "fallback"
                        } else {
                            "error"
                        };
                    }
                    match info.status {
                        "ok" => res.stats.parsed += 1,
                        "fallback" => res.stats.fallback += 1,
                        "error" => res.stats.failed += 1,
                        _ => res.stats.skipped += 1,
                    }
                    stmt_infos.push(info);
                }
                let _ = drop_tx.send(c);
            }
            drop(drop_tx);
            t_parse = parser_h.join().unwrap_or_default();
            t_drop = dropper_h.join().unwrap_or_default();
        });

        if prof {
            eprintln!(
                "[profile] parse={:?} walk={:?} finish={:?} drop={:?}",
                t_parse, t_analyze, t_fin, t_drop
            );
        }

        let (tables, table_edges, column_edges) = an.finish();
        res.tables = tables;
        res.table_edges = table_edges;
        res.column_edges = column_edges;
        res.statements = stmt_infos;
    }

    res.stats.elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    AnalyzedWorkspace {
        result: res,
        sources: texts,
    }
}
