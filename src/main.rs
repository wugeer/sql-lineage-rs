//! hive-lineage CLI

use clap::{Args, Parser, Subcommand};
use hive_sql_parse::db::query::LineageService;
use hive_sql_parse::db::schema::postgres_ddl;
use hive_sql_parse::db::{DbConfig, LineageDirection, QueryScope};
use hive_sql_parse::{analyze_with_sources, InputFile, Options, SqlDialect};
use std::io::Write;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "hive-lineage",
    about = "Hive / Spark SQL 血缘分析、PostgreSQL 追加落库与多维血缘查询"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// 分析 SQL 文件，输出血缘 JSON（支持携带 job_name 追加落库）
    Analyze(AnalyzeArgs),
    /// 启动血缘可视化与数据库血缘查询 Web 服务
    Serve(ServeArgs),
    /// 从 PostgreSQL 数据库查询表级血缘、任务级血缘、任务链路与字段级血缘
    Query(QueryArgs),
    /// 输出 PostgreSQL 自动建表与复合索引 DDL 脚本
    Ddl,
}

#[derive(Args)]
struct AnalyzeArgs {
    /// 输入 SQL 文件（可多个，按顺序视作同一脚本）
    files: Vec<PathBuf>,
    /// 输出 JSON 路径（缺省写 stdout）
    #[arg(short = 'o', long)]
    out: Option<PathBuf>,
    /// 未限定表名的默认库
    #[arg(long)]
    default_db: Option<String>,
    /// 美化输出
    #[arg(long)]
    pretty: bool,
    /// 是否将独立 SELECT 查询映射为虚拟结果表：auto(默认)/yes/no
    #[arg(long, value_name = "MODE", default_value = "auto")]
    analyze_select: String,
    #[arg(
        long,
        value_enum,
        ignore_case = true,
        default_value = "hive",
        help = "SQL 方言：hive 或 spark"
    )]
    dialect: SqlDialect,
    /// 任务标识（如 dag.task）；启用 --db-url 追加落库时必填
    #[arg(long)]
    job_name: Option<String>,
    /// PostgreSQL 连接串（如 postgres://user:pass@host:5432/db，缺省读取 HL_DB_URL）
    #[arg(long)]
    db_url: Option<String>,
    /// 禁用连接时自动执行 CREATE TABLE IF NOT EXISTS
    #[arg(long)]
    no_auto_create_table: bool,
}

#[derive(Args)]
struct ServeArgs {
    /// 监听地址
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// 监听端口
    #[arg(short = 'p', long, default_value = "3000")]
    port: u16,
    /// 启动时预加载分析的 SQL 文件（可多个）
    files: Vec<PathBuf>,
    /// 未限定表名的默认库
    #[arg(long)]
    default_db: Option<String>,
    #[arg(
        long,
        value_enum,
        ignore_case = true,
        default_value = "hive",
        help = "SQL 方言：hive 或 spark"
    )]
    dialect: SqlDialect,
    /// PostgreSQL 连接串（缺省读取 HL_DB_URL；也可在 WebUI 界面动态配置）
    #[arg(long)]
    db_url: Option<String>,
    /// 禁用连接时自动执行 CREATE TABLE IF NOT EXISTS
    #[arg(long)]
    no_auto_create_table: bool,
}

#[derive(Args)]
struct QueryCommonOpts {
    /// PostgreSQL 连接串（缺省读取环境变量 HL_DB_URL）
    #[arg(long)]
    db_url: Option<String>,
    /// 查询方向：both(默认) / upstream / downstream
    #[arg(long, value_enum, ignore_case = true, default_value = "both")]
    direction: LineageDirection,
    /// 最大遍历跳数（0 表示不限跳数穿透全链路）
    #[arg(long, default_value_t = 5)]
    hops: u32,
    /// 合并所有历史追加批次（默认仅取每个 job_name 最新批次）
    #[arg(long)]
    all_history: bool,
    /// 美化 JSON 输出
    #[arg(long)]
    pretty: bool,
}

#[derive(Args)]
struct QueryArgs {
    #[command(subcommand)]
    target: QuerySubcmd,
}

#[derive(Subcommand)]
enum QuerySubcmd {
    /// 输入 库名.表名 查询表级别的上下游加工血缘
    Table {
        /// 目标表（格式：库名.表名）
        table: String,
        #[command(flatten)]
        common: QueryCommonOpts,
    },
    /// 输入 库名.表名 查询任务级别的上下游血缘
    TableTasks {
        /// 目标表（格式：库名.表名）
        table: String,
        #[command(flatten)]
        common: QueryCommonOpts,
    },
    /// 按照任务名（如 dag.task）搜索上、下游任务链路
    Task {
        /// 任务名或搜索关键字（如 dag.task）
        job: String,
        #[command(flatten)]
        common: QueryCommonOpts,
    },
    /// 按照 库名、表名、字段名 输出上下游 库名.表名.字段名 的字段级别血缘链路
    Column {
        /// 合并格式字段名（如 库名.表名.字段名；也可改用 --db / --table / --column）
        field: Option<String>,
        /// 库名
        #[arg(long)]
        db: Option<String>,
        /// 表名
        #[arg(long)]
        table: Option<String>,
        /// 字段名
        #[arg(long)]
        column: Option<String>,
        #[command(flatten)]
        common: QueryCommonOpts,
    },
}

fn parse_select_mode(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "yes" | "true" | "on" | "all" => Some(true),
        "no" | "false" | "off" | "none" => Some(false),
        _ => None,
    }
}

fn resolve_db_url(cli_url: Option<String>) -> Option<String> {
    cli_url
        .or_else(|| std::env::var("HL_DB_URL").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn write_json_stdout<T: serde::Serialize>(val: &T, pretty: bool) -> anyhow::Result<()> {
    let json = if pretty {
        serde_json::to_string_pretty(val)?
    } else {
        serde_json::to_string(val)?
    };
    let stdout = std::io::stdout();
    let mut h = stdout.lock();
    h.write_all(json.as_bytes())?;
    h.write_all(b"\n")?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Analyze(a) => {
            let db_url = resolve_db_url(a.db_url);
            if db_url.is_some()
                && a.job_name
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or("")
                    .is_empty()
            {
                anyhow::bail!(
                    "启用数据库追加落库（--db-url）时必须通过 --job-name 指定任务名（如 dag.task）"
                );
            }
            let mut files = Vec::new();
            for p in &a.files {
                let bytes = std::fs::read(p)?;
                files.push(InputFile {
                    name: p.to_string_lossy().into_owned(),
                    bytes,
                });
            }
            let opts = Options {
                dialect: a.dialect,
                default_db: a.default_db.clone(),
                analyze_select: parse_select_mode(&a.analyze_select),
                job_name: a.job_name.clone(),
            };
            let ws = analyze_with_sources(files, &opts);
            if let Some(url) = db_url {
                let job_name = a.job_name.as_deref().unwrap_or("");
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?;
                let summary = rt.block_on(async {
                    let svc = LineageService::connect(DbConfig {
                        url,
                        auto_create_table: !a.no_auto_create_table,
                        use_hot_index: false,
                    })
                    .await?;
                    svc.save_workspace(job_name, a.dialect, a.default_db.as_deref(), &ws)
                        .await
                })?;
                eprintln!(
                    "saved job={} batch_id={} tables={} table_edges={} column_edges={}",
                    summary.job_name,
                    summary.batch_id,
                    summary.tables_written,
                    summary.table_edges_written,
                    summary.column_edges_written
                );
            }
            let res = &ws.result;
            let json = if a.pretty {
                serde_json::to_string_pretty(res)?
            } else {
                serde_json::to_string(res)?
            };
            match &a.out {
                Some(p) => std::fs::write(p, json)?,
                None => {
                    let stdout = std::io::stdout();
                    let mut h = stdout.lock();
                    h.write_all(json.as_bytes())?;
                    h.write_all(b"\n")?;
                }
            }
            eprintln!(
                "statements={} parsed={} fallback={} failed={} skipped={} tables={} table_edges={} column_edges={} elapsed={:.1}ms",
                res.stats.statements,
                res.stats.parsed,
                res.stats.fallback,
                res.stats.failed,
                res.stats.skipped,
                res.tables.len(),
                res.table_edges.len(),
                res.column_edges.len(),
                res.stats.elapsed_ms
            );
        }
        Cmd::Serve(a) => {
            let mut files = Vec::new();
            for p in &a.files {
                let bytes = std::fs::read(p)?;
                files.push(InputFile {
                    name: p.to_string_lossy().into_owned(),
                    bytes,
                });
            }
            let db_cfg = resolve_db_url(a.db_url).map(|url| DbConfig {
                url,
                auto_create_table: !a.no_auto_create_table,
                use_hot_index: true,
            });
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            rt.block_on(hive_sql_parse::server::run_with_options(
                &a.host,
                a.port,
                files,
                a.default_db,
                a.dialect,
                db_cfg,
            ))?;
        }
        Cmd::Query(q) => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            rt.block_on(async {
                match q.target {
                    QuerySubcmd::Table { table, common } => {
                        let url = resolve_db_url(common.db_url).ok_or_else(|| {
                            anyhow::anyhow!(
                                "请通过 --db-url 或环境变量 HL_DB_URL 指定 PostgreSQL 连接串"
                            )
                        })?;
                        let svc = LineageService::connect(DbConfig {
                            url,
                            auto_create_table: false,
                            use_hot_index: false,
                        })
                        .await?;
                        let res = svc
                            .query_table_lineage(
                                &table,
                                common.direction,
                                common.hops,
                                QueryScope::from_all_history(common.all_history),
                            )
                            .await?;
                        write_json_stdout(&res, common.pretty)
                    }
                    QuerySubcmd::TableTasks { table, common } => {
                        let url = resolve_db_url(common.db_url).ok_or_else(|| {
                            anyhow::anyhow!(
                                "请通过 --db-url 或环境变量 HL_DB_URL 指定 PostgreSQL 连接串"
                            )
                        })?;
                        let svc = LineageService::connect(DbConfig {
                            url,
                            auto_create_table: false,
                            use_hot_index: false,
                        })
                        .await?;
                        let res = svc
                            .query_table_task_lineage(
                                &table,
                                common.direction,
                                common.hops,
                                QueryScope::from_all_history(common.all_history),
                            )
                            .await?;
                        write_json_stdout(&res, common.pretty)
                    }
                    QuerySubcmd::Task { job, common } => {
                        let url = resolve_db_url(common.db_url).ok_or_else(|| {
                            anyhow::anyhow!(
                                "请通过 --db-url 或环境变量 HL_DB_URL 指定 PostgreSQL 连接串"
                            )
                        })?;
                        let svc = LineageService::connect(DbConfig {
                            url,
                            auto_create_table: false,
                            use_hot_index: false,
                        })
                        .await?;
                        let res = svc
                            .query_task_lineage(
                                &job,
                                common.direction,
                                common.hops,
                                QueryScope::from_all_history(common.all_history),
                            )
                            .await?;
                        write_json_stdout(&res, common.pretty)
                    }
                    QuerySubcmd::Column {
                        field,
                        db,
                        table,
                        column,
                        common,
                    } => {
                        let url = resolve_db_url(common.db_url).ok_or_else(|| {
                            anyhow::anyhow!(
                                "请通过 --db-url 或环境变量 HL_DB_URL 指定 PostgreSQL 连接串"
                            )
                        })?;
                        let svc = LineageService::connect(DbConfig {
                            url,
                            auto_create_table: false,
                            use_hot_index: false,
                        })
                        .await?;
                        let res = svc
                            .query_column_lineage(
                                db.as_deref(),
                                table.as_deref(),
                                column.as_deref(),
                                field.as_deref(),
                                common.direction,
                                common.hops,
                                QueryScope::from_all_history(common.all_history),
                            )
                            .await?;
                        write_json_stdout(&res, common.pretty)
                    }
                }
            })?;
        }
        Cmd::Ddl => {
            let stdout = std::io::stdout();
            let mut h = stdout.lock();
            h.write_all(postgres_ddl().as_bytes())?;
        }
    }
    Ok(())
}
