//! 血缘持久化与全局查询端到端测试（memory:// 后端覆盖抽取、追加、四类查询、批次口径与环路跳数）

use hive_sql_parse::db::extract::{extract_batch_payload, split_job_name};
use hive_sql_parse::db::query::LineageService;
use hive_sql_parse::db::schema::{mask_db_url, postgres_ddl};
use hive_sql_parse::db::{DbConfig, LineageDirection, QueryScope};
use hive_sql_parse::{analyze_with_sources, InputFile, Options, SqlDialect};

fn ws_of(sql: &str, job: &str) -> hive_sql_parse::AnalyzedWorkspace {
    analyze_with_sources(
        vec![InputFile {
            name: "job.sql".into(),
            bytes: sql.as_bytes().to_vec(),
        }],
        &Options {
            job_name: Some(job.into()),
            analyze_select: Some(false),
            ..Options::default()
        },
    )
}

async fn mem_service() -> LineageService {
    LineageService::connect(DbConfig {
        url: "memory://".into(),
        auto_create_table: true,
        use_hot_index: true,
    })
    .await
    .unwrap()
}

async fn save(svc: &LineageService, job: &str, sql: &str) -> hive_sql_parse::db::SaveSummary {
    let ws = ws_of(sql, job);
    svc.save_workspace(job, SqlDialect::Hive, None, &ws)
        .await
        .unwrap()
}

const PIPELINE: &str = "
INSERT OVERWRITE TABLE dwd.order_detail
SELECT order_id, cust_id, amt, dt FROM ods.orders WHERE status='paid';
INSERT OVERWRITE TABLE dws.region_daily
SELECT region, sum(amt) AS gmv FROM dwd.order_detail d JOIN dim.area a ON d.cust_id=a.cust_id GROUP BY region;
INSERT OVERWRITE TABLE ads.top_cust
SELECT cust_id, sum(amt) AS total FROM dwd.order_detail GROUP BY cust_id;
";

#[test]
fn job_name_split_and_model_field() {
    assert_eq!(
        split_job_name("dw_etl.task_order").unwrap(),
        (
            "dw_etl.task_order".into(),
            "dw_etl".into(),
            "task_order".into()
        )
    );
    assert_eq!(
        split_job_name("solo_task").unwrap().1,
        "",
        "无 DAG 前缀时 dag_name 为空"
    );
    assert!(split_job_name("  ").is_err());
    assert!(split_job_name("dag.").is_err());

    let ws = ws_of(PIPELINE, "my_dag.my_task");
    assert_eq!(ws.result.job_name.as_deref(), Some("my_dag.my_task"));
    // 序列化 JSON 含 job_name 字段
    let v = serde_json::to_value(&ws.result).unwrap();
    assert_eq!(v["job_name"], "my_dag.my_task");
    // 无 job_name 时字段整体省略（兼容旧输出）
    let ws2 = analyze_with_sources(
        vec![InputFile {
            name: "a.sql".into(),
            bytes: b"SELECT 1".to_vec(),
        }],
        &Options::default(),
    );
    let v2 = serde_json::to_value(&ws2.result).unwrap();
    assert!(v2.get("job_name").is_none());
}

#[test]
fn extract_payload_enriches_meta() {
    let ws = ws_of(PIPELINE, "etl.daily_etl");
    let p = extract_batch_payload("etl.daily_etl", SqlDialect::Hive, Some("dw"), &ws).unwrap();
    assert_eq!(p.batch.job_name, "etl.daily_etl");
    assert_eq!(p.batch.dag_name, "etl");
    assert_eq!(p.batch.task_name, "daily_etl");
    assert!(p.batch.batch_id > 0);
    assert_eq!(p.batch.default_db, "dw");
    assert!(p.batch.statement_count >= 3);
    assert_eq!(p.job_tables.len(), p.batch.table_count as usize);
    assert_eq!(p.table_edges.len(), p.batch.table_edge_count as usize);
    assert_eq!(p.column_edges.len(), p.batch.column_edge_count as usize);
    // 每个 job_tables 行都带 job_name + batch_id
    assert!(p
        .job_tables
        .iter()
        .all(|t| t.job_name == "etl.daily_etl" && t.batch_id == p.batch.batch_id));
    // 表名拆分正确
    let od = p
        .job_tables
        .iter()
        .find(|t| t.full_table_name == "dwd.order_detail")
        .unwrap();
    assert_eq!(od.db_name, "dwd");
    assert_eq!(od.table_name, "order_detail");
    assert!(od.is_target);
    // sql_preview 非空
    assert!(p.table_edges.iter().all(|e| !e.sql_preview.is_empty()));
}

#[tokio::test]
async fn memory_backend_save_and_table_lineage() {
    let svc = mem_service().await;
    assert_eq!(svc.backend_name(), "memory");
    assert!(svc.hot_index_enabled());

    let sum = save(&svc, "etl.daily_etl", PIPELINE).await;
    assert_eq!(sum.job_name, "etl.daily_etl");
    assert!(sum.tables_written >= 5);
    assert!(sum.table_edges_written >= 3);

    // 查询 1：以 dwd.order_detail 为中心的表级血缘
    let res = svc
        .query_table_lineage(
            "dwd.order_detail",
            LineageDirection::Both,
            5,
            QueryScope::LatestBatch,
        )
        .await
        .unwrap();
    assert_eq!(res.query_table, "dwd.order_detail");
    assert!(res
        .upstream_tables
        .iter()
        .any(|t| t.full_table_name == "ods.orders"));
    // dim.area 是下游 dws.region_daily 的上游，不属于 dwd.order_detail 上游
    assert!(!res
        .upstream_tables
        .iter()
        .any(|t| t.full_table_name == "dim.area"));
    assert!(res
        .downstream_tables
        .iter()
        .any(|t| t.full_table_name == "dws.region_daily"));
    assert!(res
        .downstream_tables
        .iter()
        .any(|t| t.full_table_name == "ads.top_cust"));
    // 上游一跳距离正确
    let ods = res
        .upstream_tables
        .iter()
        .find(|t| t.full_table_name == "ods.orders")
        .unwrap();
    assert_eq!(ods.distance, 1);
    assert_eq!(ods.db_name, "ods");
    // 边附带加工任务
    let e = res
        .edges
        .iter()
        .find(|e| e.source == "ods.orders" && e.target == "dwd.order_detail")
        .unwrap();
    assert!(e.jobs.contains(&"etl.daily_etl".to_string()));
    assert!(e.sql_preview.contains("order_detail"));
    // graph 视图结构
    assert_eq!(res.graph["mode"], "table");
    assert!(res.graph["tables"].as_array().unwrap().len() >= 4);
    assert!(res.graph["focus_id"].is_number());

    // 仅上游方向
    let up = svc
        .query_table_lineage(
            "dwd.order_detail",
            LineageDirection::Upstream,
            5,
            QueryScope::LatestBatch,
        )
        .await
        .unwrap();
    assert!(up.downstream_tables.is_empty());
    // 跳数限制：hops=0 时查全图，hops=1 时 ods 下游不可达
    let limited = svc
        .query_table_lineage(
            "ods.orders",
            LineageDirection::Downstream,
            1,
            QueryScope::LatestBatch,
        )
        .await
        .unwrap();
    assert!(limited.downstream_tables.iter().all(|t| t.distance <= 1
        && t.full_table_name != "ads.top_cust"
        && t.full_table_name != "dws.region_daily"));
}

#[tokio::test]
async fn memory_backend_table_task_lineage() {
    let svc = mem_service().await;
    // 两个任务：上游任务写 dwd，下游任务读 dwd 写 dws
    save(
        &svc,
        "etl.dwd_build",
        "INSERT INTO dwd.order_detail SELECT id, amt FROM ods.orders;",
    )
    .await;
    save(
        &svc,
        "etl.dws_agg",
        "INSERT INTO dws.daily SELECT cust_id, sum(amt) FROM dwd.order_detail GROUP BY cust_id;",
    )
    .await;

    let res = svc
        .query_table_task_lineage(
            "dwd.order_detail",
            LineageDirection::Both,
            5,
            QueryScope::LatestBatch,
        )
        .await
        .unwrap();
    assert_eq!(res.query_table, "dwd.order_detail");
    assert!(res.producer_jobs.contains(&"etl.dwd_build".to_string()));
    assert!(res.consumer_jobs.contains(&"etl.dws_agg".to_string()));
    assert!(res
        .upstream_jobs
        .iter()
        .any(|j| j.job_name == "etl.dwd_build"));
    assert!(res
        .downstream_jobs
        .iter()
        .any(|j| j.job_name == "etl.dws_agg"));
    // 经由锚点表的任务依赖边
    assert!(res
        .task_edges
        .iter()
        .any(|e| e.source_job == "etl.dwd_build"
            && e.target_job == "etl.dws_agg"
            && e.via_tables.contains(&"dwd.order_detail".to_string())));
    // graph：锚点表节点 + 任务节点
    assert_eq!(res.graph["mode"], "task");
    let tables = res.graph["tables"].as_array().unwrap();
    assert!(tables.iter().any(|t| t["kind"] == "anchor_table"));
    assert!(tables
        .iter()
        .any(|t| t["kind"] == "job" && t["name"] == "etl.dwd_build"));
    assert!(tables
        .iter()
        .any(|t| t["kind"] == "job" && t["name"] == "etl.dws_agg"));
}

#[tokio::test]
async fn memory_backend_task_lineage_chains() {
    let svc = mem_service().await;
    // 三任务链路：ods_load(写 ods.orders) -> dwd_build(读 ods.orders 写 dwd.order_detail) -> dws_agg(读 dwd.order_detail 写 dws.daily)
    save(
        &svc,
        "dag.ods_load",
        "INSERT INTO ods.orders SELECT * FROM ext.raw_orders;",
    )
    .await;
    save(
        &svc,
        "dag.dwd_build",
        "INSERT INTO dwd.order_detail SELECT id FROM ods.orders;",
    )
    .await;
    save(
        &svc,
        "dag.dws_agg",
        "INSERT INTO dws.daily SELECT cust_id FROM dwd.order_detail;",
    )
    .await;

    // 精确任务名查询
    let res = svc
        .query_task_lineage(
            "dag.dwd_build",
            LineageDirection::Both,
            5,
            QueryScope::LatestBatch,
        )
        .await
        .unwrap();
    assert_eq!(res.matched_job.as_deref(), Some("dag.dwd_build"));
    assert!(res
        .upstream_jobs
        .iter()
        .any(|j| j.job_name == "dag.ods_load"));
    assert!(res
        .downstream_jobs
        .iter()
        .any(|j| j.job_name == "dag.dws_agg"));
    // 完整链路：ods_load -> dwd_build -> dws_agg
    assert!(
        res.chains.iter().any(|c| {
            c.iter()
                .position(|x| x == "dag.ods_load")
                .unwrap_or(usize::MAX)
                < c.iter()
                    .position(|x| x == "dag.dwd_build")
                    .unwrap_or(usize::MAX)
                && c.iter()
                    .position(|x| x == "dag.dws_agg")
                    .unwrap_or(usize::MAX)
                    > c.iter()
                        .position(|x| x == "dag.dwd_build")
                        .unwrap_or(usize::MAX)
                && c.contains(&"dag.ods_load".to_string())
                && c.contains(&"dag.dws_agg".to_string())
        }),
        "chains={:?}",
        res.chains
    );
    // 任务依赖边带媒介表
    assert!(res.task_edges.iter().any(|e| e.source_job == "dag.ods_load"
        && e.target_job == "dag.dwd_build"
        && e.via_tables.contains(&"ods.orders".to_string())));
    // 模糊搜索
    let fuzzy = svc
        .query_task_lineage("dwd", LineageDirection::Both, 5, QueryScope::LatestBatch)
        .await
        .unwrap();
    assert!(fuzzy.matched_job.is_some());
    // 未命中
    let miss = svc
        .query_task_lineage(
            "no_such_job",
            LineageDirection::Both,
            5,
            QueryScope::LatestBatch,
        )
        .await
        .unwrap();
    assert!(miss.matched_job.is_none());
}

#[tokio::test]
async fn memory_backend_column_lineage() {
    let svc = mem_service().await;
    save(
        &svc,
        "etl.dwd_build",
        "INSERT INTO dwd.order_detail SELECT order_id AS oid, amt AS amount FROM ods.orders;",
    )
    .await;
    save(&svc, "etl.dws_agg", "INSERT INTO dws.daily SELECT cust_id, sum(amount) AS gmv FROM dwd.order_detail GROUP BY cust_id;").await;

    // 三段式参数查询
    let res = svc
        .query_column_lineage(
            Some("dwd"),
            Some("order_detail"),
            Some("amount"),
            None,
            LineageDirection::Both,
            5,
            QueryScope::LatestBatch,
        )
        .await
        .unwrap();
    assert_eq!(res.query_column, "dwd.order_detail.amount");
    assert!(res
        .upstream_columns
        .iter()
        .any(|c| c.qualified_column == "ods.orders.amt"));
    assert!(res
        .downstream_columns
        .iter()
        .any(|c| c.qualified_column == "dws.daily.gmv"));
    // 通过字段查询关联表与关联任务血缘
    assert!(res
        .upstream_tables
        .iter()
        .any(|t| t.full_table_name == "ods.orders" && t.distance == 1 && t.columns == vec!["amt"]));
    assert!(res
        .downstream_tables
        .iter()
        .any(|t| t.full_table_name == "dws.daily" && t.distance == 1 && t.columns == vec!["gmv"]));
    assert!(res
        .upstream_jobs
        .iter()
        .any(|j| j.job_name == "etl.dwd_build" && j.distance == 1));
    assert!(res
        .downstream_jobs
        .iter()
        .any(|j| j.job_name == "etl.dws_agg" && j.distance == 1));
    // sum() 聚合应标记为 aggregate
    let gmv_edge = res.edges.iter().find(|e| e.target_column == "gmv").unwrap();
    assert_eq!(gmv_edge.kind, "aggregate");
    assert!(gmv_edge.jobs.contains(&"etl.dws_agg".to_string()));
    // 端到端链路：ods.orders.amt -> dwd.order_detail.amount -> dws.daily.gmv
    assert!(
        res.chains.iter().any(|c| c
            == &vec![
                "ods.orders.amt".to_string(),
                "dwd.order_detail.amount".to_string(),
                "dws.daily.gmv".to_string()
            ]
            || (c.first().map(|x| x.as_str()) == Some("ods.orders.amt")
                && c.last().map(|x| x.as_str()) == Some("dws.daily.gmv"))),
        "chains={:?}",
        res.chains
    );
    // graph 字段级视图
    assert_eq!(res.graph["mode"], "column");
    assert!(res.graph["focus_col"].is_string());
    assert!(!res.graph["col_edges"].as_array().unwrap().is_empty());

    // 合并格式 field=库名.表名.字段名
    let res2 = svc
        .query_column_lineage(
            None,
            None,
            None,
            Some("dwd.order_detail.amount"),
            LineageDirection::Upstream,
            5,
            QueryScope::LatestBatch,
        )
        .await
        .unwrap();
    assert_eq!(res2.query_column, "dwd.order_detail.amount");
    assert!(res2
        .upstream_columns
        .iter()
        .any(|c| c.qualified_column == "ods.orders.amt"));

    // 非法输入
    assert!(svc
        .query_column_lineage(
            None,
            None,
            None,
            Some("only_one"),
            LineageDirection::Both,
            5,
            QueryScope::LatestBatch
        )
        .await
        .is_err());
}

#[tokio::test]
async fn latest_batch_vs_all_history() {
    let svc = mem_service().await;
    // 同一 job 连续追加两个版本：v1 读 ods.orders，v2 改为读 ods.orders_v2
    save(
        &svc,
        "etl.job",
        "INSERT INTO dwd.t SELECT a FROM ods.orders;",
    )
    .await;
    save(
        &svc,
        "etl.job",
        "INSERT INTO dwd.t SELECT a FROM ods.orders_v2;",
    )
    .await;

    // 默认 latest_batch：仅 v2 血缘生效
    let latest = svc
        .query_table_lineage(
            "dwd.t",
            LineageDirection::Upstream,
            5,
            QueryScope::LatestBatch,
        )
        .await
        .unwrap();
    assert!(latest
        .upstream_tables
        .iter()
        .any(|t| t.full_table_name == "ods.orders_v2"));
    assert!(!latest
        .upstream_tables
        .iter()
        .any(|t| t.full_table_name == "ods.orders"));

    // all_history：v1+v2 均在
    let all = svc
        .query_table_lineage(
            "dwd.t",
            LineageDirection::Upstream,
            5,
            QueryScope::AllHistory,
        )
        .await
        .unwrap();
    assert!(all
        .upstream_tables
        .iter()
        .any(|t| t.full_table_name == "ods.orders"));
    assert!(all
        .upstream_tables
        .iter()
        .any(|t| t.full_table_name == "ods.orders_v2"));

    // search_jobs 默认只返回每个 job 一条最新记录
    let jobs = svc
        .search_jobs("etl.job", 10, QueryScope::LatestBatch)
        .await
        .unwrap();
    assert_eq!(jobs.len(), 1);
    // search_tables 也按最新批次聚合
    let tables = svc
        .search_tables("dwd", 10, QueryScope::LatestBatch)
        .await
        .unwrap();
    let dt = tables
        .iter()
        .find(|t| t.full_table_name == "dwd.t")
        .unwrap();
    assert!(dt.producer_jobs.contains(&"etl.job".to_string()));
}

#[tokio::test]
async fn cycle_safe_and_hop_limit() {
    let svc = mem_service().await;
    // 制造环：a -> b -> c -> a（同一任务追加三批不同 SQL 也可；直接用三个任务成环更真实）
    save(&svc, "j.a", "INSERT INTO x.a SELECT v FROM x.b;").await;
    save(&svc, "j.b", "INSERT INTO x.b SELECT v FROM x.c;").await;
    save(&svc, "j.c", "INSERT INTO x.c SELECT v FROM x.a;").await;

    // hops=0 全链路时不会死循环
    let res = svc
        .query_table_lineage("x.a", LineageDirection::Both, 0, QueryScope::LatestBatch)
        .await
        .unwrap();
    assert!(res
        .upstream_tables
        .iter()
        .any(|t| t.full_table_name == "x.b"));
    assert!(res
        .upstream_tables
        .iter()
        .any(|t| t.full_table_name == "x.c"));
    // 环路下游也安全
    assert!(res
        .downstream_tables
        .iter()
        .any(|t| t.full_table_name == "x.c"));
    // 任务级链路也成环安全
    let t = svc
        .query_task_lineage("j.a", LineageDirection::Both, 0, QueryScope::LatestBatch)
        .await
        .unwrap();
    assert!(t.chains.iter().all(|c| c.len() <= 64));
}

#[tokio::test]
async fn hot_index_incremental_refresh() {
    let svc = mem_service().await;
    save(&svc, "etl.v1", "INSERT INTO dwd.t SELECT a FROM ods.s1;").await;
    let r1 = svc
        .query_table_lineage(
            "dwd.t",
            LineageDirection::Upstream,
            5,
            QueryScope::LatestBatch,
        )
        .await
        .unwrap();
    assert_eq!(r1.upstream_tables.len(), 1);
    assert_eq!(r1.upstream_tables[0].full_table_name, "ods.s1");

    // 第二批次追加：同 job 新血缘
    save(&svc, "etl.v1", "INSERT INTO dwd.t SELECT a FROM ods.s2;").await;
    // 服务内热图按水位线自动增量刷新（同连接查询即可看到新值）
    let r2 = svc
        .query_table_lineage(
            "dwd.t",
            LineageDirection::Upstream,
            5,
            QueryScope::LatestBatch,
        )
        .await
        .unwrap();
    assert_eq!(r2.upstream_tables.len(), 1);
    assert_eq!(r2.upstream_tables[0].full_table_name, "ods.s2");
}

#[tokio::test]
async fn blank_job_name_rejected() {
    let svc = mem_service().await;
    let ws = ws_of(PIPELINE, "etl.x");
    // 空白 job_name -> 明确错误
    let e = svc
        .save_workspace("   ", SqlDialect::Hive, None, &ws)
        .await
        .unwrap_err();
    assert!(e.to_string().contains("job_name"));
    let e2 = svc
        .save_workspace("dag.", SqlDialect::Hive, None, &ws)
        .await
        .unwrap_err();
    assert!(e2.to_string().contains("job_name") || e2.to_string().contains("格式非法"));
}

#[tokio::test]
async fn ddl_and_url_mask() {
    let ddl = postgres_ddl();
    for tbl in [
        "hl_job_batches",
        "hl_job_tables",
        "hl_table_edges",
        "hl_column_edges",
    ] {
        assert!(ddl.contains(tbl), "ddl missing {tbl}");
    }
    assert!(ddl.contains("CREATE TABLE IF NOT EXISTS"));
    assert!(ddl.contains("batch_id"));
    assert!(ddl.contains("job_name"));

    let masked = mask_db_url("postgres://user:secret@127.0.0.1:5432/lineage?x=1");
    assert!(!masked.contains("secret"));
    assert!(masked.contains("127.0.0.1"));
    let masked2 = mask_db_url("host=127.0.0.1 port=5432 user=u password=p@ss dbname=l");
    assert!(!masked2.contains("p@ss"));
}
