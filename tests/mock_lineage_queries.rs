//! Mock 环境样例数据入库与六维血缘查询端到端测试（memory:// 后端 + 真实 HTTP 服务链路）
//!
//! 覆盖链路：
//! - ODS (Hive + Spark) -> DWD (含临时表穿透消解) -> DWS -> ADS (含 `*` 通配穿透)
//! - 表查表 / 表查任务 / 任务查任务 / 字段查字段 / 字段查表 / 字段查任务
//! - 会话级解析 API（overview / subgraph）与任务重跑口径对比（latest_batch vs all_history）

use hive_sql_parse::db::query::LineageService;
use hive_sql_parse::db::{DbConfig, LineageDirection, QueryScope};
use hive_sql_parse::{server, SqlDialect};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

struct SampleJob {
    job_name: &'static str,
    dialect: &'static str,
    sql: &'static str,
}

const SAMPLE_JOBS: &[SampleJob] = &[
    SampleJob {
        job_name: "ods_dag.sync_orders",
        dialect: "hive",
        sql: "
            INSERT OVERWRITE TABLE ods.orders_clean
            SELECT o.order_id, o.cust_id, o.pay_amt, o.order_dt
            FROM ext.mysql_orders o
            JOIN ext.mysql_customers c ON o.cust_id = c.cust_id
            WHERE o.status = 'PAID';
        ",
    },
    SampleJob {
        job_name: "ods_dag.sync_items",
        dialect: "spark",
        sql: "
            CREATE TABLE ods.order_items_spark USING PARQUET AS
            SELECT order_id, sku_id, item_qty, unit_price, item_qty * unit_price AS item_amt
            FROM ext.mysql_order_items;
        ",
    },
    SampleJob {
        job_name: "dwd_dag.build_order_wide",
        dialect: "hive",
        sql: "
            CREATE TEMPORARY TABLE tmp_item_agg AS
            SELECT order_id, count(sku_id) AS sku_cnt, sum(item_amt) AS items_total
            FROM ods.order_items_spark
            GROUP BY order_id;

            INSERT OVERWRITE TABLE dwd.order_wide PARTITION(dt)
            SELECT o.order_id,
                   o.cust_id,
                   o.pay_amt,
                   t.sku_cnt,
                   t.items_total,
                   coalesce(o.pay_amt, t.items_total) AS final_amt,
                   o.order_dt AS dt
            FROM ods.orders_clean o
            LEFT JOIN tmp_item_agg t ON o.order_id = t.order_id;
        ",
    },
    SampleJob {
        job_name: "dws_dag.user_gmv_daily",
        dialect: "hive",
        sql: "
            INSERT OVERWRITE TABLE dws.user_gmv_daily
            SELECT w.cust_id,
                   c.region,
                   count(w.order_id) AS order_cnt,
                   sum(w.final_amt) AS gmv_amt,
                   max(w.dt) AS last_order_dt
            FROM dwd.order_wide w
            LEFT JOIN dim.customer_profile c ON w.cust_id = c.cust_id
            GROUP BY w.cust_id, c.region;
        ",
    },
    SampleJob {
        job_name: "ads_dag.region_dashboard",
        dialect: "spark",
        sql: "
            CREATE TABLE ads.region_kpi USING PARQUET AS
            SELECT region,
                   sum(gmv_amt) AS total_gmv,
                   sum(order_cnt) AS total_orders,
                   row_number() OVER (ORDER BY sum(gmv_amt) DESC) AS gmv_rank
            FROM dws.user_gmv_daily
            GROUP BY region;
        ",
    },
    SampleJob {
        job_name: "ads_dag.user_top_export",
        dialect: "hive",
        sql: "
            INSERT OVERWRITE TABLE ads.user_gmv_mirror
            SELECT * FROM dws.user_gmv_daily;
        ",
    },
    SampleJob {
        job_name: "ads_dag.vip_alert",
        dialect: "hive",
        sql: "
            INSERT INTO ads.vip_alert
            SELECT cust_id, gmv_amt AS alert_amt
            FROM ads.user_gmv_mirror
            WHERE gmv_amt > 10000;
        ",
    },
];

fn http_request(port: u16, method: &str, path: &str, body: Option<&str>) -> (u16, Value) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect mock server");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let payload = body.unwrap_or("");
    let req = if body.is_some() {
        format!(
            "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
            payload.len()
        )
    } else {
        format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n")
    };
    stream.write_all(req.as_bytes()).unwrap();

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    let text = String::from_utf8_lossy(&raw);
    let (head, resp_body) = text.split_once("\r\n\r\n").expect("valid http response");
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse::<u16>().ok())
        .unwrap_or(0);
    let json_val = serde_json::from_str(resp_body).unwrap_or_else(|e| {
        panic!("invalid json response (status={status}): {e}; body={resp_body}")
    });
    (status, json_val)
}

async fn wait_server_ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let ready = tokio::task::spawn_blocking(move || {
            if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
                let _ = s.set_read_timeout(Some(Duration::from_millis(200)));
                let _ = s.write_all(
                    b"GET /api/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                );
                let mut buf = String::new();
                if s.read_to_string(&mut buf).is_ok() && buf.contains("200") {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false);
        if ready {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("mock HTTP server on port {port} did not become ready in time");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mock_http_server_seed_samples_and_verify_all_lineage_queries() {
    let probe = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let srv = tokio::spawn(async move {
        let _ = server::run_with_options(
            "127.0.0.1",
            port,
            Vec::new(),
            None,
            SqlDialect::Hive,
            Some(DbConfig::new("memory://")),
        )
        .await;
    });
    wait_server_ready(port).await;

    // 1. 确认数据库状态为 memory 模拟后端且热图索引已开启
    let (st, db_status) =
        tokio::task::spawn_blocking(move || http_request(port, "GET", "/api/db/status", None))
            .await
            .unwrap();
    assert_eq!(st, 200);
    assert_eq!(db_status["configured"], true);
    assert_eq!(db_status["ok"], true);
    assert_eq!(db_status["backend"], "memory");
    assert_eq!(db_status["hot_index"], true);

    // 2. 通过 HTTP /api/analyze_text (save_db=true) 依次注入 Hive/Spark 样例任务
    let mut last_session = String::new();
    for job in SAMPLE_JOBS {
        let body = json!({
            "sql": job.sql,
            "name": format!("{}.sql", job.job_name),
            "dialect": job.dialect,
            "job_name": job.job_name,
            "save_db": true
        })
        .to_string();
        let (st, resp) = tokio::task::spawn_blocking(move || {
            http_request(port, "POST", "/api/analyze_text", Some(&body))
        })
        .await
        .unwrap();
        assert_eq!(st, 200, "job={} resp={resp}", job.job_name);
        assert_eq!(
            resp["stats"]["failed"], 0,
            "job={} resp={resp}",
            job.job_name
        );
        assert_eq!(resp["saved"]["job_name"], job.job_name);
        last_session = resp["session"].as_str().unwrap().to_string();
    }

    // 验证单脚本会话级视图接口（overview + 列追踪 subgraph）仍正常工作
    let sess_path = format!("/api/s/{last_session}/overview");
    let (st, ov) = tokio::task::spawn_blocking(move || http_request(port, "GET", &sess_path, None))
        .await
        .unwrap();
    assert_eq!(st, 200);
    assert_eq!(ov["counts"]["tables"], 2);

    // 3. 检索任务与表列表（确认临时表 tmp_item_agg 未泄漏入持久化表目录）
    let (st, jobs_resp) = tokio::task::spawn_blocking(move || {
        http_request(port, "GET", "/api/db/jobs?limit=50", None)
    })
    .await
    .unwrap();
    assert_eq!(st, 200);
    assert_eq!(
        jobs_resp["items"].as_array().unwrap().len(),
        SAMPLE_JOBS.len()
    );

    let (st, tbls_resp) = tokio::task::spawn_blocking(move || {
        http_request(port, "GET", "/api/db/tables?limit=50", None)
    })
    .await
    .unwrap();
    assert_eq!(st, 200);
    let all_tbl_names: Vec<&str> = tbls_resp["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["full_table_name"].as_str())
        .collect();
    assert!(!all_tbl_names.contains(&"tmp_item_agg"));
    assert!(all_tbl_names.contains(&"dwd.order_wide"));
    assert!(all_tbl_names.contains(&"ads.region_kpi"));

    // 4. 查询一：通过表查询表的上下游加工血缘（dwd.order_wide，hops=0 穿透全链）
    let (st, tbl_lin) = tokio::task::spawn_blocking(move || {
        http_request(
            port,
            "GET",
            "/api/db/lineage/table?table=dwd.order_wide&direction=both&hops=0",
            None,
        )
    })
    .await
    .unwrap();
    assert_eq!(st, 200);
    let up_tbls: Vec<&str> = tbl_lin["upstream_tables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["full_table_name"].as_str())
        .collect();
    let dn_tbls: Vec<&str> = tbl_lin["downstream_tables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["full_table_name"].as_str())
        .collect();
    assert!(up_tbls.contains(&"ods.orders_clean"));
    assert!(up_tbls.contains(&"ods.order_items_spark"));
    assert!(up_tbls.contains(&"ext.mysql_orders"));
    assert!(up_tbls.contains(&"ext.mysql_order_items"));
    assert!(dn_tbls.contains(&"dws.user_gmv_daily"));
    assert!(dn_tbls.contains(&"ads.region_kpi"));
    assert!(dn_tbls.contains(&"ads.user_gmv_mirror"));
    assert!(dn_tbls.contains(&"ads.vip_alert"));
    assert_eq!(tbl_lin["graph"]["mode"], "table");

    // 跳数裁剪验证：hops=1 时只返回直连 1 跳表，不包含 ext.* 与 ads.*
    let (st, tbl_hop1) = tokio::task::spawn_blocking(move || {
        http_request(
            port,
            "GET",
            "/api/db/lineage/table?table=dwd.order_wide&direction=both&hops=1",
            None,
        )
    })
    .await
    .unwrap();
    assert_eq!(st, 200);
    assert!(tbl_hop1["upstream_tables"]
        .as_array()
        .unwrap()
        .iter()
        .all(|t| t["distance"] == 1));
    assert!(tbl_hop1["downstream_tables"]
        .as_array()
        .unwrap()
        .iter()
        .all(|t| t["distance"] == 1));

    // 5. 查询二：通过表查询任务的上下游加工血缘（dws.user_gmv_daily）
    let (st, tbl_tasks) = tokio::task::spawn_blocking(move || {
        http_request(
            port,
            "GET",
            "/api/db/lineage/table_tasks?table=dws.user_gmv_daily&direction=both&hops=5",
            None,
        )
    })
    .await
    .unwrap();
    assert_eq!(st, 200);
    assert_eq!(
        tbl_tasks["producer_jobs"],
        json!(["dws_dag.user_gmv_daily"])
    );
    let consumers: Vec<&str> = tbl_tasks["consumer_jobs"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(consumers.contains(&"ads_dag.region_dashboard"));
    assert!(consumers.contains(&"ads_dag.user_top_export"));
    let up_jobs: Vec<&str> = tbl_tasks["upstream_jobs"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|j| j["job_name"].as_str())
        .collect();
    let dn_jobs: Vec<&str> = tbl_tasks["downstream_jobs"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|j| j["job_name"].as_str())
        .collect();
    assert!(up_jobs.contains(&"dwd_dag.build_order_wide"));
    assert!(up_jobs.contains(&"ods_dag.sync_orders"));
    assert!(up_jobs.contains(&"ods_dag.sync_items"));
    assert!(dn_jobs.contains(&"ads_dag.vip_alert"));
    assert_eq!(tbl_tasks["graph"]["mode"], "task");

    // 6. 查询三：通过任务查询任务的上下游血缘（dwd_dag.build_order_wide）
    let (st, task_lin) = tokio::task::spawn_blocking(move || {
        http_request(
            port,
            "GET",
            "/api/db/lineage/task?job=dwd_dag.build_order_wide&direction=both&hops=5",
            None,
        )
    })
    .await
    .unwrap();
    assert_eq!(st, 200);
    assert_eq!(task_lin["matched_job"], "dwd_dag.build_order_wide");
    let chains = task_lin["chains"].as_array().unwrap();
    assert!(
        chains.iter().any(|c| {
            let arr: Vec<&str> = c
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|x| x.as_str())
                .collect();
            arr.contains(&"ods_dag.sync_items")
                && arr.contains(&"dwd_dag.build_order_wide")
                && arr.contains(&"dws_dag.user_gmv_daily")
                && arr.contains(&"ads_dag.vip_alert")
        }),
        "chains={chains:?}"
    );

    // 7. 查询四/五/六：通过字段查询字段的上下游血缘 + 关联表的上下游血缘 + 关联任务的上下游血缘
    // 从中心字段 dws.user_gmv_daily.gmv_amt 出发，向上穿透临时表到 ods/ext，向下穿透 * 到 ads.vip_alert.alert_amt 与 ads.region_kpi.total_gmv
    let (st, col_lin) = tokio::task::spawn_blocking(move || {
        http_request(
            port,
            "GET",
            "/api/db/lineage/column?field=dws.user_gmv_daily.gmv_amt&direction=both&hops=0",
            None,
        )
    })
    .await
    .unwrap();
    assert_eq!(st, 200);
    assert_eq!(col_lin["query_column"], "dws.user_gmv_daily.gmv_amt");

    let up_cols: Vec<&str> = col_lin["upstream_columns"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|c| c["qualified_column"].as_str())
        .collect();
    let dn_cols: Vec<&str> = col_lin["downstream_columns"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|c| c["qualified_column"].as_str())
        .collect();
    assert!(
        up_cols.contains(&"dwd.order_wide.final_amt"),
        "up_cols={up_cols:?}"
    );
    assert!(
        up_cols.contains(&"ods.orders_clean.pay_amt"),
        "up_cols={up_cols:?}"
    );
    assert!(
        up_cols.contains(&"ods.order_items_spark.item_amt"),
        "up_cols={up_cols:?}"
    );
    assert!(
        up_cols.contains(&"ext.mysql_orders.pay_amt"),
        "up_cols={up_cols:?}"
    );
    assert!(
        up_cols.contains(&"ext.mysql_order_items.item_qty"),
        "up_cols={up_cols:?}"
    );
    assert!(
        dn_cols.contains(&"ads.region_kpi.total_gmv"),
        "dn_cols={dn_cols:?}"
    );
    assert!(
        dn_cols.contains(&"ads.vip_alert.alert_amt"),
        "dn_cols={dn_cols:?}"
    );

    // 字段 -> 上下游关联表
    let col_up_tbls: Vec<&str> = col_lin["upstream_tables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["full_table_name"].as_str())
        .collect();
    let col_dn_tbls: Vec<&str> = col_lin["downstream_tables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["full_table_name"].as_str())
        .collect();
    assert!(col_up_tbls.contains(&"dwd.order_wide"));
    assert!(col_up_tbls.contains(&"ods.order_items_spark"));
    assert!(col_up_tbls.contains(&"ext.mysql_order_items"));
    assert!(col_dn_tbls.contains(&"ads.region_kpi"));
    assert!(col_dn_tbls.contains(&"ads.vip_alert"));

    // 字段 -> 上下游关联加工任务
    let col_up_jobs: Vec<&str> = col_lin["upstream_jobs"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|j| j["job_name"].as_str())
        .collect();
    let col_dn_jobs: Vec<&str> = col_lin["downstream_jobs"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|j| j["job_name"].as_str())
        .collect();
    assert!(col_up_jobs.contains(&"dws_dag.user_gmv_daily"));
    assert!(col_up_jobs.contains(&"dwd_dag.build_order_wide"));
    assert!(col_up_jobs.contains(&"ods_dag.sync_orders"));
    assert!(col_up_jobs.contains(&"ods_dag.sync_items"));
    assert!(col_dn_jobs.contains(&"ads_dag.region_dashboard"));
    assert!(col_dn_jobs.contains(&"ads_dag.user_top_export"));
    assert!(col_dn_jobs.contains(&"ads_dag.vip_alert"));

    // 完整字段链路 chains
    let col_chains = col_lin["chains"].as_array().unwrap();
    assert!(
        col_chains.iter().any(|ch| {
            let arr: Vec<&str> = ch
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|x| x.as_str())
                .collect();
            arr.first() == Some(&"ext.mysql_order_items.item_qty")
                && arr.contains(&"dws.user_gmv_daily.gmv_amt")
                && arr.last() == Some(&"ads.region_kpi.total_gmv")
        }),
        "col_chains={col_chains:?}"
    );

    // 8. 同一任务重跑追加新批次：验证 all_history=false（仅最新） vs all_history=true（全历史合并）
    let rerun_body = json!({
        "sql": "INSERT INTO ads.vip_alert SELECT cust_id, gmv_amt AS alert_amt FROM dws.user_gmv_daily;",
        "name": "ads_dag.vip_alert.sql",
        "dialect": "hive",
        "job_name": "ads_dag.vip_alert",
        "save_db": true
    })
    .to_string();
    let (st, _) = tokio::task::spawn_blocking(move || {
        http_request(port, "POST", "/api/analyze_text", Some(&rerun_body))
    })
    .await
    .unwrap();
    assert_eq!(st, 200);

    let (st, latest_res) = tokio::task::spawn_blocking(move || {
        http_request(
            port,
            "GET",
            "/api/db/lineage/table?table=ads.vip_alert&direction=upstream&hops=1&all_history=false",
            None,
        )
    })
    .await
    .unwrap();
    assert_eq!(st, 200);
    let latest_ups: Vec<&str> = latest_res["upstream_tables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["full_table_name"].as_str())
        .collect();
    assert_eq!(latest_ups, vec!["dws.user_gmv_daily"]);

    let (st, hist_res) = tokio::task::spawn_blocking(move || {
        http_request(
            port,
            "GET",
            "/api/db/lineage/table?table=ads.vip_alert&direction=upstream&hops=1&all_history=true",
            None,
        )
    })
    .await
    .unwrap();
    assert_eq!(st, 200);
    let hist_ups: Vec<&str> = hist_res["upstream_tables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["full_table_name"].as_str())
        .collect();
    assert!(hist_ups.contains(&"dws.user_gmv_daily"));
    assert!(hist_ups.contains(&"ads.user_gmv_mirror"));

    srv.abort();
}

#[tokio::test]
async fn mock_service_hot_index_vs_direct_backend_consistency() {
    let svc_hot = LineageService::connect(DbConfig {
        url: "memory://".into(),
        auto_create_table: true,
        use_hot_index: true,
    })
    .await
    .unwrap();
    let svc_raw = LineageService::connect(DbConfig {
        url: "memory://".into(),
        auto_create_table: true,
        use_hot_index: false,
    })
    .await
    .unwrap();

    for job in SAMPLE_JOBS {
        let dialect = job.dialect.parse::<SqlDialect>().unwrap();
        let ws = hive_sql_parse::analyze_with_sources(
            vec![hive_sql_parse::InputFile {
                name: format!("{}.sql", job.job_name),
                bytes: job.sql.as_bytes().to_vec(),
            }],
            &hive_sql_parse::Options {
                dialect,
                job_name: Some(job.job_name.into()),
                analyze_select: Some(false),
                ..Default::default()
            },
        );
        svc_hot
            .save_workspace(job.job_name, dialect, None, &ws)
            .await
            .unwrap();
        svc_raw
            .save_workspace(job.job_name, dialect, None, &ws)
            .await
            .unwrap();
    }

    // 校验热图索引与直连后端四类查询结果完全一致
    let t_hot = svc_hot
        .query_table_lineage(
            "dws.user_gmv_daily",
            LineageDirection::Both,
            0,
            QueryScope::LatestBatch,
        )
        .await
        .unwrap();
    let t_raw = svc_raw
        .query_table_lineage(
            "dws.user_gmv_daily",
            LineageDirection::Both,
            0,
            QueryScope::LatestBatch,
        )
        .await
        .unwrap();
    assert_eq!(
        t_hot
            .upstream_tables
            .iter()
            .map(|x| &x.full_table_name)
            .collect::<Vec<_>>(),
        t_raw
            .upstream_tables
            .iter()
            .map(|x| &x.full_table_name)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        t_hot
            .downstream_tables
            .iter()
            .map(|x| &x.full_table_name)
            .collect::<Vec<_>>(),
        t_raw
            .downstream_tables
            .iter()
            .map(|x| &x.full_table_name)
            .collect::<Vec<_>>()
    );

    let c_hot = svc_hot
        .query_column_lineage(
            Some("dws"),
            Some("user_gmv_daily"),
            Some("gmv_amt"),
            None,
            LineageDirection::Both,
            0,
            QueryScope::LatestBatch,
        )
        .await
        .unwrap();
    let c_raw = svc_raw
        .query_column_lineage(
            Some("dws"),
            Some("user_gmv_daily"),
            Some("gmv_amt"),
            None,
            LineageDirection::Both,
            0,
            QueryScope::LatestBatch,
        )
        .await
        .unwrap();
    assert_eq!(
        c_hot
            .upstream_columns
            .iter()
            .map(|x| &x.qualified_column)
            .collect::<Vec<_>>(),
        c_raw
            .upstream_columns
            .iter()
            .map(|x| &x.qualified_column)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        c_hot
            .upstream_tables
            .iter()
            .map(|x| &x.full_table_name)
            .collect::<Vec<_>>(),
        c_raw
            .upstream_tables
            .iter()
            .map(|x| &x.full_table_name)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        c_hot
            .upstream_jobs
            .iter()
            .map(|x| &x.job_name)
            .collect::<Vec<_>>(),
        c_raw
            .upstream_jobs
            .iter()
            .map(|x| &x.job_name)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        c_hot
            .downstream_jobs
            .iter()
            .map(|x| &x.job_name)
            .collect::<Vec<_>>(),
        c_raw
            .downstream_jobs
            .iter()
            .map(|x| &x.job_name)
            .collect::<Vec<_>>()
    );
}
