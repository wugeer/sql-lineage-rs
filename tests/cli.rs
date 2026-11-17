//! CLI 子进程测试：覆盖 main.rs / bench_parse.rs。
//! 插桩覆盖率运行时，子进程继承 LLVM_PROFILE_FILE(%p) 各自落盘，llvm-cov 可合并。

use std::io::Write;
use std::process::Command;

fn write_tmp(path: &str, content: &str) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(content.as_bytes()).unwrap();
}

#[test]
fn cli_analyze() {
    let sql_path = "/tmp/hl_cli_t.sql";
    write_tmp(
        sql_path,
        "INSERT INTO dw.t SELECT a FROM dw.s;\nCREATE TABLE dw.x (b INT)",
    );
    let bin = env!("CARGO_BIN_EXE_hive-lineage");

    // 输出到 stdout
    let out = Command::new(bin)
        .args(["analyze", sql_path])
        .output()
        .unwrap();
    assert!(out.status.success(), "stderr={:?}", out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("\"tables\""), "stdout={stdout}");

    // -o 文件 + --pretty + --default-db + --analyze-select
    let out_path = "/tmp/hl_cli_out.json";
    let out = Command::new(bin)
        .args([
            "analyze",
            sql_path,
            "-o",
            out_path,
            "--pretty",
            "--default-db",
            "od",
            "--analyze-select",
            "yes",
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "stderr={:?}", out.stderr);
    let body = std::fs::read_to_string(out_path).unwrap();
    assert!(body.contains("\"tables\""));
    assert!(body.contains("dw.t"));

    // analyze-select 其它取值
    for mode in ["no", "auto", "bogus"] {
        let out = Command::new(bin)
            .args(["analyze", sql_path, "--analyze-select", mode])
            .output()
            .unwrap();
        assert!(out.status.success());
    }

    // 文件不存在 -> 报错退出
    let out = Command::new(bin)
        .args(["analyze", "/tmp/hl_no_such_file.sql"])
        .output()
        .unwrap();
    assert!(!out.status.success());

    // 无参数 / --help
    let out = Command::new(bin).output().unwrap();
    assert!(!out.status.success());
    let out = Command::new(bin).arg("--help").output().unwrap();
    assert!(out.status.success());
    let out = Command::new(bin)
        .args(["analyze", "--help"])
        .output()
        .unwrap();
    assert!(out.status.success());
}

#[test]
fn cli_serve_bind_fail() {
    let bin = env!("CARGO_BIN_EXE_hive-lineage");
    // 先占用端口，serve 绑定失败 -> 覆盖 serve 臂 + run() 错误路径
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    // serve 带预加载文件（绑定失败也会先触发 preload 路径）
    let sql_path = "/tmp/hl_cli_serve.sql";
    write_tmp(sql_path, "INSERT INTO dw.t SELECT a FROM dw.s");
    let out = Command::new(bin)
        .args([
            "serve",
            "-p",
            &port.to_string(),
            "--host",
            "127.0.0.1",
            sql_path,
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    drop(listener);

    // --help 覆盖 clap 展示
    let out = Command::new(bin)
        .args(["serve", "--help"])
        .output()
        .unwrap();
    assert!(out.status.success());
}

#[test]
fn cli_ddl_and_query() {
    let bin = env!("CARGO_BIN_EXE_hive-lineage");

    // ddl 子命令输出完整建表脚本
    let out = Command::new(bin).args(["ddl"]).output().unwrap();
    assert!(out.status.success());
    let ddl = String::from_utf8_lossy(&out.stdout);
    assert!(ddl.contains("hl_job_batches") && ddl.contains("hl_column_edges"));

    // query --help 覆盖 clap 展示
    let out = Command::new(bin)
        .args(["query", "--help"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let out = Command::new(bin)
        .args(["query", "table", "--help"])
        .output()
        .unwrap();
    assert!(out.status.success());

    // 无 db_url 且无环境变量 -> 明确报错退出
    let out = Command::new(bin)
        .args(["query", "table", "dw.t"])
        .env_remove("HL_DB_URL")
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("HL_DB_URL") || err.contains("db-url"));

    // memory:// 后端：四类查询子命令均可正常执行（空库返回空结果而非崩溃）
    for args in [
        vec![
            "query",
            "table",
            "dw.t",
            "--db-url",
            "memory://",
            "--direction",
            "upstream",
            "--hops",
            "0",
        ],
        vec![
            "query",
            "table-tasks",
            "dw.t",
            "--db-url",
            "memory://",
            "--all-history",
        ],
        vec![
            "query",
            "task",
            "etl.x",
            "--db-url",
            "memory://",
            "--direction",
            "downstream",
        ],
        vec!["query", "column", "dw.t.col", "--db-url", "memory://"],
        vec![
            "query",
            "column",
            "--db",
            "dw",
            "--table",
            "t",
            "--column",
            "col",
            "--db-url",
            "memory://",
            "--pretty",
        ],
    ] {
        let out = Command::new(bin).args(&args).output().unwrap();
        assert!(
            out.status.success(),
            "args={args:?} stderr={:?}",
            out.stderr
        );
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.contains('{'), "args={args:?} stdout={stdout}");
    }

    // 通过 HL_DB_URL 环境变量提供连接串
    let out = Command::new(bin)
        .args(["query", "table", "dw.t"])
        .env("HL_DB_URL", "memory://")
        .output()
        .unwrap();
    assert!(out.status.success(), "stderr={:?}", out.stderr);

    // query column 缺字段参数 -> 报错退出
    let out = Command::new(bin)
        .args(["query", "column", "--db-url", "memory://"])
        .output()
        .unwrap();
    assert!(!out.status.success());
}

#[test]
fn cli_analyze_job_name_and_save() {
    let sql_path = "/tmp/hl_cli_job.sql";
    write_tmp(sql_path, "INSERT INTO dw.t SELECT a FROM dw.s;");
    let bin = env!("CARGO_BIN_EXE_hive-lineage");

    // --job-name 写入输出 JSON
    let out = Command::new(bin)
        .args(["analyze", sql_path, "--job-name", "my_dag.my_task"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("\"job_name\":\"my_dag.my_task\""),
        "stdout={stdout}"
    );

    // --db-url 但无 --job-name -> 明确报错
    let out = Command::new(bin)
        .args(["analyze", sql_path, "--db-url", "memory://"])
        .env_remove("HL_DB_URL")
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("job-name") || err.contains("job_name"));

    // --db-url + --job-name -> 追加落库成功（memory:// 后端）
    let out = Command::new(bin)
        .args([
            "analyze",
            sql_path,
            "--job-name",
            "etl.cli_task",
            "--db-url",
            "memory://",
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "stderr={:?}", out.stderr);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("saved") && err.contains("etl.cli_task"),
        "stderr={err}"
    );
}

#[test]
fn cli_bench_parse() {
    let bin = env!("CARGO_BIN_EXE_bench_parse");
    // bench_parse 硬编码读取 /tmp/hl/simple.sql
    std::fs::create_dir_all("/tmp/hl").unwrap();
    write_tmp(
        "/tmp/hl/simple.sql",
        "INSERT INTO t SELECT a FROM s;\nSELECT 1",
    );
    let out = Command::new(bin).output().unwrap();
    assert!(out.status.success(), "stderr={:?}", out.stderr);
}
