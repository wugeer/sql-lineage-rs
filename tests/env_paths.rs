//! HL_* 环境变量路径覆盖。整个文件只有单个测试函数：
//! env 是进程级共享状态，拆成多个 #[test] 会在线程间互相干扰。

use hive_sql_parse::{analyze, InputFile, Options, SqlDialect};

fn input(sql: &str) -> Vec<InputFile> {
    vec![InputFile {
        name: "t.sql".into(),
        bytes: sql.as_bytes().to_vec(),
    }]
}

#[test]
fn env_paths() {
    let sql = "INSERT INTO dw.t SELECT a FROM dw.s";

    // HL_PROFILE：打印各阶段耗时（不影响结果）
    std::env::set_var("HL_PROFILE", "1");
    let r = analyze(input(sql), &Options::default());
    assert_eq!(r.stats.parsed, 1);

    // HL_THREADS：自建解析线程池
    std::env::set_var("HL_THREADS", "2");
    let r = analyze(input(sql), &Options::default());
    assert_eq!(r.stats.parsed, 1);

    // 非法值回退全局池
    std::env::set_var("HL_THREADS", "0");
    let r = analyze(input(sql), &Options::default());
    assert_eq!(r.stats.parsed, 1);
    std::env::set_var("HL_THREADS", "abc");
    let r = analyze(input(sql), &Options::default());
    assert_eq!(r.stats.parsed, 1);

    // HL_PARSE_ONLY：只解析不分析
    std::env::set_var("HL_PARSE_ONLY", "1");
    let r = analyze(input(sql), &Options::default());
    assert!(r.tables.is_empty());
    let r = analyze(
        input("CREATE TABLE dw.t USING PARQUET AS SELECT id FROM ods.s"),
        &Options {
            dialect: SqlDialect::Spark,
            ..Options::default()
        },
    );
    assert!(r.tables.is_empty());

    std::env::remove_var("HL_PARSE_ONLY");
    std::env::remove_var("HL_THREADS");
    std::env::remove_var("HL_PROFILE");

    // 清理后正常分析
    let r = analyze(input(sql), &Options::default());
    assert_eq!(r.stats.parsed, 1);
    assert!(!r.tables.is_empty());
}
