use hive_sql_parse::model::LineageResult;
use hive_sql_parse::{analyze, InputFile, Options, SqlDialect};

const SPARK_CTAS: &str = "CREATE TABLE dw.t USING PARQUET AS SELECT id FROM ods.s";

fn input(sql: &str) -> InputFile {
    InputFile {
        name: "test.sql".into(),
        bytes: sql.as_bytes().to_vec(),
    }
}

fn run(sql: &str, dialect: SqlDialect) -> LineageResult {
    analyze(
        vec![input(sql)],
        &Options {
            dialect,
            ..Options::default()
        },
    )
}

fn assert_edge(
    r: &LineageResult,
    source: &str,
    source_col: &str,
    target: &str,
    target_col: &str,
    kind: &str,
) {
    let s = r.tables.iter().find(|t| t.name == source).unwrap().id;
    let t = r.tables.iter().find(|t| t.name == target).unwrap().id;
    assert!(r.table_edges.iter().any(|e| e.source == s && e.target == t));
    assert!(r.column_edges.iter().any(|e| e.source_table == s
        && e.source_column == source_col
        && e.target_table == t
        && e.target_column == target_col
        && e.kind == kind));
}

#[test]
fn dialect_values_and_default() {
    assert_eq!(Options::default().dialect, SqlDialect::Hive);
    for (value, expected) in [
        ("hive", SqlDialect::Hive),
        ("spark", SqlDialect::Spark),
        (" SPARK ", SqlDialect::Spark),
    ] {
        assert_eq!(value.parse::<SqlDialect>().unwrap(), expected);
    }
    for value in ["", "mysql", "sparkk"] {
        assert!(value.parse::<SqlDialect>().is_err());
    }
}

#[test]
fn default_matches_explicit_hive() {
    let sql = "INSERT OVERWRITE TABLE dw.t SELECT id FROM ods.s";
    let a = analyze(vec![input(sql)], &Options::default());
    let b = run(sql, SqlDialect::Hive);
    let mut a = serde_json::to_value(a).unwrap();
    let mut b = serde_json::to_value(b).unwrap();
    a["stats"].as_object_mut().unwrap().remove("elapsed_ms");
    b["stats"].as_object_mut().unwrap().remove("elapsed_ms");
    assert_eq!(a, b);
}

#[test]
fn spark_ctas_uses_selected_dialect() {
    let r = run(SPARK_CTAS, SqlDialect::Spark);
    assert_eq!(r.stats.parsed, 1, "{:?}", r.statements);
    assert_eq!(r.stats.fallback, 0);
    assert_eq!(r.statements[0].status, "ok");
    assert!(r.statements[0].error.is_none());
    assert_edge(&r, "ods.s", "id", "dw.t", "id", "direct");
    let r = run(SPARK_CTAS, SqlDialect::Hive);
    assert_eq!(r.stats.parsed, 0);
    assert_eq!(r.stats.fallback, 1);
    assert!(r.statements[0].error.is_some());
    let r = run(SPARK_CTAS, SqlDialect::Spark);
    assert_eq!(r.stats.parsed, 1);
}

#[test]
fn spark_standalone_div_expression() {
    let r = run("SELECT id DIV 2 AS half FROM ods.s", SqlDialect::Spark);
    assert_eq!(r.stats.parsed, 1, "{:?}", r.statements);
    assert_eq!(r.stats.fallback, 0);
    assert_edge(&r, "ods.s", "id", "query_result", "half", "derived");
}

#[test]
fn spark_multifile_variables_and_use() {
    let r = analyze(
        vec![
            input("SET target=dw.t; USE ods;"),
            input(
                "CREATE TABLE ${target} USING PARQUET AS SELECT id FROM s; INSERT INTO dw.u SELECT id FROM dw.t;",
            ),
        ],
        &Options {
            dialect: SqlDialect::Spark,
            ..Options::default()
        },
    );
    assert_eq!(r.stats.parsed, 2, "{:?}", r.statements);
    assert_eq!(r.stats.fallback, 0);
    assert_eq!(r.stats.skipped, 2);
    assert_edge(&r, "ods.s", "id", "dw.t", "id", "direct");
    assert_edge(&r, "dw.t", "id", "dw.u", "id", "direct");
}

#[test]
fn spark_pipeline_across_chunks() {
    let sql = (0..257)
        .map(|i| format!("CREATE TABLE dw.t{i} USING PARQUET AS SELECT id FROM ods.s;"))
        .collect::<String>();
    let r = run(&sql, SqlDialect::Spark);
    assert_eq!(r.stats.parsed, 257);
    assert_eq!(r.stats.fallback, 0);
    assert_eq!(r.column_edges.len(), 257);
    assert_edge(&r, "ods.s", "id", "dw.t256", "id", "direct");
}
