use hive_sql_parse::model::LineageResult;
use hive_sql_parse::{analyze, InputFile, Options};

fn run(sql: &str) -> LineageResult {
    analyze(
        vec![InputFile {
            name: "q.sql".into(),
            bytes: sql.as_bytes().to_vec(),
        }],
        &Options::default(),
    )
}

fn run_opt(sql: &str, opt: Options) -> LineageResult {
    analyze(
        vec![InputFile {
            name: "q.sql".into(),
            bytes: sql.as_bytes().to_vec(),
        }],
        &opt,
    )
}

fn tid(r: &LineageResult, name: &str) -> usize {
    r.tables
        .iter()
        .find(|t| t.name == name)
        .unwrap_or_else(|| panic!("table {name} not found"))
        .id
}

fn has_tedge(r: &LineageResult, s: &str, t: &str) -> bool {
    let (si, ti) = (tid(r, s), tid(r, t));
    r.table_edges
        .iter()
        .any(|e| e.source == si && e.target == ti)
}

fn has_cedge(r: &LineageResult, st: &str, sc: &str, tt: &str, tc: &str) -> bool {
    has_kind(r, st, sc, tt, tc, "")
        || has_kind(r, st, sc, tt, tc, "direct")
        || has_kind(r, st, sc, tt, tc, "derived")
        || has_kind(r, st, sc, tt, tc, "aggregate")
}

fn has_kind(r: &LineageResult, st: &str, sc: &str, tt: &str, tc: &str, k: &str) -> bool {
    let (si, ti) = (tid(r, st), tid(r, tt));
    r.column_edges.iter().any(|e| {
        e.source_table == si
            && e.source_column == sc
            && e.target_table == ti
            && e.target_column == tc
            && (k.is_empty() || e.kind == k)
    })
}

fn kind_of(r: &LineageResult, name: &str) -> &'static str {
    r.tables
        .iter()
        .find(|t| t.name == name)
        .map(|t| t.kind)
        .unwrap_or("")
}

fn stmt_kinds(r: &LineageResult) -> Vec<&'static str> {
    r.statements.iter().map(|s| s.kind).collect()
}

// ---- do_insert：PARTITION 各种列形态 + after_columns ----

#[test]
fn t81_partition_compound_left() {
    let r = run("INSERT INTO TABLE dw.p PARTITION(ds.x='1') SELECT a FROM dw.s");
    assert!(has_tedge(&r, "dw.s", "dw.p"));
}

#[test]
fn t82_partition_dynamic_compound() {
    let r = run("INSERT INTO TABLE dw.p PARTITION(ds.x) SELECT a, x FROM dw.s");
    assert!(has_tedge(&r, "dw.s", "dw.p"));
    assert!(has_cedge(&r, "dw.s", "x", "dw.p", "x"));
}

#[test]
fn t83_partition_other_expr() {
    let r = run("INSERT INTO TABLE dw.p PARTITION(1+2='x') SELECT a FROM dw.s");
    assert!(has_tedge(&r, "dw.s", "dw.p"));
}

#[test]
fn t84_after_columns() {
    let r = run("INSERT INTO TABLE dw.p PARTITION(ds='1') (a, b) SELECT x, y FROM dw.s");
    assert!(has_tedge(&r, "dw.s", "dw.p"));
    assert!(has_cedge(&r, "dw.s", "x", "dw.p", "a"));
    assert!(has_cedge(&r, "dw.s", "y", "dw.p", "b"));
}

// ---- merge 谓词与 wildcard/values ----

#[test]
fn t85_merge_update_where() {
    let r = run("MERGE INTO dw.t USING dw.s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET a = s.a WHERE s.x > 0");
    assert!(has_tedge(&r, "dw.s", "dw.t"));
}

#[test]
fn t86_merge_update_delete_where() {
    let r = run("MERGE INTO dw.t USING dw.s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET a = s.a DELETE WHERE s.x < 0");
    assert!(has_tedge(&r, "dw.s", "dw.t"));
}

#[test]
fn t87_merge_insert_values_where() {
    let r = run("MERGE INTO dw.t USING dw.s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT VALUES (s.a, s.b) WHERE s.x > 1");
    assert!(has_tedge(&r, "dw.s", "dw.t"));
}

#[test]
fn t88_merge_insert_values_no_cols() {
    let r = run("MERGE INTO dw.t USING dw.s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT VALUES (s.a, s.b)");
    assert!(has_tedge(&r, "dw.s", "dw.t"));
}

#[test]
fn t89_merge_insert_values_expr_fallback() {
    let r = run("MERGE INTO dw.t USING dw.s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT VALUES (s.a + 1, 2)");
    assert!(has_tedge(&r, "dw.s", "dw.t"));
}

#[test]
fn t90_merge_set_wildcard() {
    let r = run("MERGE INTO dw.t USING dw.s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET *");
    assert!(has_tedge(&r, "dw.s", "dw.t"));
    let (si, ti) = (tid(&r, "dw.s"), tid(&r, "dw.t"));
    assert!(r
        .column_edges
        .iter()
        .any(|e| e.target_table == ti && e.source_table == si));
}

// ---- 自读自写 / 解析边界 ----

#[test]
fn t91_self_read_write() {
    let r = run("INSERT INTO TABLE dw.t SELECT * FROM dw.t CROSS JOIN dw.u");
    assert!(has_tedge(&r, "dw.t", "dw.t"));
    assert!(has_tedge(&r, "dw.u", "dw.t"));
}

#[test]
fn t92_derived_missing_col() {
    let r = run("INSERT INTO TABLE dw.o SELECT d.nope FROM (SELECT x FROM dw.s) d");
    assert!(has_tedge(&r, "dw.s", "dw.o"));
    assert!(!r.column_edges.iter().any(|e| e.source_column == "nope"));
}

#[test]
fn t93_star_unregistered() {
    let r = run("INSERT INTO TABLE dw.o SELECT s.* FROM dw.s s");
    assert!(has_cedge(&r, "dw.s", "*", "dw.o", "*"));
}

#[test]
fn t94_case_else() {
    let r = run("INSERT INTO TABLE dw.o SELECT CASE WHEN a > 1 THEN b ELSE c END FROM dw.s");
    assert!(has_kind(&r, "dw.s", "a", "dw.o", "_c0", "derived"));
    assert!(has_kind(&r, "dw.s", "c", "dw.o", "_c0", "derived"));
}

#[test]
fn t95_is_bool_variants() {
    let r = run(
        "INSERT INTO TABLE dw.o SELECT a IS TRUE, a IS FALSE, a IS NOT NULL, a IS NOT UNKNOWN \
         FROM dw.s",
    );
    assert!(has_kind(&r, "dw.s", "a", "dw.o", "_c0", "derived"));
    assert!(has_kind(&r, "dw.s", "a", "dw.o", "_c3", "derived"));
}

// ---- join 算子 ----

#[test]
fn t96_join_operator_kinds() {
    let r = run(
        "INSERT INTO TABLE dw.o SELECT * FROM dw.a INNER JOIN dw.b ON a.id = b.id; \
         INSERT INTO TABLE dw.o2 SELECT * FROM dw.a RIGHT JOIN dw.b ON a.id = b.id; \
         INSERT INTO TABLE dw.o3 SELECT * FROM dw.a RIGHT OUTER JOIN dw.b ON a.id = b.id; \
         INSERT INTO TABLE dw.o4 SELECT * FROM dw.a FULL OUTER JOIN dw.b ON a.id = b.id; \
         INSERT INTO TABLE dw.o5 SELECT a.id FROM dw.a LEFT SEMI JOIN dw.b ON a.id = b.id; \
         INSERT INTO TABLE dw.o6 SELECT * FROM dw.a LEFT OUTER JOIN dw.b ON a.id = b.id",
    );
    assert!(has_tedge(&r, "dw.a", "dw.o"));
    assert!(has_tedge(&r, "dw.b", "dw.o2"));
    assert!(has_tedge(&r, "dw.a", "dw.o3"));
    assert!(has_tedge(&r, "dw.a", "dw.o4"));
    assert!(has_tedge(&r, "dw.b", "dw.o5"));
    assert!(has_tedge(&r, "dw.b", "dw.o6"));
}

// ---- 其余表达式 / 结构 ----

#[test]
fn t97_named_fn_arg() {
    let r = run("INSERT INTO TABLE dw.o SELECT f(k => v) FROM dw.s");
    assert!(has_tedge(&r, "dw.s", "dw.o"));
}

#[test]
fn t98_group_by_cube_rollup() {
    let r = run(
        "INSERT INTO TABLE dw.o SELECT x, count(a) FROM dw.s GROUP BY CUBE (x, y); \
         INSERT INTO TABLE dw.o2 SELECT x, count(a) FROM dw.s GROUP BY ROLLUP (x, y)",
    );
    assert!(has_cedge(&r, "dw.s", "x", "dw.o", "x"));
    assert!(has_cedge(&r, "dw.s", "x", "dw.o2", "x"));
}

#[test]
fn t99_trim_chars() {
    let r = run("INSERT INTO TABLE dw.o SELECT TRIM('xy' FROM a), TRIM(BOTH 'x' FROM b) FROM dw.s");
    assert!(has_kind(&r, "dw.s", "a", "dw.o", "_c0", "derived"));
}

#[test]
fn t100_derived_alias_cols() {
    let r = run("INSERT INTO TABLE dw.o SELECT d.p, d.q FROM (SELECT a, b FROM dw.s) d(p, q)");
    assert!(has_cedge(&r, "dw.s", "a", "dw.o", "p"));
    assert!(has_cedge(&r, "dw.s", "b", "dw.o", "q"));
}

#[test]
fn t101_lateral_default_col_names() {
    let r = run(
        "INSERT INTO TABLE dw.o SELECT e.col FROM dw.s LATERAL VIEW explode(arr) e; \
         INSERT INTO TABLE dw.o2 SELECT p.pos, p.val FROM dw.s LATERAL VIEW posexplode(arr) p",
    );
    assert!(has_tedge(&r, "dw.s", "dw.o"));
    assert!(has_kind(&r, "dw.s", "arr", "dw.o", "col", "derived"));
    assert!(has_kind(&r, "dw.s", "arr", "dw.o2", "pos", "derived"));
    assert!(has_kind(&r, "dw.s", "arr", "dw.o2", "val", "derived"));
}

#[test]
fn t102_insert_values_setexpr() {
    let r = run("INSERT INTO TABLE dw.t VALUES (1, 'a'), (2, 'b')");
    assert_eq!(kind_of(&r, "dw.t"), "table");
    assert!(stmt_kinds(&r).contains(&"insert"));
}

#[test]
fn t103_order_by_limit() {
    let r = run("INSERT INTO TABLE dw.o SELECT a FROM dw.s ORDER BY a LIMIT 3");
    assert!(has_tedge(&r, "dw.s", "dw.o"));
}

#[test]
fn t104_update_tuple_assign() {
    let r = run("UPDATE dw.t SET (a, b) = (x, y) WHERE c = 1");
    assert_eq!(kind_of(&r, "dw.t"), "table");
}

// ---- fallback 路径：截断/畸形 DML ----

#[test]
fn t105_fallback_merge_var_target() {
    let r = run("MERGE INTO ${db}.${tbl} USING dw.s ON t.id = s.id THEN BROKEN");
    assert!(r
        .statements
        .iter()
        .any(|s| s.status == "fallback" || s.status == "ok"));
}

#[test]
fn t106_fallback_merge_number_target() {
    let r = run("MERGE INTO 123 USING dw.s ON a = b THEN BROKEN");
    assert!(r.statements.iter().any(|s| !s.status.is_empty()));
}

#[test]
fn t107_fallback_update_var() {
    let r = run("UPDATE ${t} SET a = 1 BROKEN");
    assert!(r.statements.iter().any(|s| !s.status.is_empty()));
}

#[test]
fn t108_fallback_from_leading_comma() {
    let r = run("SELECT * FROM , dw.t WHERE BROKEN(");
    assert!(r.statements.iter().any(|s| !s.status.is_empty()));
}

#[test]
fn t109_fallback_create_like_number() {
    let r = run("CREATE TABLE dw.x (a INT) LIKE 123 BROKEN TAIL");
    assert!(r.statements.iter().any(|s| !s.status.is_empty()));
}

#[test]
fn t110_fallback_like_unregistered() {
    let r = run("CREATE TABLE dw.x LIKE dw.never_seen BROKEN TAIL");
    assert_eq!(kind_of(&r, "dw.x"), "table");
}

// ---- preprocess：WITH 形态 / CREATE 修饰符 / FROM 多插 ----

#[test]
fn t111_with_insert() {
    let r = run("WITH c AS (SELECT a FROM dw.s) INSERT INTO TABLE dw.t SELECT * FROM c");
    assert!(has_tedge(&r, "dw.s", "dw.t"));
}

#[test]
fn t112_with_select_query() {
    let r = run_opt(
        "WITH c AS (SELECT a FROM dw.s) SELECT * FROM c",
        Options {
            analyze_select: Some(true),
            ..Default::default()
        },
    );
    assert!(has_tedge(&r, "dw.s", "query_result"));
}

#[test]
fn t113_create_bare_modifiers() {
    let r = run("CREATE OR REPLACE");
    assert!(r
        .statements
        .iter()
        .all(|s| s.status != "ok" || !s.kind.is_empty()));
}

#[test]
fn t114_from_without_insert() {
    let r = run("FROM dw.t SELECT a WHERE BROKEN(");
    assert!(r.statements.iter().any(|s| !s.status.is_empty()));
}

#[test]
fn t115_multi_insert_no_select_and_with_from() {
    let r = run("FROM dw.s INSERT INTO TABLE dw.t INSERT INTO TABLE dw.t2 SELECT a FROM dw.u");
    assert!(r.statements.iter().any(|s| s.kind == "insert"));
}

#[test]
fn t116_with_from_multi() {
    let r = run(
        "WITH c AS (SELECT a FROM dw.s) FROM dw.x INSERT INTO TABLE dw.t INSERT INTO TABLE dw.t2 SELECT * FROM c",
    );
    assert!(r.statements.iter().any(|s| s.kind == "insert"));
}

// ---- 第二轮补充 ----

#[test]
fn t117_partition_bare_dynamic() {
    let r = run("INSERT INTO TABLE dw.p PARTITION(ds) SELECT a, ds FROM dw.s");
    assert!(has_tedge(&r, "dw.s", "dw.p"));
}

#[test]
fn t118_merge_then_delete() {
    let r = run("MERGE INTO dw.t USING dw.s ON t.id = s.id \
         WHEN MATCHED AND s.x > 0 THEN DELETE");
    assert!(has_tedge(&r, "dw.s", "dw.t"));
}

#[test]
fn t119_update_col_srcs() {
    // UPDATE 赋值右值含来源列 -> apply_assignments 循环体非空
    let r = run("UPDATE dw.t SET a = s.x FROM dw.s;\n\
         UPDATE dw.t SET (a, b) = (SELECT x, y FROM dw.s)");
    let (si, ti) = (tid(&r, "dw.s"), tid(&r, "dw.t"));
    assert!(r
        .column_edges
        .iter()
        .any(|e| e.source_table == si && e.target_table == ti));
}

#[test]
fn t120_derived_fewer_aliases() {
    // 别名列不足 -> i >= len 的 else 路径
    let r = run("INSERT INTO TABLE dw.o SELECT d.p, d.b FROM (SELECT a, b FROM dw.s) d(p)");
    assert!(has_cedge(&r, "dw.s", "a", "dw.o", "p"));
}

#[test]
fn t121_fallback_comma_chain() {
    // `FROM , , t`：collect_name None -> j+=1 -> 逗号跳过分支
    let r = run("SELECT * FROM , , dw.t WHERE BROKEN(");
    assert!(r.statements.iter().any(|s| !s.status.is_empty()));
}

#[test]
fn t122_fallback_name_terminated_by_sym() {
    // `db.*`：collect_name 在非名 token 处返回 Some（`_` 臂非空返回）
    let r = run("SELECT * FROM db.* BROKEN( SELECT * FROM db., t WHERE x=(");
    assert!(r.statements.iter().any(|s| !s.status.is_empty()));
}

#[test]
fn t123_fallback_create_typed_cols() {
    // 列定义中的数字/符号 token -> fallback 列扫描 `_` 臂
    let r = run("CREATE TABLE dw.x (a DECIMAL(10,2), b INT) BROKEN TAIL");
    assert!(r.statements.iter().any(|s| !s.status.is_empty()));
}

#[test]
fn t124_fallback_like_trailing() {
    // LIKE 在末尾 -> while 条件终止 / collect_name None -> break
    let r = run("CREATE TABLE dw.x (a INT) LIKE");
    assert!(r.statements.iter().any(|s| !s.status.is_empty()));
}

#[test]
fn t125_fallback_like_source_only() {
    // copy_from 源表已 intern 但无列目录 -> `return;` 臂
    let r = run("INSERT INTO TABLE dw.t SELECT a FROM dw.srconly;\n\
         CREATE TABLE dw.y LIKE dw.srconly BROKEN TAIL");
    assert_eq!(kind_of(&r, "dw.y"), "table");
}

#[test]
fn t126_with_from_no_insert() {
    // WITH ... FROM 但无顶层 INSERT -> rewrite 为空 -> Prep::Parse{insert,[sub]}
    let r = run("WITH c AS (SELECT 1 FROM dw.d) FROM dw.t SELECT a WHERE BROKEN(");
    assert!(r.statements.iter().any(|s| !s.status.is_empty()));
}

#[test]
fn t127_is_variants_full() {
    let r = run(
        "INSERT INTO TABLE dw.o SELECT a IS TRUE, a IS NOT TRUE, a IS FALSE, a IS NOT FALSE, \
         a IS NULL, a IS NOT NULL, a IS UNKNOWN, a IS NOT UNKNOWN FROM dw.s",
    );
    for i in 0..8 {
        assert!(
            has_kind(&r, "dw.s", "a", "dw.o", &format!("_c{i}"), "derived"),
            "_c{i}"
        );
    }
}

#[test]
fn t128_insert_order_by() {
    let r = run("INSERT INTO TABLE dw.o SELECT a FROM dw.s ORDER BY a");
    assert!(has_tedge(&r, "dw.s", "dw.o"));
}

#[test]
fn t129_fallback_merge_no_into() {
    let r = run("MERGE dw.t USING dw.s ON x = y THEN BROKEN");
    assert!(r.statements.iter().any(|s| !s.status.is_empty()));
}

#[test]
fn t130_fallback_update_truncated() {
    let r = run("UPDATE dw.t SET a = BROKEN(");
    assert!(r.statements.iter().any(|s| !s.status.is_empty()));
}

#[test]
fn t131_fallback_create_bare() {
    let r = run("CREATE TABLE dw.x BROKEN TAIL(");
    assert!(r.statements.iter().any(|s| !s.status.is_empty()));
}

#[test]
fn t132_fallback_var_in_name() {
    // 未闭合 ${ -> Var token 进入 fallback collect_name Var 臂
    let r = run("MERGE INTO ${db. USING dw.s ON x = y THEN BROKEN");
    assert!(r.statements.iter().any(|s| !s.status.is_empty()));
}

#[test]
fn t133_multiline_string_line_numbers() {
    let sql = "INSERT INTO dw.t1 SELECT 'line1\nline2\nline3' AS s FROM dw.s;\nINSERT INTO dw.t2 SELECT a FROM dw.s;";
    let r = run(sql);
    assert_eq!(r.statements.len(), 2);
    assert_eq!(r.statements[0].line, 1);
    assert_eq!(r.statements[1].line, 4);
}

#[test]
fn t134_set_with_trailing_comment() {
    let sql =
        "SET hivevar:tgt = 'dw.out_tbl' -- 行尾注释\n;\nINSERT INTO ${tgt} SELECT a FROM dw.src;";
    let r = run(sql);
    assert_eq!(r.stats.failed, 0);
    assert!(has_tedge(&r, "dw.src", "dw.out_tbl"));
    assert!(has_cedge(&r, "dw.src", "a", "dw.out_tbl", "a"));
}

#[test]
fn t135_parenthesized_ctas_preserves_column_lineage() {
    let sql = "CREATE TABLE dw.ctas_p AS (SELECT id, amt + 1 AS amt2 FROM ods.orders);";
    let r = run(sql);
    assert_eq!(r.statements[0].kind, "ctas");
    assert_eq!(r.statements[0].status, "ok");
    assert!(has_kind(
        &r,
        "ods.orders",
        "id",
        "dw.ctas_p",
        "id",
        "direct"
    ));
    assert!(has_kind(
        &r,
        "ods.orders",
        "amt",
        "dw.ctas_p",
        "amt2",
        "derived"
    ));
}

#[test]
fn t136_non_recursive_same_name_cte_does_not_shadow_table() {
    let sql = "WITH orders AS (SELECT id, amt FROM orders) INSERT INTO dw.out SELECT id, amt FROM orders;";
    let r = run_opt(
        sql,
        Options {
            default_db: Some("ods".into()),
            ..Options::default()
        },
    );
    assert!(has_tedge(&r, "ods.orders", "dw.out"));
    assert!(has_cedge(&r, "ods.orders", "id", "dw.out", "id"));
    assert!(has_cedge(&r, "ods.orders", "amt", "dw.out", "amt"));
}

#[test]
fn t137_union_star_with_explicit_columns() {
    let sql = "INSERT INTO dw.u(a, b) SELECT * FROM ods.unknown_tbl UNION ALL SELECT x, y FROM ods.known_tbl;";
    let r = run(sql);
    assert!(has_cedge(&r, "ods.known_tbl", "x", "dw.u", "a"));
    assert!(has_cedge(&r, "ods.known_tbl", "y", "dw.u", "b"));
    assert!(has_cedge(&r, "ods.unknown_tbl", "*", "dw.u", "a"));
    assert!(has_cedge(&r, "ods.unknown_tbl", "*", "dw.u", "b"));
}
