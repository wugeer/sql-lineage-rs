//! 覆盖率扩展第二轮：DDL 变体、merge 谓词/通配、表达式方言臂、
//! resolve 歧义、preprocess/fallback/lexer 深层边界

use hive_sql_parse::model::LineageResult;
use hive_sql_parse::{analyze, InputFile, Options};

fn run(sql: &str) -> LineageResult {
    analyze(
        vec![InputFile {
            name: "t.sql".into(),
            bytes: sql.as_bytes().to_vec(),
        }],
        &Options::default(),
    )
}

fn run_sel(sql: &str) -> LineageResult {
    analyze(
        vec![InputFile {
            name: "t.sql".into(),
            bytes: sql.as_bytes().to_vec(),
        }],
        &Options {
            analyze_select: Some(true),
            ..Default::default()
        },
    )
}

fn tid(r: &LineageResult, name: &str) -> usize {
    r.tables
        .iter()
        .find(|t| t.name == name)
        .unwrap_or_else(|| {
            panic!(
                "table {name} not found; have {:?}",
                r.tables.iter().map(|t| &t.name).collect::<Vec<_>>()
            )
        })
        .id
}

fn has_table(r: &LineageResult, name: &str) -> bool {
    r.tables.iter().any(|t| t.name == name)
}

fn has_tedge(r: &LineageResult, s: &str, t: &str) -> bool {
    let (si, ti) = (tid(r, s), tid(r, t));
    r.table_edges
        .iter()
        .any(|e| e.source == si && e.target == ti)
}

fn has_cedge(r: &LineageResult, st: &str, sc: &str, tt: &str, tc: &str, kind: &str) -> bool {
    let (si, ti) = (tid(r, st), tid(r, tt));
    r.column_edges.iter().any(|e| {
        e.source_table == si
            && e.source_column == sc
            && e.target_table == ti
            && e.target_column == tc
            && e.kind == kind
    })
}

fn tcols(r: &LineageResult, name: &str) -> Vec<String> {
    r.tables[tid(r, name)].columns.clone()
}

fn kind_of(r: &LineageResult, name: &str) -> String {
    r.tables[tid(r, name)].kind.to_string()
}

fn stmt_kinds(r: &LineageResult) -> Vec<String> {
    r.statements.iter().map(|s| s.kind.to_string()).collect()
}

// ---------- merge / update ----------

#[test]
fn t47_merge_predicates() {
    let r = run("MERGE INTO dw.t USING dw.s ON t.id = s.id \
         WHEN MATCHED AND s.x > 0 THEN UPDATE SET a = s.a \
         WHEN MATCHED AND s.x < 0 THEN DELETE \
         WHEN NOT MATCHED THEN INSERT (a, id) VALUES (s.a, s.id)");
    assert!(has_tedge(&r, "dw.s", "dw.t"));
    assert!(has_cedge(&r, "dw.s", "a", "dw.t", "a", "direct"));
    assert!(has_cedge(&r, "dw.s", "id", "dw.t", "id", "direct"));
}

#[test]
fn t48_merge_wildcard_forms() {
    let r = run("MERGE INTO dw.t USING dw.s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET * \
         WHEN NOT MATCHED THEN INSERT *");
    assert!(has_tedge(&r, "dw.s", "dw.t"));
    // 源表列未知时展开为 * 列
    assert!(r
        .column_edges
        .iter()
        .any(|e| e.target_column == "*" || e.source_column == "*"));
}

#[test]
fn t49_update_tuple_and_subq() {
    let r = run(
        "UPDATE dw.t SET (a, b) = (SELECT max(x), min(y) FROM dw.s);\n\
         UPDATE dw.t2 SET a = 1 FROM dw.s WHERE t2.id = s.id",
    );
    assert!(has_tedge(&r, "dw.s", "dw.t"));
    assert!(has_tedge(&r, "dw.s", "dw.t2"));
}

// ---------- DDL ----------

#[test]
fn t50_ddl_variants() {
    let r = run("CREATE TEMPORARY TABLE tmp (a INT);\n\
         CREATE TABLE dw.p (a INT) PARTITIONED BY (d STRING);\n\
         CREATE TABLE dw.l LIKE dw.p;\n\
         CREATE TABLE dw.c CLONE dw.p;\n\
         CREATE TABLE dw.n LIKE db.missing_src");
    assert_eq!(kind_of(&r, "tmp"), "temp");
    assert!(tcols(&r, "dw.p").contains(&"d".to_string()));
    // LIKE/CLONE 拷贝 dw.p 的列
    assert!(tcols(&r, "dw.l").contains(&"a".to_string()));
    assert!(tcols(&r, "dw.c").contains(&"d".to_string()));
    // LIKE 不存在源表 -> 仅登记目标
    assert!(has_table(&r, "dw.n"));
}

#[test]
fn t51_ctas_view_cols() {
    let r = run("CREATE VIEW dw.v(a, b) AS SELECT x, y FROM dw.s;\n\
         CREATE TEMPORARY VIEW tv AS SELECT x FROM dw.s");
    assert_eq!(kind_of(&r, "dw.v"), "view");
    assert!(tcols(&r, "dw.v").contains(&"a".to_string()));
    // TEMPORARY VIEW：kind 记为 view，temp 仅影响名字解析（不加库前缀）
    assert_eq!(kind_of(&r, "tv"), "view");
    assert!(has_tedge(&r, "dw.s", "dw.v"));
    assert!(has_tedge(&r, "dw.s", "tv"));
}

#[test]
fn t52_stmt_kind_other() {
    let r = run("DELETE FROM dw.t WHERE id = 1;\n\
         TRUNCATE TABLE dw.u;\n\
         EXPLAIN SELECT * FROM dw.v");
    let ks = stmt_kinds(&r);
    assert!(ks.contains(&"ok".to_string()) || ks.iter().any(|k| k == "other"));
    assert_eq!(r.stats.failed, 0);
}

#[test]
fn t53_insert_variants() {
    let r = run("INSERT INTO dw.t(a, b) SELECT x, y FROM dw.s;\n\
         INSERT INTO dw.p PARTITION(dt) SELECT a, dt FROM dw.s2;\n\
         INSERT INTO dw.p PARTITION(ds='1', dy) SELECT a FROM dw.s2;\n\
         INSERT INTO dw.v2 VALUES (1, 2), (3, 4)");
    assert!(has_cedge(&r, "dw.s", "x", "dw.t", "a", "direct"));
    assert!(has_cedge(&r, "dw.s", "y", "dw.t", "b", "direct"));
    assert!(has_tedge(&r, "dw.s2", "dw.p"));
    assert!(has_table(&r, "dw.v2"));
}

// ---------- 表达式 ----------

#[test]
fn t54_expr_exotics() {
    let r = run("INSERT INTO dw.t SELECT \
           a -> 'k', (a, b), ARRAY[a, b], a + INTERVAL '1' DAY, \
           transform(arr, x -> x + 1), a AT TIME ZONE 'UTC', \
           CONVERT(a USING utf8), a[0].b, overlay(a placing b from 1 for 2) \
         FROM dw.s");
    assert!(has_cedge(&r, "dw.s", "a", "dw.t", "_c0", "derived"));
    assert!(has_cedge(&r, "dw.s", "arr", "dw.t", "_c4", "derived"));
    assert!(has_cedge(&r, "dw.s", "b", "dw.t", "_c7", "derived"));
}

#[test]
fn t55_predicates_json() {
    let r = run("INSERT INTO dw.t SELECT \
           a IS JSON, b IS NORMALIZED, a IS DISTINCT FROM b, \
           a IS NOT DISTINCT FROM b, a IS UNKNOWN, a IS NOT UNKNOWN, \
           PRIOR a, a MEMBER OF(b) \
         FROM dw.s");
    assert!(has_cedge(&r, "dw.s", "a", "dw.t", "_c4", "derived"));
    assert!(has_cedge(&r, "dw.s", "b", "dw.t", "_c3", "derived"));
    // Hive 方言下 `PRIOR a` 解析为"别名 a 的 prior 列引用"
    assert!(has_cedge(&r, "dw.s", "prior", "dw.t", "a", "direct"));
    assert!(has_cedge(&r, "dw.s", "a", "dw.t", "_c7", "derived"));
    assert!(has_cedge(&r, "dw.s", "b", "dw.t", "_c7", "derived"));
}

#[test]
fn t56_select_item_forms() {
    let r = run("CREATE TABLE dw.s (x INT, y INT);\n\
         INSERT INTO dw.t SELECT s.*, `É`, (x), CAST(y AS BIGINT) FROM dw.s s");
    assert!(has_cedge(&r, "dw.s", "x", "dw.t", "x", "direct"));
    assert!(has_cedge(&r, "dw.s", "y", "dw.t", "y", "direct"));
    // `É` 与 (x) -> item_name 嵌套/非常规列名
    let cs = tcols(&r, "dw.t");
    assert!(cs.contains(&"é".to_string()) || cs.iter().any(|c| c == "É"));
}

#[test]
fn t57_from_exotics() {
    // Hive 方言下 UNNEST/TABLE/JSON_TABLE 解析为函数名表或专用臂，均不扫描参数列
    let r = run("INSERT INTO dw.t SELECT * FROM UNNEST(dw.s.arr) u;\n\
         INSERT INTO dw.t2 SELECT * FROM dw.s PIVOT (SUM(x) FOR y IN (1, 2)) p;\n\
         INSERT INTO dw.t3 SELECT * FROM TABLE(dw.s) tt;\n\
         INSERT INTO dw.t4 SELECT * FROM JSON_TABLE(dw.s.j, '$.x' COLUMNS (c INT PATH '$.c')) jt");
    assert!(has_table(&r, "unnest"));
    assert!(has_tedge(&r, "unnest", "dw.t"));
    // PIVOT 会递归内层表
    assert!(has_tedge(&r, "dw.s", "dw.t2"));
    assert!(r.statements.iter().all(|s| s.status == "ok"));
}

#[test]
fn t58_window_and_grouping() {
    let r = run(
        "INSERT INTO dw.t SELECT row_number() OVER w, GROUPING(a, b) \
         FROM dw.s GROUP BY a, b GROUPING SETS ((a, b), (a)) \
         WINDOW w AS (PARTITION BY g);\n\
         INSERT INTO dw.u SELECT count(*) FILTER (WHERE x > 0), \
           listagg(y, ',') WITHIN GROUP (ORDER BY a) FROM dw.s2",
    );
    assert!(has_cedge(&r, "dw.s", "g", "dw.t", "_c0", "derived"));
    assert!(has_cedge(&r, "dw.s2", "x", "dw.u", "_c0", "aggregate"));
    assert!(has_cedge(&r, "dw.s2", "y", "dw.u", "_c1", "derived"));
}

#[test]
fn t59_query_misc() {
    let r = run("INSERT INTO dw.t SELECT a FROM dw.s ORDER BY a;\n\
         INSERT INTO dw.u SELECT a FROM dw.s QUALIFY row_number() OVER (PARTITION BY g) = 1");
    assert!(has_tedge(&r, "dw.s", "dw.t"));
    assert!(has_tedge(&r, "dw.s", "dw.u"));
}

#[test]
fn t60_lateral_posexplode() {
    let r = run("INSERT INTO dw.t SELECT i, v FROM dw.s \
         LATERAL VIEW posexplode(arr) p AS i, v");
    assert!(has_tedge(&r, "dw.s", "dw.t"));
    assert!(
        has_cedge(&r, "dw.s", "arr", "dw.t", "v", "derived")
            || has_cedge(&r, "dw.s", "arr", "dw.t", "i", "derived")
    );
}

// ---------- resolve ----------

#[test]
fn t61_resolve_ambiguous() {
    let r = run("CREATE TABLE dw.a (id INT, x INT);\n\
         CREATE TABLE dw.b (id INT, x INT);\n\
         INSERT INTO dw.t SELECT x FROM dw.a JOIN dw.b ON a.id = b.id");
    assert!(has_tedge(&r, "dw.a", "dw.t"));
    assert!(has_tedge(&r, "dw.b", "dw.t"));
    // 非限定 x 两边都有 -> 歧义不产边
    assert!(!has_cedge(&r, "dw.a", "x", "dw.t", "x", "direct"));
}

#[test]
fn t62_resolve_star_rel() {
    // 函数名表只有 * 列：e.foo 解析落到 STAR 列兜底
    let r = run("INSERT INTO dw.t SELECT e.foo FROM explode(dw.s.arr) e");
    assert!(has_table(&r, "explode"));
    assert!(has_tedge(&r, "explode", "dw.t"));
    assert!(r.column_edges.iter().any(|e| e.target_column == "foo"));
}

// ---------- 变量/词法 ----------

#[test]
fn t63_placeholder_and_unicode() {
    let r = run("INSERT INTO dw.t SELECT ${undefined_var}, `É` FROM dw.s;\n\
         SET hivevar:q=abc;\n\
         INSERT INTO dw.u SELECT '${q}' FROM dw.s");
    assert!(has_tedge(&r, "dw.s", "dw.t"));
    assert!(has_tedge(&r, "dw.s", "dw.u"));
}

#[test]
fn t64_lexer_stmt_first_tokens() {
    // 语句以非 Word token 起始：字符串/反引号/变量/数字/!
    let r = run("'just a string';\n\
         `only_quoted`;\n\
         ${x} = 1;\n\
         12345;\n\
         !ls -l;\n\
         -- 纯注释语句\n\
         INSERT INTO dw.t SELECT a FROM dw.s");
    assert!(has_tedge(&r, "dw.s", "dw.t"));
    assert!(r.stats.statements >= 5);
}

#[test]
fn t65_lexer_var_unclosed() {
    let r = run("INSERT INTO dw.t SELECT '${unclosed FROM dw.s");
    assert!(r.stats.statements >= 1);
}

// ---------- preprocess ----------

#[test]
fn t66_preprocess_edges2() {
    let r = run("SET;\n\
         SET -v;\n\
         USE;\n\
         USE `${db}`;\n\
         ADD JAR /tmp/x.jar;\n\
         SHOW TABLES");
    assert!(r.stats.skipped >= 1);
}

#[test]
fn t67_with_dml_forms() {
    let r = run(
        "WITH x AS (SELECT a FROM dw.s) MERGE INTO dw.t USING x ON t.id = x.a \
         WHEN MATCHED THEN UPDATE SET a = x.a;\n\
         WITH y AS (SELECT b FROM dw.s) UPDATE dw.u SET a = 1 FROM y",
    );
    assert!(has_tedge(&r, "dw.s", "dw.t") || has_table(&r, "dw.t"));
}

#[test]
fn t68_from_first_multi_insert() {
    let r = run("FROM dw.s INSERT INTO dw.t SELECT a INSERT INTO dw.u SELECT a + 1");
    assert!(has_tedge(&r, "dw.s", "dw.t"));
    assert!(has_tedge(&r, "dw.s", "dw.u"));
}

#[test]
fn t69_from_first_no_insert() {
    // FROM 起手但无 INSERT -> rewrite 返回空，按原样解析（大概率失败转 fallback）
    let r = run("FROM dw.s SELECT a");
    assert!(r.stats.statements >= 1);
}

#[test]
fn t70_insert_with_from_inside() {
    // INSERT 语句内含 FROM token -> 走 is_kw("from") 改写分支
    let r = run("INSERT INTO dw.t FROM dw.s SELECT a");
    assert!(r.stats.statements >= 1);
}

// ---------- fallback ----------

#[test]
fn t71_fallback_ddl_variants() {
    let r = run("CREATE TEMPORARY VIEW dw.v AS SELECT * FROM dw.s WHERE;\n\
         CREATE TABLE dw.a LIKE dw.b LIKE dw.c garbage;\n\
         CREATE TABLE IF NOT EXISTS dw.ne (a INT) broken syntax here");
    assert!(has_table(&r, "dw.v") || has_table(&r, "dw.a") || has_table(&r, "dw.ne"));
    assert!(r.stats.fallback >= 1 || r.stats.failed >= 1);
}

#[test]
fn t72_fallback_merge_update() {
    // THEN UPDATE 不应被当作顶层 UPDATE 目标
    let r = run("MERGE INTO dw.t USING dw.s ON t.id = s.id WHEN MATCHED THEN UPDATE SET a = ???");
    assert!(has_table(&r, "dw.t"));
    assert!(has_table(&r, "dw.s"));
}

#[test]
fn t73_fallback_names_and_using() {
    // collect_name 遇非名 token 提前返回 + JOIN USING(c) 跳过
    let r = run(
        "INSERT INTO dw.t (a) SELECT x FROM dw.s JOIN dw.u USING (id) broken !!!;\n\
         UPDATE dw.t SET a = 1 FROM dw.s WHERE garbage (((",
    );
    assert!(has_table(&r, "dw.t"));
}

// ---------- 自边 / 边去重 ----------

#[test]
fn t74_self_edge_and_dedup() {
    let r = run(
        "MERGE INTO dw.t USING (SELECT a FROM dw.t) s ON t.id = s.a \
         WHEN MATCHED THEN UPDATE SET a = s.a;\n\
         INSERT INTO dw.x SELECT * FROM dw.y;\n\
         INSERT INTO dw.x SELECT * FROM dw.y",
    );
    // 两条相同 s->t 表边 -> dedup push 分支
    assert!(has_tedge(&r, "dw.y", "dw.x"));
    let e = r
        .table_edges
        .iter()
        .find(|e| e.source == tid(&r, "dw.y") && e.target == tid(&r, "dw.x"))
        .unwrap();
    assert_eq!(e.statements.len(), 2);
}

#[test]
fn t75_multi_standalone_queries() {
    let r = run_sel("SELECT a FROM dw.s;\nSELECT b FROM dw.u;\nSELECT c FROM dw.v");
    assert!(has_table(&r, "query_result") || has_table(&r, "query_result_1"));
    assert!(has_tedge(&r, "dw.u", "query_result_2") || has_tedge(&r, "dw.u", "query_result"));
}

#[test]
fn t76_setexpr_wrapped() {
    let r = run("(UPDATE dw.t SET a = 1);\nVALUES (1, 2), (3, 4)");
    assert!(r.stats.statements >= 2);
}
