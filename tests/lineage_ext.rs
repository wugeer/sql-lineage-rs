//! 覆盖率扩展用例：表达式分支、集合/窗口、merge/update 变体、
//! fallback 兜底、preprocess 边界、多文件输入

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

fn run_opt(sql: &str, analyze_select: Option<bool>) -> LineageResult {
    analyze(
        vec![InputFile {
            name: "t.sql".into(),
            bytes: sql.as_bytes().to_vec(),
        }],
        &Options {
            analyze_select,
            ..Default::default()
        },
    )
}

fn run_files(files: &[(&str, &str)]) -> LineageResult {
    analyze(
        files
            .iter()
            .map(|(n, s)| InputFile {
                name: n.to_string(),
                bytes: s.as_bytes().to_vec(),
            })
            .collect(),
        &Options::default(),
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

// 17. 投影表达式各分支：cast/case/一元/嵌套/比较谓词/is 系列/map 下标
#[test]
fn t17_expr_variants() {
    let r = run(
        "CREATE TABLE ods.s (x int, y int, n string, d string, m map<string,string>);\n\
        INSERT INTO dw.t SELECT\n\
          cast(a.x as bigint) AS c1,\n\
          a.x + a.y AS c2,\n\
          CASE WHEN a.x > 0 THEN a.y ELSE a.z END AS c3,\n\
          -a.x AS c4,\n\
          (a.x * 2) AS c5,\n\
          a.x BETWEEN 1 AND 10 AS c6,\n\
          a.y IN (1, 2) AS c7,\n\
          a.x IS NULL AS c8,\n\
          a.x IS DISTINCT FROM a.y AS c9,\n\
          a.n LIKE 'p%' AS c10,\n\
          a.n RLIKE '^x' AS c11,\n\
          a.m['k'] AS c12\n\
        FROM ods.s a;",
    );
    assert_eq!(r.statements[1].status, "ok");
    // cast 透传 direct；其余均 derived
    assert!(has_cedge(&r, "ods.s", "x", "dw.t", "c1", "direct"));
    for (tc, sc) in [
        ("c2", "y"),
        ("c4", "x"),
        ("c5", "x"),
        ("c6", "x"),
        ("c7", "y"),
        ("c8", "x"),
        ("c10", "n"),
        ("c11", "n"),
        ("c12", "m"),
    ] {
        assert!(
            has_cedge(&r, "ods.s", sc, "dw.t", tc, "derived"),
            "missing {sc}->{tc}"
        );
    }
    for sc in ["x", "y", "z"] {
        assert!(
            has_cedge(&r, "ods.s", sc, "dw.t", "c3", "derived"),
            "missing case {sc}"
        );
    }
    assert!(has_cedge(&r, "ods.s", "x", "dw.t", "c9", "derived"));
    assert!(has_cedge(&r, "ods.s", "y", "dw.t", "c9", "derived"));
}

// 18. 字符串函数形式、日期函数、interval、三元以上限定名
#[test]
fn t18_expr_func_forms() {
    let r = run("CREATE TABLE ods.s (n string, d date, v double);\n\
        INSERT INTO dw.t SELECT\n\
          substring(a.n from 2 for 3) AS s1,\n\
          trim(both ' ' from a.n) AS s2,\n\
          position('e' in a.n) AS s3,\n\
          extract(year from a.d) AS s4,\n\
          ceil(a.v) AS s5,\n\
          floor(a.v) AS s6,\n\
          interval '1' day AS s7,\n\
          ods.s.n AS s8\n\
        FROM ods.s a;");
    assert_eq!(r.statements[1].status, "ok");
    for (tc, sc) in [
        ("s1", "n"),
        ("s2", "n"),
        ("s3", "n"),
        ("s4", "d"),
        ("s5", "v"),
        ("s6", "v"),
    ] {
        assert!(
            has_cedge(&r, "ods.s", sc, "dw.t", tc, "derived"),
            "missing {sc}->{tc}"
        );
    }
    // 三段限定名 ods.s.n：直接列引用 -> direct
    assert!(has_cedge(&r, "ods.s", "n", "dw.t", "s8", "direct"));
}

// 19. EXISTS / IN 子查询 / ANY / ALL：子查询表计入表级血缘
#[test]
fn t19_subquery_predicates() {
    let r = run("INSERT INTO dw.t SELECT\n\
          EXISTS(SELECT 1 FROM ods.e WHERE e.id = a.x) AS has_e,\n\
          a.x IN (SELECT k FROM ods.k) AS in_k,\n\
          a.x = ANY(SELECT m FROM ods.m) AS any_m,\n\
          a.x > ALL(SELECT n FROM ods.n) AS all_n\n\
        FROM ods.s a;");
    assert_eq!(r.statements[0].status, "ok");
    for s in ["ods.e", "ods.k", "ods.m", "ods.n"] {
        assert!(has_tedge(&r, s, "dw.t"), "missing tedge {s}");
    }
    assert!(has_cedge(&r, "ods.s", "x", "dw.t", "in_k", "derived"));
    assert!(has_cedge(&r, "ods.m", "m", "dw.t", "any_m", "derived"));
    assert!(has_cedge(&r, "ods.n", "n", "dw.t", "all_n", "derived"));
}

// 20. GROUP BY CUBE/ROLLUP/GROUPING SETS + HAVING
#[test]
fn t20_grouping() {
    let r = run(
        "INSERT INTO dw.t1 SELECT a.x, count(*) AS n FROM ods.s a GROUP BY CUBE(a.x, a.y) HAVING count(*) > 1;\n\
        INSERT INTO dw.t2 SELECT a.x, sum(a.v) AS s FROM ods.s a GROUP BY ROLLUP(a.x, a.y);\n\
        INSERT INTO dw.t3 SELECT a.x FROM ods.s a GROUP BY a.x, a.y GROUPING SETS ((a.x), (a.y), ());",
    );
    for i in 0..3 {
        assert_eq!(r.statements[i].status, "ok", "stmt {i}");
    }
    assert!(has_cedge(&r, "ods.s", "x", "dw.t1", "x", "direct"));
    assert!(has_cedge(&r, "ods.s", "v", "dw.t2", "s", "aggregate"));
    assert!(has_cedge(&r, "ods.s", "x", "dw.t3", "x", "direct"));
}

// 21. 命名窗口 + 显式 window spec：partition/order 列以 derived 进入
#[test]
fn t21_windows() {
    let r = run("INSERT INTO dw.t SELECT\n\
          a.x,\n\
          sum(a.v) OVER w AS s1,\n\
          row_number() OVER (PARTITION BY a.g ORDER BY a.d DESC) AS rn\n\
        FROM ods.s a\n\
        WINDOW w AS (PARTITION BY a.g ORDER BY a.d);");
    assert_eq!(r.statements[0].status, "ok");
    // 窗口的 partition/order 列 -> derived；sum(v) -> aggregate
    assert!(has_cedge(&r, "ods.s", "g", "dw.t", "s1", "derived"));
    assert!(has_cedge(&r, "ods.s", "d", "dw.t", "s1", "derived"));
    assert!(has_cedge(&r, "ods.s", "v", "dw.t", "s1", "aggregate"));
    assert!(has_cedge(&r, "ods.s", "g", "dw.t", "rn", "derived"));
    assert!(has_cedge(&r, "ods.s", "d", "dw.t", "rn", "derived"));
}

// 22. 限定通配符 a.* / 不匹配限定符
#[test]
fn t22_qualified_wildcard() {
    let r = run("CREATE TABLE ods.a (x int, y int);\n\
        CREATE TABLE ods.b (p int, q int);\n\
        INSERT INTO dw.t SELECT a.*, b.q FROM ods.a a JOIN ods.b b ON a.x = b.p;\n\
        INSERT INTO dw.t2 SELECT w.* FROM ods.a;");
    assert!(has_cedge(&r, "ods.a", "x", "dw.t", "x", "direct"));
    assert!(has_cedge(&r, "ods.a", "y", "dw.t", "y", "direct"));
    assert!(has_cedge(&r, "ods.b", "q", "dw.t", "q", "direct"));
    // 未知限定符 w.*：只置 unknown 标记，不展开任何列 -> 无列边但表边仍在
    let t2 = tid(&r, "dw.t2");
    assert!(!r.column_edges.iter().any(|e| e.target_table == t2));
    assert!(has_tedge(&r, "ods.a", "dw.t2"));
}

// 23. CTE 别名列重命名 + derived 表别名列
#[test]
fn t23_alias_columns() {
    let r = run("WITH c(p, q) AS (SELECT x, y FROM ods.s)\n\
        INSERT INTO dw.t SELECT p, q FROM c;\n\
        INSERT INTO dw.t2 SELECT d.p, d.q FROM (SELECT x, y FROM ods.s) d(p, q);");
    assert!(has_cedge(&r, "ods.s", "x", "dw.t", "p", "direct"));
    assert!(has_cedge(&r, "ods.s", "y", "dw.t", "q", "direct"));
    assert!(has_cedge(&r, "ods.s", "x", "dw.t2", "p", "direct"));
    assert!(has_cedge(&r, "ods.s", "y", "dw.t2", "q", "direct"));
    assert!(!has_table(&r, "c"));
}

// 24. VALUES / EXCEPT / INTERSECT / 括号包裹查询 / 嵌套 union
#[test]
fn t24_setops() {
    let r = run("INSERT INTO dw.t VALUES (1, 'a'), (2, 'b');\n\
        INSERT INTO dw.u SELECT x FROM ods.a EXCEPT SELECT x FROM ods.b;\n\
        INSERT INTO dw.v SELECT x FROM ods.a INTERSECT SELECT x FROM ods.b;\n\
        INSERT INTO dw.w SELECT * FROM ((SELECT p FROM ods.a) UNION ALL (SELECT q FROM ods.b)) u;");
    for i in 0..4 {
        assert_eq!(r.statements[i].status, "ok", "stmt {i}");
    }
    assert_eq!(tcols(&r, "dw.t"), vec!["_c0", "_c1"]);
    assert!(has_tedge(&r, "ods.a", "dw.u"));
    assert!(has_tedge(&r, "ods.b", "dw.u"));
    assert!(has_cedge(&r, "ods.a", "x", "dw.u", "x", "direct"));
    assert!(has_cedge(&r, "ods.b", "x", "dw.u", "x", "direct"));
    assert!(has_tedge(&r, "ods.a", "dw.v"));
    assert!(has_tedge(&r, "ods.b", "dw.w"));
}

// 25. 查询级 ORDER BY / DISTRIBUTE BY / SORT BY / CLUSTER BY
#[test]
fn t25_ordering() {
    let r = run(
        "INSERT INTO dw.t SELECT a.x FROM ods.s a ORDER BY a.y LIMIT 10;\n\
        INSERT INTO dw.u SELECT a.x FROM ods.s a DISTRIBUTE BY a.g SORT BY a.h;\n\
        INSERT INTO dw.v SELECT a.x FROM ods.s a CLUSTER BY a.g;",
    );
    for i in 0..3 {
        assert_eq!(r.statements[i].status, "ok", "stmt {i}");
    }
    assert!(has_cedge(&r, "ods.s", "x", "dw.t", "x", "direct"));
    assert!(has_cedge(&r, "ods.s", "x", "dw.u", "x", "direct"));
    assert!(has_cedge(&r, "ods.s", "x", "dw.v", "x", "direct"));
    // distribute/sort/cluster 列登记进源表 catalog；
    // 查询级 ORDER BY 在空 scope 中求值、不产生血缘（y 不登记）
    let s = tcols(&r, "ods.s");
    for c in ["g", "h"] {
        assert!(s.contains(&c.to_string()), "missing col {c}");
    }
    assert!(!s.contains(&"y".to_string()));
}

// 26. LATERAL VIEW posexplode 无别名默认列 + OUTER
#[test]
fn t26_lateral_defaults() {
    let r = run("INSERT INTO dw.t SELECT a.x, lv.pos, lv.val FROM ods.s a\n\
        LATERAL VIEW posexplode(split(a.arr, ',')) lv;\n\
        INSERT INTO dw.t2 SELECT a.x, lv2.e FROM ods.s a\n\
        LATERAL VIEW OUTER explode(a.arr) lv2 AS e;");
    assert_eq!(r.statements[0].status, "ok");
    assert_eq!(r.statements[1].status, "ok");
    assert!(has_cedge(&r, "ods.s", "arr", "dw.t", "pos", "derived"));
    assert!(has_cedge(&r, "ods.s", "arr", "dw.t", "val", "derived"));
    assert!(has_cedge(&r, "ods.s", "arr", "dw.t2", "e", "derived"));
}

// 27. MERGE 扩展：AND 谓词 / DELETE / INSERT *
#[test]
fn t27_merge_ext() {
    let r = run(
        "MERGE INTO dw.t USING (SELECT id, v FROM ods.s) s ON t.id = s.id\n\
        WHEN MATCHED AND s.v > 0 THEN UPDATE SET t.v = s.v\n\
        WHEN MATCHED AND s.v <= 0 THEN DELETE\n\
        WHEN NOT MATCHED AND s.id > 0 THEN INSERT (id, v) VALUES (s.id, s.v)\n\
        WHEN NOT MATCHED THEN INSERT *;",
    );
    assert_eq!(r.statements[0].kind, "merge");
    assert_eq!(r.statements[0].status, "ok");
    assert!(has_tedge(&r, "ods.s", "dw.t"));
    assert!(has_cedge(&r, "ods.s", "v", "dw.t", "v", "direct"));
    assert!(has_cedge(&r, "ods.s", "id", "dw.t", "id", "direct"));
}

// 28. UPDATE 变体：JOIN 更新 / tuple 赋值（按解析能力保留）
#[test]
fn t28_update_variants() {
    let r = run(
        "UPDATE dw.t SET t.a = s.a, t.b = s.b FROM ods.s s WHERE t.id = s.id;\n\
        UPDATE dw.u SET x = 1 WHERE EXISTS (SELECT 1 FROM ods.e e WHERE e.id = dw.u.y);",
    );
    assert_eq!(r.statements[0].status, "ok");
    assert!(has_cedge(&r, "ods.s", "a", "dw.t", "a", "direct"));
    assert!(has_cedge(&r, "ods.s", "b", "dw.t", "b", "direct"));
    assert!(has_tedge(&r, "ods.e", "dw.u"));
}

// 29. 未限定列解析：歧义 -> 无边；唯一未知表 -> 落在该表；三段限定名
#[test]
fn t29_resolve() {
    let r = run("CREATE TABLE ods.a (x int, y int);\n\
        CREATE TABLE ods.b (x int, z int);\n\
        INSERT INTO dw.t SELECT x, a.y, b.z FROM ods.a a JOIN ods.b b ON a.y = b.z;\n\
        INSERT INTO dw.t2 SELECT w FROM ods.a a JOIN ods.u u ON a.x = u.i;\n\
        INSERT INTO dw.t3 SELECT ods.a.y FROM ods.a;");
    // x 在两个已知表都存在 -> 歧义，无 x 列边
    let tt = tid(&r, "dw.t");
    assert!(!r
        .column_edges
        .iter()
        .any(|e| e.target_table == tt && e.target_column == "x"));
    assert!(has_cedge(&r, "ods.a", "y", "dw.t", "y", "direct"));
    assert!(has_cedge(&r, "ods.b", "z", "dw.t", "z", "direct"));
    // w 不属于已知表 a -> 归入唯一未知表 u
    assert!(has_cedge(&r, "ods.u", "w", "dw.t2", "w", "direct"));
    // 三段限定名 ods.a.y
    assert!(has_cedge(&r, "ods.a", "y", "dw.t3", "y", "direct"));
}

// 30. INSERT OVERWRITE [LOCAL] DIRECTORY
#[test]
fn t30_directory() {
    let r = run(
        "INSERT OVERWRITE DIRECTORY '/tmp/out_a' SELECT a FROM ods.s;\n\
        INSERT OVERWRITE LOCAL DIRECTORY '/tmp/out_b' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT b FROM ods.s;",
    );
    let dir_a = r
        .tables
        .iter()
        .find(|t| t.name == "dir:/tmp/out_a")
        .expect("dir:/tmp/out_a not found");
    assert_eq!(dir_a.kind, "directory");
    assert!(has_tedge(&r, "ods.s", "dir:/tmp/out_a"));
    assert!(has_tedge(&r, "ods.s", "dir:/tmp/out_b"));
}

// 31. fallback：截断 UPDATE / MERGE（THEN UPDATE 不重复计目标）/ 坏 INSERT
#[test]
fn t31_fallback_dml() {
    let r = run("UPDATE dw.t SET x = ;\n\
        MERGE INTO dw.m USING s ON m.id = s.id WHEN MATCHED THEN ;\n\
        INSERT INTO dw.i SELECT a, FROM ods.x x JOIN ods.y y USING (id);");
    assert_eq!(r.statements[0].status, "fallback");
    assert_eq!(r.statements[1].status, "fallback");
    assert_eq!(r.statements[2].status, "fallback");
    assert!(has_table(&r, "dw.t"));
    assert!(r.tables[tid(&r, "dw.t")].is_target);
    // merge 目标 + using 源；THEN 后的 UPDATE 不再产生目标
    assert!(has_table(&r, "dw.m"));
    assert!(has_tedge(&r, "s", "dw.m"));
    // 坏 INSERT：fallback 提取目标 + from/join 源
    assert!(has_tedge(&r, "ods.x", "dw.i"));
    assert!(has_tedge(&r, "ods.y", "dw.i"));
}

// 32. fallback DDL：PARTITIONED BY 列提取 / PRIMARY KEY 跳过 / LIKE / CLONE
#[test]
fn t32_fallback_ddl() {
    let r = run(
        "CREATE TABLE dw.b (x int, y string) PARTITIONED BY (d string, h string) STORED AS ORC;\n\
        CREATE TABLE dw.l LIKE dw.b;\n\
        CREATE TABLE dw.c CLONE dw.b;\n\
        CREATE TABLE dw.k (a int, PRIMARY KEY(a), bb string);",
    );
    for i in 0..4 {
        assert_eq!(r.statements[i].status, "ok", "stmt {i}");
    }
    let cols = tcols(&r, "dw.b");
    for c in ["x", "y", "d", "h"] {
        assert!(cols.contains(&c.to_string()), "missing col {c}");
    }
    // LIKE / CLONE 拷贝列目录
    assert_eq!(tcols(&r, "dw.l"), cols);
    assert_eq!(tcols(&r, "dw.c"), cols);
    let k = tcols(&r, "dw.k");
    assert!(k.contains(&"a".to_string()) && k.contains(&"bb".to_string()));
    assert!(!k.contains(&"primary".to_string()));
}

// 33. fallback：CTE 名不当源表；USING(col) 跳过
#[test]
fn t33_fallback_cte() {
    let r = run("WITH x AS (SELECT 1 AS v) INSERT INTO dw.t SELECT * FROM x broken tail ???;");
    assert_eq!(r.statements[0].status, "fallback");
    assert!(has_table(&r, "dw.t"));
    assert!(!has_table(&r, "x"));
}

// 34. preprocess：SKIP 关键字 / SET 变体 / USE 反引号 / CREATE INDEX / 物化视图
#[test]
fn t34_preprocess_edges() {
    let r = run("set;\n\
        set x = 1;\n\
        set hiveconf:yy = 'a b';\n\
        add jar 'x.jar';\n\
        dfs -ls /tmp;\n\
        EXPLAIN SELECT 1 FROM ods.s;\n\
        DROP TABLE ods.s;\n\
        CREATE INDEX idx ON ods.s(x);\n\
        USE `mydb`;\n\
        INSERT INTO t SELECT a FROM s;");
    assert_eq!(r.statements.len(), 10);
    assert_eq!(r.statements[0].kind, "set");
    assert_eq!(r.statements[1].kind, "set");
    assert_eq!(r.statements[2].kind, "set");
    for i in 3..8 {
        assert_eq!(r.statements[i].kind, "other", "stmt {i}");
        assert_eq!(r.statements[i].status, "skipped", "stmt {i}");
    }
    assert_eq!(r.statements[8].kind, "use");
    // USE `mydb` 生效于未限定名
    assert!(has_tedge(&r, "mydb.s", "mydb.t"));
}

// 35. CREATE MATERIALIZED VIEW / TEMPORARY VIEW / OR REPLACE VIEW（走 parser）
#[test]
fn t35_view_variants() {
    let r = run("CREATE MATERIALIZED VIEW dw.mv AS SELECT a FROM ods.s;\n\
        CREATE OR REPLACE VIEW dw.rv AS SELECT b FROM ods.s;");
    assert_eq!(r.statements[0].kind, "create_view");
    assert_eq!(r.statements[1].kind, "create_view");
    assert_eq!(r.tables[tid(&r, "dw.mv")].kind, "view");
    assert!(has_cedge(&r, "ods.s", "a", "dw.mv", "a", "direct"));
    assert!(has_cedge(&r, "ods.s", "b", "dw.rv", "b", "direct"));
}

// 36. analyze_select = Some(false)：独立 SELECT 跳过且不产 query_result
#[test]
fn t36_select_disabled() {
    let r = run_opt("SELECT x FROM ods.a;", Some(false));
    assert_eq!(r.statements[0].kind, "query");
    assert_eq!(r.statements[0].status, "skipped");
    assert!(!has_table(&r, "query_result"));
    // 纯 VALUES / TABLE 语句同样按 query 处理
    let r2 = run_opt("VALUES (1, 2);\nTABLE ods.a;", Some(true));
    assert_eq!(r2.statements.len(), 2);
}

// 37. 多文件输入：同一脚本上下文（USE 跨文件生效），file 下标正确
#[test]
fn t37_multi_file() {
    let r = run_files(&[
        ("a.sql", "USE dw;\nCREATE TABLE t1 (x int);"),
        ("b.sql", "INSERT INTO t2 SELECT x FROM t1;"),
    ]);
    assert_eq!(r.stats.files, 2);
    assert_eq!(r.files.len(), 2);
    assert_eq!(r.statements.len(), 3);
    assert_eq!(r.statements[0].file, 0);
    assert_eq!(r.statements[1].file, 0);
    assert_eq!(r.statements[2].file, 1);
    assert!(has_tedge(&r, "dw.t1", "dw.t2"));
}

// 38. lexer 边界：未闭合字符串吞掉后续 / 未闭合块注释 / BOM / ! 命令
#[test]
fn t38_lexer_edges() {
    // 未闭合字符串：从 ' 开始全吞 -> 只剩 1 条 select，解析失败
    let r = run("SELECT 'unclosed\nINSERT INTO t SELECT 1;");
    assert_eq!(r.statements.len(), 1);
    // 末尾未闭合块注释吞到 EOF
    let r2 = run("SELECT 1;\n/* never closed");
    assert_eq!(r2.statements.len(), 1);
    // BOM 头 + ! 命令行跳过
    let r3 = analyze(
        vec![InputFile {
            name: "bom.sql".into(),
            bytes: b"\xef\xbb\xbf!ls -la;\nSELECT 1;".to_vec(),
        }],
        &Options::default(),
    );
    assert_eq!(r3.statements.len(), 2);
    assert_eq!(r3.statements[0].kind, "other");
}

// 39. INSERT ... SELECT 经 WITH 包裹的 update/merge 分类已覆盖；此处测 WITH...FROM 多路改写
#[test]
fn t39_with_multi_insert() {
    let r = run("WITH w AS (SELECT a, b FROM ods.s)\n\
        FROM w\n\
        INSERT INTO dw.t1 SELECT a WHERE b > 0\n\
        INSERT OVERWRITE TABLE dw.t2 SELECT b ORDER BY a;");
    assert_eq!(r.statements[0].kind, "insert");
    assert_eq!(r.statements[0].status, "ok");
    assert!(has_tedge(&r, "ods.s", "dw.t1"));
    assert!(has_tedge(&r, "ods.s", "dw.t2"));
    assert!(has_cedge(&r, "ods.s", "a", "dw.t1", "a", "direct"));
    assert!(has_cedge(&r, "ods.s", "b", "dw.t2", "b", "direct"));
}

// 40. 表函数 / UNNEST / 嵌套 join（按解析能力保留可解析的）
#[test]
fn t40_factor_variants() {
    // 嵌套 join
    let r = run("INSERT INTO dw.t SELECT a.x, b.y FROM (ods.a a JOIN ods.b b ON a.i = b.i);");
    assert!(has_tedge(&r, "ods.a", "dw.t"));
    assert!(has_tedge(&r, "ods.b", "dw.t"));
}

// 41. 更多表达式分支：数组/map/struct/tuple/lambda/json/convert/overlay/at-time-zone 等
#[test]
fn t41_expr_more() {
    let r = run(
        "CREATE TABLE ods.s (x int, y int, n string, d date, j string, arr array<int>);\n\
        INSERT INTO dw.t SELECT\n\
          ARRAY[a.x, a.y] AS e1,\n\
          MAP('k', a.x) AS e2,\n\
          STRUCT(a.x, a.y) AS e3,\n\
          (a.x, a.y) AS e4,\n\
          transform(array(1,2), v -> v + a.y) AS e5,\n\
          a.d AT TIME ZONE 'UTC' AS e6,\n\
          CONVERT(a.x, BIGINT) AS e7,\n\
          OVERLAY(a.n PLACING 'x' FROM 1 FOR 2) AS e8,\n\
          a.j IS JSON AS e9,\n\
          a.n IS NORMALIZED AS e10,\n\
          a.n ILIKE 'P%' AS e11,\n\
          a.j -> 'k' AS e12,\n\
          a.j ->> 'k' AS e13,\n\
          DATE '2024-01-01' AS e14,\n\
          a.x IN UNNEST(a.arr) AS e15,\n\
          a.n COLLATE 'en' AS e16,\n\
          a.n SIMILAR TO 'p%' AS e17,\n\
          a.x IS UNKNOWN AS e18,\n\
          a.x IS TRUE AS e19,\n\
          a.y IS NOT FALSE AS e20,\n\
          a.x IS NOT DISTINCT FROM a.y AS e21,\n\
          a.x NOT BETWEEN 1 AND 5 AS e22,\n\
          a.n NOT LIKE 'p!_%' ESCAPE '!' AS e23,\n\
          CASE a.x WHEN 1 THEN a.y WHEN 2 THEN a.z ELSE a.w END AS e24,\n\
          a.d + interval 1 day AS e25,\n\
          a.b.c.d AS e26,\n\
          a.arr[0] AS e27,\n\
          a.arr[1:2] AS e28\n\
        FROM ods.s a;",
    );
    assert_eq!(r.statements[1].status, "ok");
    for (tc, sc) in [
        ("e1", "x"),
        ("e2", "x"),
        ("e3", "x"),
        ("e4", "x"),
        ("e5", "y"),
        ("e6", "d"),
        ("e7", "x"),
        ("e8", "n"),
        ("e9", "j"),
        ("e10", "n"),
        ("e11", "n"),
        ("e12", "j"),
        ("e13", "j"),
        ("e15", "x"),
        ("e15", "arr"),
        ("e17", "n"),
        ("e18", "x"),
        ("e19", "x"),
        ("e20", "y"),
        ("e21", "x"),
        ("e22", "x"),
        ("e23", "n"),
        ("e24", "x"),
        ("e25", "d"),
    ] {
        assert!(
            has_cedge(&r, "ods.s", sc, "dw.t", tc, "derived"),
            "missing {sc}->{tc}"
        );
    }
    // case operand 形式：条件分支结果列也进入
    assert!(has_cedge(&r, "ods.s", "y", "dw.t", "e24", "derived"));
    assert!(has_cedge(&r, "ods.s", "z", "dw.t", "e24", "derived"));
    assert!(has_cedge(&r, "ods.s", "w", "dw.t", "e24", "derived"));
    // COLLATE 与 CAST 同为透传 -> direct
    assert!(has_cedge(&r, "ods.s", "n", "dw.t", "e16", "direct"));
    // 深层限定名 a.b.c.d 无法匹配限定符 -> 无边
    let tt = tid(&r, "dw.t");
    assert!(!r
        .column_edges
        .iter()
        .any(|e| e.target_table == tt && e.target_column == "e26"));
    // 下标访问：root 与 access 链中的标识符都进入（arr 列边）
    assert!(has_cedge(&r, "ods.s", "arr", "dw.t", "e27", "derived"));
    assert!(has_cedge(&r, "ods.s", "arr", "dw.t", "e28", "derived"));
}

// 42. 函数高级形态：FILTER / WITHIN GROUP / 聚合内 ORDER BY / 带引号别名
#[test]
fn t42_func_advanced() {
    let r = run("INSERT INTO dw.t SELECT\n\
          count(*) FILTER (WHERE a.x > 0) AS f1,\n\
          percentile(a.x, 0.5) WITHIN GROUP (ORDER BY a.y) AS f2,\n\
          collect_list(a.z ORDER BY a.y) AS f3,\n\
          a.x AS \"MY COL\"\n\
        FROM ods.s a;");
    assert_eq!(r.statements[0].status, "ok");
    assert!(has_cedge(&r, "ods.s", "x", "dw.t", "f1", "aggregate"));
    assert!(has_cedge(&r, "ods.s", "x", "dw.t", "f2", "aggregate"));
    assert!(has_cedge(&r, "ods.s", "y", "dw.t", "f2", "aggregate"));
    assert!(has_cedge(&r, "ods.s", "z", "dw.t", "f3", "aggregate"));
    assert!(has_cedge(&r, "ods.s", "y", "dw.t", "f3", "aggregate"));
    assert!(has_cedge(&r, "ods.s", "x", "dw.t", "my col", "direct"));
}

// 43. FROM 变体：UNNEST / 表函数 / TABLE() / LEFT SEMI / CROSS / USING / NATURAL
#[test]
fn t43_from_variants() {
    let r = run(
        "INSERT INTO dw.t1 SELECT * FROM ods.s a, UNNEST(a.arr) u;\n\
        INSERT INTO dw.t2 SELECT f.v FROM ods.s a, explode(a.arr) f(v);\n\
        INSERT INTO dw.t3 SELECT a.x FROM ods.s a LEFT SEMI JOIN ods.e e ON a.id = e.id;\n\
        INSERT INTO dw.t4 SELECT a.x, b.y FROM ods.s a CROSS JOIN ods.b b;\n\
        INSERT INTO dw.t5 SELECT x FROM ods.a JOIN ods.b USING (i);\n\
        INSERT INTO dw.t6 SELECT x FROM ods.a NATURAL JOIN ods.b;\n\
        INSERT INTO dw.t7 SELECT * FROM TABLE(explode(array(1,2)));",
    );
    for i in 0..7 {
        assert_eq!(r.statements[i].status, "ok", "stmt {i}");
    }
    // Hive 方言下 UNNEST/explode 按函数名表处理：登记为源表并按 * 展开
    assert!(has_table(&r, "unnest"));
    assert!(has_cedge(&r, "ods.s", "*", "dw.t1", "*", "direct"));
    assert!(has_cedge(&r, "unnest", "*", "dw.t1", "*", "direct"));
    // explode(a.arr) f(v)：函数名表 explode，f.v 限定解析 -> explode.v 列边
    assert!(has_cedge(&r, "explode", "v", "dw.t2", "v", "direct"));
    assert!(has_tedge(&r, "ods.e", "dw.t3"));
    assert!(has_cedge(&r, "ods.s", "x", "dw.t3", "x", "direct"));
    assert!(has_cedge(&r, "ods.s", "x", "dw.t4", "x", "direct"));
    assert!(has_cedge(&r, "ods.b", "y", "dw.t4", "y", "direct"));
    assert!(has_tedge(&r, "ods.a", "dw.t5"));
    assert!(has_tedge(&r, "ods.b", "dw.t5"));
    assert!(has_tedge(&r, "ods.a", "dw.t6"));
    assert!(has_tedge(&r, "ods.b", "dw.t6"));
}

// 44. MERGE 通配形态：UPDATE SET * / INSERT *
#[test]
fn t44_merge_wildcards() {
    let r = run(
        "MERGE INTO dw.t USING ods.s s ON t.id = s.id WHEN MATCHED THEN UPDATE SET *;\n\
        MERGE INTO dw.u USING ods.s s ON u.id = s.id WHEN NOT MATCHED THEN INSERT *;",
    );
    assert_eq!(r.statements[0].status, "ok");
    assert_eq!(r.statements[1].status, "ok");
    // 第一条 merge 时 ods.s 列未知 -> * 列边；
    // ON 中的 s.id 已把 id 登记入 catalog，第二条按已知列展开 -> id 列边
    assert!(has_cedge(&r, "ods.s", "*", "dw.t", "*", "direct"));
    assert!(has_cedge(&r, "ods.s", "id", "dw.u", "id", "direct"));
}

// 45. UPDATE JOIN / tuple 赋值
#[test]
fn t45_update_ext() {
    let r = run("UPDATE dw.t JOIN ods.s s ON t.id = s.id SET t.a = s.a;\n\
        UPDATE dw.u SET (a, b) = (1, 2) WHERE u.id = 3;");
    assert_eq!(r.statements[0].status, "ok");
    assert_eq!(r.statements[0].kind, "update");
    assert!(has_tedge(&r, "ods.s", "dw.t"));
    assert!(has_cedge(&r, "ods.s", "a", "dw.t", "a", "direct"));
    assert_eq!(r.statements[1].status, "ok");
    // tuple 赋值目标列登记入 catalog（字面值无来源列）
    let u = tcols(&r, "dw.u");
    assert!(u.contains(&"a".to_string()) && u.contains(&"b".to_string()));
}

// 46. 括号包裹 insert 查询 / 引用整表
#[test]
fn t46_paren_query() {
    let r =
        run("INSERT INTO dw.t (SELECT x FROM ods.a);\nINSERT INTO dw.u ((SELECT y FROM ods.b));");
    assert_eq!(r.statements[0].status, "ok");
    assert_eq!(r.statements[1].status, "ok");
    assert!(has_cedge(&r, "ods.a", "x", "dw.t", "x", "direct"));
    assert!(has_cedge(&r, "ods.b", "y", "dw.u", "y", "direct"));
}
