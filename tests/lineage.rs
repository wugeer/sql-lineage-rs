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

fn run_db(sql: &str, db: &str) -> LineageResult {
    analyze(
        vec![InputFile {
            name: "t.sql".into(),
            bytes: sql.as_bytes().to_vec(),
        }],
        &Options {
            default_db: Some(db.into()),
            ..Default::default()
        },
    )
}

fn run_select(sql: &str) -> LineageResult {
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

// 1. INSERT OVERWRITE PARTITION + JOIN + GROUP BY
#[test]
fn t1_basic_insert() {
    let r = run("INSERT OVERWRITE TABLE dw.a PARTITION(dt='x') \
        SELECT x.id, sum(y.amt) AS amt FROM ods.x x LEFT JOIN ods.y y ON x.id=y.id GROUP BY x.id;");
    assert!(has_tedge(&r, "ods.x", "dw.a"));
    assert!(has_tedge(&r, "ods.y", "dw.a"));
    assert!(has_cedge(&r, "ods.x", "id", "dw.a", "id", "direct"));
    assert!(has_cedge(&r, "ods.y", "amt", "dw.a", "amt", "aggregate"));
    assert_eq!(r.statements.len(), 1);
    assert_eq!(r.statements[0].kind, "insert");
    assert_eq!(r.statements[0].status, "ok");
}

// 2. 多路 INSERT
#[test]
fn t2_multi_insert() {
    let r = run(
        "FROM ods.src s INSERT OVERWRITE TABLE dw.a SELECT s.id WHERE s.k=1 \
        INSERT OVERWRITE TABLE dw.b SELECT s.name;",
    );
    assert_eq!(r.statements.len(), 1);
    let st = &r.statements[0];
    assert_eq!(st.targets.len(), 2);
    assert!(has_tedge(&r, "ods.src", "dw.a"));
    assert!(has_tedge(&r, "ods.src", "dw.b"));
    assert!(has_cedge(&r, "ods.src", "id", "dw.a", "id", "direct"));
    assert!(has_cedge(&r, "ods.src", "name", "dw.b", "name", "direct"));
}

// 3. 嵌套 CTE 链 + 子查询 + UNION ALL
#[test]
fn t3_cte_union() {
    let r = run("WITH a AS (SELECT id, v FROM ods.t1), \
        b AS (SELECT id, v*2 AS v2 FROM a) \
        INSERT INTO dw.t \
        SELECT b.id, b.v2 FROM b \
        UNION ALL \
        SELECT x.id, (SELECT max(v) FROM ods.t3 m WHERE m.id=x.id) FROM ods.t2 x;");
    // CTE 不是输出表
    assert!(!has_table(&r, "a") && !has_table(&r, "b"));
    for s in ["ods.t1", "ods.t2", "ods.t3"] {
        assert!(has_tedge(&r, s, "dw.t"), "missing table edge {s}");
    }
    assert!(has_cedge(&r, "ods.t1", "id", "dw.t", "id", "direct"));
    assert!(has_cedge(&r, "ods.t2", "id", "dw.t", "id", "direct"));
    // v*2 派生 + max(v) 聚合 都汇入第二列 v2
    assert!(has_cedge(&r, "ods.t1", "v", "dw.t", "v2", "derived"));
    assert!(has_cedge(&r, "ods.t3", "v", "dw.t", "v2", "aggregate"));
}

// 4. LATERAL VIEW explode
#[test]
fn t4_lateral_view() {
    let r = run("INSERT OVERWRITE TABLE dw.l SELECT a.id, t.tag \
        FROM ods.x a LATERAL VIEW explode(split(a.tags, ',')) t AS tag;");
    assert!(has_cedge(&r, "ods.x", "tags", "dw.l", "tag", "derived"));
    assert!(has_cedge(&r, "ods.x", "id", "dw.l", "id", "direct"));
}

// 5. SELECT *：DDL 走 fallback 的表 vs 未知表
#[test]
fn t5_star() {
    let r = run(
        "CREATE TABLE dw.s (m map<string,string>, a array<struct<x:int>>, n int) PARTITIONED BY (dt string);\n\
        INSERT INTO dw.t1 SELECT * FROM dw.s;\n\
        INSERT INTO dw.t2 SELECT * FROM unk;\n",
    );
    // 普通 DDL 走 token 快路提取列（含 map/array<struct> 复杂类型）
    assert_eq!(r.statements[0].status, "ok");
    let s = tid(&r, "dw.s");
    let cols = &r.tables[s].columns;
    assert!(cols.contains(&"m".to_string()) && cols.contains(&"n".to_string()));
    // 展开已知列
    assert!(has_cedge(&r, "dw.s", "m", "dw.t1", "m", "direct"));
    assert!(has_cedge(&r, "dw.s", "n", "dw.t1", "n", "direct"));
    // 未知表 -> * 边
    assert!(has_cedge(&r, "unk", "*", "dw.t2", "*", "direct"));
}

// 6. 目标 DDL 已知 + 无别名表达式 -> 位置映射；动态分区列
#[test]
fn t6_positional_ddl() {
    let r = run(
        "CREATE TABLE dw.p (id bigint, v string) PARTITIONED BY (dt string, hh string);\n\
        INSERT INTO dw.p PARTITION(dt='2024', hh) SELECT x.a, concat(x.b,'!'), x.c FROM ods.z x;",
    );
    // dt 静态跳过；hh 动态映射到末尾
    assert!(has_cedge(&r, "ods.z", "a", "dw.p", "id", "direct"));
    assert!(has_cedge(&r, "ods.z", "b", "dw.p", "v", "derived"));
    assert!(has_cedge(&r, "ods.z", "c", "dw.p", "hh", "direct"));
    // 显式列清单
    let r2 = run("CREATE TABLE dw.q (a int, b int, c int);\n\
        INSERT INTO dw.q (a, c) SELECT x, y FROM ods.w;");
    assert!(has_cedge(&r2, "ods.w", "x", "dw.q", "a", "direct"));
    assert!(has_cedge(&r2, "ods.w", "y", "dw.q", "c", "direct"));
}

// 7. 变量替换
#[test]
fn t7_vars() {
    let r = run("set hivevar:db=ods;\n\
        INSERT INTO dw.t SELECT * FROM ${hivevar:db}.x;\n\
        INSERT INTO dw.u SELECT * FROM ${env}.y;\n");
    assert!(has_tedge(&r, "ods.x", "dw.t"));
    assert!(has_tedge(&r, "${env}.y", "dw.u"));
}

// 8. USE 限定 + CTE / 临时表不限定
#[test]
fn t8_use_qualify() {
    let r = run("USE dw;\n\
        CREATE TEMPORARY TABLE tmp1 (a int);\n\
        WITH c AS (SELECT x FROM ods.s) \
        INSERT INTO t1 SELECT c.x, tmp1.a FROM c JOIN tmp1 ON 1=1;");
    assert!(has_table(&r, "dw.t1"));
    assert!(has_table(&r, "tmp1"));
    assert!(!has_table(&r, "c"));
    assert!(!has_table(&r, "dw.tmp1"));
    assert!(!has_table(&r, "dw.c"));
    assert!(has_tedge(&r, "ods.s", "dw.t1"));
    assert!(has_tedge(&r, "tmp1", "dw.t1"));
    let tmp = tid(&r, "tmp1");
    assert_eq!(r.tables[tmp].kind, "temp");
    // default_db 参数同样生效
    let r2 = run_db("INSERT INTO t SELECT * FROM s;", "zz");
    assert!(has_tedge(&r2, "zz.s", "zz.t"));
}

// 9. 语句切分：字符串/注释中的 ; 、转义引号、行号与字节偏移
#[test]
fn t9_split_offsets() {
    let sql = "-- comment with ; and -- more\n\
        select 'a;b', \"c;d\"; -- trailing ;\n\
        /* block\n\
        ; comment */\n\
        INSERT INTO t SELECT 'x\\'y';\n\
        SELECT 1";
    let r = run(sql);
    assert_eq!(r.statements.len(), 3);
    let s0 = &r.statements[0];
    assert_eq!(s0.line, 2);
    assert!(
        sql[s0.start..s0.end].starts_with("select"),
        "{}",
        &sql[s0.start..s0.end]
    );
    let s1 = &r.statements[1];
    assert!(
        sql[s1.start..s1.end].starts_with("INSERT"),
        "{}",
        &sql[s1.start..s1.end]
    );
    assert_eq!(s1.line, 5);
    let s2 = &r.statements[2];
    assert!(sql[s2.start..s2.end].starts_with("SELECT 1"));
    assert_eq!(s2.line, 6);
}

// 10. GBK 输入
#[test]
fn t10_gbk() {
    let sql = "-- 中文注释 with ;\nCREATE TABLE dw.t (a int, b string);\nINSERT INTO dw.u SELECT a FROM dw.t;\nSELECT a FROM dw.t;";
    let (bytes, _, _) = encoding_rs::GBK.encode(sql);
    let r = analyze(
        vec![InputFile {
            name: "gbk.sql".into(),
            bytes: bytes.into_owned(),
        }],
        &Options::default(),
    );
    assert_eq!(r.files[0].encoding, "gbk");
    assert_eq!(r.statements.len(), 3);
    assert_eq!(r.stats.failed, 0);
    assert!(has_cedge(&r, "dw.t", "a", "dw.u", "a", "direct"));
}

// 11. CTAS / CREATE VIEW 写入 catalog，后续 SELECT * 展开
#[test]
fn t11_ctas_view_catalog() {
    let r = run("CREATE TABLE dw.ct AS SELECT a, b+1 AS b1 FROM ods.s;\n\
        CREATE VIEW dw.vv AS SELECT a, b1 FROM dw.ct;\n\
        INSERT INTO dw.t SELECT * FROM dw.vv;");
    assert_eq!(r.statements[0].kind, "ctas");
    assert_eq!(r.statements[1].kind, "create_view");
    assert_eq!(r.tables[tid(&r, "dw.ct")].columns, vec!["a", "b1"]);
    assert_eq!(r.tables[tid(&r, "dw.vv")].kind, "view");
    assert!(has_cedge(&r, "ods.s", "a", "dw.ct", "a", "direct"));
    assert!(has_cedge(&r, "ods.s", "b", "dw.ct", "b1", "derived"));
    assert!(has_cedge(&r, "dw.ct", "a", "dw.vv", "a", "direct"));
    assert!(has_cedge(&r, "dw.vv", "b1", "dw.t", "b1", "direct"));
}

// 12. 坏语句不影响其它语句
#[test]
fn t12_garbage() {
    let r = run("INSERT INTO dw.a SELECT x FROM ods.p;\n\
        this is garbage;\n\
        INSERT INTO dw.b SELECT y FROM ods.q;");
    assert_eq!(r.statements.len(), 3);
    assert_eq!(r.statements[0].status, "ok");
    assert_eq!(r.statements[1].status, "error");
    assert!(r.statements[1].error.is_some());
    assert_eq!(r.statements[2].status, "ok");
    assert!(has_tedge(&r, "ods.p", "dw.a"));
    assert!(has_tedge(&r, "ods.q", "dw.b"));
    assert!(r.stats.failed >= 1);
}

// 13. MERGE INTO：表级 + 字段级血缘（WHEN MATCHED UPDATE / NOT MATCHED INSERT）
#[test]
fn t13_merge() {
    let r = run("MERGE INTO dw.dim_cust t \
        USING (SELECT cust_id, name, region FROM ods.cust_inc WHERE dt='x') s \
        ON t.cust_id = s.cust_id \
        WHEN MATCHED THEN UPDATE SET t.name = s.name, t.region = upper(s.region) \
        WHEN NOT MATCHED THEN INSERT (cust_id, name) VALUES (s.cust_id, s.name);");
    assert_eq!(r.statements[0].kind, "merge");
    assert!(has_tedge(&r, "ods.cust_inc", "dw.dim_cust"));
    assert!(has_cedge(
        &r,
        "ods.cust_inc",
        "name",
        "dw.dim_cust",
        "name",
        "direct"
    ));
    assert!(has_cedge(
        &r,
        "ods.cust_inc",
        "region",
        "dw.dim_cust",
        "region",
        "derived"
    ));
    assert!(has_cedge(
        &r,
        "ods.cust_inc",
        "cust_id",
        "dw.dim_cust",
        "cust_id",
        "direct"
    ));
}

// 14. UPDATE：目标表 + FROM 来源列血缘
#[test]
fn t14_update() {
    let r = run("UPDATE dw.t SET v = s.v2, flag = t.flag \
        FROM (SELECT id, v2 FROM ods.src) s \
        WHERE t.id = s.id;");
    assert_eq!(r.statements[0].kind, "update");
    assert!(has_tedge(&r, "ods.src", "dw.t"));
    assert!(has_cedge(&r, "ods.src", "v2", "dw.t", "v", "direct"));
}

// 15. 纯 SELECT 脚本：自动模式映射为虚拟查询结果表
#[test]
fn t15_standalone_select() {
    let r = run("SELECT a.id, upper(b.name) AS uname FROM ods.a a JOIN ods.b b ON a.id=b.id;");
    assert_eq!(r.statements[0].kind, "query");
    assert_eq!(r.statements[0].status, "ok");
    assert!(has_tedge(&r, "ods.a", "query_result"));
    assert!(has_tedge(&r, "ods.b", "query_result"));
    assert!(has_cedge(&r, "ods.a", "id", "query_result", "id", "direct"));
    assert!(has_cedge(
        &r,
        "ods.b",
        "name",
        "query_result",
        "uname",
        "derived"
    ));
    // 显式开启：多条查询得到编号结果表
    let r2 = run_select(
        "SELECT x FROM ods.t1;\n\
         INSERT INTO dw.d SELECT x FROM ods.t1;\n\
         SELECT y FROM ods.t2;",
    );
    assert!(has_tedge(&r2, "ods.t1", "query_result_1"));
    assert!(has_tedge(&r2, "ods.t2", "query_result_2"));
    assert!(has_tedge(&r2, "ods.t1", "dw.d"));
    // 混合脚本且未显式开启：SELECT 不产生结果表
    let r3 = run("SELECT x FROM ods.t1;\nINSERT INTO dw.d SELECT x FROM ods.t1;");
    assert!(!has_table(&r3, "query_result_1") && !has_table(&r3, "query_result"));
}

// 16. WITH ... MERGE / WITH ... UPDATE 也能识别
#[test]
fn t16_with_merge() {
    let r = run("WITH s AS (SELECT id, v FROM ods.inc) \
        MERGE INTO dw.t USING s ON dw.t.id=s.id \
        WHEN MATCHED THEN UPDATE SET v=s.v;");
    assert_eq!(r.statements[0].kind, "merge");
    assert!(has_tedge(&r, "ods.inc", "dw.t"));
    assert!(has_cedge(&r, "ods.inc", "v", "dw.t", "v", "direct"));
}
