#!/usr/bin/env python3
"""生成一个给定大小的模拟 Hive 脚本，用于性能测试。

用法: gen_big_sql.py [大小MB, 默认10] [输出文件]
"""
import random
import sys

random.seed(42)

NTABLES = 2000
DBS = ["ods", "dwd", "dim", "dw", "ads", "tmp"]


def tname(i):
    return f"{DBS[i % len(DBS)]}.t_{i}"


def ddl(i, out):
    cols = "".join(
        f"    c_{j} {random.choice(['bigint', 'string', 'double', 'decimal(18,2)', 'array<string>', 'map<string,string>'])},\n"
        for j in range(random.randint(3, 10))
    )
    part = random.random() < 0.4
    out.append(
        f"CREATE TABLE IF NOT EXISTS {tname(i)} (\n{cols}    tag string\n)"
        + (" PARTITIONED BY (dt string, hh string)" if part else "")
        + " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS ORC;\n"
    )


def insert(i, out):
    tgt = tname((i * 7 + 3) % NTABLES)
    s1, s2, s3 = (tname((i * 11 + 5) % NTABLES), tname((i * 13 + 7) % NTABLES), tname((i * 17 + 9) % NTABLES))
    kind = i % 6
    if kind == 0:
        out.append(
            f"-- 第 {i} 个任务：汇总流水\n"
            f"INSERT OVERWRITE TABLE {tgt} PARTITION(dt='${{bizdate}}', hh)\n"
            f"WITH w1 AS (SELECT a.c_0, a.c_1, b.c_2 FROM {s1} a JOIN {s2} b ON a.c_0=b.c_0),\n"
            f"     w2 AS (SELECT c_0, max(c_1) AS mx FROM w1 GROUP BY c_0)\n"
            f"SELECT w1.c_0, w1.c_1, w2.mx, c.c_3,\n"
            f"       row_number() OVER (PARTITION BY w1.c_1 ORDER BY c.c_3 DESC) AS rn,\n"
            f"       CASE WHEN w1.c_2 > 100 THEN 'big' WHEN w1.c_2 > 10 THEN 'mid' ELSE 'small;--x' END AS lvl\n"
            f"FROM w1 JOIN w2 ON w1.c_0=w2.c_0 LEFT JOIN {s3} c ON w1.c_0=c.c_0\n"
            f"WHERE w1.c_2 IS NOT NULL DISTRIBUTE BY w1.c_1 SORT BY w1.c_0;\n"
        )
    elif kind == 1:
        out.append(
            f"INSERT INTO TABLE {tgt}\n"
            f"SELECT /*+ MAPJOIN(b) */ a.c_0, a.c_1, sum(coalesce(b.c_2,0)) AS total,\n"
            f"       collect_set(concat(a.c_3, ':', b.c_4)) AS tags\n"
            f"FROM {s1} a JOIN {s2} b ON a.c_0=b.c_0\n"
            f"WHERE a.dt='${{hiveconf:dt}}' AND b.c_5 IN (SELECT c_5 FROM {s3} WHERE dt='x')\n"
            f"GROUP BY a.c_0, a.c_1, a.c_3 HAVING sum(b.c_2) > 0;\n"
        )
    elif kind == 2:
        out.append(
            f"FROM {s1} s\n"
            f"INSERT OVERWRITE TABLE {tgt} PARTITION(dt='p1') SELECT s.c_0, s.c_1 WHERE s.c_2>0\n"
            f"INSERT INTO TABLE {tname((i*19+11) % NTABLES)} SELECT s.c_0, upper(s.c_3) WHERE s.c_4 IS NOT NULL\n"
            f"INSERT OVERWRITE TABLE {tname((i*23+13) % NTABLES)} SELECT s.c_0, s.c_5;\n"
        )
    elif kind == 3:
        out.append(
            f"INSERT OVERWRITE TABLE {tgt}\n"
            f"SELECT a.c_0, lv.kv, lv.pos\n"
            f"FROM {s1} a\n"
            f"LATERAL VIEW posexplode(split(a.c_6, ',')) lv AS pos, kv\n"
            f"LATERAL VIEW OUTER explode(nvl(a.c_7, array())) lv2 AS e2\n"
            f"WHERE a.c_0 NOT IN (SELECT c_0 FROM {s2});\n"
        )
    elif kind == 4:
        out.append(
            f"CREATE TABLE {tgt} AS\n"
            f"SELECT x.*, y.c_8, dense_rank() OVER (PARTITION BY x.c_9 ORDER BY y.c_8) rk\n"
            f"FROM (SELECT * FROM {s1} WHERE dt>='0') x\n"
            f"JOIN (SELECT c_0, c_8 FROM {s2} UNION ALL SELECT c_0, c_8 FROM {s3}) y\n"
            f"ON x.c_0=y.c_0;\n"
        )
    else:
        out.append(
            f"/* 注释里有分号; 还有引号 'xx;yy' -- 和更多 */\n"
            f"INSERT OVERWRITE DIRECTORY '/user/hive/out_{i}'\n"
            f"SELECT a.c_0, nvl(b.c_1, '默认值') FROM {s1} a FULL OUTER JOIN {s2} b ON a.c_0=b.c_0;\n"
        )


def junk(i, out):
    out.append(
        random.choice(
            [
                f"MSCK REPAIR TABLE {tname(i % NTABLES)};\n",
                f"ANALYZE TABLE {tname(i % NTABLES)} PARTITION(dt='x') COMPUTE STATISTICS FOR COLUMNS;\n",
                f"SET hive.exec.dynamic.partition.mode=nonstrict;\n",
                f"set hivevar:v_{i % 50}=val_{i};\n",
                f"LOAD DATA INPATH '/data/f_{i}' INTO TABLE {tname(i % NTABLES)} PARTITION(dt='x');\n",
                f"this is broken sql statement number {i}\n",
                f"SHOW PARTITIONS {tname(i % NTABLES)};\n",
                f"SELECT c_0, c_1 FROM {tname(i % NTABLES)} WHERE dt='${{env}}' LIMIT 10;\n",
            ]
        )
    )


def main():
    target_mb = float(sys.argv[1]) if len(sys.argv) > 1 else 10.0
    outfile = sys.argv[2] if len(sys.argv) > 2 else "big.sql"
    target = int(target_mb * 1024 * 1024)
    out = []
    out.append("-- 自动生成的 Hive 脚本\nSET hive.exec.parallel=true;\nUSE ods;\n")
    # 先铺一些 DDL 提供 catalog
    for i in range(200):
        ddl(i * 10, out)
    i = 0
    size = sum(len(s.encode()) for s in out)
    while size < target:
        if i % 10 == 9:
            junk(i, out)
        else:
            insert(i, out)
        i += 1
        size = sum(len(s.encode()) for s in out[-3:])
        # 避免每轮全量统计
        if i % 50 == 0:
            size = sum(len(s.encode()) for s in out)
    data = "".join(out)
    with open(outfile, "w", encoding="utf-8") as f:
        f.write(data)
    print(f"wrote {outfile}: {len(data.encode('utf-8'))/1024/1024:.2f} MB, {i} statements generated")


if __name__ == "__main__":
    main()
