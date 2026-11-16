# hive_sql_parse / hive-lineage

Hive / Spark SQL 血缘分析工具（Rust）。输入一个或多个 SQL 文件（按顺序视作同一脚本），输出表级 + 列级血缘 JSON；可将带任务标识（`dag.task`）的解析结果**纯追加写入 PostgreSQL**，并提供跨任务多跳的表级 / 任务级 / 字段级全局血缘查询与可视化 Web 服务。

## 架构总览

```
                        ┌──────────────────────── 输入 ────────────────────────┐
                        │  *.sql 文件(CLI)  │  multipart/文本上传  │  Rust 库调用 │
                        └──────────────────────────┬───────────────────────────┘
                                                   ▼
   ┌──────────────────────────── 解析流水线（src/）───────────────────────────┐
   │ decode(UTF-8|GBK) → lexer 语句切分 → preprocess 分类/变量替换/多路改写     │
   │   → rayon 并行 sqlparser 解析(Hive|Spark) → analyzer 顺序血缘分析          │
   │   → 失败语句 fallback token 兜底                                          │
   └──────────────────────────┬──────────────────────────────────────────────┘
                              ▼
                    LineageResult（表/列血缘 + job_name）
            ┌─────────────────┼───────────────────────────────┐
            ▼                 ▼                               ▼
       stdout / -o JSON   WebUI 会话内存图              extract_batch_payload
      （单脚本血缘）      （本次分析可视化）            （dag.task 拆分·临时表消解）
                                                              │ 纯 INSERT 追加
                                                              ▼
                    ┌───────────────── PostgreSQL（Append-Only）─────────────┐
                    │ hl_job_batches  批次元数据 (job_name, batch_id, 统计)    │
                    │ hl_job_tables   任务↔表读写角色                          │
                    │ hl_table_edges  表级血缘边                               │
                    │ hl_column_edges 字段级血缘边                             │
                    └──────────┬───────────────────────────┬─────────────────┘
                 WITH RECURSIVE│                           │batch_id 水位线增量同步
              （CLI 无状态深查）│                           ▼
                               │              HotGraphIndex（serve 内存热图）
                               │              u32 符号驻留 + 紧凑邻接表
                               ▼                           ▼
              ┌───────────────────── 四类全局血缘查询（db/query.rs）───────────┐
              │ ① 库.表名 → 表级上下游加工血缘                                │
              │ ② 库.表名 → 产出/消费任务级血缘                               │
              │ ③ dag.task → 上下游任务链路(+chains)                          │
              │ ④ 库.表.字段 → 字段级血缘链路(+chains)                        │
              └──────────────────────────┬───────────────────────────────────┘
                                         ▼
                        CLI JSON 输出  │  WebUI 交互血缘图（graph 图层）
```

两条查询路径：**服务端**默认走内存热图（启动时按各任务最新批次建索引，每 ≥500ms 按 `batch_id` 水位线增量合并新批次），30~50+ 跳遍历亚毫秒级；**CLI** 直连 PG 走 `WITH RECURSIVE` 递归 + 复合索引，无状态、可查全量历史。

## 特性

- **SQL 方言可选**：`hive`（默认）或 `spark`，分别基于 sqlparser 0.63 的 `HiveDialect` / `SparkSqlDialect` 原生方言解析
- **表级血缘**：每个写入目标的上游物理表集合（CTE / 子查询 / 临时表已消解，不产生虚拟节点）
- **列级血缘**：`direct`（直接映射）/ `derived`（表达式派生）/ `aggregate`（聚合）三种边
- **追加型血缘入库（PostgreSQL）**：解析结果按 `(job_name, batch_id)` 纯 `INSERT` 追加落库，自动建表 + 复合索引；同名任务多次追加后默认按最新批次生效，可切换全量历史口径
- **跨任务全局血缘查询**：`库名.表名` 表级上下游加工血缘、表关联任务级血缘、`dag.task` 任务上下游链路、`库名.表名.字段名` 字段级血缘链路
- **深血缘性能**：服务端内置 Rust 内存热图索引（`u32` 符号驻留 + 紧凑邻接表，按 `batch_id` 水位线毫秒级增量刷新），30~50+ 跳遍历亚毫秒级；CLI 无状态查询走 PG `WITH RECURSIVE` 递归
- **安全落库**：入库时对脚本内 `TEMPORARY` 临时表做传递消解折叠为物理表对物理表血缘，避免跨任务伪血缘；`job_name` 缺失/空白明确拒绝；连接串密码在日志与 API 中统一脱敏
- **语句覆盖**：
  - `INSERT`（OVERWRITE / INTO / 静态+动态 PARTITION / 显式列清单 / 位置映射）
  - 多路 `FROM src INSERT ... [INSERT ...]`（自动改写后逐路分析）
  - `CREATE TABLE`（含 PARTITIONED BY / LIKE / CLONE / TEMPORARY）、CTAS、`CREATE [MATERIALIZED|TEMPORARY|OR REPLACE] VIEW`
  - `MERGE INTO`（UPDATE/DELETE/INSERT/WHEN 谓词、通配 `SET *`/`INSERT *`）
  - `UPDATE`（含 FROM / JOIN / tuple 赋值）
  - `INSERT OVERWRITE [LOCAL] DIRECTORY` → `dir:<path>` 目标
  - `VALUES`、UNION/EXCEPT/INTERSECT、`WITH`、`LATERAL VIEW`（explode/posexplode/OUTER）、窗口函数（含 `WINDOW w AS` 命名窗口）
  - 独立 `SELECT` 自动映射为虚拟结果表 `query_result[_n]`（`--analyze-select` 控制）
- **`SET` 变量**：`set [hivevar:|hiveconf:]k=v`，`${...}` 替换；未定义变量以占位符保留并在输出还原原名
- **`USE db`** 与 `--default-db`：未限定表名的库解析
- **编码**：UTF-8 校验失败后按 GBK 解码
- **容错**：单条语句解析失败不影响其它语句；失败时降级为 token 级兜底提取（表级血缘 + DDL 列）
- **性能**：mimalloc 分配器、rayon 并行解析三级流水线（解析→顺序分析→并行释放）、FxHash、SmallVec、列名符号驻留、零拷贝借用

## 构建

```bash
cargo build --release          # 产出 target/release/hive-lineage（约 4MB）
```

release 配置已按产物体积调优（`opt-level="z"` + fat LTO + `codegen-units=1` + 符号裁剪 + `panic=abort`）；如需调试符号或更快编译，自行调整 `Cargo.toml` 的 `[profile.release]`。

## 使用

### 快速上手（端到端跑通落库 + 血缘查询）

```bash
# 1. 本地起一个 PostgreSQL（任选其一）
docker run -d --name hl-pg -e POSTGRES_PASSWORD=dev -e POSTGRES_DB=lineage -p 5432:5432 postgres:15
export HL_DB_URL=postgres://postgres:dev@127.0.0.1:5432/lineage

# 2. 三个任务按 DAG 顺序解析落库（job_name 即调度系统的 dag.task）
cat > j1.sql <<'SQL'
insert overwrite table dwd.order_detail
select o.id, o.amt, c.region from ods.orders o join ods.customers c on o.cid = c.id;
SQL
cat > j2.sql <<'SQL'
insert overwrite table dws.region_daily
select region, sum(amt) as gmv from dwd.order_detail group by region;
SQL
hive-lineage analyze j1.sql --job-name etl.j1_dwd --db-url "$HL_DB_URL"   # 首次连接自动建表
hive-lineage analyze j2.sql --job-name etl.j2_dws --db-url "$HL_DB_URL"

# 3. 四类血缘查询
hive-lineage query table  dwd.order_detail --direction both              # 表级上下游
hive-lineage query task   etl.j2_dws                                     # 任务链路
hive-lineage query column dws.region_daily.gmv --direction upstream      # 字段级链路

# 4. WebUI：serve 带库启动 → 浏览器打开 → 左侧"血缘查询"面板
hive-lineage serve -p 3000 --db-url "$HL_DB_URL"
```

无 PG 环境想先体验：连接串用 `memory://`（进程级内存库，**仅进程内有效**，退出即丢），例如 `hive-lineage serve -p 3000 --db-url memory://`。

### CLI

```bash
# 分析 SQL 文件（可多个），血缘 JSON 输出到 stdout
hive-lineage analyze a.sql b.sql

hive-lineage analyze a.sql -o out.json --pretty   # 写文件 + 美化
hive-lineage analyze a.sql --default-db ods       # 未限定名默认库
hive-lineage analyze a.sql --analyze-select yes   # auto(默认)/yes/no
hive-lineage analyze a.sql --dialect spark        # SQL 方言：hive(默认)/spark

# 携带任务标识解析并追加落库（需 PostgreSQL 连接串，也可读环境变量 HL_DB_URL）
hive-lineage analyze a.sql --job-name dw_dag.task_order_detail \
    --db-url postgres://user:pass@127.0.0.1:5432/lineage
```

### 血缘入库（PostgreSQL）

持久层为四张**纯追加写**表（仅 `INSERT`，绝不 `UPDATE`/`DELETE`/`TRUNCATE`），以 `(job_name, batch_id)` 为批次键；连接时自动执行 `CREATE TABLE IF NOT EXISTS` 与索引（`--no-auto-create-table` 可关闭）。

| 表 | 说明 |
|----|------|
| `hl_job_batches` | 任务解析批次：`job_name`、`dag_name`、`task_name`、方言、默认库、语句/表/边统计、`ingested_at` |
| `hl_job_tables` | 任务↔表读写角色：`is_source`/`is_target`、列清单、表类型 |
| `hl_table_edges` | 表级加工血缘边（含 `sql_preview` 与关联语句下标） |
| `hl_column_edges` | 字段级血缘边（`direct`/`derived`/`aggregate`） |

批次语义（追加即历史，查询默认取最新）：

```
job_name = etl.dws_daily
  │
  ├─ batch_id=…001  (首次解析落库) ─┐
  ├─ batch_id=…002  (任务重跑)    ─┤ 全部保留，绝不改写旧行
  └─ batch_id=…003  (再次重跑)    ─┘
        │
        ├─ 默认查询口径 latest_batch：仅 batch_id=…003 的血缘生效
        └─ --all-history / all_history=true：合并 …001+…002+…003 去重后查询
```

`batch_id` 为进程内单调递增的微秒级时间戳；同一 `job_name` 每次落库生成新批次，跨任务依赖边始终按"各任务最新批次"拼接，因此重跑任务不会污染下游血缘口径。

```bash
# 输出完整建表与索引 DDL（等价于 sql/postgres_ddl.sql）
hive-lineage ddl > ddl.sql && psql -f ddl.sql postgres://user:pass@host/lineage
```

### 全局血缘查询（CLI）

```bash
export HL_DB_URL=postgres://user:pass@127.0.0.1:5432/lineage

# 查询 1：输入 库名.表名 → 表级上下游加工血缘（边附带加工任务与 SQL 摘要）
hive-lineage query table dwd.order_detail --direction both --hops 5 --pretty

# 查询 2：输入 库名.表名 → 任务级上下游血缘（产出/消费任务 + 跨任务链路）
hive-lineage query table-tasks dwd.order_detail --hops 3

# 查询 3：按任务名（dag.task）搜索上下游任务链路与媒介表
hive-lineage query task dw_dag.task_order_detail --direction downstream

# 查询 4：按 库名.表名.字段名 输出字段级血缘链路（chains 为端到端字段路径）
hive-lineage query column dws.region_daily.gmv --hops 0        # 0=不限跳数
hive-lineage query column --db dws --table region_daily --column gmv --all-history
```

公共参数：`--direction both|upstream|downstream`（默认 both）、`--hops N`（默认 5，`0` 为不限跳安全上限 64）、`--all-history`、`--pretty`、`--db-url`（缺省读 `HL_DB_URL`）。输出为结构化 JSON（含 WebUI 可直接渲染的 `graph` 图层）。

### Web 服务（可视化 + 血缘查询）

```bash
hive-lineage serve --host 127.0.0.1 -p 3000 [preload.sql ...] [--default-db ods]
hive-lineage serve --dialect spark -p 3000 preload.sql   # 服务默认方言设为 spark
hive-lineage serve -p 3000 --db-url postgres://user:pass@127.0.0.1:5432/lineage  # 启动即连接血缘库
# 浏览器打开 http://127.0.0.1:3000 ，可上传大文件交互分析
```

带 `--db-url` 启动后服务端启用内存热图索引：启动时加载各任务最新批次，此后按 `batch_id` 水位线自动增量合并新入库批次（≥500ms 触发一次检查）。也可在页面右上角「数据库配置」弹窗运行时连接/切换 PostgreSQL（或 `memory://` 内置内存库快速体验）。上传面板与 SQL 编辑器均可填 `job_name` 并勾选「同时追加落库」；左侧「血缘查询」面板支持表级/表任务级/任务链路/字段血缘四种模式、方向与跳数控制、最新批次与全量历史切换，结果直接渲染为可交互血缘图（任务节点以 `dag.task` 卡片呈现，边标注加工任务或媒介表）。

### HTTP API

分析请求返回 `{"session": "sN", "initial": {...}, "counts": {...}}`，后续接口均以会话 id 为路径段。会话保留最近 32 个（`MAX_SESSIONS`）。

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 内嵌可视化页面 |
| GET | `/api/health` | `{"ok": true}` |
| POST | `/api/analyze` | multipart 上传：文件字段 + `default_db` + `analyze_select` + `dialect` |
| POST | `/api/analyze_text` | JSON `{sql, name?, default_db?, analyze_select?, dialect?}`（`auto/yes/no`） |
| GET | `/api/s/{id}/overview` | stats / files / dbs / stmt_kinds / counts |
| GET | `/api/s/{id}/tables` | 表列表；参数 `q` `role`(target/source/mid/result) `db` `offset` `limit`(默认80,最大500) |
| GET | `/api/s/{id}/table/{tid}` | 表详情 + 直接上/下游 |
| GET | `/api/s/{id}/table_by_name/{name}` | 按 `db.tbl` 名查表 |
| GET | `/api/s/{id}/subgraph` | 参数 `table`(必填) `hops`(默认1) `direction`(up/down/both)；或 `col` 触发列级全链路 |
| GET | `/api/s/{id}/statements` | 参数 `ids`(逗号分隔直取全文) 或 `status` `kind` `offset` `limit`(默认50,最大200) 分页返回预览 |
| GET | `/api/s/{id}/names` | `ids=0,1` → id→表名映射 |
| GET | `/api/s/{id}/export` | 下载完整 `LineageResult` JSON |
| POST | `/api/s/{id}/save` | 将当前会话追加落库：JSON `{job_name}` → `{saved: {...}}` |
| GET | `/api/db/status` | 数据库连接状态：`configured`/`ok`/`backend`/`masked_url`（已脱敏）/`hot_index` |
| POST | `/api/db/config` | 运行时配置连接：JSON `{url, auto_create_table?, use_hot_index?}`（`postgres://...` 或 `memory://`） |
| GET | `/api/db/ddl` | `{ddl}`：PostgreSQL 建表与索引 DDL |
| GET | `/api/db/jobs` | 任务搜索：`q`、`limit`、`all_history` → `{items: [{job_name, dag_name, task_name, latest_batch_id, source_tables, target_tables}]}` |
| GET | `/api/db/tables` | 表搜索：`q`、`limit`、`all_history` → `{items: [{full_table_name, producer_jobs, consumer_jobs}]}` |
| GET | `/api/db/lineage/table` | 表级血缘：`table`、`direction`、`hops`、`all_history` → 上下游表 + 边 + `graph` |
| GET | `/api/db/lineage/table_tasks` | 表的任务级血缘：`table`、`direction`、`hops`、`all_history` → 产出/消费任务 + 任务链路 + `graph` |
| GET | `/api/db/lineage/task` | 任务链路：`job`（支持模糊）、`direction`、`hops`、`all_history` → 上下游任务 + `chains` + `graph` |
| GET | `/api/db/lineage/column` | 字段血缘：`db`+`table`+`column` 或 `field=库名.表名.字段名`、`direction`、`hops`、`all_history` → 上下游 `库名.表名.字段名` + `chains` + `graph` |

分析接口的 `dialect` 字段（`hive`/`spark`）省略时沿用 `serve --dialect` 启动默认值，显式传入则按请求覆盖；非法值返回 400。网页上传区与 SQL 编辑器各有一个方言选择框（默认"服务默认"，即不发送该字段）。

`POST /api/analyze`（multipart）与 `POST /api/analyze_text`（JSON）新增可选字段：`job_name`（任务标识，如 `dag.task`）与 `save_db`（true 时解析后追加落库，缺 `job_name` 或未配置数据库返回 400）；响应含 `job_name` 与 `saved` 批次摘要。

### 作为库

```rust
use hive_sql_parse::{analyze, InputFile, Options, SqlDialect};

let res = analyze(
    vec![InputFile { name: "a.sql".into(), bytes: std::fs::read("a.sql")? }],
    &Options {
        dialect: SqlDialect::Spark,
        default_db: Some("ods".into()),
        analyze_select: None, // None=auto / Some(true)=yes / Some(false)=no
        job_name: Some("dw_dag.task_x".into()), // 可选任务标识，写入 LineageResult.job_name
    },
);
// res.tables / res.table_edges / res.column_edges / res.statements
```

`analyze_with_sources` 额外返回解码后文本（服务端按语句偏移提取源码用）。`Options.dialect` 默认为 `SqlDialect::Hive`；已有的完整 `Options` 字面量需补 `dialect` 字段或改用 `..Options::default()`。

## 输出模型（model.rs）

`LineageResult`：

| 字段 | 说明 |
|------|------|
| `stats` | `files` `bytes` `statements` `parsed` `fallback` `failed` `skipped` `elapsed_ms` |
| `files` | `{name, encoding}`（`utf-8`/`gbk`） |
| `tables` | `{id, name, kind, is_source, is_target, columns[]}`；`kind`: `table`/`temp`/`view`/`directory`/`result` |
| `table_edges` | `{source, target, statements[]}`（值为表 id 与语句下标） |
| `column_edges` | `{source_table, source_column, target_table, target_column, kind, statements[]}`；`kind`: `direct`/`derived`/`aggregate` |
| `statements` | `{index, file, line, start, end, kind, status, error, targets[], sources[]}`；`start/end` 为解码文本内字节偏移 |

`statements[].kind`：`insert`/`ctas`/`create_table`/`create_view`/`merge`/`update`/`query`/`set`/`use`/`other`；
`status`：`ok`/`fallback`（AST 失败但兜底提取到血缘）/`error`/`skipped`（set/use/skip 语句）。

## 工作原理

```
bytes → decode(UTF-8|GBK)
      → lexer::split_statements        字节级切分（注释/字符串/转义感知，记录行号与偏移）
      → preprocess::classify           首 token 短路分类；SET/USE/变量替换；多路 INSERT 改写
      → rayon 并行 parse（sqlparser HiveDialect/SparkSqlDialect 按所选方言，chunk=256）
      → analyzer（主线程顺序执行）      catalog 随 DDL 演进；Scope 内做列解析与歧义消解
      → 解析失败 → fallback::extract    token 级提取目标/源表与 DDL 列
      → LineageResult（serde JSON）
```

关键语义：

- **catalog 演进**：DDL/CTAS/VIEW 登记列目录，`SELECT *` 据此展开；血缘中遇到的列也补入
- **歧义消解**：未限定列名在多个已知列集合的表间重名时丢弃；唯一未知列集合的表兜底承接
- **ORDER BY（查询顶层）不产生血缘**；`DISTRIBUTE/SORT/CLUSTER BY` 参与列登记
- **虚拟表**：`query_result`（仅一条独立查询）或 `query_result_{n}`；`dir:<path>`（DIRECTORY 目标）

## 环境变量

| 变量 | 说明 |
|------|------|
| `HL_THREADS` | 解析线程池大小（默认 rayon 全局池） |
| `HL_PROFILE` | 打印各阶段耗时到 stderr |
| `HL_PARSE_ONLY` | 只测解析阶段，跳过血缘分析 |
| `HL_DB_URL` | PostgreSQL 连接串（`analyze --db-url`、`serve --db-url`、`query --db-url` 缺省值） |

## 测试与覆盖率

```bash
cargo test                      # lib 单测（analyzer/server/lexer/catalog/preprocess）+ 集成测试
./scripts/coverage.sh           # 一键生成覆盖率报告（需 llvm-tools-preview）
```

覆盖率复现（`-Cinstrument-coverage` + `llvm-cov`，报告见 stdout）：

```bash
rustup component add llvm-tools-preview   # 如未安装
./scripts/coverage.sh                     # 自动编译插桩、跑测试、合并并输出报告
```

当前基线（行覆盖口径，测试含 `tests/db_lineage.rs` 血缘入库集成套件）：

| 文件 | 行覆盖 | 备注 |
|------|--------|------|
| lib.rs | ~99% | |
| analyzer.rs | ~91% | 未覆盖多为非 Hive 方言可达的 AST 分支（Pivot/MatchRecognize/JsonTable 等的部分子臂） |
| fallback.rs | ~97% | |
| preprocess.rs | ~98% | |
| catalog.rs | ~98% | |
| lexer.rs | ~98% | |
| server.rs | ~98% | 含全部路由 handler、图遍历、错误路径与 `run()` 启动路径 |
| **总计** | **~95%** | src/ 全部源码口径（含 `main.rs`/`bench_parse.rs` 二进制壳）；接入前为 ~52% |

未覆盖区主要是：其他 SQL 方言才可产生的 AST 分支（analyzer.rs 约 184 行）、个别防御性死代码臂（如仅特定 AST 形态可达的 `continue`/`return` 兜底）。非 Hive 方言不可达的变体已通过内部 `#[cfg(test)]` 测试用 GenericDialect/手工构造 AST 尽可能覆盖。

## 性能工具

```bash
python3 scripts/gen_big_sql.py [大小MB=10] [输出=big.sql]   # 生成压测脚本
cargo build --release && HL_PROFILE=1 ./target/release/hive-lineage analyze big.sql
cargo run --bin bench_parse                                  # sqlparser 裸解析基线（读 /tmp/hl/simple.sql）
```

## 注意事项

**落库与批次**

- **必须携带 `job_name`**：`analyze --db-url`、`POST /api/s/{id}/save`、analyze 接口的 `save_db=true` 在缺失/空白 `job_name` 时一律拒绝（CLI 报错退出、HTTP 返回 400）。`job_name` 建议用调度系统的 `dag.task` 全名，入库时按第一个 `.` 拆分为 `dag_name`/`task_name`（无 `.` 时 `dag_name` 为空串）。
- **纯追加写**：只 `INSERT`，任何历史批次不会被改写；重复解析同一任务 = 新增批次而非覆盖。默认查询口径为**各任务最新批次**；`--all-history` 才合并历史全量——排查"血缘似乎没变/变了"类问题先确认口径。
- **自动建表需要建表权限**：`CREATE TABLE IF NOT EXISTS` + 索引在连接时执行，连接账号需有目标 schema 的 DDL 权限；无权限时用 `hive-lineage ddl` 导出脚本由 DBA 预建，并加 `--no-auto-create-table`（或 API `auto_create_table=false`）。
- **临时表不落库**：脚本内 `CREATE TEMPORARY TABLE`（`kind="temp"`）在入库抽取阶段被传递消解，折叠为"物理表→物理表"直接血缘，防止不同任务同名临时表产生跨任务伪血缘——所以落库后的表清单中看不到 temp 节点，属预期行为。
- **`batch_id` 是进程内单调微秒时间戳**：多写入端并发落库时依赖时钟单调性，常规调度场景（各任务按 DAG 顺序跑）无影响；极端同微秒并发写入同一任务时后到批次顺序不保证。

**查询**

- **`--hops 0` 不是真的无限**：映射为安全上限 64 跳，且遍历内置 visited 环路保护（数据回环不会死循环）。
- **`memory://` 是进程级内存库**：仅当次进程有效，不能跨 CLI 调用共享——`analyze --db-url memory://` 落库后另起 `query --db-url memory://` 查不到（空库）。跨进程共享请用 `postgres://`；`memory://` 定位是测试与 serve 演示。
- **CLI 与服务端是两条独立路径**：CLI 直连 PG（`WITH RECURSIVE`），结果即时反映最新落库；serve 走内存热图，新批次最长滞后约一个水位线刷新周期（≥500ms 轮询）。
- **任务模糊搜索**：`query task` 的 `job` 参数支持子串匹配，命中多个任务时按候选列表处理（响应含 `candidates`）；需要精确锚定时传完整 `dag.task`。

**解析与安全**

- 两种方言均以 `sqlparser` 0.63 相应 `Dialect` 能解析为界（并非全量 Hive/Spark 语法兼容）；方言外语法走 fallback 时只有表级血缘
- 不做语义校验（表/列是否真实存在）；catalog 仅随脚本内 DDL 演进
- `SET`/`USE` 在含 `${}` 变量的脚本中按顺序处理，跨文件延续
- 连接串密码在日志与 `/api/db/status` 中统一脱敏为 `***`，但 `--db-url` 明文出现在命令行时会留存在 shell 历史——生产环境建议用 `HL_DB_URL` 环境变量
- 环境变量为进程级开关，测试并发时不可安全修改

**服务**

- 会话内存上限 32 个（`MAX_SESSIONS`），超限淘汰最旧会话；会话图不落盘，服务重启后需重新上传或从血缘库查询。
- release 构建 `panic=abort`：极端情况下未捕获 panic 会直接终止进程（换取更小产物），如需 unwind 兜底请自行调整 `[profile.release]`。
