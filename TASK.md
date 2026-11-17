# 上下文
文件名：TASK.md
创建于：2026-11-14
创建者：Devin (AI)
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
1. （第一轮，已完成）检查测试覆盖率和编写文档。
2. （第二轮，本轮）修复 cargo clippy 报警；测试覆盖率提升到 95% 以上。

# 项目概述
hive_sql_parse：Hive / 数仓 SQL 血缘分析工具（Rust）。
- 库：`analyze(files, opts) -> LineageResult`，多文件按顺序视作同一脚本。
- CLI：`hive-lineage analyze|serve`（clap）。
- Web：axum 服务 + 内嵌 index.html 可视化前端。
- 流水线：lexer（语句切分/tokenize）→ preprocess（分类/变量替换/多路 INSERT 改写）→ sqlparser(HiveDialect) 并行解析 → analyzer（顺序血缘分析）→ model（serde JSON 输出）；解析失败走 fallback token 级兜底。
- 测试：3 个 lexer 单测 + 16 个集成测试（tests/lineage.rs），全部通过。

---
*以下部分由 AI 在协议执行过程中维护*
---

# 分析 (由 RESEARCH 模式填充)

## 覆盖率实测（-Cinstrument-coverage + llvm-cov，2026-11-14）

| 文件 | 行覆盖 | 区域覆盖 | 主要未覆盖区域 |
|------|--------|----------|----------------|
| lib.rs | 89.75% | 90.79% | 变量替换顺序路径、HL_PARSE_ONLY 分支 |
| preprocess.rs | 86.11% | 88.89% | rewrite_multi_insert 边界分支 |
| catalog.rs | 81.36% | 81.19% | copy_from（LIKE/CLONE 拷贝列目录） |
| fallback.rs | 69.18% | 67.40% | DIRECTORY 目标、MERGE/UPDATE 兜底、LIKE/CLONE |
| lexer.rs | 66.91% | 71.32% | 函数全部执行，部分边界分支（未闭合注释/串等） |
| analyzer.rs | 57.61% | 52.22% | expr_srcs 表达式分支（~1509-1777）、add_lateral 变体（~1209-1264）、do_merge 边缘（~700-798）、select 扩展（~992-1070）、resolve_unq/resolve_q（~1346-1376）、join_constraint（~1796-1810） |
| server.rs | 0.00% | 0.00% | 整个 Web 层（826 行）：SessionGraph 索引 + 全部 HTTP handler |

**总计：行覆盖 51.95%，区域覆盖 49.16%，函数执行 52.96%（287 中 135 未执行）**

main.rs / bench_parse.rs 为二进制目标，未计入库覆盖率。

## 测试现状
- tests/lineage.rs：16 个端到端用例，覆盖 INSERT（分区/多路/显式列）、CTE+UNION、LATERAL VIEW、SELECT * 展开、位置映射、变量替换、USE 限定、GBK、CTAS/VIEW catalog、错误隔离、MERGE、UPDATE、独立 SELECT 自动/显式模式、WITH...MERGE。
- src/lexer.rs 内部 3 个单测。
- 无 server API 测试（axum 服务完全未测）。

## 文档现状
- 无 README，无 docs/。
- 代码内注释齐全（中文），模块头有 //! 说明。

## 覆盖率复现方法
```bash
RUSTFLAGS="-Cinstrument-coverage" LLVM_PROFILE_FILE="/tmp/cov-%p-%m.profraw" cargo test
llvm-profdata merge -sparse /tmp/cov-*.profraw -o cov.profdata
llvm-cov report --instr-profile=cov.profdata --object target/debug/deps/hive_sql_parse-* --object target/debug/deps/lineage-* --sources src/
```
llvm-profdata/llvm-cov 位于 `~/.rustup/toolchains/stable-*/lib/rustlib/x86_64-unknown-linux-gnu/bin/`（需 `rustup component add llvm-tools-preview`）。

# 提议的解决方案 (由 INNOVATE 模式填充)
用户选择方案 B：文档 + 补测试（目标 ~70%+）。
- server.rs：在 `#[cfg(test)] mod tests` 中直接调用 async handler（`run_analysis`/`store_session`/`api_*`），不走网络、不重构 `run()`、不加依赖；multipart 用 `Multipart::from_request` 手工构造。
- analyzer.rs：表达式分支用 SELECT 投影覆盖（产生 derived 列边可断言）；不可解析变体（MatchAgainst/Overlay/Prior/Convert 等）跳过。
- fallback.rs：用语法错误语句触发（截断 UPDATE/MERGE、LOCAL DIRECTORY、LIKE/CLONE）。
- 新测试文件 tests/lineage_ext.rs，沿用现有断言助手风格。

# 实施计划 (由 PLAN 模式生成)
实施检查清单：
1. 更新 TASK.md（提议方案 + 本计划）
2. 新建 tests/lineage_ext.rs（analyzer 表达式/集合/merge/update/lateral/resolve、fallback、preprocess、lib 多文件）
3. src/server.rs 加 #[cfg(test)] mod tests（SessionGraph::build + 全部 handler + 错误路径 + 淘汰 + multipart）
4. cargo test 全绿；重跑 llvm-cov 对比
5. 新建 README.md
6. 新建 scripts/coverage.sh
7. cargo fmt + clippy；更新 TASK.md 进度

# 当前执行步骤 (由 EXECUTE 模式在开始执行某步骤时更新)
全部检查清单项目已执行。

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
*   2026-11-14
    *   步骤：1. 更新 TASK.md（提议方案 + 计划）
    *   修改：TASK.md
    *   更改摘要：填入方案 B 与 7 项检查清单
    *   原因：执行计划步骤 1
    *   阻碍：无
    *   用户确认状态：待确认
*   2026-11-14
    *   步骤：2. 新建 tests/lineage_ext.rs
    *   修改：tests/lineage_ext.rs（30 个用例）
    *   更改摘要：覆盖 expr_srcs 几乎全部 Hive 可解析分支、集合/窗口、merge/update 变体、lateral、resolve 歧义、fallback（目录/截断 DML/LIKE/CLONE/CTE 排除）、preprocess 边界、多文件、lexer 边界
    *   已报告微小偏差修正：
        - `ods.s.n` 三段限定名边 kind 为 direct（计划写 derived 假设）
        - `w.*` 未知限定符只置 unknown、不产生 * 列边（断言改为表边存在+无列边）
        - `GROUPING SETS` 需 `GROUP BY a,b GROUPING SETS(...)` 形式
        - 顶层 ORDER BY 不登记列（y 不入 catalog，设计行为，断言固化）
        - `!cmd` 与下一条语句需 `;` 分隔
        - `MAP('k': v)`/`MAP {k:v}` 不可解析 → 改用 `MAP('k', v)` 函数式
        - UNNEST/explode 在 Hive 方言下解析为函数名表（TableFactor::Table + Function name part），不产生特殊 factor 分支
        - COLLATE 透传 direct；第二条 merge 的源表已被第一条登记 id → 展开为 id 而非 *
    *   原因：执行计划步骤 2
    *   阻碍：member_of/match_against/prior/merge_row/expr_with_aliases 不可解析（跳过）；TableFactor::UNNEST/JsonTable/Pivot/MatchRecognize、Expr::Map/Dictionary/Convert-风格等分支非 Hive 可达
    *   用户确认状态：待确认
*   2026-11-14
    *   步骤：3. src/server.rs 加 #[cfg(test)] mod tests
    *   修改：src/server.rs 底部新增测试模块（4 个测试：select_flag/session_handlers/analyze_endpoints/session_eviction）
    *   更改摘要：直接调用 run_analysis/store_session/api_* handler；覆盖全部路由逻辑、列追踪、分页过滤、404/400、MAX_SESSIONS=32 淘汰、multipart 上传、大图(>60) initial focus 分支、index/health
    *   已报告微小偏差修正：
        - 空 SQL 不产生 400（files 恒非空）→ 400 改由无文件 multipart 触发
        - overview 表数断言 2→3（含 query_result）
        - clippy len_zero、rustfmt 格式修正
    *   原因：执行计划步骤 3
    *   阻碍：无
    *   用户确认状态：待确认
*   2026-11-14
    *   步骤：4. cargo test + llvm-cov 对比
    *   修改：无
    *   更改摘要：53 测试全绿；行覆盖 51.95% → 85.83%（库代码；含 bin 壳 84.40%）
    *   原因：执行计划步骤 4
    *   阻碍：无
    *   用户确认状态：待确认
*   2026-11-14
    *   步骤：5-6. README.md + scripts/coverage.sh
    *   修改：README.md（新建）、scripts/coverage.sh（新建，已验证端到端可跑）
    *   更改摘要：文档含特性/构建/CLI/HTTP API/库 API/输出模型/架构/环境变量/覆盖率基线/性能工具/已知限制
    *   原因：执行计划步骤 5-6
    *   阻碍：无
    *   用户确认状态：待确认
*   2026-11-14
    *   步骤：7. fmt + clippy
    *   修改：tests/lineage_ext.rs（rustfmt）、src/server.rs 测试模块格式
    *   更改摘要：新代码 fmt/clippy 干净；遗留 fmt 漂移与 2 个既有 clippy warning（analyzer.rs needless_range_loop、server.rs unnecessary_sort_by）未动
    *   原因：执行计划步骤 7
    *   阻碍：无
    *   用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（第一轮审查，第一轮任务已完成，见上方任务进度）

- 检查清单 1-7 全部完成；53 测试通过（cargo test）。
- 覆盖率复测：行覆盖 51.95% → 85.83%（库口径），server.rs 0% → 92%，analyzer.rs 58% → 79%。
- 新增文件：tests/lineage_ext.rs、README.md、scripts/coverage.sh、TASK.md；修改文件：src/server.rs（仅追加 cfg(test) 模块，生产代码零改动）。
- 全部偏差已在上文"任务进度"中报告（均为断言按实际语义固化，无实现侧变更）。
- 不可达分支（非 Hive 方言 AST 变体）与进程级环境变量路径按现状保留未覆盖，已在 README 已知限制中说明。
- 遗留未动：analyzer.rs/fallback.rs/bench_parse.rs/既有 server.rs 的 fmt 漂移、2 个既有 clippy warning。

---
# 第二轮：clippy 修复 + 覆盖率 ≥95%

## 分析（RESEARCH）
- clippy：仅 2 warning——analyzer.rs:901 needless_range_loop（`for i in 0..n` 索引 col_ids）、server.rs `dbs.sort_by` → sort_by_key(Reverse)。
- 当前覆盖 85.83%（库口径）/84.40%（含 bin 壳）；总 3988 行、缺 622 → 95% 需再覆盖 ≥424 行。
- 缺口与对策（lcov 行级清单已取证）：
  - analyzer.rs 290 行：Hive 可解析构造（merge 谓词/SET*/INSERT*/显式列、temp 表/PARTITIONED/LIKE/CLONE/CTAS列/视图列、DELETE/other 语句、动态分区、after_columns、JSON `->`、tuple、ARRAY[..]、INTERVAL、lambda、AT TIME ZONE、CONVERT、IS JSON/NORMALIZED/DISTINCT/UNKNOWN 系、PRIOR、`:=`、MEMBER OF()、a[0].b、OVERLAY、QUALIFY、FILTER/WITHIN GROUP、DICTIONARY/STRUCT、UNNEST/PIVOT/JSON_TABLE/TABLE(t)、posexplode、catalog 命中 rel、resolve 歧义/STAR 兜底、query 级 ORDER BY、(UPDATE)/VALUES set_expr、显式 `s.*`、GROUPING SETS 投影）~285 行 → tests/lineage_ext2.rs 走 analyze()。
  - 非 Hive 可解析 AST 臂（MATCH..AGAINST、ExprWithAliases、MAP{}、函数子查询参数、AsOf join、Expr::Map 等）→ analyzer.rs #[cfg(test)] 用 GenericDialect 解析（探针已验证可解析）或直接构造 AST 喂 Analyzer::analyze。
  - server.rs ~90 行：主体 run()（spawn+abort 覆盖 bind+serve await；子进程 serve -p <已占用端口> 走 bind 失败路径）+ 若干 handler 缺口（重测后补）。
  - main.rs 54 + bench_parse 11：tests/cli.rs 用 env!("CARGO_BIN_EXE_*") 子进程（LLVM_PROFILE_FILE %p 保证 profraw 可合并；bench_parse 硬编码 /tmp/hl/simple.sql 需先写文件）。
  - lexer.rs 69：first_token 非 Word 分支（语句以 '/`/${/数字开头）、tokenize 变量/符号边界 → lexer.rs tests + SQL 输入。
  - lib.rs 26：HL_THREADS/HL_PARSE_ONLY/HL_PROFILE → tests/env_paths.rs（单一测试函数避免进程内 env 竞态）。
  - preprocess 28 / fallback 28 / catalog 4：分类边界（`!cmd`、`USE`、SET 无 kv、`WITH...MERGE/UPDATE`、`FROM` 起手多插、rewrite 提前返回）、兜底（THEN UPDATE 跳过、view/temp、LIKE 链、using(、name 收尾）、`CREATE TABLE a LIKE 不存在`。

## 实施检查清单（第二轮）
1. 更新 TASK.md
2. 修复 2 个 clippy warning
3. 新建 tests/lineage_ext2.rs：Hive 可解析缺口 SQL 用例
4. analyzer.rs 底部 #[cfg(test)] mod tests：GenericDialect 解析 + 手工 AST 覆盖非 Hive 臂
5. lexer.rs tests 增补：first_token/tokenize 边界
6. 新建 tests/env_paths.rs：HL_* 环境变量路径（单测试函数）
7. 新建 tests/cli.rs：子进程 analyze（含 -o/--pretty/--analyze-select/缺文件错误）、serve 端口占用错误、bench_parse
8. server.rs tests 增补：run() spawn+abort、preload、端口占用错误路径；重测后补 handler 缺口
9. cargo test 全绿；llvm-cov 复测并迭代补漏至行覆盖 ≥95%（含 bin 壳口径）
10. cargo fmt + clippy 复核；README/TASK.md 收尾更新

## 任务进度（第二轮）
*   2026-11-14
    *   步骤：2. 修复 clippy warning
    *   修改：src/analyzer.rs（needless_range_loop → 迭代器）、src/server.rs（sort_by → sort_by_key Reverse）
    *   状态：完成，cargo clippy 干净
*   2026-11-14
    *   步骤：3-8. 覆盖率用例三轮增补
    *   修改：
        - tests/lineage_ext2.rs（30 用例）：merge 谓词/SET*/INSERT*、DDL 变体、JSON/expr 分支、update tuple、resolve 歧义、fallback 边界
        - src/analyzer.rs #[cfg(test)]（17+ 测试）：GenericDialect 解析（DDL/join 算子/expr 臂/merge ROW/paren DML/UNNEST-Function/TABLE 因子）+ 手工 AST 补丁（JsonAccess/Lambda/Convert/Trim/ExprNamed/限定通配/SetExpr::Table/函数 WHERE-LIMIT-NULLS 子句/ASOF/TableObject::TableQuery-TableFunction/ctas 列/insert 变体/unknown qualifier/standalone query）
        - tests/lineage_ext3.rs（52 用例）：分区/after_columns/merge 各变体/update 源域/lateral 默认列名/CUBE-ROLLUP/自读写/fallback 名称收尾与 LIKE-CLONE 链/WITH 各形态/多插边界
        - src/lexer.rs tests2（first_token 全 trivia/tokenize 尾部空白/变量与符号边界）、src/catalog.rs sym_pool_default、src/preprocess.rs classify_empty/substitute_repeat_and_undef
        - src/server.rs tests 增补（run() spawn+abort/preload 错误/图环与菱形/列追踪 down 循环/自引用列边/table_detail other 列/statements 过滤/upload 错误/会话淘汰/全部 api_*）
        - tests/env_paths.rs（HL_PARSE_ONLY/HL_PROFILE/HL_THREADS 含无效值）、tests/cli.rs（子进程 analyze 全选项/serve 端口占用/bench_parse）
    *   已报告微小偏差修正：
        - UNNEST/TABLE/JSON_TABLE/explode 在 HiveDialect 下走"函数名表"臂（断言固化）
        - `PRIOR a` 解析为限定列引用而非 Expr::Prior（断言固化）
        - manual_asof_join 补丁目标应为 Statement::Query 而非 Insert
        - update `SET a = c` 无自边（add_factor_ext(...,false) 目标不标 source，allow_self 过滤）→ 改用 UPDATE...FROM/子查询非空 srcs
        - GROUP BY CUBE/ROLLUP 仅产生 x 列边；lateral 默认列名 col/pos/val
        - clippy 测试侧 3 处（replace_box/len_zero/comparison_to_empty）已修
        - 覆盖率脚本清理 glob 扩充（lineage_ext2/cli/env_paths）；llvm-cov v22 与 rustc profile 不兼容，改用工具链自带 llvm-tools
    *   阻碍：wrap(_,Direct) 调用点、column_trace up 循环 continue、text/analyze 的 Ok(Err) 为不可达臂，按现状保留
    *   状态：完成，180 测试全绿
*   2026-11-14
    *   步骤：9. 覆盖率复测
    *   结果：行覆盖 85.83% → **95.08%**（239/4862 行未覆盖，src/ 全口径含 bin 壳）
    *   分文件：lib 99.07 / analyzer 90.83 / fallback 97.39 / preprocess 97.70 / catalog 98.46 / lexer 97.77 / server 98.11 / main 100 / bench_parse 94.44
    *   状态：达标（≥95%）
*   2026-11-14
    *   步骤：10. fmt + clippy + 文档收尾
    *   修改：cargo fmt 全仓格式化；删除遗留探针 examples/p2.rs；README 覆盖率表更新为 95% 基线
    *   状态：完成

## 最终审查（第二轮，REVIEW）
- 检查清单 1-10 全部执行完毕。
- `cargo clippy --all-targets`：0 warning；`cargo fmt --check`：干净。
- `cargo test`：180 个测试全绿（lib 48 + lineage 16 + ext 30 + ext2 30 + ext3 52 + cli 1 + env_paths 3）。
- `scripts/coverage.sh` 复测：行覆盖 **95.08%** ≥ 95%（权威 llvm-cov report，src/ 全部源码含二进制壳）。
- 生产代码逻辑改动仅 2 处 clippy 修复（analyzer 迭代器化、server sort_by_key），语义不变；其余全部为测试/文档/脚本新增。
- 残留未覆盖 239 行集中在 analyzer.rs（184 行，非 Hive 方言 AST 子臂）+ 防御性死代码臂，已在 README 说明。
- 实施与最终计划完全匹配，无未报告偏差。

---
# 第三轮：解析结果追加落库（PostgreSQL）+ job_name 关键字段 + 深层血缘查询与 WebUI 展示

## 分析（RESEARCH）
- 现有 `Options` 与 `LineageResult` 无 `job_name`（如 `dag.task`）字段；`server.rs` 仅维护单次上传会话内存图，无数据库持久化与跨任务血缘检索能力。
- 多任务入库关键边界：单任务内部 `CREATE TEMPORARY TABLE`（`kind == "temp"`）不能作为跨任务共享物理表落库，必须在入库抽取阶段将临时表做传递消解（折叠为上游物理表到下游物理表的直接表边与字段边），防止不同任务使用同名临时表造成跨任务伪血缘。
- 用户确认口径：
  1. 追加写（Append-Only）：严禁 UPDATE/DELETE/TRUNCATE；同 `job_name` 多次追加默认取最新 `batch_id` 生效，支持可选查询全量历史（`all_history`）。
  2. CLI 与 WebUI 双端均支持携带 `job_name` 解析落库及四类血缘查询。
  3. 自动执行 `CREATE TABLE IF NOT EXISTS` 建表并附带独立 DDL 脚本 `sql/postgres_ddl.sql`。
  4. 针对超深血缘链路性能：选定方向一——**PostgreSQL 追加持久化 + Rust 服务端内存热图增量水位线索引（`HotGraphIndex`）+ PG `WITH RECURSIVE` 复合索引递归查询**。

## 提议的解决方案（INNOVATE）
- **持久化底座（PostgreSQL Append-Only）**：使用纯 Rust `tokio-postgres`，设计四张追加明细表 `hl_job_batches`、`hl_job_tables`、`hl_table_edges`、`hl_column_edges`，配备 `(job_name, batch_id DESC)` 与源/目标表及字段复合 B-Tree 索引。
- **双查询加速路径**：
  - Web 服务端（默认最新批次口径）：内存维护 `HotGraphIndex`（`u32` 整数符号驻留 + 紧凑邻接表），按 `batch_id > watermark` 毫秒级增量同步新入库批次，30~50+ 跳超深链路亚毫秒级完成。
  - CLI 无状态查询及 `all_history` 全量历史查询：直接下推 PostgreSQL `WITH RECURSIVE`（带 `visited TEXT[]` 环路保护）单次网络往返完成多跳遍历。
- **四类血缘查询**：
  1. 输入 `库名.表名` 查询表级上下游加工血缘（边附带 `job_name` 与 SQL 摘要）。
  2. 输入 `库名.表名` 查询任务级上下游血缘（以目标表为锚点串联上游产出任务链与下游消费任务链）。
  3. 按任务名（`dag.task`）搜索并查询上下游任务链路（经共享物理表推导跨任务边）。
  4. 按 `库名`、`表名`、`字段名` 输出上下游 `库名.表名.字段名` 字段级血缘链路（含端到端路径 `chains` 与字段级可视化图）。

## 实施检查清单（第三轮，PLAN）
1. 更新 TASK.md 记录第三轮需求分析、技术方案与实施检查清单
2. 更新 Cargo.toml 引入 tokio-postgres 依赖，并在 src/model.rs 与 src/lib.rs 扩展 job_name 字段支持
3. 创建 sql/postgres_ddl.sql 与 src/db/schema.rs（PG 自动建表 DDL、索引与连接串脱敏工具）
4. 创建 src/db/mod.rs 与 src/db/extract.rs（存储模型、后端抽象接口、带临时表消解的批次数据抽取器）
5. 创建 src/db/postgres.rs、src/db/memory.rs 与 src/db/hot_index.rs（PG 追加写与 WITH RECURSIVE 查询、内存后端、Rust 内存热图增量水位线索引）
6. 创建 src/db/query.rs（封装四类血缘查询与拓扑图层构建）
7. 扩展 src/main.rs（CLI analyze 携带 job_name 追加落库、serve 数据库参数、query 四类血缘查询与 ddl 子命令）
8. 扩展 src/server.rs 与 src/web/index.html（新增 DB 配置/落库/四类血缘查询 API 及 WebUI 血缘查询面板与结果图渲染）
9. 编写单元与集成测试，更新 README.md，运行 cargo fmt、cargo clippy 与 cargo test 全面验证

## 当前执行步骤
> 全部检查清单项目已执行完毕。

## 任务进度（第三轮）
*   2026-11-16
    *   步骤：1. 更新 TASK.md
    *   修改：TASK.md
    *   更改摘要：记录第三轮分析、PostgreSQL + Rust 内存热图增量索引方案及 9 步检查清单
    *   原因：执行计划步骤 1
    *   阻碍：无
    *   状态：完成
*   2026-11-16
    *   步骤：2. 更新 Cargo.toml、src/model.rs 与 src/lib.rs
    *   修改：Cargo.toml（tokio-postgres = "0.7.15"、tokio 补 time/sync features）、Cargo.lock、src/model.rs（`LineageResult.job_name: Option<String>` 空值跳过序列化 + 结构体 Deserialize）、src/lib.rs（`Options.job_name: Option<String>`、`analyze_with_sources` 去首尾空白透传、`pub mod db`）
    *   原因：执行计划步骤 2
    *   阻碍：无
    *   状态：完成
*   2026-11-16
    *   步骤：3. 创建 sql/postgres_ddl.sql 与 src/db/schema.rs
    *   修改：sql/postgres_ddl.sql（四表 CREATE TABLE IF NOT EXISTS + 复合 B-Tree 索引）、src/db/schema.rs（`postgres_ddl()` 嵌入脚本、`mask_db_url()` URI/KV 双形态连接串密码脱敏）
    *   原因：执行计划步骤 3
    *   阻碍：无
    *   状态：完成
*   2026-11-16
    *   步骤：4. 创建 src/db/mod.rs 与 src/db/extract.rs
    *   修改：src/db/mod.rs（DbConfig/QueryScope/LineageDirection 与四表行模型、BatchPayload、SaveSummary、Raw*Subgraph、`LineageBackend` trait）、src/db/extract.rs（`next_batch_id` 进程内单调微秒批次号、`split_db_table`/`split_job_name`/`parse_qualified_column`/`format_qualified_column` 规范化拆分、`extract_batch_payload` 含 temp 表 BFS 传递消解与 direct/derived/aggregate 合成优先级）
    *   原因：执行计划步骤 4
    *   阻碍：无
    *   状态：完成
*   2026-11-16
    *   步骤：5. 创建 src/db/postgres.rs、src/db/memory.rs 与 src/db/hot_index.rs
    *   修改：src/db/postgres.rs（tokio-postgres 连接、ensure_schema 批量执行 DDL、单事务多行 INSERT 追加、latest/all_history 两口径 WITH RECURSIVE+visited 环路保护子图查询、search_jobs/search_tables 关键字检索）、src/db/memory.rs（Append-Only Vec<BatchPayload> 内存后端，全部 trait 方法委托 HotGraphIndex）、src/db/hot_index.rs（u32 符号驻留、按 job 最新批次或全历史紧凑邻接表、watermark_batch_id 增量合并、表级/任务级/字段级 BFS 遍历含跳数与环路保护）
    *   原因：执行计划步骤 5
    *   阻碍：无
    *   状态：完成
*   2026-11-16
    *   步骤：6. 创建 src/db/query.rs
    *   修改：src/db/query.rs（`LineageService` 门面：connect 按 url 前缀路由 memory:// / PgBackend、save_workspace 写库+热图实时同步、500ms 水位线增量刷新浪潮；四类查询响应结构与 graph JSON 构建——表级加工血缘、锚点表串联任务血缘、任务链路含 chains、库.表.字段字段血缘含 chains；compute_dag_levels Kahn 分层与 extract_directed_chains 环路安全链路提取）
    *   已报告微小偏差修正：
        - 迭代器闭包误用引用模式 `|(jn,&d)|` 改为 `|(jn,d)|` 并显式解引用（6 处编译错误修复）
    *   原因：执行计划步骤 6
    *   阻碍：无
    *   状态：完成
*   2026-11-16
    *   步骤：7. 扩展 src/main.rs
    *   修改：src/main.rs（Cmd 扩展为 analyze/serve/query/ddl 四子命令；analyze 新增 --job-name/--db-url/--no-auto-create-table 且 db-url 时强制校验 job_name 非空；serve 新增 --db-url/--no-auto-create-table 调 server::run_with_options；query 子命令组 table/table-tasks/task/column 含 --direction/--hops/--all-history/--pretty/--db-url（缺省读 HL_DB_URL）与三段式/合并式字段输入；ddl 输出 postgres_ddl()）
    *   原因：执行计划步骤 7
    *   阻碍：无
    *   状态：完成
*   2026-11-16
    *   步骤：8. 扩展 src/server.rs 与 src/web/index.html
    *   修改：src/server.rs（SessionGraph 补 dialect/default_db 供补存；AppState 增加 `db: RwLock<Option<Arc<LineageService>>>`；analyze/analyze_text 支持 job_name+save_db 前置校验与 saved 摘要；新增 /api/s/{id}/save、/api/db/status、/api/db/config、/api/db/ddl、/api/db/jobs、/api/db/tables、/api/db/lineage/{table,table_tasks,task,column} 共 10 条路由；run_with_options 接收可选 DbConfig 启动即建连并打印脱敏连接信息）、src/web/index.html（顶栏 DB 状态徽标与配置弹窗（含 memory:// 快填与 DDL 预览）、上传/编辑器任务名+同时追加落库、左侧血缘查询面板四模式（表级/表任务/任务链/字段血缘）+表名任务名联想候选+方向跳数+历史口径、画布支持 job/anchor_table 节点卡片与边中点标签（加工任务/媒介表）、右侧抽屉表/任务/边详情与跨模式跳转、字段血缘 chains 与 库.表.字段 清单展示）
    *   已报告微小偏差修正：
        - 现有 3 处测试用 TextAnalyzeBody 初始化补 job_name/save_db 字段
    *   原因：执行计划步骤 8
    *   阻碍：无
    *   状态：完成
*   2026-11-16
    *   步骤：9. 测试、README 与全面验证
    *   修改：tests/db_lineage.rs（新建 11 个集成测试：job_name 拆分与序列化、批次元数据抽取、memory:// 落库+表级血缘、表任务血缘、任务链路与 chains、字段血缘（三段式+合并式+aggregate 边+非法输入）、latest_batch vs all_history、三表环路+不限跳安全、热图增量水位线刷新、空白 job_name 拒绝、DDL+URL 脱敏）、src/server.rs tests（新增 db_status_and_config、db_save_and_lineage_apis、analyze_text_with_save_db 共 3 个 API 级测试，覆盖 10 条新路由正/负路径）、tests/cli.rs（新增 cli_ddl_and_query、cli_analyze_job_name_and_save 共 2 个子进程测试：ddl 输出、query help/缺 url 报错/memory:// 四类查询、job_name JSON 透传、缺 job-name 拒绝、memory:// 落库成功）、README.md（特性/血缘入库/四类查询 CLI/WebUI/HTTP API/HL_DB_URL/库 API 文档更新）、src/db/{extract,hot_index,query}.rs（3 处 type_complexity 类型别名化 + 1 处 if_same_then_else 简化）
    *   原因：执行计划步骤 9
    *   阻碍：无
    *   状态：完成

## 最终审查（第三轮，REVIEW）
- 检查清单 1-9 全部执行完毕。
- `cargo fmt` + `cargo fmt --check`：干净。
- `cargo clippy --all-targets`：0 warning（新代码 3 处 type_complexity 已类型别名化、1 处 if_same_then_else 已简化）。
- `cargo test`：213 个测试全绿（lib 60 + db_lineage 11 + lineage 16 + ext 30 + ext2 30 + ext3 52 + cli 5 + cli_dialects 2 + dialects 6 + env_paths 1）。
- 功能符合性核验：
  - 追加落库：`extract_batch_payload` 仅产出 INSERT 行（无任何 UPDATE/DELETE），四表 `(job_name, batch_id)` 追加键；`save_workspace` 经 `append_batch` 单事务多行写入。
  - `dag.task` 任务标识：`Options.job_name` → `LineageResult.job_name`（空值跳过序列化，旧输出兼容）；入库行模型 job_name/dag_name/task_name 全量携带；CLI analyze --job-name 与 WebUI 上传/编辑器任务名、POST /api/s/{id}/save 均透传；空白 job_name 在 CLI/analyze_text/save_session/save_workspace 四层均明确 400/bail。
  - 四类血缘查询：`query_table_lineage`（库.表名→上下游表+加工任务边+sql_preview）、`query_table_task_lineage`（库.表名→锚点表+产出/消费任务链）、`query_task_lineage`（dag.task 精确/模糊→上下游任务+chains+媒介表边）、`query_column_lineage`（库.表.字段 或三段式→上下游 库.表.字段名+chains+字段级 graph）全部在 memory:// 端到端验证。
  - 批次口径：latest_batch（默认各 job 最新 batch_id）vs all_history 在二次追加同 job 场景下行为差异已断言；PG 侧 scope CTE 差异有单测。
  - 深血缘：HotGraphIndex u32 符号驻留 + watermark 增量合并（hot_index_incremental_refresh 验证新批次即时可见）；BFS 含 visited 环路保护 + hops=0 映射 MAX_UNLIMITED_HOPS=64 上限（cycle_safe_and_hop_limit 验证三环不自锁）；PG 侧 WITH RECURSIVE 带 visited TEXT[] 保护。
  - 安全：mask_db_url URI/KV 双形态密码脱敏（schema 单测+集成断言）；连接串不回显明文。
  - 兼容性：既有 180+ 血缘/方言/server/CLI 测试零回归通过；job_name 为空时 JSON 输出与旧版一致。
- 未验证区：真实 PostgreSQL 实例端到端（开发环境无 PG 实例；PgBackend SQL 语义经 scope_cte/BFS 单测与 MemoryBackend 同构 trait 路径交叉验证）；Doris 未实现（按计划收敛为 PostgreSQL 单后端）。
- 实施与最终计划完全匹配，无未报告偏差。

---
# 第四轮：本地 PG 容器端到端验证 + 产物体积优化 + README 文档完善

## 实施内容（EXECUTE）

*   2026-11-16
    *   步骤：A. 本地 PostgreSQL 容器端到端验证
    *   环境：`postgres:15` 容器 `hl-pg-test`（127.0.0.1:5432，db=lineage）
    *   验证项与结果：
        - 自动建表：`hl_job_batches/hl_job_tables/hl_table_edges/hl_column_edges` 四表 + 索引首次连接自动创建 ✓
        - 追加落库：4 个 `dag.task`（etl.j1_ods→j4_ads）依次 `--job-name` 落库；`dag_name`/`task_name` 正确拆分；临时表 `tmp_agg` 被折叠为 `dws.region_daily→ads.region_report` 直接边不落 temp 节点 ✓
        - 追加语义：同 `job_name=etl.j3_dws` 二次落库产生两个并存 `batch_id`（2/3 列边），latest 口径仅见 v2 的 `last_dt` 字段血缘 ✓
        - 四类查询（CLI→PG WITH RECURSIVE）：表级 30 跳 267ms / 字段级 30 跳 282ms（`src.orders_raw.amt→ods.orders.amt→dwd.order_detail.amt→dws.region_daily.gmv→ads.region_report.gmv` 5 跳链 + aggregate 边）/ 任务级 30 跳 347ms（chains 长度 30）✓
        - serve 热图路径：`/api/db/status` 脱敏正确（`postgres://postgres:***@…`）、`hot_index:true`；30 跳任务链 HTTP 往返 69ms；服务运行期间 CLI 追加新批次 `pipe.stage_31`，约 1 秒内热图水位线增量可见（31 跳链路可查）✓
    *   结论：真实 PG 端到端全部通过，性能满足"足够快"要求
    *   状态：完成
*   2026-11-16
    *   步骤：B. release 产物体积优化
    *   修改：Cargo.toml 新增 `[profile.release]`（`opt-level="z"`、`lto=true`、`codegen-units=1`、`strip="symbols"`、`panic="abort"`）
    *   结果：hive-lineage 二进制 15MB → 4.0MB；冒烟验证 analyze/memory:// 落库/query task/ddl 输出均正常（`memory://` 跨进程查询 matched=None 为进程级内存库预期行为，已在 README 注明）
    *   状态：完成
*   2026-11-16
    *   步骤：C. README.md 按"图>表>文字"完善
    *   修改：新增"架构总览"ASCII 全景图（输入→解析流水线→LineageResult→三出口→PG 四表→双查询路径→四类查询→CLI/WebUI）；新增批次语义示意图（latest vs all_history）；新增"快速上手"端到端命令段（docker PG→analyze 落库→四类查询→serve WebUI）；`已知限制`扩展为分组"注意事项"（落库与批次/查询/解析与安全/服务，含 memory:// 进程级、DDL 权限、hops=0=64 上限、密码脱敏与 shell 历史、panic=abort 说明）；测试基线描述更新
    *   状态：完成

## 最终审查（第四轮）
- PG 容器验证项全部通过，无失败项；容器已在验证后 `docker rm -f` 清理。
- 产物体积 4.0MB，功能冒烟正常；`panic=abort` 取舍已在 README 注明。
- README 结构符合"图>表>文字"要求：1 张全景架构图 + 1 张批次语义图 + 快速上手命令段 + 分组注意事项。
- 无未报告偏差。

---
# 第五轮：功能边界修复 + 字段关联表/任务查询扩展 + 测试覆盖网补齐 + 性能优化

## 实施检查清单（第五轮，PLAN）
1. 修复解析管线边界缺陷（`lexer.rs` 跨行串行号、`preprocess.rs` `SET` 注释与括号 CTAS、`analyzer.rs` 非递归 CTE 遮蔽与不等长 `UNION`）并补充测试
2. 修复 Web 服务端与 CLI 边界（`server.rs` 会话数字序淘汰、中文 `truncated` 字符数、大小写查表、邻接裁剪与 `parse_select_flag`/`parse_select_mode` 大小写）并补充测试
3. 扩展 `ColumnLineageResponse`（`upstream_tables`/`downstream_tables`/`upstream_jobs`/`downstream_jobs`）及 WebUI `renderColumnQuerySummary` 展示与跳转并补充测试
4. 修复持久层与热图索引边界（`extract.rs` temp 自环与表名安全长度、`hot_index.rs` 索引位宽溢出/局部增量更新/检索截断优化/`*` 穿透、`query.rs` 并发同步锁与混合环链、`postgres.rs` 深跳边界与通配对齐）并补充测试
5. 补齐 `db/{mod,schema,memory,extract,hot_index,query}.rs` 与 `tests/cli.rs` 测试覆盖网并复核 `./scripts/coverage.sh`
6. 实施性能优化（`Cargo.toml` `opt-level=3`、并行预处理优化、`Catalog` 宽表去重、`Analyzer` `RelCols` 缓存与 CTE 增量回滚、`MemoryBackend` 索引缓存）并对比 10MB 压测，更新 `TASK.md` 与 `README.md`

## 任务进度（第五轮，EXECUTE）
*   2026-11-17
    *   步骤：1. 修复解析管线边界缺陷（commit `5914ac4`）
    *   修改：src/lexer.rs、src/preprocess.rs、src/analyzer.rs、tests/lineage_ext3.rs（t133~t137）
    *   状态：完成，218 测试全绿
*   2026-11-17
    *   步骤：2. 修复 Web 服务端与 CLI 边界（commit `533e8da`）
    *   修改：src/server.rs、src/main.rs
    *   状态：完成，219 测试全绿
*   2026-11-17
    *   步骤：3. 扩展字段查询关联表/任务血缘输出与 WebUI 展示（commit `be98b5f`）
    *   修改：src/db/query.rs、src/web/index.html、tests/db_lineage.rs
    *   状态：完成，219 测试全绿
*   2026-11-17
    *   步骤：4. 修复持久层与热图索引边界（commit `44a109b`）
    *   修改：src/db/extract.rs、src/db/hot_index.rs、src/db/query.rs、src/db/postgres.rs、tests/db_lineage.rs
    *   状态：完成，220 测试全绿
*   2026-11-17
    *   步骤：5. 补齐测试覆盖网（commit `20df49a`）
    *   修改：src/db/{mod,schema,extract,hot_index,memory,query}.rs、tests/{db_lineage,cli}.rs
    *   状态：完成，226 测试全绿；全仓除离线 `db/postgres.rs` 外行覆盖率达 ~95%
*   2026-11-17
    *   步骤：6. 性能优化与压测、文档收尾
    *   修改：Cargo.toml（`opt-level = 3`）、src/lib.rs、src/preprocess.rs、src/catalog.rs（含 `wide_table_set_threshold` 单测）、src/analyzer.rs、src/db/memory.rs、README.md、TASK.md
    *   压测结果（10.01MB，35,783 语句）：`elapsed` 1703.7ms → **1031.3ms**（提速约 39.5%）；其中 `decode+split` 69.9ms→23.4ms、`preprocess` 156.4ms→27.4ms、`walk` 1144.4ms→727.6ms；二进制体积 4.0MB→6.1MB
    *   状态：完成

## 最终审查（第五轮，REVIEW）
- 检查清单 1-6 全部实施完毕，无未报告偏差。
- `cargo fmt --check` 与 `cargo clippy --all-targets`：0 warning；`cargo test`：全量 227 测试通过。
- Hive/Spark 表级与列级解析、表查表/表查任务/任务查任务/字段查字段/字段查表/字段查任务全链路均经测试覆盖并验证通过。

