//! 血缘持久化存储（PostgreSQL 追加写）与超深血缘图查询模块
//!
//! 架构分层：
//! - `schema`：PostgreSQL 自动建表 DDL、复合索引定义与连接串脱敏
//! - `extract`：将 `AnalyzedWorkspace` 转换为携带 `job_name` 与单调 `batch_id` 的追加批次数据（自动消解任务内临时表）
//! - `postgres`：基于 `tokio-postgres` 的纯 `INSERT` 追加落库与 `WITH RECURSIVE` 单往返深度递归查询
//! - `memory`：内存追加存储后端（用于单元/集成测试与无外部数据库演示）
//! - `hot_index`：Rust 服务端内存热图增量水位线索引（`u32` 符号驻留 + 紧凑邻接表，亚毫秒级 30~50+ 跳遍历）
//! - `query`：统一封装表级加工血缘、表的任务级血缘、任务链路搜索、`库名.表名.字段名` 字段级血缘链路及 WebUI 图层生成

pub mod extract;
pub mod hot_index;
pub mod memory;
pub mod postgres;
pub mod query;
pub mod schema;

use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;

pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'a>>;

/// 数据库连接与索引配置
#[derive(Debug, Clone)]
pub struct DbConfig {
    /// PostgreSQL 连接串（如 `postgres://user:pass@127.0.0.1:5432/lineage`）或测试用 `memory://`
    pub url: String,
    /// 连接建立时是否自动执行 `CREATE TABLE IF NOT EXISTS` 与索引创建
    pub auto_create_table: bool,
    /// 是否启用进程内内存热图增量索引（`serve` 常驻服务默认开启，CLI 单次查询默认关闭直接走 PG `WITH RECURSIVE`）
    pub use_hot_index: bool,
}

impl DbConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            auto_create_table: true,
            use_hot_index: true,
        }
    }
}

/// 同名 `job_name` 多次追加后的查询生效口径
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryScope {
    /// 默认：每个 `job_name` 仅取最新一次 `batch_id` 的血缘数据
    #[default]
    LatestBatch,
    /// 合并所有历史追加批次并去重
    AllHistory,
}

impl QueryScope {
    pub fn from_all_history(all_history: bool) -> Self {
        if all_history {
            Self::AllHistory
        } else {
            Self::LatestBatch
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::LatestBatch => "latest_batch",
            Self::AllHistory => "all_history",
        }
    }
}

/// 血缘查询方向
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum LineageDirection {
    #[default]
    Both,
    Upstream,
    Downstream,
}

impl LineageDirection {
    pub fn want_up(self) -> bool {
        matches!(self, Self::Both | Self::Upstream)
    }

    pub fn want_down(self) -> bool {
        matches!(self, Self::Both | Self::Downstream)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Both => "both",
            Self::Upstream => "upstream",
            Self::Downstream => "downstream",
        }
    }
}

impl FromStr for LineageDirection {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "both" | "all" => Ok(Self::Both),
            "up" | "upstream" => Ok(Self::Upstream),
            "down" | "downstream" => Ok(Self::Downstream),
            other => Err(format!(
                "无效的血缘方向 '{other}'，仅支持 both / upstream / downstream"
            )),
        }
    }
}

/// `hl_job_batches` 单行模型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchRow {
    pub job_name: String,
    pub batch_id: i64,
    pub dag_name: String,
    pub task_name: String,
    pub dialect: String,
    pub default_db: String,
    pub files_json: String,
    pub statement_count: i32,
    pub parsed_count: i32,
    pub fallback_count: i32,
    pub failed_count: i32,
    pub table_count: i32,
    pub table_edge_count: i32,
    pub column_edge_count: i32,
    pub ingested_at_ms: i64,
}

/// `hl_job_tables` 单行模型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobTableRow {
    pub job_name: String,
    pub batch_id: i64,
    pub db_name: String,
    pub table_name: String,
    pub full_table_name: String,
    pub table_kind: String,
    pub is_source: bool,
    pub is_target: bool,
    pub columns: Vec<String>,
}

/// `hl_table_edges` 单行模型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableEdgeRow {
    pub job_name: String,
    pub batch_id: i64,
    pub source_db: String,
    pub source_table: String,
    pub source_full_name: String,
    pub target_db: String,
    pub target_table: String,
    pub target_full_name: String,
    pub stmt_indices: Vec<usize>,
    pub sql_preview: String,
}

/// `hl_column_edges` 单行模型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnEdgeRow {
    pub job_name: String,
    pub batch_id: i64,
    pub source_db: String,
    pub source_table: String,
    pub source_full_name: String,
    pub source_column: String,
    pub target_db: String,
    pub target_table: String,
    pub target_full_name: String,
    pub target_column: String,
    pub edge_kind: String,
    pub stmt_indices: Vec<usize>,
}

/// 一次任务解析对应的完整追加批次数据包
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchPayload {
    pub batch: BatchRow,
    pub job_tables: Vec<JobTableRow>,
    pub table_edges: Vec<TableEdgeRow>,
    pub column_edges: Vec<ColumnEdgeRow>,
}

/// 追加落库成功摘要
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaveSummary {
    pub job_name: String,
    pub dag_name: String,
    pub task_name: String,
    pub batch_id: i64,
    pub tables_written: usize,
    pub table_edges_written: usize,
    pub column_edges_written: usize,
}

/// 任务检索摘要项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSearchItem {
    pub job_name: String,
    pub dag_name: String,
    pub task_name: String,
    pub latest_batch_id: i64,
    pub ingested_at_ms: i64,
    pub source_tables: Vec<String>,
    pub target_tables: Vec<String>,
}

/// 物理表检索摘要项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSearchItem {
    pub full_table_name: String,
    pub db_name: String,
    pub table_name: String,
    pub table_kind: String,
    pub producer_jobs: Vec<String>,
    pub consumer_jobs: Vec<String>,
}

/// 底层图遍历产出的表级原始子图（供 `query` 层统一计算拓扑层与前端视图）
#[derive(Debug, Default, Clone)]
pub struct RawTableSubgraph {
    /// `(source_full_name, target_full_name, job_name, sql_preview)`
    pub edges: Vec<(String, String, String, String)>,
    /// 相关物理表的元数据与读写任务关联
    pub job_tables: Vec<JobTableRow>,
    /// 上游节点最短跳数 `(table -> distance)`
    pub up_distances: Vec<(String, u32)>,
    /// 下游节点最短跳数 `(table -> distance)`
    pub down_distances: Vec<(String, u32)>,
}

/// 底层图遍历产出的任务级原始子图
#[derive(Debug, Default, Clone)]
pub struct RawTaskSubgraph {
    /// 跨任务依赖边 `(upstream_job, downstream_job, via_full_table_name)`
    pub task_edges: Vec<(String, String, String)>,
    /// 直接产出锚点表的任务（仅 `table_task_lineage` 使用）
    pub direct_producers: Vec<String>,
    /// 直接消费锚点表的任务（仅 `table_task_lineage` 使用）
    pub direct_consumers: Vec<String>,
    /// 涉及到的所有任务最新摘要
    pub jobs: Vec<JobSearchItem>,
    /// 上游任务最短跳数
    pub up_distances: Vec<(String, u32)>,
    /// 下游任务最短跳数
    pub down_distances: Vec<(String, u32)>,
}

/// 底层图遍历产出的字段级原始子图
#[derive(Debug, Default, Clone)]
pub struct RawColumnSubgraph {
    pub edges: Vec<ColumnEdgeRow>,
    pub job_tables: Vec<JobTableRow>,
    /// `((full_table_name, column_name), distance)`
    pub up_distances: Vec<((String, String), u32)>,
    /// `((full_table_name, column_name), distance)`
    pub down_distances: Vec<((String, String), u32)>,
}

/// 存储后端统一接口
pub trait LineageBackend: Send + Sync {
    fn backend_name(&self) -> &'static str;

    fn ping(&self) -> BoxFut<'_, ()>;

    fn ensure_schema(&self) -> BoxFut<'_, ()>;

    fn append_batch<'a>(&'a self, payload: &'a BatchPayload) -> BoxFut<'a, ()>;

    fn load_latest_snapshot(&self) -> BoxFut<'_, Vec<BatchPayload>>;

    fn fetch_batches_since(&self, after_batch_id: i64) -> BoxFut<'_, Vec<BatchPayload>>;

    fn search_jobs<'a>(
        &'a self,
        keyword: &'a str,
        limit: usize,
        scope: QueryScope,
    ) -> BoxFut<'a, Vec<JobSearchItem>>;

    fn search_tables<'a>(
        &'a self,
        keyword: &'a str,
        limit: usize,
        scope: QueryScope,
    ) -> BoxFut<'a, Vec<TableSearchItem>>;

    fn query_table_subgraph<'a>(
        &'a self,
        table: &'a str,
        dir: LineageDirection,
        max_hops: u32,
        scope: QueryScope,
    ) -> BoxFut<'a, RawTableSubgraph>;

    fn query_table_task_subgraph<'a>(
        &'a self,
        table: &'a str,
        dir: LineageDirection,
        max_hops: u32,
        scope: QueryScope,
    ) -> BoxFut<'a, RawTaskSubgraph>;

    fn query_task_subgraph<'a>(
        &'a self,
        job_name: &'a str,
        dir: LineageDirection,
        max_hops: u32,
        scope: QueryScope,
    ) -> BoxFut<'a, RawTaskSubgraph>;

    fn query_column_subgraph<'a>(
        &'a self,
        full_table: &'a str,
        column: &'a str,
        dir: LineageDirection,
        max_hops: u32,
        scope: QueryScope,
    ) -> BoxFut<'a, RawColumnSubgraph>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn db_types_and_direction_parse() {
        let cfg = DbConfig::new("memory://");
        assert_eq!(cfg.url, "memory://");
        assert!(cfg.auto_create_table);
        assert!(cfg.use_hot_index);

        assert_eq!(
            "".parse::<LineageDirection>().unwrap(),
            LineageDirection::Both
        );
        assert_eq!(
            "all".parse::<LineageDirection>().unwrap(),
            LineageDirection::Both
        );
        assert_eq!(
            "up".parse::<LineageDirection>().unwrap(),
            LineageDirection::Upstream
        );
        assert_eq!(
            "downstream".parse::<LineageDirection>().unwrap(),
            LineageDirection::Downstream
        );
        assert!("sideways".parse::<LineageDirection>().is_err());

        assert_eq!(LineageDirection::Both.as_str(), "both");
        assert_eq!(LineageDirection::Upstream.as_str(), "upstream");
        assert_eq!(LineageDirection::Downstream.as_str(), "downstream");
        assert_eq!(QueryScope::from_all_history(false).as_str(), "latest_batch");
        assert_eq!(QueryScope::from_all_history(true).as_str(), "all_history");
    }
}
