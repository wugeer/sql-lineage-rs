//! 输出数据模型（serde 序列化，字段名与前端约定一致）

use serde::Serialize;

#[derive(Debug, Default, Clone, Serialize)]
pub struct LineageResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_name: Option<String>,
    pub stats: Stats,
    pub files: Vec<FileInfo>,
    pub tables: Vec<TableInfo>,
    pub table_edges: Vec<TableEdge>,
    pub column_edges: Vec<ColumnEdge>,
    pub statements: Vec<StatementInfo>,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct Stats {
    pub files: usize,
    pub bytes: usize,
    pub statements: usize,
    pub parsed: usize,
    pub fallback: usize,
    pub failed: usize,
    pub skipped: usize,
    pub elapsed_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FileInfo {
    pub name: String,
    /// "utf-8" | "gbk"
    pub encoding: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableInfo {
    pub id: usize,
    pub name: String,
    /// "table" | "temp" | "view" | "directory" | "result"
    pub kind: &'static str,
    pub is_source: bool,
    pub is_target: bool,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableEdge {
    pub source: usize,
    pub target: usize,
    pub statements: Vec<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnEdge {
    pub source_table: usize,
    pub source_column: String,
    pub target_table: usize,
    pub target_column: String,
    /// "direct" | "derived" | "aggregate"
    pub kind: &'static str,
    pub statements: Vec<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatementInfo {
    pub index: usize,
    /// 所属文件下标
    pub file: usize,
    /// 1-based 起始行号
    pub line: usize,
    /// 解码后 UTF-8 文本中的字节偏移
    pub start: usize,
    pub end: usize,
    /// "insert"|"ctas"|"create_table"|"create_view"|"merge"|"update"|"query"|"set"|"use"|"other"
    pub kind: &'static str,
    /// "ok"|"fallback"|"error"|"skipped"
    pub status: &'static str,
    pub error: Option<String>,
    pub targets: Vec<usize>,
    pub sources: Vec<usize>,
}
