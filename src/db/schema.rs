//! PostgreSQL 建表 DDL 与数据库连接串脱敏工具

const POSTGRES_DDL: &str = include_str!("../../sql/postgres_ddl.sql");

pub fn postgres_ddl() -> &'static str {
    POSTGRES_DDL
}

/// 对数据库连接串中的密码字段做脱敏展示，避免日志或接口回显泄露凭据
pub fn mask_db_url(raw: &str) -> String {
    let trimmed = raw.trim();
    if let Some(scheme_sep) = trimmed.find("://") {
        let prefix = &trimmed[..scheme_sep + 3];
        let rest = &trimmed[scheme_sep + 3..];
        let authority_end = rest.find('/').unwrap_or(rest.len());
        let authority = &rest[..authority_end];
        let path_part = &rest[authority_end..];
        if let Some(at_idx) = authority.rfind('@') {
            let userinfo = &authority[..at_idx];
            let hostpart = &authority[at_idx + 1..];
            if let Some(colon_idx) = userinfo.find(':') {
                let user = &userinfo[..colon_idx];
                return format!("{prefix}{user}:***@{hostpart}{path_part}");
            }
        }
        return trimmed.to_string();
    }

    // 键值对形式：host=... user=... password=...
    let mut parts = Vec::new();
    let mut chars = trimmed.split_whitespace().peekable();
    while let Some(tok) = chars.next() {
        let lower = tok.to_ascii_lowercase();
        if let Some(eq_idx) = lower.find('=') {
            let k = &lower[..eq_idx];
            if k == "password" || k == "passwd" || k == "pwd" {
                let orig_k = &tok[..eq_idx];
                parts.push(format!("{orig_k}=***"));
                continue;
            }
        } else if lower == "password" || lower == "passwd" || lower == "pwd" {
            parts.push(tok.to_string());
            if let Some(next_tok) = chars.peek() {
                if *next_tok == "=" {
                    parts.push(chars.next().unwrap().to_string());
                    if chars.next().is_some() {
                        parts.push("***".to_string());
                    }
                    continue;
                }
            }
        }
        parts.push(tok.to_string());
    }
    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_contains_all_tables_and_indexes() {
        let ddl = postgres_ddl();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS hl_job_batches"));
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS hl_job_tables"));
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS hl_table_edges"));
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS hl_column_edges"));
        assert!(ddl.contains("idx_hl_column_edges_target"));
    }

    #[test]
    fn mask_url_and_kv_dsn() {
        assert_eq!(
            mask_db_url("postgres://dw_user:s3cr@t:123@10.0.0.8:5432/lineage"),
            "postgres://dw_user:***@10.0.0.8:5432/lineage"
        );
        assert_eq!(
            mask_db_url("postgres://dw_user@localhost/lineage"),
            "postgres://dw_user@localhost/lineage"
        );
        assert_eq!(
            mask_db_url("host=127.0.0.1 user=postgres password=my_secret dbname=dw"),
            "host=127.0.0.1 user=postgres password=*** dbname=dw"
        );
    }
}
