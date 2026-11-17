//! 预处理：语句分类、SET 变量、USE 库、变量替换、多路 INSERT 改写

use crate::lexer::{first_token, tokenize, TokKind, Token};
use rustc_hash::FxHashMap as HashMap;
use std::borrow::Cow;

/// 预处理结果（借用原始语句切片以避免无意义的整段克隆）
pub enum Prep<'a> {
    /// 无需解析（set/use/other）
    Skip { kind: &'static str },
    /// SET 语句（可能携带一个变量赋值）
    Set { kv: Option<(String, String)> },
    /// USE db
    Use { db: Option<String> },
    /// 独立查询语句（SELECT / WITH ... SELECT），可按需启用虚拟结果表分析
    Query { segment: Cow<'a, str> },
    /// 需要送入 parser 的若干片段（多路 insert 会被拆成多条）
    Parse {
        kind: &'static str,
        segments: Vec<Cow<'a, str>>,
    },
    /// 纯 DDL（CREATE TABLE 列定义 / LIKE），token 级提取即可，无需 parser
    Ddl {
        kind: &'static str,
        text: Cow<'a, str>,
    },
}

const SKIP_KWS: &[&str] = &[
    "add", "dfs", "reset", "source", "delete", "drop", "alter", "truncate", "msck", "analyze",
    "load", "explain", "show", "desc", "describe", "grant", "revoke", "export", "import", "list",
    "reload", "lock", "unlock", "abort", "commit", "rollback", "start", "repl", "kill", "compile",
    "declare", "exec", "execute", "setenv", "quit", "exit",
];

/// 计算每个 token 之前的括号深度
fn depths(text: &str, toks: &[Token]) -> Vec<i32> {
    let mut d = vec![0i32; toks.len()];
    let mut cur = 0i32;
    for (i, t) in toks.iter().enumerate() {
        d[i] = cur;
        if t.is_sym(text, b'(') {
            cur += 1;
        } else if t.is_sym(text, b')') {
            cur -= 1;
        }
    }
    d
}

/// 在 toks[lo..] 中找首个深度为 0 且属于 kws 的关键字，返回其下标
fn first_top_kw(text: &str, toks: &[Token], ds: &[i32], lo: usize, kws: &[&str]) -> Option<usize> {
    (lo..toks.len()).find(|&i| ds[i] == 0 && kws.iter().any(|k| toks[i].is_kw(text, k)))
}

/// 变量替换：${hivevar:x} / ${hiveconf:x} / ${x} / ${env:x} ...
/// 已定义 -> 值；未定义 -> hzv{n}hz 占位符，并登记占位符 -> 原文 以便还原
pub fn substitute<'a>(
    text: &'a str,
    vars: &HashMap<String, String>,
    intern: &mut HashMap<String, usize>,
    restore: &mut Vec<(String, String)>,
) -> Cow<'a, str> {
    if !text.contains("${") {
        return Cow::Borrowed(text);
    }
    let b = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0usize;
    let mut last = 0usize;
    while i < b.len() {
        if b[i] == b'$' && i + 1 < b.len() && b[i + 1] == b'{' {
            let mut j = i + 2;
            while j < b.len() && b[j] != b'}' {
                j += 1;
            }
            if j >= b.len() {
                break;
            }
            let name = text[i + 2..j].trim();
            let key = name
                .strip_prefix("hivevar:")
                .or_else(|| name.strip_prefix("hiveconf:"))
                .unwrap_or(name);
            out.push_str(&text[last..i]);
            if let Some(v) = vars.get(key) {
                out.push_str(v);
            } else {
                let next_id = intern.len();
                let n = if let Some(&existing) = intern.get(name) {
                    existing
                } else {
                    intern.insert(name.to_string(), next_id);
                    let ph = format!("hzv{}hz", next_id);
                    let orig = format!("${{{}}}", name);
                    restore.push((ph, orig));
                    next_id
                };
                out.push_str("hzv");
                out.push_str(&n.to_string());
                out.push_str("hz");
            }
            i = j + 1;
            last = i;
        } else {
            i += 1;
        }
    }
    out.push_str(&text[last..]);
    Cow::Owned(out)
}

/// 还原占位符：hzv{n}hz -> ${原名}（无占位符时零拷贝返回）
#[inline]
pub fn restore_placeholders<'a>(name: &'a str, restore: &[(String, String)]) -> Cow<'a, str> {
    if restore.is_empty() || !name.contains("hzv") {
        return Cow::Borrowed(name);
    }
    let mut s = name.to_string();
    for (ph, orig) in restore {
        if s.contains(ph.as_str()) {
            s = s.replace(ph.as_str(), orig);
        }
    }
    Cow::Owned(s)
}

/// 记录 SET 语句中的变量赋值：set [hivevar:|hiveconf:]key=value
fn parse_set<'a>(text: &str, toks: &[Token]) -> Prep<'a> {
    if toks.len() < 2 {
        return Prep::Set { kv: None };
    }
    let eq = toks.iter().position(|t| t.is_sym(text, b'='));
    match eq {
        Some(e) if e >= 2 => {
            let key_raw = text[toks[1].start..toks[e - 1].end].trim().to_lowercase();
            let key = key_raw
                .strip_prefix("hivevar:")
                .or_else(|| key_raw.strip_prefix("hiveconf:"))
                .unwrap_or(&key_raw)
                .to_string();
            let val_end = toks.last().map(|t| t.end).unwrap_or(text.len());
            let mut value = text[toks[e].end..val_end].trim().to_string();
            if value.len() >= 2 {
                let bs = value.as_bytes();
                if (bs[0] == b'\'' && bs[value.len() - 1] == b'\'')
                    || (bs[0] == b'"' && bs[value.len() - 1] == b'"')
                {
                    value = value[1..value.len() - 1].to_string();
                }
            }
            Prep::Set {
                kv: Some((key, value)),
            }
        }
        _ => Prep::Set { kv: None },
    }
}

/// 语句分类与改写。sub 应已完成变量替换。
pub fn classify<'a>(sub: Cow<'a, str>) -> Prep<'a> {
    let Some(t0) = first_token(&sub) else {
        return Prep::Skip { kind: "other" };
    };
    if t0.is_sym(&sub, b'!') {
        return Prep::Skip { kind: "other" };
    }
    if t0.kind != TokKind::Word {
        return Prep::Parse {
            kind: "other",
            segments: vec![sub],
        };
    }
    let w0 = t0.slice(&sub);

    // 高频关键字短路：免去二次全语句 tokenize
    if w0.eq_ignore_ascii_case("insert") {
        return Prep::Parse {
            kind: "insert",
            segments: vec![sub],
        };
    }
    if w0.eq_ignore_ascii_case("merge") {
        return Prep::Parse {
            kind: "merge",
            segments: vec![sub],
        };
    }
    if w0.eq_ignore_ascii_case("update") {
        return Prep::Parse {
            kind: "update",
            segments: vec![sub],
        };
    }
    if w0.eq_ignore_ascii_case("select")
        || w0.eq_ignore_ascii_case("values")
        || w0.eq_ignore_ascii_case("table")
    {
        return Prep::Query { segment: sub };
    }
    if SKIP_KWS.iter().any(|k| w0.eq_ignore_ascii_case(k)) {
        return Prep::Skip { kind: "other" };
    }

    // 以下语句需要进一步查看后续 token 结构
    let toks = tokenize(&sub);
    if toks.is_empty() {
        return Prep::Skip { kind: "other" };
    }
    if w0.eq_ignore_ascii_case("set") {
        return parse_set(&sub, &toks);
    }
    if w0.eq_ignore_ascii_case("use") {
        let db = toks
            .get(1)
            .filter(|t| t.kind == TokKind::Word || t.kind == TokKind::QuotedWord)
            .map(|t| t.ident(&sub).to_lowercase());
        return Prep::Use { db };
    }
    if w0.eq_ignore_ascii_case("with") {
        let ds = depths(&sub, &toks);
        match first_top_kw(
            &sub,
            &toks,
            &ds,
            1,
            &["select", "insert", "from", "values", "merge", "update"],
        ) {
            Some(i) if toks[i].is_kw(&sub, "insert") => {
                return Prep::Parse {
                    kind: "insert",
                    segments: vec![sub],
                };
            }
            Some(i) if toks[i].is_kw(&sub, "merge") => {
                return Prep::Parse {
                    kind: "merge",
                    segments: vec![sub],
                };
            }
            Some(i) if toks[i].is_kw(&sub, "update") => {
                return Prep::Parse {
                    kind: "update",
                    segments: vec![sub],
                };
            }
            Some(i) if toks[i].is_kw(&sub, "from") => {
                let segs = rewrite_multi_insert(&sub, &toks, &ds);
                if !segs.is_empty() {
                    return Prep::Parse {
                        kind: "insert",
                        segments: segs.into_iter().map(Cow::Owned).collect(),
                    };
                }
                return Prep::Parse {
                    kind: "insert",
                    segments: vec![sub],
                };
            }
            _ => return Prep::Query { segment: sub },
        }
    }
    if w0.eq_ignore_ascii_case("from") {
        let ds = depths(&sub, &toks);
        let segs = rewrite_multi_insert(&sub, &toks, &ds);
        return Prep::Parse {
            kind: "insert",
            segments: if segs.is_empty() {
                vec![sub]
            } else {
                segs.into_iter().map(Cow::Owned).collect()
            },
        };
    }
    if w0.eq_ignore_ascii_case("create") {
        let ds = depths(&sub, &toks);
        let mods = [
            "or",
            "replace",
            "temp",
            "temporary",
            "external",
            "global",
            "local",
            "transient",
            "volatile",
            "unlogged",
            "iceberg",
            "if",
            "not",
            "exists",
            "snapshot",
            "materialized",
        ];
        let mut i = 1;
        while i < toks.len()
            && toks[i].kind == TokKind::Word
            && mods.iter().any(|m| toks[i].is_kw(&sub, m))
        {
            i += 1;
        }
        let obj = if i < toks.len() {
            toks[i].slice(&sub)
        } else {
            ""
        };
        if obj.eq_ignore_ascii_case("table") {
            let paren_ctas = first_top_kw(&sub, &toks, &ds, i + 1, &["as"])
                .map(|as_i| {
                    let mut k = as_i + 1;
                    while k < toks.len() && toks[k].is_sym(&sub, b'(') {
                        k += 1;
                    }
                    k > as_i + 1
                        && k < toks.len()
                        && (toks[k].is_kw(&sub, "select")
                            || toks[k].is_kw(&sub, "with")
                            || toks[k].is_kw(&sub, "values"))
                })
                .unwrap_or(false);
            let is_ctas =
                first_top_kw(&sub, &toks, &ds, i + 1, &["select", "with"]).is_some() || paren_ctas;
            if is_ctas {
                return Prep::Parse {
                    kind: "ctas",
                    segments: vec![sub],
                };
            }
            return Prep::Ddl {
                kind: "create_table",
                text: sub,
            };
        }
        if obj.eq_ignore_ascii_case("view") {
            return Prep::Parse {
                kind: "create_view",
                segments: vec![sub],
            };
        }
        return Prep::Skip { kind: "other" };
    }
    Prep::Parse {
        kind: "other",
        segments: vec![sub],
    }
}

/// 多路 INSERT 改写：
/// `[WITH ...] FROM src INSERT ... SELECT ... [INSERT ... SELECT ...]`
/// -> 每个 INSERT 生成 `WITH ... INSERT ... SELECT ... FROM src [尾部子句]`
fn rewrite_multi_insert(text: &str, toks: &[Token], ds: &[i32]) -> Vec<String> {
    let first = match first_top_kw(text, toks, ds, 0, &["from", "insert"]) {
        Some(i) => i,
        None => return vec![],
    };
    let prefix = if toks[first].is_kw(text, "insert") {
        return vec![text.to_string()];
    } else {
        text[..toks[first].start].to_string()
    };
    if !toks[first].is_kw(text, "from") {
        return vec![];
    }
    let first_ins = match first_top_kw(text, toks, ds, first + 1, &["insert"]) {
        Some(i) => i,
        None => return vec![],
    };
    let src = text[toks[first].end..toks[first_ins].start]
        .trim()
        .to_string();
    if src.is_empty() {
        return vec![];
    }
    let mut ins_pos: Vec<usize> = Vec::new();
    for i in first_ins..toks.len() {
        if ds[i] == 0 && toks[i].is_kw(text, "insert") {
            ins_pos.push(i);
        }
    }
    if ins_pos.is_empty() {
        return vec![];
    }
    const BOUNDARY: &[&str] = &[
        "where",
        "group",
        "having",
        "order",
        "sort",
        "distribute",
        "cluster",
        "limit",
        "window",
        "union",
        "lateral",
        "except",
        "intersect",
    ];
    let mut out = Vec::with_capacity(ins_pos.len());
    for (k, &ip) in ins_pos.iter().enumerate() {
        let seg_tok_end = if k + 1 < ins_pos.len() {
            ins_pos[k + 1]
        } else {
            toks.len()
        };
        let seg_start = toks[ip].start;
        let seg_end = if seg_tok_end < toks.len() {
            toks[seg_tok_end].start
        } else {
            text.len()
        };
        let seg = &text[seg_start..seg_end];
        let sel = (ip..seg_tok_end).find(|&i| ds[i] == 0 && toks[i].is_kw(text, "select"));
        let new_seg = match sel {
            None => seg.to_string(),
            Some(si) => {
                let has_from =
                    (si + 1..seg_tok_end).any(|i| ds[i] == 0 && toks[i].is_kw(text, "from"));
                if has_from {
                    seg.to_string()
                } else {
                    let boundary = (si + 1..seg_tok_end)
                        .find(|&i| ds[i] == 0 && BOUNDARY.iter().any(|k| toks[i].is_kw(text, k)))
                        .map(|i| toks[i].start)
                        .unwrap_or(seg_end);
                    let mut s = String::with_capacity(seg.len() + src.len() + 8);
                    s.push_str(&text[seg_start..boundary]);
                    s.push_str(" FROM ");
                    s.push_str(&src);
                    s.push(' ');
                    s.push_str(&text[boundary..seg_end]);
                    s
                }
            }
        };
        out.push(format!("{}{}", prefix, new_seg));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_empty() {
        assert!(matches!(classify(Cow::Borrowed("")), Prep::Skip { .. }));
        assert!(matches!(
            classify(Cow::Borrowed("   \n\t  ")),
            Prep::Skip { .. }
        ));
    }

    #[test]
    fn substitute_repeat_and_undef() {
        // 同一 ${x} 重复出现 -> intern.get 命中 existing 臂
        let mut vars = HashMap::default();
        vars.insert("k".to_string(), "v1".to_string());
        let mut intern = HashMap::default();
        let mut restore = Vec::new();
        let out = substitute(
            "SELECT ${k}, ${k}, ${u}, ${u} FROM t",
            &vars,
            &mut intern,
            &mut restore,
        );
        assert!(out.contains("v1"));
        assert_eq!(intern.len(), 1);
        assert_eq!(restore.len(), 1);
    }
}
