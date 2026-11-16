//! 解析失败时的兜底：基于 token 提取表级血缘与 DDL 列

use crate::analyzer::Analyzer;
use crate::lexer::{tokenize, TokKind, Token};

/// 同时跟踪小括号与尖括号（<...>）深度（`<=` 不算开括号）
fn two_depths(text: &str, toks: &[Token]) -> (Vec<i32>, Vec<i32>) {
    let mut p = vec![0i32; toks.len()];
    let mut a = vec![0i32; toks.len()];
    let (mut dp, mut da) = (0i32, 0i32);
    for (i, t) in toks.iter().enumerate() {
        p[i] = dp;
        a[i] = da;
        if t.is_sym(text, b'(') {
            dp += 1;
        } else if t.is_sym(text, b')') {
            dp -= 1;
        } else if t.is_sym(text, b'<') {
            let next_eq = toks
                .get(i + 1)
                .map(|n| n.is_sym(text, b'='))
                .unwrap_or(false);
            if !next_eq {
                da += 1;
            }
        } else if t.is_sym(text, b'>') && da > 0 {
            da -= 1;
        }
    }
    (p, a)
}

/// 从 i 开始收集一个点分名字（Word/QuotedWord/Var 由 `.` 连接），返回结束下标（不含）
fn collect_name(text: &str, toks: &[Token], i: usize) -> Option<(Vec<String>, usize)> {
    let mut parts = Vec::new();
    let mut j = i;
    loop {
        let t = toks.get(j)?;
        match t.kind {
            TokKind::Word | TokKind::QuotedWord => {
                parts.push(t.ident(text).to_lowercase());
            }
            TokKind::Var => {
                parts.push(t.slice(text).to_lowercase());
            }
            _ => {
                return if parts.is_empty() {
                    None
                } else {
                    Some((parts, j))
                }
            }
        }
        j += 1;
        if toks.get(j).map(|t| t.is_sym(text, b'.')).unwrap_or(false) {
            j += 1;
            continue;
        }
        return Some((parts, j));
    }
}

/// 抽取 `(` 后的列定义名：depth1&angle0 上 `(` 或 `,` 之后的第一个标识符
fn extract_cols(text: &str, toks: &[Token], lp: usize, pdep: &[i32], adep: &[i32]) -> Vec<String> {
    let mut cols = Vec::new();
    let mut want = false;
    for (i, t) in toks.iter().enumerate().skip(lp) {
        if pdep[i] > 1 {
            continue;
        }
        if pdep[i] == 0 && i > lp {
            break;
        }
        if adep[i] != 0 {
            continue;
        }
        if t.is_sym(text, b'(') || t.is_sym(text, b',') {
            want = true;
            continue;
        }
        if want {
            match t.kind {
                TokKind::Word | TokKind::QuotedWord => {
                    let c = t.ident(text).to_lowercase();
                    if t.kind == TokKind::Word
                        && matches!(c.as_str(), "primary" | "foreign" | "unique" | "constraint")
                    {
                        want = false;
                        continue;
                    }
                    if !cols.iter().any(|x| x == &c) {
                        cols.push(c);
                    }
                }
                _ => {}
            }
            want = false;
        }
    }
    cols
}

/// 兜底提取：返回是否找到了至少一个目标
pub fn extract(text: &str, a: &mut Analyzer) -> bool {
    let toks = tokenize(text);
    if toks.is_empty() {
        return false;
    }
    let (pdep, adep) = two_depths(text, &toks);
    // 收集 CTE 名：with x as (  /  , x as (
    let mut ctes: Vec<String> = Vec::new();
    for i in 3..toks.len() {
        if toks[i].is_sym(text, b'(')
            && toks[i - 1].is_kw(text, "as")
            && matches!(toks[i - 2].kind, TokKind::Word | TokKind::QuotedWord)
            && (toks[i - 3].is_kw(text, "with") || toks[i - 3].is_sym(text, b','))
        {
            ctes.push(toks[i - 2].ident(text).to_lowercase());
        }
    }
    let mut found_target = false;
    let create_mods = [
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
    let mut i = 0;
    while i < toks.len() {
        let t = toks[i];
        if t.is_kw(text, "insert") && pdep[i] == 0 {
            let mut j = i + 1;
            while j < toks.len()
                && ["overwrite", "into", "table", "local"]
                    .iter()
                    .any(|k| toks[j].is_kw(text, k))
            {
                j += 1;
            }
            if j < toks.len() && toks[j].is_kw(text, "directory") {
                if let Some(p) = toks.get(j + 1).filter(|t| t.kind == TokKind::Str) {
                    let raw = p.slice(text);
                    let path = raw.trim_matches(|c| c == '\'' || c == '"');
                    let id = a.intern(&format!("dir:{path}"));
                    a.set_kind(id, "directory");
                    a.mark_target(id);
                    found_target = true;
                }
                i = j + 2;
                continue;
            }
            if let Some((parts, end)) = collect_name(text, &toks, j) {
                a.intern_target(&parts);
                found_target = true;
                i = end;
                continue;
            }
        }
        if t.is_kw(text, "merge") && pdep[i] == 0 {
            let mut j = i + 1;
            if j < toks.len() && toks[j].is_kw(text, "into") {
                j += 1;
            }
            if let Some((parts, end)) = collect_name(text, &toks, j) {
                a.intern_target(&parts);
                found_target = true;
                i = end;
                continue;
            }
        }
        if t.is_kw(text, "update") && pdep[i] == 0 {
            // 仅当不是 MERGE 子句中的 WHEN MATCHED THEN UPDATE 时，才视作顶层 UPDATE 目标表
            let prev_then = i > 0 && toks[i - 1].is_kw(text, "then");
            if !prev_then {
                if let Some((parts, end)) = collect_name(text, &toks, i + 1) {
                    a.intern_target(&parts);
                    found_target = true;
                    i = end;
                    continue;
                }
            }
        }
        if t.is_kw(text, "create") && pdep[i] == 0 {
            let mut j = i + 1;
            let mut temporary = false;
            while j < toks.len() {
                if toks[j].is_kw(text, "temp") || toks[j].is_kw(text, "temporary") {
                    temporary = true;
                }
                if create_mods.iter().any(|k| toks[j].is_kw(text, k)) {
                    j += 1;
                } else {
                    break;
                }
            }
            if j < toks.len() && (toks[j].is_kw(text, "table") || toks[j].is_kw(text, "view")) {
                let is_view = toks[j].is_kw(text, "view");
                let mut k = j + 1;
                while k < toks.len()
                    && ["if", "not", "exists"]
                        .iter()
                        .any(|kw| toks[k].is_kw(text, kw))
                {
                    k += 1;
                }
                if let Some((parts, end)) = collect_name(text, &toks, k) {
                    let qn = if temporary {
                        parts.join(".")
                    } else {
                        a.qualify(&parts)
                    };
                    let id = a.intern(&qn);
                    a.mark_target(id);
                    if is_view {
                        a.set_kind(id, "view");
                    }
                    if temporary {
                        a.set_kind(id, "temp");
                        if let Some(last) = parts.last() {
                            a.add_temp(last);
                        }
                    }
                    found_target = true;
                    if !is_view {
                        if let Some(lp) =
                            toks.get(end).filter(|t| t.is_sym(text, b'(')).map(|_| end)
                        {
                            let cols = extract_cols(text, &toks, lp, &pdep, &adep);
                            let mut more = Vec::new();
                            for m in lp..toks.len().saturating_sub(2) {
                                if pdep[m] == 0
                                    && toks[m].is_kw(text, "partitioned")
                                    && toks[m + 1].is_kw(text, "by")
                                    && toks[m + 2].is_sym(text, b'(')
                                {
                                    more = extract_cols(text, &toks, m + 2, &pdep, &adep);
                                    break;
                                }
                                if pdep[m] == 0
                                    && [
                                        "row",
                                        "stored",
                                        "location",
                                        "as",
                                        "select",
                                        "like",
                                        "clustered",
                                        "skewed",
                                        "sorted",
                                        "into",
                                        "tblproperties",
                                    ]
                                    .iter()
                                    .any(|k| toks[m].is_kw(text, k))
                                {
                                    break;
                                }
                            }
                            let mut all = cols;
                            for c in more {
                                if !all.iter().any(|x| x == &c) {
                                    all.push(c);
                                }
                            }
                            if !all.is_empty() {
                                a.register_columns(id, &all);
                            }
                        }
                    }
                    // CREATE TABLE x LIKE y / CLONE：拷贝列目录
                    let mut l = end;
                    while l + 1 < toks.len()
                        && (toks[l].is_kw(text, "like") || toks[l].is_kw(text, "clone"))
                    {
                        if let Some((lparts, lend)) = collect_name(text, &toks, l + 1) {
                            let lq = a.qualify(&lparts);
                            a.copy_columns_from_name(id, &lq);
                            l = lend;
                        } else {
                            break;
                        }
                    }
                    i = end;
                    continue;
                }
            }
        }
        if (t.is_kw(text, "from") || t.is_kw(text, "join") || t.is_kw(text, "using"))
            && pdep[i] == 0
        {
            // JOIN ... USING (col) 中 USING 后紧跟 '('，不属于表引用
            if t.is_kw(text, "using")
                && toks
                    .get(i + 1)
                    .map(|nt| nt.is_sym(text, b'('))
                    .unwrap_or(false)
            {
                i += 1;
                continue;
            }
            let d = pdep[i];
            let mut j = i + 1;
            loop {
                match collect_name(text, &toks, j) {
                    Some((parts, end)) => {
                        let is_fn = toks.get(end).map(|t| t.is_sym(text, b'(')).unwrap_or(false);
                        if !is_fn && !(parts.len() == 1 && ctes.iter().any(|c| c == &parts[0])) {
                            a.intern_source(&parts);
                        }
                        j = end;
                    }
                    None => {
                        j += 1;
                    }
                }
                if toks.get(j).map(|t| t.is_sym(text, b',')).unwrap_or(false)
                    && pdep.get(j).copied().unwrap_or(-1) == d
                {
                    j += 1;
                    continue;
                }
                break;
            }
            i = j;
            continue;
        }
        i += 1;
    }
    found_target
}
