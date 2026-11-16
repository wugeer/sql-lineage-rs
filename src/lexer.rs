//! 手写字节级词法器：语句切分 + 简单 token 化
//! 支持 Hive 语法特性：反引号标识符、\ 转义、-- 行注释、/* */ 块注释、${...} 变量

/// 切分出的语句（字节偏移均指向解码后的 UTF-8 文本）
#[derive(Debug)]
pub struct RawStatement {
    /// 首个非空白/非注释字符的位置
    pub start: usize,
    /// 结束位置（不含 `;`，已去除尾部空白）
    pub end: usize,
    /// 1-based 起始行号
    pub line: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokKind {
    /// 普通标识符/关键字（含反引号标识符）
    Word,
    /// 反引号标识符（不作关键字比较）
    QuotedWord,
    /// '...' 或 "..." 字符串
    Str,
    Num,
    /// ${...} 变量引用
    Var,
    /// 单字符符号：() , ; . = 等
    Sym,
}

#[derive(Debug, Clone, Copy)]
pub struct Token {
    pub kind: TokKind,
    pub start: usize,
    pub end: usize,
}

impl Token {
    /// 是否与给定关键字相等（仅 Word，大小写不敏感）
    #[inline]
    pub fn is_kw(&self, text: &str, kw: &str) -> bool {
        self.kind == TokKind::Word && text[self.start..self.end].eq_ignore_ascii_case(kw)
    }

    #[inline]
    pub fn is_sym(&self, text: &str, c: u8) -> bool {
        self.kind == TokKind::Sym && text.as_bytes()[self.start] == c
    }

    #[inline]
    pub fn slice<'a>(&self, text: &'a str) -> &'a str {
        &text[self.start..self.end]
    }

    /// 取标识符文本并去除反引号
    pub fn ident<'a>(&self, text: &'a str) -> &'a str {
        let s = self.slice(text);
        if s.len() >= 2 && s.starts_with('`') && s.ends_with('`') {
            &s[1..s.len() - 1]
        } else {
            s
        }
    }
}

#[inline]
fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b == b'@' || b >= 0x80
}

#[inline]
fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b == b'@' || b >= 0x80
}

#[inline]
fn is_ws(b: u8) -> bool {
    b == b' ' || b == b'\t' || b == b'\r' || b == b'\n' || b == 0x0b || b == 0x0c
}

/// 单次扫描：跳过空白与注释，返回下一个语义字符的起始位置与该行号
/// 返回 None 表示到达结尾
fn skip_trivia(b: &[u8], mut i: usize, mut line: usize) -> Option<(usize, usize)> {
    while i < b.len() {
        let c = b[i];
        if is_ws(c) {
            if c == b'\n' {
                line += 1;
            }
            i += 1;
        } else if c == b'-' && i + 1 < b.len() && b[i + 1] == b'-' {
            // -- 行注释
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if c == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
            // /* */ 块注释
            i += 2;
            while i < b.len() {
                if b[i] == b'\n' {
                    line += 1;
                }
                if b[i] == b'*' && i + 1 < b.len() && b[i + 1] == b'/' {
                    i += 2;
                    break;
                }
                i += 1;
            }
        } else if b[i..].starts_with(b"\xef\xbb\xbf") {
            // U+FEFF BOM 视作空白
            i += 3;
        } else {
            return Some((i, line));
        }
    }
    None
}

/// 跳过字符串/标识符体（支持 \' \" \` 反斜杠转义），返回结束偏移（不含终止引号）
fn skip_quoted(b: &[u8], mut i: usize, q: u8) -> usize {
    while i < b.len() {
        match b[i] {
            x if x == q => return i,
            b'\\' => {
                i += 2;
            }
            _ => i += 1,
        }
    }
    b.len()
}

/// 将文本按顶层 `;` 切分为语句，返回（去除空白/纯注释的）语句列表
pub fn split_statements(text: &str) -> Vec<RawStatement> {
    let b = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut line = 1usize;
    // 当前语句的起始位置/行号（惰性确定）
    let mut seg_start: Option<(usize, usize)> = None;
    let mut seg_content_end = 0usize; // 最后一个语义字符之后的位置

    macro_rules! flush {
        ($end:expr) => {
            if let Some((s, l)) = seg_start.take() {
                // 去掉尾部空白
                let e = if seg_content_end > s {
                    seg_content_end
                } else {
                    $end
                };
                if e > s {
                    out.push(RawStatement {
                        start: s,
                        end: e,
                        line: l,
                    });
                }
            }
        };
    }

    while i < b.len() {
        let c = b[i];
        match c {
            b'\'' | b'"' | b'`' => {
                if seg_start.is_none() {
                    seg_start = Some((i, line));
                }
                i = skip_quoted(b, i + 1, c);
                if i < b.len() {
                    i += 1; // 跳过收尾引号
                }
                seg_content_end = i;
            }
            b'-' if i + 1 < b.len() && b[i + 1] == b'-' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < b.len() && b[i + 1] == b'*' => {
                i += 2;
                while i < b.len() {
                    if b[i] == b'\n' {
                        line += 1;
                    }
                    if b[i] == b'*' && i + 1 < b.len() && b[i + 1] == b'/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b';' => {
                flush!(i);
                i += 1;
            }
            _ => {
                if is_ws(c) {
                    if c == b'\n' {
                        line += 1;
                    }
                } else {
                    if seg_start.is_none() {
                        seg_start = Some((i, line));
                    }
                    seg_content_end = i + 1;
                }
                i += 1;
            }
        }
    }
    flush!(b.len());
    out
}

/// 快速读取文本中的首个有效 token（零堆分配，用于预处理分类短路）
pub fn first_token(text: &str) -> Option<Token> {
    let b = text.as_bytes();
    let (mut i, _) = skip_trivia(b, 0, 1)?;
    if i >= b.len() {
        return None;
    }
    let c = b[i];
    let start = i;
    if c == b'\'' || c == b'"' {
        i = skip_quoted(b, i + 1, c);
        if i < b.len() {
            i += 1;
        }
        Some(Token {
            kind: TokKind::Str,
            start,
            end: i,
        })
    } else if c == b'`' {
        i = skip_quoted(b, i + 1, c);
        if i < b.len() {
            i += 1;
        }
        Some(Token {
            kind: TokKind::QuotedWord,
            start,
            end: i,
        })
    } else if c == b'$' && i + 1 < b.len() && b[i + 1] == b'{' {
        i += 2;
        while i < b.len() && b[i] != b'}' {
            i += 1;
        }
        if i < b.len() {
            i += 1;
        }
        Some(Token {
            kind: TokKind::Var,
            start,
            end: i,
        })
    } else if c.is_ascii_digit() {
        while i < b.len() && (is_ident_char(b[i]) || b[i] == b'.') {
            i += 1;
        }
        Some(Token {
            kind: TokKind::Num,
            start,
            end: i,
        })
    } else if is_ident_start(c) {
        while i < b.len() && is_ident_char(b[i]) {
            i += 1;
        }
        Some(Token {
            kind: TokKind::Word,
            start,
            end: i,
        })
    } else {
        Some(Token {
            kind: TokKind::Sym,
            start,
            end: i + 1,
        })
    }
}

/// 对单个语句文本做 token 化（跳过注释与空白）
pub fn tokenize(text: &str) -> Vec<Token> {
    let b = text.as_bytes();
    let mut toks = Vec::new();
    let mut i = match skip_trivia(b, 0, 1) {
        Some((p, _)) => p,
        None => return toks,
    };
    while i < b.len() {
        // 跳过注释/空白
        match skip_trivia(b, i, 1) {
            Some((p, _)) => i = p,
            None => break,
        }
        if i >= b.len() {
            break;
        }
        let c = b[i];
        let start = i;
        if c == b'\'' || c == b'"' {
            i = skip_quoted(b, i + 1, c);
            if i < b.len() {
                i += 1;
            }
            toks.push(Token {
                kind: TokKind::Str,
                start,
                end: i,
            });
        } else if c == b'`' {
            i = skip_quoted(b, i + 1, c);
            if i < b.len() {
                i += 1;
            }
            toks.push(Token {
                kind: TokKind::QuotedWord,
                start,
                end: i,
            });
        } else if c == b'$' && i + 1 < b.len() && b[i + 1] == b'{' {
            // ${...} 变量
            i += 2;
            while i < b.len() && b[i] != b'}' {
                i += 1;
            }
            if i < b.len() {
                i += 1;
            }
            toks.push(Token {
                kind: TokKind::Var,
                start,
                end: i,
            });
        } else if c.is_ascii_digit() {
            while i < b.len() && (is_ident_char(b[i]) || b[i] == b'.') {
                i += 1;
            }
            toks.push(Token {
                kind: TokKind::Num,
                start,
                end: i,
            });
        } else if is_ident_start(c) {
            while i < b.len() && is_ident_char(b[i]) {
                i += 1;
            }
            toks.push(Token {
                kind: TokKind::Word,
                start,
                end: i,
            });
        } else {
            i += 1;
            toks.push(Token {
                kind: TokKind::Sym,
                start,
                end: i,
            });
        }
    }
    toks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_basic() {
        let s = "select 1; select 2 -- c;nt\n; /* ; */ select ';' ;";
        let stmts = split_statements(s);
        assert_eq!(stmts.len(), 3);
        assert_eq!(&s[stmts[2].start..stmts[2].end], "select ';'");
    }

    #[test]
    fn line_numbers() {
        let s = "-- hi\nselect 1;\n\nselect 2;";
        let stmts = split_statements(s);
        assert_eq!(stmts[0].line, 2);
        assert_eq!(stmts[1].line, 4);
    }

    #[test]
    fn escaped_quote() {
        let s = "select 'a\\'b;c'; select 2;";
        let stmts = split_statements(s);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn first_token_kinds() {
        assert_eq!(first_token("'str'").unwrap().kind, TokKind::Str);
        assert_eq!(first_token("`qw`").unwrap().kind, TokKind::QuotedWord);
        assert_eq!(first_token("${v}").unwrap().kind, TokKind::Var);
        assert_eq!(first_token("12.5").unwrap().kind, TokKind::Num);
        assert_eq!(first_token("word").unwrap().kind, TokKind::Word);
        assert!(first_token("   -- 纯注释").is_none());
        assert!(first_token("").is_none());
        // 未闭合引号/变量：token 截到文本末尾
        assert_eq!(first_token("'unclosed").unwrap().kind, TokKind::Str);
        assert_eq!(first_token("${v").unwrap().kind, TokKind::Var);
        assert_eq!(first_token("`q").unwrap().kind, TokKind::QuotedWord);
    }

    #[test]
    fn trivia_variants() {
        // 块注释、行间 BOM、行尾纯注释
        let toks = tokenize("a /* c\n*/ \u{feff} b");
        assert_eq!(toks.len(), 2);
        assert!(tokenize(" -- tail only").is_empty());
        assert!(tokenize("").is_empty());
    }

    #[test]
    fn tokenize_kinds() {
        let toks = tokenize("a 's' `q` ${v} 1.5 ( x )");
        let kinds: Vec<TokKind> = toks.iter().map(|t| t.kind).collect();
        assert!(kinds.contains(&TokKind::Str));
        assert!(kinds.contains(&TokKind::QuotedWord));
        assert!(kinds.contains(&TokKind::Var));
        assert!(kinds.contains(&TokKind::Num));
        assert!(kinds.contains(&TokKind::Sym));
        // 未闭合 ${var -> 截到末尾
        let t2 = tokenize("x ${v");
        assert_eq!(t2.last().unwrap().kind, TokKind::Var);
        // 未闭合字符串
        let t3 = tokenize("x 'unclosed");
        assert_eq!(t3.last().unwrap().kind, TokKind::Str);
        // 数字+点
        let t4 = tokenize("1.2.3");
        assert_eq!(t4[0].kind, TokKind::Num);
    }
}

// 覆盖率补充：全 trivia / 未闭合 / 尾随空白边界（附加测试模块）
#[cfg(test)]
mod tests2 {
    use super::*;

    #[test]
    fn first_token_all_trivia() {
        assert!(first_token("  /* c */ \n -- x\n").is_none());
        assert!(first_token("").is_none());
        // skip_trivia 正常消费到 EOF -> i >= len 的 return None 臂
        assert!(first_token(" /* c */ ").is_none());
        assert!(first_token("   ").is_none());
    }

    #[test]
    fn tokenize_unclosed_tail() {
        let toks = tokenize("a /* unclosed");
        assert_eq!(toks.len(), 1);
        // 尾部空白 -> skip_trivia 返回 i >= len -> break 臂
        let toks = tokenize("x; \n ");
        assert!(toks.len() >= 2);
    }
}
