use sqlparser::dialect::HiveDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;
use std::time::Instant;

fn main() {
    let text = std::fs::read_to_string("/tmp/hl/simple.sql").unwrap();
    let d = HiveDialect {};
    let t = Instant::now();
    let n = Tokenizer::new(&d, &text).tokenize().unwrap().len();
    println!("tokenize: {:?} ({} tokens)", t.elapsed(), n);

    let t = Instant::now();
    let mut p = Parser::new(&d)
        .with_recursion_limit(256)
        .try_with_sql(&text)
        .unwrap();
    let r = p.parse_statements();
    println!(
        "one-shot parse all: {:?} -> {:?}",
        t.elapsed(),
        r.map(|v| v.len()).map_err(|e| e.to_string())
    );
}
