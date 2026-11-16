use std::io::Write;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

const SPARK_CTAS: &str = "CREATE TABLE dw.t USING PARQUET AS SELECT id FROM ods.s";

fn unique_sql_file() -> std::path::PathBuf {
    let name = format!(
        "hl_cli_dialect_{}_{}.sql",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let path = std::env::temp_dir().join(name);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    f.write_all(SPARK_CTAS.as_bytes()).unwrap();
    path
}

fn analyze_json(path: &std::path::Path, extra: &[&str]) -> serde_json::Value {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_hive-lineage"));
    cmd.arg("analyze").arg(path).args(extra);
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap()
}

fn assert_spark_ctas_edge(v: &serde_json::Value) {
    let s = v["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "ods.s")
        .unwrap()["id"]
        .as_u64()
        .unwrap();
    let t = v["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "dw.t")
        .unwrap()["id"]
        .as_u64()
        .unwrap();
    assert!(v["column_edges"].as_array().unwrap().iter().any(|e| {
        e["source_table"] == s
            && e["source_column"] == "id"
            && e["target_table"] == t
            && e["target_column"] == "id"
            && e["kind"] == "direct"
    }));
}

#[test]
fn cli_dialect_selection() {
    let path = unique_sql_file();
    for (extra, parsed, fallback) in [
        (vec![], 0, 1),
        (vec!["--dialect", "hive"], 0, 1),
        (vec!["--dialect", "spark"], 1, 0),
        (vec!["--dialect", "SPARK"], 1, 0),
    ] {
        let v = analyze_json(&path, &extra);
        assert_eq!(v["stats"]["parsed"].as_u64().unwrap(), parsed, "{extra:?}");
        assert_eq!(
            v["stats"]["fallback"].as_u64().unwrap(),
            fallback,
            "{extra:?}"
        );
        if parsed == 1 {
            assert_spark_ctas_edge(&v);
        }
    }
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn cli_dialect_validation() {
    for sub in ["analyze", "serve"] {
        let out = Command::new(env!("CARGO_BIN_EXE_hive-lineage"))
            .args([sub, "--dialect", "mysql"])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2));
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("mysql"), "{stderr}");

        let out = Command::new(env!("CARGO_BIN_EXE_hive-lineage"))
            .args([sub, "--help"])
            .output()
            .unwrap();
        assert!(out.status.success());
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.contains("--dialect"), "{stdout}");
        assert!(stdout.contains("hive"), "{stdout}");
        assert!(stdout.contains("spark"), "{stdout}");
    }
}
