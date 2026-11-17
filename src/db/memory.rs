//! 内存追加存储后端（严格模拟 Append-Only 批次表，用于单元/集成测试与 `memory://` 快速体验）

use crate::db::hot_index::HotGraphIndex;
use crate::db::{
    BatchPayload, BoxFut, JobSearchItem, LineageBackend, LineageDirection, QueryScope,
    RawColumnSubgraph, RawTableSubgraph, RawTaskSubgraph, TableSearchItem,
};
use rustc_hash::FxHashMap as HashMap;
use std::sync::RwLock;

#[derive(Default)]
pub struct MemoryBackend {
    batches: RwLock<Vec<BatchPayload>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    fn build_index(&self, scope: QueryScope) -> HotGraphIndex {
        let guard = self.batches.read().unwrap();
        HotGraphIndex::from_batches(&guard, scope == QueryScope::AllHistory)
    }
}

impl LineageBackend for MemoryBackend {
    fn backend_name(&self) -> &'static str {
        "memory"
    }

    fn ping(&self) -> BoxFut<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn ensure_schema(&self) -> BoxFut<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn append_batch<'a>(&'a self, payload: &'a BatchPayload) -> BoxFut<'a, ()> {
        Box::pin(async move {
            self.batches.write().unwrap().push(payload.clone());
            Ok(())
        })
    }

    fn load_latest_snapshot(&self) -> BoxFut<'_, Vec<BatchPayload>> {
        Box::pin(async move {
            let guard = self.batches.read().unwrap();
            let mut latest: HashMap<String, BatchPayload> = HashMap::default();
            for bp in guard.iter() {
                match latest.get(&bp.batch.job_name) {
                    Some(cur) if cur.batch.batch_id >= bp.batch.batch_id => {}
                    _ => {
                        latest.insert(bp.batch.job_name.clone(), bp.clone());
                    }
                }
            }
            let mut out: Vec<BatchPayload> = latest.into_values().collect();
            out.sort_by_key(|x| x.batch.batch_id);
            Ok(out)
        })
    }

    fn fetch_batches_since(&self, after_batch_id: i64) -> BoxFut<'_, Vec<BatchPayload>> {
        Box::pin(async move {
            let guard = self.batches.read().unwrap();
            let mut out: Vec<BatchPayload> = guard
                .iter()
                .filter(|bp| bp.batch.batch_id > after_batch_id)
                .cloned()
                .collect();
            out.sort_by_key(|x| x.batch.batch_id);
            Ok(out)
        })
    }

    fn search_jobs<'a>(
        &'a self,
        keyword: &'a str,
        limit: usize,
        scope: QueryScope,
    ) -> BoxFut<'a, Vec<JobSearchItem>> {
        Box::pin(async move { Ok(self.build_index(scope).search_jobs(keyword, limit)) })
    }

    fn search_tables<'a>(
        &'a self,
        keyword: &'a str,
        limit: usize,
        scope: QueryScope,
    ) -> BoxFut<'a, Vec<TableSearchItem>> {
        Box::pin(async move { Ok(self.build_index(scope).search_tables(keyword, limit)) })
    }

    fn query_table_subgraph<'a>(
        &'a self,
        table: &'a str,
        dir: LineageDirection,
        max_hops: u32,
        scope: QueryScope,
    ) -> BoxFut<'a, RawTableSubgraph> {
        Box::pin(async move {
            Ok(self
                .build_index(scope)
                .query_table_subgraph(table, dir, max_hops))
        })
    }

    fn query_table_task_subgraph<'a>(
        &'a self,
        table: &'a str,
        dir: LineageDirection,
        max_hops: u32,
        scope: QueryScope,
    ) -> BoxFut<'a, RawTaskSubgraph> {
        Box::pin(async move {
            Ok(self
                .build_index(scope)
                .query_table_task_subgraph(table, dir, max_hops))
        })
    }

    fn query_task_subgraph<'a>(
        &'a self,
        job_name: &'a str,
        dir: LineageDirection,
        max_hops: u32,
        scope: QueryScope,
    ) -> BoxFut<'a, RawTaskSubgraph> {
        Box::pin(async move {
            Ok(self
                .build_index(scope)
                .query_task_subgraph(job_name, dir, max_hops))
        })
    }

    fn query_column_subgraph<'a>(
        &'a self,
        full_table: &'a str,
        column: &'a str,
        dir: LineageDirection,
        max_hops: u32,
        scope: QueryScope,
    ) -> BoxFut<'a, RawColumnSubgraph> {
        Box::pin(async move {
            Ok(self
                .build_index(scope)
                .query_column_subgraph(full_table, column, dir, max_hops))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::extract::extract_batch_payload;
    use crate::{analyze_with_sources, InputFile, Options, SqlDialect};

    fn make_batch(job: &str, sql: &str) -> BatchPayload {
        let ws = analyze_with_sources(
            vec![InputFile {
                name: "m.sql".into(),
                bytes: sql.as_bytes().to_vec(),
            }],
            &Options {
                job_name: Some(job.into()),
                analyze_select: Some(false),
                ..Options::default()
            },
        );
        extract_batch_payload(job, SqlDialect::Hive, None, &ws).unwrap()
    }

    #[tokio::test]
    async fn memory_snapshot_and_delta_fetch() {
        let mem = MemoryBackend::new();
        mem.ensure_schema().await.unwrap();
        mem.ping().await.unwrap();

        let b1 = make_batch("dag.a", "INSERT INTO dwd.t SELECT x FROM ods.s1;");
        let b2 = make_batch("dag.a", "INSERT INTO dwd.t SELECT x FROM ods.s2;");
        let b3 = make_batch("dag.b", "INSERT INTO dws.u SELECT x FROM dwd.t;");
        mem.append_batch(&b1).await.unwrap();
        mem.append_batch(&b2).await.unwrap();
        mem.append_batch(&b3).await.unwrap();

        let snap = mem.load_latest_snapshot().await.unwrap();
        assert_eq!(snap.len(), 2);
        assert!(snap.iter().any(|p| p.batch.batch_id == b2.batch.batch_id));
        assert!(snap.iter().any(|p| p.batch.batch_id == b3.batch.batch_id));

        let delta = mem.fetch_batches_since(b1.batch.batch_id).await.unwrap();
        assert_eq!(delta.len(), 2);
        assert_eq!(delta[0].batch.batch_id, b2.batch.batch_id);
        assert_eq!(delta[1].batch.batch_id, b3.batch.batch_id);
    }
}
