//! 表 ID -> 列符号 ID 目录，按语句顺序逐步积累（DDL / CTAS / 血缘中出现的列）

use rustc_hash::FxHashMap as HashMap;

pub type ColId = u32;
pub const STAR_COL: ColId = 0;

/// 列名符号驻留池（消除重复列名字符串分配与哈希开销）
pub struct SymPool {
    strs: Vec<String>,
    ids: HashMap<String, ColId>,
}

impl Default for SymPool {
    fn default() -> Self {
        Self::new()
    }
}

impl SymPool {
    pub fn new() -> Self {
        let mut pool = SymPool {
            strs: Vec::with_capacity(128),
            ids: HashMap::default(),
        };
        let star = "*".to_string();
        pool.strs.push(star.clone());
        pool.ids.insert(star, STAR_COL);
        pool
    }

    #[inline]
    pub fn intern(&mut self, s: &str) -> ColId {
        if let Some(&id) = self.ids.get(s) {
            return id;
        }
        let id = self.strs.len() as ColId;
        let owned = s.to_string();
        self.strs.push(owned.clone());
        self.ids.insert(owned, id);
        id
    }

    #[inline]
    pub fn resolve(&self, id: ColId) -> &str {
        &self.strs[id as usize]
    }
}

#[derive(Default)]
pub struct Catalog {
    /// 表 ID -> 有序去重列符号列表（None 表示该表尚无已知列目录）
    cols: Vec<Option<Vec<ColId>>>,
}

impl Catalog {
    fn ensure(&mut self, tid: usize) {
        if self.cols.len() <= tid {
            self.cols.resize_with(tid + 1, || None);
        }
    }

    /// 覆盖式登记（DDL / CTAS）
    pub fn register(&mut self, tid: usize, cols: impl IntoIterator<Item = ColId>) {
        self.ensure(tid);
        let mut list = Vec::new();
        for c in cols {
            if !list.contains(&c) {
                list.push(c);
            }
        }
        self.cols[tid] = Some(list);
    }

    /// 追加单列（血缘中出现的新列）
    #[inline]
    pub fn add_column(&mut self, tid: usize, col: ColId) {
        self.ensure(tid);
        let entry = self.cols[tid].get_or_insert_with(Vec::new);
        if !entry.contains(&col) {
            entry.push(col);
        }
    }

    #[inline]
    pub fn get(&self, tid: usize) -> Option<&[ColId]> {
        self.cols.get(tid).and_then(|v| v.as_deref())
    }

    /// 把源表的列目录拷贝并入目标表（用于 CREATE TABLE LIKE / CLONE）
    pub fn copy_from(&mut self, dst_tid: usize, src_tid: usize) {
        let Some(src) = self.get(src_tid).map(|s| s.to_vec()) else {
            return;
        };
        for c in src {
            self.add_column(dst_tid, c);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sym_pool_default() {
        let mut p = SymPool::default();
        let a = p.intern("x");
        let b = p.intern("x");
        assert_eq!(a, b);
    }
}
