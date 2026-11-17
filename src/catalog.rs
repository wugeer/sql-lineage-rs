//! 表 ID -> 列符号 ID 目录，按语句顺序逐步积累（DDL / CTAS / 血缘中出现的列）

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

pub type ColId = u32;
pub const STAR_COL: ColId = 0;
const SET_THRESHOLD: usize = 16;

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
    /// 宽表去重辅助集合（列数超过 SET_THRESHOLD 时启用，避免 O(C^2) 线性查重）
    col_sets: Vec<Option<HashSet<ColId>>>,
    /// 每张表的列目录修改版本号（用于缓存 RelCols）
    vers: Vec<u32>,
}

impl Catalog {
    fn ensure(&mut self, tid: usize) {
        if self.cols.len() <= tid {
            self.cols.resize_with(tid + 1, || None);
            self.col_sets.resize_with(tid + 1, || None);
            self.vers.resize(tid + 1, 0);
        }
    }

    #[inline]
    pub fn version(&self, tid: usize) -> u32 {
        self.vers.get(tid).copied().unwrap_or(0)
    }

    /// 覆盖式登记（DDL / CTAS）
    pub fn register(&mut self, tid: usize, cols: impl IntoIterator<Item = ColId>) {
        self.ensure(tid);
        let mut list = Vec::new();
        let mut set: Option<HashSet<ColId>> = None;
        for c in cols {
            if let Some(s) = &mut set {
                if s.insert(c) {
                    list.push(c);
                }
            } else if !list.contains(&c) {
                list.push(c);
                if list.len() >= SET_THRESHOLD {
                    let mut hs: HashSet<ColId> =
                        HashSet::with_capacity_and_hasher(list.len() * 2, Default::default());
                    hs.extend(list.iter().copied());
                    set = Some(hs);
                }
            }
        }
        self.cols[tid] = Some(list);
        self.col_sets[tid] = set;
        self.vers[tid] = self.vers[tid].wrapping_add(1);
    }

    /// 追加单列（血缘中出现的新列）
    #[inline]
    pub fn add_column(&mut self, tid: usize, col: ColId) {
        self.ensure(tid);
        let entry = self.cols[tid].get_or_insert_with(Vec::new);
        if let Some(s) = &mut self.col_sets[tid] {
            if s.insert(col) {
                entry.push(col);
                self.vers[tid] = self.vers[tid].wrapping_add(1);
            }
            return;
        }
        if !entry.contains(&col) {
            entry.push(col);
            if entry.len() >= SET_THRESHOLD {
                let mut hs: HashSet<ColId> =
                    HashSet::with_capacity_and_hasher(entry.len() * 2, Default::default());
                hs.extend(entry.iter().copied());
                self.col_sets[tid] = Some(hs);
            }
            self.vers[tid] = self.vers[tid].wrapping_add(1);
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

    #[test]
    fn wide_table_set_threshold() {
        let mut p = SymPool::new();
        let mut cat = Catalog::default();
        let cols: Vec<ColId> = (0..24).map(|i| p.intern(&format!("c_{i}"))).collect();
        cat.register(0, cols.iter().copied().chain(cols.iter().copied()));
        assert_eq!(cat.get(0).unwrap().len(), 24);
        assert!(cat.version(0) > 0);

        for &c in &cols {
            cat.add_column(1, c);
        }
        cat.add_column(1, cols[0]);
        let extra = p.intern("c_extra");
        cat.add_column(1, extra);
        assert_eq!(cat.get(1).unwrap().len(), 25);
    }
}
