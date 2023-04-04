use crate::data::log_record::LogRecordPos;
use crate::index::Indexer;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

// BTree 索引，主要封装了标准库中的 BTreeMap 结构
pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl BTree {
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let mut write_guard = self.tree.write();
        let remove_res = write_guard.remove(&key);
        remove_res.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_put() {
        let bt = BTree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert_eq!(res1, true);
    }

    #[test]
    fn test_btree_get() {
        let bt = BTree::new();
        bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        let res1 = bt.get("".as_bytes().to_vec());
        assert!(res1.is_some());
        assert_eq!(res1.unwrap().file_id, 1);
        assert_eq!(res1.unwrap().offset, 10);
    }

    #[test]
    fn test_btree_delete() {
        let bt = BTree::new();
        bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        let res1 = bt.delete("".as_bytes().to_vec());
        assert_eq!(res1, true);
        let res2 = bt.delete("aaa".as_bytes().to_vec());
        assert_eq!(res2, false);
    }
}
