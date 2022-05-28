//! 一般的 hash table 性能没问题，但有一个问题是在 rehash 时，会导致所有数据被 copy 并 rehash.
//! redis 版本的 hash table 用两个常规的 hash table 换着用
//! redis 的 sds 采用 siphash 方法，这在 std::hash 中有提供，所以直接使用
//! 

use std::{hash::{Hash, Hasher}, collections::hash_map::DefaultHasher, borrow::{BorrowMut, Borrow}};

use super::perfstr::sds::SDS;

/// redis 版本 hash table，由两个 hash table 交替组成，支持渐进式 rehash（即将单次全部 rehash 这样的耗时逻辑处理成一次请求处理若干个 slot 的渐进方式）。
pub struct Dict<V> {
    main_table: HashTable<SDS, V>,
    back_table: Option<HashTable<SDS, V>>,
    /// 正在 rehashing?
    /// rehash 所在的 slot index，这个只针对 main_table
    rehash_idx: Option<usize>,
}

impl<V: Default> Dict<V> {
    pub fn new() -> Self {
        Self { 
            main_table: HashTable::<SDS, V>::with_capacity(0), 
            back_table: None, 
            rehash_idx: None,
        }
    }

    fn is_rehashing(&self) -> bool {
        self.rehash_idx.is_some()
    }

    fn start_rehashing(&mut self) {
        if self.is_rehashing() {
            return
        }
        // 每次扩2倍
        self.back_table = Some(HashTable::with_capacity(2*self.main_table.slots_cnt())); 
        self.rehash_idx = Some(0);
    }

    /// 渐进 rehash。每步(step)只 rehash 几个 slots。
    /// 10个空 slot 也算一步
    fn try_rehash_step(&mut self, mut step: usize) {
        if !self.is_rehashing() {
            return;
        }
        let mut start_idx = self.rehash_idx.unwrap();
        let mut latest_idx = start_idx;
        let max_slots_idx_to_check = (10 * step + start_idx).max(self.main_table.slots_cnt() as usize - 1);
        for idx in start_idx..=max_slots_idx_to_check {
            latest_idx = idx;
            let mut cursor = &mut self.main_table.slots[idx];
            if cursor.is_none() {
                // 本来就没有
                continue
            }
            loop {
                match cursor {
                    None => break,
                    Some(node) => {
                        let key = std::mem::replace(&mut node.k, SDS::empty());
                        let value = std::mem::take(&mut node.v);
                        self.back_table.as_mut().unwrap().insert(key, value);
                        self.main_table.cnt -= 1;
                        cursor = &mut node.next;
                    },
                }
            }
            self.main_table.slots[idx] = None; // 清空该 slot
            step -= 1;
            if step <= 0 || self.main_table.cnt == 0 {
                break;
            }
        }
        if self.main_table.cnt == 0 || latest_idx >= self.main_table.slots_cnt() as usize {
            // 已经 rehash 完成
            self.rehash_idx = None;
            let new_table = self.back_table.take().unwrap();
            self.main_table = new_table;
            return
        }
        self.rehash_idx = Some(latest_idx);
    }

    /// 返回当前表中所有的值数量
    pub fn value_cnt(&self) -> u64 {
        self.main_table.cnt + if let Some(bak) = &self.back_table {
            bak.cnt
        } else {
            0
        }
    }
    /// 新增 kv
    pub fn insert(&mut self, key: SDS, v: V) -> Option<V> {
        self.try_rehash_step(1);
        if self.is_rehashing() {
            let old_in_main = self.main_table.remove(&key);
            let old = self.back_table
                .as_mut()
                .unwrap()
                .insert(key, v);
            if old.is_some() {
                // 已经迁移或者新增到新表了，不需要检查旧表
                old
            } else {
                old_in_main
            }
        } else {
            let old = self.main_table.insert(key, v);
            if old.is_none() {
                // 新增的，且不在 rehashing ，则考虑开启 rehashing
                if self.main_table.need_expand() {
                    self.start_rehashing();
                }
            }
            old
        }
    }

    /// 删除
    pub fn remove(&mut self, key: &SDS) -> Option<V> {
        self.try_rehash_step(1);
        let new_val = self.back_table
            .as_mut()
            .and_then(|t| t.remove(key));
        if new_val.is_some() {
            new_val
        } else {
            self.main_table.remove(key)
        }
    }

    /// 查找 value
    pub fn get(&mut self, key: &SDS) -> Option<&V> {
        if self.value_cnt() == 0 {
            return None;
        }
        self.try_rehash_step(1);
        self.back_table.as_ref()
            .and_then(|table| table.get(key))
            .or_else(|| self.main_table.get(key))
    }
}

/// 非 rust 内置的 hash table，用于对齐 redis 实现，自己实现主要是为了支持渐进式 rehash。
struct HashTable<K: Hash, V> {
    slots: Vec<HashEntry<K, V>>,
    /// 当前 hash table 中存在的数据量
    cnt: u64,
    /// slots 数以2为底的指数值，即 self.slots.len() = 1usize << self.slot_cnt_exp。这是为了方便分配及取模
    slot_cnt_exp: u64,
}

type HashEntry<K, V> = Option<Box<Node<K, V>>>;

// #[derive(Clone, Copy)]
/// 存放在 hash slot 中的项，使用单链表方式解决 hash 冲突。
struct Node<K, V> {
    k: K,
    v: V,
    next: HashEntry<K, V>,
}

impl<K: Hash, V> Node<K, V> {
    fn new(k: K, v: V) -> Self {
        Self { k: k, v: v, next:None }
    }
}

macro_rules! remain {
    ($val:expr, $exp:expr) => {
        ($val & ((1u64 << $exp) - 1)) as usize
    };
}


const MIN_EXP: u64 = 3;

impl<K, V: Default> HashTable<K, V>
where K: Eq + Hash, 
{
    pub fn with_capacity(size: u64) -> Self {
        let slot_cnt_exp = Self::compute_exp(size);
        let size = (1u64<<slot_cnt_exp) as usize;
        let mut slots = Vec::new();
        slots.resize_with(size, || None);
        Self { slots, cnt: 0, slot_cnt_exp}
    }

    fn slots_cnt(&self) -> u64 {
        1 << self.slot_cnt_exp
    }

    /// 需要扩展？
    /// 参考 redis 版本，使用最简单的数据量>=slots 数量来判断
    pub fn need_expand(&self) -> bool {
        return self.cnt >= self.slots_cnt()
    }

    fn compute_exp(size: u64) -> u64 {
        assert!(size <= 63);
        for i in MIN_EXP..size {
            if 1u64 << i >= size {
                return i
            }
        }
        64
    }

    fn gen_hash<Q: ?Sized>(key: &Q) -> u64
        where K: Borrow<Q>,
        Q: Hash + Eq, 
    {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    ///
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where K: Borrow<Q>,
        Q: Hash + Eq + ?Sized, 
    {
        let hash = Self::gen_hash(key);
        let slot_idx = remain!(hash, self.slot_cnt_exp);
        let mut cursor = self.slots[slot_idx].as_ref();
        while let Some(cur) = cursor {
            if key.borrow() == cur.k.borrow() {
                return Some(&cur.v)
            }
            cursor = cur.next.as_ref();
        }
        None
    }

    /// 插入 key，并返回原有值.
    pub fn insert(&mut self, key: K, v: V) -> Option<V> {
        let hash = Self::gen_hash(key.borrow());
        let slot_idx = remain!(hash, self.slot_cnt_exp); 
        let mut cursor = &mut self.slots[slot_idx];
        loop {
            match cursor {
                None => {
                    // 到了链表最后一个
                    let node = Node::new(key, v);
                    *cursor = Some(Box::new(node));
                    self.cnt += 1;
                    return None
                },
                Some(ori) if ori.k == key => {
                    let old = std::mem::replace(&mut ori.v, v);
                    return Some(old)
                },
                Some(node) => {
                    cursor = &mut node.next;
                },
            }
        }
    }

    /// 删除 key
    pub fn remove<Q>(&mut self, key: &Q) -> Option<V> 
        where K: Borrow<Q>,
        Q: Hash + Eq + ?Sized, 
    {
        let hash = Self::gen_hash(key);
        let slot_idx = remain!(hash, self.slot_cnt_exp);
        if self.slots[slot_idx].is_none() {
            return None
        }
        let mut fast = &mut self.slots[slot_idx];
        loop {
            match fast {
                None => {
                    return None
                },
                Some(node) if node.k.borrow() == key.borrow() => {
                    let v = std::mem::take(&mut node.v);
                    *fast = node.next.take();
                    self.cnt -= 1;
                    return Some(v);
                }, 
                Some(node) => {
                    fast = &mut node.next;
                }
            }
        }
    }
}

#[cfg(test)]
mod test_hashtable {
    use crate::ds::dict::MIN_EXP;

    use super::HashTable;

    #[test]
    fn basis_copy_key() {
        let mut table = HashTable::with_capacity(4);
        assert_eq!(table.cnt, 0);
        assert_eq!(table.slot_cnt_exp, MIN_EXP);
        table.insert("first".to_string(), 1);
        let val = table.get("first");
        assert!(val.is_some());
        assert_eq!(*val.unwrap(), 1);
        let second = "second".to_string();
        table.insert(second, 2);
        assert_eq!(table.cnt, 2);
        let val = table.get("second");
        assert_eq!(*val.unwrap(), 2);

        assert!(table.remove("third").is_none());
        assert_eq!(table.cnt, 2);

        assert_eq!(table.remove(&"second".to_string()).unwrap(), 2);
        assert_eq!(table.cnt, 1); 
    }
}