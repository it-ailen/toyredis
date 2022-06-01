use std::{rc::Rc, cell::{Cell, RefCell}, borrow::{BorrowMut, Borrow}, iter::Skip};
use rand::{self, Rng};
use core::cmp::Ordering;
use std::fmt::Debug;

#[derive(Debug)]
pub struct Skiplist<Member: PartialEq> {
    // /// 指向 level-0 的头部
    // head: *mut Node<Member>,
    // /// 指向 level-0 的尾部
    // tail: *mut Node<Member>,
    /// 各层的链表头
    level_links: Vec<*mut Node<Member>>,
    /// skiplist 的层级
    level: usize,
    /// 快表中的长度，即 level-0 中的节点数
    length: usize,
    /// 随机跳跃的概率，取值在 0~100 之间
    skip_percentage: usize,
}

// impl<T: PartialEq + Debug> Debug for Skiplist<T> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         let res = f.debug_struct("Skiplist")
//             .field("length", &self.length)
//             .field("level", &self.level);
        
        
//         f.debug_struct("Skiplist").field("level_links", &self.level_links).field("level", &self.level).field("length", &self.length).field("skip_percentage", &self.skip_percentage).finish()
//     }
// }

// type Node<Member> = Rc<RefCell<SkiplistNode<Member>>>;

const MAX_LEVELS: usize = 32;
const DEFAULT_SKIP_PERCENTAGE: usize = 25;


struct Node<Member: PartialEq> {
    pub score: f64,
    /// 存入数据
    pub data: Member,
    /// 各层链表。层级越高，索引级别越高。
    levels: Vec<*mut Node<Member>>,
    /// 指向前一个节点
    backward: *mut Node<Member>,
}

impl<T: PartialEq + Debug> Debug for Node<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("score", &self.score)
            .field("data", &self.data)
            .field("level", &self.levels.len())
            .finish()
    }
}

impl<T: PartialEq> PartialEq for Node<T> {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.data == other.data
    }
}

impl<T: PartialEq + PartialOrd> PartialOrd for Node<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(if self.score < other.score || (self.score == other.score && self.data < other.data) {
            std::cmp::Ordering::Less
        } else if self.score == other.score && self.data == other.data {
            std::cmp::Ordering::Equal
        } else {
            std::cmp::Ordering::Greater
        })
    }
}

impl<M: PartialEq> Drop for Skiplist<M> {
    fn drop(&mut self) {
        if self.length == 0 {
            return
        }
        let mut next = self.level_links[0];
        while !next.is_null() {
            let tail = unsafe {(*next).levels[0]};
            unsafe {
                (*next).backward = std::ptr::null_mut();
                (*next).levels = vec![];
                let _ = Box::from_raw(next);
            }
            next = tail;
            self.length -=1;
        }
        assert_eq!(self.length, 0);
        self.level = 0;
        self.level_links = vec![];
    }
}

#[derive(Debug)]
pub struct RangeItem<T> {
    /// 分数
    pub score: f64,
    /// 实际内容
    pub data: T,
    skiplevel: usize,
}

impl<T> RangeItem<T> {
    fn new(score: f64, data: T, skiplevel: usize) -> Self {
        Self { score, data, skiplevel }
    }
}


impl<Member> Skiplist<Member>
where Member: Ord 
{
    pub fn new() -> Self {
        Self { 
            // head: std::ptr::null_mut(), 
            // tail: std::ptr::null_mut(), 
            level_links: vec![],
            level: 0, 
            length: 0,
            skip_percentage: DEFAULT_SKIP_PERCENTAGE,
        }
    }

    fn cmp(left: (f64, &Member), right: (f64, &Member)) -> core::cmp::Ordering {
        if left.0 < right.0 || (left.0 == right.0 && left.1 < right.1) {
            Ordering::Less
        } else if left.0 == right.0 && left.1 == right.1 {
            Ordering::Equal
        } else {
            Ordering::Greater
        }
    }

    pub fn insert(&mut self, data: Member, score: f64) {
        let level = self.random_level();
        self.do_insert(data, score, level)
    }

    fn do_insert(&mut self, data: Member, score: f64, level: usize) {
        // empty skiplist, insert node directly
        let new_node  = Box::new(Node::new(data, score, level));
        // 消费掉 Box 外壳，并返回内部数据指针。这是 rust 主动分配堆数据的经典操作
        let new_node = Box::into_raw(new_node);
        for _ in self.level..level {
            // 补充链表头，新增的 level 直接从头指向
            self.level_links
                .push(new_node);
        }
        if self.length == 0 {
            // 原来为空，直接加上
            self.length += 1;
            self.level = level;
            return
        }
        // find position first
        let mut slow: *mut Node<Member> = std::ptr::null_mut();// 指向上一个，空表示在 skiplist 起点
        'out: for level_cursor in (0..level.min(self.level)).rev() {
            let mut next = if slow.is_null() {
                self.level_links[level_cursor]
            } else {
                unsafe {
                    (*slow).levels[level_cursor]
                }
            };
            while !next.is_null() {
                let next_score = unsafe {
                    (*next).score
                };
                let next_data = unsafe {
                    &(*next).data
                };
                match Self::cmp((score, unsafe{&(*new_node).data}), (next_score, next_data)) {
                    Ordering::Less => {
                        // 就在当前区间
                        unsafe {
                            (*new_node).levels[level_cursor] = next;
                        }
                        if slow.is_null() {
                            self.level_links[level_cursor] = new_node;
                        } else {
                            unsafe {
                                (*slow).levels[level_cursor] = new_node;
                            }
                        }
                        if level_cursor > 0 {
                            // 未到第0层，则继续找下一层
                            continue 'out;
                        }
                        // 已经到 0层了，需要加了 backward 指针
                        unsafe {
                            (*next).backward = new_node;
                        }
                        if !slow.is_null() {
                            unsafe {
                                (*new_node).backward = slow;
                            }
                        }
                    },
                    Ordering::Equal => {
                        // 不允许重复插入
                        return;
                    },
                    _ => {
                        // 后一个区间，slow 就移位
                        slow = next;
                        next = unsafe {
                            (*slow).levels[level_cursor]
                        };
                    },
                }
            }
            // 一直到结尾, new_node 同层后就没有数据了
            if slow.is_null() {
                self.level_links[level_cursor] = new_node;
            } else {
                unsafe {
                    (*slow).levels[level_cursor] = new_node;
                }
            }
            if level_cursor == 0 {
                if !slow.is_null() {
                    unsafe {
                        (*new_node).backward = slow;
                    }
                }
            }
        }
        self.length += 1;
        if level > self.level {
            self.level = level;
        }
    }

    fn do_find(&self, score: f64, data: &Member) -> Option<&Node<Member>> {
        if self.length == 0 {
            return None
        }
        let mut slow: *mut Node<Member> = std::ptr::null_mut();
        'out: for level_cursor in (0..self.level).rev() {
            let mut next = if slow.is_null() {
                self.level_links[level_cursor]
            } else {
                unsafe {
                    (*slow).levels[level_cursor]
                }
            };
            while !next.is_null() {
                let next_score = unsafe {
                    (*next).score
                };
                let next_data = unsafe {
                    &(*next).data
                };
                match Self::cmp((score, data), (next_score, next_data)) {
                    Ordering::Less => {
                        if level_cursor > 0 {
                            continue 'out;
                        }
                        return None
                    },
                    Ordering::Equal => {
                        return Some(unsafe{&(*next)})
                    },
                    Ordering::Greater => {
                        slow = next;
                        next = unsafe {
                            (*slow).levels[level_cursor]
                        };
                        continue
                    },
                };
            }
        }
        None
    }

    /// 查找 (score, data) 是否在表内
    pub fn exists(&self, score: f64, data: &Member) -> bool {
        self.do_find(score, data).is_some()
    }

    pub fn remove(&mut self, score: f64, data: &Member) -> bool {
        if self.length == 0 {
            return false;
        }
        let mut slow: *mut Node<Member> = std::ptr::null_mut();
        'out: for cur_level in (0..self.level).rev() {
            let mut next = if slow.is_null() {
                self.level_links[cur_level]
            } else {
                unsafe {
                    (*slow).levels[cur_level]
                }
            };
            while !next.is_null() {
                let next_score = unsafe {
                    (*next).score
                };
                let next_data = unsafe {
                    &(*next).data
                };
                match Self::cmp((score, data), (next_score, next_data)) {
                    Ordering::Less => {
                        // 在区间之间
                        if cur_level > 0 {
                            continue 'out;
                        }
                        // 扫描完成，没有发现
                        return false;
                    },
                    Ordering::Equal => {
                        if slow.is_null() {
                            self.level_links[cur_level] = unsafe {(*next).levels[cur_level]};
                        } else {
                            unsafe {
                                (*slow).levels[cur_level] = (*next).levels[cur_level];
                            }
                        }
                        if cur_level == 0 {
                            if !slow.is_null() {
                                unsafe {
                                    (*(*next).levels[0]).backward = slow;
                                }
                            }
                            self.length -= 1;
                            let _ = unsafe {Box::from_raw(next)}; // 清除
                            return true
                        }
                        continue 'out;
                    },
                    Ordering::Greater => {
                        slow = next;
                        next = unsafe {
                            (*slow).levels[cur_level]
                        };
                        continue;
                    },
                }
            }
        }
        false
    }

    /// 随机当前结点的该跳的层次
    fn random_level(&self) -> usize {
        let mut rand_gen = rand::thread_rng();
        let mut level = 1;
        while rand_gen.gen_ratio(self.skip_percentage as u32, 100) {
            level += 1
        }
        if level >= MAX_LEVELS {
            MAX_LEVELS
        } else {
            level
        }
    }

    fn do_range_tuple(&self, min: Option<f64>, max: Option<f64>, offset: usize, limit: usize) -> Vec<(f64, &Member, usize)> {
        self.do_range(min, max, offset, limit)
            .into_iter()
            .map(|i| (i.score, i.data, i.skiplevel))
            .collect()
    }

    fn do_range(&self, min: Option<f64>, max: Option<f64>, mut offset: usize, mut limit: usize) -> Vec<RangeItem<&Member>> {
        if limit == 0 {
            limit = usize::MAX;
        }
        let mut result = vec![];
        if self.length == 0 {
            return result
        }
        let mut first = self.level_links[0];
        if let Some(min_score) = min {
            let mut slow: *mut Node<Member> = std::ptr::null_mut();
            'out: for level in (0..self.level).rev() {
                let mut next = if slow.is_null() {
                    self.level_links[level]
                } else {
                    unsafe {
                        (*slow).levels[level]
                    }
                };
                while !next.is_null() {
                    let next_score = unsafe{(*next).score};
                    if min_score <= next_score {
                        // 起始点在范围内
                        if level > 0 {
                            continue 'out;
                        }
                        // 已经到第0层了，可以通过 backword 往 前找
                        let mut pre = unsafe {
                            (*next).backward
                        };
                        first = next;
                        while !pre.is_null() {
                            let pre_score = unsafe {(*pre).score};
                            if pre_score <= min_score {
                                first = pre;
                                pre = unsafe{ (*pre).backward };
                                continue;
                            } else {
                                break;
                            }
                        }
                    } else {
                        // 起始点在下一个区间
                        slow = next;
                        next = unsafe {
                            (*slow).levels[level]
                        };
                    }
                }
            }
        }
        let mut cursor = first;
        while !cursor.is_null() {
            if offset > 0 {
                offset -= 1;
                cursor = unsafe {(*cursor).levels[0]};
                continue;
            }
            if limit == 0 {
                break;
            }
            if let Some(m) = max {
                if m < unsafe{(*cursor).score} {
                    break;
                }
            }
            limit -= 1;
            result.push(RangeItem{
                score: unsafe{(*cursor).score},
                data: unsafe{&(*cursor).data},
                skiplevel: unsafe{(*cursor).levels.len()},
            });
            cursor = unsafe{(*cursor).levels[0]};
        }
        result
    }
}

impl<Member: PartialEq> Node<Member> {
    pub fn new(data: Member, score: f64, level: usize) -> Self {
        Self {
            score,
            data,
            levels: vec![std::ptr::null_mut(); level],
            backward: std::ptr::null_mut(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::ds::skiplist::RangeItem;

    use super::Skiplist;

    #[test]
    fn basis() {
        let mut list = Skiplist::new();
        list.do_insert(2, 2f64, 2);
        assert_eq!(list.length, 1);
        assert_eq!(list.level, 2);
        assert_eq!(list.level_links.len(), list.level);
        assert!(list.exists(2f64, &2));
        println!("list: {:?}", list);
        let r: Vec<(f64, &i32, usize)> = list.do_range_tuple(None, None, 0, 0);
        assert_eq!(r, vec![(2f64, &2, 2)]);
        assert!(list.remove(2f64, &2));
        assert_eq!(list.length, 0);
        assert_eq!(list.level, 2);
    }
}