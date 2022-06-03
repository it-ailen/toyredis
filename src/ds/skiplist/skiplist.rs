use rand::Rng;
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
    /// 各层距离下一个节点的距离（中间的节点数）。这是为了提高查找效率
    level_spans: Vec<usize>,
    /// skiplist 的层级
    level: usize,
    /// 快表中的长度，即 level-0 中的节点数
    length: usize,
    /// 随机跳跃的概率，取值在 0~100 之间
    skip_percentage: usize,
}

const MAX_LEVELS: usize = 32;
const DEFAULT_SKIP_PERCENTAGE: usize = 25;


struct Node<Member: PartialEq> {
    pub score: f64,
    /// 存入数据
    pub data: Member,
    /// 各层链表。层级越高，索引级别越高。
    pub levels: Vec<*mut Node<Member>>,
    /// 距离同层下个节点间的距离（中间的节点数）。这是为了提高查找效率
    spans: Vec<usize>,
    /// 指向前一个节点
    pub backward: *mut Node<Member>,
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
                let _ = Box::from_raw(next);
            }
            next = tail;
            self.length -=1;
        }
        assert_eq!(self.length, 0);
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


/// 边界
pub struct Bound {
    /// 边界分数
    bound: f64,
    /// 是否排除边界
    exclusive: bool,
}

impl Bound {
    pub fn new(bound: f64, exclusive: bool) -> Self {
        Self { bound, exclusive }
    }

    pub fn new_exclusive(bound: f64) -> Self {
        Self { bound, exclusive: true }
    }

    pub fn new_inclusive(bound: f64) -> Self {
        Self { bound, exclusive: false }
    }

    fn toggle(&self) -> Self {
        Self { exclusive: !self.exclusive, ..(*self) }
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
            level_spans: vec![],
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
        self.do_insert(data, score, level);
    }

    fn do_insert(&mut self, data: Member, score: f64, level: usize) -> Option<*mut Node<Member>> {
        // empty skiplist, insert node directly
        let new_node  = Box::new(Node::new(data, score, level));
        // 消费掉 Box 外壳，并返回内部数据指针。这是 rust 主动分配堆数据的经典操作
        let new_node = Box::into_raw(new_node);
        for _ in self.level..level {
            // 补充链表头，新增的 level 直接从头指向
            self.level_links
                .push(new_node);
            // for new levels, set length as initial span
            self.level_spans
                .push(self.length);
        }
        if self.length == 0 {
            // 原来为空，直接加上
            self.length += 1;
            self.level = level;
            return Some(new_node);
        }
        // 指向上一个，空表示在 skiplist 起点
        let mut slow: *mut Node<Member> = std::ptr::null_mut();
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
                        break 'out;
                    },
                    Ordering::Equal => {
                        // 不允许重复插入
                        return None;
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
        // 修正 span
        'out2: for level_cursor in 1..level {
            let mut slow: *mut Node<Member> = std::ptr::null_mut();
            let mut slow_span = self.level_spans[level_cursor];
            let mut next = self.level_links[level_cursor];
            loop {
                if next as u64 == new_node as u64 {
                    // 已经到达最后一个
                    let mut pre = unsafe {
                        (*new_node).backward
                    };
                    let mut span_before = 0;
                    while !pre.is_null() && pre != slow {
                        pre = unsafe {
                            (*pre).backward
                        };
                        span_before += 1;
                    }
                    let span_after = slow_span - span_before;
                    unsafe {
                        (*new_node).spans[level_cursor] = span_after;
                    }
                    if slow.is_null() {
                        self.level_spans[level_cursor] = span_before;
                    } else {
                        unsafe {
                            (*slow).spans[level_cursor] = span_before;
                        }
                    }
                    continue 'out2;
                } else {
                    slow = next;
                    slow_span = unsafe {
                        (*slow).spans[level_cursor]
                    };
                    next = unsafe {
                        (*next).levels[level_cursor]
                    };
                }
            }
        }
        // for the upper levels, the inserted item will only influence the span of ranges
        'out3: for level_cursor in level..self.level {
            let mut slow: *mut Node<Member> = std::ptr::null_mut();
            let mut next = if slow.is_null() {
                self.level_links[level_cursor]
            } else {
                unsafe {
                    (*slow).levels[level_cursor]
                }
            };
            while !next.is_null() {
                if unsafe {*new_node < *next} {
                    if slow.is_null() {
                        self.level_spans[level_cursor] += 1;
                    } else {
                        unsafe {
                            (*slow).spans[level_cursor] += 1;
                        }
                    }
                    continue 'out3;
                } else {
                    slow = next;
                    next = unsafe {
                        (*next).levels[level_cursor]
                    };
                }
            }
            if slow.is_null() {
                self.level_spans[level_cursor] += 1;
            } else {
                unsafe {
                    (*slow).spans[level_cursor] += 1;
                }
            } 
        }
        self.length += 1;
        if level > self.level {
            self.level = level;
        }
        Some(new_node)
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

    pub fn clear(&mut self) -> usize {
        if self.length == 0 {
            return 0
        }
        let count = self.length;
        self.length = 0;
        self.level = 0;
        while !self.level_links[0].is_null() {
            let node = unsafe {
                Box::from_raw(self.level_links[0])
            };
            self.level_links[0] = node.levels[0];
        }
        self.level_links.clear();
        self.level_spans.clear();
        count
    }

    pub fn remove(&mut self, score: f64, data: &Member) -> bool {
        if self.length == 0 {
            return false;
        }
        let mut to_remove: *mut Node<Member> = std::ptr::null_mut();
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
                                if !(unsafe {(*next).levels[0]}.is_null()) {
                                    unsafe {
                                        (*(*next).levels[0]).backward = slow;
                                    }
                                }
                            }
                            self.length -= 1;
                            // found it
                            to_remove = next;
                            break 'out;
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
        // amend span now
        if !to_remove.is_null() {
            // found it, remove now
            let item_level = unsafe {
                (*to_remove).levels.len()
            };
            for level in 1..item_level {
                // null for the start list
                let span_after = unsafe {
                    (*to_remove).spans[level]
                };
                let mut slow: *mut Node<Member> = std::ptr::null_mut(); 
                let mut next = self.level_links[level];
                loop {
                    if next.is_null() || unsafe{*next > *to_remove} {
                        // the item to remove is the tail of this level, just update the span;
                        // or it is in current range (slow, next)
                        if slow.is_null() {
                            self.level_spans[level] += span_after;
                        } else {
                            unsafe {
                                (*slow).spans[level] += span_after;
                            }
                        };
                        break;
                    } else {
                        slow = next;
                        next = unsafe {
                            (*slow).levels[level]
                        };
                    }
                }
            }
            for level in item_level..self.level {
                let mut slow: *mut Node<Member> = std::ptr::null_mut();
                let mut next = self.level_links[level];
                loop {
                    if next.is_null() || unsafe{*next > *to_remove} {
                        // the item to remove is the tail of this level, just update the span;
                        // or it is in current range (slow, next)
                        if slow.is_null() {
                            self.level_spans[level] -= 1;
                        } else {
                            unsafe {
                                (*slow).spans[level] -= 1;
                            }
                        };
                        break;
                    } else {
                        slow = next;
                        next = unsafe {
                            (*slow).levels[level]
                        };
                    }
                }
            }
            let _ = unsafe{Box::from_raw(to_remove)};
            return true
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

    fn do_range_tuple(&self, min: Option<Bound>, max: Option<Bound>, offset: usize, limit: usize) -> Vec<(f64, &Member, usize)> {
        self.do_range(min, max, offset, limit)
            .into_iter()
            .map(|i| (i.score, i.data, i.skiplevel))
            .collect()
    }

    fn count_element_upto(&self, up: &Bound) -> usize {
        let mut count = 0;
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
                let next_score = unsafe {
                    (*next).score
                };
                let span = if slow.is_null() {
                    self.level_spans[level]
                } else {
                    unsafe {
                        (*slow).spans[level]
                    }
                };
                if next_score > up.bound || (up.bound == next_score && up.exclusive) {
                    // 当前区间内，查找下一层
                    continue 'out;
                } else {
                    count += span + 1;
                    slow = next;
                    next = unsafe {
                        (*slow).levels[level]
                    };
                }
            }
        }
        count
    }

    /// 获取指定范围内的数据量，支持 `zcount (start end` 操作
    pub fn range_count(&self, min: Option<Bound>, max: Option<Bound>) -> usize {
        match (min, max) {
            (None, None) => self.length,
            (None, Some(max)) => self.count_element_upto(&max),
            (Some(min), None) => self.length - self.count_element_upto(&min.toggle()),
            (Some(min), Some(max)) => self.count_element_upto(&max) - self.count_element_upto(&min.toggle()),
        }
    }

    fn do_range(&self, min: Option<Bound>, max: Option<Bound>, mut offset: usize, mut limit: usize) -> Vec<RangeItem<&Member>> {
        if limit == 0 {
            limit = usize::MAX;
        }
        let mut result = vec![];
        if self.length == 0 {
            return result
        }
        let mut first = self.level_links[0];
        if let Some(min) = min {
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
                    if (next_score < min.bound) || (next_score == min.bound && min.exclusive) {
                        // 起始点在下一个区间
                        slow = next;
                        next = unsafe {
                            (*slow).levels[level]
                        };
                        continue
                    } else {
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
                            if pre_score > min.bound || (pre_score == min.bound && !min.exclusive) {
                                first = pre;
                                pre = unsafe{ (*pre).backward };
                                continue;
                            } else {
                                break;
                            }
                        }
                        break 'out;
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
            if let Some(ref m) = max {
                let cur_score = unsafe {(*cursor).score};
                if (cur_score > m.bound) || (m.exclusive && cur_score == m.bound) {
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
            spans: vec![0; level],
        }
    }
}

#[cfg(test)]
mod test {
    use crate::ds::skiplist::skiplist::Bound;

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

    #[test]
    fn check_span() {
        let mut list = Skiplist::new();
        let inserted_22 = list.do_insert(22, 22f64, 1).unwrap();
        for level in 0..list.level {
            assert_eq!(list.level_spans[level], 0);
            assert_eq!(unsafe{(*inserted_22).spans[level]}, 0);
        }
        let inserted_19 = list.do_insert(19, 19f64, 2).unwrap();
        assert_eq!(unsafe {
            (*inserted_19).spans[0]
        }, 0);
        assert_eq!(unsafe{(*inserted_19).spans[1]}, 1);
        let inserted_7 = list.do_insert(7, 7f64, 4).unwrap();
        assert_eq!(unsafe{(*inserted_7).spans[0]}, 0);
        assert_eq!(unsafe{(*inserted_7).spans[1]}, 0);
        assert_eq!(unsafe{(*inserted_7).spans[2]}, 2);
        assert_eq!(unsafe{(*inserted_7).spans[3]}, 2);
        let inserted_3 = list.do_insert(3, 3f64, 1);
        assert_eq!(list.level_spans[0], 0);
        assert_eq!(list.level_spans[1], 1);
        assert_eq!(list.level_spans[2], 1);
        assert_eq!(list.level_spans[3], 1);
        let inserted_37 = list.do_insert(37, 37f64, 3).unwrap();
        for l in 0..3 {
            assert_eq!(unsafe{(*inserted_37).spans[l]}, 0);
        }
        assert_eq!(unsafe{(*inserted_19).spans[1]}, 1);
        assert_eq!(unsafe{(*inserted_7).spans[2]}, 2);
        assert_eq!(unsafe{(*inserted_7).spans[3]}, 3);

        let inserted_11 = list.do_insert(11, 11f64, 1).unwrap();
        assert_eq!(unsafe{(*inserted_7).spans[1]}, 1);
        assert_eq!(unsafe{(*inserted_7).spans[2]}, 3);
        assert_eq!(unsafe{(*inserted_7).spans[3]}, 4);

        list.do_insert(26, 26f64, 1);
        assert_eq!(unsafe{(*inserted_19).spans[1]}, 2);
        assert_eq!(unsafe{(*inserted_7).spans[2]}, 4);
        assert_eq!(unsafe{(*inserted_7).spans[3]}, 5);

        // (-inf, 3]
        assert_eq!(list.count_element_upto(&Bound::new_inclusive(3f64)), 1);
        assert_eq!(list.count_element_upto(&Bound::new_exclusive(3f64)), 0);
        assert_eq!(list.count_element_upto(&Bound::new_inclusive(7f64)), 2);
        assert_eq!(list.count_element_upto(&Bound::new_exclusive(7f64)), 1);
        assert_eq!(list.count_element_upto(&Bound::new_inclusive(11f64)), 3);
        assert_eq!(list.count_element_upto(&Bound::new_exclusive(11f64)), 2);
        assert_eq!(list.count_element_upto(&Bound::new_inclusive(19f64)), 4);
        assert_eq!(list.count_element_upto(&Bound::new_exclusive(19f64)), 3);

        // [3, 19)]
        assert_eq!(
            list.range_count(
                Some(Bound::new_inclusive(3f64)), 
                Some(Bound::new_exclusive(19f64))
        ), 3);
        // (3, 22)
        assert_eq!(
            list.range_count(
                Some(Bound::new_exclusive(3f64)), 
                Some(Bound::new_exclusive(22f64))
        ), 3);
        // [4, +inf)
        assert_eq!(
            list.range_count(
                Some(Bound::new_inclusive(4f64)), 
                None
        ), 6);

        // (-inf, inf)
        assert_eq!(
            list.range_count(
                None,
                None
        ), list.length);
        // remove and check span again
        list.remove(22f64, &22);
        assert_eq!(unsafe{(*inserted_19).spans[1]}, 1);
        assert_eq!(unsafe{(*inserted_7).spans[2]}, 3);
        assert_eq!(unsafe{(*inserted_7).spans[3]}, 4);

        list.remove(7f64, &7);
        assert_eq!(list.level_spans[1], 2);
        assert_eq!(list.level_spans[2], 4);
        assert_eq!(list.level_spans[3], 5);

        list.remove(37f64, &37);
        assert_eq!(unsafe{(*inserted_19).spans[1]}, 1);
        assert_eq!(list.level_spans[2], 4);
        assert_eq!(list.level_spans[3], 4);

        // [4, +inf)
        assert_eq!(
            list.range_count(
                Some(Bound::new_inclusive(4f64)), 
                None
        ), 3); 
        
    }

    #[test]
    fn check_clear() {
        let mut list = Skiplist::new();
        list.do_insert(22, 22f64, 1);
        assert_eq!(list.level, 1);
        assert_eq!(list.length, 1);
        list.do_insert(19, 19f64, 2);
        assert_eq!(list.level, 2);
        assert_eq!(list.length, 2);
        list.do_insert(7, 7f64, 4);
        assert_eq!(list.level, 4);
        assert_eq!(list.length, 3);
        list.do_insert(3, 3f64, 1);
        assert_eq!(list.level, 4);
        assert_eq!(list.length, 4);
        list.do_insert(37, 37f64, 3);
        assert_eq!(list.level, 4);
        assert_eq!(list.length, 5);
        list.clear();
        assert_eq!(list.length, 0);
    }

    #[test]
    fn check_level() {
        let mut list = Skiplist::new();
        list.do_insert(22, 22f64, 1);
        assert_eq!(list.level, 1);
        assert_eq!(list.length, 1);
        list.do_insert(19, 19f64, 2);
        assert_eq!(list.level, 2);
        assert_eq!(list.length, 2);
        list.do_insert(7, 7f64, 4);
        assert_eq!(list.level, 4);
        assert_eq!(list.length, 3);
        list.do_insert(3, 3f64, 1);
        assert_eq!(list.level, 4);
        assert_eq!(list.length, 4);
        list.do_insert(37, 37f64, 3);
        assert_eq!(list.level, 4);
        assert_eq!(list.length, 5);
        list.do_insert(11, 11f64, 1);
        assert_eq!(list.level, 4);
        assert_eq!(list.length, 6);
        list.do_insert(26, 26f64, 1);
        assert_eq!(list.level, 4);
        assert_eq!(list.length, 7);
        let r: Vec<(f64, &i32, usize)> = list.do_range_tuple(None, None, 0, 0);
        assert_eq!(r, vec![(3f64, &3, 1), (7f64, &7, 4), (11f64, &11, 1), (19f64, &19, 2), (22f64, &22, 1), (26f64, &26, 1), (37f64, &37, 3)]);

        let r = list.do_range_tuple(Some(Bound::new(19f64, false)), None, 0, 3);
        assert_eq!(r, vec![(19f64, &19, 2), (22f64, &22, 1), (26f64, &26, 1)]); 

        let r = list.do_range_tuple(Some(Bound::new(19f64, false)), None, 1, 2);
        assert_eq!(r, vec![(22f64, &22, 1), (26f64, &26, 1)]); 

        let r = list.do_range_tuple(Some(Bound::new(19f64, false)), Some(Bound::new(22f64, false)), 0, 3);
        assert_eq!(r, vec![(19f64, &19, 2), (22f64, &22, 1)]); 

        let r = list.do_range_tuple(Some(Bound::new(19f64, false)), Some(Bound::new(22f64, true)), 0, 3);
        assert_eq!(r, vec![(19f64, &19, 2)]); 

        let hit = list.do_find(3f64, &3).unwrap();
        assert_eq!(hit.score, 3f64);
        assert_eq!(hit.data, 3);
        assert_eq!(hit.levels.len(), 1);
        assert!(list.do_find(22f64, &0).is_none());

        assert!(list.remove(3f64, &3));
        let r = list.do_range_tuple(None, None, 0, 0);
        assert_eq!(r, vec![(7f64, &7, 4), (11f64, &11, 1), (19f64, &19, 2), (22f64, &22, 1), (26f64, &26, 1), (37f64, &37, 3)]);

        assert!(!list.remove(3f64, &3));
        assert!(list.remove(11f64, &11));
        let r = list.do_range_tuple(None, None, 0, 0);
        assert_eq!(r, vec![(7f64, &7, 4), (19f64, &19, 2), (22f64, &22, 1), (26f64, &26, 1), (37f64, &37, 3)]);

        assert!(list.remove(37f64, &37));
        let r = list.do_range_tuple(None, None, 0, 0);
        assert_eq!(r, vec![(7f64, &7, 4), (19f64, &19, 2), (22f64, &22, 1), (26f64, &26, 1)]);

        assert!(!list.remove(37f64, &37));

        assert!(list.remove(19f64, &19));
        let r = list.do_range_tuple(None, None, 0, 0);
        assert_eq!(r, vec![(7f64, &7, 4), (22f64, &22, 1), (26f64, &26, 1)]);

        assert!(list.remove(26f64, &26));
        let r = list.do_range_tuple(None, None, 0, 0);
        assert_eq!(r, vec![(7f64, &7, 4), (22f64, &22, 1)]);

        assert!(list.remove(7f64, &7));
        let r = list.do_range_tuple(None, None, 0, 0);
        assert_eq!(r, vec![(22f64, &22, 1)]);

        assert!(list.remove(22f64, &22));
        let r = list.do_range_tuple(None, None, 0, 0);
        assert_eq!(r, vec![]);
    }
}