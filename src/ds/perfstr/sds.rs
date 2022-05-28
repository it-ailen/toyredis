//! SDS(Simple Dynamic String，简单动态字符串），redis 官方提供的一种字符串实现。
//! 由于 redis 本身是用 C 实现的，C原始的 `char*` 是以 '\0' 结尾的简单字符数组，无法方便地实现 O(1) 获取长度、方便地 append 等功能，所以提供了这一版本。
//! 在本库中，我也将用 rust 实现这一版本。至于不用 rust 内置 string 的原因，在前面已说清楚

use super::SmartString;


/// 最大预分配空间，高于该值就不再二倍方式增长。
const MAX_PREALLOC: usize = 1024*1024;

/// SDS(Simple Dynamic String)
/// 
/// # Hash
/// 由于 SipHash 在 rust 中已标记为 deprecated，故暂时使用 default hash 替代(todo check why SipHash is deprecated?)
/// 
#[derive(Clone, Eq)]
pub struct SDS {
    /// 当前字符串大小
    cur_len: usize,
    /// 已分配的的空间中，空闲的空间字节数
    free: usize,
    /// 真正的字符串数据，没有 '\0' 结尾
    data: Vec<u8>, 
}

impl SDS {
    /// 对应sdsempty。
    /// #Return
    ///     返回一个空的字符串
    pub fn empty() -> Self {
        Self { cur_len: 0, free: 0, data: vec![], }
    }

    /// 初始化一个 SDS
    pub fn new(init: &[u8]) -> Self {
        let mut inst = Self::empty();
        inst.append(init);
        inst
    }

    /// 清除所有内容。
    pub fn clear(&mut self) {
        *self = Self::empty();
    }

    fn expand(&mut self, required_len: usize) {
        if required_len <= self.free {
            // 已经够了
            return;
        }
        let mut new_size = (required_len + self.cur_len);
        if 2*new_size <= MAX_PREALLOC {
            new_size *= 2;
        } else {
            new_size += MAX_PREALLOC;
        }
        // let mut new_data = Vec::with_capacity(new_size);
        let mut new_data = vec![0u8; new_size];
        new_data[..self.cur_len].clone_from_slice(&self.data[..self.cur_len]);
        self.free = new_size - self.cur_len;
        self.data = new_data;
    }
}

impl SmartString for SDS {
    fn len(&self) -> usize {
        self.cur_len
    }

    fn append(&mut self, data: &[u8]) {
        self.expand(data.len());
        self.data[self.cur_len..self.cur_len+data.len()].copy_from_slice(data);
        self.cur_len += data.len();
        self.free -= data.len();
    }

    fn val(&self) -> &[u8] {
        &self.data[..self.cur_len]
    }
}

impl PartialEq for SDS {
    fn eq(&self, other: &Self) -> bool {
        self.val() == other.val()
    }
}

impl std::hash::Hash for SDS {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let cur_data = &self.data[..self.cur_len];
        cur_data.hash(state);
    }
}


#[cfg(test)]
pub mod test {
    use crate::ds::perfstr::SmartString;

    use super::SDS;
    use super::MAX_PREALLOC;

    #[test]
    fn basis() {
        let mut sds = SDS::empty();
        assert_eq!(sds.len(), 0);
        assert_eq!(sds.free, 0);
        assert_eq!(sds.data.len(), 0);

        let piece = "little string".as_bytes();
        let mut last_len = 0;
        let mut last_cap = 0;
        sds.append(piece);
        assert_eq!(sds.len(), piece.len());
        assert_eq!(sds.data.len(), 2*piece.len());
        assert_eq!(sds.free, sds.data.len() - sds.len());

        assert_eq!(sds.val(), piece);

        last_len = sds.len();
        last_cap = sds.data.len();

        let append = " again".as_bytes();
        sds.append(append);
        assert_eq!(sds.len(), last_len+append.len());
        assert_eq!(sds.val(), [piece, append].concat());
        assert_eq!(sds.data.len(), last_cap);
        assert_eq!(sds.free, sds.data.len() - sds.len());

        last_len = sds.len();
        last_cap = sds.data.len();

        sds.append("1234567890".as_bytes());
        assert_eq!(sds.len(), last_len+10);
        assert_eq!(sds.data.len(), 2*(last_len+10));
        assert_eq!(sds.free, sds.data.len() - sds.len());

        last_len = sds.len();
        last_cap = sds.data.len();

        sds.append(&vec![1u8; MAX_PREALLOC]);
        assert_eq!(sds.len(), last_len+MAX_PREALLOC);
        assert_eq!(sds.data.len(), sds.len() + MAX_PREALLOC);
        assert_eq!(sds.free, sds.data.len() - sds.len());
        
        last_len = sds.len();
        last_cap = sds.data.len();
        sds.append(&vec![2u8; MAX_PREALLOC]);
        assert_eq!(sds.len(), last_len+MAX_PREALLOC);
        assert_eq!(sds.data.len(), sds.len());
        assert_eq!(sds.free, sds.data.len() - sds.len());

        last_len = sds.len();
        last_cap = sds.data.len();
        println!("last len: {}, last_cap: {}", last_len, last_cap);
        sds.append(&vec![1]);
        assert_eq!(sds.len(), last_len + 1);
        assert_eq!(sds.data.len(), last_cap+1+MAX_PREALLOC);

        sds.clear();
        assert_eq!(sds.len(), 0);
        assert_eq!(sds.free, 0);
        assert_eq!(sds.data.len(), 0); 

    }
}