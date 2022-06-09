//! The ziplist is a specially encoded *dually linked list* that is designed to be very memory efficient. It *stores both strings and integer values*, where integers are encoded as actual integers instead of a series of characters. It allows push and pop operations on either side of the list in O(1) time.
//! 
//! # Why use ziplist? 为什么使用 ziplist？
//! 一个普通的双向链表，链表中每一项都占用独立的一块内存，各项之间用地址指针（或引用）连接起来。这种方式会带来大量的内存碎片，而且地址指针也会占用额外的内存。
//! 而ziplist却是将表中每一项存放在前后连续的地址空间内，一个ziplist整体占用一大块内存。它是一个表（list），但其实不是一个链表（linked list），只是
//! 一片连续的内存区域。
//! 
//! 

use std::{mem, vec};

use byteorder::{BigEndian, ByteOrder};

use super::error::{ZLResult, ZLError};

const ZIPLIST_BYTES_OFF: usize = 0;
const ZIPLIST_BYTES_SIZE: usize = 4;
const ZIPLIST_TAILOFF_OFF: usize = ZIPLIST_BYTES_OFF + ZIPLIST_BYTES_SIZE;
const ZIPLIST_TAILOFF_SIZE: usize = 4;
const ZIPLIST_LEN_OFF: usize = ZIPLIST_TAILOFF_OFF + ZIPLIST_TAILOFF_SIZE;
const ZIPLIST_LEN_SIZE: usize = 2;
const ZIPLIST_HEADER_SIZE: usize = ZIPLIST_LEN_OFF + ZIPLIST_LEN_SIZE;
const ZIPLIST_CONTENT_OFF: usize = ZIPLIST_HEADER_SIZE;


const ZIPLIST_I16_ENC: u8 = 0b1100_0000;
const ZIPLIST_I32_ENC: u8 = 0b1101_0000;
const ZIPLIST_I64_ENC: u8 = 0b1110_0000;
const ZIPLIST_I24_ENC: u8 = 0b1111_0000;
const ZIPLIST_I8_ENC: u8 = 0b1111_1110;

#[derive(Clone, Copy)]
enum Encoding {
    // 字符串类型, usize 为字符串长度
    String(usize),
    Integer(i64),
}

impl Encoding {
    fn is_str(&self) -> bool {
        match self {
            Encoding::String(_) => true,
            _ => false,
        }
    }
    /// 获取编码本身所占的字节数。
    fn encoding_len(&self) -> usize {
        match self {
            Encoding::String(sz) => {
                if *sz < 1<<6 {
                    1
                } else if *sz < 1<<14 {
                    2
                } else {
                    assert!(*sz < 1 << 32);
                    5
                }
            },
            Encoding::Integer(i) => {
                if *i >=0 && *i <= 12 {
                    1
                } else if *i >= i8::MIN as i64 && *i <= i8::MAX as i64 {
                    1 + mem::size_of::<i8>()
                } else if *i >= i16::MIN as i64 && *i <= i16::MAX as i64 {
                    1 + mem::size_of::<i16>()
                } else if *i >= -(1<<23) && *i <= (1<<23) - 1 {
                    1 + 3
                } else if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                    1 + mem::size_of::<i32>()
                } else {
                    1 + mem::size_of::<i64>()
                }
            },
        }
    }

    /// 编码字节数 + 内容长度（针对 string）
    fn encoding_len_with_content(&self) -> usize {
        match self {
            Encoding::String(sz) => {
                self.encoding_len() + *sz
            },
            Encoding::Integer(_) => {
                self.encoding_len()
            },
        }
    }

    /// 当前 encoding 的字节表示，按 index 返回对应位置的字节。
    pub fn encoding_bytes_by_index(&self, idx: usize) -> Option<u8> {
        match *self {
            Encoding::String(_) => self.encoding_index_str(idx),
            Encoding::Integer(_) => self.encoding_index_int(idx),
        }
    }

    fn unwrap_str(&self) -> usize {
        match self {
            Encoding::String(sz) => *sz,
            Encoding::Integer(_) => unreachable!(),
        }
    }

    fn unwrap_int(&self) -> i64 {
        match self {
            Encoding::String(_) => unreachable!(),
            Encoding::Integer(i) => *i,
        }
    }

    /// 根据 idx 索引 encode 编码后的 vec<u8>
    fn encoding_index_str(&self, idx: usize) -> Option<u8> {
        let len = self.encoding_len();
        let mut v = 0;
        if idx == 0 {
            match len {
                2 => v |= 0b0100_0000,
                5 => { 
                    return Some(0b1000_0000); 
                } ,
                _ => {},
            }
        }
        if idx >= len {
            return None
        }
        let sz = self.unwrap_str();
        v |= (sz >> ((len - idx - 1) * 8)) & 0xff;
        Some(v as u8)
    }

    fn encoding_index_int(&self, idx: usize) -> Option<u8> {
        let len = self.encoding_len();
        if idx >= len {
            return None
        }
        let i = self.unwrap_int();
        if idx == 0 {
            match len {
                1 => {return Some(i as u8 | 0b1111_0000)},
                2 => {return Some(ZIPLIST_I8_ENC) },
                3 => {return Some(ZIPLIST_I16_ENC)},
                4 => {return Some(ZIPLIST_I24_ENC)},
                5 => {return Some(ZIPLIST_I32_ENC)},
                9 => {return Some(ZIPLIST_I64_ENC)}
                _ => unreachable!(),
            }
        }
        let v = (i >> ((len - idx - 1) * 8)) & 0xff;
        Some(v as u8)
    }

    fn parse(src: &[u8]) -> ZLResult<Self> {
        if src[0] & 0b1100_0000 == 0b1100_0000 {
            // int
            Self::parse_int_encoding(src)
        } else {
            // string
            Self::parse_str_encoding(src)
        }
    }

    fn parse_str_encoding(src: &[u8]) -> ZLResult<Self> {
        let sz = match src[0] & 0b1100_0000 {
            0b0000_0000 => 1usize,
            0b0100_0000 => 2usize,
            0b1000_0000 => 5usize,
            _ => panic!("not possible"),
        };
        let mut v = src[0] as usize & 0b0011_1111;
        for i in 1..sz {
            // 大端模式
            v <<= 8;
            v |= src[i] as usize;
        }
        Ok(Self::String(v))
    }
    
    fn parse_int_encoding(src: &[u8]) -> ZLResult<Self> {
        let sz = match src[0] {
            ZIPLIST_I8_ENC => mem::size_of::<u8>(),
            ZIPLIST_I16_ENC => mem::size_of::<u16>(),
            ZIPLIST_I24_ENC => 3 * mem::size_of::<u8>(),
            ZIPLIST_I32_ENC => mem::size_of::<u32>(),
            ZIPLIST_I64_ENC => mem::size_of::<u64>(),
            _ => {
                if src[0] >> 4 != 0xf {
                    return Err(ZLError::InvalidEntryEncoding);
                }
                let k = src[0] & 0xf;
                if !(k > 0 && k < 12) {
                    return Err(ZLError::InvalidEntryEncoding);
                }
                return Ok(Self::Integer(k as i64))
            },
        };
        let mut v = if src[1] >> 7 == 1 {
            -1i64
        } else {
            0
        };
        for idx in 0..sz {
            v <<= 8;
            v |= src[idx + 1] as i64;
        }
        Ok(Self::Integer(v))
    }
}

struct EncodingIter {
    enc: Encoding,
    offset: usize,
}

impl Iterator for EncodingIter {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        let v = self.enc.encoding_bytes_by_index(self.offset);
        if v.is_some() {
            self.offset += 1;
        }
        v
    }
}

impl IntoIterator for Encoding {
    type Item = u8;
    type IntoIter = EncodingIter;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            enc: self.clone(),
            offset: 0,
        }
    }
}

pub enum ZipEntryValue {
    Bytes(Vec<u8>),
    Int(i64),
}

impl ZipEntryValue {
    fn unwrap_bytes(&self) -> &[u8] {
        match self {
            Self::Bytes(s) => s,
            _ => panic!("fail unwrapping to bytes"),
        }
    }

    fn unwrap_int(&self) -> i64 {
        match self {
            Self::Int(k) => *k,
            _ => panic!("fail unwrapping to int"),
        }
    }
}

/// 只读的 zip entry，用于只读访问
pub struct ZipEntry{
    prevrawlen: usize,
    prevrawlen_size: usize,
    encoding: Encoding,
    // content: &'a [u8],
}

impl ZipEntry {
    fn parse(src: &[u8]) -> Self {
        let prevrawlen = Self::parse_prevrawlen(src);
        let prevrawlen_size = Self::prevrawlen_size(prevrawlen);
        let encoding = Encoding::parse(&src[prevrawlen_size..]).unwrap();
        Self{
            prevrawlen,
            prevrawlen_size,
            encoding,
            // content: src,
        }
    }

    #[inline]
    fn prevrawlen_size(len: usize) -> usize {
        if len < 0xfe {
            1
        } else {
            5
        }
    }

    fn parse_prevrawlen(src: &[u8]) -> usize {
        if src[0] < 0xfe {
            return src[0] as usize;
        }
        let mut v: usize = 0;
        for i in 1..=4 {
            v <<= 8;
            v |= src[i] as usize;
        }
        v
    }

    fn encode_prevrawlen(prevrawlen: usize) -> Vec<u8> {
        if prevrawlen < 0xfe {
            vec![prevrawlen as u8]
        } else {
            let mut v = vec![0u8; 5];
            v[0] = 0xfe;
            BigEndian::write_u32(&mut v, prevrawlen as u32);
            v
        }
    }

    fn check_len(src: &[u8]) -> usize {
        let prevrawlen = Self::parse_prevrawlen(src);
        let prevrawlen_size = Self::prevrawlen_size(prevrawlen);
        let encoding = Encoding::parse(&src[prevrawlen_size..]).unwrap();
        prevrawlen_size + encoding.encoding_len_with_content()
    }

    fn header_size(&self) -> usize {
        self.prevrawlen_size + self.encoding.encoding_len()
    }

    fn entry_size(&self) -> usize {
        self.prevrawlen_size + self.encoding.encoding_len_with_content()
    }

    fn value<'a>(&self, bytes: &[u8]) -> ZipEntryValue {
        let header_size = self.header_size();
        match self.encoding {
            Encoding::String(sz) => ZipEntryValue::Bytes(bytes[header_size..header_size+sz].to_vec()),
            Encoding::Integer(i) => ZipEntryValue::Int(i),
        }
    }


    fn iter<'a>(&self, bytes: &'a [u8]) -> std::iter::Chain<std::iter::Chain<vec::IntoIter<u8>, EncodingIter>, std::iter::Cloned<std::slice::Iter<'a, u8>>>   {
        let prevrawlen_bytes = if self.prevrawlen_size == 1 {
            vec![self.prevrawlen as u8]
        } else {
            let mut v = vec![0u8; self.prevrawlen_size];
            v[0] = 0xfe;
            BigEndian::write_u32(&mut v, self.prevrawlen as u32);
            v
        };
        let content_iter = if self.encoding.is_str() {
            bytes[self.header_size()..].iter().cloned::<'a, _>()
        } else {
            "".as_bytes().iter().cloned::<'a, _>()
        };
        prevrawlen_bytes
            .into_iter()
            .chain(self.encoding.into_iter())
            .chain(content_iter)
    }
}

/// mutable zip entry
struct ZipEntryMut<'a> {
    list: &'a mut ZipList,
    offset: usize,
}

pub struct ZipList(Vec<u8>);

impl ZipList {
    pub fn new() -> Self {
        let mut src = vec![0u8; ZIPLIST_HEADER_SIZE];
        BigEndian::write_u32(&mut src[ZIPLIST_BYTES_OFF..], ZIPLIST_HEADER_SIZE as u32);
        BigEndian::write_u32(&mut src[ZIPLIST_TAILOFF_OFF..], ZIPLIST_HEADER_SIZE as u32);
        Self(src)
    }

    fn set_tail_offset(&mut self, tail_offset: usize) {
        BigEndian::write_u32(&mut self.0[ZIPLIST_TAILOFF_OFF..], tail_offset as u32);
    }

    fn tail_offset(&self) -> usize {
        BigEndian::read_u32(&self.0[ZIPLIST_TAILOFF_OFF..]) as usize
    }

    fn read_entry_cnt(&self) -> usize {
        BigEndian::read_u16(&self.0[ZIPLIST_LEN_OFF..]) as usize
    }

    pub fn get_entry_cnt(&self) -> usize {
        let cnt = self.read_entry_cnt();
        if cnt < 0xffff {
            cnt
        } else {
            self.count_entry()
        }
    }

    fn set_entry_cnt(&mut self, len: usize) {
        let len = if len >= 0xffff {
            0xffff
        } else {
            len as u16
        };
        BigEndian::write_u16(&mut self.0[ZIPLIST_LEN_OFF..], len);
    }

    fn bytes_size(&self) -> usize {
        BigEndian::read_u32(&self.0[ZIPLIST_BYTES_OFF..]) as usize
    }

    fn set_bytes_size(&mut self, sz: usize) {
        println!("set_bytes_size: {}", sz);
        BigEndian::write_u32(&mut self.0[ZIPLIST_BYTES_OFF..], sz as u32);
    }

    fn push_tail(&mut self, encoding: Encoding, content: &[u8]) -> ZLResult<()> {
        let mut tail_offset = self.tail_offset();
        let cnt = self.read_entry_cnt();
        let prevrawlen = if cnt > 0 {
            ZipEntry::check_len(&self.0[tail_offset..])
        } else {
            0
        };
        tail_offset += prevrawlen;
        let prevrawlen_size = ZipEntry::prevrawlen_size(prevrawlen);
        let ze = ZipEntry{
            prevrawlen,
            prevrawlen_size,
            encoding,
        };
        let required_len = prevrawlen_size + encoding.encoding_len_with_content();
        self.0.splice(tail_offset..tail_offset, vec![0u8; required_len]);
        (&mut self.0[tail_offset..]).iter_mut().zip(ze.iter(content)).for_each(|(a, b)| *a = b);
        self.set_bytes_size(self.bytes_size() + required_len);
        self.set_tail_offset(tail_offset);
        self.set_entry_cnt(cnt + 1);
        Ok(())
    }

    pub fn push_tail_string(&mut self, content: &[u8]) -> ZLResult<()> {
        let encoding = Encoding::String(content.len());
        self.push_tail(encoding, content)
    }

    pub fn push_tail_int(&mut self, val: i64) -> ZLResult<()> {
        let encoding = Encoding::Integer(val);
        self.push_tail(encoding, &[])
    }

    fn count_entry(&self) -> usize {
        let mut cnt = 0;
        let mut offset = self.tail_offset();
        while offset >= ZIPLIST_CONTENT_OFF {
            cnt += 1;
            let skip = ZipEntry::parse_prevrawlen(&self.0[offset..]);
            if skip  == 0 {
                break;
            }
            offset -= skip;
        }
        cnt
    }

    pub fn iter(&self) -> ZipListIter {
        ZipListIter{
            ziplist: self,
            cur_offset: self.tail_offset(),
        }
    }

    pub fn pop_front(&mut self) -> Option<ZipEntryValue> {
        if self.read_entry_cnt() == 0 {
            return None
        }
        let first = ZipEntry::parse(&self.0[ZIPLIST_HEADER_SIZE..]);
        let val = first.value(&self.0[ZIPLIST_HEADER_SIZE..]);
        let mut cur_offset = ZIPLIST_HEADER_SIZE;
        // 指向原来的下一个 entry 开头
        let mut next_off = cur_offset + first.entry_size();
        let mut last_size = 0usize;
        let ori_bytes = self.bytes_size();
        // 从 first.entry_size 变成了 0
        let mut prevlen_changed = true;
        while next_off < ori_bytes {
            let entry = ZipEntry::parse(&self.0[next_off..]);
            let entry_size = entry.entry_size();
            if prevlen_changed  {
                if entry.prevrawlen_size == last_size {
                    // 这次没变化，后面就不再变化了
                    prevlen_changed = false;
                }
                let prevlen_bytes = ZipEntry::encode_prevrawlen(last_size);
                self.0[cur_offset..].copy_from_slice(&prevlen_bytes);
                cur_offset += prevlen_bytes.len();
                self.0.copy_within(next_off+entry.prevrawlen_size..next_off+entry_size, cur_offset);
                cur_offset += entry_size - entry.prevrawlen_size;
                last_size = prevlen_bytes.len() + entry_size - entry.prevrawlen_size;
            } else {
                last_size = entry_size;
                self.0.copy_within(next_off..next_off+entry_size, cur_offset);
                cur_offset += entry_size;
            }
            next_off += entry_size;
        }
        self.set_bytes_size(ori_bytes-first.entry_size());
        self.set_tail_offset(cur_offset);
        let ori_cnt = self.read_entry_cnt();
        if ori_cnt < 0xffff {
            self.set_entry_cnt(ori_cnt-1);
        } else {
            self.set_entry_cnt(self.count_entry());
        }
        Some(val)
    }

}

pub struct ZipListIter<'a> {
    ziplist: &'a ZipList,
    cur_offset: usize,
}

impl<'a> Iterator for ZipListIter<'a> {
    type Item = (usize, ZipEntry);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_offset >= self.ziplist.bytes_size() {
            return None;
        }
        let ori_offset = self.cur_offset;
        let entry = ZipEntry::parse(&self.ziplist.0[self.cur_offset..]);
        self.cur_offset += entry.entry_size();
        Some((ori_offset, entry))
    }
}

#[cfg(test)]
mod tests {
    use crate::ds::ziplist::{ZipEntry, Encoding};

    use super::{ZipList, ZIPLIST_HEADER_SIZE};

    #[test]
    fn push_and_pop() {
        let mut zl = ZipList::new();
        assert_eq!(zl.bytes_size(), ZIPLIST_HEADER_SIZE);
        assert_eq!(zl.get_entry_cnt(), 0);
        let mut last_bytes_size = zl.bytes_size();

        // 插入第一个元素：int 1
        zl.push_tail_int(1).unwrap();
        let mut enc = Encoding::Integer(1);
        assert_eq!(zl.bytes_size(), last_bytes_size + 1 + enc.encoding_len_with_content());
        assert_eq!(zl.get_entry_cnt(), 1);
        assert_eq!(zl.tail_offset(), ZIPLIST_HEADER_SIZE);
        last_bytes_size = zl.bytes_size();
        let mut last_tail_offset = zl.tail_offset();
        let mut prevrawlen = 1+enc.encoding_len_with_content();
        // 插入第2 个元素： string [1u8; 253]
        zl.push_tail_string(&vec![1u8; 253]).unwrap();
        enc = Encoding::String(253);
        assert_eq!(zl.bytes_size(), last_bytes_size 
        + 1 /* prevrawlen */
        + 2  /* encoding */
        + 253 /* content len */);
        assert_eq!(zl.get_entry_cnt(), 2);
        assert_eq!(zl.tail_offset(), last_tail_offset + prevrawlen);
        prevrawlen = zl.bytes_size() - last_bytes_size;
        last_bytes_size = zl.bytes_size();
        last_tail_offset = zl.tail_offset();

        // 插入第3 个元素：string[2u8; 0xffff]
        zl.push_tail_string(&vec![2u8; 0xffff]).unwrap();
        assert_eq!(zl.bytes_size(), last_bytes_size
        + 5 /* prevrawlen */
        + 5 /* encoding */
        + 0xffff /* content len */);
        assert_eq!(zl.get_entry_cnt(), 3);
        assert_eq!(zl.tail_offset(), last_tail_offset + prevrawlen);

        let mut iter = zl.iter();
        let (offset, entry) = iter.next().unwrap();
        
    }

    #[test]
    fn move_bytes() {
        let mut v = Vec::new();
        for i in 0..5 {
            v.push(i as u8);
        } 
        v.copy_within(3.., 1);
        assert_eq!(v, vec![0, 3, 4, 3, 4]);
    }
}