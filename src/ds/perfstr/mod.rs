
/// 系统内的 string 实现，key/value 等使用到的 string 都将用这个 trait 的实现来代替
/// 为什么不直接使用内置的 String 或者 &str 呢？
/// 原因是 String/str 都是严格的 utf8 编码字符串，redis 面向的字符串实际上只是字节数组，可能并非是 utf8 编码。
pub trait SmartString {
    /// 返回字符串长度
    fn len(&self) -> usize;
    /// 
    fn append(&mut self, data: &[u8]);

    fn val(&self) -> &[u8];
}

pub mod sds;