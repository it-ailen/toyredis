/// listpack -- suitable to store lists of string elements in a representation which is 
/// - space efficient
/// - can be efficiently accessed from left to right and from right to left.
/// 
/// refers to [here](https://github.com/antirez/listpack)
/// 

/// 压缩链表中的节点。
/// 
/// Nodes of the listpack.
/// 
/// refers to 
enum ListpackEntry {
    String(Vec<u8>),
    Integer(i64),
}