pub mod cmd;
pub mod connection;
pub mod frame;
pub mod ds;

// dyn trait 是 DST，使用时会导致不可编辑，所以用 Box 包裹
pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;