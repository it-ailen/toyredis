
#[derive(thiserror::Error, Debug)]
pub enum ZLError {
    #[error("Invalid ziplist given, error is `{0}`")]
    Invalid(String),
    #[error("Invalid zipentry, {0}")]
    InvalidEntry(String),
    #[error("invalid entry encoding")]
    InvalidEntryEncoding,
    #[error("Invalid offset({0}) is given")]
    OutOfRange(usize),
    #[error("zlend given")]
    Zlend,
    #[error("Unknown error, {0}")]
    Unknown(String),
}

pub type ZLResult<T> = Result<T, ZLError>;