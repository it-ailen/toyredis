use std::{io::Cursor, num::TryFromIntError, string::FromUtf8Error, fmt};

use bytes::{Bytes, Buf};

pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

impl Frame {
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            // +xxx\r\n 或者 -xxx\r\n
            b'+' | b'-' => {
                get_line(src)?;
                Ok(())
            },
            // // -xxx\r\n
            // b'-' => {
            //     get_line(src)?;
            //     Ok(())
            // },
            // :123\r\n
            b':' => {
                let _ = get_decimal(src)?;
                Ok(())
            },
            // `$123\r\n` 或者 `$-1\r\n'
            b'$' => {
                if b'-' == peek_u8(src)? {
                    // Skip '-1\r\n'
                    skip(src, 4);
                } else {
                    let len: usize = get_decimal(src)?.try_into()?;
                    // skip that number of bytes + 2 (\r\n).
                    skip(src, len+2);
                }
                Ok(())
            },
            // `*12` 后端跟 12 个元素
            b'*' => {
                let len = get_decimal(src)?;
                for _ in 0..len {
                    Frame::check(src)?;
                }
                Ok(())
            }
            actual => Err(format!("protocol error; invalid frame type byte `{}`", actual).into()),
        }
    }

    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'+' => {
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::Simple(string))
            }
            // -xxxx 表示错误
            b'-' => {
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::Error(string))
            }
            b':' => {
                let n = get_decimal(src)?;
                Ok(Frame::Integer(n))
            }
            b'$' => {
                // $- 开头时，必须是 $-1\r\n，表示 Null
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;
                    if b"-1" != line {
                        return Err("protocol error; invalid frame format".into());
                    }
                    Ok(Frame::Null)
                } else {
                    // $lenxxxx\r\n，len 表示后续 xxx 的长度，为 bulk write 的数据
                    let len = get_decimal(src)?.try_into()?;
                    let n = len+2; // 跳过 \r\n
                    if src.remaining() < n {
                        return Err(Error::Incomplete)
                    }
                    let data = Bytes::copy_from_slice(&src.chunk()[..len]);
                    skip(src, n)?;
                    Ok(Frame::Bulk(data))
                }
            }
            b'*' => {
                let len = get_decimal(src)? as usize;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }
                Ok(Frame::Array(out))
            }
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug)]
pub enum Error {
    /// 数据帧不完整
    Incomplete,
    Other(crate::Error),
}

impl From<String> for Error {
    fn from(src: String) -> Self {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Self {
        src.to_string().into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_: FromUtf8Error) -> Self {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(src.get_u8())
}


fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(src.chunk()[0])
}

fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = src.position() as usize;
    let ori_data = src.get_ref();
    let end = ori_data.len() as usize;
    for _i in start..end {
        // if ori_data[i] == b'\r' && ori_data[i+1] == b'\n' {
        //     src.set_position((i+2) as u64); // 跳过\r\n
            // return Ok(&ori_data[start..i]);
        // }
    }
    Err(Error::Incomplete)
}

/// 解析出行首的数字
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    let line = get_line(src)?;
    use atoi::atoi;
    atoi::<u64>(line).ok_or_else(||  "protocol error; invalid frame format".into())
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }
    src.advance(n);
    Ok(())
}