use std::io::Cursor;

use bytes::{BytesMut, Buf};
use tokio::io::{AsyncReadExt, self, AsyncWriteExt};
use tokio::net::TcpStream;
use crate::Result;

use crate::frame::Frame;


/// 对一个客户端连接的抽象，负责数据读写。redis协议可参见[这儿](https://redis.io/docs/reference/protocol-spec/)
struct Connection {
    stream: TcpStream,
    /// stream 本身是面向连接的，单次读取可能不是正好一个 frame，所以需要一个缓冲区将数据暂存
    buffer: BytesMut, 
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream, buffer: BytesMut::with_capacity(4096) }
    }

    pub async fn read_frame(&mut self) 
        -> Result<Option<Frame>> {
            loop {
                // 先尝试从 buffer 中读取一个 frame
                if let Some(frame) = self.parse_frame()? {
                    return Ok(Some(frame));
                }
                // 0 表示 EOF，即客户端关闭了连接
                if 0 == self.stream.read_buf(&mut self.buffer).await? {
                    if self.buffer.is_empty() {
                        return Ok(None)
                    } else {
                        return Err("connection reset by peer".into());
                    }
                }
            }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Array(val) => {
                self.stream.write_u8(b'*').await?;
                self.write_decimal(val.len() as u64).await?;
                for entry in val {
                    self.write_value(entry).await?;
                }
            }
            _ => self.write_value(frame).await?,
            
        }
        self.stream.flush().await
    }

    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(data) => {
                self.stream.write_u8(b'$').await?;
                self.write_decimal(data.len() as u64).await?;
                self.stream.write_all(data).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(_val) => todo!(),
        }
        Ok(())
    }

    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;
        // todo why not use u64.to_string() instead?
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(buf, "{}", val);

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;
        Ok(())
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        use crate::frame::Error::Incomplete;
        let mut buf = Cursor::new(&self.buffer[..]);
        match Frame::check(&mut buf) {
            Ok(_) => {
                let len = buf.position() as usize;
                // 回滚 cursor
                buf.set_position(0);
                let frame = Frame::parse(&mut buf)?;
                buf.advance(len);
                Ok(Some(frame))
            },
            // 数据不完整，需要从 socket 中重新读取到 buffer，再次尝试解析
            Err(Incomplete) => Ok(None),
            // 出错啦
            Err(e) => Err(e.into()),
        }
    }
}