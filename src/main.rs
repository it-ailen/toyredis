use mini_redis::{Connection, Frame};
use tokio::net::{TcpListener, TcpStream};


#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        // 在主线程中处理，并使用 await 进行了阻塞，使得命令只能被串行处理。
        let (socket , _) = listener.accept().await.unwrap();
        process(socket).await;
    }
}

/// 第一版本。仅打印请求，并返回一个错误。
async fn process(socket: TcpStream) {
    let mut connection = Connection::new(socket);
    if let Some(frame) = connection.read_frame().await.unwrap() {
        println!("GOT: {:?}", frame);
        let response = Frame::Error("unimplemented".into());
        connection.write_frame(&response).await.unwrap();
    }
}
