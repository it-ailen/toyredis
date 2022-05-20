use std::collections::HashMap;

use mini_redis::{Connection, Frame, Command::{Set, Get, self}};
use tokio::net::{TcpListener, TcpStream};


#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        // 在主线程中处理，并使用 await 进行了阻塞，使得命令只能被串行处理。
        let (socket , _) = listener.accept().await.unwrap();
        // 将 process 放到任务中支持
        // 一个 tokio 任务是一个异步绿色线程，通过 tokio::spawn 创建，返回 JoinHandle 句柄
        // 创建的任务被调度到执行器中。
        //  Tokio 创建一个任务时，该任务类型的生命周期必须是 'static。所以这里用 move 转移所有权
        // 使用 move 后，数据只能被 一个任务使用
        tokio::spawn(async move {
            process(socket).await;
        });
    }
}

/// 利用 HashMap 实现简单的 Set/Get
async fn process(socket: TcpStream) {
    let mut db = HashMap::new();
    let mut connection = Connection::new(socket);
    // 使用 `read_frame` 方法从连接获取一个数据帧：一条redis命令 + 相应的数据
    // 通过 while 连续处理一个 tcp 内的请求
    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                db.insert(cmd.key().to_string(), cmd.value().to_vec());
                Frame::Simple("OK".into())
            },
            Get(cmd) => {
                if let Some(value) = db.get(cmd.key()) {
                    Frame::Bulk(value.clone().into())
                } else {
                    Frame::Null
                }
            },
            _ => {
                Frame::Error("unimplemented".into())
            }
        };
        connection.write_frame(&response).await.unwrap();
    }
}
