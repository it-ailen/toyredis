use std::{collections::HashMap, sync::{Arc, Mutex}};

use bytes::Bytes;
use mini_redis::{Connection, Frame, Command::{Set, Get, self}};
use tokio::net::{TcpListener, TcpStream};


#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("start server...");
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    loop {
        // 在主线程中处理，并使用 await 进行了阻塞，使得命令只能被串行处理。
        let (socket , _) = listener.accept().await.unwrap();

        // 增加一次引用计数
        let db = db.clone(); 
        // 将 process 放到任务中支持
        // 一个 tokio 任务是一个异步绿色线程，通过 tokio::spawn 创建，返回 JoinHandle 句柄
        // 创建的任务被调度到执行器中。
        //  Tokio 创建一个任务时，该任务类型的生命周期必须是 'static。所以这里用 move 转移所有权
        // 使用 move 后，数据只能被 一个任务使用
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

/// 数据库类型，使用别名方式构造
/// 在使用 Tokio 编写异步代码时，一个常见的错误无条件地使用 tokio::sync::Mutex ，而真相是：Tokio 提供的异步锁只应该在跨多个 .await调用时使用，而且 Tokio 的 Mutex 实际上内部使用的也是 std::sync::Mutex。
///多补充几句，在异步代码中，关于锁的使用有以下经验之谈：
///锁如果在多个 .await 过程中持有，应该使用 Tokio 提供的锁，原因是 .await的过程中锁可能在线程间转移，若使用标准库的同步锁存在死锁的可能性，例如某个任务刚获取完锁，还没使用完就因为 .await 让出了当前线程的所有权，结果下个任务又去获取了锁，造成死锁
///锁竞争不多的情况下，使用 std::sync::Mutex
///锁竞争多，可以考虑使用三方库提供的性能更高的锁，例如 parking_lot::Mutex
type Db = Arc<Mutex<HashMap<String, Bytes>>>;

/// 利用 HashMap 实现简单的 Set/Get
// Vec<u8> 在 copy 时，底层数据（堆）也会被复制一次，所以采用 bytes::Bytes 类型来替换，它内部使用类似 Arc 的机制实现，可以避免没必要的数据拷贝。
async fn process(socket: TcpStream, db: Db) {
    let mut connection = Connection::new(socket);
    // 使用 `read_frame` 方法从连接获取一个数据帧：一条redis命令 + 相应的数据
    // 通过 while 连续处理一个 tcp 内的请求
    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let mut db = db.lock().unwrap();
                // Bytes.clone() 不会复制堆上数据
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".into())
            },
            Get(cmd) => {
                let db = db.lock().unwrap();
                if let Some(value) = db.get(cmd.key()) {
                    Frame::Bulk(value.clone())
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
