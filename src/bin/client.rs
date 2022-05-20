use mini_redis::client;
use tokio::sync::{mpsc, oneshot};
use toyredis::cmd::Command::{Get, Set};


#[tokio::main]
async fn main() {
    // 设置 32 长度的缓冲队列
    let (tx, mut rx) = mpsc::channel(32);
    let manager = tokio::spawn(async move {
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

    // tx(发送者) 都被回收(drop)时，rx 会收到一个 None，这里 while 就会退出
        while let Some(c) = rx.recv().await {

            match c {
                Get { key, resp } => {
                    let res = client.get(&key).await;
                    let _ = resp.send(res);
                },
                Set { key, value, resp } => {
                    let res = client.set(&key, value).await;
                    let _ = resp.send(res);
                },
            }

        }
    });

    let tx2 = tx.clone();

    let t1 = tokio::spawn(async move {
        let (resp_send, resp_recv) = oneshot::channel();
        tx.send(Get { key: "hello".into(), resp: resp_send }).await.unwrap();
        let resp = resp_recv.await;
        println!("Get {:?}", resp);
    });
    let t2 = tokio::spawn(async move {
        let (resp_send, resp_recv) = oneshot::channel();
        tx2.send(Set { key: "hello".into(), value: "world".into(), resp: resp_send }).await.unwrap();
        let resp = resp_recv.await;
        println!("Set {:?}", resp);
    });

    t2.await.unwrap();
    t1.await.unwrap();
    manager.await.unwrap();
}