use bytes::Bytes;
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum Command {
    Get {
        key: String,
        resp: Responder<Option<Bytes>>,
    },
    Set {
        key: String,
        value: Bytes,
        resp: Responder<()>,
    }
}

type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;