use mini_redis::{client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = client::connect("127.0.0.1:6379").await?;
    client.set("hello", "world".into()).await?;
    let result = client.get("hello").await?;
    println!("got value of ({:?}) from server, {:?}", "hello", result);
    println!("got value of unknown from server, {:?}", client.get("unknown").await?);
    Ok(())
}