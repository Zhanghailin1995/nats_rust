use client::client::Client;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = "127.0.0.1:4222";
    let mut c = Client::connect(addr).await?;
    c.sub_message(
        "test".into(),
        None,
        Box::new(move |msg| {
            println!("recv:{}", unsafe { std::str::from_utf8_unchecked(msg) });
            Ok(())
        }),
    ).await?;
    tokio::time::delay_for(tokio::time::Duration::from_secs(50)).await;
    println!("close connection");
    c.close();
    Ok(())
}
