#[macro_use]
extern crate log;
extern crate pretty_env_logger;

use std::fmt::{Debug, format};
use std::sync::Arc;
use std::time::Duration;
use rand::prelude::*;
use tokio::sync;

use odoo_client::client::OdooClient;

// Example app of using the client to connect to the odoo server
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("RUST_LOG", "TRACE");
    pretty_env_logger::init();

    let client = Arc::new(OdooClient::new("127.0.0.1:9058".to_string()));

    let (tx, rx) = sync::mpsc::unbounded_channel();
    let client_async = client.clone();
    tokio::spawn(async move {
        info!("initializing  stream");
        client_async.append_to_stream("STREAM_KEY_1", rx, None).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut i = 0;
    loop {
      // let client_async = client.clone();
        // tokio::spawn(async move {
        //     info!("sending SET query");
        //     let sleep_time: u8 = random();
        //     let val = format!("Hello {}", sleep_time);
        //     client_async.set_key("KEY_1", val.as_bytes(), None).await.unwrap();
        // });

         let client_async = client.clone();
         let x: i64 = random();
        let tx2 = tx.clone();
        tokio::spawn(async move {
            let mgs = format!("message-ID: {}", x);
            info!("appending to stream: {}", mgs.as_str());
            tx2.send(mgs.as_bytes().to_vec()).expect("errr")
        });


        let sleep_time: u8 = random();
        info!("thread_sleeping for {}", sleep_time);
        tokio::time::sleep(Duration::from_millis(sleep_time as u64)).await;
        if i ==  500 {
            // tokio::time::sleep(Duration::from_secs(60u64)).await;
            break;
        }
        i = i + 1;

    }

    Ok(())
}

