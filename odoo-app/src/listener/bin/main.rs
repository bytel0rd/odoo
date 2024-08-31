#[macro_use]
extern crate log;
extern crate pretty_env_logger;

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync;

use odoo_client::client::OdooClient;

// Example app of using the client to connect to the odoo server
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("RUST_LOG", "TRACE");
    pretty_env_logger::init();

    let client = Arc::new(OdooClient::new("127.0.0.1:9058".to_string()));
    let (tx, mut rx) = sync::mpsc::channel(100);


    let client_async = client.clone();
    let tx_1 = tx.clone();
    tokio::spawn(async move {
        info!("started reading from stream");
        client_async.listen_to_stream("STREAM_KEY_1", None, None, tx_1).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(30)).await;

    while let Some(result) = rx.recv().await {
        match result {
            Ok(data) => {
                let stream_item = String::from_utf8(data).unwrap();
                info!("received from stream response: {}", stream_item);
            }
            Err(err) => {
                error!("received error response: {}", err);
                break;
            }
        }
    }

    // loop {
    //
    //
    //     let client_async = client.clone();
    //     tokio::spawn(async move {
    //         info!("sending GET query....");
    //         match client_async.get_key("KEY_1").await {
    //             Ok(Some(value)) => {
    //                 info!("got response: {}", String::from_utf8(value).unwrap());
    //             }
    //             Err(err) => {
    //                 error!("Got error: {:?}", err);
    //             }
    //             _ => {}
    //         }

    // while let Ok(Some(value)) = client_async.get_key("KEY_1").await {
    //     info!("loop response: {}", String::from_utf8(value).unwrap());
    //
    // }
    //     });
    //
    //     //
    //     let sleep_time: u8 = random();
    //     info!("polling GET in for {}", sleep_time);
        tokio::time::sleep(Duration::from_secs(30)).await;
    //     // }
    //
    // }

    Ok(())
}