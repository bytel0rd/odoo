#[macro_use]
extern crate log;
extern crate pretty_env_logger;

use std::fmt::{Debug, format};
use std::sync::Arc;
use std::time::Duration;
use rand::prelude::*;

use odoo_client::client::OdooClient;

// Example app of using the client to connect to the odoo server
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("RUST_LOG", "TRACE");
    pretty_env_logger::init();

    let client = Arc::new(OdooClient::new("127.0.0.1:9058".to_string()));


    let client_async = client.clone();
    tokio::spawn(async move {
        info!("started reading from stream");
        client_async.listen_to_stream("HNY-W1PJ2DP2-202312-251407-27|64047", None, None, |stream_item| {
            info!("received from stream response: {:?}", stream_item);

        }).await.unwrap();
    });

    // loop {
        //
        // let client_async = client.clone();
        // tokio::spawn(async move {
        //     info!("sending GET query....");
        //     while let Some(value) = client_async.get_key("KEY_1").await.unwrap() {
        //         info!("got response: {}", String::from_utf8(value).unwrap());
        //     };
        // });
    //
    // let client_async = client.clone();
    // tokio::spawn(async move {
    //     info!("sending GET query....");
    //     while let Some(value) = client_async.get_key("KEY_1").await.unwrap() {
    //         info!("got response: {}", String::from_utf8(value).unwrap());
    //     };
    // });

        //
        let sleep_time: u8 = random();
        info!("polling GET in for {}", sleep_time);
        tokio::time::sleep(Duration::from_secs(60u64)).await;
    // }

    Ok(())
}

