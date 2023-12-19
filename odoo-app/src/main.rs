#[macro_use]
extern crate log;
extern crate pretty_env_logger;

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use odoo_client::client::OdooClient;

// Example app of using the client to connect to the odoo server
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("RUST_LOG", "TRACE");
    pretty_env_logger::init();

    let client = Arc::new(OdooClient::new("127.0.0.1:8080".to_string()));

    loop {
        // let client_async = client.clone();
        // tokio::spawn(async move {
        //     info!("sending SET query");
        //     client_async.set_key("KEY_1", "Hello".as_bytes(), None).await.unwrap();
        // });
        //
        // let client_async = client.clone();
        // tokio::spawn(async move {
        //     info!("sending GET query");
        //     if let Some(value) = client_async.get_key("KEY_1").await.unwrap() {
        //         info!("got response: {}", String::from_utf8(value).unwrap());
        //     };
        // });

        let client_async = client.clone();
        tokio::spawn(async move {
            info!("appending to stream");
            client_async.append_to_stream("STREAM_KEY_1", "Hello".as_bytes(), None).await.unwrap();
        });

        let client_async = client.clone();
        tokio::spawn(async move {
            info!("reading  from stream");
            client_async.listen_to_stream("STREAM_KEY_1", None, None, |stream_item| {
                if let Some(value) = stream_item {
                    info!("received from stream response: {}", String::from_utf8(value).unwrap());
                };
            }).await.unwrap();
        });


        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    Ok(())
}

