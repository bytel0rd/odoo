Key insert doc

```rust
extern crate pretty_env_logger;
#[macro_use] extern crate log;

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use odoo_core::helpers::hash_key_to_unsigned_int;
use odoo_core::key_listeners::ListenerHub;
use odoo_core::key_store::KeyStore;

#[tokio::main]
async fn main() -> Result<(), ()> {
std::env::set_var("RUST_LOG", "TRACE");
pretty_env_logger::init();
let keystore = Arc::new(KeyStore::new());
let event_hub = Arc::new(ListenerHub::new());

    let (tx, rx) = crossbeam::channel::unbounded();
    keystore.register_listener("Listener_Hub".to_string(), tx.clone());

    let event_hub_async = event_hub.clone();
    tokio::spawn(async move {
        while let Ok(event) = rx.recv() {
            event_hub_async.notify(event).expect("Notification message");
        }
    });

    let event_hub_async = event_hub.clone();
    tokio::spawn(async move {
        let (tx, rx) = crossbeam::channel::bounded(5);
        let channel_uuid = event_hub_async.register_listener(tx).expect("Register listeners");
        event_hub_async.register_event(&channel_uuid, hash_key_to_unsigned_int("KEY_1".as_bytes())).expect("Register Event Listener");
        event_hub_async.register_event(&channel_uuid, hash_key_to_unsigned_int("KEY_2".as_bytes())).expect("Register Event Listener");
        while let Ok(event) = rx.recv() {
            println!("{} Channel 1: {:?}", chrono::Utc::now().naive_utc().timestamp(), event);
        }
    });

    let event_hub_async = event_hub.clone();
    tokio::spawn(async move {
        let (tx, rx) = crossbeam::channel::bounded(5);
        let channel_uuid = event_hub_async.register_listener(tx).expect("Register listeners");
        event_hub_async.register_event(&channel_uuid, hash_key_to_unsigned_int("KEY_1".as_bytes())).expect("Register Event Listener");
        while let Ok(event) = rx.recv() {
            println!("{} Channel 2: {:?}", chrono::Utc::now().naive_utc().timestamp(), event);
        }
    });



    for i in 0..10 {
        let keystore_async = keystore.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(i)).await;
            keystore_async.add_key("KEY_1", i.to_string().into_bytes(), Some(Duration::from_millis(20)));
        });
        // if (i % 3) == 0 {
        //     let keystore_async = keystore.clone();
        //     tokio::spawn(async move {
        //         tokio::time::sleep(Duration::from_millis(i)).await;
        //         keystore_async.add_key("KEY_2", i.to_string().into_bytes());
        //     });
        // }

    }
    println!("Finished!");

    loop {}

    Ok(())
}
```