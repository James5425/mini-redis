use std::{
    collections::HashMap,
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
};

use crate::{connection::handle_client, store::Store};

mod connection;
mod protocol;
mod store;

fn main() {
    let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));
    let listener = TcpListener::bind("127.0.0.1:6123").expect("Failed to bind 127.0.0.1:6123");

    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => s,
            Err(e) => {
                eprintln!("accept failed {}", e);
                continue;
            }
        };
        let kv_store = Arc::clone(&store);
        thread::spawn(|| {
            handle_client(stream, kv_store);
        });
    }
}
