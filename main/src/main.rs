use std::{
    collections::HashMap,
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
};

use log::{debug, error, info};

use crate::{connection::handle_client, store::Store};

mod connection;
mod protocol;
mod store;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));
    let listener = TcpListener::bind("127.0.0.1:6123").unwrap_or_else(|e| {
        error!("Failed to bind 127.0.0.1:6123: {e}");
        std::process::exit(1);
    });
    info!("Listening on 127.0.0.1:6123");

    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => {
                debug!("Accepted connection from {:?}", s.peer_addr());
                s
            }
            Err(e) => {
                error!("Failed to accept connection: {e}");
                continue;
            }
        };
        let kv_store = Arc::clone(&store);
        thread::spawn(|| {
            handle_client(stream, kv_store);
        });
    }
}
