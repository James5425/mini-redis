use std::{io::{Read, Write}, net::{TcpListener, TcpStream}};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6123").unwrap();

    for stream in listener.incoming() {
        handle_client(stream.unwrap());
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut buff: [u8; 1024] = [0; 1024];

    // let size = stream.read(&mut buff).unwrap();
    let size = stream.read(&mut buff).unwrap();
    let text = String::from_utf8_lossy(&buff);
    println!("{:?}", text);

    stream.write_all(&[65, 66]);
}