use core::num;
use std::{fmt::{Error, Result}, io::{Read, Write}, iter::Zip, net::{TcpListener, TcpStream}};


fn main() {
    let listener = TcpListener::bind("127.0.0.1:6123").unwrap();

    for stream in listener.incoming() {
        handle_client(stream.unwrap());
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut buffer: Vec<u8> = Vec::new();
    let mut temp_buff: [u8; 1024] = [0; 1024];

    loop {
        let size = stream.read(&mut temp_buff).unwrap();
        
        if size == 0 {
            break;
        }

        buffer.extend_from_slice(&temp_buff[..size]);
        loop {
            match try_parse_request(&buffer) {
                Some((elements, consumed)) => {
                    let response = dispatch(&elements);
                    println!("{:?}", elements.iter().map(|e| String::from_utf8_lossy(e)).collect::<Vec<_>>());
                    stream.write_all(&response).unwrap();
                    buffer.drain(..consumed);
                },
                None => break,
            }
        }
    }
}


/// returns the index from the START of the provided buf to the end of a redis request segment denoted by \r\n
fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

/// Tries to parse a redis style request, returns None if malformed or if it is missing bytes
fn try_parse_request(buf: &[u8]) -> Option<(Vec<Vec<u8>>, usize)>  {
    let mut pos = 0;

    if buf.first() != Some(&b'*') {
        return None
    }
    pos += 1;

    let crlf = find_crlf(&buf[pos..])?;

    let number_of_elements: usize = std::str::from_utf8(&buf[pos..pos + crlf]).ok()?.parse().ok()?;
    
    pos += crlf + 2;

    let mut elements = Vec::with_capacity(number_of_elements);
    for _ in 0..number_of_elements {
        if buf.get(pos) != Some(&b'$') {
            return None;
        }
        pos += 1;
        let crlf = find_crlf(&buf[pos..])?;

        let len: usize = std::str::from_utf8(&buf[pos..pos + crlf]).ok()?.parse().ok()?;
        pos += crlf + 2;

        if buf.len() < pos + len + 2 {
            return None;
        }
        elements.push(buf[pos..pos + len].to_vec());
        pos += len + 2;
    }

    return Some((elements, pos));
}

fn dispatch(elements: &Vec<Vec<u8>>) -> Vec<u8> {
    return b"+OK\r\n".to_vec()
}