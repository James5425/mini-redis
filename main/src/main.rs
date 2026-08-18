use std::{collections::HashMap, io::{Read, Write}, net::{TcpListener, TcpStream}, sync::{Arc, Mutex, MutexGuard}, thread};

enum Reply {
    Ok,
    Bulk(Vec<u8>),
    Nil,
    Int(i64),
    Error(String),
}

enum Action {
    Set,
    Del,
    Get
}

enum RequestError {
    Missing,
    InvalidUtf8,
    UnknownCommand(String),
}

type Store = Mutex<HashMap<Vec<u8>, Vec<u8>>>;

fn main() {
    let store: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));
    let listener = TcpListener::bind("127.0.0.1:6123").unwrap();

    for stream in listener.incoming() {
        let kv_store = Arc::clone(&store);
        thread::spawn(|| {
            handle_client(stream.unwrap(), kv_store);
        });
    }
}

fn handle_client(mut stream: TcpStream, kv_store: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>) {
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
                    let kv_store = Arc::clone(&kv_store);
                    let response: Vec<u8> = dispatch(&elements, kv_store).into();
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

fn dispatch(elements: &Vec<Vec<u8>>, kv_store: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>) -> Reply {
    match request_type(elements) {
        Ok(action) => match action {
            Action::Set => {
                set_value(elements, &kv_store)
            },
            Action::Del => {
                delete_item(elements, &kv_store)
            },
            Action::Get => {
                get_item(elements, &kv_store)
            },
        },
        Err(err) => Reply::Error(err.into()),
    }
}

/// Sets the value of key. If the key exists it is overwritten.
fn set_value(elements: &[Vec<u8>], kv_store: &Store) -> Reply {
    let [_, key, value] = elements else {
        return Reply::Error("ERR wrong number of arguments for 'set' command".into());
    };

    let mut data = kv_store.lock_recover();

    data.insert(key.clone(), value.clone());
    Reply::Ok
}

/// Deletes keys from the store if they exist.
fn delete_item(elements: &[Vec<u8>], kv_store: &Store) -> Reply {
    let mut count = 0;

    let [_, keys @ ..] = elements else {
        return Reply::Error("ERR wrong number of arguments for 'set' command".into());
    };

    let mut data = kv_store.lock_recover();

    keys.iter().for_each(|key| {
        match data.remove(key) {
            Some(_) => count += 1,
            None => {},
        }
    });

    Reply::Int(count)
}

/// Get the value of key. If the key does not exist, nil is returned. An error is returned if the value stored 
/// at key is not a string, because GET only handles string values.
fn get_item(elements: &[Vec<u8>], kv_store: &Store) -> Reply {
    let [_, key] = elements else {
        return Reply::Error("ERR wrong number of arguments for 'get' command".into());
    };

    let data = kv_store.lock_recover();

    match data.get(key) {
        Some(val) => Reply::Bulk(val.clone()),
        None => Reply::Nil,
    }
}

trait LockRecover<T>
where
    T: ?Sized
{
    fn lock_recover(&self) -> MutexGuard<'_, T>;
}

impl<T> LockRecover<T> for Mutex<T> 
where
    T: ?Sized
{
    fn lock_recover(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|e| {
            eprintln!("WARN: store mutex was poisoned by a panicking thread; recovering");
            self.clear_poison();
            e.into_inner()
        })
    }
}


fn request_type(elements: &[Vec<u8>]) -> Result<Action, RequestError> {
    let first = elements.first().ok_or(RequestError::Missing)?;
    parse_action(first)
}

fn parse_action(element: &[u8]) -> Result<Action, RequestError> {
    let command = std::str::from_utf8(element).map_err(|_| RequestError::InvalidUtf8)?;
    match command {
        c if c.eq_ignore_ascii_case("set") => Ok(Action::Set),
        c if c.eq_ignore_ascii_case("del") => Ok(Action::Del),
        c if c.eq_ignore_ascii_case("get") => Ok(Action::Get),
        other => Err(RequestError::UnknownCommand(other.to_string())),
    }
}

impl From<Reply> for Vec<u8> {
    fn from(value: Reply) -> Self {
        match value {
            Reply::Ok => b"+OK\r\n".to_vec(),
            Reply::Bulk(data) => {
                let mut out = format!("${}\r\n", data.len()).into_bytes();
                out.extend_from_slice(&data);
                out.extend(b"\r\n");
                out
            },
            Reply::Nil => b"$-1\r\n".to_vec(),
            Reply::Int(n) => format!(":{}\r\n", n).into_bytes(),
            Reply::Error(msg) => format!("-{}\r\n", msg).into_bytes(),
        }
    }
}

impl From<RequestError> for String {
    fn from(value: RequestError) -> Self {
        match value {
            RequestError::Missing => String::from("ERR missing argument(s)"),
            RequestError::InvalidUtf8 => String::from("ERR invalid data"),
            RequestError::UnknownCommand(msg) => format!("ERR unknown command \'{}\'", msg),
        }
    }
}