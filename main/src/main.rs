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
    InvalidFormat
}

enum Parsed {
    Complete(Vec<Vec<u8>>, usize),
    Incomplete
}

enum ProtocolError {
    Malformed,
    BadLength
}

type Store = Mutex<HashMap<Vec<u8>, Vec<u8>>>;

const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
const MAX_MULTIBULK: usize = 1024 * 1024;
const MAX_ERR_LEN: usize = 128;

fn main() {
    let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));
    let listener = TcpListener::bind("127.0.0.1:6123").unwrap();

    for stream in listener.incoming() {
        let kv_store = Arc::clone(&store);
        thread::spawn(|| {
            handle_client(stream.unwrap(), kv_store);
        });
    }
}

fn handle_client(mut stream: TcpStream, kv_store: Arc<Store>) {
    let mut buffer: Vec<u8> = Vec::new();
    let mut temp_buff: [u8; 1024] = [0; 1024];

    loop {
        loop {    
            match try_parse_request(&buffer) {
                Ok(Parsed::Complete(elements, consumed)) => {
                    let response: Vec<u8> = dispatch(&elements, &kv_store).into();
                    stream.write_all(&response).unwrap();
                    buffer.drain(..consumed);
                }
                Ok(Parsed::Incomplete) => break,
                Err(e) => {
                    let _ = stream.write_all(&Vec::from(Reply::Error(e.into())));
                    return;
                }
            }
        }

        match stream.read(&mut temp_buff) {
            Ok(0) => return,
            Ok(n)  => buffer.extend_from_slice(&temp_buff[..n]),
            Err(_) => return
        }
    }
}


/// Returns the index from the START of the provided buf to the end of a redis request segment denoted by \r\n
fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

/// Tries to parse a redis style request, returns ProtocolError on malformed input or Parsed enum.
fn try_parse_request(buf: &[u8]) -> Result<Parsed, ProtocolError>  {
    let mut pos = 0;

    match buf.first() {
        None => return Ok(Parsed::Incomplete),
        Some(&b'*') => {}
        Some(_) => return Err(ProtocolError::Malformed),
    }

    pos += 1;

    let crlf = match find_crlf(&buf[pos..]) {
        Some(idx) => idx,
        None => return Ok(Parsed::Incomplete)
    };

    let number_of_elements: usize = std::str::from_utf8(&buf[pos..pos + crlf])
        .map_err(|_| ProtocolError::Malformed)?
        .parse()
        .map_err(|_| ProtocolError::BadLength)?;

    if number_of_elements > MAX_MULTIBULK {
        return Err(ProtocolError::BadLength);
    }

    pos += crlf + 2;

    let mut elements = Vec::with_capacity(number_of_elements);
    for _ in 0..number_of_elements {
        let Some(&marker) = buf.get(pos) else {
            return Ok(Parsed::Incomplete);
        };

        if marker != b'$' {
            return Err(ProtocolError::Malformed);
        }

        pos += 1;

        let crlf = match find_crlf(&buf[pos..]) {
            Some(idx) => idx,
            None => return Ok(Parsed::Incomplete)
        };

        let len: usize = std::str::from_utf8(&buf[pos..pos + crlf])
            .map_err(|_| ProtocolError::Malformed)?
            .parse()
            .map_err(|_| ProtocolError::BadLength)?;

        if len > MAX_BULK_LEN {
            return Err(ProtocolError::BadLength);
        }
        pos += crlf + 2;

        if buf.len() < pos + len + 2 {
            return Ok(Parsed::Incomplete);
        }
        elements.push(buf[pos..pos + len].to_vec());
        pos += len + 2;
    }

    Ok(Parsed::Complete(elements, pos))
}

fn dispatch(elements: &[Vec<u8>], kv_store: &Store) -> Reply {
    match request_type(elements) {
        Ok(action) => match action {
            Action::Set => {
                set_value(elements, kv_store)
            },
            Action::Del => {
                delete_item(elements, kv_store)
            },
            Action::Get => {
                get_item(elements, kv_store)
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
    let keys = match elements {
        [_, keys @ ..] if !keys.is_empty() => keys,
        _ => return Reply::Error("ERR wrong number of arguments for 'del' command".into())
    };

    let mut data = kv_store.lock_recover();

    let count = keys.iter().filter_map(|key| data.remove(key)).count();

    Reply::Int(count as i64)
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
            Reply::Error(mut msg) => {
                if msg.len() > MAX_ERR_LEN {
                    let end = (0..=MAX_ERR_LEN).rev()
                        .find(|&i| msg.is_char_boundary(i))
                        .unwrap_or(0);
                    msg.truncate(end);
                }
                format!("-{}\r\n", msg.replace(['\r', '\n'], " ")).into_bytes()
            }
        }
    }
}

impl From<RequestError> for String {
    fn from(value: RequestError) -> Self {
        match value {
            RequestError::Missing => String::from("ERR missing argument(s)"),
            RequestError::InvalidUtf8 => String::from("ERR invalid data"),
            RequestError::UnknownCommand(msg) => format!("ERR unknown command \'{}\'", msg.replace(['\r', '\n'], " ")),
            RequestError::InvalidFormat => String::from("ERR invalid request format"),
        }
    }
}

impl From<ProtocolError> for String {
    fn from(value: ProtocolError) -> Self {
        match value {
            ProtocolError::Malformed => String::from("ERR Protocol error: malformed request."),
            ProtocolError::BadLength => String::from("ERR Protocol error: invalid bulk length."),
        }
    }
}