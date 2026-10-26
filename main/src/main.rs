use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex, MutexGuard},
    thread,
};

#[derive(Debug, PartialEq)]
enum Reply {
    Ok,
    Bulk(Vec<u8>),
    Nil,
    Int(i64),
    Error(String),
}

#[derive(Debug, PartialEq)]
enum Action {
    Set,
    Del,
    Get,
}

#[derive(Debug, PartialEq)]
enum RequestError {
    Missing,
    InvalidUtf8,
    UnknownCommand(String),
}

#[derive(Debug, PartialEq)]
enum Parsed {
    Complete(Vec<Vec<u8>>, usize),
    Incomplete,
}

#[derive(Debug, PartialEq)]
enum ProtocolError {
    Malformed,
    BadLength,
}

type Store = Mutex<HashMap<Vec<u8>, Vec<u8>>>;

const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
const MAX_MULTIBULK: usize = 1024 * 1024;
const MAX_ERR_LEN: usize = 128;
const MAX_INLINE_LEN: usize = 64;

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

fn handle_client<S>(mut stream: S, kv_store: Arc<Store>)
where
    S: Read + Write,
{
    let mut buffer: Vec<u8> = Vec::new();
    let mut temp_buff: [u8; 1024] = [0; 1024];

    loop {
        loop {
            match try_parse_request(&buffer) {
                Ok(Parsed::Complete(elements, consumed)) => {
                    let response: Vec<u8> = dispatch(&elements, &kv_store).into();
                    if stream.write_all(&response).is_err() {
                        return;
                    } // The client disconnected
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
            Ok(n) => buffer.extend_from_slice(&temp_buff[..n]),
            Err(_) => return,
        }
    }
}

/// Returns the index from the START of the provided buf to the end of a redis request segment denoted by \r\n
fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

/// Tries to parse a redis style request, returns ProtocolError on malformed input or Parsed enum.
fn try_parse_request(buf: &[u8]) -> Result<Parsed, ProtocolError> {
    let mut pos = 0;

    match buf.first() {
        None => return Ok(Parsed::Incomplete),
        Some(&b'*') => {}
        Some(_) => return Err(ProtocolError::Malformed),
    }

    pos += 1;

    let crlf = match find_crlf(&buf[pos..]) {
        Some(idx) if idx > MAX_INLINE_LEN => return Err(ProtocolError::Malformed),
        Some(idx) => idx,
        None => {
            // A trailing CR may be the first byte of a split CRLF terminator.
            let header_len = buf.len() - pos - usize::from(buf.last() == Some(&b'\r'));
            if header_len > MAX_INLINE_LEN {
                return Err(ProtocolError::Malformed);
            }
            return Ok(Parsed::Incomplete);
        }
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
            Some(idx) if idx > MAX_INLINE_LEN => return Err(ProtocolError::Malformed),
            Some(idx) => idx,
            None => {
                // A trailing CR may be the first byte of a split CRLF terminator.
                let header_len = buf.len() - pos - usize::from(buf.last() == Some(&b'\r'));
                if header_len > MAX_INLINE_LEN {
                    return Err(ProtocolError::Malformed);
                }
                return Ok(Parsed::Incomplete);
            }
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

        if &buf[pos + len..pos + len + 2] != b"\r\n" {
            return Err(ProtocolError::Malformed);
        }

        elements.push(buf[pos..pos + len].to_vec());
        pos += len + 2;
    }

    Ok(Parsed::Complete(elements, pos))
}

fn dispatch(elements: &[Vec<u8>], kv_store: &Store) -> Reply {
    match request_type(elements) {
        Ok(action) => match action {
            Action::Set => set_value(elements, kv_store),
            Action::Del => delete_item(elements, kv_store),
            Action::Get => get_item(elements, kv_store),
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
        _ => return Reply::Error("ERR wrong number of arguments for 'del' command".into()),
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
    T: ?Sized,
{
    fn lock_recover(&self) -> MutexGuard<'_, T>;
}

impl<T> LockRecover<T> for Mutex<T>
where
    T: ?Sized,
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
            }
            Reply::Nil => b"$-1\r\n".to_vec(),
            Reply::Int(n) => format!(":{}\r\n", n).into_bytes(),
            Reply::Error(mut msg) => {
                if msg.len() > MAX_ERR_LEN {
                    let end = (0..=MAX_ERR_LEN)
                        .rev()
                        .find(|&i| msg.is_char_boundary(i))
                        .unwrap_or(0);
                    msg.truncate(end);
                }
                format!("-{}\r\n", msg.replace(['\r', '\n'], " ")).into_bytes()
            }
        }
    }
}
// TODO: swap both of these From<_> for String impls for Display + std::error::Error
impl From<RequestError> for String {
    fn from(value: RequestError) -> Self {
        match value {
            RequestError::Missing => String::from("ERR missing argument(s)"),
            RequestError::InvalidUtf8 => String::from("ERR invalid data"),
            // TODO: drop this replace(), From<Reply> for Vec<u8> already sanitises CRLF on the way out
            RequestError::UnknownCommand(msg) => {
                format!("ERR unknown command \'{}\'", msg.replace(['\r', '\n'], " "))
            }
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

#[cfg(test)]
mod tests {
    use std::{iter, vec};

    use super::*;

    #[test]
    fn try_parse_request_complete_request() {
        let buf = b"*1\r\n$5\r\nhello\r\n".to_vec();
        let result = vec![b"hello".to_vec()];

        assert_eq!(
            try_parse_request(&buf),
            Ok(Parsed::Complete(result, buf.len()))
        );
    }

    #[test]
    fn try_parse_request_partial_request_missing_end_crlf() {
        let buf = b"*1\r\n$5\r\nhello\r".to_vec();

        assert_eq!(try_parse_request(&buf), Ok(Parsed::Incomplete));
    }

    #[test]
    fn try_parse_request_checks_first_byte_is_asterisk() {
        let buf = b"1\r\n$5\r\nhello\r".to_vec();

        assert_eq!(try_parse_request(&buf), Err(ProtocolError::Malformed));
    }

    #[test]
    fn try_parse_request_checks_first_byte_exists() {
        let buf = b"".to_vec();

        assert_eq!(try_parse_request(&buf), Ok(Parsed::Incomplete));
    }

    #[test]
    fn try_parse_request_checks_exceeds_max_line_len_by_one() {
        let mut buf = b"*".to_vec();
        let size = MAX_INLINE_LEN + 1;
        buf.extend(iter::repeat(b'0').take(size));

        assert_eq!(try_parse_request(&buf), Err(ProtocolError::Malformed));
    }

    #[test]
    fn try_parse_request_checks_missing_crlf() {
        let buf = b"*".to_vec();

        assert_eq!(try_parse_request(&buf), Ok(Parsed::Incomplete));
    }

    #[test]
    fn try_parse_request_takes_no_items() {
        let buf = b"*0\r\n".to_vec();

        assert_eq!(
            try_parse_request(&buf),
            Ok(Parsed::Complete(vec!(), buf.len()))
        );
    }

    #[test]
    fn try_parse_request_array_length_invalid_utf8() {
        let buf = b"*\xFF\r\n".to_vec();

        assert_eq!(try_parse_request(&buf), Err(ProtocolError::Malformed));
    }

    #[test]
    fn try_parse_request_overflow_usize() {
        let buf = format!("*{}0\r\n", usize::MAX).into_bytes();

        assert_eq!(try_parse_request(&buf), Err(ProtocolError::BadLength));
    }

    #[test]
    fn try_parse_request_over_max_multibulk() {
        let buf = format!("*{}\r\n", MAX_MULTIBULK + 1).into_bytes();

        assert_eq!(try_parse_request(&buf), Err(ProtocolError::BadLength));
    }

    #[test]
    fn try_parse_request_over_marker_missing() {
        let buf = b"*1\r\n".to_vec();

        assert_eq!(try_parse_request(&buf), Ok(Parsed::Incomplete));
    }

    #[test]
    fn try_parse_request_over_marker_malformed() {
        let buf = b"*1\r\n?".to_vec();

        assert_eq!(try_parse_request(&buf), Err(ProtocolError::Malformed));
    }

    #[test]
    fn try_parse_request_missing_crlf_in_first_element() {
        let buf = b"*1\r\n$\r".to_vec();

        assert_eq!(try_parse_request(&buf), Ok(Parsed::Incomplete));
    }

    #[test]
    fn try_parse_request_checks_first_element_exceeds_max_line_len_by_one() {
        let mut buf = b"*1\r\n$".to_vec();
        let size = MAX_INLINE_LEN + 1;
        buf.extend(iter::repeat(b'0').take(size));

        assert_eq!(try_parse_request(&buf), Err(ProtocolError::Malformed));
    }

    #[test]
    fn try_parse_request_first_element_overflow_usize() {
        let buf = format!("*1\r\n${}0\r\n", usize::MAX).into_bytes();

        assert_eq!(try_parse_request(&buf), Err(ProtocolError::BadLength));
    }

    #[test]
    fn try_parse_request_first_element_over_max_bulk_len() {
        let buf = format!("*1\r\n${}\r\n", MAX_BULK_LEN + 1).into_bytes();

        assert_eq!(try_parse_request(&buf), Err(ProtocolError::BadLength));
    }

    #[test]
    fn try_parse_request_first_element_on_max_bulk_len() {
        let buf = format!("*1\r\n${}\r\n", MAX_BULK_LEN).into_bytes();

        assert_eq!(try_parse_request(&buf), Ok(Parsed::Incomplete));
    }

    #[test]
    fn try_parse_request_multiple_bulk_strings() {
        let buf = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".to_vec();
        let result = vec![b"hello".to_vec(), b"world".to_vec()];

        assert_eq!(
            try_parse_request(&buf),
            Ok(Parsed::Complete(result, buf.len()))
        );
    }

    #[test]
    fn try_parse_request_multiple_requests_return_one() {
        let buf = b"*1\r\n$5\r\nhello\r\n*1\r\n$5\r\nworld\r\n".to_vec();
        let result = vec![b"hello".to_vec()];

        assert_eq!(try_parse_request(&buf), Ok(Parsed::Complete(result, 15)));
    }

    #[test]
    fn try_parse_request_multiple_with_second_partial_requests_return_first() {
        let buf = b"*1\r\n$5\r\nhello\r\n*1\r".to_vec();
        let result = vec![b"hello".to_vec()];

        assert_eq!(try_parse_request(&buf), Ok(Parsed::Complete(result, 15)));
    }

    #[test]
    fn try_parse_request_bulk_zero_length() {
        let buf = b"*1\r\n$0\r\n\r\n".to_vec();

        assert_eq!(
            try_parse_request(&buf),
            Ok(Parsed::Complete(vec![vec![]], 10))
        );
    }

    #[test]
    fn try_parse_request_invalid_payload_terminator() {
        let buf = b"*1\r\n$5\r\nhelloXX\r\n".to_vec();

        assert_eq!(try_parse_request(&buf), Err(ProtocolError::Malformed));
    }

    #[test]
    fn try_parse_request_invalid_length_letter() {
        let buf = b"*a\r\n$5\r\nhello\r\n".to_vec();

        assert_eq!(try_parse_request(&buf), Err(ProtocolError::BadLength));
    }

    #[test]
    fn try_parse_request_invalid_length_negative_number() {
        let buf = b"*-1\r\n$5\r\nhello\r\n".to_vec();

        assert_eq!(try_parse_request(&buf), Err(ProtocolError::BadLength));
    }

    #[test]
    fn try_parse_request_invalid_body_length_letter() {
        let buf = b"*1\r\n$a\r\nhello\r\n".to_vec();

        assert_eq!(try_parse_request(&buf), Err(ProtocolError::BadLength));
    }

    #[test]
    fn try_parse_request_bulk_length_invalid_utf8() {
        let buf = b"*1\r\n$\xFF\r\nhello\r\n".to_vec();

        assert_eq!(try_parse_request(&buf), Err(ProtocolError::Malformed));
    }

    #[test]
    fn try_parse_request_invalid_body_length_negative_number() {
        let buf = b"*1\r\n$-1\r\nhello\r\n".to_vec();

        assert_eq!(try_parse_request(&buf), Err(ProtocolError::BadLength));
    }

    #[test]
    fn try_parse_request_payload_contains_crlf() {
        let buf = b"*1\r\n$2\r\n\r\n\r\n".to_vec();
        let result = vec![b"\r\n".to_vec()];

        assert_eq!(
            try_parse_request(&buf),
            Ok(Parsed::Complete(result, buf.len()))
        );
    }

    #[test]
    fn try_parse_request_payload_contains_nul() {
        let buf = b"*1\r\n$1\r\n\x00\r\n".to_vec();
        let result = vec![b"\x00".to_vec()];

        assert_eq!(
            try_parse_request(&buf),
            Ok(Parsed::Complete(result, buf.len()))
        );
    }

    #[test]
    fn try_parse_request_payload_contains_invalid_utf8() {
        let buf = b"*1\r\n$5\r\nhell\xFF\r\n".to_vec();
        let result = vec![b"hell\xFF".to_vec()];

        assert_eq!(
            try_parse_request(&buf),
            Ok(Parsed::Complete(result, buf.len()))
        );
    }

    #[test]
    fn try_parse_request_bulk_header_over_limit_with_crlf() {
        let mut buf = b"*1\r\n$".to_vec();
        buf.extend(iter::repeat(b'0').take(MAX_INLINE_LEN + 1));
        buf.extend_from_slice(b"\r\n");

        assert_eq!(try_parse_request(&buf), Err(ProtocolError::Malformed));
    }

    #[test]
    fn try_parse_request_bulk_header_at_limit() {
        let mut buf = b"*1\r\n$".to_vec();
        buf.extend(iter::repeat(b'0').take(MAX_INLINE_LEN));

        assert_eq!(try_parse_request(&buf), Ok(Parsed::Incomplete));
        buf.extend_from_slice(b"\r\n");
        assert_eq!(try_parse_request(&buf), Ok(Parsed::Incomplete));
        buf.extend_from_slice(b"\r\n");
        assert_eq!(
            try_parse_request(&buf),
            Ok(Parsed::Complete(vec![vec![]], buf.len()))
        );
    }

    #[test]
    fn try_parse_request_array_header_over_limit_with_crlf() {
        let mut buf = b"*".to_vec();
        buf.extend(iter::repeat(b'0').take(MAX_INLINE_LEN + 1));
        buf.extend_from_slice(b"\r\n");

        assert_eq!(try_parse_request(&buf), Err(ProtocolError::Malformed));
    }

    #[test]
    fn try_parse_request_array_header_at_limit() {
        let mut buf = b"*".to_vec();
        buf.extend(iter::repeat(b'0').take(MAX_INLINE_LEN));

        assert_eq!(try_parse_request(&buf), Ok(Parsed::Incomplete));
        buf.extend_from_slice(b"\r\n");
        assert_eq!(
            try_parse_request(&buf),
            Ok(Parsed::Complete(vec![], buf.len()))
        );
    }

    #[test]
    fn try_parse_request_header_at_limit_with_split_crlf() {
        for prefix in [b"*".as_slice(), b"*1\r\n$".as_slice()] {
            let mut buf = prefix.to_vec();
            buf.extend(iter::repeat(b'0').take(MAX_INLINE_LEN));
            buf.push(b'\r');

            assert_eq!(
                try_parse_request(&buf),
                Ok(Parsed::Incomplete),
                "header prefix: {prefix:?}"
            );
        }
    }

    #[test]
    fn try_parse_request_empty_array_length() {
        let buf = b"*\r\n";
        assert_eq!(try_parse_request(buf), Err(ProtocolError::BadLength));
    }

    #[test]
    fn try_parse_request_empty_bulk_length() {
        let buf = b"*1\r\n$\r\n";
        assert_eq!(try_parse_request(buf), Err(ProtocolError::BadLength));
    }

    #[test]
    fn try_parse_request_array_count_at_limit() {
        let buf = format!("*{}\r\n", MAX_MULTIBULK).into_bytes();

        assert_eq!(try_parse_request(&buf), Ok(Parsed::Incomplete));
    }

    #[test]
    fn try_parse_request_every_proper_prefix_is_incomplete() {
        let buf = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";

        for end in 0..buf.len() {
            assert_eq!(
                try_parse_request(&buf[..end]),
                Ok(Parsed::Incomplete),
                "prefix length: {end}"
            );
        }
        assert_eq!(
            try_parse_request(buf),
            Ok(Parsed::Complete(
                vec![b"hello".to_vec(), b"world".to_vec()],
                buf.len()
            ))
        );
    }

    #[test]
    fn try_parse_request_invalid_second_element_marker() {
        let buf = b"*2\r\n$5\r\nhello\r\n?";

        assert_eq!(try_parse_request(buf), Err(ProtocolError::Malformed));
    }

    #[test]
    fn try_parse_request_checks_each_payload_terminator_byte() {
        for terminator in [b"X\n", b"\rX"] {
            let mut buf = b"*1\r\n$5\r\nhello".to_vec();
            buf.extend_from_slice(terminator);
            assert_eq!(
                try_parse_request(&buf),
                Err(ProtocolError::Malformed),
                "terminator: {terminator:?}"
            );
        }
    }

    #[test]
    fn find_crlf_exists() {
        assert_eq!(find_crlf(b"\r\n"), Some(0));
    }

    #[test]
    fn find_crlf_doesnt_exist() {
        assert_eq!(find_crlf(b"\r"), None);
    }

    #[test]
    fn find_crlf_returns_first_instance() {
        assert_eq!(find_crlf(b"a\r\nb\r\n"), Some(1));
    }

    #[test]
    fn find_crlf_byte_order_matters() {
        assert_eq!(find_crlf(b"\n\r"), None);
    }

    #[test]
    fn find_crlf_correct_offset() {
        assert_eq!(find_crlf(b"hello\r\n"), Some(5));
    }

    #[test]
    fn find_crlf_empty() {
        assert_eq!(find_crlf(b""), None);
    }

    #[test]
    fn set_value_sets_correctly() {
        let elements: [Vec<u8>; 3] = [&b"SET"[..], b"hello", b"world"].map(|b| b.to_vec());
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());

        assert_eq!(set_value(&elements, &store), Reply::Ok);
        assert_eq!(
            store.lock_recover().get("hello".as_bytes()),
            Some(&b"world".to_vec())
        );
    }

    #[test]
    fn set_value_too_few_params() {
        let elements: [Vec<u8>; 2] = [&b"SET"[..], b"world"].map(|b| b.to_vec());
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());

        assert_eq!(
            set_value(&elements, &store),
            Reply::Error("ERR wrong number of arguments for 'set' command".into())
        );
        assert_eq!(store.lock_recover().len(), 0);
    }

    #[test]
    fn set_value_too_many_params() {
        let elements: [Vec<u8>; 4] = [&b"SET"[..], b"hello", b"world", b"1234"].map(|b| b.to_vec());
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());

        assert_eq!(
            set_value(&elements, &store),
            Reply::Error("ERR wrong number of arguments for 'set' command".into())
        );
        assert_eq!(store.lock_recover().len(), 0);
    }

    #[test]
    fn set_value_error_doesnt_touch_store() {
        let elements: [Vec<u8>; 4] = [&b"SET"[..], b"hello", b"world", b"1234"].map(|b| b.to_vec());
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store
            .lock_recover()
            .insert(b"hello".to_vec(), b"test".to_vec());

        assert_eq!(
            set_value(&elements, &store),
            Reply::Error("ERR wrong number of arguments for 'set' command".into())
        );
        assert_eq!(store.lock_recover().len(), 1);
        assert_eq!(
            store.lock_recover().get("hello".as_bytes()),
            Some(&b"test".to_vec())
        );
    }

    #[test]
    fn set_value_overrides_existing_value() {
        let elements: [Vec<u8>; 3] = [&b"SET"[..], b"hello", b"world"].map(|b| b.to_vec());

        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store
            .lock_recover()
            .insert(b"hello".to_vec(), b"test".to_vec());

        assert_eq!(set_value(&elements, &store), Reply::Ok);
        assert_eq!(
            store.lock_recover().get("hello".as_bytes()),
            Some(&b"world".to_vec())
        );
        assert_eq!(store.lock_recover().len(), 1);
    }

    #[test]
    fn set_value_binary_value() {
        let elements: [Vec<u8>; 3] = [&b"SET"[..], b"\xFF", b"\xFF"].map(|b| b.to_vec());

        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());

        assert_eq!(set_value(&elements, &store), Reply::Ok);
        assert_eq!(
            store.lock_recover().get(&b"\xFF".to_vec()),
            Some(&b"\xFF".to_vec())
        );
    }

    #[test]
    fn set_value_empty_value() {
        let elements: [Vec<u8>; 3] = [&b"SET"[..], b"hello", b""].map(|b| b.to_vec());

        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());

        assert_eq!(set_value(&elements, &store), Reply::Ok);
        assert_eq!(
            store.lock_recover().get(&b"hello".to_vec()),
            Some(&b"".to_vec())
        );
    }

    #[test]
    fn set_value_empty_key() {
        let elements: [Vec<u8>; 3] = [&b"SET"[..], b"", b"world"].map(|b| b.to_vec());

        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());

        assert_eq!(set_value(&elements, &store), Reply::Ok);
        assert_eq!(
            store.lock_recover().get(&b"".to_vec()),
            Some(&b"world".to_vec())
        );
    }

    #[test]
    fn get_item_gets_correctly() {
        let elements: [Vec<u8>; 2] = [&b"GET"[..], b"hello"].map(|b| b.to_vec());
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store
            .lock_recover()
            .insert(b"hello".to_vec(), b"world".to_vec());

        assert_eq!(get_item(&elements, &store), Reply::Bulk(b"world".to_vec()));
        assert_eq!(store.lock_recover().len(), 1);
        assert_eq!(
            store.lock_recover().get(b"hello".as_slice()),
            Some(&b"world".to_vec())
        );
    }

    #[test]
    fn get_item_missing_key() {
        let elements: [Vec<u8>; 2] = [&b"GET"[..], b"missing"].map(|b| b.to_vec());
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store
            .lock_recover()
            .insert(b"hello".to_vec(), b"world".to_vec());

        assert_eq!(get_item(&elements, &store), Reply::Nil);
        assert_eq!(store.lock_recover().len(), 1);
        assert_eq!(
            store.lock_recover().get(b"hello".as_slice()),
            Some(&b"world".to_vec())
        );
    }

    #[test]
    fn get_item_too_few_params() {
        let elements: [Vec<u8>; 1] = [b"GET".to_vec()];
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store
            .lock_recover()
            .insert(b"hello".to_vec(), b"world".to_vec());

        assert_eq!(
            get_item(&elements, &store),
            Reply::Error("ERR wrong number of arguments for 'get' command".into())
        );
        assert_eq!(store.lock_recover().len(), 1);
        assert_eq!(
            store.lock_recover().get(b"hello".as_slice()),
            Some(&b"world".to_vec())
        );
    }

    #[test]
    fn get_item_too_many_params() {
        let elements: [Vec<u8>; 3] = [&b"GET"[..], b"hello", b"extra"].map(|b| b.to_vec());
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store
            .lock_recover()
            .insert(b"hello".to_vec(), b"world".to_vec());

        assert_eq!(
            get_item(&elements, &store),
            Reply::Error("ERR wrong number of arguments for 'get' command".into())
        );
        assert_eq!(store.lock_recover().len(), 1);
        assert_eq!(
            store.lock_recover().get(b"hello".as_slice()),
            Some(&b"world".to_vec())
        );
    }

    #[test]
    fn get_item_binary_key_and_value() {
        let elements: [Vec<u8>; 2] = [&b"GET"[..], b"\xFF\x00\r\n"].map(|b| b.to_vec());
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store
            .lock_recover()
            .insert(b"\xFF\x00\r\n".to_vec(), b"\x00\xFF\r\n".to_vec());

        assert_eq!(
            get_item(&elements, &store),
            Reply::Bulk(b"\x00\xFF\r\n".to_vec())
        );
    }

    #[test]
    fn get_item_empty_value() {
        let elements: [Vec<u8>; 2] = [&b"GET"[..], b"hello"].map(|b| b.to_vec());
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store.lock_recover().insert(b"hello".to_vec(), b"".to_vec());

        assert_eq!(get_item(&elements, &store), Reply::Bulk(vec![]));
    }

    #[test]
    fn get_item_empty_key() {
        let elements: [Vec<u8>; 2] = [&b"GET"[..], b""].map(|b| b.to_vec());
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store.lock_recover().insert(b"".to_vec(), b"world".to_vec());

        assert_eq!(get_item(&elements, &store), Reply::Bulk(b"world".to_vec()));
    }

    #[test]
    fn delete_item_deletes_correctly() {
        let elements: [Vec<u8>; 2] = [&b"DEL"[..], b"hello"].map(|b| b.to_vec());
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store
            .lock_recover()
            .insert(b"hello".to_vec(), b"world".to_vec());
        store
            .lock_recover()
            .insert(b"other".to_vec(), b"test".to_vec());

        assert_eq!(delete_item(&elements, &store), Reply::Int(1));
        assert_eq!(store.lock_recover().get(b"hello".as_slice()), None);
        assert_eq!(store.lock_recover().len(), 1);
        assert_eq!(
            store.lock_recover().get(b"other".as_slice()),
            Some(&b"test".to_vec())
        );
    }

    #[test]
    fn delete_item_missing_key() {
        let elements: [Vec<u8>; 2] = [&b"DEL"[..], b"missing"].map(|b| b.to_vec());
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store
            .lock_recover()
            .insert(b"hello".to_vec(), b"world".to_vec());

        assert_eq!(delete_item(&elements, &store), Reply::Int(0));
        assert_eq!(store.lock_recover().len(), 1);
        assert_eq!(
            store.lock_recover().get(b"hello".as_slice()),
            Some(&b"world".to_vec())
        );
    }

    #[test]
    fn delete_item_multiple_keys() {
        let elements: [Vec<u8>; 3] = [&b"DEL"[..], b"hello", b"other"].map(|b| b.to_vec());
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store
            .lock_recover()
            .insert(b"hello".to_vec(), b"world".to_vec());
        store
            .lock_recover()
            .insert(b"other".to_vec(), b"test".to_vec());

        assert_eq!(delete_item(&elements, &store), Reply::Int(2));
        assert_eq!(store.lock_recover().len(), 0);
    }

    #[test]
    fn delete_item_existing_and_missing_keys() {
        let elements: [Vec<u8>; 3] = [&b"DEL"[..], b"missing", b"hello"].map(|b| b.to_vec());
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store
            .lock_recover()
            .insert(b"hello".to_vec(), b"world".to_vec());

        assert_eq!(delete_item(&elements, &store), Reply::Int(1));
        assert_eq!(store.lock_recover().len(), 0);
    }

    #[test]
    fn delete_item_duplicate_keys() {
        let elements: [Vec<u8>; 3] = [&b"DEL"[..], b"hello", b"hello"].map(|b| b.to_vec());
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store
            .lock_recover()
            .insert(b"hello".to_vec(), b"world".to_vec());

        assert_eq!(delete_item(&elements, &store), Reply::Int(1));
        assert_eq!(store.lock_recover().len(), 0);
    }

    #[test]
    fn delete_item_too_few_params() {
        let elements: [Vec<u8>; 1] = [b"DEL".to_vec()];
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store
            .lock_recover()
            .insert(b"hello".to_vec(), b"world".to_vec());

        assert_eq!(
            delete_item(&elements, &store),
            Reply::Error("ERR wrong number of arguments for 'del' command".into())
        );
        assert_eq!(store.lock_recover().len(), 1);
        assert_eq!(
            store.lock_recover().get(b"hello".as_slice()),
            Some(&b"world".to_vec())
        );
    }

    #[test]
    fn delete_item_binary_key() {
        let elements: [Vec<u8>; 2] = [&b"DEL"[..], b"\xFF\x00\r\n"].map(|b| b.to_vec());
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store
            .lock_recover()
            .insert(b"\xFF\x00\r\n".to_vec(), b"world".to_vec());

        assert_eq!(delete_item(&elements, &store), Reply::Int(1));
        assert_eq!(store.lock_recover().len(), 0);
    }

    #[test]
    fn delete_item_empty_key() {
        let elements: [Vec<u8>; 2] = [&b"DEL"[..], b""].map(|b| b.to_vec());
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store.lock_recover().insert(b"".to_vec(), b"".to_vec());

        assert_eq!(delete_item(&elements, &store), Reply::Int(1));
        assert_eq!(store.lock_recover().len(), 0);
    }

    #[test]
    fn parse_action_supported_commands() {
        assert_eq!(parse_action(b"SET"), Ok(Action::Set));
        assert_eq!(parse_action(b"GET"), Ok(Action::Get));
        assert_eq!(parse_action(b"DEL"), Ok(Action::Del));
    }

    #[test]
    fn parse_action_mixed_case() {
        assert_eq!(parse_action(b"sEt"), Ok(Action::Set));
        assert_eq!(parse_action(b"gEt"), Ok(Action::Get));
        assert_eq!(parse_action(b"dEl"), Ok(Action::Del));
    }

    #[test]
    fn parse_action_unknown_command() {
        assert_eq!(
            parse_action(b"PING"),
            Err(RequestError::UnknownCommand("PING".into()))
        );
    }

    #[test]
    fn parse_action_empty_command() {
        assert_eq!(
            parse_action(b""),
            Err(RequestError::UnknownCommand("".into()))
        );
    }

    #[test]
    fn parse_action_invalid_utf8() {
        assert_eq!(parse_action(b"\xFF"), Err(RequestError::InvalidUtf8));
    }

    #[test]
    fn request_type_missing_command() {
        assert_eq!(request_type(&[]), Err(RequestError::Missing));
    }

    #[test]
    fn dispatch_routes_commands() {
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        let set_elements: [Vec<u8>; 3] = [&b"sEt"[..], b"hello", b"world"].map(|b| b.to_vec());
        let get_elements: [Vec<u8>; 2] = [&b"gEt"[..], b"hello"].map(|b| b.to_vec());
        let del_elements: [Vec<u8>; 2] = [&b"dEl"[..], b"hello"].map(|b| b.to_vec());

        assert_eq!(dispatch(&set_elements, &store), Reply::Ok);
        assert_eq!(
            store.lock_recover().get(b"hello".as_slice()),
            Some(&b"world".to_vec())
        );
        assert_eq!(
            dispatch(&get_elements, &store),
            Reply::Bulk(b"world".to_vec())
        );
        assert_eq!(dispatch(&del_elements, &store), Reply::Int(1));
        assert_eq!(dispatch(&get_elements, &store), Reply::Nil);
        assert_eq!(store.lock_recover().len(), 0);
    }

    #[test]
    fn dispatch_invalid_commands_dont_touch_store() {
        let store: Store = Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new());
        store
            .lock_recover()
            .insert(b"hello".to_vec(), b"world".to_vec());

        assert_eq!(
            dispatch(&[], &store),
            Reply::Error("ERR missing argument(s)".into())
        );
        assert_eq!(
            dispatch(&[b"".to_vec()], &store),
            Reply::Error("ERR unknown command ''".into())
        );
        assert_eq!(
            dispatch(&[b"PING".to_vec()], &store),
            Reply::Error("ERR unknown command 'PING'".into())
        );
        assert_eq!(
            dispatch(&[b"\xFF".to_vec()], &store),
            Reply::Error("ERR invalid data".into())
        );
        assert_eq!(store.lock_recover().len(), 1);
        assert_eq!(
            store.lock_recover().get(b"hello".as_slice()),
            Some(&b"world".to_vec())
        );
    }

    #[test]
    fn reply_ok_encodes_correctly() {
        assert_eq!(Vec::from(Reply::Ok), b"+OK\r\n".to_vec());
    }

    #[test]
    fn reply_bulk_encodes_correctly() {
        assert_eq!(
            Vec::from(Reply::Bulk(b"hello".to_vec())),
            b"$5\r\nhello\r\n".to_vec()
        );
    }

    #[test]
    fn reply_bulk_empty() {
        assert_eq!(Vec::from(Reply::Bulk(vec![])), b"$0\r\n\r\n".to_vec());
    }

    #[test]
    fn reply_bulk_binary() {
        assert_eq!(
            Vec::from(Reply::Bulk(b"\xFF\x00\r\n".to_vec())),
            b"$4\r\n\xFF\x00\r\n\r\n".to_vec()
        );
    }

    #[test]
    fn reply_bulk_unicode_uses_byte_length() {
        assert_eq!(
            Vec::from(Reply::Bulk("é".as_bytes().to_vec())),
            b"$2\r\n\xC3\xA9\r\n".to_vec()
        );
    }

    #[test]
    fn reply_nil_encodes_correctly() {
        assert_eq!(Vec::from(Reply::Nil), b"$-1\r\n".to_vec());
    }

    #[test]
    fn reply_int_encodes_correctly() {
        assert_eq!(Vec::from(Reply::Int(0)), b":0\r\n".to_vec());
        assert_eq!(Vec::from(Reply::Int(12)), b":12\r\n".to_vec());
        assert_eq!(Vec::from(Reply::Int(-1)), b":-1\r\n".to_vec());
        assert_eq!(
            Vec::from(Reply::Int(i64::MIN)),
            b":-9223372036854775808\r\n".to_vec()
        );
        assert_eq!(
            Vec::from(Reply::Int(i64::MAX)),
            b":9223372036854775807\r\n".to_vec()
        );
    }

    #[test]
    fn reply_error_encodes_correctly() {
        assert_eq!(
            Vec::from(Reply::Error("ERR test".into())),
            b"-ERR test\r\n".to_vec()
        );
        assert_eq!(Vec::from(Reply::Error("".into())), b"-\r\n".to_vec());
    }

    #[test]
    fn reply_error_sanitises_crlf() {
        assert_eq!(
            Vec::from(Reply::Error("ERR a\rb\nc\r\nd".into())),
            b"-ERR a b c  d\r\n".to_vec()
        );
    }

    #[test]
    fn reply_error_length_boundaries() {
        for length in [MAX_ERR_LEN - 1, MAX_ERR_LEN, MAX_ERR_LEN + 1] {
            let response = Vec::from(Reply::Error("x".repeat(length)));
            let expected = format!("-{}\r\n", "x".repeat(length.min(MAX_ERR_LEN))).into_bytes();

            assert_eq!(response, expected);
        }
    }

    #[test]
    fn reply_error_truncates_at_utf8_boundary() {
        let message = format!("{}éz", "x".repeat(MAX_ERR_LEN - 1));
        let expected = format!("-{}\r\n", "x".repeat(MAX_ERR_LEN - 1)).into_bytes();

        assert_eq!(Vec::from(Reply::Error(message)), expected);
    }

    #[derive(Default)]
    struct TestStream {
        chunks: std::collections::VecDeque<Vec<u8>>,
        output: Vec<u8>,
        read_calls: usize,
        write_calls: usize,
        read_error: bool,
        write_error: bool,
        max_write: Option<usize>,
    }

    impl TestStream {
        fn new(chunks: &[&[u8]]) -> Self {
            Self {
                chunks: chunks.iter().map(|chunk| chunk.to_vec()).collect(),
                ..Self::default()
            }
        }
    }

    impl Read for TestStream {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.read_calls += 1;
            if let Some(chunk) = self.chunks.front_mut() {
                let count = chunk.len().min(buf.len());
                buf[..count].copy_from_slice(&chunk[..count]);
                chunk.drain(..count);
                if chunk.is_empty() {
                    self.chunks.pop_front();
                }
                return Ok(count);
            }
            if self.read_error {
                return Err(std::io::ErrorKind::ConnectionReset.into());
            }
            Ok(0)
        }
    }

    impl Write for TestStream {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.write_calls += 1;
            if self.write_error {
                return Err(std::io::ErrorKind::BrokenPipe.into());
            }
            let count = self.max_write.unwrap_or(buf.len()).min(buf.len());
            self.output.extend_from_slice(&buf[..count]);
            Ok(count)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn handle_client_complete_request() {
        let mut stream = TestStream::new(&[b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n"]);
        let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));

        handle_client(&mut stream, Arc::clone(&store));

        assert_eq!(stream.output, b"+OK\r\n".to_vec());
        assert_eq!(
            store.lock_recover().get(b"hello".as_slice()),
            Some(&b"world".to_vec())
        );
    }

    #[test]
    fn handle_client_fragmented_request() {
        let mut stream = TestStream::new(&[b"*2\r", b"\n$3\r\nGET\r\n$5\r\nhe", b"llo\r\n"]);
        let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));
        store
            .lock_recover()
            .insert(b"hello".to_vec(), b"world".to_vec());

        handle_client(&mut stream, Arc::clone(&store));

        assert_eq!(stream.output, b"$5\r\nworld\r\n".to_vec());
        assert_eq!(stream.read_calls, 4);
    }

    #[test]
    fn handle_client_multiple_requests() {
        let mut stream = TestStream::new(&[b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n*2\r\n$3\r\nDEL\r\n$5\r\nhello\r\n"]);
        let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));

        handle_client(&mut stream, Arc::clone(&store));

        assert_eq!(stream.output, b"+OK\r\n$5\r\nworld\r\n:1\r\n".to_vec());
        assert_eq!(store.lock_recover().len(), 0);
        assert_eq!(stream.read_calls, 2);
    }

    #[test]
    fn handle_client_complete_then_partial_request() {
        let mut stream = TestStream::new(&[
            b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n*2\r\n$3\r\nGET\r\n$5\r\nhe",
            b"llo\r\n",
        ]);
        let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));

        handle_client(&mut stream, Arc::clone(&store));

        assert_eq!(stream.output, b"+OK\r\n$5\r\nworld\r\n".to_vec());
        assert_eq!(
            store.lock_recover().get(b"hello".as_slice()),
            Some(&b"world".to_vec())
        );
        assert_eq!(stream.read_calls, 3);
    }

    #[test]
    fn handle_client_protocol_error_stops_processing() {
        let mut stream =
            TestStream::new(&[b"?", b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n"]);
        let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));

        handle_client(&mut stream, Arc::clone(&store));

        assert_eq!(
            stream.output,
            b"-ERR Protocol error: malformed request.\r\n".to_vec()
        );
        assert_eq!(stream.read_calls, 1);
        assert_eq!(store.lock_recover().len(), 0);
    }

    #[test]
    fn handle_client_bad_length() {
        let mut stream = TestStream::new(&[b"*a\r\n"]);
        let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));

        handle_client(&mut stream, store);

        assert_eq!(
            stream.output,
            b"-ERR Protocol error: invalid bulk length.\r\n".to_vec()
        );
        assert_eq!(stream.read_calls, 1);
    }

    #[test]
    fn handle_client_command_error_continues_processing() {
        let mut stream =
            TestStream::new(&[b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n"]);
        let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));

        handle_client(&mut stream, store);

        assert_eq!(
            stream.output,
            b"-ERR unknown command 'PING'\r\n$-1\r\n".to_vec()
        );
    }

    #[test]
    fn handle_client_eof_with_partial_request() {
        let mut stream = TestStream::new(&[b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nwor"]);
        let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));

        handle_client(&mut stream, Arc::clone(&store));

        assert_eq!(stream.output, vec![]);
        assert_eq!(stream.read_calls, 2);
        assert_eq!(store.lock_recover().len(), 0);
    }

    #[test]
    fn handle_client_empty_eof() {
        let mut stream = TestStream::default();
        let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));

        handle_client(&mut stream, store);

        assert_eq!(stream.output, vec![]);
        assert_eq!(stream.read_calls, 1);
    }

    #[test]
    fn handle_client_read_error() {
        let mut stream = TestStream::new(&[b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nwor"]);
        stream.read_error = true;
        let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));

        handle_client(&mut stream, Arc::clone(&store));

        assert_eq!(stream.output, vec![]);
        assert_eq!(stream.read_calls, 2);
        assert_eq!(store.lock_recover().len(), 0);
    }

    #[test]
    fn handle_client_write_error_stops_processing() {
        let mut stream = TestStream::new(&[
            b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n*2\r\n$3\r\nDEL\r\n$5\r\nhello\r\n",
        ]);
        stream.write_error = true;
        let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));

        handle_client(&mut stream, Arc::clone(&store));

        assert_eq!(stream.output, vec![]);
        assert_eq!(stream.write_calls, 1);
        assert_eq!(stream.read_calls, 1);
        assert_eq!(
            store.lock_recover().get(b"hello".as_slice()),
            Some(&b"world".to_vec())
        );
    }

    #[test]
    fn handle_client_short_writes() {
        let mut stream = TestStream::new(&[b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n"]);
        stream.max_write = Some(2);
        let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));
        store
            .lock_recover()
            .insert(b"hello".to_vec(), b"world".to_vec());

        handle_client(&mut stream, store);

        assert_eq!(stream.output, b"$5\r\nworld\r\n".to_vec());
        assert_eq!(stream.write_calls, 6);
    }

    #[test]
    fn handle_client_zero_write_stops_processing() {
        let mut stream = TestStream::new(&[b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n"]);
        stream.max_write = Some(0);
        let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));

        handle_client(&mut stream, store);

        assert_eq!(stream.output, vec![]);
        assert_eq!(stream.write_calls, 1);
        assert_eq!(stream.read_calls, 1);
    }

    #[test]
    fn handle_client_protocol_error_write_failure() {
        let mut stream = TestStream::new(&[b"?"]);
        stream.write_error = true;
        let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));

        handle_client(&mut stream, store);

        assert_eq!(stream.output, vec![]);
        assert_eq!(stream.write_calls, 1);
        assert_eq!(stream.read_calls, 1);
    }

    #[test]
    fn lock_recover_clears_poison_and_preserves_data() {
        let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));
        let thread_store = Arc::clone(&store);
        let result = thread::spawn(move || {
            let mut data = thread_store.lock().unwrap();
            data.insert(b"hello".to_vec(), b"world".to_vec());
            panic!("poison store");
        })
        .join();

        assert!(result.is_err());
        assert!(store.is_poisoned());
        assert_eq!(
            store.lock_recover().get(b"hello".as_slice()),
            Some(&b"world".to_vec())
        );
        assert!(!store.is_poisoned());
        store
            .lock_recover()
            .insert(b"other".to_vec(), b"test".to_vec());
        assert_eq!(
            store.lock().unwrap().get(b"other".as_slice()),
            Some(&b"test".to_vec())
        );
    }
}
