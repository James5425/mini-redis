use std::{
    io::{Read, Write},
    sync::Arc,
};

use log::{debug, warn};

use crate::{
    protocol::{Parsed, Reply, try_parse_request},
    store::{Store, dispatch},
};

pub(crate) fn handle_client<S>(mut stream: S, kv_store: Arc<Store>)
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
                    if let Err(e) = stream.write_all(&response) {
                        warn!("Failed to write response: {e}");
                        return;
                    }
                    buffer.drain(..consumed);
                }
                Ok(Parsed::Incomplete) => break,
                Err(e) => {
                    warn!("Closing connection after protocol error: {e:?}");
                    if let Err(e) = stream.write_all(&Vec::from(Reply::Error(e.into()))) {
                        debug!("Failed to write protocol error response: {e}");
                    }
                    return;
                }
            }
        }

        match stream.read(&mut temp_buff) {
            Ok(0) => {
                debug!(
                    "Client closed connection; {} buffered bytes discarded",
                    buffer.len()
                );
                return;
            }
            Ok(n) => buffer.extend_from_slice(&temp_buff[..n]),
            Err(e) => {
                warn!("Failed to read request: {e}");
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex};

    use super::*;
    use crate::store::LockRecover;

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
    fn handle_client_empty_eof() {
        let mut stream = TestStream::default();
        let store: Arc<Store> = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));

        handle_client(&mut stream, store);

        assert_eq!(stream.output, vec![]);
        assert_eq!(stream.read_calls, 1);
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
}
