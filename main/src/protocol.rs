#[derive(Debug, PartialEq)]
pub(crate) enum Reply {
    Ok,
    Bulk(Vec<u8>),
    Nil,
    Int(i64),
    Error(String),
}

#[derive(Debug, PartialEq)]
pub(crate) enum ProtocolError {
    Malformed,
    BadLength,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Parsed {
    Complete(Vec<Vec<u8>>, usize),
    Incomplete,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Action {
    Set,
    Del,
    Get,
}

#[derive(Debug, PartialEq)]
pub(crate) enum RequestError {
    Missing,
    InvalidUtf8,
    UnknownCommand(String),
}

const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
const MAX_MULTIBULK: usize = 1024 * 1024;
const MAX_ERR_LEN: usize = 128;
const MAX_INLINE_LEN: usize = 64;

/// Tries to parse a redis style request, returns ProtocolError on malformed input or Parsed enum.
pub(crate) fn try_parse_request(buf: &[u8]) -> Result<Parsed, ProtocolError> {
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

pub(crate) fn request_type(elements: &[Vec<u8>]) -> Result<Action, RequestError> {
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

/// Returns the index from the START of the provided buf to the end of a redis request segment denoted by \r\n
fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
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

impl From<ProtocolError> for String {
    fn from(value: ProtocolError) -> Self {
        match value {
            ProtocolError::Malformed => String::from("ERR Protocol error: malformed request."),
            ProtocolError::BadLength => String::from("ERR Protocol error: invalid bulk length."),
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

#[cfg(test)]
mod tests {
    use std::iter;

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
    fn request_type_missing_command() {
        assert_eq!(request_type(&[]), Err(RequestError::Missing));
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
}
