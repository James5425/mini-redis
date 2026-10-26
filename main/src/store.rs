use std::{
    collections::HashMap,
    sync::{Mutex, MutexGuard},
};

use crate::protocol::{Action, Reply, request_type};

pub(crate) type Store = Mutex<HashMap<Vec<u8>, Vec<u8>>>;

pub(crate) fn dispatch(elements: &[Vec<u8>], kv_store: &Store) -> Reply {
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

pub(crate) trait LockRecover<T>
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

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use super::*;

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
