use crate::Request;
use crate::Response;

pub(crate) trait Serialize {
    fn serialize(self) -> Vec<u8>;
}

impl<'a> Serialize for Request<'a> {
    fn serialize(self) -> Vec<u8> {
        match self {
            Request::Get(key) => {
                let mut data = Vec::with_capacity(key.len() + 5);
                data.push(1);
                data.extend((key.len() as u32).to_be_bytes());
                data.extend(key.as_bytes());
                data
            }
            Request::Set { key, value } => {
                let mut data = Vec::with_capacity(key.len() + value.len() + 9);
                data.push(2);
                data.extend((key.len() as u32).to_be_bytes());
                data.extend(key.as_bytes());
                data.extend((value.len() as u32).to_be_bytes());
                data.extend(value.as_bytes());
                data
            }
            Request::Delete(key) => {
                let mut data = Vec::with_capacity(key.len() + 5);
                data.push(3);
                data.extend((key.len() as u32).to_be_bytes());
                data.extend(key.as_bytes());
                data
            }
            Request::Flush => {
                vec![4]
            }
        }
    }
}

impl Serialize for Response {
    fn serialize(self) -> Vec<u8> {
        match self {
            Response::Get(maybe_key) => {
                let key_len = maybe_key.as_ref().map(|k| k.len()).unwrap_or(0);
                // Reserve enough space so we don't have to reallocate
                let mut data = Vec::with_capacity(key_len + 5);
                data.push(1);
                if let Some(key) = maybe_key {
                    data.extend((key.len() as u32).to_be_bytes());
                    data.extend(key.as_bytes());
                }
                data
            }
            Response::Set => {
                vec![2]
            }
            Response::Delete => {
                vec![3]
            }
            Response::Flush => {
                vec![4]
            }
        }
    }
}
