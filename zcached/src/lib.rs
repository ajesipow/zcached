mod client;
mod db;
mod error;
mod serialization;
mod server;

use std::str::from_utf8;

pub use client::Client;
pub use db::Database;
pub use db::DB;
use error::Result;
pub use server::Server;
use tracing::debug;

use crate::error::ParsingError;

#[derive(Debug, PartialEq)]
pub enum Response {
    Get(Option<String>),
    Set,
    Delete,
    Flush,
}

pub enum Request<'a> {
    Get(&'a str),
    Set { key: &'a str, value: &'a str },
    Delete(&'a str),
    Flush,
}

pub(crate) fn parse_request(input: &[u8]) -> Result<Option<(Request<'_>, usize)>> {
    let mut cursor = 0;
    let Some(op_code) = input.get(cursor) else {
        return Ok(None);
    };
    cursor += 1;

    // We don't use 0 as opcode as we're using 0-initialised buffers in the server which would
    // lead to wrong parsing.
    let request = match &op_code {
        1 => read_element(input, &mut cursor)?.map(Request::Get),
        2 => {
            match (
                read_element(input, &mut cursor),
                read_element(input, &mut cursor),
            ) {
                (Ok(Some(key)), Ok(Some(value))) => Some(Request::Set { key, value }),
                (Ok(_), Ok(_)) => None,
                (Err(e), _) | (_, Err(e)) => return Err(e),
            }
        }
        3 => read_element(input, &mut cursor)?.map(Request::Delete),
        4 => Some(Request::Flush),
        _ => return Ok(None),
    };
    Ok(request.map(|req| (req, cursor)))
}

pub(crate) fn parse_response(input: &[u8]) -> Result<Option<Response>> {
    let mut cursor = 0;
    let Some(op_code) = input.get(cursor) else {
        return Ok(None);
    };
    cursor += 1;

    // We don't use 0 as opcode as we're using 0-initialised buffers in the server which would
    // lead to wrong parsing.
    let response = match &op_code {
        1 => {
            let key = read_element(input, &mut cursor)?;
            Response::Get(key.map(ToString::to_string))
        }
        2 => Response::Set,
        3 => Response::Delete,
        4 => Response::Flush,
        _ => return Ok(None),
    };
    Ok(Some(response))
}

/// Reads an element (key or value) from the buffer and advances the cursor.
fn read_element<'a>(
    input: &'a [u8],
    cursor: &mut usize,
) -> Result<Option<&'a str>> {
    // The element's length is serialized with 4 bytes
    let element_size_len = 4;
    // Check that enough bytes are in input
    let element_size_end = *cursor + element_size_len;
    if input.len() < element_size_end {
        debug!("not enough data for reading element size");
        return Ok(None);
    }
    let bytes = input[*cursor..element_size_end]
        .try_into()
        .map_err(|_| ParsingError::Other)?;
    let element_size = u32::from_be_bytes(bytes) as usize;
    if element_size == 0 {
        return Ok(None);
    }
    *cursor = element_size_end;
    // Check that enough bytes are in input
    let element_end = *cursor + element_size;
    if input.len() < element_end {
        debug!("not enough data for reading full element");
        return Ok(None);
    }
    let element_bytes = &input[*cursor..element_end];
    *cursor += element_size;
    let element = from_utf8(element_bytes).map_err(ParsingError::from)?;
    Ok(Some(element))
}
