use std::str::from_utf8;

pub enum Request<'a> {
    Get(&'a str),
    Set { key: &'a str, value: &'a str },
    Delete(&'a str),
    Flush,
}

// TODO Error handling

pub fn parse_request(input: &[u8]) -> Option<(Request<'_>, usize)> {
    let mut cursor = 0;
    let op_code = input.get(cursor)?;
    cursor += 1;

    let request = match &op_code {
        1 => {
            let key = read_element(input, &mut cursor)?;
            Request::Get(key)
        }
        2 => {
            let key = read_element(input, &mut cursor)?;
            let value = read_element(input, &mut cursor)?;
            Request::Set { key, value }
        }
        3 => {
            let key = read_element(input, &mut cursor)?;
            Request::Delete(key)
        }
        4 => Request::Flush,
        _ => return None,
    };
    Some((request, cursor))
}

pub fn serialize_request(request: Request) -> Vec<u8> {
    match request {
        Request::Get(key) => {
            let mut data = vec![];
            data.push(1);
            data.extend((key.len() as u32).to_be_bytes());
            data.extend(key.as_bytes());
            println!("{:?}", data);
            data
        }
        Request::Set { key, value } => {
            let mut data = vec![];
            data.push(2);
            data.extend((key.len() as u32).to_be_bytes());
            data.extend(key.as_bytes());
            data.extend((value.len() as u32).to_be_bytes());
            data.extend(value.as_bytes());
            println!("{:?}", data);
            data
        }
        Request::Delete(key) => {
            let mut data = vec![];
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

/// Reads an element (key or value) from the buffer and advances the cursor.
fn read_element<'a>(
    input: &'a [u8],
    cursor: &mut usize,
) -> Option<&'a str> {
    let element_size_len = 4;
    // check enough bytes in input
    let element_size_end = *cursor + element_size_len;
    if input.len() < element_size_end {
        println!("not enough data for reading element size");
        return None;
    }
    let element_size =
        u32::from_be_bytes(input[*cursor..element_size_end].try_into().unwrap()) as usize;
    *cursor = element_size_end;
    // check enough bytes in input
    let element_end = *cursor + element_size;
    if input.len() < element_end {
        println!("not enough data for reading full element");
        return None;
    }
    let element_bytes = &input[*cursor..element_end];
    *cursor += element_size;
    let element = from_utf8(&element_bytes).unwrap();
    Some(element)
}
