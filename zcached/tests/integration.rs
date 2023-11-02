use std::thread;

use zcached::Client;
use zcached::Response;
use zcached::Server;

#[test]
fn setting_and_getting_a_key_works() {
    let addr = "127.0.0.1:9876";
    let server = Server::builder()
        .address(addr)
        .initial_buffer_size(256)
        .max_buffer_size(1024)
        .build()
        .unwrap();
    thread::spawn(move || {
        server.run();
    });

    let mut client = Client::connect(addr);
    let key = "abc";
    let value = "123".to_string();
    assert_eq!(client.get(key).unwrap(), Response::Get(None));
    assert_eq!(client.set(key, &value).unwrap(), Response::Set);
    assert_eq!(client.get(key).unwrap(), Response::Get(Some(value)));
}

#[test]
fn deleting_a_key_works() {
    let addr = "127.0.0.1:9876";
    let server = Server::builder()
        .address(addr)
        .initial_buffer_size(256)
        .max_buffer_size(1024)
        .build()
        .unwrap();
    thread::spawn(move || {
        server.run();
    });

    let mut client = Client::connect(addr);
    let key = "abc";
    let value = "123".to_string();
    assert_eq!(client.set(key, &value).unwrap(), Response::Set);
    assert_eq!(client.get(key).unwrap(), Response::Get(Some(value)));
    assert_eq!(client.delete(key).unwrap(), Response::Delete);
    assert_eq!(client.get(key).unwrap(), Response::Get(None));
}

#[test]
fn flushing_works() {
    let addr = "127.0.0.1:9876";
    let server = Server::builder()
        .address(addr)
        .initial_buffer_size(256)
        .max_buffer_size(1024)
        .build()
        .unwrap();
    thread::spawn(move || {
        server.run();
    });

    let mut client = Client::connect(addr);
    let key_1 = "abc";
    let key_2 = "def";
    let value = "123".to_string();
    assert_eq!(client.set(key_1, &value).unwrap(), Response::Set);
    assert_eq!(client.set(key_2, &value).unwrap(), Response::Set);
    assert_eq!(
        client.get(key_1).unwrap(),
        Response::Get(Some(value.clone()))
    );
    assert_eq!(client.get(key_2).unwrap(), Response::Get(Some(value)));
    assert_eq!(client.flush().unwrap(), Response::Delete);
    assert_eq!(client.get(key_1).unwrap(), Response::Get(None));
    assert_eq!(client.get(key_2).unwrap(), Response::Get(None));
}
