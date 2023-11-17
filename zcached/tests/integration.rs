use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use zcached::Client;
use zcached::Database;
use zcached::Response;
use zcached::Server;
use zcached::DB;

#[test]
fn setting_and_getting_a_key_works() {
    let host = "127.0.0.1";
    let server = Server::builder()
        .address(format!("{host}:0"))
        .initial_buffer_size(256)
        .max_buffer_size(1024)
        .build()
        .unwrap();
    let port = server.port().unwrap();
    thread::spawn(move || {
        server.run();
    });

    let mut client = Client::connect(format!("{host}:{port}"));
    let key = "abc";
    let value = "123".to_string();
    assert_eq!(client.get(key).unwrap(), Response::Get(None));
    assert_eq!(client.set(key, &value).unwrap(), Response::Set);
    assert_eq!(client.get(key).unwrap(), Response::Get(Some(value)));
}

#[test]
fn deleting_a_key_works() {
    let host = "127.0.0.1";
    let server = Server::builder()
        .address(format!("{host}:0"))
        .initial_buffer_size(256)
        .max_buffer_size(1024)
        .build()
        .unwrap();
    let port = server.port().unwrap();
    thread::spawn(move || {
        server.run();
    });

    let mut client = Client::connect(format!("{host}:{port}"));
    let key = "abc";
    let value = "123".to_string();
    assert_eq!(client.set(key, &value).unwrap(), Response::Set);
    assert_eq!(client.get(key).unwrap(), Response::Get(Some(value)));
    assert_eq!(client.delete(key).unwrap(), Response::Delete);
    assert_eq!(client.get(key).unwrap(), Response::Get(None));
}

#[test]
fn flushing_works() {
    let host = "127.0.0.1";
    let server = Server::builder()
        .address(format!("{host}:0"))
        .initial_buffer_size(256)
        .max_buffer_size(1024)
        .build()
        .unwrap();
    let port = server.port().unwrap();
    thread::spawn(move || {
        server.run();
    });

    let mut client = Client::connect(format!("{host}:{port}"));
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
    assert_eq!(client.flush().unwrap(), Response::Flush);
    assert_eq!(client.get(key_1).unwrap(), Response::Get(None));
    assert_eq!(client.get(key_2).unwrap(), Response::Get(None));
}

#[test]
fn test_basic_contention() {
    let db = DB::new();
    let keys: Vec<_> = (0..10).map(|i| i.to_string()).collect();
    let mut lock = db.write().unwrap();
    for key in &keys {
        lock.insert(key.clone(), "value".to_string());
    }
    drop(lock);
    let iterations = 100_000;
    let n_threads = 4;
    let join_handles: Vec<JoinHandle<_>> = (0..n_threads)
        .map(|_| {
            let db_clone = db.clone();
            let keys_clone = keys.clone();
            thread::spawn(move || {
                let now = Instant::now();
                for _ in 0..iterations {
                    for key in &keys_clone {
                        db_clone.get(key).unwrap();
                    }
                }
                now.elapsed() / iterations
            })
        })
        .collect();
    let result: Result<Vec<Duration>, _> = join_handles.into_iter().map(|jh| jh.join()).collect();
    let durations = result.unwrap();
    println!("durations: {durations:?}");
    assert!(durations.into_iter().all(|d| d < Duration::from_micros(5)));
}
