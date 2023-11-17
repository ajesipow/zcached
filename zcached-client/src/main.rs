use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;

use zcached::Database;
use zcached::DB;

fn main() {
    let db = DB::new();
    let key = "abc".to_string();
    let mut lock = db.write().unwrap();
    lock.insert(key.clone(), "value".to_string());
    drop(lock);
    let iterations = 100_000;
    let n_threads = 4;
    let join_handles: Vec<JoinHandle<_>> = (0..n_threads)
        .map(|_| {
            let db_clone = db.clone();
            let key_clone = key.clone();
            thread::spawn(move || {
                let now = Instant::now();
                for _ in 0..iterations {
                    db_clone.get(&key_clone).unwrap();
                }
                now.elapsed() / iterations
            })
        })
        .collect();
    let results: Result<Vec<_>, _> = join_handles.into_iter().map(|jh| jh.join()).collect();
    println!("results: {:?}", results.unwrap());
}
