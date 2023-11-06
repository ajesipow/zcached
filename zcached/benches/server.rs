use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::thread;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rand::distributions::Alphanumeric;
use rand::distributions::DistString;
use rand::distributions::Distribution;
use rand::distributions::Uniform;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use zcached::Client;
use zcached::Database;
use zcached::Server;
use zcached::DB;

fn get_db_key(c: &mut Criterion) {
    let db = DB::new();
    let (senders, receivers): (Vec<Sender<()>>, Vec<Receiver<()>>) =
        (0..10).map(|_| channel()).unzip();

    // let (mut rng, keys, values) = get_random_data();
    // let (data, data_distribution) = get_data_actions_and_distributions(&mut rng, &keys, &values);

    // We spawn some threads that can receive work in the actual benchmark.
    // This saves us the overhead of creating new threads for each iteration of the benchmark,
    // which would dominate the measurement.
    for rx in receivers {
        let db_clone = db.clone();
        thread::spawn(move || loop {
            // We create these strings ahead of time so that the allocation does not contribute to
            // the benchmark.
            let key = "hello".to_string();
            let value = "world".to_string();
            if rx.recv().is_ok() {
                db_clone.get(&key).unwrap();
                db_clone.insert(key, value).unwrap();
            }
        });
    }

    db.insert("hello".to_string(), "world".to_string()).unwrap();
    c.bench_function("get DB key", |b| {
        b.iter(|| {
            for tx in senders.iter() {
                tx.send(()).unwrap()
            }
        })
    });
}

fn get_key(c: &mut Criterion) {
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
    client.set("hello", "world").unwrap();

    c.bench_function("get key", |b| b.iter(|| client.get("hello")));
}

#[derive(Debug)]
enum RandomAccessClientSetup<'a> {
    Set { key: &'a str, value: &'a str },
    Get(&'a str),
    Delete(&'a str),
    Flush,
}

fn random_client_action<'a>(
    client: &mut Client,
    data: &'a [RandomAccessClientSetup<'a>],
    data_distribution: &Uniform<usize>,
    rng: &mut StdRng,
) {
    match data[data_distribution.sample(rng)] {
        RandomAccessClientSetup::Set { key, value } => {
            client.set(key, value).unwrap();
        }
        RandomAccessClientSetup::Get(key) => {
            client.get(key).unwrap();
        }
        RandomAccessClientSetup::Delete(key) => {
            client.delete(key).unwrap();
        }
        RandomAccessClientSetup::Flush => {
            client.flush().unwrap();
        }
    };
}

fn get_random_data() -> (StdRng, Vec<String>, Vec<String>) {
    let mut rng = StdRng::seed_from_u64(42);
    let keys: Vec<String> = (0..10_000)
        .map(|_| {
            let key_len = rng.gen_range(5..=32);
            Alphanumeric.sample_string(&mut rng, key_len)
        })
        .collect();
    let values: Vec<String> = (0..10_000)
        .map(|_| {
            let value_len = rng.gen_range(32..=256);
            Alphanumeric.sample_string(&mut rng, value_len)
        })
        .collect();

    (rng, keys, values)
}

fn get_data_actions_and_distributions<'a>(
    rng: &mut StdRng,
    keys: &'a [String],
    values: &'a [String],
) -> (Vec<RandomAccessClientSetup<'a>>, Uniform<usize>) {
    let key_dist = Uniform::from(0..keys.len());
    let value_dist = Uniform::from(0..values.len());
    let data: Vec<RandomAccessClientSetup> = (0..100_000)
        .map(|_| match rng.gen::<f64>() {
            x if x <= 0.4 => {
                let key = &keys[key_dist.sample(rng)];
                let value = &values[value_dist.sample(rng)];
                RandomAccessClientSetup::Set { key, value }
            }
            x if 0.4 < x && x <= 0.8 => {
                let key = &keys[key_dist.sample(rng)];
                RandomAccessClientSetup::Get(key)
            }
            x if 0.8 < x && x <= 0.95 => {
                let key = &keys[key_dist.sample(rng)];
                RandomAccessClientSetup::Delete(key)
            }
            _ => RandomAccessClientSetup::Flush,
        })
        .collect();
    let distribution = Uniform::from(0..data.len());
    (data, distribution)
}

fn set_and_get_random_access(c: &mut Criterion) {
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

    let (mut rng, keys, values) = get_random_data();
    let (data, data_distribution) = get_data_actions_and_distributions(&mut rng, &keys, &values);

    c.bench_function("set_and_get_random_access", |b| {
        b.iter(|| random_client_action(&mut client, &data, &data_distribution, &mut rng))
    });
}

criterion_group!(benches, get_db_key, get_key, set_and_get_random_access,);
criterion_main!(benches);
