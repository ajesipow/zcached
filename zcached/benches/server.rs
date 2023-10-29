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
use zcached::Server;

fn get_key(c: &mut Criterion) {
    thread::spawn(move || {
        let server = Server::new("127.0.0.1:6599");
        server.run();
    });
    let mut client = Client::connect("127.0.0.1:6599");
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
) {
    let mut rng = StdRng::seed_from_u64(42);
    match data[data_distribution.sample(&mut rng)] {
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

fn set_and_get_random_access(c: &mut Criterion) {
    thread::spawn(move || {
        let server = Server::new("127.0.0.1:6598");
        server.run();
    });

    // Seed the server with some data
    let mut client = Client::connect("127.0.0.1:6598");

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
    let key_dist = Uniform::from(0..keys.len());
    let value_dist = Uniform::from(0..values.len());
    let data: Vec<RandomAccessClientSetup> = (0..100_000)
        .map(|_| match rng.gen::<f64>() {
            x if x <= 0.4 => {
                let key = &keys[key_dist.sample(&mut rng)];
                let value = &values[value_dist.sample(&mut rng)];
                RandomAccessClientSetup::Set { key, value }
            }
            x if 0.4 < x && x <= 0.8 => {
                let key = &keys[key_dist.sample(&mut rng)];
                RandomAccessClientSetup::Get(key)
            }
            x if 0.8 < x && x <= 0.95 => {
                let key = &keys[key_dist.sample(&mut rng)];
                RandomAccessClientSetup::Delete(key)
            }
            _ => RandomAccessClientSetup::Flush,
        })
        .collect();

    let data_distribution = Uniform::from(0..data.len());

    c.bench_function("set_and_get_random_access", |b| {
        b.iter(|| random_client_action(&mut client, &data, &data_distribution))
    });
}

criterion_group!(benches, get_key, set_and_get_random_access,);
criterion_main!(benches);
