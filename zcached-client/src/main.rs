use std::thread;
use std::thread::sleep;
use std::thread::JoinHandle;
use std::time::Duration;

use rand::distributions::Alphanumeric;
use rand::distributions::DistString;
use rand::distributions::Distribution;
use rand::distributions::Uniform;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use zcached::Client;

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
            println!("SET {} {}", key, value);
        }
        RandomAccessClientSetup::Get(key) => {
            client.get(key).unwrap();
            println!("GET {}", key);
        }
        RandomAccessClientSetup::Delete(key) => {
            client.delete(key).unwrap();
            println!("DEL {}", key);
        }
        RandomAccessClientSetup::Flush => {
            client.flush().unwrap();
            println!("FLU");
        }
    };
}

fn main() {
    let threads: Vec<JoinHandle<()>> = (0..10)
        .map(|_| {
            thread::spawn(|| {
                let mut client = Client::connect("127.0.0.1:7891");
                let mut rng = StdRng::from_entropy();
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
                for _ in 0..10 {
                    random_client_action(&mut client, &data, &data_distribution, &mut rng);
                    sleep(Duration::from_millis(20));
                }
            })
        })
        .collect();

    for jh in threads {
        jh.join().unwrap()
    }
}
