use std::thread::sleep;
use std::time::Duration;

use zcached::Client;

fn main() {
    let mut client = Client::connect("127.0.0.1:7891");
    sleep(Duration::from_secs(1));
    client.get("abc").unwrap();
    println!("GET");
    sleep(Duration::from_secs(1));
    client.set("abc", "ghi").unwrap();
    println!("SET");
    sleep(Duration::from_secs(1));
    client.get("abc").unwrap();
    println!("GET");
    sleep(Duration::from_secs(1));
    client.set("123", "This is some longer text that did not fit into a single TCP request. This is an even longer text for testing the resizing of the buffer. There is even more data in here now. Look at that").unwrap();
    println!("SET");
    sleep(Duration::from_secs(1));
    client.get("123").unwrap();
    println!("GET");
    sleep(Duration::from_secs(1));
}
