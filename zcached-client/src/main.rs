use zcached::Client;

fn main() {
    let mut client = Client::connect("127.0.0.1:7891");

    client.get("abc");
    client.set("abc", "ghi");
    client.set("123", "This is some longer text that did not fit into a single TCP request. This is an even longer text for testing the resizing of the buffer. There is even more data in here now. Look at that");
}
