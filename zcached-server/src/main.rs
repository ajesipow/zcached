use zcached::Server;

fn main() {
    let server = Server::bind("127.0.0.1:7891");

    println!("Starting server on port: {}", server.port().unwrap());
    server.run();
}
