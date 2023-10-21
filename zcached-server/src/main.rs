use zcached::Server;

fn main() {
    let server = Server::new("127.0.0.1:7891");
    server.run();
}
