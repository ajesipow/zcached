use zcached::Server;

fn main() {
    let server = Server::bind("127.0.0.1:7891");
    server.run();
}
