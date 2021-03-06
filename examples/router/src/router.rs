extern crate cowrpc;
extern crate ctrlc;

#[macro_use]
extern crate log;
extern crate env_logger;

use cowrpc::async_router::CowRpcRouter;
use cowrpc::transport::tls::{TlsOptionsBuilder};

fn main() {
    env_logger::init();

    let tls_options = TlsOptionsBuilder::new()
        .cert_from_pkcs12(include_bytes!("../certs/router.local.pfx"), "")
        .acceptor()
        .unwrap();
    let (router, _router_handle) = CowRpcRouter::new("wss://router.local:12345", Some(tls_options))
                                                 .expect("new router failed");

    if let Err(e) = router.run() {
        error!("Failed to run router: {}", e);
    }
}
