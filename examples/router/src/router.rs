extern crate cowrpc;
extern crate ctrlc;

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate tokio;

use futures::future::BoxFuture;

use cowrpc::async_router::CowRpcRouter;
use cowrpc::transport::tls::{TlsOptionsBuilder};
use futures::FutureExt;

#[tokio::main]
async fn main() {
    env_logger::init();

    let tls_options = TlsOptionsBuilder::new()
        .cert_from_pkcs12(include_bytes!("../certs/router.local.pfx"), "")
        .acceptor()
        .unwrap();
    //let (router, _router_handle) = CowRpcRouter::new("wss://router.local:12345", Some(tls_options)).await.expect("new router failed");
    let (mut router, _router_handle) = CowRpcRouter::new("tcp://localhost:12346", Some(tls_options)).await.expect("new router failed");

    router.verify_identity_callback(verify_identity_callback).await;

    if let Err(e) = router.spawn().await {
        error!("Failed to run router: {}", e);
    }
}

fn verify_identity_callback(cow_id: u32, msg: &[u8]) -> BoxFuture<(Vec<u8>, Option<String>)>  {
    let req_string = String::from_utf8_lossy(msg).to_string();

    async move {
        (b"HTTP/1.1 200 OK\r\n\r\n".to_vec(), Some("server".to_string()))
    }.boxed()
}
