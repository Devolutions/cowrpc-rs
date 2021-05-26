extern crate cowrpc;
extern crate ctrlc;

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate tokio;

use cowrpc::router::CowRpcRouterBuilder;
use futures::future::BoxFuture;
use futures::FutureExt;

#[tokio::main]
async fn main() {
    env_logger::init();

    let mut router = CowRpcRouterBuilder::new()
        .listener_url("ws://localhost:12346")
        .build()
        .await;

    router.verify_identity_callback(verify_identity_callback).await;

    match router.start().await {
        Ok(handle) => {
            // Wait forever
            handle.wait().await;
        }
        Err(e) => {
            error!("Router failed to start: {}", e);
        }
    }
}

fn verify_identity_callback(cow_id: u32, msg: &[u8]) -> BoxFuture<(Vec<u8>, Option<String>)> {
    let req_string = String::from_utf8_lossy(msg).to_string();
    info!(
        "verify identity request received: cow_id={:#010X} \r\n {}",
        cow_id, req_string
    );

    async move { (b"HTTP/1.1 200 OK\r\n\r\n".to_vec(), Some("server".to_string())) }.boxed()
}
