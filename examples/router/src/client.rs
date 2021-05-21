extern crate cowrpc;
extern crate env_logger;
extern crate log;
extern crate tls_api;

use cowrpc::peer::CowRpcPeer;
use log::info;
use std::time::Duration;

#[tokio::main]
async fn main() {
    env_logger::init();

    let mut peer = CowRpcPeer::new("ws://127.0.0.1:12346", None);

    peer.start().await.expect("Peer start failed");

    let server_id = peer
        .resolve_async("server", Duration::from_secs(10))
        .await
        .expect("resolve failed");
    info!("server cow_id = {:#010X}", server_id);

    let server_name = peer
        .resolve_reverse_async(server_id, Duration::from_secs(10))
        .await
        .expect("reverse resolve failed");
    info!("server name = {}", server_name);

    let mut http_req = format!("GET {} HTTP/1.1 \r\n", "/");
    http_req.push_str("\r\n");

    let http_response = peer
        .call_http_async_v2(server_id, http_req.into_bytes(), Duration::from_secs(10))
        .await
        .expect("call_http failed");
    let http_response = String::from_utf8_lossy(&http_response).to_string();
    info!("http_response received: {}", http_response);

    peer.stop().await.expect("Peer stop failed");
}
