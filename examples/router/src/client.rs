extern crate cowrpc;
extern crate env_logger;
extern crate log;
extern crate tls_api;

use cowrpc::async_peer::CowRpcPeer;
use std::time::Duration;
use log::{info, error};

#[tokio::main]
async fn main() {
    env_logger::init();

    let (peer, peer_handle) = CowRpcPeer::new_client("tcp://127.0.0.1:12346", None, cowrpc::CowRpcMode::ROUTED);

    let task_handle = tokio::spawn(peer.run());

    // TODO : Find a way to remove that
    std::thread::sleep(Duration::from_secs(2));

    let server_id = peer_handle.resolve_async("server", Duration::from_secs(10)).await.expect("resolve failed");
    info!("server cow_id = {:#010X}", server_id);

    let server_name = peer_handle.resolve_reverse_async(server_id, Duration::from_secs(10)).await.expect("reverse resolve failed");
    info!("server name = {}", server_name);

    let mut http_req = format!("GET {} HTTP/1.1 \r\n", "/");
    http_req.push_str("\r\n");


    let http_response = peer_handle.call_http_async_v2(server_id, http_req.into_bytes(), Duration::from_secs(10)).await.expect("call_http failed");
    let http_response = String::from_utf8_lossy(&http_response).to_string();
    info!("http_response received: {}", http_response);

    if let Ok(Err(e)) = task_handle.await {
        error!("Client stopped with error: {}", e);
    }
}
