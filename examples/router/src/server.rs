extern crate cowrpc;
extern crate env_logger;
extern crate log;
extern crate tls_api;

use std::time::Duration;
use log::{info, error};
use cowrpc::async_peer::CowRpcPeer;
use futures::future::err;

#[tokio::main]
async fn main() {
    env_logger::init();

    let (peer, peer_handle) = CowRpcPeer::new_client("tcp://127.0.0.1:12346", None, cowrpc::CowRpcMode::ROUTED);

    let task_handle = tokio::spawn(peer.run());

    // TODO : REMOVE THAT
    std::thread::sleep(Duration::from_secs(2));

    let mut verify_req = format!("GET {} HTTP/1.1 \r\n", "/");
    verify_req.push_str(&format!("Den_ID: {} \r\n", "server"));
    verify_req.push_str(&format!("Den-Pop-Token: {}", "pop_token"));
    verify_req.push_str("\r\n");

    let result = peer_handle.verify_async(verify_req.into_bytes(), Duration::from_secs(10)).await.expect("verify failed");
    info!("Verify returned: {}", std::str::from_utf8(&result).unwrap());

    if let Ok(Err(e)) = task_handle.await {
        error!("Server stopped with error: {}", e);
    }

    // let task_handle = tokio::spawn(async move {
    //     peer.spawn().await
    // });
    // peer_handle.identify_async("server", CowRpcIdentityType::UPN, Duration::from_secs(10)).await.expect("identify failed");
    //
    // if let Err(e) = task_handle.await {
    //     error!("Failed to run router: {}", e);
    // }
    // let (cancel_handle, _cancel_event) = CancelEvent::new();
    // let rpc = Arc::new(CowRpc::new(cowrpc::CowRpcRole::PEER, cowrpc::CowRpcMode::ROUTED));
    //
    // let test_iface = TestIface;
    // let _ = rpc
    //     .register_iface(
    //         cow_test_iface_get_def(),
    //         Some(Box::new(test_cow::ServerDispatch::new(test_iface))),
    //     ).expect("register_iface failed");
    //
    // let test2_iface = Test2Iface;
    // let _ = rpc
    //     .register_iface(
    //         cow_test2_iface_get_def(),
    //         Some(Box::new(test2_cow::ServerDispatch::new(test2_iface))),
    //     ).expect("register_iface failed");
    //
    // let iface_name_iface = IfaceNameIface;
    // let _ = rpc
    //     .register_iface(
    //         cow_ifacename_iface_get_def(),
    //         Some(Box::new(iface_cow::ServerDispatch::new(iface_name_iface))),
    //     ).expect("register_iface failed");
    //
    // let tls_options = TlsOptionsBuilder::new()
    //     .add_root_certificate(Certificate::from_der(include_bytes!("../certs/rootCA.der").to_vec()))
    //     .connector()
    //     .unwrap();
    //
    // let peer = Client::new(&rpc, "wss://router.local:12345")
    //     .timeout(Duration::from_secs(10))
    //     .tls_options(tls_options)
    //     .connect()
    //     .expect("client_connect failed");
    //
    // CowRpcPeer::start(&peer);
    //
    // peer.identify_sync(
    //     "server",
    //     CowRpcIdentityType::UPN,
    //     Duration::from_secs(10),
    //     Some(&cancel_handle),
    // ).expect("Identify failed");
    //
    // let _ = peer.wait_thread_to_finish();
    //
    // peer.stop(None).expect("stop failed");
}
