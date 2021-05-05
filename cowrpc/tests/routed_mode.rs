use cowrpc::async_peer::{CallFuture, CowRpcPeer};
use cowrpc::async_router::CowRpcRouter;
use cowrpc::error::CowRpcError;
use cowrpc::CowRpcCallContext;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::time::Duration;

const ROUTER_URL: &str = "ws://localhost:12346";
const SERVER_IDENTITY: &str = "server";
const VERIFY_REQUEST: &'static [u8] = b"GET / HTTP/1.1 \r\nDen_ID: server \r\nDen-Pop-Token: pop_token\r\n";
const VERIFY_RESPONSE: &'static [u8] = b"HTTP/1.1 200 OK\r\n\r\n";
const HTTP_REQUEST: &'static [u8] = b"GET / HTTP/1.1 \r\n\r\n";
const HTTP_RESPONSE: &'static [u8] = b"HTTP/1.1 400 BAD REQUEST\r\n\r\n";
const HTTP_BIG_REQUEST: &'static [u8] = &[0u8; 5000]; // We don't care what is the request, we only want more than 4096 bytes to split the request in many WS messages.

#[tokio::test(threaded_scheduler)]
async fn router_peers() {
    tokio::spawn(start_router());
    // Wait router to be up and running. TODO remove
    std::thread::sleep(Duration::from_secs(1));

    let mut server = start_server().await.expect("server start failed");
    let mut client = start_client().await.expect("client start failed");

    client.stop().await.expect("client stop failed");
    server.stop().await.expect("server stop failed");
}

async fn start_router() -> Result<(), CowRpcError> {
    let (mut router, _router_handle) = CowRpcRouter::new(ROUTER_URL, None).await.expect("new router failed");

    router.verify_identity_callback(verify_identity_callback).await;

    router.run().await?;

    Ok(())
}

fn verify_identity_callback(_cow_id: u32, verify_request: &[u8]) -> BoxFuture<(Vec<u8>, Option<String>)> {
    assert_eq!(verify_request, VERIFY_REQUEST);
    async move { (VERIFY_RESPONSE.to_vec(), Some(SERVER_IDENTITY.to_string())) }.boxed()
}

async fn start_server() -> Result<CowRpcPeer, CowRpcError> {
    let mut peer = CowRpcPeer::new(ROUTER_URL, None);

    peer.on_http_msg_callback(on_http_call);
    peer.start().await.expect("peer can't start");

    let verify_response = peer
        .verify_async(VERIFY_REQUEST.to_vec(), Duration::from_secs(10))
        .await
        .expect("verify failed");

    assert_eq!(verify_response, VERIFY_RESPONSE);

    Ok(peer)
}

fn on_http_call(_ctx: CowRpcCallContext, request: &mut [u8]) -> CallFuture<Vec<u8>> {
    match request.as_ref() {
        HTTP_REQUEST => {}
        HTTP_BIG_REQUEST => {}
        _ => {
            assert!(false, "Unknown request");
        }
    }
    Box::new(futures::future::ok(HTTP_RESPONSE.to_vec()))
}

async fn start_client() -> Result<CowRpcPeer, CowRpcError> {
    let mut peer = CowRpcPeer::new(ROUTER_URL, None);

    peer.start().await.expect("Peer start failed");

    let server_id = peer
        .resolve_async(SERVER_IDENTITY, Duration::from_secs(10))
        .await
        .expect("resolve failed");

    let server_name = peer
        .resolve_reverse_async(server_id, Duration::from_secs(10))
        .await
        .expect("reverse resolve failed");

    assert!(server_name.eq(SERVER_IDENTITY));

    let http_response = peer
        .call_http_async_v2(server_id, HTTP_REQUEST.to_vec(), Duration::from_secs(10))
        .await
        .expect("call_http failed");

    assert_eq!(http_response, HTTP_RESPONSE);

    let http_response = peer
        .call_http_async_v2(server_id, HTTP_BIG_REQUEST.to_vec(), Duration::from_secs(10))
        .await
        .expect("call_http failed");

    assert_eq!(http_response, HTTP_RESPONSE);

    Ok(peer)
}
