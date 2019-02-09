#[macro_use]
extern crate cowrpc_derive;

use std::io;
use std::process::exit;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use cowrpc::error::CowRpcError;
use cowrpc::peer::CowRpcPeer;
use cowrpc::router::CowRpcRouter;
use cowrpc::{Client, CowRpc, CowRpcIdentityType};

use crate::ifaces::test_cow;
use crate::ifaces::test_cow::*;
use crate::ready_event::ReadyEvent;

mod ifaces;
mod ready_event;

fn router(ready_event: Arc<ReadyEvent>) {
    let mut router = match CowRpcRouter::new("tcp://127.0.0.1:12346", None) {
        Ok(router) => router,
        Err(_) => {
            println!("Failed to bind to port, aborting test");
            exit(0);
        }
    };

    ready_event.set();

    match router.run(None) {
        Ok(()) => (),
        Err(CowRpcError::Io(ref e)) if e.kind() == io::ErrorKind::AddrInUse => {
            println!("Failed to bind to port, aborting test");
            exit(0);
        }
        Err(e) => panic!(format!("router.run() failed: {}", e)),
    }
}

fn server(ready_event: Arc<ReadyEvent>) {
    let rpc = Arc::new(CowRpc::new(cowrpc::CowRpcRole::PEER, cowrpc::CowRpcMode::ROUTED));

    let test_iface = TestIface;
    let _ = rpc
        .register_iface(
            cow_test_iface_get_def(),
            Some(Box::new(test_cow::ServerDispatch::new(test_iface))),
        ).expect("register_iface failed");

    let peer = Client::new(&rpc, "tcp://127.0.0.1:12346")
        .timeout(Duration::from_secs(5))
        .connect()
        .expect("client_connect failed");

    CowRpcPeer::start(&peer);

    peer.identify_sync("server", CowRpcIdentityType::UPN, Duration::from_secs(5), None)
        .expect("Identify failed");

    ready_event.set();        

    let _ = peer.wait_thread_to_finish();

    peer.stop(None).expect("stop failed");
}

fn client() {
    let rpc = Arc::new(CowRpc::new(cowrpc::CowRpcRole::PEER, cowrpc::CowRpcMode::ROUTED));

    let iface_test_id = rpc
        .register_iface(cow_test_iface_get_def(), None)
        .expect("register_iface (test) failed");

    let peer = Client::new(&rpc, "tcp://127.0.0.1:12346")
        .timeout(Duration::from_secs(5))
        .connect()
        .expect("client_connect failed");

    CowRpcPeer::start(&peer);

    peer.identify_sync("client", CowRpcIdentityType::NONE, Duration::from_secs(5), None)
        .expect("Identify failed");
    let server_id = peer
        .resolve_sync("server", Duration::from_secs(5), None)
        .expect("Resolve failed");

    // Resolve the server with the server id.
    let server_name = peer
        .resolve_reverse_sync(server_id, Duration::from_secs(5), None)
        .expect("Resolve reverse failed");
    assert_eq!(server_name, "server");

    let bind_context_test = peer
        .bind_sync(server_id, iface_test_id, Duration::from_secs(5), None)
        .expect("bind_sync failed (test)");

    // Send RPC calls.
    let params = TestHelloInParams {
        id: 0,
        name: String::from("Hello"),
    };

    let result = CowTest::hello(&peer, bind_context_test.clone(), params).expect("call to Hello failed");
    assert_eq!(result.cow_return, "Test1");

    let params = TestFnc1InParams { id: 99 };
    let result = CowTest::fnc1(&peer, bind_context_test.clone(), params).expect("call to Fnc1 failed");
    assert_eq!(result.cow_return, 12);

    // Unbind.
    peer.unbind_sync(bind_context_test, Duration::from_secs(5), None)
        .expect("unbind_sync test failed");
    peer.stop(None).expect("stop failed");
}

#[test]
fn router_peers() {
    let router_ready_event = Arc::new(ReadyEvent::new());
    let router_ready_event2 = router_ready_event.clone();
    let server_ready_event = Arc::new(ReadyEvent::new());
    let server_ready_event2 = server_ready_event.clone();

    let _router_thread = thread::spawn(|| router(router_ready_event2));
    router_ready_event.wait();
    let _server_thread = thread::spawn(|| server(server_ready_event2));
    server_ready_event.wait();
    client();
}
