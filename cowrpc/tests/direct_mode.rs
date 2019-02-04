extern crate cowrpc;
#[macro_use]
extern crate cowrpc_derive;
extern crate rmp;

use std::process::exit;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use cowrpc::peer::CowRpcPeer;
use cowrpc::*;

use ifaces::test_cow;
use ifaces::test_cow::*;
use ready_event::ReadyEvent;

mod ifaces;
mod ready_event;

fn client() {
    let rpc = Arc::new(CowRpc::new(cowrpc::CowRpcRole::PEER, cowrpc::CowRpcMode::DIRECT));

    let iface_test_id = rpc
        .register_iface(cow_test_iface_get_def(), None)
        .expect("register_iface (test) failed");

    // Connect to the server and bind the RPC interfaces.
    let peer = client_connect(&rpc, "tcp://127.0.0.1:12345", Some(Duration::from_secs(5)), None)
        .expect("client_connect failed");
    CowRpcPeer::start(&peer);

    let server_id = 0;
    let bind_context_test = peer
        .bind_sync(server_id, iface_test_id, Duration::from_secs(5), None)
        .expect("bind_sync failed (test)");

    // Send the RPC calls.
    let params = TestHelloInParams {
        id: 0,
        name: String::from("Hello"),
    };

    let result = CowTest::hello(&peer, bind_context_test.clone(), params).expect("call to Hello failed");
    assert_eq!(result.cow_return, "Test1");
    let params = TestFnc1InParams { id: 99 };
    let result = CowTest::fnc1(&peer, bind_context_test.clone(), params).expect("call to Fnc1 failed");
    assert_eq!(result.cow_return, 12);

    // Unbind from the server.
    peer.unbind_sync(bind_context_test, Duration::from_secs(5), None)
        .expect("unbind_sync test failed");

    peer.stop(None).expect("stop failed");
    println!("client done");
}

fn server(ready_event: Arc<ReadyEvent>) {
    let rpc = Arc::new(CowRpc::new(cowrpc::CowRpcRole::PEER, cowrpc::CowRpcMode::DIRECT));

    let test_iface = TestIface;
    let _ = rpc
        .register_iface(
            cow_test_iface_get_def(),
            Some(Box::new(test_cow::ServerDispatch::new(test_iface))),
        ).expect("register_iface failed");

    if let Err(_) = rpc.server_listen("tcp://127.0.0.1:12345", None, None) {
        println!("failed to bind to port, skipping the test.");
        exit(0);
    }

    ready_event.set();

    if let Some(peer) =
        cowrpc::server_connect(&rpc, Some(Duration::from_secs(10)), None).expect("server_connect failed")
    {
        CowRpcPeer::start(&peer);
        let _ = peer.wait_thread_to_finish();
    }
}

#[test]
fn direct_peers() {
    let ready_event = Arc::new(ReadyEvent::new());
    let ready_event2 = ready_event.clone();
    let _server_thread = thread::spawn(move || server(ready_event2));
    ready_event.wait();
    client();
}
