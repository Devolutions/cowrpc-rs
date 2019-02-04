extern crate cow_ifaces;
extern crate cowrpc;
extern crate env_logger;

use cowrpc::cancel_event::CancelEvent;
use cowrpc::peer::CowRpcPeer;
use cowrpc::*;

use cow_ifaces::test2_cow::*;
use cow_ifaces::test_cow::*;

use std::sync::Arc;
use std::time::Duration;

fn main() {
    env_logger::init();

    let (cancel_handle, _cancel_event) = CancelEvent::new();
    let rpc = Arc::new(CowRpc::new(cowrpc::CowRpcRole::PEER, cowrpc::CowRpcMode::DIRECT));

    let iface_test_id = rpc
        .register_iface(cow_test_iface_get_def(), None)
        .expect("register_iface (test) failed");
    let iface_test2_id = rpc
        .register_iface(cow_test2_iface_get_def(), None)
        .expect("register_iface (test2) failed");

    // Connect to the server and bind the RPC interfaces.
    let peer = client_connect(&rpc, "tcp://127.0.0.1:12345", Some(Duration::from_secs(10)), None)
        .expect("client_connect failed");
    CowRpcPeer::start(&peer);

    let server_id = 0;
    let bind_context_test = peer
        .bind_sync(server_id, iface_test_id, Duration::from_secs(10), Some(&cancel_handle))
        .expect("bind_sync failed (test)");
    let bind_context_test2 = peer
        .bind_sync(server_id, iface_test2_id, Duration::from_secs(10), Some(&cancel_handle))
        .expect("bind_sync failed (test2)");

    // Send the RPC calls.
    let params = TestFnc1InParams { id: 99 };
    let result = CowTest::fnc1(&peer, bind_context_test.clone(), params).expect("call to Fnc1 failed");
    println!("Call to Fnc1 returned : {}", result.cow_return);

    let params = TestHelloInParams {
        id: 0,
        name: String::from("Hello"),
    };
    let result = CowTest::hello(&peer, bind_context_test.clone(), params).expect("call to Hello failed");
    println!("Call to Hello returned : {}", result.cow_return);

    CowTest::goodbye(&peer, bind_context_test.clone(), TestGoodbyeInParams {}).expect("call to Goodbye failed");
    println!("Call to Goodbye done");

    let params = Test2HelloInParams {
        id: 0,
        name: String::from("Hello"),
    };
    let result = CowTest2::hello(&peer, bind_context_test2.clone(), params).expect("call to Hello failed");
    println!("Call to Hello returned : {}", result.cow_return);

    // Unbind from the server.
    peer.unbind_sync(bind_context_test, Duration::from_secs(10), Some(&cancel_handle))
        .expect("unbind_sync test failed");
    peer.unbind_sync(bind_context_test2, Duration::from_secs(10), Some(&cancel_handle))
        .expect("unbind_sync test2 failed");

    peer.stop(None).expect("stop failed");
}
