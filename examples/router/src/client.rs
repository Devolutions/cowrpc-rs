extern crate cow_ifaces;
extern crate cowrpc;
extern crate env_logger;
extern crate log;
extern crate tls_api;

use tls_api::Certificate;

use cowrpc::cancel_event::CancelEvent;
use cowrpc::peer::CowRpcPeer;
use cowrpc::*;

use cow_ifaces::iface_cow::*;
use cow_ifaces::test2_cow::*;
use cow_ifaces::test_cow::*;

use std::sync::Arc;
use std::time::Duration;

fn main() {
    env_logger::init();

    let (cancel_handle, _cancel_event) = CancelEvent::new();
    let rpc = Arc::new(CowRpc::new(cowrpc::CowRpcRole::PEER, cowrpc::CowRpcMode::ROUTED));

    let iface_test_id = rpc
        .register_iface(cow_test_iface_get_def(), None)
        .expect("register_iface (test) failed");
    let iface_test2_id = rpc
        .register_iface(cow_test2_iface_get_def(), None)
        .expect("register_iface (test2) failed");
    let ifacename_id = rpc
        .register_iface(cow_ifacename_iface_get_def(), None)
        .expect("register_iface (ifacename) failed");

    let tls_options = TlsOptionsBuilder::new()
        .add_root_certificate(Certificate::from_der(include_bytes!("../certs/rootCA.der").to_vec()))
        .connector()
        .unwrap();

    let peer = Client::new(&rpc, "wss://router.local:12345")
        .timeout(Duration::from_secs(10))
        .tls_options(tls_options)
        .connect()
        .expect("client_connect failed");
    
    CowRpcPeer::start(&peer);

    peer.identify_sync(
        "client",
        CowRpcIdentityType::NONE,
        Duration::from_secs(10),
        Some(&cancel_handle),
    ).expect("Identify failed");
    let server_id = peer
        .resolve_sync("server", Duration::from_secs(10), Some(&cancel_handle))
        .expect("Resolve failed");
    println!("SERVER_ID={}", server_id);

    // Resolve the server with the server id.
    let server_name = peer
        .resolve_reverse_sync(server_id, Duration::from_secs(10), Some(&cancel_handle))
        .expect("Resolve reverse failed");
    println!("Server name={}", server_name);
    assert_eq!(server_name, "server");

    let bind_context_test = peer
        .bind_sync(server_id, iface_test_id, Duration::from_secs(10), Some(&cancel_handle))
        .expect("bind_sync failed (test)");
    let bind_context_test2 = peer
        .bind_sync(server_id, iface_test2_id, Duration::from_secs(10), Some(&cancel_handle))
        .expect("bind_sync failed (test2)");
    let bind_context_ifacename = peer
        .bind_sync(server_id, ifacename_id, Duration::from_secs(10), Some(&cancel_handle))
        .expect("bind_sync failed (ifacename)");

    // Send RPC calls.
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

    let params = IfaceNameCallNameInParams {
        param1: 0,
        param2: 1,
        param3: String::new(),
        param5: 0.0,
        param4: String::from("3"),
        param7: vec![1, 2, 3],
        param9: true,
    };
    let result = CowIfaceName::callname(&peer, bind_context_ifacename.clone(), params).expect("Call to testfct failed");
    println!(
        "Call to testfct returned. Output value: {}-{}-{}-{:?}",
        result.param2, &result.param4, result.param6, &result.param8
    );

    let params = IfaceNameCallName2InParams {
        param1: 0,
        param2: 1,
        param3: String::new(),
        param4: String::from("3"),
    };
    let result =
        CowIfaceName::callname2(&peer, bind_context_ifacename.clone(), params).expect("Call to testfct2 failed");
    println!(
        "Call to testfct2 returned: {}. Output value: {}-{}",
        result.cow_return, result.param2, &result.param4
    );

    // Unbind.
    peer.unbind_sync(bind_context_test, Duration::from_secs(10), Some(&cancel_handle))
        .expect("unbind_sync test failed");
    peer.unbind_sync(bind_context_test2, Duration::from_secs(10), Some(&cancel_handle))
        .expect("unbind_sync test2 failed");
    peer.unbind_sync(bind_context_ifacename, Duration::from_secs(10), Some(&cancel_handle))
        .expect("unbind_sync ifacename failed");

    peer.stop(None).expect("stop failed");
}
