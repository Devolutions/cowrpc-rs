extern crate cow_ifaces;
extern crate cowrpc;
#[macro_use]
extern crate log;
extern crate env_logger;

use cowrpc::peer::CowRpcPeer;
use cowrpc::*;
use std::sync::Arc;
use std::time::Duration;

use cow_ifaces::test2_cow;
use cow_ifaces::test2_cow::*;
use cow_ifaces::test_cow;
use cow_ifaces::test_cow::*;

struct TestIface;

impl CowTestIface for TestIface {
    fn hello(&self, _call_ctx: CowRpcCallContext, params: TestHelloInParams) -> TestHelloOutParams {
        println!(
            "CALL RECEIVED TO Hello on iface Test1 with params {}-{}",
            params.id, &params.name
        );
        TestHelloOutParams {
            cow_return: String::from("Test1"),
        }
    }

    fn fnc1(&self, _call_ctx: CowRpcCallContext, params: TestFnc1InParams) -> TestFnc1OutParams {
        println!("CALL RECEIVED TO Fnc1 with param {}", params.id);
        TestFnc1OutParams { cow_return: 12 }
    }

    fn goodbye(&self, _call_ctx: CowRpcCallContext, _params: TestGoodbyeInParams) -> TestGoodbyeOutParams {
        println!("CALL RECEIVED TO Goodbye");
        TestGoodbyeOutParams { cow_return: () }
    }
}

struct Test2Iface;

impl CowTest2Iface for Test2Iface {
    fn hello(&self, _call_ctx: CowRpcCallContext, params: Test2HelloInParams) -> Test2HelloOutParams {
        println!(
            "CALL RECEIVED TO Hello on iface Test2 with param {}-{}",
            params.id, &params.name
        );
        Test2HelloOutParams {
            cow_return: String::from("Test2"),
        }
    }
}

fn main() {
    env_logger::init();

    let rpc = Arc::new(CowRpc::new(cowrpc::CowRpcRole::PEER, cowrpc::CowRpcMode::DIRECT));

    let test_iface = TestIface;
    let _ = rpc
        .register_iface(
            cow_test_iface_get_def(),
            Some(Box::new(test_cow::ServerDispatch::new(test_iface))),
        ).expect("register_iface failed");

    let test2_iface = Test2Iface;
    let _ = rpc
        .register_iface(
            cow_test2_iface_get_def(),
            Some(Box::new(test2_cow::ServerDispatch::new(test2_iface))),
        ).expect("register_iface failed");

    rpc.server_listen("tcp://127.0.0.1:12345", None, None)
        .expect("server_listen failed");

    loop {
        if let Some(peer) =
            cowrpc::server_connect(&rpc, Some(Duration::from_secs(10)), None).expect("server_connect failed")
        {
            CowRpcPeer::start(&peer);
            let _ = peer.wait_thread_to_finish();
            info!("Client disconnected");
        }
    }
}
