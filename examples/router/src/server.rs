extern crate cowrpc;
extern crate env_logger;
extern crate log;
extern crate tls_api;

use std::time::Duration;
use log::{info, error};
use cowrpc::async_peer::CowRpcPeer;
// use cowrpc::transport::tls::TlsOptionsBuilder;

// use cow_ifaces::iface_cow;
// use cow_ifaces::iface_cow::*;
// use cow_ifaces::test2_cow;
// use cow_ifaces::test2_cow::*;
// use cow_ifaces::test_cow;
// use cow_ifaces::test_cow::*;

// struct TestIface;
//
// impl CowTestIface for TestIface {
//     fn hello(&self, _call_ctx: CowRpcCallContext, params: TestHelloInParams) -> TestHelloOutParams {
//         println!(
//             "CALL RECEIVED TO Hello on iface Test1 with params {}-{}",
//             params.id, &params.name
//         );
//         TestHelloOutParams {
//             cow_return: String::from("Test1"),
//         }
//     }
//
//     fn fnc1(&self, _call_ctx: CowRpcCallContext, params: TestFnc1InParams) -> TestFnc1OutParams {
//         println!("CALL RECEIVED TO Fnc1 with param {}", params.id);
//         TestFnc1OutParams { cow_return: 12 }
//     }
//
//     fn goodbye(&self, _call_ctx: CowRpcCallContext, _params: TestGoodbyeInParams) -> TestGoodbyeOutParams {
//         println!("CALL RECEIVED TO Goodbye");
//         TestGoodbyeOutParams { cow_return: () }
//     }
// }
//
// struct Test2Iface;
//
// impl CowTest2Iface for Test2Iface {
//     fn hello(&self, _call_ctx: CowRpcCallContext, params: Test2HelloInParams) -> Test2HelloOutParams {
//         println!(
//             "CALL RECEIVED TO Hello on iface Test2 with param {}-{}",
//             params.id, &params.name
//         );
//         Test2HelloOutParams {
//             cow_return: String::from("Test2"),
//         }
//     }
// }
//
// struct IfaceNameIface;
//
// impl CowIfaceNameIface for IfaceNameIface {
//     fn callname(
//         &self,
//         _caller_ctx: CowRpcCallContext,
//         params: IfaceNameCallNameInParams,
//     ) -> IfaceNameCallNameOutParams {
//         println!("CALL RECEIVED TO callname on IfaceName with param :");
//         println!("param1={}", params.param1);
//         println!("param2={}", params.param2);
//         println!("param3={}", &params.param3);
//         println!("param4={}", &params.param4);
//         println!("param5={}", params.param5);
//         println!("param7={:?}", &params.param7);
//         println!("param9={}", params.param9);
//
//         IfaceNameCallNameOutParams {
//             param2: 100,
//             param4: String::from("101"),
//             param6: params.param5 as f64,
//             param8: params.param7.clone(),
//             cow_return: (),
//         }
//     }
//
//     fn callname2(
//         &self,
//         _caller_ctx: CowRpcCallContext,
//         params: IfaceNameCallName2InParams,
//     ) -> IfaceNameCallName2OutParams {
//         println!(
//             "CALL RECEIVED TO callname2 on IfaceName with param {}-{}-{}-{}",
//             params.param1, params.param2, &params.param3, &params.param4
//         );
//
//         IfaceNameCallName2OutParams {
//             param2: 100,
//             param4: String::from("101"),
//             cow_return: 102,
//         }
//     }
// }

#[tokio::main]
async fn main() {
    env_logger::init();

    let (peer, peer_handle) = CowRpcPeer::new_client("tcp://127.0.0.1:12346", None, cowrpc::CowRpcMode::ROUTED);

    if let Err(e) = peer.spawn().await {
        error!("Failed to run server: {}", e);
    }

    let mut verify_req = format!("GET {} HTTP/1.1 \r\n", "/");
    verify_req.push_str(&format!("Den_ID: {} \r\n", "server"));
    verify_req.push_str(&format!("Den-Pop-Token: {}", "pop_token"));
    verify_req.push_str("\r\n");

    let result = peer_handle.verify_async(verify_req.into_bytes(), Duration::from_secs(10)).await.expect("verify failed");
    info!("Verify returned: {}", std::str::from_utf8(&result).unwrap());
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
