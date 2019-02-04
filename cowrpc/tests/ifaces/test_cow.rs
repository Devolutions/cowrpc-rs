#![allow(warnings)]

#[derive(CowRpcIface)]
struct Test {
    hello: Hello,
    fnc1: Fnc1,
}

#[derive(CowRpcCall)]
#[cowrpc(iface_name = "Test")]
struct Hello {
    #[cowrpc(input)]
    id: i32,
    #[cowrpc(input)]
    name: String,
    #[cowrpc(result)]
    cow_return: String,
}

#[derive(CowRpcCall)]
#[cowrpc(iface_name = "Test")]
struct Fnc1 {
    #[cowrpc(input)]
    id: i32,
    #[cowrpc(result)]
    cow_return: i32,
}

pub trait CowTestIface: Send + Sync {
    fn hello(&self, call_ctx: CowRpcCallContext, params: TestHelloInParams) -> TestHelloOutParams;
    fn fnc1(&self, call_ctx: CowRpcCallContext, params: TestFnc1InParams) -> TestFnc1OutParams;
}

pub struct TestIface;

impl CowTestIface for TestIface {
    fn hello(&self, _call_ctx: CowRpcCallContext, params: TestHelloInParams) -> TestHelloOutParams {
        assert_eq!(params.id, 0);
        assert_eq!(params.name, "Hello");
        TestHelloOutParams {
            cow_return: String::from("Test1"),
        }
    }

    fn fnc1(&self, _call_ctx: CowRpcCallContext, params: TestFnc1InParams) -> TestFnc1OutParams {
        assert_eq!(params.id, 99);
        TestFnc1OutParams { cow_return: 12 }
    }
}
