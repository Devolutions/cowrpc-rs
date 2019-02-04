#![allow(warnings)]

#[derive(CowRpcIface)]
struct Test {
    hello: Hello,
    fnc1: Fnc1,
    goodbye: Goodbye,
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

#[derive(CowRpcCall)]
#[cowrpc(iface_name = "Test")]
struct Goodbye {
    #[cowrpc(result)]
    cow_return: (),
}

pub trait CowTestIface: Send + Sync {
    fn hello(&self, call_ctx: CowRpcCallContext, params: TestHelloInParams) -> TestHelloOutParams;
    fn fnc1(&self, call_ctx: CowRpcCallContext, params: TestFnc1InParams) -> TestFnc1OutParams;
    fn goodbye(&self, call_ctx: CowRpcCallContext, params: TestGoodbyeInParams) -> TestGoodbyeOutParams;
}
