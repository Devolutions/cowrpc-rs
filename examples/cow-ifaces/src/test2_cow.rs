#![allow(warnings)]

#[derive(CowRpcIface)]
struct Test2 {
    hello: Hello,
}

#[derive(CowRpcCall)]
#[cowrpc(iface_name = "Test2")]
struct Hello {
    #[cowrpc(input)]
    id: i32,
    #[cowrpc(input)]
    name: String,
    #[cowrpc(result)]
    cow_return: String,
}

pub trait CowTest2Iface: Send + Sync {
    fn hello(&self, call_ctx: CowRpcCallContext, params: Test2HelloInParams) -> Test2HelloOutParams;
}
