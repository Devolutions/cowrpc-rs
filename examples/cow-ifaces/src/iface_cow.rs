#[allow(dead_code)]
#[derive(CowRpcIface)]
struct IfaceName {
    callname: CallName,
    callname2: CallName2,
}

#[allow(dead_code)]
#[derive(CowRpcCall)]
#[cowrpc(iface_name = "IfaceName")]
struct CallName {
    #[cowrpc(input)]
    param1: u64,
    #[cowrpc(input)]
    #[cowrpc(output)]
    param2: u64,
    #[cowrpc(input)]
    param3: String,
    #[cowrpc(input)]
    #[cowrpc(output)]
    param4: String,
    #[cowrpc(input)]
    param5: f32,
    #[cowrpc(output)]
    param6: f64,
    #[cowrpc(input)]
    param7: Vec<u8>,
    #[cowrpc(output)]
    param8: Vec<u8>,
    #[cowrpc(input)]
    param9: bool,
    #[cowrpc(result)]
    cow_return: (),
}

#[allow(dead_code)]
#[derive(CowRpcCall)]
#[cowrpc(iface_name = "IfaceName")]
struct CallName2 {
    #[cowrpc(input)]
    param1: u64,
    #[cowrpc(input)]
    #[cowrpc(output)]
    param2: u64,
    #[cowrpc(input)]
    param3: String,
    #[cowrpc(input)]
    #[cowrpc(output)]
    param4: String,
    #[cowrpc(result)]
    cow_return: u64,
}

pub trait CowIfaceNameIface: Send + Sync {
    fn callname(&self, call_ctx: CowRpcCallContext, params: IfaceNameCallNameInParams) -> IfaceNameCallNameOutParams;
    fn callname2(&self, call_ctx: CowRpcCallContext, params: IfaceNameCallName2InParams)
        -> IfaceNameCallName2OutParams;
}
