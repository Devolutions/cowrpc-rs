#![recursion_limit = "128"]
extern crate proc_macro;
extern crate proc_macro2;
#[macro_use]
extern crate quote;
extern crate syn;

mod call;
mod iface;

#[proc_macro_derive(CowRpcCall, attributes(cowrpc))]
pub fn derive_cowrpc_call(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    match call::impl_cowrpc_call(input) {
        Ok(gen) => {
            //            {
            //                use std::io::Write;
            //                let stdout = ::std::io::stdout();
            //                let mut handle = stdout.lock();
            //                handle.write(format!("///////////////BEGIN CALL CODE GENERATED///////////////\n{}\n///////////////END CALL CODE GENERATED///////////////\n", gen).as_bytes()).unwrap();
            //            }
            gen.into()
        }
        Err(msg) => panic!(msg),
    }
}

#[proc_macro_derive(CowRpcIface, attributes(cowrpc))]
pub fn derive_cowrpc_iface(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    match iface::impl_cowrpc_iface(input) {
        Ok(gen) => {
            //            {
            //                use std::io::Write;
            //                let stdout = ::std::io::stdout();
            //                let mut handle = stdout.lock();
            //                handle.write(format!("///////////////BEGIN IFACE CODE GENERATED///////////////\n{}\n///////////////END IFACE CODE GENERATED///////////////\n", gen).as_bytes()).unwrap();
            //            }
            gen.into()
        }
        Err(msg) => panic!(msg),
    }
}
