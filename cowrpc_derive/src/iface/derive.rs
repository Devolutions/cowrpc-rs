use iface::attr::validate_data_attributes;
use proc_macro;
use proc_macro2::Span;
use quote::Tokens;
use std::collections::HashMap;
use syn;
use syn::{DeriveInput, Ident};

const IFACE_NAME_IDENT: u32 = 0;
const IFACE_CLIENT_IDENT: u32 = 1;
const IFACE_SERVER_IDENT: u32 = 2;
const IFACE_DEF_CONST_IDENT: u32 = 3;
const IFACE_DEF_TYPE_IDENT: u32 = 4;

pub fn impl_cowrpc_iface(input: proc_macro::TokenStream) -> Result<Tokens, String> {
    let input: DeriveInput = syn::parse(input).unwrap();

    let attributes = validate_data_attributes(&input.attrs)?;

    if attributes.async {
        // Gen Async Definition
        let idents = get_iface_async_idents(&input)?;

        let usage_tokens = gen_async_usages()?;
        let server_tokens = gen_async_server_common(&idents)?;
        let iface_def_tokens = gen_iface_def(&idents, &input, true)?;

        let quote = quote! {
            #usage_tokens

            #server_tokens
            #iface_def_tokens
        };

        Ok(quote)
    } else {
        // Gen Sync Definition

        let idents = get_iface_idents(&input)?;

        let usage_tokens = gen_usages()?;
        let server_tokens = gen_server_common(&idents)?;
        let iface_def_tokens = gen_iface_def(&idents, &input, false)?;

        let quote = quote! {
            #usage_tokens

            #server_tokens
            #iface_def_tokens
        };

        Ok(quote)
    }
}

fn gen_usages() -> Result<Tokens, String> {
    Ok(quote! {
        use std::io::{Read, Write};
        use std::sync::Arc;
        use rmp;
        use cowrpc;
        use cowrpc::msgpack::msgpack_decode_binary;
        use cowrpc::{CowRpcBindContext, CowRpcCallContext, CowRpcIfaceReg, CowRpcParams, CowRpcProcReg};
        use cowrpc::peer::CowRpcPeer;
        use cowrpc::error::{CowRpcError, CowRpcErrorCode, Result};
    })
}

fn gen_async_usages() -> Result<Tokens, String> {
    Ok(quote! {
        use std::io::{Read, Write};
        use std::sync::Arc;
        use rmp;
        use futures::{Future, future::err};
        use cowrpc;
        use cowrpc::msgpack::msgpack_decode_binary;
        use cowrpc::{CowRpcAsyncBindContext, CowRpcCallContext, CowRpcIfaceReg, CowRpcParams, CowRpcProcReg};
        use cowrpc::async_peer::CowRpcPeerHandle;
        use cowrpc::{CowFuture, CallFuture};
        use cowrpc::error::{CowRpcError, CowRpcErrorCode, Result};
    })
}

///
/// Generate structures and variables who defined the interface. It looks like this :
///
/// const COW_IFACENAME_CALLNAME_ID : u16 = 0u16 ;
/// const COW_IFACENAME_CALLNAME2_ID : u16 = 1u16 ;
/// const COW_IFACENAME_PROC_COUNT : usize = 2usize ;
///
/// static COW_IFACENAME_IFACE_DEF : CowIfaceNameIfaceDef = CowIfaceNameIfaceDef {
///    name : stringify ! ( IfaceName ) ,
///    procs : [ CowRpcProcReg { name : stringify ! ( CallName ) , id : COW_IFACENAME_CALLNAME_ID , } ,
///        CowRpcProcReg { name : stringify ! ( CallName2 ) , id : COW_IFACENAME_CALLNAME2_ID , } ,
///    ]
/// } ;
/// struct CowIfaceNameIfaceDef {
///    name : & 'static str , procs : [ CowRpcProcReg ; COW_IFACENAME_PROC_COUNT ] ,
/// }
/// pub fn cow_ifacename_iface_get_def ( ) -> CowRpcIfaceReg {
///    return CowRpcIfaceReg :: new ( COW_IFACENAME_IFACE_DEF . name , COW_IFACENAME_IFACE_DEF . procs . to_vec ( ) ) ;
/// }
///
/// impl cowrpc :: Server for ServerDispatch {
///    fn dispatch_call ( & self , _caller_id : u32 , proc_id : u16 , payload : & mut Vec < u8 > ) -> Result < Box < CowRpcParams >> {
///        match proc_id {
///            COW_IFACENAME_CALLNAME_ID => {
///                callname ( & self . server , payload )
///            }
///            COW_IFACENAME_CALLNAME2_ID => {
///                callname2 ( & self . server , payload )
///            }
///            _ => Err ( CowRpcError :: CowRpcFailure ( CowRpcErrorCode :: ProcId ) ) ,
///        }
///    }
///}
fn gen_iface_def(idents: &HashMap<u32, Ident>, input: &DeriveInput, async: bool) -> Result<Tokens, String> {
    let iface_name_ident = idents.get(&IFACE_NAME_IDENT).unwrap();
    let iface_def_const_ident = idents.get(&IFACE_DEF_CONST_IDENT).unwrap();
    let iface_def_type_ident = idents.get(&IFACE_DEF_TYPE_IDENT).unwrap();

    let fields = match input.data {
        syn::Data::Struct(ref data) => data.fields.clone(),
        syn::Data::Enum(_) => return Err(String::from("#[derive(CowRpcCall)] only apply to structs at this time")),
        syn::Data::Union(_) => return Err(String::from("#[derive(CowRpcCall)] only apply to structs at this time")),
    };

    let named_fields = match fields {
        syn::Fields::Named(ref f_named) => f_named.named.clone(),
        syn::Fields::Unnamed(_) => return Err(String::from("Unnamed fields are not implemented at this time -> #[derive(CowRpcCall)] only apply to structs with named fields")),
        syn::Fields::Unit => return Err(String::from("Unit fields are not implemented at this time -> #[derive(CowRpcCall)] only apply to structs with named fields")),
    };

    let mut id_tokens: Vec<Tokens> = Vec::new();
    let mut proc_tokens = Vec::new();
    let mut server_call_tokens = Vec::new();

    for (i, f) in named_fields.iter().enumerate() {
        let t = f.ty.clone();
        if let Some(ref ident) = f.ident {
            let id_ident = Ident::new(
                &format!(
                    "COW_{}_{}_ID",
                    iface_name_ident.to_string().to_uppercase(),
                    ident.to_string().to_uppercase()
                ),
                Span::call_site(),
            );
            let index = i as u16;
            id_tokens.push(quote!(const #id_ident: u16 = #index;));

            proc_tokens.push(quote! {
                CowRpcProcReg {
                    name: stringify!(#t),
                    id: #id_ident,
                },
            });

            server_call_tokens.push(quote! {
                #id_ident => {
                    #ident(&self.server, payload, caller_id)
                }
            });
        }
    }

    let dispatch_tokens = gen_server_dispatch_impl(server_call_tokens, async)?;

    let proc_count_ident = Ident::new(
        &format!("COW_{}_PROC_COUNT", iface_name_ident.to_string().to_uppercase()),
        Span::call_site(),
    );
    let index = named_fields.iter().len();
    id_tokens.push(quote!(const #proc_count_ident: usize = #index;));

    let iface_get_def_ident = Ident::new(
        &format!("cow_{}_iface_get_def", iface_name_ident.to_string().to_lowercase()),
        Span::call_site(),
    );
    Ok(quote! {
        #(#id_tokens)*
        static #iface_def_const_ident: #iface_def_type_ident = #iface_def_type_ident {
            name: stringify!(#iface_name_ident),
            procs: [#(#proc_tokens)*]
        };

        struct #iface_def_type_ident {
            name: &'static str,
            procs: [CowRpcProcReg; #proc_count_ident],
        }

        pub fn #iface_get_def_ident() -> CowRpcIfaceReg {
            return CowRpcIfaceReg::new(#iface_def_const_ident.name, #iface_def_const_ident.procs.to_vec());
        }

        #dispatch_tokens
    })
}

fn gen_server_dispatch_impl(server_call_tokens: Vec<Tokens>, async: bool) -> Result<Tokens, String> {
    if async {
        Ok(quote! {
            impl cowrpc::AsyncServer for ServerAsyncDispatch {
                fn dispatch_call(&self, caller_id: u32, proc_id: u16, payload: &mut Vec<u8>) -> CowFuture<Box<CowRpcParams>> {
                    match proc_id {
                    #(#server_call_tokens)*
                    _ => Box::new(::futures::failed(CowRpcError::CowRpcFailure(CowRpcErrorCode::ProcId))),
                    }
                }
            }
        })
    } else {
        Ok(quote! {
            impl cowrpc::Server for ServerDispatch {
                fn dispatch_call(&self, caller_id: u32, proc_id: u16, payload: &mut Vec<u8>) -> Result<Box<CowRpcParams>> {
                    match proc_id {
                    #(#server_call_tokens)*
                    _ => Err(CowRpcError::CowRpcFailure(CowRpcErrorCode::ProcId)),
                    }
                }
            }
        })
    }
}

///
/// Generate common structures needed. It looks like this:
///
/// pub struct CowIfaceName ;
/// pub struct ServerDispatch {
///    server : Box < CowIfaceNameIface > ,
/// }
/// impl ServerDispatch {
///    pub fn new < T : 'static + CowIfaceNameIface > ( iface : T ) -> ServerDispatch {
///        ServerDispatch { server : Box :: new ( iface ) , }
///    }
/// }
///
fn gen_server_common(idents: &HashMap<u32, Ident>) -> Result<Tokens, String> {
    let iface_client_ident = idents.get(&IFACE_CLIENT_IDENT).unwrap();
    let iface_server_ident = idents.get(&IFACE_SERVER_IDENT).unwrap();

    Ok(quote! {
        pub struct #iface_client_ident;
        pub struct ServerDispatch {
            server: Box<#iface_server_ident>,
        }
        impl ServerDispatch {
            pub fn new<T: 'static + #iface_server_ident>(iface:T) -> ServerDispatch {
                ServerDispatch {
                    server: Box::new(iface),
                }
            }
        }
    })
}

///
/// Generate common structures needed. It looks like this:
///
/// pub struct CowIfaceNameAsync ;
/// pub struct ServerAsyncDispatch {
///    server : Box < CowIfaceNameIfaceAsync > ,
/// }
/// impl ServerAsyncDispatch {
///    pub fn new < T : 'static + CowIfaceNameIfaceAsync > ( iface : T ) -> ServerAsyncDispatch {
///        ServerAsyncDispatch { server : Box :: new ( iface ) , }
///    }
/// }
///
fn gen_async_server_common(idents: &HashMap<u32, Ident>) -> Result<Tokens, String> {
    let iface_client_ident = idents.get(&IFACE_CLIENT_IDENT).unwrap();
    let iface_server_ident = idents.get(&IFACE_SERVER_IDENT).unwrap();

    Ok(quote! {
        pub struct #iface_client_ident;
        pub struct ServerAsyncDispatch {
            server: Box<#iface_server_ident>,
        }
        impl ServerAsyncDispatch {
            pub fn new<T: 'static + #iface_server_ident>(iface:T) -> ServerAsyncDispatch {
                ServerAsyncDispatch {
                    server: Box::new(iface),
                }
            }
        }
    })
}

fn get_iface_idents(input: &DeriveInput) -> Result<HashMap<u32, Ident>, String> {
    let mut idents = HashMap::new();

    let iface_name = input.ident.to_string();
    idents.insert(IFACE_NAME_IDENT, Ident::new(&iface_name.to_string(), Span::call_site()));
    idents.insert(
        IFACE_CLIENT_IDENT,
        Ident::new(&format!("Cow{}", iface_name), Span::call_site()),
    );
    idents.insert(
        IFACE_SERVER_IDENT,
        Ident::new(&format!("Cow{}Iface", iface_name), Span::call_site()),
    );
    idents.insert(
        IFACE_DEF_CONST_IDENT,
        Ident::new(
            &format!("COW_{}_IFACE_DEF", iface_name.to_uppercase()),
            Span::call_site(),
        ),
    );
    idents.insert(
        IFACE_DEF_TYPE_IDENT,
        Ident::new(&format!("Cow{}IfaceDef", iface_name), Span::call_site()),
    );
    Ok(idents)
}

fn get_iface_async_idents(input: &DeriveInput) -> Result<HashMap<u32, Ident>, String> {
    let mut idents = HashMap::new();

    let iface_name = input.ident.to_string();
    idents.insert(
        IFACE_NAME_IDENT,
        Ident::new(&format!("{}", iface_name), Span::call_site()),
    );
    idents.insert(
        IFACE_CLIENT_IDENT,
        Ident::new(&format!("Cow{}", iface_name), Span::call_site()),
    );
    idents.insert(
        IFACE_SERVER_IDENT,
        Ident::new(&format!("Cow{}IfaceAsync", iface_name), Span::call_site()),
    );
    idents.insert(
        IFACE_DEF_CONST_IDENT,
        Ident::new(
            &format!("COW_{}_IFACE_DEF", iface_name.to_uppercase()),
            Span::call_site(),
        ),
    );
    idents.insert(
        IFACE_DEF_TYPE_IDENT,
        Ident::new(&format!("Cow{}IfaceDef", iface_name), Span::call_site()),
    );
    Ok(idents)
}
