use proc_macro;
use proc_macro2::Span;
use quote::Tokens;
use std::collections::HashMap;
use syn;
use syn::{DeriveInput, Ident};

use crate::call::attr::*;

const IFACE_NAME_IDENT: u32 = 0;
const IFACE_CLIENT_IDENT: u32 = 1;
const IFACE_SERVER_IDENT: u32 = 2;
const CALL_IDENT: u32 = 3;
const IN_PARAM_IDENT: u32 = 4;
const OUT_PARAM_IDENT: u32 = 5;
const CALL_ID_IDENT: u32 = 6;

pub fn impl_cowrpc_call(input: proc_macro::TokenStream) -> Result<Tokens, String> {
    let input: DeriveInput = syn::parse(input).unwrap();

    let data_attrs = validate_data_attributes(&input.attrs)?;

    if data_attrs.r#async {
        // Gen Async Proc
        let idents = if let Some(iface_name) = &data_attrs.iface_name {
            get_call_async_idents(&input, iface_name)?
        } else {
            return Err("Attribute iface_name is mandatory on the structure!".to_string());
        };

        let param_serialization_tokens = gen_param_structs(&idents, &input)?; // doesn't change
        let client_function_tokens = gen_async_client_function(&idents, &input)?;
        let server_function_tokens = gen_async_server_function(&idents, &input)?;

        Ok(quote! {
            #param_serialization_tokens
            #client_function_tokens
            #server_function_tokens
        })
    } else {
        // Gen Sync Proc
        let idents = if let Some(iface_name) = &data_attrs.iface_name {
            get_call_idents(&input, iface_name)?
        } else {
            return Err("Attribute iface_name is mandatory on the structure!".to_string());
        };

        let param_serialization_tokens = gen_param_structs(&idents, &input)?;
        let client_function_tokens = gen_client_function(&idents, &input)?;
        let server_function_tokens = gen_server_function(&idents, &input)?;

        Ok(quote! {
            #param_serialization_tokens
            #client_function_tokens
            #server_function_tokens
        })
    }
}

fn get_call_idents(input: &DeriveInput, iface_name: &str) -> Result<HashMap<u32, Ident>, String> {
    let mut idents = HashMap::new();

    let call_name = input.ident.to_string();

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
        Ident::new(&format!("Cow{}Iface", iface_name), Span::call_site()),
    );
    idents.insert(
        IN_PARAM_IDENT,
        Ident::new(&format!("{}{}InParams", iface_name, call_name), Span::call_site()),
    );
    idents.insert(
        OUT_PARAM_IDENT,
        Ident::new(&format!("{}{}OutParams", iface_name, call_name), Span::call_site()),
    );
    idents.insert(
        CALL_ID_IDENT,
        Ident::new(
            &format!("COW_{}_{}_ID", iface_name.to_uppercase(), call_name.to_uppercase()),
            Span::call_site(),
        ),
    );

    idents.insert(
        CALL_IDENT,
        Ident::new(&format!("{}", call_name.to_lowercase()), Span::call_site()),
    );
    Ok(idents)
}

fn get_call_async_idents(input: &DeriveInput, iface_name: &str) -> Result<HashMap<u32, Ident>, String> {
    let mut idents = HashMap::new();

    let call_name = input.ident.to_string();

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
        IN_PARAM_IDENT,
        Ident::new(&format!("{}{}InParams", iface_name, call_name), Span::call_site()),
    );
    idents.insert(
        OUT_PARAM_IDENT,
        Ident::new(&format!("{}{}OutParams", iface_name, call_name), Span::call_site()),
    );
    idents.insert(
        CALL_ID_IDENT,
        Ident::new(
            &format!("COW_{}_{}_ID", iface_name.to_uppercase(), call_name.to_uppercase()),
            Span::call_site(),
        ),
    );

    idents.insert(
        CALL_IDENT,
        Ident::new(&call_name.to_lowercase().to_string(), Span::call_site()),
    );
    Ok(idents)
}

///
/// Generate the function called when the server receives a call. It looks like this :
///
/// fn callname ( server : & Box < CowIfaceNameIface > , payload : & mut Vec < u8 > , caller_id: u32 ) -> Result < Box < CowRpcParams >> {
///    let mut payload = & payload [ .. ] ;
///    let mut param = IfaceNameCallNameInParams :: read_from ( & mut payload ) ? ;
///    let cow_return = server . callname ( caller_id, & param . param1 , & mut param . param2 , & param . param3 , & mut param . param4 , ) ;
///    let out_param = IfaceNameCallNameOutParams { param2 : param . param2 . clone ( ) , param4 : param . param4 . clone ( ) , cow_return , } ;
///    Ok ( Box :: new ( out_param ) )
/// }
fn gen_server_function(idents: &HashMap<u32, Ident>, input: &DeriveInput) -> Result<Tokens, String> {
    let call_ident = idents.get(&CALL_IDENT).unwrap();
    let in_param_ident = idents.get(&IN_PARAM_IDENT).unwrap();
    let iface_server_ident = idents.get(&IFACE_SERVER_IDENT).unwrap();

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

    let mut return_ident_opt = None;

    for f in named_fields.iter() {
        let attribute = validate_data_attributes(&f.attrs)?;
        if let Some(ref ident) = f.ident {
            match (attribute.input, attribute.output, attribute.result) {
                //input AND output or input or output
                (true, true, false) | (false, true, false) | (true, false, false) => {}
                //Return type
                (false, false, true) => {
                    return_ident_opt = Some(*ident);
                }
                //other combinaison ==> Error.
                (_, _, _) => {
                    return Err(format!("Wrong attribute combinaison defined on field {}. Attribute input and/or output or result has to be added.", ident));
                }
            }
        }
    }

    // Return param is mandatory
    if let Some(_return_ident) = return_ident_opt {
        Ok(quote! {
            fn #call_ident(server: &Box<#iface_server_ident>, payload: &mut Vec<u8>, caller_id: u32) -> Result<Box<CowRpcParams>> {
                let mut payload = &payload[..];
                let mut param = #in_param_ident::read_from(&mut payload)?;
                let out_param = server.#call_ident(CowRpcCallContext::new(caller_id), param);

                Ok(Box::new(out_param))
            }
        })
    } else {
        Err("At least one attribute has to be defined for the return param (attribute result)!".into())
    }
}

fn gen_async_server_function(idents: &HashMap<u32, Ident>, input: &DeriveInput) -> Result<Tokens, String> {
    let call_ident = idents.get(&CALL_IDENT).unwrap();
    let in_param_ident = idents.get(&IN_PARAM_IDENT).unwrap();
    let iface_server_ident = idents.get(&IFACE_SERVER_IDENT).unwrap();

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

    let mut return_ident_opt = None;

    for f in named_fields.iter() {
        let attribute = validate_data_attributes(&f.attrs)?;
        if let Some(ref ident) = f.ident {
            match (attribute.input, attribute.output, attribute.result) {
                //input AND output or input or output
                (true, true, false) | (false, true, false) | (true, false, false) => {}
                //Return type
                (false, false, true) => {
                    return_ident_opt = Some(*ident);
                }
                //other combinaison ==> Error.
                (_, _, _) => {
                    return Err(format!("Wrong attribute combinaison defined on field {}. Attribute input and/or output or result has to be added.", ident));
                }
            }
        }
    }

    let return_type_token = quote!(CowFuture<Box<CowRpcParams>>);

    // Return param is mandatory
    if let Some(_return_ident) = return_ident_opt {
        Ok(quote! {
            fn #call_ident(server: &Box<#iface_server_ident>, payload: &mut Vec<u8>, caller_id: u32) -> #return_type_token {
                let mut payload = &payload[..];
                let fut: #return_type_token = match #in_param_ident::read_from(&mut payload) {
                    Ok(param) => {
                        Box::new(server.#call_ident(CowRpcCallContext::new(caller_id), param).map(|out_param| Box::new(out_param) as Box<CowRpcParams>).map_err(|_| CowRpcError::Internal("Futures returned an unexpected error".to_string())))
                    }
                    Err(e) => {
                        Box::new(err(e.into()))
                    }
                };

                fut
            }
        })
    } else {
        Err("At least one attribute has to be defined for the return param (attribute result)!".into())
    }
}

///
/// Generate the function called when the client wants to send a call to the server. It looks like this :
///
/// impl CowIfaceName {
///    pub fn callname ( peer : & CowRpcPeer , bind_context : Arc < CowRpcBindContext > , param1 : & u64 , param2 : & mut u64 , param3 : & String , param4 : & mut String , ) -> Result < u64 > {
///        let mut params = IfaceNameCallNameInParams { param1 : param1 . clone ( ) , param2 : param2 . clone ( ) , param3 : param3 . clone ( ) , param4 : param4 . clone ( ) , } ;
///        let output_param : IfaceNameCallNameOutParams = peer . call_sync ( bind_context , COW_IFACENAME_CALLNAME_ID , & mut params ) ? ;
///        * param2 = output_param . param2 ;
///        * param4 = output_param . param4 ;
///        Ok ( output_param . cow_return )
///    }
/// }
fn gen_client_function(idents: &HashMap<u32, Ident>, input: &DeriveInput) -> Result<Tokens, String> {
    let call_ident = idents.get(&CALL_IDENT).unwrap();
    let iface_client_ident = idents.get(&IFACE_CLIENT_IDENT).unwrap();
    let in_param_ident = idents.get(&IN_PARAM_IDENT).unwrap();
    let out_params_ident = idents.get(&OUT_PARAM_IDENT).unwrap();
    let call_id_ident = idents.get(&CALL_ID_IDENT).unwrap();

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

    let mut return_token_opt = None;
    let mut return_type_token_opt = None;

    for f in named_fields.iter() {
        let t = f.ty.clone();
        if let Some(ref ident) = f.ident {
            let attribute = validate_data_attributes(&f.attrs)?;
            match (attribute.input, attribute.output, attribute.result) {
                (true, true, false) | (true, false, false) | (false, true, false) => {}
                (false, false, true) => {
                    return_type_token_opt = Some(quote!(Result<#t>));
                    return_token_opt = Some(quote!(Ok(output_param.#ident)));
                }
                (_, _, _) => {
                    return Err(format!("Wrong attribute combinaison defined on field {}. Attribute input and/or output or result has to be added.", ident));
                }
            }
        }
    }

    let return_token = quote!(Result<#out_params_ident>);

    if let (Some(_return_token), Some(_return_type_token)) = (return_token_opt, return_type_token_opt) {
        Ok(quote! {
            impl #iface_client_ident {
                pub fn #call_ident(peer: &CowRpcPeer, bind_context: Arc<CowRpcBindContext>, params: #in_param_ident) -> #return_token {
                    let output_param: #out_params_ident = peer.call_sync(bind_context, #call_id_ident, &params)?;
                    Ok(output_param)
                }
            }
        })
    } else {
        Err("At least one attribute has to be defined for the return param (attribute result)!".into())
    }
}

fn gen_async_client_function(idents: &HashMap<u32, Ident>, input: &DeriveInput) -> Result<Tokens, String> {
    let call_ident = idents.get(&CALL_IDENT).unwrap();
    let iface_client_ident = idents.get(&IFACE_CLIENT_IDENT).unwrap();
    let in_param_ident = idents.get(&IN_PARAM_IDENT).unwrap();
    let out_params_ident = idents.get(&OUT_PARAM_IDENT).unwrap();
    let call_id_ident = idents.get(&CALL_ID_IDENT).unwrap();

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

    let mut return_token_opt = None;
    let mut return_type_token_opt = None;

    for f in named_fields.iter() {
        let t = f.ty.clone();
        if let Some(ref ident) = f.ident {
            let attribute = validate_data_attributes(&f.attrs)?;
            match (attribute.input, attribute.output, attribute.result) {
                (true, true, false) | (true, false, false) | (false, true, false) => {}
                (false, false, true) => {
                    return_type_token_opt = Some(quote!(Result<#t>));
                    return_token_opt = Some(quote!(Ok(output_param.#ident)));
                }
                (_, _, _) => {
                    return Err(format!("Wrong attribute combinaison defined on field {}. Attribute input and/or output or result has to be added.", ident));
                }
            }
        }
    }

    let return_token = quote!(CowFuture<#out_params_ident>);

    if let (Some(_return_token), Some(_return_type_token)) = (return_token_opt, return_type_token_opt) {
        Ok(quote! {
            impl #iface_client_ident {
                pub fn #call_ident(peer: &CowRpcPeerHandle, bind_context: Arc<CowRpcAsyncBindContext>, params: #in_param_ident) -> #return_token {
                    peer.call_async::<#in_param_ident, #out_params_ident>(bind_context, #call_id_ident, params)
                }
            }
        })
    } else {
        Err("At least one attribute has to be defined for the return param (attribute result)!".into())
    }
}

///
/// Generate structures and functions use to serialize/deserialize message pack message. It looks like this :
///
/// struct IfaceNameCallNameInParams {
///    param1 : u64 ,
///    param2 : u64 ,
///    param3 : String ,
///    param4 : String ,
/// }
/// impl cowrpc :: CowRpcParams for IfaceNameCallNameInParams {
///    fn read_from < R : Read > ( mut reader : & mut R ) -> Result < Self > where Self : Sized , {
///        let _ = rmp :: decode :: read_array_len ( & mut reader ) . map_err ( | _ | CowRpcError :: CowRpcFailure ( CowRpcErrorCode :: Payload ) ) ? ;
///        let param1 = rmp :: decode :: read_u64 ( & mut reader ) . map_err ( | _ | CowRpcError :: CowRpcFailure ( CowRpcErrorCode :: Payload ) ) ? ;
///        let param2 = rmp :: decode :: read_u64 ( & mut reader ) . map_err ( | _ | CowRpcError :: CowRpcFailure ( CowRpcErrorCode :: Payload ) ) ? ;
///        let param3 ; { let mut buf = [ 0 ; 64 ] ; param3 = String :: from ( rmp :: decode :: read_str ( & mut reader , & mut buf ) . map_err ( | _ | CowRpcError :: CowRpcFailure ( CowRpcErrorCode :: Payload ) ) ? ) ; }
///        let param4 ; { let mut buf = [ 0 ; 64 ] ; param4 = String :: from ( rmp :: decode :: read_str ( & mut reader , & mut buf ) . map_err ( | _ | CowRpcError :: CowRpcFailure ( CowRpcErrorCode :: Payload ) ) ? ) ; }
///        let params = IfaceNameCallNameInParams { param1 , param2 , param3 , param4 , } ;
///        Ok ( params )
///    }
///
///    fn write_to ( & self , mut writer : & mut Write ) -> Result < ( ) > {
///        let _ = rmp :: encode :: write_array_len ( & mut writer , 4u32 ) ;
///        let _ = rmp :: encode :: write_u64 ( & mut writer , self . param1 ) ;
///        let _ = rmp :: encode :: write_u64 ( & mut writer , self . param2 ) ;
///        let _ = rmp :: encode :: write_str ( & mut writer , & self . param3 ) ;
///        let _ = rmp :: encode :: write_str ( & mut writer , & self . param4 ) ; Ok ( ( ) )
///    }
/// }
///
fn gen_param_structs(idents: &HashMap<u32, Ident>, input: &DeriveInput) -> Result<Tokens, String> {
    let in_param_ident = idents.get(&IN_PARAM_IDENT).unwrap();
    let out_param_ident = idents.get(&OUT_PARAM_IDENT).unwrap();

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

    let mut input_field_tokens: Vec<Tokens> = Vec::new();
    let mut output_field_tokens: Vec<Tokens> = Vec::new();

    for f in named_fields.iter() {
        let t = f.ty.clone();
        let attribute = validate_data_attributes(&f.attrs)?;
        if attribute.input {
            if let Some(ref ident) = f.ident {
                input_field_tokens.push(quote!(pub #ident: #t,));
            }
        }
        if attribute.output || attribute.result {
            if let Some(ref ident) = f.ident {
                output_field_tokens.push(quote!(pub #ident: #t,));
            }
        }
    }

    let input_struct_token = quote! {
        pub struct #in_param_ident {
            #(#input_field_tokens)*
        }
    };

    let output_struct_token = quote! {
        pub struct #out_param_ident {
            #(#output_field_tokens)*
        }
    };

    let input_data: DeriveInput = syn::parse(input_struct_token.clone().into()).unwrap();
    let input_block_token = gen_cowrpcparam_impl_block(&input_data)?;

    let output_data: DeriveInput = syn::parse(output_struct_token.clone().into()).unwrap();
    let output_block_token = gen_cowrpcparam_impl_block(&output_data)?;

    Ok(quote! {
        #input_struct_token
        #input_block_token
        #output_struct_token
        #output_block_token
    })
}

///
/// Generate functions use to serialize/deserialize message pack message.
///
fn gen_cowrpcparam_impl_block(input: &DeriveInput) -> Result<Tokens, String> {
    let ident: &Ident = &input.ident;

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

    let mut field_tokens: Vec<Tokens> = Vec::new();
    let mut read_tokens: Vec<Tokens> = Vec::new();
    let mut write_tokens: Vec<Tokens> = Vec::new();

    let field_qty: u32 = named_fields.len() as u32;
    for f in named_fields.iter() {
        let t = f.ty.clone();
        if let Some(ref ident) = f.ident {
            field_tokens.push(quote!(#ident,));
            match t {
                ::syn::Type::Path(type_path) => {
                    if let Some(typ) = type_path.path.segments.iter().last() {
                        let typ_str = typ.ident.to_string();
                        match typ.ident.to_string().as_ref() {
                            "i16" | "u16" | "i32" | "u32" | "i64" | "u64" | "usize" | "isize" => {
                                let read_fct = Ident::new("read_int", ident.span());
                                let write_fct = Ident::new(&format!("write_{}", typ_str), ident.span());
                                read_tokens.push(quote! {let #ident = rmp::decode::#read_fct::<#typ, R>(&mut reader).map_err(|_| CowRpcError::CowRpcFailure(CowRpcErrorCode::Payload))?;});
                                write_tokens.push(quote! {let _ = rmp::encode::#write_fct(&mut writer, self.#ident);})
                            }
                            "i8" | "u8" | "f32" | "f64" | "bool" => {
                                let read_fct = Ident::new(&format!("read_{}", typ_str), ident.span());
                                let write_fct = Ident::new(&format!("write_{}", typ_str), ident.span());
                                read_tokens.push(quote! {let #ident = rmp::decode::#read_fct(&mut reader).map_err(|_| CowRpcError::CowRpcFailure(CowRpcErrorCode::Payload))?;});
                                write_tokens.push(quote! {let _ = rmp::encode::#write_fct(&mut writer, self.#ident);})
                            }
                            "String" => {
                                read_tokens.push(quote! {
                                    let #ident;
                                    {
                                        let len = rmp::decode::read_str_len(&mut reader).map_err(|_| CowRpcError::CowRpcFailure(CowRpcErrorCode::Payload))?;
                                        let vec = msgpack_decode_binary(&mut reader, len as usize)?;
                                        #ident = String::from_utf8(vec).map_err(|_| CowRpcError::CowRpcFailure(CowRpcErrorCode::Payload))?;
                                    }
                                });
                                write_tokens.push(quote! {let _ = rmp::encode::write_str(&mut writer, &self.#ident);})
                            }
                            "Vec" => {
                                read_tokens.push(quote! {
                                    let #ident;
                                    {
                                        let len = rmp::decode::read_bin_len(&mut reader).map_err(|_| CowRpcError::CowRpcFailure(CowRpcErrorCode::Payload))?;
                                        #ident = msgpack_decode_binary(&mut reader, len as usize)?;
                                    }

                                });
                                write_tokens.push(quote! {let _ = rmp::encode::write_bin(&mut writer, &self.#ident);})
                            }
                            x => {
                                return Err(format!("Type {} is not supported", x));
                            }
                        }
                    }
                }
                ::syn::Type::Tuple(ref type_tuple) if type_tuple.elems.is_empty() => {
                    read_tokens.push(quote! {
                        let #ident =
                            rmp::decode::read_nil(&mut reader).map_err(|_| CowRpcError::CowRpcFailure(CowRpcErrorCode::Payload))?;
                    });
                    write_tokens.push(quote!(let _ = rmp::encode::write_nil(&mut writer).expect("");))
                }
                _ => {
                    return Err("Invalid type in the cowrpc call structure".to_string());
                }
            }
        }
    }

    Ok(quote! {
        impl cowrpc::CowRpcParams for #ident {
            fn read_from<R: Read>(mut reader: &mut R) -> Result<Self>
            where
                Self: Sized,
            {
                let _ = rmp::decode::read_array_len(&mut reader).map_err(|_| CowRpcError::CowRpcFailure(CowRpcErrorCode::Payload))?;
                #(#read_tokens)*
                let params = #ident { #(#field_tokens)* };
                Ok(params)
            }

            fn write_to(&self, mut writer: &mut Write) -> Result<()> {
                let _ = rmp::encode::write_array_len(&mut writer, #field_qty);
                #(#write_tokens)*
                Ok(())
            }
        }
    })
}
