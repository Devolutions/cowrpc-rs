use std::str::FromStr;
use syn;
use syn::Attribute;
use syn::Meta::{List, NameValue, Word};
use syn::NestedMeta::{Literal, Meta};

const ATTRIBUTE_NAME: &'static str = "cowrpc";
const INPUT_ATTRIBUTE_NAME: &'static str = "input";
const OUTPUT_ATTRIBUTE_NAME: &'static str = "output";
const ASYNC_ATTRIBUTE_NAME: &'static str = "async";
const IFACE_NAME_ATTRIBUTE_NAME: &'static str = "iface_name";
const RESULT_ATTRIBUTE_NAME: &'static str = "result";

pub struct DataAttribute {
    pub iface_name: Option<String>,
    pub input: bool,
    pub output: bool,
    pub result: bool,
    pub async: bool,
}

pub fn value_from_lit<T: FromStr>(lit: &syn::Lit, attr_name: &str) -> Result<T, String> {
    if let syn::Lit::Str(ref lit) = *lit {
        let value =
            T::from_str(&lit.value()).map_err(move |_| format!("Unable to parse attribute value for {}", attr_name));
        return value;
    } else {
        Err("Unable to parse attribute value".to_string())
    }
}

pub fn get_meta_items(attr: &syn::Attribute) -> Option<Vec<syn::NestedMeta>> {
    if attr.path.segments.len() == 1 && attr.path.segments[0].ident == ATTRIBUTE_NAME {
        match attr.interpret_meta() {
            Some(List(ref meta)) => Some(meta.nested.iter().cloned().collect()),
            _ => None,
        }
    } else {
        None
    }
}

pub fn validate_data_attributes(attrs: &Vec<Attribute>) -> Result<DataAttribute, String> {
    let mut attribs = DataAttribute {
        iface_name: None,
        input: false,
        output: false,
        result: false,
        async: false,
    };

    for meta_items in attrs.iter().filter_map(get_meta_items) {
        for meta in meta_items {
            match meta {
                Meta(Word(name)) if name == INPUT_ATTRIBUTE_NAME => attribs.input = true,
                Meta(Word(name)) if name == OUTPUT_ATTRIBUTE_NAME => attribs.output = true,
                Meta(Word(name)) if name == RESULT_ATTRIBUTE_NAME => attribs.result = true,
                Meta(Word(name)) if name == ASYNC_ATTRIBUTE_NAME => attribs.async = true,
                Meta(NameValue(ref m)) if m.ident == IFACE_NAME_ATTRIBUTE_NAME => {
                    let name: String = value_from_lit(&m.lit, IFACE_NAME_ATTRIBUTE_NAME)?;
                    attribs.iface_name = Some(name);
                }
                Meta(NameValue(_)) => {
                    return Err("There is no named value attribute you can use on data types with cowrpc".to_string())
                }
                Meta(List(ref _m)) => {
                    return Err("There is no list attribute you can use on data types with cowrpc".to_string())
                }
                Meta(Word(_)) => {
                    return Err("There is no word attribute you can use on data types with cowrpc".to_string())
                }
                Literal(_) => {
                    return Err("There is no litteral attribute you can use on data types with cowrpc".to_string())
                }
            }
        }
    }

    Ok(attribs)
}
