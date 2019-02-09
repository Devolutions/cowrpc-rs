use syn;
use syn::Attribute;
use syn::Meta::{List, NameValue, Word};
use syn::NestedMeta::{Literal, Meta};

const ATTRIBUTE_NAME: &'static str = "cowrpc";
const ASYNC_ATTRIBUTE_NAME: &'static str = "async";

pub struct DataAttribute {
    pub r#async: bool,
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
    let mut attribs = DataAttribute { r#async: false };

    for meta_items in attrs.iter().filter_map(get_meta_items) {
        for meta in meta_items {
            match meta {
                Meta(Word(name)) if name == ASYNC_ATTRIBUTE_NAME => attribs.r#async = true,
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
