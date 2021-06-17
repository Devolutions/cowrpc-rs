#[macro_use]
extern crate mouscache_derive;

pub use crate::proto::{CowRpcMessage, Message};
pub use crate::transport::{CowFuture, CowRpcMessageInterceptor, MessageInjector as CowRpcMessageInjector};

use crate::error::{CowRpcError, Result};

// Re-export mouscache
pub use mouscache;

pub mod error;
pub mod peer;
pub mod router;

mod proto;

mod transport;
pub use self::transport::TlsOptions;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CowRpcMode {
    Routed,
    Direct,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CowRpcIdentityType {
    /// Anonymous identity
    None,
    /// User Principal Name
    Upn,
    /// Server Principal Name
    Spn,
}

impl std::string::ToString for CowRpcIdentityType {
    fn to_string(&self) -> String {
        use crate::CowRpcIdentityType::*;

        match *self {
            None => "NONE".to_string(),
            Upn => "UPN".to_string(),
            Spn => "SPN".to_string(),
        }
    }
}

impl std::str::FromStr for CowRpcIdentityType {
    type Err = CowRpcError;

    fn from_str(s: &str) -> Result<Self> {
        use crate::CowRpcIdentityType::*;

        Ok(match s {
            "UPN" => Upn,
            "SPN" => Spn,
            _ => None,
        })
    }
}

impl CowRpcIdentityType {
    pub fn try_from(typ: u8) -> Result<Self> {
        match typ {
            0 => Ok(CowRpcIdentityType::None),
            1 => Ok(CowRpcIdentityType::Upn),
            2 => Ok(CowRpcIdentityType::Spn),
            _ => Err(error::CowRpcError::Proto(format!("Unknown identity type - ({})", typ))),
        }
    }

    pub fn into(&self) -> u8 {
        match *self {
            CowRpcIdentityType::None => 0,
            CowRpcIdentityType::Upn => 1,
            CowRpcIdentityType::Spn => 2,
        }
    }
}

/// Provides information about the remote peer when processing a call.
pub struct CowRpcCallContext {
    caller_id: u32,
}

impl CowRpcCallContext {
    pub fn new(caller_id: u32) -> Self {
        CowRpcCallContext { caller_id }
    }

    /// Returns the id of peer sending the call.
    pub fn get_caller_id(&self) -> u32 {
        self.caller_id
    }
}
