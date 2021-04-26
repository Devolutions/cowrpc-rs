//TODO FD REMOVE
//use mio_extras;
use mouscache;
use crate::proto;
use crate::proto::*;
use std;
use std::fmt;
use crate::transport;

#[derive(Debug)]
pub enum CowRpcError {
    Io(std::io::Error),
    TimeoutMpsc(std::sync::mpsc::RecvTimeoutError),
    Internal(String),
    Proto(String),
    CowRpcFailure(CowRpcErrorCode),
    Cancel,
    Timeout,
    Transport(transport::CowRpcTransportError),
}

impl fmt::Display for CowRpcError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CowRpcError::Io(ref error) => error.fmt(f),
            CowRpcError::TimeoutMpsc(ref error) => error.fmt(f),
            CowRpcError::Internal(ref desc) => write!(f, "Internal error: {}", desc),
            CowRpcError::Proto(ref desc) => write!(f, "Protocol error: {}", desc),
            CowRpcError::CowRpcFailure(ref error_code) => write!(f, "Flag failure: {}", error_code),
            CowRpcError::Cancel => write!(f, "Operation cancelled"),
            CowRpcError::Timeout => write!(f, "Operation timed out"),
            CowRpcError::Transport(ref error) => error.fmt(f),
        }
    }
}

impl std::error::Error for CowRpcError {
    fn description(&self) -> &str {
        match self {
            CowRpcError::Io(ref error) => error.description(),
            CowRpcError::TimeoutMpsc(ref error) => error.description(),
            CowRpcError::Internal(_) => "Internal error",
            CowRpcError::Proto(_) => "Protocol error",
            CowRpcError::CowRpcFailure(_) => "Flag failure",
            CowRpcError::Cancel => "Operation cancelled",
            CowRpcError::Timeout => "Operation timed out",
            CowRpcError::Transport(ref error) => error.description(),
        }
    }
}

impl From<transport::CowRpcTransportError> for CowRpcError {
    fn from(error: transport::CowRpcTransportError) -> CowRpcError {
        CowRpcError::Transport(error)
    }
}

impl From<std::io::Error> for CowRpcError {
    fn from(error: std::io::Error) -> CowRpcError {
        CowRpcError::Io(error)
    }
}

impl From<std::sync::mpsc::RecvTimeoutError> for CowRpcError {
    fn from(error: std::sync::mpsc::RecvTimeoutError) -> CowRpcError {
        CowRpcError::TimeoutMpsc(error)
    }
}

impl From<std::sync::mpsc::TryRecvError> for CowRpcError {
    fn from(error: std::sync::mpsc::TryRecvError) -> CowRpcError {
        CowRpcError::Internal(format!("TryRecvError: {}", error))
    }
}

// impl From<mio_extras::channel::SendError<proto::CowRpcMessage>> for CowRpcError {
//     fn from(error: mio_extras::channel::SendError<proto::CowRpcMessage>) -> Self {
//         CowRpcError::Internal(format!("mio_extras::channel::SendError: {}", error))
//     }
// }

impl From<mouscache::CacheError> for CowRpcError {
    fn from(e: mouscache::CacheError) -> Self {
        CowRpcError::Internal(format!("mouscache::CacheError : {}", e))
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum CowRpcErrorCode {
    Success,
    Internal,
    Version,
    Role,
    Arch,
    Capabilities,
    Unresolved,
    Unreachable,
    Transport,
    Memory,
    State,
    Proto,
    Header,
    Payload,
    Size,
    Type,
    Offset,
    Flags,
    Routing,
    SrcId,
    DstId,
    ReamId,
    CallId,
    IfaceId,
    ProcId,
    Argument,
    Timeout,
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    NotImplemented,
    NotBound,
    Unavailable,
    Cancelled,
    AlreadyBound,
    UnknownError, // Always the last one
}

impl Into<u16> for CowRpcErrorCode {
    fn into(self) -> u16 {
        let mut error_code: u16 = match self {
            CowRpcErrorCode::Success => 0,
            CowRpcErrorCode::Internal => 1,
            CowRpcErrorCode::Version => 2,
            CowRpcErrorCode::Role => 3,
            CowRpcErrorCode::Arch => 4,
            CowRpcErrorCode::Capabilities => 5,
            CowRpcErrorCode::Unresolved => 6,
            CowRpcErrorCode::Unreachable => 7,
            CowRpcErrorCode::Transport => 8,
            CowRpcErrorCode::Memory => 9,
            CowRpcErrorCode::State => 10,
            CowRpcErrorCode::Proto => 11,
            CowRpcErrorCode::Header => 12,
            CowRpcErrorCode::Payload => 13,
            CowRpcErrorCode::Size => 14,
            CowRpcErrorCode::Type => 15,
            CowRpcErrorCode::Offset => 16,
            CowRpcErrorCode::Flags => 17,
            CowRpcErrorCode::Routing => 18,
            CowRpcErrorCode::SrcId => 19,
            CowRpcErrorCode::DstId => 20,
            CowRpcErrorCode::ReamId => 21,
            CowRpcErrorCode::CallId => 22,
            CowRpcErrorCode::IfaceId => 23,
            CowRpcErrorCode::ProcId => 24,
            CowRpcErrorCode::Argument => 25,
            CowRpcErrorCode::Timeout => 26,
            CowRpcErrorCode::BadRequest => 27,
            CowRpcErrorCode::Unauthorized => 28,
            CowRpcErrorCode::Forbidden => 29,
            CowRpcErrorCode::NotFound => 30,
            CowRpcErrorCode::NotImplemented => 31,
            CowRpcErrorCode::NotBound => 32,
            CowRpcErrorCode::Unavailable => 33,
            CowRpcErrorCode::Cancelled => 34,
            CowRpcErrorCode::AlreadyBound => 35,
            CowRpcErrorCode::UnknownError => 0x00ff,
        };

        if error_code != 0 {
            error_code |= COW_RPC_FLAG_FAILURE;
        }

        error_code
    }
}

impl From<u8> for CowRpcErrorCode {
    fn from(error_code: u8) -> Self {
        match error_code {
            0 => CowRpcErrorCode::Success,
            1 => CowRpcErrorCode::Internal,
            2 => CowRpcErrorCode::Version,
            3 => CowRpcErrorCode::Role,
            4 => CowRpcErrorCode::Arch,
            5 => CowRpcErrorCode::Capabilities,
            6 => CowRpcErrorCode::Unresolved,
            7 => CowRpcErrorCode::Unreachable,
            8 => CowRpcErrorCode::Transport,
            9 => CowRpcErrorCode::Memory,
            10 => CowRpcErrorCode::State,
            11 => CowRpcErrorCode::Proto,
            12 => CowRpcErrorCode::Header,
            13 => CowRpcErrorCode::Payload,
            14 => CowRpcErrorCode::Size,
            15 => CowRpcErrorCode::Type,
            16 => CowRpcErrorCode::Offset,
            17 => CowRpcErrorCode::Flags,
            18 => CowRpcErrorCode::Routing,
            19 => CowRpcErrorCode::SrcId,
            20 => CowRpcErrorCode::DstId,
            21 => CowRpcErrorCode::ReamId,
            22 => CowRpcErrorCode::CallId,
            23 => CowRpcErrorCode::IfaceId,
            24 => CowRpcErrorCode::ProcId,
            25 => CowRpcErrorCode::Argument,
            26 => CowRpcErrorCode::Timeout,
            27 => CowRpcErrorCode::BadRequest,
            28 => CowRpcErrorCode::Unauthorized,
            29 => CowRpcErrorCode::Forbidden,
            30 => CowRpcErrorCode::NotFound,
            31 => CowRpcErrorCode::NotImplemented,
            32 => CowRpcErrorCode::NotBound,
            33 => CowRpcErrorCode::Unavailable,
            34 => CowRpcErrorCode::Cancelled,
            35 => CowRpcErrorCode::AlreadyBound,
            _ => CowRpcErrorCode::UnknownError,
        }
    }
}
impl From<u16> for CowRpcErrorCode {
    fn from(flag: u16) -> Self {
        let is_error = flag & COW_RPC_FLAG_FAILURE != 0;
        if !is_error {
            CowRpcErrorCode::Success
        } else {
            let error_code = (flag & COW_RPC_FLAG_MASK_ERROR) as u8;
            if error_code == 0 {
                CowRpcErrorCode::UnknownError
            } else {
                CowRpcErrorCode::from(error_code)
            }
        }
    }
}

impl fmt::Display for CowRpcErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
        match *self {
            CowRpcErrorCode::Success => write!(f, "Success"),
            CowRpcErrorCode::Internal => write!(f, "Internal"),
            CowRpcErrorCode::Version => write!(f, "Version"),
            CowRpcErrorCode::Role => write!(f, "Role"),
            CowRpcErrorCode::Arch => write!(f, "Arch"),
            CowRpcErrorCode::Capabilities => write!(f, "Capabilities"),
            CowRpcErrorCode::Unresolved => write!(f, "Unresolved"),
            CowRpcErrorCode::Unreachable => write!(f, "Unreachable"),
            CowRpcErrorCode::Transport => write!(f, "Transport"),
            CowRpcErrorCode::Memory => write!(f, "Memory"),
            CowRpcErrorCode::State => write!(f, "State"),
            CowRpcErrorCode::Proto => write!(f, "Proto"),
            CowRpcErrorCode::Header => write!(f, "Header"),
            CowRpcErrorCode::Payload => write!(f, "Payload"),
            CowRpcErrorCode::Size => write!(f, "Size"),
            CowRpcErrorCode::Type => write!(f, "Type"),
            CowRpcErrorCode::Offset => write!(f, "Offset"),
            CowRpcErrorCode::Flags => write!(f, "Flags"),
            CowRpcErrorCode::Routing => write!(f, "Routing"),
            CowRpcErrorCode::SrcId => write!(f, "SrcId"),
            CowRpcErrorCode::DstId => write!(f, "DstId"),
            CowRpcErrorCode::ReamId => write!(f, "ReamId"),
            CowRpcErrorCode::CallId => write!(f, "CallId"),
            CowRpcErrorCode::IfaceId => write!(f, "IfaceId"),
            CowRpcErrorCode::ProcId => write!(f, "ProcId"),
            CowRpcErrorCode::Argument => write!(f, "Argument"),
            CowRpcErrorCode::Timeout => write!(f, "Timeout"),
            CowRpcErrorCode::BadRequest => write!(f, "BadRequest"),
            CowRpcErrorCode::Unauthorized => write!(f, "Unauthorized"),
            CowRpcErrorCode::Forbidden => write!(f, "Forbidden"),
            CowRpcErrorCode::NotFound => write!(f, "NotFound"),
            CowRpcErrorCode::NotImplemented => write!(f, "NotImplemented"),
            CowRpcErrorCode::NotBound => write!(f, "NotBound"),
            CowRpcErrorCode::Unavailable => write!(f, "Unavailable"),
            CowRpcErrorCode::Cancelled => write!(f, "Cancelled"),
            CowRpcErrorCode::AlreadyBound => write!(f, "AlreadyBound"),
            CowRpcErrorCode::UnknownError => write!(f, "UnknownError"),
        }
    }
}

pub type Result<T> = std::result::Result<T, CowRpcError>;
