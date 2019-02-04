use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use error::{CowRpcError, Result};
use router::CowRpcIdentity;
use std;
use std::io::{Read, Write};
use CowRpcIdentityType;

pub const _COW_RPC_ERROR_MSG_ID: u8 = 0;
pub const COW_RPC_HANDSHAKE_MSG_ID: u8 = 1;
pub const COW_RPC_TERMINATE_MSG_ID: u8 = 9;
pub const COW_RPC_REGISTER_MSG_ID: u8 = 11;
pub const COW_RPC_IDENTIFY_MSG_ID: u8 = 13;
pub const COW_RPC_VERIFY_MSG_ID: u8 = 14;
pub const COW_RPC_RESOLVE_MSG_ID: u8 = 15;
pub const COW_RPC_BIND_MSG_ID: u8 = 16;
pub const COW_RPC_UNBIND_MSG_ID: u8 = 17;
pub const COW_RPC_CALL_MSG_ID: u8 = 21;
pub const COW_RPC_RESULT_MSG_ID: u8 = 22;
pub const COW_RPC_HTTP_MSG_ID: u8 = 23;

pub const COW_RPC_FLAG_DIRECT: u16 = 0x0100;
pub const COW_RPC_FLAG_SERVER: u16 = 0x1000;
pub const _COW_RPC_FLAG_RESERVED: u16 = 0x2000;
pub const COW_RPC_FLAG_RESPONSE: u16 = 0x4000;
pub const COW_RPC_FLAG_FAILURE: u16 = 0x8000;
pub const _COW_RPC_FLAG_SUCCESS: u16 = 0x0000;
pub const COW_RPC_FLAG_MASK_ERROR: u16 = 0x00FF;
pub const COW_RPC_FLAG_REVERSE: u16 = 0x0100;
pub const COW_RPC_FLAG_FINAL: u16 = 0x0200;

pub const COW_RPC_ARCH_FLAG_64_BIT: u16 = 0x0001;

pub const COW_RPC_DEF_FLAG_NAMED: u16 = 0x1000;
pub const COW_RPC_DEF_FLAG_EMPTY: u16 = 0x2000;

pub trait Message {
    fn read_from<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized;
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()>;
    fn get_size(&self) -> u32;
}

impl Message for String {
    fn read_from<R: Read>(reader: &mut R) -> Result<String> {
        let length = reader.read_u8()?;
        let mut string = vec![0; length as usize];
        reader.read_exact(&mut string)?;
        reader.read_u8()?; // null terminator
        Ok(string.iter().map(|c| *c as char).collect())
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        let length = self.len() as u8;
        writer.write_u8(length)?;
        writer.write_all(&self.chars().map(|c| c as u8).collect::<Vec<u8>>())?;
        writer.write_u8(0u8)?;
        Ok(())
    }

    fn get_size(&self) -> u32 {
        // u8 for the length + the vector itself + null terminator
        (1 + self.len() + 1) as u32
    }
}

#[derive(Debug, Clone)]
pub enum CowRpcMessage {
    Handshake(CowRpcHdr, CowRpcHandshakeMsg),
    Register(CowRpcHdr, CowRpcRegisterMsg),
    Identity(CowRpcHdr, CowRpcIdentityMsg),
    Resolve(CowRpcHdr, CowRpcResolveMsg),
    Bind(CowRpcHdr, CowRpcBindMsg),
    Unbind(CowRpcHdr, CowRpcUnbindMsg),
    Call(CowRpcHdr, CowRpcCallMsg, Vec<u8>),
    Result(CowRpcHdr, CowRpcResultMsg, Vec<u8>),
    Verify(CowRpcHdr, CowRpcVerifyMsg, Vec<u8>),
    Http(CowRpcHdr, CowRpcHttpMsg, Vec<u8>),
    Terminate(CowRpcHdr),
}

impl CowRpcMessage {
    pub fn is_response(&self) -> bool {
        match self {
            CowRpcMessage::Handshake(ref header, _) => header.is_response(),
            CowRpcMessage::Register(ref header, _) => header.is_response(),
            CowRpcMessage::Identity(ref header, _) => header.is_response(),
            CowRpcMessage::Resolve(ref header, _) => header.is_response(),
            CowRpcMessage::Bind(ref header, _) => header.is_response(),
            CowRpcMessage::Unbind(ref header, _) => header.is_response(),
            CowRpcMessage::Call(ref header, _, _) => header.is_response(),
            CowRpcMessage::Result(ref header, _, _) => header.is_response(),
            CowRpcMessage::Terminate(ref header) => header.is_response(),
            CowRpcMessage::Verify(ref header, _, _) => header.is_response(),
            CowRpcMessage::Http(ref header, _, _) => header.is_response(),
        }
    }

    pub fn is_unbind(&self) -> bool {
        match self {
            CowRpcMessage::Unbind(_, _) => true,
            _ => false,
        }
    }

    pub fn is_handshake(&self) -> bool {
        match self {
            CowRpcMessage::Handshake(_, _) => true,
            _ => false,
        }
    }

    pub fn is_register(&self) -> bool {
        match self {
            CowRpcMessage::Register(_, _) => true,
            _ => false,
        }
    }

    pub fn get_msg_info(&self) -> String {
        match self {
            CowRpcMessage::Handshake(ref header, _) => format!(
                "{} {}",
                header.get_msg_name(),
                if header.is_response() { "Response" } else { "Request" }
            ),
            CowRpcMessage::Register(ref header, _) => format!(
                "{} {}",
                header.get_msg_name(),
                if header.is_response() { "Response" } else { "Request" }
            ),
            CowRpcMessage::Identity(ref header, ref i) => format!(
                "{} {} : Identity {}",
                header.get_msg_name(),
                if header.is_response() { "Response" } else { "Request" },
                i.identity
            ),
            CowRpcMessage::Resolve(ref header, _) => format!(
                "{} {}",
                header.get_msg_name(),
                if header.is_response() { "Response" } else { "Request" }
            ),
            CowRpcMessage::Bind(ref header, _) => format!(
                "{} {}",
                header.get_msg_name(),
                if header.is_response() {
                    format!("Response | {} -> {}", header.dst_id, header.src_id)
                } else {
                    format!("Request | {} -> {}", header.src_id, header.dst_id)
                }
            ),
            CowRpcMessage::Unbind(ref header, _) => format!(
                "{} {}",
                header.get_msg_name(),
                if header.is_response() {
                    format!("Response | {} -> {}", header.dst_id, header.src_id)
                } else {
                    format!("Request | {} -> {}", header.src_id, header.dst_id)
                }
            ),
            CowRpcMessage::Call(ref header, ref c, _) => format!(
                "{} iface {} proc {} | {} -> {}",
                header.get_msg_name(),
                c.iface_id,
                c.proc_id,
                header.src_id,
                header.dst_id
            ),
            CowRpcMessage::Result(ref header, ref r, _) => format!(
                "{} iface {} proc {} | {} -> {}",
                header.get_msg_name(),
                r.iface_id,
                r.proc_id,
                header.src_id,
                header.dst_id
            ),
            CowRpcMessage::Terminate(ref header) => format!(
                "{} {}",
                header.get_msg_name(),
                if header.is_response() { "Response" } else { "Request" }
            ),
            CowRpcMessage::Verify(ref header, _, _) => format!(
                "{} {}",
                header.get_msg_name(),
                if header.is_response() { "Response" } else { "Request" }
            ),
            CowRpcMessage::Http(ref header, _, _) => format!(
                "{} {}",
                header.get_msg_name(),
                if header.is_response() { "Response" } else { "Request" }
            ),
        }
    }

    pub fn get_msg_name(&self) -> &str {
        match self {
            CowRpcMessage::Handshake(ref header, _) => header.get_msg_name(),
            CowRpcMessage::Register(ref header, _) => header.get_msg_name(),
            CowRpcMessage::Identity(ref header, _) => header.get_msg_name(),
            CowRpcMessage::Resolve(ref header, _) => header.get_msg_name(),
            CowRpcMessage::Bind(ref header, _) => header.get_msg_name(),
            CowRpcMessage::Unbind(ref header, _) => header.get_msg_name(),
            CowRpcMessage::Call(ref header, _, _) => header.get_msg_name(),
            CowRpcMessage::Result(ref header, _, _) => header.get_msg_name(),
            CowRpcMessage::Terminate(ref header) => header.get_msg_name(),
            CowRpcMessage::Verify(ref header, _, _) => header.get_msg_name(),
            CowRpcMessage::Http(ref header, _, _) => header.get_msg_name(),
        }
    }

    pub fn is_handled_by_router(&self) -> bool {
        match self {
            CowRpcMessage::Handshake(_, _) => true,
            CowRpcMessage::Register(_, _) => true,
            CowRpcMessage::Identity(_, _) => true,
            CowRpcMessage::Resolve(_, _) => true,
            CowRpcMessage::Bind(_, _) => false,
            CowRpcMessage::Unbind(_, _) => false,
            CowRpcMessage::Call(_, _, _) => false,
            CowRpcMessage::Result(_, _, _) => false,
            CowRpcMessage::Terminate(_) => true,
            CowRpcMessage::Verify(_, _, _) => true,
            CowRpcMessage::Http(_, _, _) => false,
        }
    }

    pub fn get_src_id(&self) -> u32 {
        match self {
            CowRpcMessage::Handshake(ref header, _) => header.src_id,
            CowRpcMessage::Register(ref header, _) => header.src_id,
            CowRpcMessage::Identity(ref header, _) => header.src_id,
            CowRpcMessage::Resolve(ref header, _) => header.src_id,
            CowRpcMessage::Bind(ref header, _) => header.src_id,
            CowRpcMessage::Unbind(ref header, _) => header.src_id,
            CowRpcMessage::Call(ref header, _, _) => header.src_id,
            CowRpcMessage::Result(ref header, _, _) => header.src_id,
            CowRpcMessage::Terminate(ref header) => header.src_id,
            CowRpcMessage::Verify(ref header, _, _) => header.src_id,
            CowRpcMessage::Http(ref header, _, _) => header.src_id,
        }
    }

    pub fn get_dst_id(&self) -> u32 {
        match self {
            CowRpcMessage::Handshake(ref header, _) => header.dst_id,
            CowRpcMessage::Register(ref header, _) => header.dst_id,
            CowRpcMessage::Identity(ref header, _) => header.dst_id,
            CowRpcMessage::Resolve(ref header, _) => header.dst_id,
            CowRpcMessage::Bind(ref header, _) => header.dst_id,
            CowRpcMessage::Unbind(ref header, _) => header.dst_id,
            CowRpcMessage::Call(ref header, _, _) => header.dst_id,
            CowRpcMessage::Result(ref header, _, _) => header.dst_id,
            CowRpcMessage::Terminate(ref header) => header.dst_id,
            CowRpcMessage::Verify(ref header, _, _) => header.dst_id,
            CowRpcMessage::Http(ref header, _, _) => header.dst_id,
        }
    }

    pub fn swap_src_dst(&mut self) {
        match self {
            CowRpcMessage::Handshake(ref mut header, _) => header.swap_src_dst(),
            CowRpcMessage::Register(ref mut header, _) => header.swap_src_dst(),
            CowRpcMessage::Identity(ref mut header, _) => header.swap_src_dst(),
            CowRpcMessage::Resolve(ref mut header, _) => header.swap_src_dst(),
            CowRpcMessage::Bind(ref mut header, _) => header.swap_src_dst(),
            CowRpcMessage::Unbind(ref mut header, _) => header.swap_src_dst(),
            CowRpcMessage::Call(ref mut header, _, _) => header.swap_src_dst(),
            CowRpcMessage::Result(ref mut header, _, _) => header.swap_src_dst(),
            CowRpcMessage::Terminate(ref mut header) => header.swap_src_dst(),
            CowRpcMessage::Verify(ref mut header, _, _) => header.swap_src_dst(),
            CowRpcMessage::Http(ref mut header, _, _) => header.swap_src_dst(),
        }
    }

    pub fn add_flag(&mut self, flag: u16) {
        match self {
            CowRpcMessage::Handshake(ref mut header, _) => header.add_flag(flag),
            CowRpcMessage::Register(ref mut header, _) => header.add_flag(flag),
            CowRpcMessage::Identity(ref mut header, _) => header.add_flag(flag),
            CowRpcMessage::Resolve(ref mut header, _) => header.add_flag(flag),
            CowRpcMessage::Bind(ref mut header, _) => header.add_flag(flag),
            CowRpcMessage::Unbind(ref mut header, _) => header.add_flag(flag),
            CowRpcMessage::Call(ref mut header, _, _) => header.add_flag(flag),
            CowRpcMessage::Result(ref mut header, _, _) => header.add_flag(flag),
            CowRpcMessage::Terminate(ref mut header) => header.add_flag(flag),
            CowRpcMessage::Verify(ref mut header, _, _) => header.add_flag(flag),
            CowRpcMessage::Http(ref mut header, _, _) => header.add_flag(flag),
        }
    }
}

impl Message for CowRpcMessage {
    fn read_from<R: Read>(mut reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let header = CowRpcHdr::read_from(&mut reader)?;
        match header.msg_type {
            COW_RPC_HANDSHAKE_MSG_ID => {
                let msg = CowRpcHandshakeMsg::read_from(&mut reader)?;
                Ok(CowRpcMessage::Handshake(header, msg))
            }
            COW_RPC_REGISTER_MSG_ID => {
                let msg = CowRpcRegisterMsg::read_from(&mut reader)?;
                Ok(CowRpcMessage::Register(header, msg))
            }
            COW_RPC_IDENTIFY_MSG_ID => {
                let msg = CowRpcIdentityMsg::read_from(&mut reader)?;
                Ok(CowRpcMessage::Identity(header, msg))
            }
            COW_RPC_RESOLVE_MSG_ID => {
                let msg = CowRpcResolveMsg::read_from(&mut reader, header.flags)?;
                Ok(CowRpcMessage::Resolve(header, msg))
            }
            COW_RPC_BIND_MSG_ID => {
                let msg = CowRpcBindMsg::read_from(&mut reader)?;
                Ok(CowRpcMessage::Bind(header, msg))
            }
            COW_RPC_UNBIND_MSG_ID => {
                let msg = CowRpcUnbindMsg::read_from(&mut reader)?;
                Ok(CowRpcMessage::Unbind(header, msg))
            }
            COW_RPC_CALL_MSG_ID => {
                let msg = CowRpcCallMsg::read_from(&mut reader)?;

                let mut v = vec![0u8; (header.size - u32::from(header.offset)) as usize];
                reader.read_exact(&mut v)?;
                Ok(CowRpcMessage::Call(header, msg, v))
            }
            COW_RPC_RESULT_MSG_ID => {
                let msg = CowRpcResultMsg::read_from(&mut reader)?;

                let mut v = vec![0u8; (header.size - u32::from(header.offset)) as usize];
                reader.read_exact(&mut v)?;
                Ok(CowRpcMessage::Result(header, msg, v))
            }
            COW_RPC_TERMINATE_MSG_ID => {
                Ok(CowRpcMessage::Terminate(header))
            }
            COW_RPC_VERIFY_MSG_ID => {
                let msg = CowRpcVerifyMsg::read_from(&mut reader)?;
                let mut v = vec![0u8; (header.size - u32::from(header.offset)) as usize];
                reader.read_exact(&mut v)?;
                Ok(CowRpcMessage::Verify(header, msg, v))
            }
            COW_RPC_HTTP_MSG_ID => {
                let msg = CowRpcHttpMsg::read_from(&mut reader)?;
                let mut v = vec![0u8; (header.size - u32::from(header.offset)) as usize];
                reader.read_exact(&mut v)?;
                Ok(CowRpcMessage::Http(header, msg, v))
            }

            _ => Err(CowRpcError::Internal(format!(
                "Msg type unknown - ({})",
                header.msg_type
            ))),
        }
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        match self {
            CowRpcMessage::Handshake(ref hdr, ref msg) => {
                hdr.write_to(writer)?;
                msg.write_to(writer)?;
                Ok(())
            }
            CowRpcMessage::Register(ref hdr, ref msg) => {
                hdr.write_to(writer)?;
                msg.write_to(writer)?;
                Ok(())
            }
            CowRpcMessage::Identity(ref hdr, ref msg) => {
                hdr.write_to(writer)?;
                msg.write_to(writer)?;
                Ok(())
            }
            CowRpcMessage::Resolve(ref hdr, ref msg) => {
                hdr.write_to(writer)?;
                msg.write_to(writer, hdr.flags)?;
                Ok(())
            }
            CowRpcMessage::Bind(ref hdr, ref msg) => {
                hdr.write_to(writer)?;
                msg.write_to(writer)?;
                Ok(())
            }
            CowRpcMessage::Unbind(ref hdr, ref msg) => {
                hdr.write_to(writer)?;
                msg.write_to(writer)?;
                Ok(())
            }
            CowRpcMessage::Call(ref hdr, ref msg, ref payload) => {
                hdr.write_to(writer)?;
                msg.write_to(writer)?;
                writer.write_all(&payload)?;
                Ok(())
            }
            CowRpcMessage::Result(ref hdr, ref msg, ref payload) => {
                hdr.write_to(writer)?;
                msg.write_to(writer)?;
                writer.write_all(&payload)?;
                Ok(())
            }
            CowRpcMessage::Terminate(ref hdr) => {
                hdr.write_to(writer)?;
                Ok(())
            }
            CowRpcMessage::Verify(ref hdr, msg, ref payload) => {
                hdr.write_to(writer)?;
                msg.write_to(writer)?;
                writer.write_all(&payload)?;
                Ok(())
            }
            CowRpcMessage::Http(ref hdr, msg, ref payload) => {
                hdr.write_to(writer)?;
                msg.write_to(writer)?;
                writer.write_all(&payload)?;
                Ok(())
            }
        }
    }

    fn get_size(&self) -> u32 {
        unimplemented!()
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct CowRpcHdr {
    pub size: u32,
    pub msg_type: u8,
    pub offset: u8,
    pub flags: u16,
    pub src_id: u32,
    pub dst_id: u32,
}

impl Default for CowRpcHdr {
    fn default() -> Self {
        CowRpcHdr {
            size: 0,
            msg_type: 0,
            offset: 0,
            flags: 0,
            src_id: 0,
            dst_id: 0,
        }
    }
}

impl CowRpcHdr {
    pub fn is_response(&self) -> bool {
        (self.flags & COW_RPC_FLAG_RESPONSE) != 0
    }
    pub fn is_failure(&self) -> bool {
        (self.flags & COW_RPC_FLAG_FAILURE) != 0
    }

    pub fn from_server(&self) -> bool {
        (self.flags & COW_RPC_FLAG_SERVER) != 0
    }

    pub fn is_reverse(&self) -> bool {
        (self.flags & COW_RPC_FLAG_REVERSE) != 0
    }

    fn get_msg_name(&self) -> &str {
        match self.msg_type {
            COW_RPC_HANDSHAKE_MSG_ID => "Handshake",
            COW_RPC_REGISTER_MSG_ID => "Register",
            COW_RPC_RESOLVE_MSG_ID => "Resolve",
            COW_RPC_IDENTIFY_MSG_ID => "Identify",
            COW_RPC_BIND_MSG_ID => "Bind",
            COW_RPC_UNBIND_MSG_ID => "Unbind",
            COW_RPC_CALL_MSG_ID => "Call",
            COW_RPC_RESULT_MSG_ID => "Result",
            COW_RPC_TERMINATE_MSG_ID => "Terminate",
            COW_RPC_VERIFY_MSG_ID => "Verify",
            COW_RPC_HTTP_MSG_ID => "Http",
            _ => "Unknown",
        }
    }

    fn swap_src_dst(&mut self) {
        std::mem::swap(&mut self.dst_id, &mut self.src_id);
    }

    fn add_flag(&mut self, flag: u16) {
        self.flags |= flag;
    }
}

impl Message for CowRpcHdr {
    fn read_from<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let hdr = CowRpcHdr {
            size: reader.read_u32::<LittleEndian>()?,
            msg_type: reader.read_u8()?,
            offset: reader.read_u8()?,
            flags: reader.read_u16::<LittleEndian>()?,
            src_id: reader.read_u32::<LittleEndian>()?,
            dst_id: reader.read_u32::<LittleEndian>()?,
        };
        Ok(hdr)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_u32::<LittleEndian>(self.size)?;
        writer.write_u8(self.msg_type)?;
        writer.write_u8(self.offset)?;
        writer.write_u16::<LittleEndian>(self.flags)?;
        writer.write_u32::<LittleEndian>(self.src_id)?;
        writer.write_u32::<LittleEndian>(self.dst_id)?;
        Ok(())
    }

    fn get_size(&self) -> u32 {
        16u32
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct CowRpcHandshakeMsg {
    pub version_major: u8,
    pub version_minor: u8,
    pub arch_flags: u16,
    pub role_flags: u16,
    pub capabilities: u16,
    pub reserved: u32,
}

impl Default for CowRpcHandshakeMsg {
    fn default() -> Self {
        CowRpcHandshakeMsg {
            version_major: 1,
            version_minor: 0,
            arch_flags: if std::mem::size_of::<usize>() == 8 {
                COW_RPC_ARCH_FLAG_64_BIT
            } else {
                0
            },
            role_flags: 0,
            capabilities: 0,
            reserved: 0,
        }
    }
}

impl Message for CowRpcHandshakeMsg {
    fn read_from<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let msg = CowRpcHandshakeMsg {
            version_major: reader.read_u8()?,
            version_minor: reader.read_u8()?,
            arch_flags: reader.read_u16::<LittleEndian>()?,
            role_flags: reader.read_u16::<LittleEndian>()?,
            capabilities: reader.read_u16::<LittleEndian>()?,
            reserved: reader.read_u32::<LittleEndian>()?,
        };
        Ok(msg)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_u8(self.version_major)?;
        writer.write_u8(self.version_minor)?;
        writer.write_u16::<LittleEndian>(self.arch_flags)?;
        writer.write_u16::<LittleEndian>(self.role_flags)?;
        writer.write_u16::<LittleEndian>(self.capabilities)?;
        writer.write_u32::<LittleEndian>(self.reserved)?;
        Ok(())
    }
    fn get_size(&self) -> u32 {
        12u32
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CowRpcRegisterMsg {
    pub ifaces: Vec<CowRpcIfaceDef>,
}

impl Message for CowRpcRegisterMsg {
    fn read_from<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let msg = CowRpcRegisterMsg {
            ifaces: Vec::<CowRpcIfaceDef>::read_from(reader)?,
        };

        Ok(msg)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        self.ifaces.write_to(writer)
    }

    fn get_size(&self) -> u32 {
        self.ifaces.get_size()
    }
}

impl Message for Vec<CowRpcIfaceDef> {
    fn read_from<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let len = reader.read_u16::<LittleEndian>()?;
        let mut v = Vec::with_capacity(len as usize);

        for _ in 0..len {
            v.push(CowRpcIfaceDef::read_from(reader)?);
        }

        Ok(v)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_u16::<LittleEndian>(self.len() as u16)?;

        for iface in self {
            iface.write_to(writer)?;
        }
        Ok(())
    }

    fn get_size(&self) -> u32 {
        let mut size = 2u32;

        for msg in self {
            size += msg.get_size();
        }

        size
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CowRpcBindMsg {
    pub ifaces: Vec<CowRpcIfaceDef>,
}

impl Message for CowRpcBindMsg {
    fn read_from<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let msg = CowRpcBindMsg {
            ifaces: Vec::<CowRpcIfaceDef>::read_from(reader)?,
        };

        Ok(msg)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        self.ifaces.write_to(writer)
    }

    fn get_size(&self) -> u32 {
        self.ifaces.get_size()
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CowRpcUnbindMsg {
    pub ifaces: Vec<CowRpcIfaceDef>,
}

impl Message for CowRpcUnbindMsg {
    fn read_from<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let msg = CowRpcUnbindMsg {
            ifaces: Vec::<CowRpcIfaceDef>::read_from(reader)?,
        };

        Ok(msg)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        self.ifaces.write_to(writer)
    }

    fn get_size(&self) -> u32 {
        self.ifaces.get_size()
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CowRpcIfaceDef {
    pub id: u16,
    pub flags: u16,
    pub name: String,
    pub procs: Vec<CowRpcProcDef>,
}

impl CowRpcIfaceDef {
    pub fn is_failure(&self) -> bool {
        (self.flags & COW_RPC_FLAG_FAILURE) != 0
    }
}

impl Message for CowRpcIfaceDef {
    fn read_from<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let mut iface = CowRpcIfaceDef::default();
        iface.id = reader.read_u16::<LittleEndian>()?;
        iface.flags = reader.read_u16::<LittleEndian>()?;

        if iface.flags & COW_RPC_DEF_FLAG_NAMED != 0 {
            iface.name = String::read_from(reader)?;
        }

        if iface.flags & COW_RPC_DEF_FLAG_EMPTY == 0 {
            iface.procs = Vec::<CowRpcProcDef>::read_from(reader)?;
        }

        Ok(iface)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_u16::<LittleEndian>(self.id)?;
        writer.write_u16::<LittleEndian>(self.flags)?;

        if self.flags & COW_RPC_DEF_FLAG_NAMED != 0 {
            self.name.write_to(writer)?;
        }

        if self.flags & COW_RPC_DEF_FLAG_EMPTY == 0 {
            self.procs.write_to(writer)?;
        }

        Ok(())
    }

    fn get_size(&self) -> u32 {
        let mut size = 2u32 + //id
            2u32;

        if self.flags & COW_RPC_DEF_FLAG_NAMED != 0 {
            size += self.name.get_size();
        }

        if self.flags & COW_RPC_DEF_FLAG_EMPTY == 0 {
            size += self.procs.get_size();
        }

        size
    }
}

impl Default for CowRpcIfaceDef {
    fn default() -> Self {
        CowRpcIfaceDef {
            id: 0,
            flags: 0,
            name: String::new(),
            procs: Vec::new(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CowRpcProcDef {
    pub id: u16,
    pub flags: u16,
    pub name: String,
}

impl Message for CowRpcProcDef {
    fn read_from<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let mut procedure = CowRpcProcDef::default();

        procedure.id = reader.read_u16::<LittleEndian>()?;
        procedure.flags = reader.read_u16::<LittleEndian>()?;

        if procedure.flags & COW_RPC_DEF_FLAG_NAMED != 0 {
            procedure.name = String::read_from(reader)?;
        }

        Ok(procedure)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_u16::<LittleEndian>(self.id)?;
        writer.write_u16::<LittleEndian>(self.flags)?;

        if self.flags & COW_RPC_DEF_FLAG_NAMED != 0 {
            self.name.write_to(writer)?;
        }

        Ok(())
    }

    fn get_size(&self) -> u32 {
        let mut size = 4u32; // id(2) + flag(2)

        if self.flags & COW_RPC_DEF_FLAG_NAMED != 0 {
            size += self.name.get_size();
        }

        size
    }
}

impl Message for Vec<CowRpcProcDef> {
    fn read_from<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let len = reader.read_u16::<LittleEndian>()?;
        let mut v = Vec::with_capacity(len as usize);

        for _ in 0..len {
            v.push(CowRpcProcDef::read_from(reader)?);
        }

        Ok(v)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_u16::<LittleEndian>(self.len() as u16)?;

        for procedure in self {
            procedure.write_to(writer)?;
        }

        Ok(())
    }

    fn get_size(&self) -> u32 {
        let mut size = 2u32;

        for procedure in self {
            size += procedure.get_size();
        }

        size
    }
}

impl Default for CowRpcProcDef {
    fn default() -> Self {
        CowRpcProcDef {
            id: 0,
            flags: 0,
            name: String::new(),
        }
    }
}

impl Message for CowRpcIdentityType {
    fn read_from<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let typ = reader.read_u8()?;
        Ok(CowRpcIdentityType::try_from(typ)?)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_u8(self.into())?;
        Ok(())
    }

    fn get_size(&self) -> u32 {
        1
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CowRpcIdentityMsg {
    pub typ: CowRpcIdentityType,
    pub flags: u16,
    pub identity: String,
}

impl Message for CowRpcIdentityMsg {
    fn read_from<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let typ = CowRpcIdentityType::read_from(reader)?;
        let len = reader.read_u8()?;
        let flags = reader.read_u16::<LittleEndian>()?;

        // We can't read a string since the len of the string is not in front of the string. So we rebuild the string ourself.
        let mut string = vec![0; len as usize];
        reader.read_exact(&mut string)?;
        reader.read_u8()?; // null terminator
        let identity = string.iter().map(|c| *c as char).collect();

        let identity = CowRpcIdentityMsg { typ, flags, identity };

        Ok(identity)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        self.typ.write_to(writer)?;
        writer.write_u8(self.identity.len() as u8)?;
        writer.write_u16::<LittleEndian>(self.flags)?;

        writer.write_all(&self.identity.chars().map(|c| c as u8).collect::<Vec<u8>>())?;
        writer.write_u8(0u8)?; // null terminator

        Ok(())
    }

    fn get_size(&self) -> u32 {
        // typ + len + flags = 4, string has a null terminator
        let size = 4 + self.identity.len() + 1;
        size as u32
    }
}

impl Default for CowRpcIdentityMsg {
    fn default() -> Self {
        CowRpcIdentityMsg {
            typ: CowRpcIdentityType::UPN,
            flags: 0,
            identity: String::new(),
        }
    }
}

impl From<CowRpcIdentity> for CowRpcIdentityMsg {
    fn from(inner_identity: CowRpcIdentity) -> Self {
        CowRpcIdentityMsg {
            identity: inner_identity.name,
            typ: inner_identity.typ,
            flags: 0,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CowRpcResolveMsg {
    pub node_id: u32,
    pub identity: Option<CowRpcIdentityMsg>,
}

impl CowRpcResolveMsg {
    pub fn read_from<R: Read>(reader: &mut R, flag: u16) -> Result<Self> {
        let node_id = reader.read_u32::<LittleEndian>()?;

        let identity = if CowRpcResolveMsg::is_identity_present(flag) {
            Some(CowRpcIdentityMsg::read_from(reader)?)
        } else {
            None
        };

        let msg = CowRpcResolveMsg { node_id, identity };

        Ok(msg)
    }

    pub fn write_to<W: Write>(&self, writer: &mut W, flag: u16) -> Result<()> {
        writer.write_u32::<LittleEndian>(self.node_id)?;
        if CowRpcResolveMsg::is_identity_present(flag) {
            self.identity.as_ref().unwrap().write_to(writer)?;
        }
        Ok(())
    }

    pub fn get_size(&self, flag: u16) -> u32 {
        let mut size = 4;
        if CowRpcResolveMsg::is_identity_present(flag) {
            size += self.identity.as_ref().unwrap().get_size();
        }

        size
    }

    fn is_identity_present(flag: u16) -> bool {
        // Identity is omitted in a resolve reverse request AND in the response if there is a failure.
        let is_req_reverse = (flag & COW_RPC_FLAG_REVERSE != 0) && (flag & COW_RPC_FLAG_RESPONSE == 0);
        let is_rsp_reverse_failure = (flag & COW_RPC_FLAG_REVERSE != 0)
            && (flag & COW_RPC_FLAG_RESPONSE != 0)
            && (flag & COW_RPC_FLAG_FAILURE != 0);

        !is_req_reverse && !is_rsp_reverse_failure
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CowRpcCallMsg {
    pub call_id: u32,
    pub iface_id: u16,
    pub proc_id: u16,
}

impl Message for CowRpcCallMsg {
    fn read_from<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let msg = CowRpcCallMsg {
            call_id: reader.read_u32::<LittleEndian>()?,
            iface_id: reader.read_u16::<LittleEndian>()?,
            proc_id: reader.read_u16::<LittleEndian>()?,
        };

        Ok(msg)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_u32::<LittleEndian>(self.call_id)?;
        writer.write_u16::<LittleEndian>(self.iface_id)?;
        writer.write_u16::<LittleEndian>(self.proc_id)?;
        Ok(())
    }

    fn get_size(&self) -> u32 {
        8u32
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CowRpcResultMsg {
    pub call_id: u32,
    pub iface_id: u16,
    pub proc_id: u16,
}

impl Message for CowRpcResultMsg {
    fn read_from<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let msg = CowRpcResultMsg {
            call_id: reader.read_u32::<LittleEndian>()?,
            iface_id: reader.read_u16::<LittleEndian>()?,
            proc_id: reader.read_u16::<LittleEndian>()?,
        };

        Ok(msg)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_u32::<LittleEndian>(self.call_id)?;
        writer.write_u16::<LittleEndian>(self.iface_id)?;
        writer.write_u16::<LittleEndian>(self.proc_id)?;
        Ok(())
    }

    fn get_size(&self) -> u32 {
        8u32
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CowRpcVerifyMsg {
    pub call_id: u32,
}

impl Message for CowRpcVerifyMsg {
    fn read_from<R: Read>(reader: &mut R) -> Result<Self>
        where
            Self: Sized,
    {
        let msg = CowRpcVerifyMsg {
            call_id: reader.read_u32::<LittleEndian>()?,
        };

        Ok(msg)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_u32::<LittleEndian>(self.call_id)?;
        Ok(())
    }

    fn get_size(&self) -> u32 {
        4u32
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CowRpcHttpMsg {
    pub call_id: u32,
}

impl Message for CowRpcHttpMsg {
    fn read_from<R: Read>(reader: &mut R) -> Result<Self>
        where
            Self: Sized,
    {
        let msg = CowRpcHttpMsg {
            call_id: reader.read_u32::<LittleEndian>()?,
        };

        Ok(msg)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_u32::<LittleEndian>(self.call_id)?;
        Ok(())
    }

    fn get_size(&self) -> u32 {
        4u32
    }
}

//TODO Test all structures
#[test]
fn test_identity_serialize() {
    let s1 = CowRpcIdentityMsg {
        typ: CowRpcIdentityType::SPN,
        flags: 12345,
        identity: String::from("identity"),
    };

    let mut v = Vec::new();
    assert!(s1.write_to(&mut v).is_ok());
    assert_eq!(s1.get_size(), v.len() as u32);

    let mut sv: &[u8] = &v;
    let s2 = CowRpcIdentityMsg::read_from(&mut sv).unwrap();

    assert_eq!(s1, s2);
    assert_eq!(s1.get_size(), s2.get_size());
}

#[test]
fn test_resolve_serialize() {
    let s1 = CowRpcResolveMsg {
        node_id: 999,
        identity: Some(CowRpcIdentityMsg {
            typ: CowRpcIdentityType::SPN,
            flags: 12345,
            identity: String::from("identity"),
        }),
    };

    let flag: u16 = 0;
    let mut v = Vec::new();
    assert!(s1.write_to(&mut v, flag).is_ok());
    assert_eq!(s1.get_size(flag), v.len() as u32);

    let mut sv: &[u8] = &v;
    let s2 = CowRpcResolveMsg::read_from(&mut sv, flag).unwrap();

    assert_eq!(s1, s2);
    assert_eq!(s1.get_size(flag), s2.get_size(flag));
}
