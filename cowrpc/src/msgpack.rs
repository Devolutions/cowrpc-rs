use crate::error::Result;
use rmp;
use std::io::Read;

pub fn msgpack_decode_binary<R: Read>(rd: &mut R, len: usize) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    let mut i = 0;
    while i < len {
        let byte: u8 = rmp::decode::read_data_u8(rd).expect("39");
        buf.push(byte);
        i += 1;
    }

    Ok(buf)
}
