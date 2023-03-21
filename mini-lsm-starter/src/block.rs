#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

pub const SIZE_OF_U16: usize = std::mem::size_of::<u16>(); //bytes

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        let offsets_len = self.offsets.len();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.put_u16(offsets_len as u16);
        buf.into()
    }

    pub fn decode(data: &[u8]) -> Self {
        let offsets_len = (&data[data.len() - SIZE_OF_U16..]).get_u16() as usize;
        let data_end = data.len() - offsets_len * SIZE_OF_U16 - SIZE_OF_U16;
        let offsets_raw = &data[data_end..data.len() - SIZE_OF_U16];
        let offsets = offsets_raw
            .chunks(SIZE_OF_U16)
            .map(|mut offset| offset.get_u16())
            .collect();
        let data = data[..data_end].to_vec();
        Self { data, offsets }
    }
}

#[cfg(test)]
mod tests;
