use super::{Block, SIZE_OF_U16};
use bytes::BufMut;

/// Builds a block.
pub struct BlockBuilder {
    data: Vec<u8>,
    offsets: Vec<u16>,
    block_size: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            data: Vec::new(),
            offsets: Vec::new(),
            block_size,
        }
    }

    fn estimated_size(&self) -> usize {
        // data + offsets + num_elements
        self.data.len() + self.offsets.len() * SIZE_OF_U16 + SIZE_OF_U16
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        if self.estimated_size() + SIZE_OF_U16 + SIZE_OF_U16 * 2 + key.len() + value.len()
            > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        assert!(!key.is_empty());

        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(key.len() as u16);
        self.data.put(key);
        self.data.put_u16(value.len() as u16);
        self.data.put(value);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
