use bytes::Buf;
use std::sync::Arc;

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    block: Arc<Block>,
    key: Vec<u8>,
    value: Vec<u8>,
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut it = BlockIterator::new(block);
        it.seek_to_first();
        it
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut it = BlockIterator::new(block);
        it.seek_to_key(key);
        it
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    fn seek_to(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value.clear();
            return;
        }
        self.idx = idx;
        let offset = self.block.offsets[idx];
        self.seek_to_offset(offset as usize);
    }

    fn seek_to_offset(&mut self, offset: usize) {
        // read keylen
        // read key
        // read valuelen
        // read value
        let mut entry = &self.block.data[offset..];
        let key_len = entry.get_u16() as usize;
        let key = entry[..key_len].to_vec();
        entry.advance(key_len);
        self.key.clear();
        self.key.extend(key);
        let value_len = entry.get_u16() as usize;
        let value = entry[..value_len].to_vec();
        entry.advance(value_len);
        self.value.clear();
        self.value.extend(value);
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.seek_to(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        // binary search
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = (low + high) / 2;
            self.seek_to(mid);
            assert!(self.is_valid());
            match self.key().cmp(key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Equal => return,
                std::cmp::Ordering::Greater => high = mid,
            }
        }
        self.seek_to(low);
    }
}
