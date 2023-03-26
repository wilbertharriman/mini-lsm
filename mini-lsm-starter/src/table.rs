mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::lsm_storage::BlockCache;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(&meta.first_key);
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta: Vec<BlockMeta> = Vec::new();
        while buf.has_remaining() {
            let offset: usize = buf.get_u32() as usize;
            let key_len: usize = buf.get_u16() as usize;
            let key = buf.copy_to_bytes(key_len);
            block_meta.push(BlockMeta {
                offset,
                first_key: key,
            })
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(File, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut buf: Vec<u8> = vec![0; len as usize];
        self.0.read_exact_at(&mut buf[..], offset)?;
        Ok(buf)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        // let mut file = File::create(path)?;
        // file.write_all(&data)?;
        // Ok(FileObject(file, data.len() as u64))
        std::fs::write(path, &data)?;
        Ok(FileObject(
            File::options().read(true).write(false).open(path)?,
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        // File::open(path);
        unimplemented!()
    }
}

pub struct SsTable {
    id: usize,
    file: FileObject,
    block_metas: Vec<BlockMeta>,
    block_meta_offset: usize,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let data_len = file.1;
        let block_meta_offset_raw = file.read(data_len - 4, 4)?;
        let block_meta_offset: u64 = (&block_meta_offset_raw[..]).get_u32() as u64;

        let block_meta_raw = file.read(block_meta_offset, data_len - 4 - block_meta_offset)?;

        Ok(SsTable {
            id,
            file,
            block_metas: BlockMeta::decode_block_meta(&block_meta_raw[..]),
            block_meta_offset: block_meta_offset as usize,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_metas[block_idx].offset;
        let next_offset = self
            .block_metas
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);
        let data = self
            .file
            .read(offset as u64, (next_offset - offset) as u64)?;
        Ok(Arc::new(Block::decode(&data[..])))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        // binary search on blockmetas
        self.block_metas
            .partition_point(|meta| key >= meta.first_key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

#[cfg(test)]
mod tests;
