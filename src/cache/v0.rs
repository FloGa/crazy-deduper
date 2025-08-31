use std::borrow::Cow;
use std::cell::OnceCell;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::{DedupCache, FileChunk, FileWithChunks, HashingAlgorithm};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct FileWithChunksOnDisk<'a> {
    path: Cow<'a, String>,
    size: u64,
    mtime: SystemTime,
    chunks: Option<Vec<FileChunkOnDisk<'a>>>,
    hashing_algorithm: HashingAlgorithm,
}

impl<'a> From<&'a FileWithChunks> for FileWithChunksOnDisk<'a> {
    fn from(value: &'a FileWithChunks) -> Self {
        Self {
            path: Cow::Borrowed(&value.path),
            size: value.size,
            mtime: value.mtime,
            chunks: value
                .chunks
                .get()
                .map(|chunks| chunks.iter().map(FileChunkOnDisk::from).collect()),
            hashing_algorithm: value.hashing_algorithm,
        }
    }
}

impl From<FileWithChunksOnDisk<'_>> for FileWithChunks {
    fn from(value: FileWithChunksOnDisk) -> Self {
        Self {
            base: Default::default(),
            path: value.path.into_owned(),
            size: value.size,
            mtime: value.mtime,
            chunks: value
                .chunks
                .map(|chunks| {
                    OnceCell::from(chunks.into_iter().map(FileChunk::from).collect::<Vec<_>>())
                })
                .unwrap_or_default(),
            hashing_algorithm: value.hashing_algorithm,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct FileChunkOnDisk<'a> {
    start: u64,
    size: u64,
    hash: Cow<'a, String>,
}

impl<'a> From<&'a FileChunk> for FileChunkOnDisk<'a> {
    fn from(value: &'a FileChunk) -> Self {
        Self {
            start: value.start,
            size: value.size,
            hash: Cow::Borrowed(&value.hash),
        }
    }
}

impl From<FileChunkOnDisk<'_>> for FileChunk {
    fn from(value: FileChunkOnDisk) -> Self {
        Self {
            start: value.start,
            size: value.size,
            hash: value.hash.into_owned(),
            path: None,
        }
    }
}

#[derive(Default, Deserialize, Serialize)]
#[serde(transparent)]
pub(crate) struct CacheOnDisk<'a>(Vec<FileWithChunksOnDisk<'a>>);

impl<'a> CacheOnDisk<'a> {
    pub(crate) fn into_owned(self) -> Vec<FileWithChunks> {
        self.0.into_iter().map(FileWithChunks::from).collect()
    }
}

impl<'a> From<&'a DedupCache> for CacheOnDisk<'a> {
    fn from(value: &'a DedupCache) -> Self {
        CacheOnDisk(value.values().map(FileWithChunksOnDisk::from).collect())
    }
}
