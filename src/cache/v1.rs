use std::borrow::Cow;
use std::cell::OnceCell;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::cache::v0;
use crate::{DedupCache, FileChunk, FileWithChunks, HashingAlgorithm};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct FileWithChunksOnDisk<'a> {
    #[serde(borrow)]
    path: Cow<'a, str>,
    size: u64,
    mtime: SystemTime,
    chunks: Option<Vec<FileChunkOnDisk<'a>>>,
}

impl<'a> From<&'a FileWithChunks> for FileWithChunksOnDisk<'a> {
    fn from(value: &'a FileWithChunks) -> Self {
        Self {
            path: value.path.as_str().into(),
            size: value.size,
            mtime: value.mtime,
            chunks: value
                .chunks
                .get()
                .map(|chunks| chunks.iter().map(FileChunkOnDisk::from).collect()),
        }
    }
}

impl From<FileWithChunksOnDisk<'_>> for FileWithChunks {
    fn from(value: FileWithChunksOnDisk) -> Self {
        Self {
            base: Default::default(),
            path: value.path.to_string(),
            size: value.size,
            mtime: value.mtime,
            chunks: value
                .chunks
                .map(|chunks| {
                    OnceCell::from(chunks.into_iter().map(FileChunk::from).collect::<Vec<_>>())
                })
                .unwrap_or_default(),
            hashing_algorithm: Default::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct FileChunkOnDisk<'a> {
    start: u64,
    size: u64,
    hash: &'a str,
}

impl<'a> From<&'a FileChunk> for FileChunkOnDisk<'a> {
    fn from(value: &'a FileChunk) -> Self {
        Self {
            start: value.start,
            size: value.size,
            hash: value.hash.as_str(),
        }
    }
}

impl From<FileChunkOnDisk<'_>> for FileChunk {
    fn from(value: FileChunkOnDisk) -> Self {
        Self {
            start: value.start,
            size: value.size,
            hash: value.hash.to_owned(),
            path: None,
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub(crate) struct CacheOnDisk<'a> {
    #[serde(borrow)]
    files: Vec<FileWithChunksOnDisk<'a>>,
    hashing_algorithm: HashingAlgorithm,
}

impl<'a> From<v0::CacheOnDisk<'a>> for CacheOnDisk<'a> {
    fn from(value: v0::CacheOnDisk<'a>) -> Self {
        Self {
            hashing_algorithm: value
                .0
                .iter()
                .map(|fwcd| fwcd.hashing_algorithm)
                .next()
                .unwrap_or_default(),
            files: value
                .0
                .into_iter()
                .map(|fwcd| FileWithChunksOnDisk {
                    path: fwcd.path,
                    size: fwcd.size,
                    mtime: fwcd.mtime,
                    chunks: fwcd.chunks.map(|vec_fcd| {
                        vec_fcd
                            .into_iter()
                            .map(|fcd| FileChunkOnDisk {
                                start: fcd.start,
                                size: fcd.size,
                                hash: fcd.hash,
                            })
                            .collect()
                    }),
                })
                .collect(),
        }
    }
}

impl<'a> CacheOnDisk<'a> {
    pub(crate) fn into_owned(self) -> Vec<FileWithChunks> {
        self.files
            .into_iter()
            .map(|fwcd| FileWithChunks {
                hashing_algorithm: self.hashing_algorithm,
                ..FileWithChunks::from(fwcd)
            })
            .collect()
    }
}

impl<'a> From<&'a DedupCache> for CacheOnDisk<'a> {
    fn from(value: &'a DedupCache) -> Self {
        CacheOnDisk {
            hashing_algorithm: value
                .values()
                .map(|fwc| fwc.hashing_algorithm)
                .next()
                .unwrap_or_default(),
            files: value.values().map(FileWithChunksOnDisk::from).collect(),
        }
    }
}
