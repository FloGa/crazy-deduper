use std::borrow::Cow;
use std::time::SystemTime;

use serde::Deserialize;

use crate::HashingAlgorithm;

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct FileWithChunksOnDisk<'a> {
    #[serde(borrow)]
    pub(crate) path: Cow<'a, str>,
    pub(crate) size: u64,
    pub(crate) mtime: SystemTime,
    pub(crate) chunks: Option<Vec<FileChunkOnDisk<'a>>>,
    pub(crate) hashing_algorithm: HashingAlgorithm,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct FileChunkOnDisk<'a> {
    pub(crate) start: u64,
    pub(crate) size: u64,
    pub(crate) hash: &'a str,
}

#[derive(Debug, Default, Deserialize)]
#[serde(transparent)]
pub(crate) struct CacheOnDisk<'a>(#[serde(borrow)] pub(crate) Vec<FileWithChunksOnDisk<'a>>);
