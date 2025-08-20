use serde::{Deserialize, Serialize};

use crate::{DedupCache, FileWithChunks};

pub(crate) fn from_str(s: &str) -> serde_json::Result<Vec<FileWithChunks>> {
    serde_json::from_str::<CacheOnDisk>(s).map(CacheOnDisk::into_inner)
}

pub(crate) fn to_writer(writer: impl std::io::Write, cache: &DedupCache) -> serde_json::Result<()> {
    serde_json::to_writer(writer, &CacheOnDiskBorrowed::from(cache))
}

#[derive(Default, Deserialize)]
#[serde(transparent)]
struct CacheOnDisk(Vec<FileWithChunks>);

impl CacheOnDisk {
    fn into_inner(self) -> Vec<FileWithChunks> {
        self.0
    }
}

#[derive(Serialize)]
#[serde(transparent)]
struct CacheOnDiskBorrowed<'a>(Vec<&'a FileWithChunks>);

impl<'a> From<&'a DedupCache> for CacheOnDiskBorrowed<'a> {
    fn from(value: &'a DedupCache) -> Self {
        CacheOnDiskBorrowed(value.values().collect())
    }
}
