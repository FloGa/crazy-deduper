use serde::{Deserialize, Serialize};

use crate::{DedupCache, FileWithChunks};

#[derive(Default, Deserialize)]
#[serde(transparent)]
pub(crate) struct CacheOnDisk(Vec<FileWithChunks>);

impl CacheOnDisk {
    pub(crate) fn into_inner(self) -> Vec<FileWithChunks> {
        self.0
    }
}

#[derive(Serialize)]
#[serde(transparent)]
pub(crate) struct CacheOnDiskBorrowed<'a>(Vec<&'a FileWithChunks>);

impl<'a> From<&'a DedupCache> for CacheOnDiskBorrowed<'a> {
    fn from(value: &'a DedupCache) -> Self {
        CacheOnDiskBorrowed(value.values().collect())
    }
}
