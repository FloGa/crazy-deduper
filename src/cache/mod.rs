use crate::cache::v0::{CacheOnDisk, CacheOnDiskBorrowed};
use crate::{DedupCache, FileWithChunks};

mod v0;

pub(crate) fn from_str(s: &str) -> serde_json::Result<Vec<FileWithChunks>> {
    serde_json::from_str::<CacheOnDisk>(s).map(CacheOnDisk::into_inner)
}

pub(crate) fn to_writer(writer: impl std::io::Write, cache: &DedupCache) -> serde_json::Result<()> {
    serde_json::to_writer(writer, &CacheOnDiskBorrowed::from(cache))
}
