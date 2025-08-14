use serde::{Deserialize, Serialize};

use crate::FileWithChunks;

#[derive(Default, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct CacheOnDisk(Vec<FileWithChunks>);

impl CacheOnDisk {
    pub(crate) fn into_inner(self) -> Vec<FileWithChunks> {
        self.0
    }
}
