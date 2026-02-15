use std::borrow::Cow;
use std::cell::OnceCell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::cache::v0;
use crate::{DedupCache, FileChunk, FileWithChunks, HashingAlgorithm};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SystemTimeOnDisk {
    #[serde(rename = "s")]
    secs: u64,
    #[serde(rename = "n")]
    nanos: u32,
}

impl From<SystemTime> for SystemTimeOnDisk {
    fn from(value: SystemTime) -> Self {
        let duration = value.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let secs = duration.as_secs();
        let nanos = duration.subsec_nanos();
        Self { secs, nanos }
    }
}

impl From<SystemTimeOnDisk> for SystemTime {
    fn from(value: SystemTimeOnDisk) -> Self {
        SystemTime::UNIX_EPOCH + Duration::new(value.secs, value.nanos)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct FileWithChunksOnDisk<'a> {
    #[serde(rename = "s")]
    size: u64,
    #[serde(rename = "m")]
    mtime: SystemTimeOnDisk,
    #[serde(borrow)]
    #[serde(rename = "c")]
    chunks: Option<Vec<FileChunkOnDisk<'a>>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct FileChunkOnDisk<'a> {
    #[serde(rename = "s")]
    start: u64,
    #[serde(rename = "i")]
    size: u64,
    #[serde(rename = "h")]
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

impl<'a> From<v0::FileWithChunksOnDisk<'a>> for FileWithChunksOnDisk<'a> {
    fn from(value: v0::FileWithChunksOnDisk<'a>) -> Self {
        Self {
            size: value.size,
            mtime: value.mtime.into(),
            chunks: value.chunks.map(|vec_fcd| {
                vec_fcd
                    .into_iter()
                    .map(|fcd| FileChunkOnDisk {
                        start: fcd.start,
                        size: fcd.size,
                        hash: fcd.hash,
                    })
                    .collect()
            }),
        }
    }
}

impl<'a> From<&'a FileWithChunks> for FileWithChunksOnDisk<'a> {
    fn from(value: &'a FileWithChunks) -> Self {
        Self {
            size: value.size,
            mtime: value.mtime.into(),
            chunks: value.chunks.get().map(|chunks| {
                chunks
                    .iter()
                    .map(|fcd| FileChunkOnDisk {
                        start: fcd.start,
                        size: fcd.size,
                        hash: fcd.hash.as_str(),
                    })
                    .collect()
            }),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub(crate) enum Node<'a> {
    Path(#[serde(borrow)] BTreeMap<Cow<'a, str>, Box<Node<'a>>>),
    File(#[serde(borrow)] FileWithChunksOnDisk<'a>),
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub(crate) struct CacheOnDisk<'a> {
    #[serde(borrow)]
    #[serde(rename = "f")]
    files: BTreeMap<Cow<'a, str>, Box<Node<'a>>>,
    #[serde(rename = "h")]
    hashing_algorithm: HashingAlgorithm,
}

fn create_empty_path_node_box<'a>() -> Box<Node<'a>> {
    Box::new(Node::Path(BTreeMap::new()))
}

impl<'a> From<v0::CacheOnDisk<'a>> for CacheOnDisk<'a> {
    fn from(value: v0::CacheOnDisk<'a>) -> Self {
        let hashing_algorithm = value
            .0
            .iter()
            .map(|fwcd| fwcd.hashing_algorithm)
            .next()
            .unwrap_or_default();

        let mut files = BTreeMap::new();
        for fwcd in value.0 {
            let mut leaf = &mut files;
            let path = Path::new(fwcd.path.as_ref());
            for component in path.parent().unwrap().iter() {
                leaf = if let Node::Path(map) = leaf
                    .entry(component.to_string_lossy().into_owned().into())
                    .or_insert_with(create_empty_path_node_box)
                    .as_mut()
                {
                    map
                } else {
                    unreachable!()
                };
            }
            leaf.insert(
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
                    .into(),
                Box::new(Node::File(fwcd.into())),
            );
        }

        Self {
            hashing_algorithm,
            files,
        }
    }
}

impl<'a> CacheOnDisk<'a> {
    pub(crate) fn into_owned(self) -> Vec<FileWithChunks> {
        let hashing_algorithm = self.hashing_algorithm;

        let mut files = Vec::new();

        fn walk(
            files_list: &mut Vec<FileWithChunks>,
            files_map: BTreeMap<Cow<str>, Box<Node>>,
            path_base: PathBuf,
            hashing_algorithm: HashingAlgorithm,
        ) {
            for (path, node) in files_map.into_iter() {
                let path_buf = path_base.join(path.as_ref());
                match *node {
                    Node::Path(files_map) => {
                        walk(files_list, files_map, path_buf, hashing_algorithm)
                    }
                    Node::File(fwcd) => files_list.push(FileWithChunks {
                        base: Default::default(),
                        path: path_buf.into_os_string().into_string().unwrap(),
                        size: fwcd.size,
                        mtime: fwcd.mtime.into(),
                        chunks: fwcd
                            .chunks
                            .map(|chunks| {
                                OnceCell::from(
                                    chunks.into_iter().map(FileChunk::from).collect::<Vec<_>>(),
                                )
                            })
                            .unwrap_or_default(),
                        hashing_algorithm,
                    }),
                }
            }
        }

        walk(&mut files, self.files, PathBuf::new(), hashing_algorithm);

        files
    }
}

impl<'a> From<&'a DedupCache> for CacheOnDisk<'a> {
    fn from(value: &'a DedupCache) -> Self {
        let hashing_algorithm = value
            .values()
            .map(|fwc| fwc.hashing_algorithm)
            .next()
            .unwrap_or_default();

        let mut files = BTreeMap::new();
        for fwc in value.values() {
            let mut leaf = &mut files;
            let path = Path::new(&fwc.path);
            for component in path.parent().unwrap().iter() {
                leaf = if let Node::Path(map) = leaf
                    .entry(component.to_string_lossy().into_owned().into())
                    .or_insert_with(create_empty_path_node_box)
                    .as_mut()
                {
                    map
                } else {
                    unreachable!()
                };
            }
            leaf.insert(
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
                    .into(),
                Box::new(Node::File(fwc.into())),
            );
        }

        Self {
            hashing_algorithm,
            files,
        }
    }
}
