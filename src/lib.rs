use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use walkdir::WalkDir;

#[derive(Debug, Error)]
pub enum Error {
    #[error("'{0}' already exists!")]
    AlreadyExists(PathBuf),

    #[error("'{0}' not properly initialized")]
    NotInitialized(PathBuf),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Walkdir(#[from] walkdir::Error),

    #[error(transparent)]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

type Result<R> = std::result::Result<R, Error>;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FileWithChunks {
    pub path: String,
    pub size: u64,
    pub mtime: SystemTime,
    pub chunks: Vec<FileChunk>,
}

impl FileWithChunks {
    pub fn try_new(source_path: impl Into<PathBuf>, path: impl Into<PathBuf>) -> Result<Self> {
        let source_path = source_path.into();

        let path = path.into();
        let metadata = path.metadata()?;
        let chunks = FileWithChunks::calculate_chunks(&path)?;

        let path = path
            .strip_prefix(&source_path)
            .unwrap()
            .to_string_lossy()
            .to_string();
        let size = metadata.len();
        let mtime = metadata.modified()?;

        Ok(Self {
            path,
            size,
            mtime,
            chunks,
        })
    }

    fn calculate_chunks(path: impl AsRef<Path>) -> Result<Vec<FileChunk>> {
        let input = BufReader::new(File::open(&path)?);
        let mut bytes = input.bytes();

        let mut chunks = Vec::new();
        let size = path.as_ref().metadata()?.len();

        // Process file in MiB chunks.
        for start in (0..).take_while(|i| i * 1024 * 1024 < size) {
            let chunk = bytes
                .by_ref()
                .take(1024 * 1024)
                .flatten()
                .collect::<Vec<_>>();

            let mut hasher = Sha256::new();
            hasher.update(&chunk);
            let hash = hasher.finalize();
            let hash = base16ct::lower::encode_string(&hash);

            let file_chunk = FileChunk::new(start * 1024 * 1024, chunk.len() as u64, hash);

            chunks.push(file_chunk);
        }

        Ok(chunks)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FileChunk {
    pub start: u64,
    pub size: u64,
    pub hash: String,
    pub path: Option<String>,
}

impl FileChunk {
    pub fn new(start: u64, size: u64, hash: String) -> Self {
        Self {
            start,
            size,
            hash,
            path: None,
        }
    }
}

pub struct DedupCache(HashMap<String, FileWithChunks>);

impl DedupCache {
    fn new() -> DedupCache {
        Self(HashMap::new())
    }

    fn read_from_file(&mut self, path: impl AsRef<Path>) {
        let cache_from_file: Vec<FileWithChunks> = File::open(path)
            .map(BufReader::new)
            .map(|reader| serde_json::from_reader(reader))
            .map(|result| result.unwrap())
            .unwrap_or_default();

        for x in cache_from_file {
            self.insert(x.path.clone(), x);
        }
    }

    fn write_to_file(&self, path: impl AsRef<Path>) {
        std::fs::create_dir_all(path.as_ref().parent().unwrap()).unwrap();
        File::create(path)
            .map(BufWriter::new)
            .map(|writer| serde_json::to_writer(writer, &self.iter().collect::<Vec<_>>()))
            .unwrap()
            .unwrap();
    }

    pub fn get(&self, path: &str) -> Option<&FileWithChunks> {
        self.0.get(path)
    }

    fn insert(&mut self, path: String, fwc: FileWithChunks) {
        self.0.insert(path, fwc);
    }

    pub fn contains_key(&self, path: &str) -> bool {
        self.0.contains_key(path)
    }

    pub fn iter(&self) -> impl Iterator<Item = &FileWithChunks> {
        self.0.values()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct Deduper {
    source_path: PathBuf,
    cache_path: PathBuf,
    pub cache: DedupCache,
}

impl Deduper {
    pub fn new(source_path: impl Into<PathBuf>, cache_path: impl Into<PathBuf>) -> Self {
        let source_path = source_path.into();
        let cache_path = cache_path.into();

        let mut cache = DedupCache::new();
        cache.read_from_file(&cache_path);

        let dir_walker = WalkDir::new(&source_path)
            .min_depth(1)
            .same_file_system(true);

        for entry in dir_walker {
            let entry = entry.unwrap().into_path();

            if entry.is_file()
                && !cache.contains_key(
                    &entry
                        .strip_prefix(&source_path)
                        .unwrap()
                        .to_string_lossy()
                        .to_string(),
                )
            {
                let fwc = FileWithChunks::try_new(&source_path, &entry).unwrap();
                cache.insert(fwc.path.clone(), fwc);
            }
        }

        Self {
            source_path,
            cache_path,
            cache,
        }
    }

    pub fn write_cache(&self) {
        self.cache.write_to_file(&self.cache_path);
    }

    pub fn get_chunks(&self) -> HashMap<String, FileChunk> {
        self.cache
            .iter()
            .flat_map(|fwc| {
                fwc.chunks.iter().map(|chunk| {
                    (
                        chunk.hash.clone(),
                        FileChunk {
                            path: Some(fwc.path.clone()),
                            ..chunk.clone()
                        },
                    )
                })
            })
            .collect()
    }

    pub fn write_chunks(&self, target_path: impl Into<PathBuf>) {
        let target_path = target_path.into();
        let data_dir = target_path.join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        for (_, chunk) in self.get_chunks() {
            let chunk_file = data_dir.join(&chunk.hash);
            if !chunk_file.exists() {
                let mut out = File::create(chunk_file).unwrap();
                let data_in = BufReader::new(
                    File::open(self.source_path.join(chunk.path.as_ref().unwrap())).unwrap(),
                )
                .bytes()
                .skip(chunk.start as usize)
                .take(chunk.size as usize)
                .flatten()
                .collect::<Vec<_>>();
                out.write_all(&data_in).unwrap();
            }
        }
    }
}

pub struct Hydrator {
    source_path: PathBuf,
    pub cache: DedupCache,
}

impl Hydrator {
    pub fn new(source_path: impl Into<PathBuf>, cache_path: impl Into<PathBuf>) -> Self {
        let source_path = source_path.into();
        let cache_path = cache_path.into();

        let mut cache = DedupCache::new();
        cache.read_from_file(&cache_path);

        Self { source_path, cache }
    }

    pub fn write_cache(&self) {
        self.cache.write_to_file("cache.json");
    }

    pub fn get_chunks(&self) -> HashMap<String, FileChunk> {
        self.cache
            .iter()
            .flat_map(|fwc| {
                fwc.chunks.iter().map(|chunk| {
                    (
                        chunk.hash.clone(),
                        FileChunk {
                            path: Some(fwc.path.clone()),
                            ..chunk.clone()
                        },
                    )
                })
            })
            .collect()
    }

    pub fn restore_files(&self, target_path: impl Into<PathBuf>) {
        let data_dir = self.source_path.join("data");
        let target_path = target_path.into();
        std::fs::create_dir_all(&target_path).unwrap();
        for fwc in self.cache.iter() {
            let target = target_path.join(&fwc.path);
            std::fs::create_dir_all(&target.parent().unwrap()).unwrap();
            let mut target = BufWriter::new(File::create(&target).unwrap());
            for chunk in &fwc.chunks {
                let mut source = File::open(data_dir.join(&chunk.hash)).unwrap();
                std::io::copy(&mut source, &mut target).unwrap();
            }
            target.flush().unwrap();
        }
    }
}
