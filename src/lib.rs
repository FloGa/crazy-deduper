use std::cell::OnceCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::ops::Deref;
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

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(from = "Option<T>")]
#[serde(into = "Option<T>")]
struct LazyOption<T>(OnceCell<T>)
where
    T: Clone;

impl<T> From<Option<T>> for LazyOption<T>
where
    T: Clone,
{
    fn from(value: Option<T>) -> Self {
        Self(if let Some(value) = value {
            OnceCell::from(value)
        } else {
            OnceCell::new()
        })
    }
}

impl<T> Into<Option<T>> for LazyOption<T>
where
    T: Clone,
{
    fn into(self) -> Option<T> {
        self.0.get().cloned()
    }
}

impl<T> Deref for LazyOption<T>
where
    T: Clone,
{
    type Target = OnceCell<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FileWithChunks {
    base: PathBuf,
    pub path: String,
    pub size: u64,
    pub mtime: SystemTime,
    chunks: LazyOption<Vec<FileChunk>>,
}

impl PartialEq for FileWithChunks {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path && self.size == other.size && self.mtime == other.mtime
    }
}

impl Eq for FileWithChunks {}

impl FileWithChunks {
    pub fn try_new(source_path: impl Into<PathBuf>, path: impl Into<PathBuf>) -> Result<Self> {
        let base = source_path.into();

        let path = path.into();
        let metadata = path.metadata()?;

        let path = path
            .strip_prefix(&base)
            .unwrap()
            .to_string_lossy()
            .to_string();
        let size = metadata.len();
        let mtime = metadata.modified()?;

        Ok(Self {
            base,
            path,
            size,
            mtime,
            chunks: Default::default(),
        })
    }

    pub fn get_chunks(&self) -> Option<&Vec<FileChunk>> {
        self.chunks.get()
    }

    pub fn get_or_calculate_chunks(&self) -> Result<&Vec<FileChunk>> {
        if self.chunks.get().is_none() {
            let chunks = self.calculate_chunks()?;

            // Cannot panic, we already checked that it is empty.
            self.chunks.set(chunks.clone()).unwrap();
        }

        Ok(self.chunks.get().unwrap())
    }

    fn calculate_chunks(&self) -> Result<Vec<FileChunk>> {
        let path = self.base.join(&self.path);

        let input = BufReader::new(File::open(&path)?);
        let mut bytes = input.bytes();

        let mut chunks = Vec::new();
        let size = path.metadata()?.len();

        let mut hasher = Sha256::new();

        // Process file in MiB chunks.
        for start in (0..).take_while(|i| i * 1024 * 1024 < size) {
            let chunk = bytes
                .by_ref()
                .take(1024 * 1024)
                .flatten()
                .collect::<Vec<_>>();

            hasher.update(&chunk);
            let hash = hasher.finalize_reset();
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
    #[serde(skip)]
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

    pub fn get_chunks(&self) -> Result<impl Iterator<Item = (String, FileChunk, bool)> + '_> {
        Ok(self.iter().flat_map(|fwc| {
            let mut dirty = fwc.get_chunks().is_none();

            fwc.get_or_calculate_chunks()
                .unwrap()
                .iter()
                .map(move |chunk| {
                    let result = (
                        chunk.hash.clone(),
                        FileChunk {
                            path: Some(fwc.path.clone()),
                            ..chunk.clone()
                        },
                        dirty,
                    );

                    dirty = false;

                    result
                })
        }))
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

            if !entry.is_file() {
                continue;
            }

            let fwc = FileWithChunks::try_new(&source_path, &entry).unwrap();

            if let Some(fwc_cache) = cache.get(&fwc.path) {
                if fwc == *fwc_cache {
                    continue;
                }
            }

            cache.insert(fwc.path.clone(), fwc);
        }

        Self {
            source_path,
            cache_path,
            cache,
        }
    }

    pub fn write_cache(&self) {
        let temp_path = self.cache_path.clone().with_extension(format!(
            "tmp.{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        self.cache.write_to_file(&temp_path);
        std::fs::rename(temp_path, &self.cache_path).unwrap();
    }

    pub fn write_chunks(&mut self, target_path: impl Into<PathBuf>) -> Result<()> {
        let target_path = target_path.into();
        let data_dir = target_path.join("data");
        std::fs::create_dir_all(&data_dir)?;
        for (_, chunk, _) in self.cache.get_chunks()? {
            let chunk_file = data_dir.join(&chunk.hash);
            if !chunk_file.exists() {
                let mut out = File::create(chunk_file)?;
                let data_in = BufReader::new(File::open(
                    self.source_path.join(chunk.path.as_ref().unwrap()),
                )?)
                .bytes()
                .skip(chunk.start as usize)
                .take(chunk.size as usize)
                .flatten()
                .collect::<Vec<_>>();
                out.write_all(&data_in)?;
            }
        }

        Ok(())
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

    pub fn restore_files(&self, target_path: impl Into<PathBuf>) {
        let data_dir = self.source_path.join("data");
        let target_path = target_path.into();
        std::fs::create_dir_all(&target_path).unwrap();
        for fwc in self.cache.iter() {
            let target = target_path.join(&fwc.path);
            std::fs::create_dir_all(&target.parent().unwrap()).unwrap();
            let target_file = File::create(&target).unwrap();
            let mut target = BufWriter::new(&target_file);
            for chunk in fwc.get_chunks().unwrap() {
                let mut source = File::open(data_dir.join(&chunk.hash)).unwrap();
                std::io::copy(&mut source, &mut target).unwrap();
            }
            target.flush().unwrap();
            target_file.set_modified(fwc.mtime).unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_fs::prelude::*;
    use assert_fs::TempDir;

    use super::*;

    #[test]
    fn compare_filechunk_objects() -> anyhow::Result<()> {
        let temp = TempDir::new()?;

        let file_1 = temp.child("file_1");
        std::fs::write(&file_1, "content_1")?;

        let file_2 = temp.child("file_2");
        std::fs::write(&file_2, "content_2")?;

        let fwc_1 = FileWithChunks::try_new(&temp.path(), &file_1.path())?;
        let fwc_1_same = FileWithChunks::try_new(&temp.path(), &file_1.path())?;
        let fwc_2 = FileWithChunks::try_new(&temp.path(), &file_2.path())?;

        assert_eq!(fwc_1, fwc_1);
        assert_eq!(fwc_1, fwc_1_same);
        assert_ne!(fwc_1, fwc_2);

        File::open(&file_1)?.set_modified(SystemTime::now())?;

        let fwc_1_new = FileWithChunks::try_new(&temp.path(), &file_1.path())?;

        assert_ne!(fwc_1, fwc_1_new);

        Ok(())
    }
}
