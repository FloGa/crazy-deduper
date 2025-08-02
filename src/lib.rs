use std::cell::OnceCell;
use std::collections::HashMap;
use std::collections::hash_map::IntoIter;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use file_declutter::FileDeclutter;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use walkdir::WalkDir;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
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

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum HashingAlgorithm {
    MD5,
    SHA1,
    SHA256,
    SHA512,
}

impl HashingAlgorithm {
    fn select_hasher(&self) -> Box<dyn sha2::digest::DynDigest> {
        match self {
            Self::MD5 => Box::new(md5::Md5::default()),
            Self::SHA1 => Box::new(sha1::Sha1::default()),
            Self::SHA256 => Box::new(sha2::Sha256::default()),
            Self::SHA512 => Box::new(sha2::Sha512::default()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FileWithChunks {
    #[serde(skip)]
    base: PathBuf,
    pub path: String,
    pub size: u64,
    pub mtime: SystemTime,
    chunks: LazyOption<Vec<FileChunk>>,
    hashing_algorithm: HashingAlgorithm,
}

impl PartialEq for FileWithChunks {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path && self.size == other.size && self.mtime == other.mtime
    }
}

impl Eq for FileWithChunks {}

impl FileWithChunks {
    pub fn try_new(
        source_path: impl Into<PathBuf>,
        path: impl Into<PathBuf>,
        hashing_algorithm: HashingAlgorithm,
    ) -> Result<Self> {
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
            hashing_algorithm,
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

        let size = path.metadata()?.len();

        let hashing_algorithm = self.hashing_algorithm;

        // Process file in MiB chunks.
        let chunk_size = 1024 * 1024;
        if size == 0 {
            let hasher = hashing_algorithm.select_hasher();
            let hash = hasher.finalize();
            let hash = base16ct::lower::encode_string(&hash);

            std::iter::once(Ok::<FileChunk, Error>(FileChunk::new(0, 0, hash))).collect()
        } else {
            (0..(size + chunk_size - 1) / chunk_size)
                .into_par_iter()
                .map(|start| {
                    let mut input = BufReader::new(File::open(&path)?);
                    input.seek(SeekFrom::Start(start * chunk_size)).unwrap();

                    let chunk = input
                        .bytes()
                        .by_ref()
                        .take(chunk_size as usize)
                        .flatten()
                        .collect::<Vec<_>>();

                    let mut hasher = hashing_algorithm.select_hasher();
                    hasher.update(&chunk);
                    let hash = hasher.finalize();
                    let hash = base16ct::lower::encode_string(&hash);

                    Ok::<FileChunk, Error>(FileChunk::new(
                        start * chunk_size,
                        chunk.len() as u64,
                        hash,
                    ))
                })
                .collect()
        }
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
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn from_hashmap(hash_map: HashMap<String, FileWithChunks>) -> Self {
        Self(hash_map)
    }

    fn read_from_file(&mut self, path: impl AsRef<Path>) {
        let reader = File::open(&path).map(BufReader::new);

        let cache_from_file: Vec<FileWithChunks> =
            if path.as_ref().extension() == Some("zst".as_ref()) {
                reader
                    .and_then(zstd::Decoder::with_buffer)
                    .map(|reader| serde_json::from_reader(reader))
                    .map(|result| result.unwrap())
                    .unwrap_or_default()
            } else {
                reader
                    .map(|reader| serde_json::from_reader(reader))
                    .map(|result| result.unwrap())
                    .unwrap_or_default()
            };

        for x in cache_from_file {
            self.insert(x.path.clone(), x);
        }
    }

    fn write_to_file(&self, path: impl AsRef<Path>) {
        std::fs::create_dir_all(path.as_ref().parent().unwrap()).unwrap();
        let writer = File::create(&path).map(BufWriter::new);

        if path.as_ref().extension() == Some("zst".as_ref()) {
            writer
                .and_then(|writer| zstd::Encoder::new(writer, 0))
                .map(|encoder| encoder.auto_finish())
                .map(|writer| serde_json::to_writer(writer, &self.values().collect::<Vec<_>>()))
                .unwrap()
                .unwrap();
        } else {
            writer
                .map(|writer| serde_json::to_writer(writer, &self.values().collect::<Vec<_>>()))
                .unwrap()
                .unwrap();
        }
    }

    pub fn get_chunks(&self) -> Result<impl Iterator<Item = (String, FileChunk, bool)> + '_> {
        Ok(self.values().flat_map(|fwc| {
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

    pub fn get_mut(&mut self, path: &str) -> Option<&mut FileWithChunks> {
        self.0.get_mut(path)
    }

    fn insert(&mut self, path: String, fwc: FileWithChunks) {
        self.0.insert(path, fwc);
    }

    pub fn contains_key(&self, path: &str) -> bool {
        self.0.contains_key(path)
    }

    pub fn into_iter(self) -> IntoIter<String, FileWithChunks> {
        self.0.into_iter()
    }

    pub fn values(&self) -> impl Iterator<Item = &FileWithChunks> {
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
    pub fn new(
        source_path: impl Into<PathBuf>,
        cache_paths: Vec<impl Into<PathBuf>>,
        hashing_algorithm: HashingAlgorithm,
        same_file_system: bool,
    ) -> Self {
        let source_path = source_path.into();

        let mut cache = DedupCache::new();

        let cache_path = {
            let mut cache_path = Default::default();
            for cache_path_from_iter in cache_paths.into_iter().rev() {
                cache_path = cache_path_from_iter.into();
                cache.read_from_file(&cache_path);
            }
            cache_path
        };

        cache = DedupCache::from_hashmap(
            cache
                .into_iter()
                .filter(|(path, _)| source_path.join(path).exists())
                .collect(),
        );

        let dir_walker = WalkDir::new(&source_path)
            .min_depth(1)
            .same_file_system(same_file_system);

        for entry in dir_walker {
            let entry = entry.unwrap().into_path();

            if !entry.is_file() {
                continue;
            }

            let fwc = FileWithChunks::try_new(&source_path, &entry, hashing_algorithm).unwrap();

            if let Some(fwc_cache) = cache.get_mut(&fwc.path) {
                if fwc == *fwc_cache {
                    fwc_cache.base = source_path.clone();
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
            "tmp.{}.{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            self.cache_path
                .extension()
                .unwrap_or("ext".as_ref())
                .to_str()
                .unwrap()
        ));
        self.cache.write_to_file(&temp_path);
        std::fs::rename(temp_path, &self.cache_path).unwrap();
    }

    pub fn write_chunks(
        &mut self,
        target_path: impl Into<PathBuf>,
        declutter_levels: usize,
    ) -> Result<()> {
        let target_path = target_path.into();
        let data_dir = target_path.join("data");
        std::fs::create_dir_all(&data_dir)?;
        for (_, chunk, _) in self.cache.get_chunks()? {
            let mut chunk_file = PathBuf::from(&chunk.hash);
            if declutter_levels > 0 {
                chunk_file = FileDeclutter::oneshot(chunk_file, declutter_levels);
            }
            chunk_file = data_dir.join(chunk_file);

            if !chunk_file.exists() {
                std::fs::create_dir_all(&chunk_file.parent().unwrap())?;
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
    pub fn new(source_path: impl Into<PathBuf>, cache_paths: Vec<impl Into<PathBuf>>) -> Self {
        let source_path = source_path.into();

        let mut cache = DedupCache::new();

        for cache_path in cache_paths.into_iter().rev() {
            let cache_path = cache_path.into();
            cache.read_from_file(&cache_path);
        }

        Self { source_path, cache }
    }

    pub fn restore_files(&self, target_path: impl Into<PathBuf>, declutter_levels: usize) {
        let data_dir = self.source_path.join("data");
        let target_path = target_path.into();
        std::fs::create_dir_all(&target_path).unwrap();
        for fwc in self.cache.values() {
            let target = target_path.join(&fwc.path);
            std::fs::create_dir_all(&target.parent().unwrap()).unwrap();
            let target_file = File::create(&target).unwrap();
            let mut target = BufWriter::new(&target_file);
            for chunk in fwc.get_chunks().unwrap() {
                let mut chunk_file = PathBuf::from(&chunk.hash);
                if declutter_levels > 0 {
                    chunk_file = FileDeclutter::oneshot(chunk_file, declutter_levels);
                }
                chunk_file = data_dir.join(chunk_file);

                let mut source = File::open(chunk_file).unwrap();
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
    use assert_fs::{NamedTempFile, TempDir};

    use super::*;

    #[test]
    fn compare_filechunk_objects() -> anyhow::Result<()> {
        let temp = TempDir::new()?;

        let file_1 = temp.child("file_1");
        std::fs::write(&file_1, "content_1")?;

        let file_2 = temp.child("file_2");
        std::fs::write(&file_2, "content_2")?;

        let fwc_1 = FileWithChunks::try_new(&temp.path(), &file_1.path(), HashingAlgorithm::MD5)?;
        let fwc_1_same =
            FileWithChunks::try_new(&temp.path(), &file_1.path(), HashingAlgorithm::MD5)?;
        let fwc_2 = FileWithChunks::try_new(&temp.path(), &file_2.path(), HashingAlgorithm::MD5)?;

        assert_eq!(fwc_1, fwc_1);
        assert_eq!(fwc_1, fwc_1_same);
        assert_ne!(fwc_1, fwc_2);

        File::open(&file_1)?.set_modified(SystemTime::now())?;

        let fwc_1_new =
            FileWithChunks::try_new(&temp.path(), &file_1.path(), HashingAlgorithm::MD5)?;

        assert_ne!(fwc_1, fwc_1_new);

        Ok(())
    }

    #[test]
    fn check_all_hashing_algorithms() -> anyhow::Result<()> {
        let algorithms = &[
            (HashingAlgorithm::MD5, "0fb073cd346f46f60c15e719f3820482"),
            (
                HashingAlgorithm::SHA1,
                "5503f5edc1bba66a7733c5ec38f4e9d449021be9",
            ),
            (
                HashingAlgorithm::SHA256,
                "e8c73ac958a87f17906b092bd99f37038788ee23b271574aad6d5bf1c76cc61c",
            ),
            (HashingAlgorithm::SHA512, "e6eda213df25f96ca380dd07640df530574e380c1b93d5d863fec05d5908a4880a3075fef4a438cfb1023cc51affb4624002f54b4790fe8362c7de032eb39aaa"),
        ];

        let temp = TempDir::new()?;
        let file = temp.child("file");
        std::fs::write(&file, "hello rust")?;

        for (algorithm, expected_hash) in algorithms.iter().copied() {
            let cache_file = NamedTempFile::new("cache.json")?;

            let chunks = Deduper::new(temp.path(), vec![cache_file.path()], algorithm, true)
                .cache
                .get_chunks()?
                .collect::<Vec<_>>();

            assert_eq!(chunks.len(), 1, "Too many chunks");

            let (hash, _, _) = &chunks[0];
            assert_eq!(
                hash, &expected_hash,
                "Algorithm {:?} does not produce expected hash",
                algorithm
            );
        }

        Ok(())
    }
}
