//! # Crazy Deduper
//!
//! [![badge github]][url github]
//! [![badge crates.io]][url crates.io]
//! [![badge docs.rs]][url docs.rs]
//! [![badge license]][url license]
//!
//! [//]: # (@formatter:off)
//! [badge github]: https://img.shields.io/badge/github-FloGa%2Fcrazy--deduper-green
//! [badge crates.io]: https://img.shields.io/crates/v/crazy-deduper
//! [badge docs.rs]: https://img.shields.io/docsrs/crazy-deduper
//! [badge license]: https://img.shields.io/crates/l/crazy-deduper
//!
//! [url github]: https://github.com/FloGa/crazy-deduper
//! [url crates.io]: https://crates.io/crates/crazy-deduper
//! [url docs.rs]: https://docs.rs/crazy-deduper
//! [url license]: https://github.com/FloGa/crazy-deduper/blob/develop/LICENSE
//! [//]: # (@formatter:on)
//!
//! > Deduplicates files into content-addressed chunks with selectable hash algorithms and restores them via a persistent
//! > cache.
//!
//! *Crazy Deduper* is a Rust tool that splits files into fixed-size chunks, identifies them using configurable hash
//! algorithms (MD5, SHA1, SHA256, SHA512), and deduplicates redundant data into a content-addressed store. It maintains an
//! incremental cache for speed, supports atomic cache updates, and can reverse the process (hydrate) to reconstruct
//! original files. Optional decluttering of chunk paths and filesystem boundary awareness make it flexible for real-world
//! workflows.
//!
//! This crate is split into an [Application](#application) part and a [Library](#library) part.
//!
//! ## Application
//!
//! ### Installation
//!
//! This tool can be installed easily through Cargo via `crates.io`:
//!
//! ```shell
//! cargo install --locked crazy-deduper
//! ```
//!
//! Please note that the `--locked` flag is necessary here to have the exact same dependencies as when the application was
//! tagged and tested. Without it, you might get more up-to-date versions of dependencies, but you have the risk of
//! undefined and unexpected behavior if the dependencies changed some functionalities. The application might even fail to
//! build if the public API of a dependency changed too much.
//!
//! Alternatively, pre-built binaries can be downloaded from the [GitHub releases][gh-releases] page.
//!
//! [gh-releases]: https://github.com/FloGa/crazy-deduper/releases
//!
//! ### Usage
//!
//! ```text
//! Usage: crazy-deduper [OPTIONS] <SOURCE> <TARGET>
//!
//! Arguments:
//!   <SOURCE>
//!           Source directory
//!
//!   <TARGET>
//!           Target directory
//!
//! Options:
//!       --cache-file <CACHE_FILE>
//!           Path to cache file
//!
//!           Can be used multiple times. The files are read in reverse order, so they should be sorted with the most accurate ones in the beginning. The first given will be written.
//!
//!       --hashing-algorithm <HASHING_ALGORITHM>
//!           Hashing algorithm to use for chunk filenames
//!
//!           [default: sha1]
//!           [possible values: md5, sha1, sha256, sha512]
//!
//!       --same-file-system
//!           Limit file listing to same file system
//!
//!       --declutter-levels <DECLUTTER_LEVELS>
//!           Declutter files into this many subdirectory levels
//!
//!           [default: 0]
//!
//!   -d, --decode
//!           Invert behavior, restore tree from deduplicated data
//!
//!           [aliases: --hydrate]
//!
//!   -h, --help
//!           Print help (see a summary with '-h')
//!
//!   -V, --version
//!           Print version
//! ```
//!
//! To create a deduped version of `source` directory to `deduped`, you can use:
//!
//! ```shell
//! crazy-deduper --declutter-levels 3 --cache-file cache.json.zst source deduped
//! ```
//!
//! If the cache file ends with `.zst`, it will be encoded (or decoded in the case of hydrating) using the ZSTD compression
//! algorithm. For any other extension, plain JSON will be used.
//!
//! To restore (hydrate) the directory again into the directory `hydrated`, you can use:
//!
//! ```shell
//! crazy-deduper --declutter-levels 3 --cache-file cache.json.zst deduped hydrated
//! ```
//!
//! Please note that for now you need to specify the same decluttering level as you did when deduping the source directory.
//! This limitation will be lifted in a future version.
//!
//! ### Cache Files
//!
//! The cache file is necessary to keep track of all file chunks and hashes. Without the cache you would not be able to
//! restore your files.
//!
//! The cache file can be re-used, even if the source directory changed. It keeps track of the file sizes and modification
//! times and only re-hashes new or changed files. Deleted files are deleted from the cache.
//!
//! You can also use older cache files in addition to a new one:
//!
//! ```shell
//! crazy-deduper --cache-file cache.json.zst --cache-file cache-from-yesterday.json.zst source deduped
//! ```
//!
//! The cache files are read in reverse order in which they are given on the command line, so the content of earlier cache
//! files is preferred over later ones. Hence, you should put your most accurate cache files to the beginning. Moreover, the
//! first given cache file is the one that will be written to, it does not need to exist.
//!
//! In the given example, if `cache.json.zst` does not exist, the internal cache is pre-filled from
//! `cache-from-yesterday.json.zst` so that only new and modified files need to be re-hashed. The result is then written
//! into `cache.json.zst`.
//!
//! ## Library
//!
//! ### Installation
//!
//! To add the `crazy-deduper` library to your project, you can use:
//!
//! ```shell
//! cargo add crazy-deduper
//! ```
//!
//! ### Usage
//!
//! The following is a short summary of how this library is intended to be used.
//!
//! #### Copy Chunks
//!
//! This is an example of how to re-create the main functionality of the [Application](#application).
//!
//! ```rust no_run
//! fn main() {
//!     // Deduplicate
//!     let mut deduper = crazy_deduper::Deduper::new(
//!         "source",
//!         vec!["cache.json.zst"],
//!         crazy_deduper::HashingAlgorithm::MD5,
//!         true,
//!     );
//!     deduper.write_chunks("deduped", 3).unwrap();
//!     deduper.write_cache();
//!
//!     // Hydrate again
//!     let hydrator = crazy_deduper::Hydrator::new("deduped", vec!["cache.json.zst"]);
//!     hydrator.restore_files("hydrated", 3);
//! }
//! ```
//!
//! #### Get File Chunks as an Iterator
//!
//! This method can be used if you want to implement your own logic and you only need the chunk objects.
//!
//! ```rust no_run
//! fn main() {
//!     let deduper = crazy_deduper::Deduper::new(
//!         "source",
//!         vec!["cache.json.zst"],
//!         crazy_deduper::HashingAlgorithm::MD5,
//!         true,
//!     );
//!
//!     for (hash, chunk, dirty) in deduper.cache.get_chunks().unwrap() {
//!         // Chunks and hashes are calculated on the fly, so you don't need to wait for the whole
//!         // directory tree to be hashed.
//!         println!("{hash:?}: {chunk:?}");
//!         if dirty {
//!             // This is just a simple example. Please do not write after every hash calculation, the
//!             // IO overhead will slow things down dramatically. You should write only every 10
//!             // seconds or so. Please be aware that you can kill the execution at any time. Since
//!             // the cache will be written atomically and re-used on subsequent calls, you can
//!             // terminate and resume at any point.
//!             deduper.write_cache();
//!         }
//!     }
//! }
//! ```

use std::cell::OnceCell;
use std::collections::hash_map::IntoIter;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use file_declutter::FileDeclutter;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use walkdir::WalkDir;

use crate::cache::CacheOnDisk;

mod cache;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

type Result<R> = std::result::Result<R, Error>;

#[cfg(unix)]
fn read_at_chunk(file: &File, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
    use std::os::unix::fs::FileExt;
    let mut buf = vec![0u8; len];
    let mut pos = 0usize;
    while pos < len {
        let read = file.read_at(&mut buf[pos..], offset + pos as u64)?;
        if read == 0 {
            break;
        }
        pos += read;
    }
    buf.truncate(pos);
    Ok(buf)
}

#[cfg(windows)]
fn read_at_chunk(file: &File, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
    use std::os::windows::fs::FileExt;
    // Create a clone of the file handle for parallel access.
    let mut handle = file.try_clone()?;
    let mut buf = vec![0u8; len];
    let mut pos = 0usize;
    while pos < len {
        let read = handle.seek_read(&mut buf[pos..], offset + pos as u64)?;
        if read == 0 {
            break;
        }
        pos += read;
    }
    buf.truncate(pos);
    Ok(buf)
}

/// Reads a cache file from the specified path and returns its content as a `String`.
///
/// This function can handle regular text files as well as compressed files with
/// a `.zst` extension (Zstandard-compressed files). If the file is compressed,
/// it will automatically decompress it before returning the content.
fn read_cache_file(path: &Path) -> std::io::Result<String> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let mut reader: Box<dyn Read> = if path.extension() == Some("zst".as_ref()) {
        let decoder = zstd::Decoder::with_buffer(reader)?;
        Box::new(decoder)
    } else {
        Box::new(reader)
    };

    let mut buffer = String::new();
    reader.read_to_string(&mut buffer)?;

    Ok(buffer)
}

/// A lazily initialized optional value that can be serialized/deserialized via `Option<T>`.
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

/// Supported hashing algorithms used to identify chunks.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum HashingAlgorithm {
    MD5,
    SHA1,
    SHA256,
    SHA512,
}

impl HashingAlgorithm {
    /// Returns a dynamically dispatched hasher instance corresponding to `self`.
    fn select_hasher(&self) -> Box<dyn sha2::digest::DynDigest> {
        match self {
            Self::MD5 => Box::new(md5::Md5::default()),
            Self::SHA1 => Box::new(sha1::Sha1::default()),
            Self::SHA256 => Box::new(sha2::Sha256::default()),
            Self::SHA512 => Box::new(sha2::Sha512::default()),
        }
    }
}

/// Represents a file in the source tree along with its chunked representation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FileWithChunks {
    #[serde(skip)]
    base: PathBuf,
    /// Path of the file relative to the source root.
    pub path: String,
    /// File size in bytes.
    pub size: u64,
    /// Modification time of the file.
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
    /// Creates a new instance by reading metadata from `path` under `source_path`.
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

    /// Returns already computed chunks if present.
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

    /// Returns existing chunks or computes them if absent.
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
            // Open file once and read it in parallel.
            let file = Arc::new(File::open(&path)?);
            let total_chunks = (size + chunk_size - 1) / chunk_size;

            (0..total_chunks)
                .into_par_iter()
                .map(|chunk_idx| {
                    let offset = chunk_idx * chunk_size;
                    let len = chunk_size.min(size.saturating_sub(offset)) as usize;

                    let data = read_at_chunk(&file, offset, len)?;

                    let mut hasher = hashing_algorithm.select_hasher();
                    hasher.update(&data);
                    let hash = hasher.finalize();
                    let hash = base16ct::lower::encode_string(&hash);

                    Ok::<FileChunk, Error>(FileChunk::new(offset, data.len() as u64, hash))
                })
                .collect()
        }
    }
}

/// A single chunk of a file, including its offset in the original file, size, and hash.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FileChunk {
    pub start: u64,
    pub size: u64,
    pub hash: String,
    #[serde(skip)]
    pub path: Option<String>,
}

impl FileChunk {
    /// Constructs a new `FileChunk`.
    pub fn new(start: u64, size: u64, hash: String) -> Self {
        Self {
            start,
            size,
            hash,
            path: None,
        }
    }
}

/// In-memory cache of `FileWithChunks` indexed by their relative paths.
pub struct DedupCache(HashMap<String, FileWithChunks>);

impl DedupCache {
    /// Creates an empty dedup cache.
    fn new() -> Self {
        Self(HashMap::new())
    }

    /// Constructs a cache from an existing hashmap.
    fn from_hashmap(hash_map: HashMap<String, FileWithChunks>) -> Self {
        Self(hash_map)
    }

    /// Reads cache entries from a file. Supports optional zstd compression based on extension.
    fn read_from_file(&mut self, path: impl AsRef<Path>) {
        let path = path.as_ref();

        let cache_from_file: CacheOnDisk = {
            let cache_from_file = read_cache_file(path);
            cache_from_file
                .ok()
                .and_then(|s| serde_json::from_str(s.as_str()).ok())
                .unwrap_or_default()
        };

        for x in cache_from_file.into_inner() {
            self.insert(x.path.clone(), x);
        }
    }

    /// Writes the cache to a file, optionally compressing with zstd if extension suggests.
    fn write_to_file(&self, path: impl AsRef<Path>) {
        let path = path.as_ref();

        if path.file_name().is_none() {
            return;
        }

        std::fs::create_dir_all(path.parent().unwrap()).unwrap();

        let writer = File::create(&path).map(BufWriter::new);

        if path.extension() == Some("zst".as_ref()) {
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

    /// Iterates over all chunks, yielding the chunk hash, enriched `FileChunk` with path, and a
    /// flag indicating if it was freshly calculated.
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

/// Primary deduper: scans a source directory, maintains a chunk cache, and writes deduplicated
/// chunk data to a target location.
pub struct Deduper {
    source_path: PathBuf,
    cache_path: PathBuf,
    pub cache: DedupCache,
}

impl Deduper {
    /// Initializes a new `Deduper`:
    /// - Loads provided cache files in reverse order (so later ones override earlier),
    /// - Prunes missing entries,
    /// - Scans the source tree and updates or inserts modified/new files.
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

    /// Atomically writes the internal cache back to its backing file.
    pub fn write_cache(&self) {
        if self.cache_path.file_name().is_none() {
            return;
        }

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

    /// Writes all chunks from the current cache to `target_path/data`, applying optional
    /// decluttering (path splitting) to reduce directory entropy.
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
                let mut src = BufReader::new(File::open(
                    self.source_path.join(chunk.path.as_ref().unwrap()),
                )?);
                src.seek(SeekFrom::Start(chunk.start))?;
                let mut limited = src.take(chunk.size);
                std::io::copy(&mut limited, &mut out)?;
            }
        }

        Ok(())
    }
}

/// Rebuilds original files from deduplicated chunk storage using a cache.
pub struct Hydrator {
    source_path: PathBuf,
    pub cache: DedupCache,
}

impl Hydrator {
    /// Loads the cache(s) and prepares for hydration.
    pub fn new(source_path: impl Into<PathBuf>, cache_paths: Vec<impl Into<PathBuf>>) -> Self {
        let source_path = source_path.into();

        let mut cache = DedupCache::new();

        for cache_path in cache_paths.into_iter().rev() {
            let cache_path = cache_path.into();
            cache.read_from_file(&cache_path);
        }

        Self { source_path, cache }
    }

    /// Restores files into `target_path` by concatenating their chunks. `declutter_levels` must
    /// match the level used during deduplication.
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

    /// Check if all chunk files listed in the cache are present in source directory.
    pub fn check_cache(&self, declutter_levels: usize) -> bool {
        let mut success = true;

        let path_data = self.source_path.join("data");
        for (hash, meta) in self
            .cache
            .get_chunks()
            .unwrap()
            .map(|(hash, meta, ..)| (PathBuf::from(hash), meta))
        {
            let path = path_data.join(FileDeclutter::oneshot(hash, declutter_levels));

            if !path.exists() {
                eprintln!("Does not exist: {}", path.display());
                success = false;
                continue;
            }

            if path.metadata().unwrap().len() != meta.size {
                eprintln!(
                    "Does not have expected size of {}: {}",
                    meta.size,
                    path.display()
                );
                success = false;
                continue;
            }
        }

        success
    }

    /// List files in source directory that are not listed in cache.
    pub fn list_extra_files(&self, declutter_levels: usize) -> impl Iterator<Item = PathBuf> {
        let files_in_cache = FileDeclutter::new_from_iter(
            self.cache
                .get_chunks()
                .unwrap()
                .map(|(hash, ..)| PathBuf::from(hash)),
        )
        .base(&self.source_path.join("data"))
        .levels(declutter_levels)
        .map(|(_, path)| path)
        .collect::<HashSet<_>>();

        WalkDir::new(&self.source_path.join("data"))
            .min_depth(1)
            .same_file_system(false)
            .into_iter()
            .filter(move |entry| {
                entry
                    .as_ref()
                    .map(|entry| {
                        entry.file_type().is_file() && !files_in_cache.contains(entry.path())
                    })
                    .unwrap_or_default()
            })
            .flatten()
            .map(|entry| entry.into_path())
    }

    /// Delete files in source directory that are not listed in cache.
    pub fn delete_extra_files(&self, declutter_levels: usize) -> anyhow::Result<()> {
        for path in self.list_extra_files(declutter_levels) {
            std::fs::remove_file(&path)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use assert_fs::fixture::ChildPath;
    use assert_fs::prelude::*;
    use assert_fs::{NamedTempFile, TempDir};

    use super::*;

    fn setup() -> anyhow::Result<(TempDir, ChildPath, ChildPath, ChildPath)> {
        let temp = TempDir::new()?;

        let origin = temp.child("origin");
        origin.create_dir_all()?;
        origin.child("README.md").write_str("Hello, world!")?;

        let deduped = temp.child("deduped");
        deduped.create_dir_all()?;

        let cache = temp.child("cache.json");

        {
            let mut deduper = Deduper::new(
                origin.to_path_buf(),
                vec![cache.to_path_buf()],
                HashingAlgorithm::MD5,
                true,
            );
            deduper.write_chunks(deduped.to_path_buf(), 3)?;
            deduper.write_cache();
        }

        Ok((temp, origin, deduped, cache))
    }

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

        // Some operating systems like Windows need write access to the file to be able to set the
        // modified time.
        OpenOptions::new()
            .write(true)
            .open(&file_1)?
            .set_modified(SystemTime::now())?;

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
            (
                HashingAlgorithm::SHA512,
                "e6eda213df25f96ca380dd07640df530574e380c1b93d5d863fec05d5908a4880a3075fef4a438cfb1023cc51affb4624002f54b4790fe8362c7de032eb39aaa",
            ),
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

    #[test]
    fn check_cache() -> anyhow::Result<()> {
        let (_temp, _origin, deduped, cache) = setup()?;

        assert!(
            Hydrator::new(deduped.to_path_buf(), vec![cache.to_path_buf()]).check_cache(3),
            "Cache checking failed when it shouldn't"
        );

        std::fs::remove_dir_all(deduped.child("data").read_dir()?.next().unwrap()?.path())?;

        assert!(
            !Hydrator::new(deduped.to_path_buf(), vec![cache.to_path_buf()]).check_cache(3),
            "Cache checking didn't fail when it should"
        );

        Ok(())
    }

    #[test]
    fn check_list_extra() -> anyhow::Result<()> {
        let (_temp, _origin, deduped, cache) = setup()?;

        assert_eq!(
            Hydrator::new(deduped.to_path_buf(), vec![cache.to_path_buf()])
                .list_extra_files(3)
                .count(),
            0,
            "Extra files present when there shouldn't be"
        );

        deduped
            .child("data")
            .child("extra_file")
            .write_str("Hello, world!")?;

        assert_eq!(
            Hydrator::new(deduped.to_path_buf(), vec![cache.to_path_buf()])
                .list_extra_files(3)
                .count(),
            1,
            "Number of extra files present is not 1"
        );

        deduped
            .child("data")
            .child("e")
            .child("x")
            .child("t")
            .child("extra_file")
            .write_str("Hello, world!")?;

        assert_eq!(
            Hydrator::new(deduped.to_path_buf(), vec![cache.to_path_buf()])
                .list_extra_files(3)
                .count(),
            2,
            "Number of extra files present is not 2"
        );

        Ok(())
    }
}
