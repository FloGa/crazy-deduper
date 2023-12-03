use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

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
}

type Result<R> = std::result::Result<R, Error>;

pub fn ensure_init(base: &PathBuf) -> Result<()> {
    if base.exists() {
        return Err(Error::AlreadyExists(base.to_path_buf()));
    }

    std::fs::create_dir(base)?;
    std::fs::create_dir(base.join("config"))?;
    std::fs::create_dir(base.join("data"))?;
    std::fs::create_dir(base.join("tree"))?;

    Ok(())
}

pub fn check_init(base: &PathBuf) -> Result<()> {
    let all_exist = base.exists()
        && base.join("config").exists()
        && base.join("data").exists()
        && base.join("tree").exists();

    if !all_exist {
        return Err(Error::NotInitialized(base.to_path_buf()));
    }

    Ok(())
}

pub struct DedupFileChunk {
    pub offset: u64,
    pub size: usize,
    pub hash: String,
}

pub struct DedupFile {
    pub path: PathBuf,
    pub chunks: Vec<DedupFileChunk>,
}

impl DedupFile {
    fn new_impl(path: PathBuf, target: Option<PathBuf>) -> Result<Self> {
        let mut chunks = Vec::new();

        let size = path.metadata()?.len();
        let input = BufReader::new(File::open(&path)?);
        let mut bytes = input.bytes();

        // Process file in MiB chunks.
        for i in (0..).take_while(|i| i * 1024 * 1024 < size) {
            let chunk = bytes
                .by_ref()
                .take(1024 * 1024)
                .flatten()
                .collect::<Vec<_>>();

            let mut hasher = Sha256::new();
            hasher.update(&chunk);
            let hash = hasher.finalize();
            let hash = base16ct::lower::encode_string(&hash);

            if let Some(target) = &target {
                std::fs::write(target.join("data").join(&hash), &chunk)?;
            }

            let dedup_chunk = DedupFileChunk {
                offset: i * 1024 * 1024,
                size: chunk.len(),
                hash,
            };

            chunks.push(dedup_chunk);
        }

        Ok(Self { path, chunks })
    }

    pub fn new(path: PathBuf) -> Result<Self> {
        Self::new_impl(path, None)
    }

    pub fn new_with_target(path: PathBuf, target: PathBuf) -> Result<Self> {
        Self::new_impl(path, Some(target))
    }
}

pub enum DedupItem {
    Dir(PathBuf),
    File(DedupFile),
    Unsupported(PathBuf),
}

pub struct DedupItemBuilder {
    source: PathBuf,
    target: Option<PathBuf>,
}

impl DedupItemBuilder {
    pub fn new(source: PathBuf) -> Self {
        Self {
            source,
            target: None,
        }
    }

    pub fn with_target(mut self, target: PathBuf) -> Self {
        self.target = Some(target);
        self
    }

    pub fn build(self) -> Result<DedupItem> {
        let path = self.source;

        let metadata = path.metadata()?.file_type();

        if metadata.is_dir() {
            return Ok(DedupItem::Dir(path));
        }

        if !metadata.is_file() {
            return Ok(DedupItem::Unsupported(path));
        }

        DedupFile::new_impl(path, self.target).map(|f| DedupItem::File(f))
    }
}

pub fn populate(source: &PathBuf, target: &PathBuf) -> Result<()> {
    for source_entry in WalkDir::new(source) {
        let source_entry = source_entry?;

        if source_entry.depth() == 0 {
            continue;
        }

        let target_path = target.join("tree").join(
            source_entry
                .path()
                .components()
                .skip(source.components().count())
                .collect::<PathBuf>(),
        );

        let dedup_item = DedupItemBuilder::new(source_entry.into_path())
            .with_target(target.to_path_buf())
            .build()?;

        match dedup_item {
            DedupItem::Dir(_) => {
                std::fs::create_dir(target_path)?;
            }
            DedupItem::Unsupported(_) => {}
            DedupItem::File(dedup_file) => {
                let contents = if dedup_file.chunks.is_empty() {
                    Vec::new()
                } else {
                    let hashes = dedup_file
                        .chunks
                        .iter()
                        .map(|dc| dc.hash.clone())
                        .collect::<Vec<_>>();

                    // Return compressed list of newline separated hashes.
                    zstd::bulk::compress(&hashes.join("\n").into_bytes(), 0)?
                };

                std::fs::write(&target_path, contents)?;
            }
        }
    }

    Ok(())
}

pub fn hydrate(source: &PathBuf, target: &PathBuf) -> Result<()> {
    if target.exists() {
        return Err(Error::AlreadyExists(target.to_path_buf()));
    }

    std::fs::create_dir(target)?;

    for source_entry in WalkDir::new(source.join("tree")) {
        let source_entry = source_entry?;

        if source_entry.depth() == 0 {
            continue;
        }

        let target_path = target.join(
            source_entry
                .path()
                .components()
                .skip(source.join("tree").components().count())
                .collect::<PathBuf>(),
        );

        let metadata = source_entry.metadata()?;
        if metadata.is_dir() {
            std::fs::create_dir(target_path)?;
        } else if metadata.is_file() {
            let contents = if metadata.len() > 0 {
                let input = File::open(source_entry.path())?;
                zstd::stream::decode_all(input)?
            } else {
                Vec::new()
            };

            let mut out = BufWriter::new(File::create(&target_path)?);
            if metadata.len() > 0 {
                for hash in String::from_utf8(contents)?.split("\n") {
                    out.write_all(
                        &BufReader::new(File::open(source.join("data").join(hash))?)
                            .bytes()
                            .flatten()
                            .collect::<Vec<_>>(),
                    )?;
                }
            }
        }
    }

    Ok(())
}
