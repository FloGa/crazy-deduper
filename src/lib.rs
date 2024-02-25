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

#[derive(Debug)]
pub struct DedupFileChunk {
    pub offset: u64,
    pub size: usize,
    pub hash: String,
}

#[derive(Debug)]
pub struct DedupFile {
    pub source_path: PathBuf,
    pub target_path: Option<PathBuf>,
    pub chunks: Vec<DedupFileChunk>,
}

impl DedupFile {
    fn new_with_args(args: DedupItemArgs) -> Result<Self> {
        let mut chunks = Vec::new();

        let size = args.source_path.metadata()?.len();
        let input = BufReader::new(File::open(&args.source_path)?);
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

            if let Some(target) = &args.target_base {
                std::fs::write(target.join("data").join(&hash), &chunk)?;
            }

            let dedup_chunk = DedupFileChunk {
                offset: i * 1024 * 1024,
                size: chunk.len(),
                hash,
            };

            chunks.push(dedup_chunk);
        }

        if let Some(target) = &args.target_path {
            let contents = if chunks.is_empty() {
                Vec::new()
            } else {
                // Return compressed list of newline separated hashes.
                zstd::bulk::compress(
                    &chunks
                        .iter()
                        .map(|chunk| chunk.hash.clone())
                        .collect::<Vec<_>>()
                        .join("\n")
                        .into_bytes(),
                    0,
                )?
            };

            std::fs::write(&target, contents)?;
        }

        Ok(Self {
            source_path: args.source_path,
            target_path: args.target_path,
            chunks,
        })
    }

    pub fn new(path: PathBuf) -> Result<Self> {
        Self::new_with_args(DedupItemArgs::new(path))
    }
}

#[derive(Debug)]
pub struct DedupDir {
    pub source_path: PathBuf,
    pub target_path: Option<PathBuf>,
}

impl DedupDir {
    fn new(args: DedupItemArgs) -> Result<Self> {
        if let Some(target_path) = &args.target_path {
            std::fs::create_dir(target_path)?;
        }

        return Ok(Self {
            source_path: args.source_path,
            target_path: args.target_path,
        });
    }
}

#[derive(Debug)]
struct DedupItemArgs {
    source_path: PathBuf,
    target_base: Option<PathBuf>,
    target_path: Option<PathBuf>,
}

impl DedupItemArgs {
    fn new(source_path: PathBuf) -> Self {
        Self {
            source_path,
            target_base: None,
            target_path: None,
        }
    }

    fn new_with_base_paths(
        source_path: PathBuf,
        source_base: PathBuf,
        target_base: PathBuf,
    ) -> Self {
        let target_path = target_base.join("tree").join(
            source_path
                .components()
                .skip(source_base.components().count())
                .collect::<PathBuf>(),
        );

        Self {
            source_path,
            target_base: Some(target_base),
            target_path: Some(target_path),
        }
    }
}

#[derive(Debug)]
pub enum DedupItem {
    Dir(DedupDir),
    File(DedupFile),
    Unsupported(PathBuf),
}

impl DedupItem {
    fn new_with_args(args: DedupItemArgs) -> Result<Self> {
        let path = args.source_path.to_path_buf();

        let metadata = path.metadata()?.file_type();

        return if metadata.is_dir() {
            DedupDir::new(args).map(|d| DedupItem::Dir(d))
        } else if metadata.is_file() {
            DedupFile::new_with_args(args).map(|f| DedupItem::File(f))
        } else {
            Ok(DedupItem::Unsupported(path))
        };
    }

    pub fn new(source_path: PathBuf) -> Result<Self> {
        Self::new_with_args(DedupItemArgs::new(source_path))
    }
}

fn create_dedup_iter_impl<'a>(
    source: &'a PathBuf,
    target: Option<&'a PathBuf>,
) -> impl Iterator<Item = Result<DedupItem>> + 'a {
    WalkDir::new(source)
        .min_depth(1)
        .into_iter()
        .map(move |source_entry| {
            let source_entry = source_entry?;

            let args = if let Some(target) = target {
                DedupItemArgs::new_with_base_paths(
                    source_entry.into_path(),
                    source.to_path_buf(),
                    target.to_path_buf(),
                )
            } else {
                DedupItemArgs::new(source_entry.into_path())
            };

            DedupItem::new_with_args(args)
        })
}

pub fn create_dedup_iter<'a>(source: &'a PathBuf) -> impl Iterator<Item = Result<DedupItem>> + 'a {
    create_dedup_iter_impl(source, None)
}

pub fn create_dedup_iter_with_target<'a>(
    source: &'a PathBuf,
    target: &'a PathBuf,
) -> impl Iterator<Item = Result<DedupItem>> + 'a {
    create_dedup_iter_impl(source, Some(target))
}

pub fn populate(source: &PathBuf, target: &PathBuf) -> Result<()> {
    for result in create_dedup_iter_with_target(source, target) {
        if let Err(e) = result {
            return Err(e);
        }
    }

    Ok(())
}

pub fn hydrate(source: &PathBuf, target: &PathBuf) -> Result<()> {
    if target.exists() {
        return Err(Error::AlreadyExists(target.to_path_buf()));
    }

    std::fs::create_dir(target)?;

    for source_entry in WalkDir::new(source.join("tree")).min_depth(1) {
        let source_entry = source_entry?;

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

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use assert_fs::prelude::*;
    use assert_fs::TempDir;

    use super::*;

    #[test]
    fn small_files() -> Result<()> {
        let temp = TempDir::new()?;
        let source = temp.child("source");
        source.create_dir_all()?;

        let file_1 = source.child("file-1");
        let file_2 = source.child("file-2");
        let file_3 = source.child("file-3");

        std::fs::write(&file_1, "content")?;
        std::fs::write(&file_2, "content")?;
        std::fs::write(&file_3, "another content")?;

        let dedup_1 = DedupFile::new(file_1.to_path_buf())?;
        let dedup_2 = DedupFile::new(file_2.to_path_buf())?;
        let dedup_3 = DedupFile::new(file_3.to_path_buf())?;

        assert_eq!(dedup_1.chunks.len(), 1);
        assert_eq!(dedup_2.chunks.len(), 1);
        assert_eq!(dedup_3.chunks.len(), 1);

        assert_eq!(
            dedup_1.chunks.get(0).unwrap().hash,
            dedup_2.chunks.get(0).unwrap().hash
        );

        assert_ne!(
            dedup_1.chunks.get(0).unwrap().hash,
            dedup_3.chunks.get(0).unwrap().hash
        );

        Ok(())
    }

    #[test]
    fn iter_tree() -> Result<()> {
        let temp = TempDir::new()?;
        let source = temp.child("source");
        source.create_dir_all()?;

        let file_1 = source.child("file-1");
        let file_2 = {
            let subdir = source.child("subdir-1");
            subdir.create_dir_all()?;
            subdir.child("file-2")
        };
        let file_3 = {
            let subdir = source.child("subdir-2");
            subdir.create_dir_all()?;
            subdir.child("file-3")
        };

        let contents = "content";
        std::fs::write(&file_1, contents)?;
        std::fs::write(&file_2, contents)?;
        std::fs::write(&file_3, contents)?;

        let source_path = source.to_path_buf();
        let dedup_tree = create_dedup_iter(&source_path)
            .flatten()
            .collect::<Vec<_>>();
        let dedup_files = dedup_tree
            .iter()
            .filter_map(|d| {
                if let DedupItem::File(f) = d {
                    Some(f)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // 3 files + 2 dirs
        assert_eq!(dedup_tree.len(), 3 + 2);
        assert_eq!(dedup_files.len(), 3);

        let hashes = dedup_files
            .iter()
            .flat_map(|f| &f.chunks)
            .map(|c| c.hash.clone())
            .collect::<Vec<_>>();

        assert_eq!(hashes.iter().max(), hashes.iter().min());

        Ok(())
    }
}
