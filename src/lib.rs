use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use anyhow::{bail, Result};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

pub fn ensure_init(base: &PathBuf) -> Result<()> {
    if base.exists() {
        bail!("'{}' already exists!", base.display());
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
        bail!("'{}' not properly initialized!", base.display());
    }

    Ok(())
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

        let metadata = source_entry.metadata()?;
        if metadata.is_dir() {
            std::fs::create_dir(target_path)?;
        } else if metadata.is_file() {
            let size = metadata.len();
            let contents = if size == 0 {
                Vec::new()
            } else {
                let mut hashes = Vec::new();
                let input = BufReader::new(File::open(source_entry.path())?);
                let mut bytes = input.bytes();

                // Process file in MiB chunks.
                for _ in (0..).take_while(|i| i * 1024 * 1024 < size) {
                    let chunk = bytes
                        .by_ref()
                        .take(1024 * 1024)
                        .flatten()
                        .collect::<Vec<_>>();

                    let mut hasher = Sha256::new();
                    hasher.update(&chunk);
                    let hash = hasher.finalize();
                    let hash = base16ct::lower::encode_string(&hash);

                    std::fs::write(target.join("data").join(&hash), &chunk)?;
                    hashes.push(hash);
                }

                // Return compressed list of newline separated hashes.
                zstd::bulk::compress(&hashes.join("\n").into_bytes(), 0)?
            };

            std::fs::write(&target_path, contents)?;
        }
    }

    Ok(())
}

pub fn hydrate(source: &PathBuf, target: &PathBuf) -> Result<()> {
    if target.exists() {
        bail!("'{}' already exists!", target.display());
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
