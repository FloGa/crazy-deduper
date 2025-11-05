use std::path::PathBuf;

use anyhow::Result;
use assert_fs::TempDir;
use assert_fs::prelude::*;
use crazy_deduper::{Deduper, HashingAlgorithm};

#[test]
fn check_public_properties() -> Result<()> {
    let temp = TempDir::new()?;
    let source = temp.child("source");
    source.create_dir_all()?;

    let cache_file = temp.child("cache.json");

    let file = source.child("file");
    std::fs::write(&file, "content")?;

    let source_path = source.to_path_buf();
    let mut deduper = Deduper::new(
        source_path,
        vec![cache_file.path()],
        HashingAlgorithm::MD5,
        true,
    );

    let cache = &mut deduper.cache;
    assert_eq!(cache.len(), 1, "Expected file count is not 1");

    let fcw = cache.values().next().unwrap();
    assert_eq!(PathBuf::from(&fcw.path), file.strip_prefix(&source)?);

    let chunks = fcw.get_or_calculate_chunks()?;
    assert_eq!(chunks.len(), 1, "Number of chunks is not 1");

    let chunk = chunks.get(0).unwrap();
    assert_ne!(chunk.size, 0, "Chunk size is 0");
    assert_ne!(chunk.hash, "", "Chunk hash is empty");

    Ok(())
}
