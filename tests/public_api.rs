use anyhow::{bail, Result};
use assert_fs::prelude::*;
use assert_fs::TempDir;

use crazy_deduper::{create_dedup_iter, DedupItem};

#[test]
fn check_public_properties() -> Result<()> {
    let temp = TempDir::new()?;
    let source = temp.child("source");
    source.create_dir_all()?;

    let file = source.child("file");
    std::fs::write(&file, "content")?;

    let source_path = source.to_path_buf();
    let mut dedup_tree = create_dedup_iter(&source_path);
    let DedupItem::File(dedup_file) = dedup_tree.next().unwrap()? else {
        bail!("not a file")
    };
    assert_eq!(dedup_file.source_path, file.to_path_buf());

    let chunks = dedup_file.chunks;
    assert_eq!(chunks.len(), 1);

    let chunk = chunks.get(0).unwrap();
    assert_eq!(chunk.offset, 0);
    assert_ne!(chunk.size, 0);
    assert_ne!(chunk.hash, "");

    Ok(())
}
