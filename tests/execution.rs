use std::fs;
use std::fs::File;
use std::ops::Add;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use assert_cmd::Command;
use assert_fs::fixture::ChildPath;
use assert_fs::prelude::*;
use assert_fs::TempDir;
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

mod common;

fn fixture_with_cache_file(
    setup_origin: fn(&ChildPath) -> Result<()>,
    check_dedup: fn(&ChildPath) -> Result<()>,
    cache_file: PathBuf,
) -> Result<()> {
    let temp = TempDir::new()?;

    let path_dedup = temp.child("dedup");
    let path_rehydrated = temp.child("rehydrate");

    let path_origin = temp.child("origin");
    path_origin.create_dir_all()?;

    setup_origin(&path_origin)?;

    Command::new(&*common::BIN_PATH)
        .arg(path_origin.path())
        .arg(path_dedup.path())
        .arg("--cache-file")
        .arg(cache_file.as_path())
        .arg("--hashing-algorithm")
        .arg("sha256")
        .assert()
        .success();

    for entry in fs::read_dir(path_dedup.child("data"))? {
        let entry = entry?;
        assert_eq!(
            entry.file_name().to_string_lossy(),
            base16ct::lower::encode_string(&Sha256::digest(&fs::read(entry.path())?))
        );
    }

    check_dedup(&path_dedup)?;

    Command::new(&*common::BIN_PATH)
        .arg(path_dedup.path())
        .arg(path_rehydrated.path())
        .arg("--cache-file")
        .arg(cache_file.as_path())
        .arg("-d")
        .assert()
        .success();

    // Check if we can replicate the original directory.
    let files_origin = WalkDir::new(&path_origin).min_depth(1).sort_by_file_name();
    let files_rehydrated = WalkDir::new(&path_rehydrated)
        .min_depth(1)
        .sort_by_file_name();

    for (file_origin, file_rehydrated) in files_origin.into_iter().zip(files_rehydrated.into_iter())
    {
        let file_origin = file_origin?;
        let file_rehydrated = file_rehydrated?;

        assert_eq!(file_origin.depth(), file_rehydrated.depth());
        assert_eq!(file_origin.file_type(), file_rehydrated.file_type());
        assert_eq!(file_origin.file_name(), file_rehydrated.file_name());

        if file_origin.file_type().is_file() {
            assert_eq!(
                file_origin.metadata()?.modified()?,
                file_rehydrated.metadata()?.modified()?
            );
            assert_eq!(
                fs::read(file_origin.path())?,
                fs::read(file_rehydrated.path())?
            );
        }
    }

    Ok(())
}

fn fixture(
    setup_origin: fn(&ChildPath) -> Result<()>,
    check_dedup: fn(&ChildPath) -> Result<()>,
) -> Result<()> {
    let cache_file = TempDir::new()?.child("cache.json");
    fixture_with_cache_file(setup_origin, check_dedup, cache_file.to_path_buf())
}

#[test]
fn empty_dir() -> Result<()> {
    fn setup_origin(_path_origin: &ChildPath) -> Result<()> {
        Ok(())
    }

    fn check_dedup(path_dedup: &ChildPath) -> Result<()> {
        assert_eq!(fs::read_dir(path_dedup.child("data"))?.count(), 0);
        Ok(())
    }

    fixture(setup_origin, check_dedup)?;

    Ok(())
}

#[test]
fn small_files() -> Result<()> {
    fn setup_origin(path_origin: &ChildPath) -> Result<()> {
        for subdir in 0..=1 {
            let subdir = path_origin.child(format!("subdir-{subdir}"));
            subdir.create_dir_all()?;
            for file in 0..=4 {
                fs::write(
                    subdir.child(format!("file-{file}")),
                    format!("Content {file}"),
                )?;
            }
        }

        Ok(())
    }

    fn check_dedup(path_dedup: &ChildPath) -> Result<()> {
        // Each 2 of the 10 files are identical, so there are 5 deduplicated data blocks.
        assert_eq!(fs::read_dir(path_dedup.child("data"))?.count(), 5);

        Ok(())
    }

    fixture(setup_origin, check_dedup)?;

    Ok(())
}

#[test]
fn big_files() -> Result<()> {
    fn setup_origin(path_origin: &ChildPath) -> Result<()> {
        let mut bytes = (0..u8::MAX).cycle();
        for file in 0..=1 {
            fs::write(
                path_origin.child(format!("file-{file}")),
                bytes.by_ref().take(1500 * 1024).collect::<Vec<_>>(),
            )?;
        }

        fs::copy(path_origin.child("file-0"), path_origin.child("file-9"))?;

        Ok(())
    }

    fn check_dedup(path_dedup: &ChildPath) -> Result<()> {
        // Each of the 2 files have 2 data blocks. File-9 is completely deduplicated.
        assert_eq!(fs::read_dir(path_dedup.child("data"))?.count(), 4);

        Ok(())
    }

    fixture(setup_origin, check_dedup)?;

    Ok(())
}

#[test]
fn exact_1mb_files() -> Result<()> {
    fn setup_origin(path_origin: &ChildPath) -> Result<()> {
        let mut bytes = (0..u8::MAX).cycle();
        for file in 0..=1 {
            fs::write(
                path_origin.child(format!("file-{file}")),
                bytes.by_ref().take(1024 * 1024).collect::<Vec<_>>(),
            )?;
        }

        fs::copy(path_origin.child("file-0"), path_origin.child("file-9"))?;

        Ok(())
    }

    fn check_dedup(path_dedup: &ChildPath) -> Result<()> {
        // Each of the 2 files have exactly 1 data block. File-9 is completely deduplicated.
        assert_eq!(fs::read_dir(path_dedup.child("data"))?.count(), 2);

        Ok(())
    }

    fixture(setup_origin, check_dedup)?;

    Ok(())
}

#[test]
fn modified_files_same_cache() -> Result<()> {
    fn setup_origin_1(path_origin: &ChildPath) -> Result<()> {
        let child = path_origin.child("file");
        fs::write(&child, "1")?;
        File::open(&child)?.set_modified(SystemTime::UNIX_EPOCH)?;
        Ok(())
    }

    // Same size as 1, however with different timestamp
    fn setup_origin_2(path_origin: &ChildPath) -> Result<()> {
        let child = path_origin.child("file");
        fs::write(&child, "2")?;
        File::open(&child)?.set_modified(SystemTime::UNIX_EPOCH.add(Duration::new(1, 0)))?;
        Ok(())
    }

    // Same timestamp as 2, however with different size
    fn setup_origin_12(path_origin: &ChildPath) -> Result<()> {
        let child = path_origin.child("file");
        fs::write(&child, "12")?;
        File::open(&child)?.set_modified(SystemTime::UNIX_EPOCH.add(Duration::new(1, 0)))?;
        Ok(())
    }

    fn check_dedup(_path_dedup: &ChildPath) -> Result<()> {
        Ok(())
    }

    let cache_file = TempDir::new()?.child("cache.json");

    fixture_with_cache_file(setup_origin_1, check_dedup, cache_file.to_path_buf())?;
    fixture_with_cache_file(setup_origin_2, check_dedup, cache_file.to_path_buf())?;
    fixture_with_cache_file(setup_origin_12, check_dedup, cache_file.to_path_buf())?;

    Ok(())
}
