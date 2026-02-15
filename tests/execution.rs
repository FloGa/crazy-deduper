use std::fs;
use std::fs::OpenOptions;
use std::ops::Add;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use assert_cmd::Command;
use assert_fs::TempDir;
use assert_fs::fixture::ChildPath;
use assert_fs::prelude::*;
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

mod common;

fn fixture_impl(
    setup_origin: fn(&ChildPath) -> Result<()>,
    check_dedup: fn(&ChildPath) -> Result<()>,
    additional_args: Vec<impl Into<String>>,
    cache_file: PathBuf,
) -> Result<()> {
    let temp = TempDir::new()?;

    let additional_args = additional_args
        .into_iter()
        .map(Into::into)
        .collect::<Vec<_>>();

    let path_dedup = temp.child("dedup");
    let path_rehydrated = temp.child("rehydrate");

    let path_origin = temp.child("origin");
    path_origin.create_dir_all()?;

    setup_origin(&path_origin)?;

    let mut command = Command::new(&*common::BIN_PATH);
    command
        .arg(path_origin.path())
        .arg(path_dedup.path())
        .arg("--cache-file")
        .arg(cache_file.as_path())
        .arg("--hashing-algorithm")
        .arg("sha256");

    for arg in &additional_args {
        command.arg(arg);
    }

    command.assert().success();

    for entry in WalkDir::new(path_dedup.child("data"))
        .into_iter()
        .filter_entry(|f| f.file_type().is_file())
        .filter_map(|e| e.ok())
    {
        assert_eq!(
            entry.file_name().to_string_lossy(),
            base16ct::lower::encode_string(&Sha256::digest(&fs::read(entry.path())?))
        );
    }

    check_dedup(&path_dedup)?;

    let mut command = Command::new(&*common::BIN_PATH);
    command
        .arg(path_dedup.path())
        .arg(path_rehydrated.path())
        .arg("--cache-file")
        .arg(cache_file.as_path())
        .arg("-d");

    for arg in &additional_args {
        command.arg(arg);
    }

    command.assert().success();

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

fn fixture_with_cache_file(
    setup_origin: fn(&ChildPath) -> Result<()>,
    check_dedup: fn(&ChildPath) -> Result<()>,
    cache_file: PathBuf,
) -> Result<()> {
    fixture_impl(setup_origin, check_dedup, Vec::<String>::new(), cache_file)
}

fn fixture(
    setup_origin: fn(&ChildPath) -> Result<()>,
    check_dedup: fn(&ChildPath) -> Result<()>,
) -> Result<()> {
    let cache_file = TempDir::new()?.child("cache.json");
    fixture_with_cache_file(setup_origin, check_dedup, cache_file.to_path_buf())
}

fn fixture_with_additional_args(
    setup_origin: fn(&ChildPath) -> Result<()>,
    check_dedup: fn(&ChildPath) -> Result<()>,
    additional_args: Vec<impl Into<String>>,
) -> Result<()> {
    let cache_file = TempDir::new()?.child("cache.json");
    fixture_impl(
        setup_origin,
        check_dedup,
        additional_args,
        cache_file.to_path_buf(),
    )
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
fn empty_file() -> Result<()> {
    fn setup_origin(path_origin: &ChildPath) -> Result<()> {
        path_origin.child("empty").touch()?;
        Ok(())
    }

    fn check_dedup(path_dedup: &ChildPath) -> Result<()> {
        let items = fs::read_dir(path_dedup.child("data"))?
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(items.len(), 1);
        assert_eq!(items.first().unwrap().metadata()?.len(), 0);
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
        OpenOptions::new()
            .write(true)
            .open(&child)?
            .set_modified(SystemTime::UNIX_EPOCH)?;
        Ok(())
    }

    // Same size as 1, however with different timestamp
    fn setup_origin_2(path_origin: &ChildPath) -> Result<()> {
        let child = path_origin.child("file");
        fs::write(&child, "2")?;
        OpenOptions::new()
            .write(true)
            .open(&child)?
            .set_modified(SystemTime::UNIX_EPOCH.add(Duration::new(1, 0)))?;
        Ok(())
    }

    // Same timestamp as 2, however with different size
    fn setup_origin_12(path_origin: &ChildPath) -> Result<()> {
        let child = path_origin.child("file");
        fs::write(&child, "12")?;
        OpenOptions::new()
            .write(true)
            .open(&child)?
            .set_modified(SystemTime::UNIX_EPOCH.add(Duration::new(1, 0)))?;
        Ok(())
    }

    fn check_dedup(_path_dedup: &ChildPath) -> Result<()> {
        Ok(())
    }

    let cache_file = TempDir::new()?.child("cache.json");

    fixture_with_cache_file(setup_origin_1, check_dedup, cache_file.to_path_buf())?;
    fixture_with_cache_file(setup_origin_2, check_dedup, cache_file.to_path_buf())?;
    fixture_with_cache_file(setup_origin_12, check_dedup, cache_file.to_path_buf())?;

    let cache_file = TempDir::new()?.child("cache.json.zst");

    fixture_with_cache_file(setup_origin_1, check_dedup, cache_file.to_path_buf())?;

    Ok(())
}

#[test]
fn multiple_unrelated_sources_with_same_cache() -> Result<()> {
    fn setup_origin_1(path_origin: &ChildPath) -> Result<()> {
        let child = path_origin.child("file-1");
        fs::write(&child, "1")?;
        Ok(())
    }

    fn setup_origin_2(path_origin: &ChildPath) -> Result<()> {
        let child = path_origin.child("file-2");
        fs::write(&child, "2")?;
        Ok(())
    }

    fn check_dedup(_path_dedup: &ChildPath) -> Result<()> {
        Ok(())
    }

    let cache_file = TempDir::new()?.child("cache.json");

    fixture_with_cache_file(setup_origin_1, check_dedup, cache_file.to_path_buf())?;
    fixture_with_cache_file(setup_origin_2, check_dedup, cache_file.to_path_buf())?;

    Ok(())
}

#[test]
fn file_declutter() -> Result<()> {
    fn setup_origin(path_origin: &ChildPath) -> Result<()> {
        path_origin.child("empty").touch()?;
        Ok(())
    }

    fn check_dedup(path_dedup: &ChildPath) -> Result<()> {
        // There should be 3 levels of subdirectories inside data

        let mut dir;
        let mut dirs;

        dir = path_dedup.child("data").to_path_buf();

        dirs = fs::read_dir(dir)?.flatten().collect::<Vec<_>>();
        assert_eq!(dirs.len(), 1, "1");
        assert!(dirs.first().unwrap().file_type()?.is_dir());

        dir = dirs.first().unwrap().path();
        dirs = fs::read_dir(dir)?.flatten().collect::<Vec<_>>();
        assert_eq!(dirs.len(), 1);
        assert!(dirs.first().unwrap().file_type()?.is_dir());

        dir = dirs.first().unwrap().path();
        dirs = fs::read_dir(dir)?.flatten().collect::<Vec<_>>();
        assert_eq!(dirs.len(), 1);
        assert!(dirs.first().unwrap().file_type()?.is_dir());

        dir = dirs.first().unwrap().path();
        dirs = fs::read_dir(dir)?.flatten().collect::<Vec<_>>();
        assert_eq!(dirs.len(), 1);
        assert!(dirs.first().unwrap().file_type()?.is_file());

        Ok(())
    }

    fixture_with_additional_args(setup_origin, check_dedup, vec!["--declutter-levels", "3"])?;

    Ok(())
}

#[test]
fn turn_file_into_folder_same_cache() -> Result<()> {
    fn setup_origin_1(path_origin: &ChildPath) -> Result<()> {
        let child = path_origin.child("file");
        fs::write(&child, "1")?;
        Ok(())
    }

    // Turn file into a folder
    fn setup_origin_2(path_origin: &ChildPath) -> Result<()> {
        let child = path_origin.child("file");
        child.create_dir_all()?;
        fs::write(&child.child("file"), "1")?;
        Ok(())
    }

    // Turn folder into a file again
    fn setup_origin_3(path_origin: &ChildPath) -> Result<()> {
        let child = path_origin.child("file");
        fs::write(&child, "1")?;
        Ok(())
    }

    fn check_dedup(_path_dedup: &ChildPath) -> Result<()> {
        Ok(())
    }

    let cache_file = TempDir::new()?.child("cache.json");

    fixture_with_cache_file(setup_origin_1, check_dedup, cache_file.to_path_buf())?;
    fixture_with_cache_file(setup_origin_2, check_dedup, cache_file.to_path_buf())?;
    fixture_with_cache_file(setup_origin_3, check_dedup, cache_file.to_path_buf())?;

    Ok(())
}
