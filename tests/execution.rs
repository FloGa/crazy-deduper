use std::fs;

use anyhow::Result;
use assert_cmd::Command;
use assert_fs::fixture::ChildPath;
use assert_fs::prelude::*;
use assert_fs::TempDir;

mod common;

fn fixture(
    setup_origin: fn(&ChildPath) -> Result<()>,
    check_dedup: fn(&ChildPath) -> Result<()>,
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
        .assert()
        .success();

    check_dedup(&path_dedup)?;

    Command::new(&*common::BIN_PATH)
        .arg(path_dedup.path())
        .arg(path_rehydrated.path())
        .arg("-d")
        .assert()
        .success();

    // Check if we can replicate the original directory.
    assert!(!dir_diff::is_different(path_origin, path_rehydrated).unwrap());

    Ok(())
}

#[test]
fn empty_dir() -> Result<()> {
    fn setup_origin(_path_origin: &ChildPath) -> Result<()> {
        Ok(())
    }

    fn check_dedup(path_dedup: &ChildPath) -> Result<()> {
        assert_eq!(fs::read_dir(path_dedup.child("data"))?.count(), 0);
        assert_eq!(fs::read_dir(path_dedup.child("tree"))?.count(), 0);
        Ok(())
    }

    fixture(setup_origin, check_dedup)?;

    Ok(())
}

#[test]
fn empty_subdirs() -> Result<()> {
    fn setup_origin(path_origin: &ChildPath) -> Result<()> {
        path_origin.child("subdir1").create_dir_all()?;
        path_origin.child("subdir2").create_dir_all()?;
        Ok(())
    }

    fn check_dedup(path_dedup: &ChildPath) -> Result<()> {
        assert_eq!(fs::read_dir(path_dedup.child("data"))?.count(), 0);
        assert_eq!(fs::read_dir(path_dedup.child("tree"))?.count(), 2);
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
        assert_eq!(fs::read_dir(path_dedup.child("tree"))?.count(), 2);
        assert_eq!(
            fs::read_dir(path_dedup.child("tree").child("subdir-0"))?.count(),
            5
        );
        assert_eq!(
            fs::read_dir(path_dedup.child("tree").child("subdir-1"))?.count(),
            5
        );

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
        assert_eq!(fs::read_dir(path_dedup.child("tree"))?.count(), 3);

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
        assert_eq!(fs::read_dir(path_dedup.child("tree"))?.count(), 3);

        Ok(())
    }

    fixture(setup_origin, check_dedup)?;

    Ok(())
}
