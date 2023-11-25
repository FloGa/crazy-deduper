use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use sha2::{Digest, Sha256};
use walkdir::WalkDir;

fn ensure_init(base: &PathBuf) {
    if base.exists() {
        panic!("'{}' already exists!", base.display());
    }

    std::fs::create_dir(base).unwrap();
    std::fs::create_dir(base.join("config")).unwrap();
    std::fs::create_dir(base.join("data")).unwrap();
    std::fs::create_dir(base.join("tree")).unwrap();
}

fn check_init(base: &PathBuf) {
    let all_exist = base.exists()
        && base.join("config").exists()
        && base.join("data").exists()
        && base.join("tree").exists();

    if !all_exist {
        panic!("'{}' not properly initialized!", base.display());
    }
}

fn populate(source: &PathBuf, target: &PathBuf) {
    for source_entry in WalkDir::new(source) {
        let source_entry = source_entry.unwrap();

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

        let metadata = source_entry.metadata().unwrap();
        if metadata.is_dir() {
            std::fs::create_dir(target_path).unwrap();
        } else if metadata.is_file() {
            let input = BufReader::new(File::open(source_entry.path()).unwrap());
            let mut hashes = Vec::new();
            let mut bytes = input.bytes();
            loop {
                let chunk = bytes
                    .by_ref()
                    .take(1024 * 1024)
                    .flatten()
                    .collect::<Vec<_>>();
                if chunk.is_empty() {
                    break;
                }
                let mut hasher = Sha256::new();
                hasher.update(&chunk);
                let hash = hasher.finalize();
                let hash = base16ct::lower::encode_string(&hash);
                std::fs::write(target.join("data").join(&hash), &chunk).unwrap();
                hashes.push(hash);
                if chunk.len() != 1024 * 1024 {
                    break;
                }
            }
            let contents = hashes.join("\n").into_bytes();
            let contents = if contents.is_empty() {
                contents
            } else {
                zstd::bulk::compress(&contents, 0).unwrap()
            };
            std::fs::write(&target_path, contents).unwrap();
        }
    }
}

fn hydrate(source: &PathBuf, target: &PathBuf) {
    if target.exists() {
        panic!("'{}' already exists!", target.display());
    }

    std::fs::create_dir(target).unwrap();

    for source_entry in WalkDir::new(source.join("tree")) {
        let source_entry = source_entry.unwrap();

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

        let metadata = source_entry.metadata().unwrap();
        if metadata.is_dir() {
            std::fs::create_dir(target_path).unwrap();
        } else if metadata.is_file() {
            let contents = if metadata.len() > 0 {
                let input = File::open(source_entry.path()).unwrap();
                zstd::stream::decode_all(input).unwrap()
            } else {
                Vec::new()
            };

            let mut out = BufWriter::new(File::create(&target_path).unwrap());
            if metadata.len() > 0 {
                for hash in String::from_utf8(contents).unwrap().split("\n") {
                    out.write_all(
                        &BufReader::new(File::open(source.join("data").join(hash)).unwrap())
                            .bytes()
                            .flatten()
                            .collect::<Vec<_>>(),
                    )
                    .unwrap();
                }
            }
        }
    }
}

fn main() {
    let source = PathBuf::from(std::env::args_os().nth(1).unwrap());
    let target = PathBuf::from(std::env::args_os().nth(2).unwrap());
    let should_hydrate = std::env::args_os().nth(3).map_or(false, |arg| arg == "-d");

    if !should_hydrate {
        ensure_init(&target);
        populate(&source, &target);
    } else {
        check_init(&source);
        hydrate(&source, &target);
    }
}
