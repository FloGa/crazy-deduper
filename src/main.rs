use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use clap::Parser;
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Source directory
    source: PathBuf,

    /// Target directory
    target: PathBuf,

    /// Invert behavior, restore tree from deduplicated data
    #[arg(long, short, visible_alias = "hydrate")]
    decode: bool,
}

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
            let size = metadata.len();
            let contents = if size == 0 {
                Vec::new()
            } else {
                let mut hashes = Vec::new();
                let input = BufReader::new(File::open(source_entry.path()).unwrap());
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

                    std::fs::write(target.join("data").join(&hash), &chunk).unwrap();
                    hashes.push(hash);
                }

                // Return compressed list of newline separated hashes.
                zstd::bulk::compress(&hashes.join("\n").into_bytes(), 0).unwrap()
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
    let args = Cli::parse();

    let source = args.source;
    let target = args.target;

    if !args.decode {
        ensure_init(&target);
        populate(&source, &target);
    } else {
        check_init(&source);
        hydrate(&source, &target);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_cli() {
        use clap::CommandFactory;
        Cli::command().debug_assert()
    }
}
