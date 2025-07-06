use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use crazy_deduper::{Deduper, HashingAlgorithm, Hydrator};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Source directory
    source: PathBuf,

    /// Target directory
    target: PathBuf,

    /// Path to cache file
    #[arg(long)]
    cache_file: PathBuf,

    /// Hashing algorithm to use for chunk filenames
    #[arg(long, value_enum, default_value_t = HashingAlgorithmArgument::SHA1)]
    hashing_algorithm: HashingAlgorithmArgument,

    /// Invert behavior, restore tree from deduplicated data
    #[arg(long, short, visible_alias = "hydrate")]
    decode: bool,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, ValueEnum)]
pub enum HashingAlgorithmArgument {
    MD5,
    SHA1,
    SHA256,
    SHA512,
}

impl From<HashingAlgorithmArgument> for HashingAlgorithm {
    fn from(value: HashingAlgorithmArgument) -> Self {
        match value {
            HashingAlgorithmArgument::MD5 => HashingAlgorithm::MD5,
            HashingAlgorithmArgument::SHA1 => HashingAlgorithm::SHA1,
            HashingAlgorithmArgument::SHA256 => HashingAlgorithm::SHA256,
            HashingAlgorithmArgument::SHA512 => HashingAlgorithm::SHA512,
        }
    }
}

fn main() -> Result<()> {
    let args = Cli::parse();

    let source = args.source;
    let target = args.target;
    let cache_file = args.cache_file;

    if !args.decode {
        let mut deduper = Deduper::new(source, cache_file, args.hashing_algorithm.into());
        deduper.write_chunks(target)?;
        deduper.write_cache();
    } else {
        let hydrator = Hydrator::new(source, cache_file);
        hydrator.restore_files(target);
    }

    Ok(())
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
