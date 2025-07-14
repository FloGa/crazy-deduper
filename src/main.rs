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

    /// Path to cache files
    ///
    /// The files are read in reverse order, so they should be sorted with the most accurate ones
    /// in the beginning. The first given will be written.
    #[arg(long)]
    #[arg(alias = "cache-file")]
    cache_files: Vec<PathBuf>,

    /// Hashing algorithm to use for chunk filenames
    #[arg(long, value_enum, default_value_t = HashingAlgorithmArgument::SHA1)]
    hashing_algorithm: HashingAlgorithmArgument,

    /// Limit file listing to same file system
    #[arg(long)]
    same_file_system: bool,

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
    let cache_files = args.cache_files;
    let same_file_system = args.same_file_system;

    if !args.decode {
        let mut deduper = Deduper::new(
            source,
            cache_files,
            args.hashing_algorithm.into(),
            same_file_system,
        );
        deduper.write_chunks(target)?;
        deduper.write_cache();
    } else {
        let hydrator = Hydrator::new(source, cache_files);
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
