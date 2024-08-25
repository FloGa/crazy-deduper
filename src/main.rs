use anyhow::Result;
use clap::Parser;
use crazy_deduper::{Deduper, Hydrator};
use std::path::PathBuf;

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

    /// Invert behavior, restore tree from deduplicated data
    #[arg(long, short, visible_alias = "hydrate")]
    decode: bool,
}

fn main() -> Result<()> {
    let args = Cli::parse();

    let source = args.source;
    let target = args.target;
    let cache_file = args.cache_file;

    if !args.decode {
        let deduper = Deduper::new(source, cache_file);
        deduper.write_chunks(target);
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
