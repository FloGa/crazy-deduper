use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use crazy_deduper::{check_init, ensure_init, hydrate, populate};

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

fn main() -> Result<()> {
    let args = Cli::parse();

    let source = args.source;
    let target = args.target;

    if !args.decode {
        ensure_init(&target)?;
        populate(&source, &target)?;
    } else {
        check_init(&source)?;
        hydrate(&source, &target)?;
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
