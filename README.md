# Crazy Deduper

[![badge github]][url github]
[![badge crates.io]][url crates.io]
[![badge docs.rs]][url docs.rs]
[![badge license]][url license]

[//]: # (@formatter:off)
[badge github]: https://img.shields.io/badge/github-FloGa%2Fcrazy--deduper-green
[badge crates.io]: https://img.shields.io/crates/v/crazy-deduper
[badge docs.rs]: https://img.shields.io/docsrs/crazy-deduper
[badge license]: https://img.shields.io/crates/l/crazy-deduper

[url github]: https://github.com/FloGa/crazy-deduper
[url crates.io]: https://crates.io/crates/crazy-deduper
[url docs.rs]: https://docs.rs/crazy-deduper
[url license]: https://github.com/FloGa/crazy-deduper/blob/develop/LICENSE
[//]: # (@formatter:on)

> Deduplicates files into content-addressed chunks with selectable hash algorithms and restores them via a persistent
> cache.

*Crazy Deduper* is a Rust tool that splits files into fixed-size chunks, identifies them using configurable hash
algorithms (MD5, SHA1, SHA256, SHA512), and deduplicates redundant data into a content-addressed store. It maintains an
incremental cache for speed, supports atomic cache updates, and can reverse the process (hydrate) to reconstruct
original files. Optional decluttering of chunk paths and filesystem boundary awareness make it flexible for real-world
workflows.

This crate is split into an [Application](#application) part and a [Library](#library) part.

## Application

### Installation

This tool can be installed easily through Cargo via `crates.io`:

```shell
cargo install --locked crazy-deduper
```

Please note that the `--locked` flag is necessary here to have the exact same dependencies as when the application was
tagged and tested. Without it, you might get more up-to-date versions of dependencies, but you have the risk of
undefined and unexpected behavior if the dependencies changed some functionalities. The application might even fail to
build if the public API of a dependency changed too much.

Alternatively, pre-built binaries can be downloaded from the [GitHub releases][gh-releases] page.

[gh-releases]: https://github.com/FloGa/crazy-deduper/releases

### Usage

<!--% !cargo --quiet run -- --help | tail -n+3 %-->

```text
Usage: crazy-deduper [OPTIONS] <SOURCE> <TARGET>

Arguments:
  <SOURCE>
          Source directory

  <TARGET>
          Target directory

Options:
      --cache-file <CACHE_FILE>
          Path to cache file
          
          Can be used multiple times. The files are read in reverse order, so they should be sorted with the most accurate ones in the beginning. The first given will be written.

      --hashing-algorithm <HASHING_ALGORITHM>
          Hashing algorithm to use for chunk filenames
          
          [default: sha1]
          [possible values: md5, sha1, sha256, sha512]

      --same-file-system
          Limit file listing to same file system

      --declutter-levels <DECLUTTER_LEVELS>
          Declutter files into this many subdirectory levels
          
          [default: 0]

  -d, --decode
          Invert behavior, restore tree from deduplicated data
          
          [aliases: --hydrate]

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

To create a deduped version of `source` directory to `deduped`, you can use:

```shell
crazy-deduper --declutter-levels 3 --cache-file cache.json.zst source deduped
```

If the cache file ends with `.zst`, it will be encoded (or decoded in the case of hydrating) using the ZSTD compression
algorithm. For any other extension, plain JSON will be used.

To restore (hydrate) the directory again into the directory `hydrated`, you can use:

```shell
crazy-deduper --declutter-levels 3 --cache-file cache.json.zst deduped hydrated
```

Please note that for now you need to specify the same decluttering level as you did when deduping the source directory.
This limitation will be lifted in a future version.

### Cache Files

The cache file is necessary to keep track of all file chunks and hashes. Without the cache you would not be able to
restore your files.

The cache file can be re-used, even if the source directory changed. It keeps track of the file sizes and modification
times and only re-hashes new or changed files. Deleted files are deleted from the cache.

You can also use older cache files in addition to a new one:

```shell
crazy-deduper --cache-file cache.json.zst --cache-file cache-from-yesterday.json.zst source deduped
```

The cache files are read in reverse order in which they are given on the command line, so the content of earlier cache
files is preferred over later ones. Hence, you should put your most accurate cache files to the beginning. Moreover, the
first given cache file is the one that will be written to, it does not need to exist.

In the given example, if `cache.json.zst` does not exist, the internal cache is pre-filled from
`cache-from-yesterday.json.zst` so that only new and modified files need to be re-hashed. The result is then written
into `cache.json.zst`.

## Library

### Installation

To add the `crazy-deduper` library to your project, you can use:

```shell
cargo add crazy-deduper
```

### Usage

The following is a short summary of how this library is intended to be used.

#### Copy Chunks

This is an example of how to re-create the main functionality of the [Application](#application).

```rust no_run
fn main() {
    // Deduplicate
    let mut deduper = crazy_deduper::Deduper::new(
        "source",
        vec!["cache.json.zst"],
        crazy_deduper::HashingAlgorithm::MD5,
        true,
    );
    deduper.write_chunks("deduped", 3).unwrap();
    deduper.write_cache();

    // Hydrate again
    let hydrator = crazy_deduper::Hydrator::new("deduped", vec!["cache.json.zst"]);
    hydrator.restore_files("hydrated", 3);
}
```

#### Get File Chunks as an Iterator

This method can be used if you want to implement your own logic and you only need the chunk objects.

```rust no_run
fn main() {
    let deduper = crazy_deduper::Deduper::new(
        "source",
        vec!["cache.json.zst"],
        crazy_deduper::HashingAlgorithm::MD5,
        true,
    );

    for (hash, chunk, dirty) in deduper.cache.get_chunks().unwrap() {
        // Chunks and hashes are calculated on the fly, so you don't need to wait for the whole
        // directory tree to be hashed.
        println!("{hash:?}: {chunk:?}");
        if dirty {
            // This is just a simple example. Please do not write after every hash calculation, the
            // IO overhead will slow things down dramatically. You should write only every 10
            // seconds or so. Please be aware that you can kill the execution at any time. Since
            // the cache will be written atomically and re-used on subsequent calls, you can
            // terminate and resume at any point.
            deduper.write_cache();
        }
    }
}
```
