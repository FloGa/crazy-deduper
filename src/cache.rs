use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{DedupCache, FileWithChunks};

mod v0;
mod v1;
use v1 as latest;

/// Reads a cache file from the specified path and returns its content as a `String`.
///
/// This function can handle regular text files as well as compressed files with
/// a `.zst` extension (Zstandard-compressed files). If the file is compressed,
/// it will automatically decompress it before returning the content.
fn read_cache_file(path: &Path) -> std::io::Result<String> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let mut reader: Box<dyn Read> = if path.extension() == Some("zst".as_ref()) {
        let decoder = zstd::Decoder::with_buffer(reader)?;
        Box::new(decoder)
    } else {
        Box::new(reader)
    };

    let mut buffer = String::new();
    reader.read_to_string(&mut buffer)?;

    Ok(buffer)
}

/// Creates a cache writer for the specified path.
///
/// This function creates a writer that writes to the given path. If the file extension of the path
/// is `.zst`, the writer will compress the data using Zstandard compression. Otherwise, it will
/// use a buffered writer without compression.
fn get_cache_writer(path: &Path) -> std::io::Result<Box<dyn Write>> {
    let file = File::create(&path)?;
    let writer = BufWriter::new(file);

    Ok(if path.extension() == Some("zst".as_ref()) {
        let encoder = zstd::Encoder::new(writer, 0)?.auto_finish();
        Box::new(encoder)
    } else {
        Box::new(writer)
    })
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "v")]
enum CacheOnDisk<'a> {
    #[serde(rename = "1")]
    V1 {
        #[serde(borrow)]
        c: v1::CacheOnDisk<'a>,
    },
    #[serde(untagged, skip_serializing)]
    V0(v0::CacheOnDisk<'a>),
}

impl<'a> CacheOnDisk<'a> {
    fn parse(json: &'a str) -> serde_json::Result<CacheOnDisk<'a>> {
        serde_json::from_str(json)
    }

    fn migrate(self) -> Option<Self> {
        match self {
            CacheOnDisk::V0(v0) => Some(CacheOnDisk::V1 { c: v0.into() }),
            CacheOnDisk::V1 { .. } => None,
        }
    }

    fn into_latest(self) -> latest::CacheOnDisk<'a> {
        if let CacheOnDisk::V1 { c: cache } = self {
            cache
        } else {
            // We are checking for the latest, so we can safely unwrap.
            self.migrate().unwrap().into_latest()
        }
    }
}

pub(crate) fn read_from_file(path: impl AsRef<Path>) -> Vec<FileWithChunks> {
    let path = path.as_ref();

    let cache_from_file = read_cache_file(path);
    cache_from_file
        .ok()
        .and_then(|s| {
            CacheOnDisk::parse(&s)
                .map(CacheOnDisk::into_latest)
                .map(latest::CacheOnDisk::into_owned)
                .ok()
        })
        .unwrap_or_default()
}

pub(crate) fn write_to_file(path: impl AsRef<Path>, cache: &DedupCache) {
    let path = path.as_ref();

    if path.file_name().is_none() {
        return;
    }

    std::fs::create_dir_all(path.parent().unwrap()).unwrap();

    let writer = get_cache_writer(&path);

    let versioned_cache = CacheOnDisk::V1 {
        c: latest::CacheOnDisk::from(cache),
    };

    writer
        .map(|writer| serde_json::to_writer(writer, &versioned_cache))
        .unwrap()
        .unwrap();
}
