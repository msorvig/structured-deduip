use crossbeam::atomic::AtomicCell;
use filesystemtable::{FsTable, FsIngester};
use rayon::iter::{ParallelBridge, ParallelIterator};
use rayon::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::convert::TryFrom;
use std::fmt;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::{cmp::Ordering, fs::File, io, iter::Scan};
use thiserror::Error;

pub struct DedupBuilder {
    root: PathBuf,
    digest_file: Option<PathBuf>,
}

impl DedupBuilder {
    pub fn new<P: AsRef<Path>>(root: P) -> DedupBuilder {
        DedupBuilder {
            root: root.as_ref().into(),
            digest_file: None,
        }
    }

    pub fn with_digest_file<P: AsRef<Path>>(&mut self, digest_file: P) -> &mut Self {
        self.digest_file = Some(digest_file.as_ref().into());
        self
    }

    pub fn build(&self) -> Dedup {
        // Get file system table - either from a provided table file,
        // or by scanning the root path
        let stored_table = self.digest_file.as_ref().and_then(|path| {
            match load_entries_from_file(&path) {
                Ok(entries) => Some(entries),
                Err(err) => {
                    // log errroer
                    // delete file - we can't read it so it may be corrupted
                    None
                }
            }
        });
        let table = match stored_table {
            Some(table) => table,
            None => {
                let entries = FsIngester::new(&self.root).ingest();
                match self.digest_file.as_ref() {
                    Some(path) => {
                        let _res = save_entries_to_file(path, &entries);
                    }
                    None => {}
                }
                entries
            }
        };

        Dedup {
            root: self.root.clone(),
            digest_file: self.digest_file.clone(),
            table,
        }
    }
}

pub struct Dedup {
    root: PathBuf,
    digest_file: Option<PathBuf>,
    table: FsTable
}

impl Dedup {
    fn scan<P: AsRef<Path>>(root: P) -> FsTable {
        FsIngester::new(root.as_ref()).ingest()
    }

    pub fn scan_additional<P: AsRef<Path>>(&mut self, dir: P) {
        // TODO verify dir is subdir of root
        self.table.extend_at(&Dedup::scan(dir), "");
    }

    pub fn dedup(&mut self) {}

    pub fn dedup_additional<P: AsRef<Path>>(&mut self, dir: P) {
        let _entries = Dedup::scan(dir);
    }

    pub fn stats(&self) {
        let mut size = 0u64;
        for entry in self.table.iter_files() {
            size += entry.size();
        }

        println!("file count {}", self.table.len());
        println!("totl size  {}", size);
    }

    pub fn stats_marginal<P: AsRef<Path>>(&self, dir: P) {}
}

#[derive(Error, Debug)]
enum EntriesFileError {
    #[error("file io error")]
    FileIo(#[from] io::Error),
    #[error("data format error")]
    DataFormat(#[from] Box<bincode::ErrorKind>),
}

fn load_entries_from_file(path: &Path) -> Result<FsTable, EntriesFileError> {
    let compressed_bytes = std::fs::read(path)?;
    let bytes = zstd::stream::decode_all(&*compressed_bytes)?;
    let entries = bincode::deserialize(&bytes)?;
    Ok(entries)
}

fn save_entries_to_file(path: &Path, entries: &FsTable) -> Result<(), EntriesFileError> {
    let bytes = bincode::serialize(entries)?;
    let compressed_bytes = zstd::stream::encode_all(&*bytes, 0)?;
    std::fs::write(path, &compressed_bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let cwd = std::env::current_dir().unwrap();

        let mut dedup_1 = DedupBuilder::new(&cwd)
            .with_digest_file("/tmp/dedup_digest1")
            .build();

        let src_dir = cwd.join("src");
        dedup_1.dedup();
        dedup_1.dedup_additional(&src_dir);
        dedup_1.stats();
        dedup_1.stats_marginal(&src_dir);
    }

    #[test]
    fn qtcode() {
        let cwd = std::env::current_dir().unwrap();

        let dedup = DedupBuilder::new("/home/msorvig/code/qtbuilder-data2/sources/")
            .with_digest_file("/tmp/dedup_digest2")
            .build();
    }
}
