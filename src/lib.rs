use crossbeam::atomic::AtomicCell;
use jwalk::{DirEntry, WalkDir};
use rayon::iter::{ParallelBridge, ParallelIterator};
use rayon::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::convert::TryFrom;
use std::fmt;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::{cmp::Ordering, fs::File, io, iter::Scan};
use thiserror::Error;

mod filesystemtable;
mod pathstore;

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
        // Get file system entries - either from a provided entries file,
        // or by scanning the root path
        let stored_entries = self.digest_file.as_ref().and_then(|path| {
            match load_entries_from_file(&path) {
                Ok(entries) => Some(entries),
                Err(err) => {
                    // log errroer
                    // delete file - we can't read it so it may be corrupted
                    None
                }
            }
        });
        let entries = match stored_entries {
            Some(entries) => entries,
            None => {
                let entries = Dedup::scan(&self.root);
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
            entries,
        }
    }
}

pub struct Dedup {
    root: PathBuf,
    digest_file: Option<PathBuf>,
    entries: Vec<FileEntry>, // sorted
}

impl Dedup {
    fn scan<P: AsRef<Path>>(root: P) -> Vec<FileEntry> {
        let entries_it = WalkDir::new(&root)
            .follow_links(false)
            .sort(true)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|item| item.file_type().is_file())
            .filter_map(|item| FileEntry::from_jwalk_entry(&item))
            .par_bridge()
            .map(|item| {
                item.digest.store(compute_file_digest(&item.path));
                item
            });

        let mut entries: Vec<FileEntry> = entries_it.collect();
        entries.par_sort();
        entries.dedup(); // note: deups the _entry_ list

        println!("scanned {} files", entries.len());
        entries
    }

    pub fn scan_additional<P: AsRef<Path>>(&mut self, dir: P) {
        self.entries.extend(Dedup::scan(dir));
        self.entries.par_sort();
        self.entries.dedup();
    }

    pub fn dedup(&mut self) {}

    pub fn dedup_additional<P: AsRef<Path>>(&mut self, dir: P) {
        let _entries = Dedup::scan(dir);
    }

    pub fn stats(&self) {
        let mut size = 0u64;
        for entry in &self.entries {
            size += entry.len;
        }

        println!("file count {}", self.entries.len());
        println!("totl size  {}", size);
    }

    pub fn stats_marginal<P: AsRef<Path>>(&self, dir: P) {}
}

fn compute_digest(input: &[u8]) -> u128 {
    let mut buffer: [u8; 16] = [0; 16];
    blake3::Hasher::new()
        .update(input)
        .finalize_xof()
        .fill(&mut buffer);
    u128::from_ne_bytes(buffer)
}

fn compute_file_digest(path: &Path) -> Option<u128> {
    match std::fs::File::open(path) {
        Ok(mut file) => {
            let mut data = Vec::new();
            let _ = file.read_to_end(&mut data);
            Some(compute_digest(&data))
        }
        Err(e) => {
            panic!("Error opening {:?} {:?}", path, e);
        }
    }
}

#[derive(Error, Debug)]
enum EntriesFileError {
    #[error("file io error")]
    FileIo(#[from] io::Error),
    #[error("data format error")]
    DataFormat(#[from] Box<bincode::ErrorKind>),
}

fn load_entries_from_file(path: &Path) -> Result<Vec<FileEntry>, EntriesFileError> {
    let compressed_bytes = std::fs::read(path)?;
    let bytes = zstd::stream::decode_all(&*compressed_bytes)?;
    let entries = bincode::deserialize(&bytes)?;
    Ok(entries)
}

fn save_entries_to_file(path: &Path, entries: &Vec<FileEntry>) -> Result<(), EntriesFileError> {
    let bytes = bincode::serialize(&entries)?;
    let compressed_bytes = zstd::stream::encode_all(&*bytes, 0)?;
    std::fs::write(path, &compressed_bytes)?;
    Ok(())
}

type JWalkDirEntry = DirEntry<((), ())>;

// Looks like crossbeam::AtomicCell does not implement
// Eq, Ord, Serialize, etc. Create newtype wich does that
// here.
struct AtomicCellU128(AtomicCell<Option<u128>>);

impl AtomicCellU128 {
    fn new(opt: Option<u128>) -> AtomicCellU128 {
        AtomicCellU128(AtomicCell::new(opt))
    }

    fn load(&self) -> Option<u128> {
        self.0.load()
    }

    fn store(&self, val: Option<u128>) {
        self.0.store(val)
    }
}

impl PartialEq for AtomicCellU128 {
    fn eq(&self, other: &Self) -> bool {
        self.0.load() == other.0.load()
    }
}

impl Eq for AtomicCellU128 {}

impl Ord for AtomicCellU128 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.load().cmp(&other.0.load())
    }
}

impl PartialOrd for AtomicCellU128 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for AtomicCellU128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FusedAtomicOptionU128")
            .field("val", &self.0.load())
            .finish()
    }
}

impl Serialize for AtomicCellU128 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.load().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AtomicCellU128 {
    fn deserialize<D>(deserializer: D) -> Result<AtomicCellU128, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(AtomicCellU128(AtomicCell::new(
            Option::<u128>::deserialize(deserializer)?,
        )))
    }
}

#[derive(Serialize, Deserialize, Debug, Eq)]
struct FileEntry {
    name: String,
    path: PathBuf,
    len: u64,
    digest: AtomicCellU128,
}

impl FileEntry {
    fn from_jwalk_entry(dir_entry: &JWalkDirEntry) -> Option<FileEntry> {
        // Skip files with non-unicode names
        let file_name = match dir_entry.file_name().to_str() {
            Some(name) => name,
            None => return None,
        };

        // Skip files with inaccessible metadata
        let metadata = match dir_entry.metadata() {
            Ok(data) => data,
            Err(_) => return None,
        };

        Some(FileEntry {
            name: file_name.to_string(),
            path: dir_entry.path(),
            len: metadata.len(),
            digest: AtomicCellU128::new(None),
        })
    }

    fn load_digest(&self) -> u128 {
        match self.digest.load() {
            Some(digest) => digest,
            None => {
                let digest = compute_file_digest(&self.path);
                self.digest.store(digest);
                digest.unwrap()
            }
        }
    }
}

impl PartialEq for FileEntry {
    fn eq(&self, other: &Self) -> bool {
        self.load_digest() == other.load_digest()
    }
}

impl Ord for FileEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        let digest_ordering = self.load_digest().cmp(&other.load_digest());
        match digest_ordering {
            Ordering::Equal => self.path.cmp(&other.path),
            _ => digest_ordering,
        }
    }
}

impl PartialOrd for FileEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TryFrom<JWalkDirEntry> for FileEntry {
    type Error = ();

    fn try_from(dir_entry: JWalkDirEntry) -> Result<Self, Self::Error> {
        match FileEntry::from_jwalk_entry(&dir_entry) {
            Some(entry) => Ok(entry),
            None => Err(()),
        }
    }
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
