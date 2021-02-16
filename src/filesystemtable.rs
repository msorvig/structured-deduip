use jwalk::{DirEntry, WalkDir};
use log::warn;
use rayon::{
    iter::{IntoParallelRefIterator, IntoParallelRefMutIterator, ParallelBridge, ParallelIterator},
    slice::{Iter, ParallelSliceMut},
};
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    cmp::Ordering,
    fmt, fs,
    io::Read,
    num::NonZeroU128,
    os::unix::prelude::MetadataExt,
    path::{Path, PathBuf},
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::pathstore::pathstore::PathStore;

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct Flags {
    pub is_dir: bool,
    pub dotfile: bool,
    pub symlink: bool,
    pub readable: bool,
    pub writable: bool,
    pub executable: bool,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct Entry {
    pub path: u32,
    pub size: u64,
    pub flags: Flags,
    pub content: Content,
    pub digest: Option<NonZeroU128>,
}

// Per-entry content
//
// Several types of content is supported:
//  None:   No content
//  Bytes:  Inline stored content, with u32 ref to content start
//  Digest: Address of some externally stored content.
#[derive(Serialize, Deserialize, Debug)]
pub enum Content {
    None,
    Bytes(u32),
    Digest(u32),
}

impl Default for Content {
    fn default() -> Self {
        Content::None
    }
}
#[derive(Serialize, Deserialize)]
pub struct Table {
    pub entries: Vec<Entry>,
    pub paths: PathStore,
    pub content: Vec<u8>,
}

pub struct Ingester<'a> {
    table: RefCell<&'a mut Table>,
    src: PathBuf,
    dst: Option<PathBuf>,
    create_directory_entries: bool,
    ingest_file_content: bool,
    compute_digests: bool,
}

impl Ingester<'_> {
    pub fn new<P: AsRef<Path>>(table: &mut Table, src: P) -> Ingester {
        Ingester {
            table: RefCell::new(table),
            src: src.as_ref().to_owned(),
            dst: None,
            create_directory_entries: false,
            ingest_file_content: false,
            compute_digests: false,
        }
    }

    pub fn into_dst<P: AsRef<Path>>(&mut self, dst: P) -> &mut Self {
        self.dst = Some(dst.as_ref().to_owned());
        self
    }

    pub fn create_directory_entries(&mut self, enable: bool) -> &mut Self {
        self.create_directory_entries = enable;
        self
    }

    pub fn ingest_file_content(&mut self, enable: bool) -> &mut Self {
        self.ingest_file_content = enable;
        self
    }

    pub fn compute_digests(&mut self, enable: bool) -> &mut Self {
        self.compute_digests = enable;
        self
    }

    pub fn ingest(&mut self) {
        Table::do_ingest(self);
    }
}

impl Table {
    pub fn new() -> Table {
        Table {
            entries: Vec::new(),
            paths: PathStore::new(),
            content: Vec::new(),
        }
    }

    pub fn ingester<P: AsRef<Path>>(&mut self, src: P) -> Ingester {
        Ingester::new(self, src)
    }

    pub fn do_ingest(ingester: &mut Ingester) {
        let mut table_ref = ingester.table.borrow_mut();
        let table: &mut Table = &mut table_ref;

        let paths = &mut table.paths;
        let src = &ingester.src;

        let entries_it = WalkDir::new(src)
            .follow_links(false)
            .sort(true)
            .into_iter()
            .filter_map(Result::ok)
            .map(|entry| {
                let stripped = entry.path().strip_prefix(src).unwrap().to_owned();
                let path_index = paths.add_path(&stripped);
                let mut flags: Flags = Default::default();
                let mut size = 0;
                match entry.metadata() {
                    Ok(meta) => {
                        flags.is_dir = meta.is_dir();
                        size = meta.size();
                    }
                    Err(err) => {
                        warn!("No metadata for  {:?}", entry.path()) // ### when can this happen?
                    }
                }

                Entry {
                    path: path_index,
                    size: size,
                    flags: flags,
                    content: Content::None,
                    digest: None,
                }
            })
            .filter(|entry| -> bool {
                let is_file = !entry.flags.is_dir;
                is_file || ingester.create_directory_entries
            });

        let mut new_entries = entries_it.collect::<Vec<_>>();

        // load file content if requested
        let compute_digest = ingester.compute_digests;
        if ingester.ingest_file_content {
            let mut table_content = Arc::new(Mutex::new(&mut table.content));
            new_entries.par_iter_mut().for_each(|entry| {
                let full_path = src.join(paths.get_path(entry.path));
                match fs::read(full_path) {
                    Ok(file_contents) => {
                        // Insert file content
                        let mut guard = table_content.lock().unwrap();
                        let index = guard.len() as u32;
                        entry.content = Content::Bytes(index);
                        guard.extend(file_contents.iter());

                        // Compute the digest right now while we have the file contents
                        if compute_digest {
                            entry.digest = Some(Table::compute_content_digest(&file_contents));
                        }
                    }
                    Err(err) => {
                        warn!("Could not load content {:?}", err);
                    }
                }
            });
        }

        // compute digest if requested
        if !ingester.ingest_file_content && ingester.compute_digests {
            new_entries.par_iter_mut().for_each(|entry| {
                let full_path = src.join(paths.get_path(entry.path));
                entry.digest = Table::compute_file_digest(&full_path);
            });
        }

        // Finally extend the table. Make sure the entries are sorted and dedupe'd
        table.entries.extend(new_entries.into_iter());
        {
            let paths = &mut table.paths;
            table
                .entries
                .par_sort_unstable_by(|a, b| paths.cmp_paths(a.path, b.path));
            table.entries.dedup_by(|a, b| a.path == b.path)
        }

        // TODO: finally finally, recompute digests and sizes for directory entries, if present
    }

    fn compute_content_digest(input: &[u8]) -> NonZeroU128 {
        let mut buffer: [u8; 16] = [0; 16];
        blake3::Hasher::new()
            .update(input)
            .finalize_xof()
            .fill(&mut buffer);

        let digest = u128::from_ne_bytes(buffer);

        // Brazenly assume the digest can't be 0 and return a NonZeroU128
        assert_ne!(digest, 0);
        unsafe { NonZeroU128::new_unchecked(digest) }
    }

    fn compute_file_digest(path: &Path) -> Option<NonZeroU128> {
        match std::fs::File::open(path) {
            Ok(mut file) => {
                let mut data = Vec::new();
                let _ = file.read_to_end(&mut data);
                Some(Table::compute_content_digest(&data))
            }
            Err(e) => {
                panic!("Error opening {:?} {:?}", path, e);
            }
        }
    }

    fn compute_entry_digest(entry: &mut Entry, paths: &PathStore, base_path: &Path) {
        if entry.digest.is_some() {
            return;
        }
        let path = base_path.join(paths.get_path(entry.path));
        entry.digest = Table::compute_file_digest(&path);
    }

    pub fn compute_all_digests(&mut self, base_path: &Path) {
        let paths = &self.paths;
        self.entries.par_iter_mut().for_each(|entry| {
            Table::compute_entry_digest(entry, paths, base_path);
        });
    }
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table")
            .field("file count", &self.entries.len())
            .field("entries", &self.entries)
            .finish()
    }
}

// TableEntry holds a reference to a table and an inner Entry
// (both may be required to resolve some proerties, such as
// the path)
pub struct TableEntry<'a> {
    table: &'a Table,
    entry: &'a Entry,
}

impl TableEntry<'_> {
    pub fn new<'a>(table: &'a Table, entry: &'a Entry) -> TableEntry<'a> {
        TableEntry { table, entry }
    }
    pub fn path(&self) -> PathBuf {
        self.table.paths.get_path(self.entry.path)
    }

    pub fn size(&self) -> u64 {
        self.entry.size
    }

    pub fn flags(&self) -> &Flags {
        &self.entry.flags
    }

    pub fn content(&self) -> &Content {
        &self.entry.content
    }

    pub fn contained_content(&self) -> Option<&[u8]> {
        match self.entry.content {
            Content::Bytes(index) => {
                let ind = index as usize;
                let siz = self.entry.size as usize; // TODO make sure u32 size is enforced
                Some(&self.table.content[ind..siz])
            }
            _ => None,
        }
    }

    pub fn digest(&self) -> Option<NonZeroU128> {
        self.entry.digest
    }
}

pub struct TableIterator<'a> {
    table: &'a Table,
    entires_iter: std::slice::Iter<'a, Entry>,
}

impl<'a> TableIterator<'a> {
    pub fn new(table: &'a Table, entires_iter: std::slice::Iter<'a, Entry>) -> TableIterator<'a> {
        TableIterator {
            table,
            entires_iter,
        }
    }
}

impl<'a> Iterator for TableIterator<'a> {
    type Item = TableEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.entires_iter.next();
        entry.and_then(|entry| Some(TableEntry::new(self.table, entry)))
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.entires_iter.size_hint()
    }
}

impl Table {
    fn iter<'a>(&'a self) -> TableIterator<'a> {
        TableIterator::new(self, self.entries.iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_ingest() {
        let cwd = std::env::current_dir().unwrap();

        let mut table = Table::new();
        table
            .ingester(cwd.join("src"))
            .compute_digests(true)
            .ingest();

        //println!("{:?}", table);

        let iter = table.iter();
        for e in iter {
            println!("Entry: {:?} {} bytes", e.path(), e.size());
        }
    }
}
