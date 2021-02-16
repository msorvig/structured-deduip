use clap::{App, Arg};
use indicatif::{MultiProgress, ProgressBar, ProgressIterator, ProgressStyle};
use jwalk::{DirEntry, WalkDir};
use rayon::iter::{ParallelBridge, ParallelIterator};
use rayon::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{cmp::Ordering, sync::Arc};
use std::{io::Read, path::Path, sync::Mutex};
// use std::{cmp::Ordering, collections::HashMap, fs::File,
extern crate num_cpus;
use crossbeam::atomic::AtomicCell;
use itertools::Itertools;
use std::convert::TryFrom;
use std::fmt;
use std::path::PathBuf;

type JWalkDirEntry = DirEntry<((), ())>;

fn scan_dir2(path: &str) -> Vec<JWalkDirEntry> {
    let threads = num_cpus::get();

    // find all immediate subdirectories of the given path
    let roots: Vec<_> = WalkDir::new(path)
        .follow_links(false)
        .sort(true)
        .max_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|item| item.file_type().is_dir())
        .skip(1) // skip "path"
        .collect();

    // scan each subdirectoy, print progress to stdout, collect entries
    let entries = Arc::new(Mutex::new(Vec::<DirEntry<((), ())>>::new()));
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads + 1)
        .build()
        .unwrap();
    pool.install(|| {
        rayon::scope(|s| {
            let m = MultiProgress::new();
            for i in 0..roots.len() {
                let pb = m.add(ProgressBar::new(0));
                let root = &roots[i];
                let entries2 = entries.clone();
                s.spawn(move |_| {
                    let path = root.path().to_string_lossy().to_string();

                    pb.set_style(ProgressStyle::default_spinner().clone());
                    pb.set_message(&format!("Scanning {}", path));

                    let e: Vec<_> = WalkDir::new(path)
                        .follow_links(false)
                        .parallelism(jwalk::Parallelism::Serial) // TODO: use threadpool
                        .sort(true)
                        .into_iter()
                        .filter_map(Result::ok)
                        .inspect(|_| {
                            pb.tick();
                        })
                        .collect();
                    let length = e.len();

                    entries2.lock().unwrap().extend(e);

                    pb.finish_with_message(&format!("Done ({}) {:?}", length, root.path()));
                });
            }
            m.join().unwrap();
        });
    });

    // unwrap the entries
    let mut final_entries = Arc::try_unwrap(entries).unwrap().into_inner().unwrap();

    println!("Sorting by name");

    final_entries.par_sort_unstable_by(|a, b| a.file_name().cmp(b.file_name()));

    return final_entries;
}
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

fn filter_files(entries: Vec<JWalkDirEntry>) -> Vec<FileEntry> {
    let min_file_size = 1024; // Skip small files
    entries
        .par_iter()
        .filter_map(|jentry| {
            // match FileEntry::try_from(jentry) ### y u no work
            FileEntry::from_jwalk_entry(jentry)
        })
        .filter(|entry| entry.len > min_file_size)
        .collect()
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

fn compute_digests(entries: &mut Vec<FileEntry>) {
    entries.iter().progress().par_bridge().for_each(|entry| {
        let digest = compute_file_digest(&entry.path);
        entry.digest.store(digest);
        //  println!("digest for {:?} {:?}", entry.path,  digest);
    });
}

fn group_by_digest(numbers: &Vec<FileEntry>) -> impl Iterator<Item = &[FileEntry]> {
    numbers.iter().enumerate().peekable().batching(move |it| {
        match it.next() {
            None => None,
            Some(elem) => {
                // it.next() gave us the beginning of a group. Determine the end
                // by looping until peek() gives an element with a different value.

                let begin_i = elem.0;
                let group_value = elem.1;
                let mut end_i = begin_i;
                loop {
                    match it.peek() {
                        None => break,
                        Some(elem) => {
                            if elem.1 == group_value {
                                end_i += 1;
                                it.next();
                            } else {
                                break;
                            }
                        }
                    }
                }

                // yield the group as a slice
                Some(&numbers[begin_i..end_i + 1])
            }
        }
    })
}

fn compute_savings(entries: Vec<JWalkDirEntry>) {
    println!("Verifying files/filtering small files");
    let mut file_entries = filter_files(entries);
    let file_count = file_entries.len();
    let file_bytes = file_entries.iter().fold(0, |acc, entry| acc + entry.len);

    println!("Have {} files with {} bytes", file_count, file_bytes);

    println!("Compute digests");
    compute_digests(&mut file_entries);

    println!("Sorting by digest");
    file_entries.par_sort_unstable_by(|a, b| a.digest.load().cmp(&b.digest.load()));

    let groups_it = group_by_digest(&file_entries);

    for g in groups_it.filter(|x| x.len() > 10).take(10) {
        let mut pk_g = g.iter().peekable();
        let first = *pk_g.peek().unwrap();
        println!("");
        println!(
            "Group: {} file count {} file size {} digest {:?}",
            first.name,
            g.len(),
            first.len,
            first.digest.load()
        );
        println!("Files:");
        for file in pk_g {
            println!("   {}", file.path.to_str().unwrap());
        }
    }

    println!("Duped  : {} bytes", file_bytes);
    //    println!("Deduped: {} bytes", dedup_bytes);
    println!("files : {}", file_count);
    //println!("groups: {}", group_count);

    //Crate histogram

    /*
     */

    /*
        let groups_it = file_entries.iter().batching(|it| {
            let itpk = it.peekable();
            match itpk.next() {
                Some (first) => {

                }
                None => None
            }
        });
    */

    //  let _it = entries.into_iter();
    /*
        let group_it = entries.into_iter().batching(|entry|{
            // group into batches of equal size
           // let batch_size = entry.metadata().unwrap().len();
            4
        });
    */
}

fn main() {
    let scan = App::new("scan")
        .about("scan folder for files")
        .arg(Arg::new("path").about("Specifies filesystem path"))
        .arg(
            Arg::new("save")
                .short('s')
                .long("--save")
                .about("Save file list to disk"),
        );
    let compute = App::new("compute")
        .about("compute (potential) dedup savings")
        .arg(Arg::new("path").about("Specifies filesystem path"))
        .arg(
            Arg::new("load")
                .short('l')
                .long("--load")
                .about("Load file list from disk"),
        );
    let dedup = App::new("dedup")
        .about("deduplicate files")
        .arg(Arg::new("path").about("Specifies filesystem path"))
        .arg(
            Arg::new("load")
                .short('l')
                .long("--load")
                .about("Load file list from disk"),
        );

    let matches = App::new("llvmbuilder")
        .subcommand(scan)
        .subcommand(compute)
        .subcommand(dedup)
        .get_matches();

    //println!("helo, {:?}", matches);

    match matches.subcommand() {
        Some(("scan", args)) => {
            println!("{:?}", args);
            match args.value_of("path") {
                Some(path) => {
                    println!("scan {:?}", path);
                    let entries = scan_dir2(path);
                    println!("scan found {:?} files", entries.len());

                    if args.is_present("save") {
                        let filified_path = path.replace("/", "-");
                        let file_name = "dedupfiles".to_string() + &filified_path;
                        println!("Saving file list to {}", file_name);
                        /*
                                                match bincode::serialize(&entries) {
                                                    Err(err) => panic!("serialize_digests error {:?}", err),
                                                    Ok(vec) => return vec,
                                                }
                        */
                    }
                }
                None => {
                    println!("Missing path argument");
                }
            }
        }
        Some(("compute", args)) => {
            println!("compute");
            match args.value_of("path") {
                Some(path) => {
                    println!("compute {:?}", path);
                    let entries = scan_dir2(path);
                    compute_savings(entries);
                    //find_candidates(path, 1);
                }
                None => {
                    println!("Missing path argument");
                }
            }
        }
        Some(("dedup", _args)) => {
            println!("dedup");
        }

        Some((command, _args)) => {
            println!("Unknownn command: {:}", command);
        }
        None => {
            println!("Missing command");
        }
    }
}
