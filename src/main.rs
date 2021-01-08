use clap::{App, Arg};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use jwalk::{WalkDir, DirEntry};
//use rayon::iter::{ParallelIterator};
use rayon::prelude::*;
use std::sync::Mutex;
use std::sync::Arc;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Entry {
    file_name: String,
    file_path: String,
    size: Option<u64>,
    digest: Option<u128>,
}

fn scan_dir2(path: &str)  -> Vec::<DirEntry<((),())>> {
    let threads = 8;

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
    let entries = Arc::new(Mutex::new(Vec::<DirEntry<((),())>>::new()));
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

                    let e : Vec<_> = WalkDir::new(path)
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

                    entries2.lock()
                    .unwrap()
                    .extend(e);

                    pb.finish_with_message(&format!("Done ({}) {:?}", length, root.path()));
                });
            }
            m.join().unwrap();
        });
    });

    // unwrap the entries
    let mut final_entries = Arc::try_unwrap(entries)
    .unwrap()
    .into_inner()
    .unwrap();

    println!("Sorting...");

    final_entries.par_sort_unstable_by(|a, b| {
        a.file_name().cmp(b.file_name())
    });

    println!("Sort done");

    return final_entries;
}

fn main() {
    let scan = App::new("scan")
        .about("scan folder for files")
        .arg(Arg::new("path").about("Specifies filesystem path"))
        .arg(Arg::new("save").short('s').long("--save").about("Save file list to disk"));
    let compute = App::new("compute")
        .about("compute (potential) dedup savings")
        .arg(Arg::new("path").about("Specifies filesystem path"))
        .arg(Arg::new("load").short('l').long("--load").about("Load file list from disk"));
    let dedup = App::new("dedup").about("deduplicate files")
        .arg(Arg::new("path").about("Specifies filesystem path"))
        .arg(Arg::new("load").short('l').long("--load").about("Load file list from disk"));

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
