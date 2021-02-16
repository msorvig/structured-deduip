pub mod pathstore {

    //!
    //! Provides efficient storage of file system paths
    //!
    //! Pathstore splits paths into its sub-parts and
    //! stores the parts in a tree structure such that
    //! each common part is stored only once.
    //!

    use bimap::BiHashMap;
    use bimap::BiMap;
    use serde::{Deserialize, Serialize};
    use smallvec::SmallVec;
    use std::ffi::{OsStr, OsString};
    use std::path::{Path, PathBuf};
    use std::{cmp::Ordering, convert::TryInto};

    /// The PathStore struct
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct PathStore {
        parts: BiHashMap<u32, OsString>, // part-index <-> part-string
        paths: BiHashMap<u32, (u32, Option<u32>)>, // path-index <-> (part-index, next-path-index)
    }

    struct PathIteratorReverse<'a>(Option<u32>, &'a BiHashMap<u32, (u32, Option<u32>)>);

    impl Iterator for PathIteratorReverse<'_> {
        type Item = u32;
        fn next(&mut self) -> Option<Self::Item> {
            match self.0 {
                Some(index) => {
                    let (part_index, path_index) = self.1.get_by_left(&index).unwrap();
                    self.0 = *path_index;
                    Some(*part_index)
                }
                None => None,
            }
        }
    }

    impl PathStore {
        /// Creates a new PathStore
        pub fn new() -> PathStore {
            let mut store = PathStore {
                parts: BiMap::new(),
                paths: BiMap::new(),
            };

            // Add empty part and path
            store.parts.insert(0, OsString::new());
            store.paths.insert(0, (0, None));

            store
        }

        fn insert_or_lookup_part(parts: &mut BiHashMap<u32, OsString>, part: &OsStr) -> u32 {
            match parts.get_by_right(&part.to_os_string()) {
                Some(index) => *index,
                None => {
                    let i: u32 = parts.len().try_into().unwrap();
                    parts.insert(i, part.to_os_string());
                    i
                }
            }
        }

        fn insert_or_lookup_path(
            paths: &mut BiHashMap<u32, (u32, Option<u32>)>,
            index: u32,
            next_index: Option<u32>,
        ) -> u32 {
            match paths.get_by_right(&(index, next_index)) {
                Some(index) => *index,
                None => {
                    let i: u32 = paths.len().try_into().unwrap();
                    paths.insert(i, (index, next_index));
                    i
                }
            }
        }

        /// Adds a path to the path store.
        ///
        /// Returns the index for the given path.
        pub fn add_path(&mut self, path: &Path) -> u32 {
            if path.as_os_str().len() == 0 {
                return 0;
            }

            let parts = &mut self.parts;
            let paths = &mut self.paths;

            let mut indexed_parts = path
                .iter()
                .map(|part| (Self::insert_or_lookup_part(parts, part), part));

            let mut current_path_index: Option<u32> = None;
            while let Some((index, _)) = indexed_parts.next() {
                current_path_index = Some(Self::insert_or_lookup_path(
                    paths,
                    index,
                    current_path_index,
                ));
            }
            current_path_index.unwrap()
        }

        /// Returns the path for the given index.
        pub fn get_path(&self, index: u32) -> PathBuf {
            PathIteratorReverse(Some(index), &self.paths)
                .collect::<SmallVec<[u32; 16]>>()
                .into_iter()
                .rev()
                .map(|part_index| self.parts.get_by_left(&part_index).unwrap())
                .collect()
        }

        pub fn cmp_paths(&self, a_path: u32, b_path: u32) -> Ordering {
            if a_path == b_path {
                return Ordering::Equal;
            }

            // Get forward path iterators
            let a_it = PathIteratorReverse(Some(a_path), &self.paths)
                .collect::<SmallVec<[u32; 16]>>()
                .into_iter()
                .rev();
            let b_it = PathIteratorReverse(Some(b_path), &self.paths)
                .collect::<SmallVec<[u32; 16]>>()
                .into_iter()
                .rev();

            // Compare parts until a difference is found; else the paths are  Equal
            for (a_part, b_part) in a_it.zip(b_it) {
                if a_part == b_part {
                    continue;
                }
                let a_str = self.parts.get_by_left(&a_part).unwrap();
                let b_str = self.parts.get_by_left(&b_part).unwrap();
                return a_str.cmp(b_str);
            }
            Ordering::Equal
        }
    }

    #[test]
    fn test_pathstore() {
        let mut path_store = PathStore::new();

        let a_path = Path::new("/path/to/afile.zip");
        let b_path = Path::new("/path/to/bfile.zip");
        let a_again_path = Path::new("/different/path/to/afile.zip");
        let c_path = Path::new("path/to/local/cfile.zip");
        let c2_path = Path::new("path/to/local/c2file.zip");
        let empty_path = Path::new("");

        let a_index = path_store.add_path(a_path);
        let b_index = path_store.add_path(b_path);
        let a_again_index = path_store.add_path(a_again_path);
        let c_index = path_store.add_path(c_path);
        let c2_index = path_store.add_path(c2_path);

        assert_ne!(a_index, b_index);
        assert_ne!(a_index, c_index);
        assert_ne!(c2_index, c_index);
        assert_eq!(a_again_index, a_again_index);

        assert_ne!(a_index, 0); // 0  is reserved for then empty path
        let empty_index = path_store.add_path(empty_path);
        assert_eq!(empty_index, 0);

        assert_eq!(path_store.get_path(a_index), a_path);
        assert_eq!(path_store.get_path(b_index), b_path);
        assert_eq!(path_store.get_path(c_index), c_path);
        assert_eq!(path_store.get_path(empty_index), empty_path);

        assert_eq!(path_store.cmp_paths(a_index, a_index), Ordering::Equal);
        assert_eq!(path_store.cmp_paths(a_index, b_index), Ordering::Less);
        assert_eq!(path_store.cmp_paths(b_index, a_index), Ordering::Greater);

        assert_eq!(
            path_store.cmp_paths(empty_index, empty_index),
            Ordering::Equal
        );
    }
}
