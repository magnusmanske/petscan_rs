//! Membership sets of page keys that spill to sorted run files on disk.
//!
//! Set operations on large query results only need page identity
//! `(namespace, title)`, never the metadata attached by a data source, for the
//! operand that is merely subtracted or intersected away. This module keeps
//! those keys in memory while the set is small (the common case, and identical
//! to the old hash-set behaviour) and transparently switches to sorted runs on
//! disk once it grows past [`DEFAULT_SPILL_THRESHOLD`].
//!
//! Runs are written as length-prefixed `(i64 namespace, UTF-8 title)` records,
//! sorted and deduplicated. Set operations then either stream the runs
//! (union/intersection/difference between two key sets) or probe the retained
//! page list run by run, so peak memory stays near one run plus the operands
//! that must keep their entries.
//!
//! Deliberately *not* a general store: entries are never reconstructed from a
//! key set. The caller keeps the side whose metadata matters and only uses a
//! key set for membership.

use crate::pagelist::PageList;
use crate::pagelist_entry::PageListEntry;
use anyhow::{Context, Result, anyhow};
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use wikimisc::mediawiki::api::NamespaceID;
use wikimisc::mediawiki::title::Title;

/// Keys held in memory before a set starts writing runs to disk. This is a
/// per-query bound, so it also bounds the number of large sets 50 concurrent
/// requests can hold at once.
pub const DEFAULT_SPILL_THRESHOLD: usize = 250_000;

/// Environment override for the spill threshold, mainly for benchmarks and
/// for tuning on Toolforge without a rebuild. Values below 1 are ignored.
pub const SPILL_THRESHOLD_ENV: &str = "PETSCAN_SPILL_THRESHOLD";

/// A title longer than this is rejected rather than allocated: MediaWiki
/// limits titles to 255 bytes, so anything larger means a corrupt run file.
const MAX_TITLE_BYTES: u32 = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeySetLimits {
    /// Number of keys kept in memory before spilling.
    pub threshold: usize,
}

impl Default for KeySetLimits {
    fn default() -> Self {
        Self {
            threshold: DEFAULT_SPILL_THRESHOLD,
        }
    }
}

impl KeySetLimits {
    /// Limits from the environment, falling back to the defaults. Invalid or
    /// zero values keep the default rather than disabling the safety net.
    pub fn from_env() -> Self {
        std::env::var(SPILL_THRESHOLD_ENV)
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|threshold| *threshold > 0)
            .map_or_else(Self::default, |threshold| Self { threshold })
    }
}

/// The identity of a page: this is exactly what [`PageListEntry`]'s `Hash` and
/// `Eq` use, so a key set and a `HashSet<PageListEntry>` agree on membership.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct PageKey {
    namespace: NamespaceID,
    title: String,
}

impl PageKey {
    fn of(entry: &PageListEntry) -> Self {
        Self {
            namespace: entry.title().namespace_id(),
            title: entry.title().pretty().to_string(),
        }
    }

    /// A probe entry with the same identity as this key, for hash lookups in a
    /// `HashSet<PageListEntry>`.
    fn entry(&self) -> PageListEntry {
        PageListEntry::new(Title::new(&self.title, self.namespace))
    }
}

/// A sorted, deduplicated stream of keys. Either an in-memory vector or a
/// heap-merged set of run files (which owns the temporary directory, so the
/// files live as long as the iterator).
enum KeyIter {
    Memory(std::vec::IntoIter<PageKey>),
    Runs {
        heap: BinaryHeap<Reverse<(PageKey, usize)>>,
        readers: Vec<RunReader>,
        last: Option<PageKey>,
        _dir: Option<TempDir>,
    },
}

impl KeyIter {
    fn next_key(&mut self) -> Result<Option<PageKey>> {
        match self {
            Self::Memory(iter) => Ok(iter.next()),
            Self::Runs {
                heap,
                readers,
                last,
                ..
            } => loop {
                let Some(Reverse((key, index))) = heap.pop() else {
                    return Ok(None);
                };
                let next = readers[index]
                    .next_key()
                    .with_context(|| "reading key run")?;
                if let Some(next) = next {
                    heap.push(Reverse((next, index)));
                }
                // Deduplicate keys that landed in more than one run.
                if last.as_ref() == Some(&key) {
                    continue;
                }
                *last = Some(key.clone());
                return Ok(Some(key));
            },
        }
    }
}

impl Iterator for KeyIter {
    type Item = Result<PageKey>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_key() {
            Ok(Some(key)) => Some(Ok(key)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }
}

/// Reads one sorted run. Records are `i64` namespace, `u32` byte length, then
/// the UTF-8 title.
struct RunReader {
    reader: BufReader<File>,
}

impl RunReader {
    fn open(path: &Path) -> Result<Self> {
        let file =
            File::open(path).with_context(|| format!("opening key run {}", path.display()))?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    fn next_key(&mut self) -> Result<Option<PageKey>> {
        let Some(namespace) = read_namespace(&mut self.reader)? else {
            return Ok(None);
        };
        let mut length = [0_u8; 4];
        self.reader
            .read_exact(&mut length)
            .with_context(|| "truncated key run header")?;
        let length = u32::from_le_bytes(length);
        if length > MAX_TITLE_BYTES {
            return Err(anyhow!("implausible title length {length} in key run"));
        }
        let mut title = vec![0_u8; length as usize];
        self.reader
            .read_exact(&mut title)
            .with_context(|| "truncated key run title")?;
        let title = String::from_utf8(title).with_context(|| "invalid UTF-8 in key run")?;
        Ok(Some(PageKey { namespace, title }))
    }
}

/// Reads the 8-byte namespace, returning `None` cleanly at end of file.
fn read_namespace(reader: &mut impl Read) -> Result<Option<NamespaceID>> {
    let mut bytes = [0_u8; 8];
    let mut first = [0_u8; 1];
    if reader.read(&mut first)? == 0 {
        return Ok(None);
    }
    bytes[0] = first[0];
    reader
        .read_exact(&mut bytes[1..])
        .with_context(|| "truncated key run namespace")?;
    Ok(Some(NamespaceID::from_le_bytes(bytes)))
}

fn write_run(dir: &Path, index: usize, keys: &[PageKey]) -> Result<PathBuf> {
    let path = dir.join(format!("run-{index:08}"));
    let file =
        File::create(&path).with_context(|| format!("creating key run {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    for key in keys {
        writer.write_all(&key.namespace.to_le_bytes())?;
        let length = u32::try_from(key.title.len())
            .ok()
            .filter(|length| *length <= MAX_TITLE_BYTES)
            .ok_or_else(|| anyhow!("title too long for a key run: {}", key.title.len()))?;
        writer.write_all(&length.to_le_bytes())?;
        writer.write_all(key.title.as_bytes())?;
    }
    writer.flush().with_context(|| "flushing key run")?;
    Ok(path)
}

/// An unordered membership set with a bounded in-memory footprint.
#[derive(Debug)]
pub struct KeySet {
    limits: KeySetLimits,
    mem: HashSet<PageKey>,
    buffer: Vec<PageKey>,
    runs: Vec<PathBuf>,
    dir: Option<TempDir>,
    spilling: bool,
    inserted: usize,
}

impl KeySet {
    fn with_limits(limits: KeySetLimits) -> Self {
        Self {
            limits,
            mem: HashSet::new(),
            buffer: Vec::new(),
            runs: Vec::new(),
            dir: None,
            spilling: false,
            inserted: 0,
        }
    }

    /// Consumes a page list into its keys. The list's entries (and their
    /// strings) are dropped as they are converted, so the spill itself does not
    /// make a full second copy.
    pub fn from_pagelist(pagelist: PageList, limits: KeySetLimits) -> Result<Self> {
        Self::from_entries(pagelist.take_entries(), limits)
    }

    fn from_entries(entries: HashSet<PageListEntry>, limits: KeySetLimits) -> Result<Self> {
        let mut set = Self::with_limits(limits);
        for entry in entries {
            set.insert(PageKey::of(&entry))?;
        }
        set.finish()
    }

    /// The number of keys inserted, including duplicates. Only for logging and
    /// tests: the exact distinct size needs a merge over all runs.
    pub const fn inserted(&self) -> usize {
        self.inserted
    }

    /// Whether this set has switched to disk. False means it is still a plain
    /// in-memory set and behaves exactly like the old code.
    pub const fn is_spilled(&self) -> bool {
        self.spilling
    }

    fn insert(&mut self, key: PageKey) -> Result<()> {
        self.inserted += 1;
        if !self.spilling {
            self.mem.insert(key);
            if self.mem.len() > self.limits.threshold {
                self.start_spilling()?;
            }
            return Ok(());
        }
        self.buffer.push(key);
        if self.buffer.len() >= self.limits.threshold {
            self.flush_run()?;
        }
        Ok(())
    }

    /// Moves the in-memory keys into the run buffer and frees the old table.
    fn start_spilling(&mut self) -> Result<()> {
        self.spilling = true;
        let mem = std::mem::take(&mut self.mem);
        self.buffer.extend(mem);
        if self.buffer.len() >= self.limits.threshold {
            self.flush_run()?;
        }
        Ok(())
    }

    fn flush_run(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let dir = self.ensure_dir()?.to_path_buf();
        let mut keys = std::mem::take(&mut self.buffer);
        keys.sort_unstable();
        keys.dedup();
        let path = write_run(&dir, self.runs.len(), &keys)?;
        self.runs.push(path);
        Ok(())
    }

    fn ensure_dir(&mut self) -> Result<&Path> {
        if self.dir.is_none() {
            self.dir = Some(
                tempfile::Builder::new()
                    .prefix("petscan-keys-")
                    .tempdir()
                    .with_context(|| "creating key spill directory")?,
            );
        }
        self.dir
            .as_ref()
            .map(|dir| dir.path())
            .ok_or_else(|| anyhow!("key spill directory disappeared"))
    }

    /// Flushes any partial run. Must be called before reading the set.
    fn finish(mut self) -> Result<Self> {
        if self.spilling {
            self.flush_run()?;
        }
        Ok(self)
    }

    /// Drops the entries that are (`keep`) or are not (`!keep`) in this key
    /// set. This is the metadata-preserving side of an intersection or
    /// difference: the page list keeps its own entries, the key set only says
    /// which identities survive.
    pub fn retain_entries(&self, entries: &mut HashSet<PageListEntry>, keep: bool) -> Result<()> {
        if !self.spilling {
            entries.retain(|entry| self.mem.contains(&PageKey::of(entry)) == keep);
            return Ok(());
        }
        if keep {
            // Build the result by moving matching entries out of the original
            // set. This is correct across multiple runs: a page in any run is
            // kept, and `take` returns it at most once.
            let mut old = std::mem::take(entries);
            let mut kept = HashSet::new();
            for run in &self.runs {
                let mut reader = RunReader::open(run)?;
                while let Some(key) = reader.next_key()? {
                    if let Some(entry) = old.take(&key.entry()) {
                        kept.insert(entry);
                    }
                }
            }
            *entries = kept;
        } else {
            // Removal is monotonic, so runs can be applied one at a time
            // without accumulating a second large set.
            for run in &self.runs {
                let mut reader = RunReader::open(run)?;
                while let Some(key) = reader.next_key()? {
                    entries.remove(&key.entry());
                }
            }
        }
        Ok(())
    }

    fn into_sorted_iter(self) -> Result<KeyIter> {
        if !self.spilling {
            let mut keys: Vec<PageKey> = self.mem.into_iter().collect();
            keys.sort_unstable();
            keys.dedup();
            return Ok(KeyIter::Memory(keys.into_iter()));
        }
        let mut readers = Vec::with_capacity(self.runs.len());
        let mut heap = BinaryHeap::new();
        for (index, path) in self.runs.iter().enumerate() {
            let mut reader = RunReader::open(path)?;
            if let Some(key) = reader.next_key()? {
                heap.push(Reverse((key, index)));
            }
            readers.push(reader);
        }
        Ok(KeyIter::Runs {
            heap,
            readers,
            last: None,
            _dir: self.dir,
        })
    }

    pub fn union(self, other: Self, limits: KeySetLimits) -> Result<Self> {
        let mut out = Self::with_limits(limits);
        let mut left = self.into_sorted_iter()?;
        let mut right = other.into_sorted_iter()?;
        let mut a = left.next_key()?;
        let mut b = right.next_key()?;
        while let Some(order) = key_order(&a, &b) {
            match order {
                Ordering::Less => {
                    out.insert(a.take().ok_or_else(missing_key)?)?;
                    a = left.next_key()?;
                }
                Ordering::Greater => {
                    out.insert(b.take().ok_or_else(missing_key)?)?;
                    b = right.next_key()?;
                }
                Ordering::Equal => {
                    out.insert(a.take().ok_or_else(missing_key)?)?;
                    a = left.next_key()?;
                    b = right.next_key()?;
                }
            }
        }
        out.finish()
    }

    pub fn intersect(self, other: Self, limits: KeySetLimits) -> Result<Self> {
        let mut out = Self::with_limits(limits);
        let mut left = self.into_sorted_iter()?;
        let mut right = other.into_sorted_iter()?;
        let mut a = left.next_key()?;
        let mut b = right.next_key()?;
        while let Some(order) = key_order(&a, &b) {
            match order {
                Ordering::Less => a = left.next_key()?,
                Ordering::Greater => b = right.next_key()?,
                Ordering::Equal => {
                    out.insert(a.take().ok_or_else(missing_key)?)?;
                    a = left.next_key()?;
                    b = right.next_key()?;
                }
            }
        }
        out.finish()
    }

    pub fn difference(self, other: Self, limits: KeySetLimits) -> Result<Self> {
        let mut out = Self::with_limits(limits);
        let mut left = self.into_sorted_iter()?;
        let mut right = other.into_sorted_iter()?;
        let mut a = left.next_key()?;
        let mut b = right.next_key()?;
        while let Some(order) = key_order(&a, &b) {
            match order {
                Ordering::Less => {
                    out.insert(a.take().ok_or_else(missing_key)?)?;
                    a = left.next_key()?;
                }
                Ordering::Greater => b = right.next_key()?,
                Ordering::Equal => {
                    a = left.next_key()?;
                    b = right.next_key()?;
                }
            }
        }
        out.finish()
    }
}

fn missing_key() -> anyhow::Error {
    anyhow!("key stream ended unexpectedly")
}

/// Orders two optional keys; `None` means both streams are exhausted.
fn key_order(a: &Option<PageKey>, b: &Option<PageKey>) -> Option<Ordering> {
    match (a, b) {
        (None, None) => None,
        (None, Some(_)) => Some(Ordering::Greater),
        (Some(_), None) => Some(Ordering::Less),
        (Some(a), Some(b)) => Some(a.cmp(b)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn make_list(items: &[(NamespaceID, &str)]) -> PageList {
        let list = PageList::new_from_wiki("enwiki");
        for (namespace, title) in items {
            list.add_entry(PageListEntry::new(Title::new(title, *namespace)));
        }
        list
    }

    fn keys_of(items: &[(NamespaceID, &str)]) -> HashSet<(NamespaceID, String)> {
        items
            .iter()
            .map(|(namespace, title)| (*namespace, (*title).to_string()))
            .collect()
    }

    fn key_set(items: &[(NamespaceID, &str)], threshold: usize) -> KeySet {
        KeySet::from_pagelist(
            make_list(items),
            KeySetLimits {
                threshold: threshold.max(1),
            },
        )
        .unwrap()
    }

    fn materialize(set: KeySet) -> HashSet<(NamespaceID, String)> {
        set.sorted_iter_for_test()
            .unwrap()
            .map(|key| key.map(|key| (key.namespace, key.title)))
            .collect::<Result<HashSet<_>>>()
            .unwrap()
    }

    impl KeySet {
        fn sorted_iter_for_test(self) -> Result<KeyIter> {
            self.into_sorted_iter()
        }
    }

    /// A set at or below the threshold never touches disk.
    #[test]
    fn small_set_stays_in_memory() {
        let set = key_set(&[(0, "A"), (0, "B")], 100);
        assert!(!set.is_spilled());
        assert_eq!(set.inserted(), 2);
        assert_eq!(materialize(set).len(), 2);
    }

    /// Duplicate identities collapse, as they do in a `HashSet<PageListEntry>`.
    #[test]
    fn duplicates_collapse() {
        let set = key_set(&[(0, "A"), (0, "A"), (1, "A")], 100);
        assert_eq!(materialize(set), keys_of(&[(0, "A"), (1, "A")]));
    }

    /// Growing past the threshold spills, including keys already in memory.
    #[test]
    fn threshold_spills_and_preserves_keys() {
        let items = [
            (0, "A"),
            (0, "B"),
            (0, "C"),
            (0, "D"),
            (0, "E"),
            (0, "F"),
            (0, "G"),
            (0, "H"),
        ];
        let set = key_set(&items, 3);
        assert!(set.is_spilled());
        assert_eq!(materialize(set), keys_of(&items));
    }

    /// Multiple runs with Unicode, empty and namespaced titles merge correctly.
    #[test]
    fn merge_across_runs_is_sorted_and_complete() {
        let titles = [
            (0, "Zebra"),
            (0, "Äpfel"),
            (0, ""),
            (4, "Project"),
            (-1, "Special"),
            (0, "apple"),
            (120, "Property:P31"),
        ];
        let set = key_set(&titles, 1);
        assert!(set.is_spilled());
        let materialized: Vec<_> = {
            let mut keys: Vec<_> = materialize(set).into_iter().collect();
            keys.sort();
            keys
        };
        assert_eq!(materialized.len(), titles.len());
        assert_eq!(materialized, {
            let mut expected: Vec<_> = keys_of(&titles).into_iter().collect();
            expected.sort();
            expected
        });
    }

    #[test]
    fn union_matches_hash_set() {
        let a = [(0, "A"), (0, "B"), (1, "C")];
        let b = [(0, "B"), (0, "D"), (2, "E")];
        let expected: HashSet<_> = keys_of(&a).union(&keys_of(&b)).cloned().collect();
        for threshold in [1, 2, 100] {
            let result = key_set(&a, threshold)
                .union(key_set(&b, threshold), KeySetLimits { threshold })
                .unwrap();
            assert_eq!(materialize(result), expected, "threshold {threshold}");
        }
    }

    #[test]
    fn intersect_matches_hash_set() {
        let a = [(0, "A"), (0, "B"), (1, "C")];
        let b = [(0, "B"), (1, "C"), (2, "E")];
        let expected: HashSet<_> = keys_of(&a).intersection(&keys_of(&b)).cloned().collect();
        for threshold in [1, 2, 100] {
            let result = key_set(&a, threshold)
                .intersect(key_set(&b, threshold), KeySetLimits { threshold })
                .unwrap();
            assert_eq!(materialize(result), expected, "threshold {threshold}");
        }
    }

    #[test]
    fn difference_matches_hash_set() {
        let a = [(0, "A"), (0, "B"), (1, "C"), (0, "D")];
        let b = [(0, "B"), (1, "C"), (2, "E")];
        let expected: HashSet<_> = keys_of(&a).difference(&keys_of(&b)).cloned().collect();
        for threshold in [1, 2, 100] {
            let result = key_set(&a, threshold)
                .difference(key_set(&b, threshold), KeySetLimits { threshold })
                .unwrap();
            assert_eq!(materialize(result), expected, "threshold {threshold}");
        }
    }

    #[test]
    fn empty_operands() {
        let empty: Vec<(NamespaceID, &str)> = vec![];
        let a = [(0, "A")];
        for threshold in [1, 100] {
            let limits = KeySetLimits { threshold };
            assert_eq!(
                materialize(
                    key_set(&empty, threshold)
                        .union(key_set(&a, threshold), limits)
                        .unwrap()
                ),
                keys_of(&a)
            );
            assert!(
                materialize(
                    key_set(&a, threshold)
                        .intersect(key_set(&empty, threshold), limits)
                        .unwrap()
                )
                .is_empty()
            );
            assert_eq!(
                materialize(
                    key_set(&a, threshold)
                        .difference(key_set(&empty, threshold), limits)
                        .unwrap()
                ),
                keys_of(&a)
            );
        }
    }

    #[test]
    fn retain_entries_intersection_keeps_metadata() {
        let with_id = |title: &str, namespace: NamespaceID, id: u32| {
            let mut entry = PageListEntry::new(Title::new(title, namespace));
            entry.set_page_id(Some(id));
            entry
        };
        let mut entries: HashSet<PageListEntry> = [
            with_id("A", 0, 42),
            with_id("B", 0, 43),
            with_id("C", 1, 44),
        ]
        .into_iter()
        .collect();
        let keep = key_set(&[(0, "A"), (1, "C")], 1);
        assert!(keep.is_spilled());
        keep.retain_entries(&mut entries, true).unwrap();
        assert_eq!(entries.len(), 2);
        let ids: HashSet<_> = entries.iter().filter_map(|e| e.page_id()).collect();
        assert_eq!(ids, HashSet::from([42, 44]));
    }

    #[test]
    fn retain_entries_difference_removes_from_all_runs() {
        // Deleting "B" (run 0) and "D" (later run) must both take effect.
        let removed = [(0, "B"), (0, "X"), (0, "D"), (0, "Y"), (0, "F")];
        let keep = key_set(&removed, 1);
        assert!(keep.is_spilled());
        let mut entries: HashSet<PageListEntry> = ["A", "B", "C", "D", "E", "F"]
            .into_iter()
            .map(|title| PageListEntry::new(Title::new(title, 0)))
            .collect();
        keep.retain_entries(&mut entries, false).unwrap();
        let titles: HashSet<_> = entries.iter().map(|e| e.title().pretty()).collect();
        assert_eq!(titles, HashSet::from(["A", "C", "E"]));
    }

    #[test]
    fn empty_set_does_not_create_a_temp_dir() {
        let set = key_set(&[], 100);
        assert!(!set.is_spilled());
        assert!(set.dir.is_none());
    }

    #[test]
    fn run_file_rejects_absurd_title_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run-bad");
        let mut file = File::create(&path).unwrap();
        file.write_all(&0_i64.to_le_bytes()).unwrap();
        file.write_all(&u32::MAX.to_le_bytes()).unwrap();
        drop(file);
        let mut reader = RunReader::open(&path).unwrap();
        assert!(reader.next_key().is_err());
    }

    #[test]
    fn retain_entries_in_memory_path() {
        let mut entries: HashSet<PageListEntry> = ["A", "B", "C"]
            .into_iter()
            .map(|title| PageListEntry::new(Title::new(title, 0)))
            .collect();
        let keys = key_set(&[(0, "A"), (0, "C")], 100);
        assert!(!keys.is_spilled());
        keys.retain_entries(&mut entries, true).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|e| e.title().pretty() == "A"));
        assert!(entries.iter().any(|e| e.title().pretty() == "C"));

        keys.retain_entries(&mut entries, false).unwrap();
        assert!(entries.is_empty());
    }

    /// Random sets through every operation at several thresholds, including
    /// boundaries where the set grows a run mid-insertion.
    #[test]
    fn random_operations_match_hash_set() {
        use rand::Rng;
        let mut rng = rand::rng();
        for _round in 0..10 {
            let make = |generator: &mut rand::rngs::ThreadRng| -> Vec<(NamespaceID, String)> {
                (0..200)
                    .map(|_| {
                        let namespace = generator.random_range(-1..3);
                        let title = format!("Page{}", generator.random_range(0..150));
                        (namespace, title)
                    })
                    .collect()
            };
            let a = make(&mut rng);
            let b = make(&mut rng);
            fn refs(items: &[(NamespaceID, String)]) -> Vec<(NamespaceID, &str)> {
                items
                    .iter()
                    .map(|(namespace, title)| (*namespace, title.as_str()))
                    .collect()
            }
            let (a_refs, b_refs) = (refs(&a), refs(&b));
            for threshold in [1, 7, 50, 1000] {
                let limits = KeySetLimits { threshold };
                assert_eq!(
                    materialize(
                        key_set(&a_refs, threshold)
                            .union(key_set(&b_refs, threshold), limits)
                            .unwrap()
                    ),
                    keys_of(&a_refs).union(&keys_of(&b_refs)).cloned().collect(),
                    "union at threshold {threshold}"
                );
                assert_eq!(
                    materialize(
                        key_set(&a_refs, threshold)
                            .intersect(key_set(&b_refs, threshold), limits)
                            .unwrap()
                    ),
                    keys_of(&a_refs)
                        .intersection(&keys_of(&b_refs))
                        .cloned()
                        .collect(),
                    "intersection at threshold {threshold}"
                );
                assert_eq!(
                    materialize(
                        key_set(&a_refs, threshold)
                            .difference(key_set(&b_refs, threshold), limits)
                            .unwrap()
                    ),
                    keys_of(&a_refs)
                        .difference(&keys_of(&b_refs))
                        .cloned()
                        .collect(),
                    "difference at threshold {threshold}"
                );
            }
        }
    }
}
