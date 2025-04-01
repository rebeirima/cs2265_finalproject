use std::fmt::Write;
use std::{
    cmp::Ordering,
    collections::HashMap,
    path::{Path, PathBuf},
};

use bytes::Buf;
use disk_level::DiskLevel;
use mem_level::MemLevel;
use merge_iter::merge_sorted_commands;
use table::{BlockMut, Command, Table, TableBuilder};
use tokio::sync::RwLock;

use crate::config::{MAX_FILE_SIZE_BYTES, MEM_CAPACITY, NUM_LEVELS};

pub mod bloom;
pub mod disk_level;
pub mod mem_level;
pub mod merge_iter;
pub mod once_done;
pub mod table;

/// TODO: explain how I compact levels

pub enum GetResult {
    NotFound,
    Deleted,
    Value(i32),
}

pub struct Database {
    data_directory: PathBuf,
    memory: RwLock<MemLevel>,
    disk: [RwLock<DiskLevel>; NUM_LEVELS],
}

impl Database {
    pub fn new(data_directory: PathBuf) -> Self {
        let memory = MemLevel::new(&data_directory);
        let disk: [RwLock<DiskLevel>; NUM_LEVELS] = std::array::from_fn(|idx| {
            RwLock::new(DiskLevel::new(&data_directory, (idx + 1) as u32))
        });

        Self {
            data_directory,
            memory: RwLock::new(memory),
            disk,
        }
    }

    pub async fn insert(&self, key: i32, value: i32) {
        let mut mem_write = self.memory.write().await;
        mem_write.insert(key, value);

        // keep holding the write guard so
        if mem_write.len() >= MEM_CAPACITY as usize {
            let old_mem = mem_write.clear();
            drop(mem_write);
            self.handle_overflow(old_mem).await;
        }
    }

    pub async fn load(&self, mut data: &[u8]) {
        // a bit better than multiple calls to insert as locks are kept to a minimum
        let mut mem_write = self.memory.write().await;
        while data.has_remaining() {
            let key = data.get_i32();
            let val = data.get_i32();

            mem_write.insert(key, val);

            if mem_write.len() >= MEM_CAPACITY as usize {
                let old_mem = mem_write.clear();
                drop(mem_write);
                self.handle_overflow(old_mem).await;
                mem_write = self.memory.write().await;
            }
        }
    }

    pub async fn delete(&self, key: i32) {
        let mut mem_write = self.memory.write().await;
        mem_write.delete(key);
        if mem_write.len() >= MEM_CAPACITY as usize {
            let old_mem = mem_write.clear();
            drop(mem_write);
            self.handle_overflow(old_mem).await;
        }
    }

    async fn handle_overflow(&self, mem: MemLevel) {
        let l0_table = mem.write_to_table(self.data_directory.join("level0").as_path());

        let mut cur = self.disk[0].write().await;
        merge(&mut vec![l0_table], &mut cur);

        for i in 0..(NUM_LEVELS - 1) {
            if cur.is_over_file_capacity() {
                if cur.average_table_utilization() <= 0.5 {
                    compact_in_place(&mut cur);
                    assert!(!cur.is_over_file_capacity());
                    break;
                }
                let mut next = self.disk[i + 1].write().await;
                merge(&mut cur.tables, &mut next);
                cur = next;
            } else {
                break;
            }
        }

        if cur.is_over_file_capacity() {
            compact_in_place(&mut cur);
        }
    }

    pub async fn get(&self, key: i32) -> Option<i32> {
        match self.memory.read().await.get(key) {
            GetResult::Deleted => return None,
            GetResult::Value(val) => return Some(val),
            GetResult::NotFound => {}
        };

        for i in 0..NUM_LEVELS {
            match self.disk[i].read().await.get(key) {
                GetResult::Deleted => return None,
                GetResult::Value(val) => return Some(val),
                GetResult::NotFound => {}
            };
        }

        None
    }

    pub async fn range(
        &self,
        min_key: i32,
        max_key: i32,
    ) -> Option<impl Iterator<Item = (i32, i32)>> {
        if min_key > max_key {
            return None;
        }

        let mut res: HashMap<i32, Option<i32>> = HashMap::new();

        let mem = self.memory.read().await;
        for (&key, &val) in mem.range(min_key..=max_key) {
            res.insert(key, val);
        }

        let mut cur_level = self.disk[0].read().await;
        drop(mem); // drop here instead of before so no writer can write to lvl1

        for i in 0..NUM_LEVELS {
            if !cur_level.tables.is_empty() {
                if let Some(locate_min) = cur_level.locate_nearest(min_key) {
                    for command in cur_level.tables[locate_min.table_index]
                        .iter_commands_from(locate_min.block_index, false)
                        .chain(
                            (&cur_level
                                .tables
                                .get(locate_min.table_index..)
                                .unwrap_or(&[]))
                                .iter()
                                .flat_map(|t| t.iter_commands_from(0, false)),
                        )
                    {
                        if command.key() < min_key {
                            continue;
                        }

                        if command.key() > max_key {
                            break;
                        }

                        res.entry(command.key()).or_insert(command.value());
                    }
                }
            }

            if let Some(next) = self.disk.get(i + 1) {
                let next_level = next.read().await;
                cur_level = next_level;
            }
        }

        if res.is_empty() {
            None
        } else {
            Some(res.into_iter().filter_map(|(key, val)| Some((key, val?))))
        }
    }

    pub async fn write_stats(&self, to: &mut String) {
        let mut tally: HashMap<i32, bool> = HashMap::new();
        let mut level_counts = [0_usize; NUM_LEVELS + 1];

        to.push_str("\n---------------- Dump ----------------\n");

        let mem = self.memory.read().await;
        for (&key, &val) in mem.iter() {
            if let Some(value) = val {
                write!(to, "{key}:{value}:L0 ").unwrap();
                level_counts[0] += 1;
            }
            tally.insert(key, val.is_some());
        }

        to.push_str("\n\n");

        let mut cur_level = self.disk[0].read().await;
        drop(mem);

        for i in 0..NUM_LEVELS {
            if !cur_level.tables.is_empty() {
                for command in cur_level
                    .tables
                    .iter()
                    .flat_map(|t| t.iter_commands_from(0, false))
                {
                    if let Command::Put(key, val) = command {
                        write!(to, "{key}:{val}:L{} ", i + 1).unwrap();
                        level_counts[i + 1] += 1;
                    }
                    tally
                        .entry(command.key())
                        .or_insert(command.value().is_some());
                }
                to.push_str("\n\n");
            }

            if let Some(next) = self.disk.get(i + 1) {
                let next_level = next.read().await;
                cur_level = next_level;
            }
        }

        to.push_str("\n---------------- TLDR ----------------\n");

        writeln!(
            to,
            "Logical Pairs: {}",
            tally.into_values().filter(|v| *v).count()
        )
        .unwrap();
        for (idx, counts) in level_counts.into_iter().enumerate() {
            if counts == 0 {
                continue;
            }
            writeln!(to, "LVL{idx}: {counts}").unwrap();
        }
    }

    pub fn cleanup(self) {
        let mem = self.memory.into_inner();

        if !mem.is_empty() {
            mem.write_to_table(self.data_directory.join("level0").as_path());
        }
    }
}

fn build_tables<I: Iterator<Item = Command>>(mut iter: I, to_dir: &Path) -> Vec<Table> {
    let mut block = BlockMut::new();
    let mut new_tables = vec![];

    let mut tb = TableBuilder::new(to_dir);
    while let Some(command) = iter.next() {
        if !block.push_command(command) {
            tb.insert_block(&block);

            if tb.full() {
                let new_table = tb.build();
                tb = TableBuilder::new(to_dir);
                new_tables.push(new_table);
            }
            block.clear();
            block.push_command(command);
        }
    }
    if !block.is_empty() {
        tb.insert_block(&block);
        block.clear();
    }
    if !tb.is_empty() {
        new_tables.push(tb.build());
    }

    new_tables
}

fn compact_in_place(level: &mut DiskLevel) {
    let first_partial_table = level
        .tables
        .iter()
        .position(|t| t.file_size < MAX_FILE_SIZE_BYTES as u64)
        .unwrap();
    let partial_tables = level.tables.split_off(first_partial_table);

    let commands = partial_tables
        .iter()
        .flat_map(|t| t.iter_commands_from(0, true));

    let mut new_tables = build_tables(commands, &level.level_directory);
    level.tables.append(&mut new_tables);
}

fn merge(l1: &mut Vec<Table>, l2: &mut DiskLevel) {
    let intersections = find_intersections(l1, &l2.tables);

    match intersections {
        IntersectionResult::NoIntersections(indices) => {
            for &idx in indices.iter().rev() {
                let table = &mut l1[idx];
                table.rename(&l2.level_directory);
                l2.tables.push(l1.remove(idx));
            }
        }
        IntersectionResult::IntersectingGroups(groups) => {
            let mut new_tables = vec![];

            for group in groups.iter() {
                let (slice_start, slice_end) = group.tables1;
                let l1_commands = (&mut l1[slice_start..slice_end])
                    .iter()
                    .flat_map(|t| t.iter_commands_from(0, true));

                let (slice_start, slice_end) = group.tables2;
                let l2_commands = (&mut l2.tables[slice_start..slice_end])
                    .iter()
                    .flat_map(|t| t.iter_commands_from(0, true));

                let merge_commands_iter = merge_sorted_commands(l1_commands, l2_commands);
                new_tables.append(&mut build_tables(merge_commands_iter, &l2.level_directory));
            }

            for idx in groups.iter().flat_map(|g| g.tables1.0..g.tables1.1).rev() {
                l1.remove(idx);
            }

            for idx in groups.iter().flat_map(|g| g.tables2.0..g.tables2.1).rev() {
                l2.tables.remove(idx);
            }

            l2.tables.append(&mut new_tables);
        }
    }

    l2.sort_tables();
}

enum IntersectionResult {
    NoIntersections(Vec<usize>),
    IntersectingGroups(Vec<IntersectionGroup>),
}

struct IntersectionGroup {
    tables1: (usize, usize),
    tables2: (usize, usize),
}

fn find_intersections<'a>(tables_l1: &'a [Table], tables_l2: &'a [Table]) -> IntersectionResult {
    let mut non_intersecting = Vec::new();
    let mut intersecting_groups = Vec::new();

    let mut i = 0;
    let mut j = 0;

    while i < tables_l1.len() {
        let start_i = i;

        while j < tables_l2.len() && tables_l1[i].intersects(&tables_l2[j]) == Ordering::Greater {
            j += 1;
        }

        let start_j = j;

        let mut intersected = false;
        while j < tables_l2.len() && tables_l1[i].intersects(&tables_l2[j]) == Ordering::Equal {
            intersected = true;
            j += 1;
        }

        if intersected {
            i += 1;

            while i < tables_l1.len() {
                let intersects_prev = tables_l1[i].intersects(&tables_l2[j - 1]);
                let intersects_cur = if j < tables_l2.len() {
                    tables_l1[i].intersects(&tables_l2[j])
                } else {
                    Ordering::Less
                };
                if intersects_prev == Ordering::Equal || intersects_cur == Ordering::Equal {
                    if intersects_cur == Ordering::Equal {
                        j += 1;
                    }
                    while j < tables_l2.len()
                        && tables_l1[i].intersects(&tables_l2[j]) == Ordering::Equal
                    {
                        j += 1;
                    }
                } else {
                    break;
                }
                i += 1;
            }

            intersecting_groups.push(IntersectionGroup {
                tables1: (start_i, i),
                tables2: (start_j, j),
            });
        } else {
            non_intersecting.push(i);
            i += 1;
        }
    }

    if !non_intersecting.is_empty() {
        IntersectionResult::NoIntersections(non_intersecting)
    } else {
        IntersectionResult::IntersectingGroups(intersecting_groups)
    }
}
