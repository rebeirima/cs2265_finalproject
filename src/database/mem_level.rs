use std::{collections::BTreeMap, fs, ops::Deref, path::Path};

use super::{
    table::{BlockMut, Command, Table, TableBuilder, TableView},
    GetResult,
};

pub struct MemLevel {
    data: BTreeMap<i32, Option<i32>>,
}

impl Deref for MemLevel {
    type Target = BTreeMap<i32, Option<i32>>;

    fn deref(&self) -> &Self::Target {
        return &self.data;
    }
}

impl MemLevel {
    pub fn new(data_directory: &Path) -> Self {
        let level_directory = data_directory.join("level0");
        fs::create_dir_all(&level_directory).unwrap();

        let mut res = Self {
            data: BTreeMap::new(),
        };

        if let Some(Ok(entry)) = fs::read_dir(&level_directory).unwrap().into_iter().next() {
            for command in
                TableView::new(entry.path(), 0).flat_map(|b| unsafe { b.as_ref().unwrap().iter() })
            {
                match command {
                    Command::Delete(key) => res.delete(key),
                    Command::Put(key, val) => res.insert(key, val),
                };
            }
            let _ = fs::remove_file(&entry.path());
        };

        return res;
    }

    pub fn insert(&mut self, key: i32, value: i32) {
        self.data.insert(key, Some(value));
    }

    pub fn delete(&mut self, key: i32) {
        self.data.insert(key, None);
    }

    pub fn get(&self, key: i32) -> GetResult {
        match self.data.get(&key).cloned() {
            None => GetResult::NotFound,
            Some(None) => GetResult::Deleted,
            Some(Some(val)) => GetResult::Value(val),
        }
    }

    pub fn write_to_table(&self, to_dir: &Path) -> Table {
        let mut iter = self.iter();
        let mut tb = TableBuilder::new(to_dir);

        let mut block = BlockMut::new();
        while let Some((&key, &val)) = iter.next() {
            let command = match val {
                None => Command::Delete(key),
                Some(val) => Command::Put(key, val),
            };

            if !block.push_command(command) {
                tb.insert_block(&block);
                block.clear();
                block.push_command(command);
            }
        }
        if !block.is_empty() {
            tb.insert_block(&block);
        }

        tb.build()
    }

    pub fn clear(&mut self) -> MemLevel {
        let data = std::mem::take(&mut self.data);
        MemLevel { data }
    }
}
