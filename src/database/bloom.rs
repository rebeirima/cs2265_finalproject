use fixedbitset::FixedBitSet;
use std::hash::{BuildHasher, Hash, Hasher, RandomState};

#[derive(Debug, Default)]
pub struct Bloom {
    inner: FixedBitSet,
    random_state: RandomState,
}

impl Bloom {
    pub fn new(capacity: usize) -> Self {
        return Self {
            inner: FixedBitSet::with_capacity(capacity),
            random_state: RandomState::new(),
        };
    }

    pub fn put(&mut self, key: i32) {
        self.inner.put(self.get_index(key));
    }

    pub fn maybe_contains(&self, key: i32) -> bool {
        self.inner[self.get_index(key)]
    }

    fn get_index(&self, key: i32) -> usize {
        let mut hasher = self.random_state.build_hasher();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.inner.len()
    }
}
