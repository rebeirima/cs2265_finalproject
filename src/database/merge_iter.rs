use std::{cmp::Ordering, iter::Peekable};

use super::table::Command;

pub struct MergeCommands<I1, I2>
where
    I1: Iterator<Item = Command>,
    I2: Iterator<Item = Command>,
{
    iter1: Peekable<I1>,
    iter2: Peekable<I2>,
}

impl<I1, I2> Iterator for MergeCommands<I1, I2>
where
    I1: Iterator<Item = Command>,
    I2: Iterator<Item = Command>,
{
    type Item = Command;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.iter1.peek(), self.iter2.peek()) {
            (Some(&v1), Some(&v2)) => match v1.key().cmp(&v2.key()) {
                Ordering::Less => self.iter1.next(),
                Ordering::Greater => self.iter2.next(),
                Ordering::Equal => {
                    self.iter2.next(); // ignore older command in iter2
                    self.iter1.next()
                }
            },
            (Some(_), None) => self.iter1.next(),
            (None, Some(_)) => self.iter2.next(),
            (None, None) => None,
        }
    }
}

pub fn merge_sorted_commands<I1, I2>(iter1: I1, iter2: I2) -> MergeCommands<I1, I2>
where
    I1: Iterator<Item = Command>,
    I2: Iterator<Item = Command>,
{
    MergeCommands {
        iter1: iter1.peekable(),
        iter2: iter2.peekable(),
    }
}
