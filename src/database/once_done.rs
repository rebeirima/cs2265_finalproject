pub struct OnceDone<I: Iterator, F: FnMut(&mut I)> {
    fun: F,
    iter: I,
}

impl<I: Iterator, F: FnMut(&mut I)> Iterator for OnceDone<I, F> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            None => {
                (self.fun)(&mut self.iter);
                None
            }
            Some(v) => Some(v),
        }
    }
}

pub trait OnceDoneTrait: Iterator + Sized {
    fn once_done<F: FnMut(&mut Self)>(self, fun: F) -> OnceDone<Self, F> {
        OnceDone { fun, iter: self }
    }
}

impl<T: Iterator + Sized> OnceDoneTrait for T {}
