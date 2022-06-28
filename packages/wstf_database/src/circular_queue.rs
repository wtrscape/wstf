use std::{
    iter::{Chain, Rev},
    ptr,
    slice::{Iter as SliceIter, IterMut as SliceIterMut},
};

#[derive(Clone, Debug)]
pub struct CircularQueue<T> {
    data: Vec<T>,
    capacity: usize,
    insertion_index: usize,
    reverse_idx: usize,
}

pub type Iter<'a, T> = Chain<Rev<SliceIter<'a, T>>, Rev<SliceIter<'a, T>>>;
pub type IterMut<'a, T> = Chain<Rev<SliceIterMut<'a, T>>, Rev<SliceIterMut<'a, T>>>;

impl<T> CircularQueue<T> {
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        if capacity == 0 {
            panic!("capacity must be greater than 0");
        }

        Self {
            data: Vec::with_capacity(capacity),
            capacity,
            insertion_index: 0,
            reverse_idx: 0,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    pub fn clear(&mut self) {
        self.data.clear();
        self.insertion_index = 0;
    }

    pub fn push(&mut self, x: T) {
        if self.data.len() < self.capacity() {
            self.data.push(x);
        } else {
            self.data[self.insertion_index] = x;
        }

        if self.reverse_idx > 0 {
            self.reverse_idx -= 1;
        }

        self.insertion_index = (self.insertion_index + 1) % self.capacity();
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.data.len() == 0 {
            None
        } else if self.data.len() < self.capacity() {
            self.data.pop()
        } else if self.reverse_idx == self.capacity() {
            None
        } else {
            self.reverse_idx += 1;
            self.insertion_index += self.capacity - 1;
            self.insertion_index %= self.capacity;
            unsafe { Some(ptr::read(self.data.get_unchecked(self.insertion_index))) }
        }
    }

    #[inline]
    pub fn iter(&self) -> Iter<T> {
        let (a, b) = self.data.split_at(self.insertion_index);
        a.iter().rev().chain(b.iter().rev())
    }

    #[inline]
    pub fn iter_mut(&mut self) -> IterMut<T> {
        let (a, b) = self.data.split_at_mut(self.insertion_index);
        a.iter_mut().rev().chain(b.iter_mut().rev())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn zero_capacity() {
        let _ = CircularQueue::<i32>::with_capacity(0);
    }

    #[test]
    fn empty_queue() {
        let q = CircularQueue::<i32>::with_capacity(5);

        assert_eq!(q.iter().next(), None);
    }

    #[test]
    fn partially_full_queue() {
        let mut q = CircularQueue::with_capacity(5);
        q.push(1);
        q.push(2);
        q.push(3);

        assert_eq!(q.len(), 3);

        let res: Vec<_> = q.iter().map(|&x| x).collect();
        assert_eq!(res, [3, 2, 1]);
    }

    #[test]
    fn full_queue() {
        let mut q = CircularQueue::with_capacity(5);
        q.push(1);
        q.push(2);
        q.push(3);
        q.push(4);
        q.push(5);

        assert_eq!(q.len(), 5);

        let res: Vec<_> = q.iter().map(|&x| x).collect();
        assert_eq!(res, [5, 4, 3, 2, 1]);
    }

    #[test]
    fn over_full_queue() {
        let mut q = CircularQueue::with_capacity(5);
        q.push(1);
        q.push(2);
        q.push(3);
        q.push(4);
        q.push(5);
        q.push(6);
        q.push(7);

        assert_eq!(q.len(), 5);

        let res: Vec<_> = q.iter().map(|&x| x).collect();
        assert_eq!(res, [7, 6, 5, 4, 3]);
    }

    #[test]
    fn clear() {
        let mut q = CircularQueue::with_capacity(5);
        q.push(1);
        q.push(2);
        q.push(3);
        q.push(4);
        q.push(5);
        q.push(6);
        q.push(7);

        q.clear();

        assert_eq!(q.len(), 0);
        assert_eq!(q.iter().next(), None);

        q.push(1);
        q.push(2);
        q.push(3);

        assert_eq!(q.len(), 3);

        let res: Vec<_> = q.iter().map(|&x| x).collect();
        assert_eq!(res, [3, 2, 1]);
    }

    #[test]
    fn popping_then_pushing() {
        let mut q = CircularQueue::with_capacity(5);
        q.push(1);
        q.push(2);
        q.push(3);
        q.push(4);
        q.push(5);
        q.push(6);
        q.push(7);

        let res = q.pop();
        assert_eq!(res, Some(7));
        let res = q.pop();
        assert_eq!(res, Some(6));
        let res = q.pop();
        assert_eq!(res, Some(5));
        let res = q.pop();
        assert_eq!(res, Some(4));
        let res = q.pop();
        assert_eq!(res, Some(3));
        let res = q.pop();
        assert_eq!(res, None);

        q.push(1);
        q.push(2);
        q.push(3);
        q.push(4);
        q.push(5);
        q.push(6);
        q.push(7);

        let res = q.pop();
        assert_eq!(res, Some(7));
        let res = q.pop();
        assert_eq!(res, Some(6));
        let res = q.pop();
        assert_eq!(res, Some(5));
        let res = q.pop();
        assert_eq!(res, Some(4));
        let res = q.pop();
        assert_eq!(res, Some(3));
        let res = q.pop();
        assert_eq!(res, None);
    }

    #[test]
    fn popping() {
        let mut q = CircularQueue::with_capacity(5);
        q.push(1);
        q.push(2);
        q.push(3);
        q.push(4);
        q.push(5);
        q.push(6);
        q.push(7);

        let res = q.pop();
        assert_eq!(res, Some(7));
        let res = q.pop();
        assert_eq!(res, Some(6));
        let res = q.pop();
        assert_eq!(res, Some(5));
        let res = q.pop();
        assert_eq!(res, Some(4));
        let res = q.pop();
        assert_eq!(res, Some(3));
        let res = q.pop();
        assert_eq!(res, None);
    }

    #[test]
    fn mutable_iterator() {
        let mut q = CircularQueue::with_capacity(5);
        q.push(1);
        q.push(2);
        q.push(3);
        q.push(4);
        q.push(5);
        q.push(6);
        q.push(7);

        for x in q.iter_mut() {
            *x *= 2;
        }

        let res: Vec<_> = q.iter().map(|&x| x).collect();
        assert_eq!(res, [14, 12, 10, 8, 6]);
    }

    #[test]
    fn zero_sized() {
        let mut q = CircularQueue::with_capacity(3);
        assert_eq!(q.capacity(), 3);

        q.push(());
        q.push(());
        q.push(());
        q.push(());

        assert_eq!(q.len(), 3);

        let mut iter = q.iter();
        assert_eq!(iter.next(), Some(&()));
        assert_eq!(iter.next(), Some(&()));
        assert_eq!(iter.next(), Some(&()));
        assert_eq!(iter.next(), None);
    }
}
