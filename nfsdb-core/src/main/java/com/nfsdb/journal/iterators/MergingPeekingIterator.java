package com.nfsdb.journal.iterators;

import java.util.List;

public class MergingPeekingIterator<T> extends MergingIterator<T> implements PeekingIterator<T> {

    @Override
    public T peekLast() {
        T va = ((PeekingIterator<T>) a).peekLast();
        T vb = ((PeekingIterator<T>) b).peekLast();
        return comparator.compare(va, vb) > 0 ? va : vb;
    }

    @Override
    public T peekFirst() {
        T va = ((PeekingIterator<T>) a).peekFirst();
        T vb = ((PeekingIterator<T>) b).peekFirst();
        return comparator.compare(va, vb) < 0 ? va : vb;
    }

    @Override
    public boolean isEmpty() {
        return ((PeekingIterator<T>) a).isEmpty() || ((PeekingIterator<T>) b).isEmpty();
    }

    public PeekingIterator<T> $peeking(List<PeekingIterator<T>> iterators) {
        return $peeking(iterators, 0);
    }

    public PeekingIterator<T> $peeking(List<PeekingIterator<T>> iterators, int index) {
        if (iterators == null || iterators.size() == 0) {
            throw new IllegalArgumentException();
        }

        if (iterators.size() - index == 1) {
            return iterators.get(index);
        }

        return new MergingPeekingIterator<T>().$peeking(iterators, ++index);
    }

}
