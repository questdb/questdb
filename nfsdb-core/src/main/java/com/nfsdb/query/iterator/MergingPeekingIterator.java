/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/
package com.nfsdb.query.iterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class MergingPeekingIterator<T> extends MergingIterator<T> implements PeekingIterator<T> {

    public static <T, X extends PeekingIterator<T>> PeekingIterator<T> mergePeek(List<X> iterators, Comparator<T> comparator) {
        return mergePeek(iterators, comparator, 0);
    }

    private static <T, X extends PeekingIterator<T>> PeekingIterator<T> mergePeek(List<X> iterators, Comparator<T> comparator, int index) {
        if (iterators == null || iterators.isEmpty()) {
            throw new IllegalArgumentException();
        }

        if (iterators.size() - index == 1) {
            return iterators.get(index);
        }

        return new MergingPeekingIterator<T>().$new(iterators.get(index), mergePeek(iterators, comparator, ++index), comparator);
    }

    public MergingPeekingIterator<T> $new(Iterator<T> a, Iterator<T> b, Comparator<T> comparator) {
        this.a = a;
        this.b = b;
        this.comparator = comparator;
        this.nextA = null;
        this.nextB = null;
        return this;
    }

    @Override
    public boolean isEmpty() {
        return ((PeekingIterator<T>) a).isEmpty() || ((PeekingIterator<T>) b).isEmpty();
    }

    @Override
    public T peekFirst() {
        T va = ((PeekingIterator<T>) a).peekFirst();
        T vb = ((PeekingIterator<T>) b).peekFirst();
        return comparator.compare(va, vb) < 0 ? va : vb;
    }

    @Override
    public T peekLast() {
        T va = ((PeekingIterator<T>) a).peekLast();
        T vb = ((PeekingIterator<T>) b).peekLast();
        return comparator.compare(va, vb) > 0 ? va : vb;
    }
}
