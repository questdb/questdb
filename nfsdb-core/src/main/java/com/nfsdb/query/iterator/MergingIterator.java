/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.query.iterator;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.collections.ImmutableIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class MergingIterator<T> extends AbstractImmutableIterator<T> {

    Iterator<T> a;
    Iterator<T> b;
    Comparator<T> comparator;
    T nextA;
    T nextB;

    public static <T, X extends ImmutableIterator<T>> ImmutableIterator<T> merge(List<X> iterators, Comparator<T> comparator) {
        return merge(iterators, comparator, 0);
    }

    public MergingIterator<T> $new(Iterator<T> a, Iterator<T> b, Comparator<T> comparator) {
        this.a = a;
        this.b = b;
        this.comparator = comparator;
        this.nextA = null;
        this.nextB = null;
        return this;
    }

    @Override
    public boolean hasNext() {
        return nextA != null || a.hasNext() || nextB != null || b.hasNext();
    }

    @Override
    public T next() {
        T result;

        if (nextA == null && a.hasNext()) {
            nextA = a.next();
        }

        if (nextB == null && b.hasNext()) {
            nextB = b.next();
        }

        if (nextB == null || (nextA != null && comparator.compare(nextA, nextB) < 0)) {
            result = nextA;
            nextA = null;
        } else {
            result = nextB;
            nextB = null;
        }

        return result;
    }

    private static <T, X extends ImmutableIterator<T>> ImmutableIterator<T> merge(List<X> iterators, Comparator<T> comparator, int index) {
        if (iterators == null || iterators.isEmpty()) {
            throw new IllegalArgumentException();
        }

        if (iterators.size() - index == 1) {
            return iterators.get(index);
        }

        return new MergingIterator<T>().$new(iterators.get(index), merge(iterators, comparator, ++index), comparator);
    }
}
