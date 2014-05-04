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

package com.nfsdb.journal.iterators;

import java.util.Comparator;
import java.util.Iterator;

public class MergingIterator<T> implements Iterator<T>, Iterable<T> {

    private final Iterator<T> a;
    private final Iterator<T> b;
    private final Comparator<T> comparator;
    private T nextA;
    private T nextB;

    public MergingIterator(Iterator<T> a, Iterator<T> b, Comparator<T> comparator) {
        this.a = a;
        this.b = b;
        this.comparator = comparator;
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

    @Override
    public void remove() {

    }

    @Override
    public Iterator<T> iterator() {
        return this;
    }
}
