/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.iter;

import com.questdb.std.AbstractImmutableIterator;
import com.questdb.std.ImmutableIterator;

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

    private static <T, X extends ImmutableIterator<T>> ImmutableIterator<T> merge(List<X> iterators, Comparator<T> comparator, final int index) {
        if (iterators == null || iterators.isEmpty()) {
            throw new IllegalArgumentException();
        }

        if (iterators.size() - index == 1) {
            return iterators.get(index);
        }

        return new MergingIterator<T>().$new(iterators.get(index), merge(iterators, comparator, index + 1), comparator);
    }
}
