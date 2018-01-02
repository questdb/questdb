/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.store.query.iter;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class MergingPeekingIterator<T> extends MergingIterator<T> implements PeekingIterator<T> {

    public static <T, X extends PeekingIterator<T>> PeekingIterator<T> mergePeek(List<X> iterators, Comparator<T> comparator) {
        return mergePeek(iterators, comparator, 0);
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

    private static <T, X extends PeekingIterator<T>> PeekingIterator<T> mergePeek(List<X> iterators, Comparator<T> comparator, int index) {
        if (iterators == null || iterators.isEmpty()) {
            throw new IllegalArgumentException();
        }

        if (iterators.size() - index == 1) {
            return iterators.get(index);
        }

        return new MergingPeekingIterator<T>().$new(iterators.get(index), mergePeek(iterators, comparator, index + 1), comparator);
    }
}
