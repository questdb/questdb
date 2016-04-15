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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.iter;

import com.nfsdb.std.AbstractImmutableIterator;
import com.nfsdb.std.ImmutableIterator;

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
