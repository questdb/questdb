/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
