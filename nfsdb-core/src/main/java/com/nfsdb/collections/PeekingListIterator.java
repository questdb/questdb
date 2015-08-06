/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.collections;

import com.nfsdb.query.iterator.PeekingIterator;

import java.util.Iterator;
import java.util.List;

public class PeekingListIterator<T> extends AbstractImmutableIterator<T> implements PeekingIterator<T> {
    private List<T> delegate;
    private Iterator<T> iterator;

    public PeekingListIterator() {
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        return iterator.next();
    }

    @Override
    public boolean isEmpty() {
        return delegate == null || delegate.isEmpty();
    }

    @Override
    public T peekFirst() {
        return delegate.get(0);
    }

    @Override
    public T peekLast() {
        return delegate.get(delegate.size() - 1);
    }

    public void setDelegate(List<T> delegate) {
        this.delegate = delegate;
        this.iterator = delegate.iterator();
    }
}
