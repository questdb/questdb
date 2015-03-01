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

import com.nfsdb.Journal;
import com.nfsdb.Partition;
import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;

public class PartitionIterator<T> extends AbstractImmutableIterator<T> implements JournalIterator<T>, PeekingIterator<T> {
    private final long lo;
    private final long hi;
    private final Partition<T> partition;
    private long cursor;

    public PartitionIterator(Partition<T> partition, long lo, long hi) {
        this.partition = partition;
        this.lo = lo;
        this.cursor = lo;
        this.hi = hi;
    }

    @Override
    public Journal<T> getJournal() {
        return partition.getJournal();
    }

    @Override
    public boolean hasNext() {
        return cursor <= hi;
    }

    @Override
    public T next() {
        return get(cursor++);
    }

    @Override
    public boolean isEmpty() {
        return cursor > hi;
    }

    @Override
    public T peekFirst() {
        return get(lo);
    }

    @Override
    public T peekLast() {
        return get(hi);
    }

    private T get(long localRowID) {
        try {
            if (!partition.isOpen()) {
                partition.open();
            }
            return partition.read(localRowID);
        } catch (JournalException e) {
            throw new JournalRuntimeException("Cannot read partition " + partition + " at " + localRowID, e);
        }
    }
}
