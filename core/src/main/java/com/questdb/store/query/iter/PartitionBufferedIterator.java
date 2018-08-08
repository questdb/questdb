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

import com.questdb.std.ex.JournalException;
import com.questdb.store.Journal;
import com.questdb.store.JournalRuntimeException;
import com.questdb.store.Partition;

public class PartitionBufferedIterator<T> implements JournalIterator<T>, PeekingIterator<T>, com.questdb.std.ImmutableIterator<T> {
    private final long hi;
    private final long lo;
    private final T obj;
    private final Partition<T> partition;
    private long cursor;

    public PartitionBufferedIterator(Partition<T> partition, long lo, long hi) {
        this.lo = lo;
        this.cursor = lo;
        this.hi = hi;
        this.obj = partition.getJournal().newObject();
        this.partition = partition;
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
            partition.read(localRowID, obj);
            return obj;
        } catch (JournalException e) {
            throw new JournalRuntimeException("Cannot read partition " + partition + " at " + localRowID, e);
        }
    }
}
