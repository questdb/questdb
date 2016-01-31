/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.iter;

import com.nfsdb.Journal;
import com.nfsdb.Partition;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.std.AbstractImmutableIterator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS"})
public class PartitionBufferedIterator<T> extends AbstractImmutableIterator<T> implements JournalIterator<T>, PeekingIterator<T> {
    private final long hi;
    private final long lo;
    private final T obj;
    private final Partition<T> partition;
    private long cursor;

    @SuppressFBWarnings({"CD_CIRCULAR_DEPENDENCY"})
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
