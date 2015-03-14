/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
import com.nfsdb.utils.Rows;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED", "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS"})
public class JournalBufferedIterator<T> extends AbstractImmutableIterator<T> implements JournalPeekingIterator<T> {
    private final List<JournalIteratorRange> ranges;
    private final Journal<T> journal;
    private final T obj;
    private boolean hasNext = true;
    private int currentIndex = 0;
    private long currentRowID;
    private long currentUpperBound;
    private Partition<T> partition;

    public JournalBufferedIterator(Journal<T> journal, List<JournalIteratorRange> ranges) {
        this.ranges = ranges;
        this.journal = journal;
        this.obj = journal.newObject();
        updateVariables();
    }

    @Override
    public Journal<T> getJournal() {
        return journal;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public boolean isEmpty() {
        return ranges == null || ranges.isEmpty();
    }

    @Override
    public T next() {
        partition.read(currentRowID, obj);
        if (currentRowID < currentUpperBound) {
            currentRowID++;
        } else {
            currentIndex++;
            updateVariables();
        }
        return obj;
    }

    @Override
    public T peekFirst() {
        JournalIteratorRange w = ranges.get(0);
        try {
            journal.read(Rows.toRowID(w.partitionID, w.lo), obj);
            return obj;
        } catch (JournalException e) {
            throw new JournalRuntimeException("Error in iterator at last element", e);
        }
    }

    @Override
    public T peekLast() {
        JournalIteratorRange w = ranges.get(ranges.size() - 1);
        try {
            journal.read(Rows.toRowID(w.partitionID, w.hi), obj);
            return obj;
        } catch (JournalException e) {
            throw new JournalRuntimeException("Error in iterator at last element", e);
        }
    }

    private void updateVariables() {
        if (currentIndex < ranges.size()) {
            JournalIteratorRange w = ranges.get(currentIndex);
            currentRowID = w.lo;
            currentUpperBound = w.hi;
            try {
                partition = journal.getPartition(w.partitionID, true);
            } catch (JournalException e) {
                throw new JournalRuntimeException(e);
            }
        } else {
            hasNext = false;
        }
    }
}
