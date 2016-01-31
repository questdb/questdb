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
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.misc.Rows;
import com.nfsdb.std.AbstractImmutableIterator;
import com.nfsdb.std.ObjList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
public class JournalIteratorImpl<T> extends AbstractImmutableIterator<T> implements JournalPeekingIterator<T> {
    private final ObjList<JournalIteratorRange> ranges;
    private final Journal<T> journal;
    private boolean hasNext = true;
    private int currentIndex = 0;
    private long currentRowID;
    private long currentUpperBound;
    private int currentPartitionID;

    public JournalIteratorImpl(Journal<T> journal, ObjList<JournalIteratorRange> ranges) {
        this.ranges = ranges;
        this.journal = journal;
        updateVariables();
        hasNext = hasNext && currentRowID <= currentUpperBound;
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
    public T next() {
        try {
            T result = journal.read(Rows.toRowID(currentPartitionID, currentRowID));
            if (currentRowID < currentUpperBound) {
                currentRowID++;
            } else {
                currentIndex++;
                updateVariables();
            }
            return result;
        } catch (JournalException e) {
            throw new JournalRuntimeException("Error in iterator [%s]", e, this);
        }
    }

    @Override
    public boolean isEmpty() {
        return ranges == null || ranges.size() == 0;
    }

    @Override
    public T peekFirst() {
        JournalIteratorRange w = ranges.get(0);
        try {
            return journal.read(Rows.toRowID(w.partitionID, w.lo));
        } catch (JournalException e) {
            throw new JournalRuntimeException("Error in iterator at first element", e);
        }
    }

    @Override
    public T peekLast() {
        JournalIteratorRange w = ranges.getLast();
        try {
            return journal.read(Rows.toRowID(w.partitionID, w.hi));
        } catch (JournalException e) {
            throw new JournalRuntimeException("Error in iterator at last element", e);
        }
    }

    @Override
    public String toString() {
        return "JournalIteratorImpl{" +
                "currentRowID=" + currentRowID +
                ", currentUpperBound=" + currentUpperBound +
                ", currentPartitionID=" + currentPartitionID +
                ", currentIndex=" + currentIndex +
                ", journal=" + journal +
                '}';
    }

    private void updateVariables() {
        if (currentIndex < ranges.size()) {
            JournalIteratorRange w = ranges.getQuick(currentIndex);
            currentRowID = w.lo;
            currentUpperBound = w.hi;
            currentPartitionID = w.partitionID;
        } else {
            hasNext = false;
        }
    }

}
