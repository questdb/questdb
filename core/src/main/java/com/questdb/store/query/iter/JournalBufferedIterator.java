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

import com.questdb.std.ObjList;
import com.questdb.std.Rows;
import com.questdb.std.ex.JournalException;
import com.questdb.store.Journal;
import com.questdb.store.JournalRuntimeException;
import com.questdb.store.Partition;

public class JournalBufferedIterator<T> implements JournalPeekingIterator<T>, com.questdb.std.ImmutableIterator<T> {
    private final ObjList<JournalIteratorRange> ranges;
    private final Journal<T> journal;
    private final T obj;
    private boolean hasNext = true;
    private int currentIndex = 0;
    private long currentRowID;
    private long currentUpperBound;
    private Partition<T> partition;

    public JournalBufferedIterator(Journal<T> journal, ObjList<JournalIteratorRange> ranges) {
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
    public boolean isEmpty() {
        return ranges == null || ranges.size() == 0;
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
        JournalIteratorRange w = ranges.getLast();
        try {
            journal.read(Rows.toRowID(w.partitionID, w.hi), obj);
            return obj;
        } catch (JournalException e) {
            throw new JournalRuntimeException("Error in iterator at last element", e);
        }
    }

    private void updateVariables() {
        if (currentIndex < ranges.size()) {
            JournalIteratorRange w = ranges.getQuick(currentIndex);
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
