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

package com.nfsdb.journal.iterators;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalImmutableIteratorException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.utils.Rows;

import java.util.Iterator;
import java.util.List;

public class JournalIteratorImpl<T> implements JournalIterator<T> {
    private final List<JournalIteratorRange> ranges;
    private final Journal<T> journal;
    private boolean hasNext = true;
    private int currentIndex = 0;
    private long currentRowID;
    private long currentUpperBound;
    private int currentPartitionID;

    public JournalIteratorImpl(Journal<T> journal, List<JournalIteratorRange> ranges) {
        this.ranges = ranges;
        this.journal = journal;
        updateVariables();
        hasNext = hasNext && currentRowID <= currentUpperBound;
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
            throw new JournalRuntimeException("Error in iterator [" + this + "]", e);
        }
    }

    @Override
    public void remove() {
        throw new JournalImmutableIteratorException();
    }

    @Override
    public Iterator<T> iterator() {
        return this;
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

    @Override
    public Journal<T> getJournal() {
        return journal;
    }

    private void updateVariables() {
        if (currentIndex < ranges.size()) {
            JournalIteratorRange w = ranges.get(currentIndex);
            currentRowID = w.lowerRowIDBound;
            currentUpperBound = w.upperRowIDBound;
            currentPartitionID = w.partitionID;
        } else {
            hasNext = false;
        }
    }

}
