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

public class JournalBufferedIterator<T> implements JournalIterator<T>, PeekingIterator<T> {
    boolean hasNext = true;
    private final List<JournalIteratorRange> ranges;
    private final Journal<T> journal;
    private final T obj;
    private int currentIndex = 0;
    private long currentRowID;
    private long currentUpperBound;
    private int currentPartitionID;

    public JournalBufferedIterator(Journal<T> journal, List<JournalIteratorRange> ranges) {
        this.ranges = ranges;
        this.journal = journal;
        this.obj = journal.newObject();
        updateVariables();
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public T next() {
        try {
            journal.clearObject(obj);
            journal.read(Rows.toRowID(currentPartitionID, currentRowID), obj);
            if (currentRowID < currentUpperBound) {
                currentRowID++;
            } else {
                currentIndex++;
                updateVariables();
            }
            return obj;
        } catch (JournalException e) {
            throw new JournalRuntimeException("Error in iterator [" + this + "]", e);
        }
    }

    @Override
    public T peekLast() {
        JournalIteratorRange w = ranges.get(ranges.size() - 1);
        try {
            journal.clearObject(obj);
            journal.read(Rows.toRowID(w.partitionID, w.hi), obj);
            return obj;
        } catch (JournalException e) {
            throw new JournalRuntimeException("Error in iterator at last element", e);
        }
    }

    @Override
    public T peekFirst() {
        JournalIteratorRange w = ranges.get(0);
        try {
            journal.clearObject(obj);
            journal.read(Rows.toRowID(w.partitionID, w.lo), obj);
            return obj;
        } catch (JournalException e) {
            throw new JournalRuntimeException("Error in iterator at last element", e);
        }
    }

    @Override
    public boolean isEmpty() {
        return ranges == null || ranges.size() == 0;
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
        return "JournalBufferedIterator{" +
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
            currentRowID = w.lo;
            currentUpperBound = w.hi;
            currentPartitionID = w.partitionID;
        } else {
            hasNext = false;
        }
    }
}
