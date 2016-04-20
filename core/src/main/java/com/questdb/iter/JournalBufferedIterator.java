/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
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
 ******************************************************************************/

package com.questdb.iter;

import com.questdb.Journal;
import com.questdb.Partition;
import com.questdb.ex.JournalException;
import com.questdb.ex.JournalRuntimeException;
import com.questdb.misc.Rows;
import com.questdb.std.AbstractImmutableIterator;
import com.questdb.std.ObjList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED", "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS"})
public class JournalBufferedIterator<T> extends AbstractImmutableIterator<T> implements JournalPeekingIterator<T> {
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
