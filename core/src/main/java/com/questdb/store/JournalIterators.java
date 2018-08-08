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

package com.questdb.store;

import com.questdb.std.ObjList;
import com.questdb.std.Rows;
import com.questdb.std.ex.JournalException;
import com.questdb.store.query.OrderedResultSetBuilder;
import com.questdb.store.query.iter.*;

import java.util.Iterator;

public final class JournalIterators {

    private JournalIterators() {
    }

    public static <T> JournalPeekingIterator<T> bufferedIterator(Journal<T> journal) {
        return new JournalBufferedIterator<>(journal, createRanges(journal));
    }

    public static <T> JournalPeekingIterator<T> bufferedIterator(Journal<T> journal, Interval interval) {
        return new JournalBufferedIterator<>(journal, createRanges(journal, interval));
    }

    public static <T> JournalPeekingIterator<T> bufferedIterator(Journal<T> journal, long rowid) {
        return new JournalBufferedIterator<>(journal, createRanges(journal, rowid));
    }

    public static <T> JournalConcurrentIterator<T> concurrentIterator(Journal<T> journal) {
        return new JournalConcurrentIterator<>(journal, createRanges(journal), 1024);
    }

    public static <T> JournalConcurrentIterator<T> concurrentIterator(Journal<T> journal, Interval interval) {
        return new JournalConcurrentIterator<>(journal, createRanges(journal, interval), 1024);
    }

    public static <T> JournalConcurrentIterator<T> concurrentIterator(Journal<T> journal, long rowid) {
        return new JournalConcurrentIterator<>(journal, createRanges(journal, rowid), 1024);
    }

    public static <T> JournalPeekingIterator<T> incrementBufferedIterator(Journal<T> journal) {
        try {
            long lo = journal.getMaxRowID();
            journal.refresh();
            return new JournalBufferedIterator<>(journal, createRanges(journal, journal.incrementRowID(lo)));
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    public static <T> JournalPeekingIterator<T> incrementIterator(Journal<T> journal) {
        try {
            long lo = journal.getMaxRowID();
            journal.refresh();
            return new JournalIteratorImpl<>(journal, createRanges(journal, journal.incrementRowID(lo)));
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    public static <T> JournalIterator<T> iterator(Journal<T> journal, Interval interval) {
        return new JournalIteratorImpl<>(journal, createRanges(journal, interval));
    }

    public static <T> JournalPeekingIterator<T> iterator(Journal<T> journal, long rowid) {
        return new JournalIteratorImpl<>(journal, createRanges(journal, rowid));
    }

    public static <T> Iterator<T> iterator(Journal<T> journal) {
        return new JournalIteratorImpl<>(journal, createRanges(journal));
    }

    private static <T> ObjList<JournalIteratorRange> createRanges(Journal<T> journal) {
        final int partitionCount = journal.getPartitionCount();
        ObjList<JournalIteratorRange> ranges = new ObjList<>(partitionCount);
        try {
            for (int i = 0; i < partitionCount; i++) {
                Partition<T> p = journal.getPartition(i, true);
                long size = p.size();
                if (size > 0) {
                    ranges.add(new JournalIteratorRange(p.getPartitionIndex(), 0, size - 1));
                }
            }
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
        return ranges;
    }

    private static <T> ObjList<JournalIteratorRange> createRanges(Journal<T> journal, Interval interval) {
        final ObjList<JournalIteratorRange> ranges = new ObjList<>();
        try {
            journal.iteratePartitions(new OrderedResultSetBuilder<T>(interval) {
                @Override
                public void read(long lo, long hi) {
                    ranges.add(new JournalIteratorRange(partition.getPartitionIndex(), lo, hi));
                }
            });
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
        return ranges;
    }

    private static <T> ObjList<JournalIteratorRange> createRanges(Journal<T> journal, long lo) {
        ObjList<JournalIteratorRange> ranges = new ObjList<>();
        int loPartitionID = Rows.toPartitionIndex(lo);
        long loLocalRowID = Rows.toLocalRowID(lo);

        try {
            int count = journal.getPartitionCount();
            for (int i = loPartitionID; i < count; i++) {
                long localRowID = 0;
                if (i == loPartitionID) {
                    localRowID = loLocalRowID;
                }

                Partition<T> p = journal.getPartition(i, true);
                long size = p.size();
                if (size > 0) {
                    ranges.add(new JournalIteratorRange(p.getPartitionIndex(), localRowID, size - 1));
                }
            }
            return ranges;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }
}
