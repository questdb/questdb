/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb;

import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.query.OrderedResultSetBuilder;
import com.nfsdb.query.iterator.*;
import com.nfsdb.utils.Interval;
import com.nfsdb.utils.Rows;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Iterator;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED", "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS"})
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
                public void read(long lo, long hi) throws JournalException {
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
