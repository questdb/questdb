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

package com.nfsdb.journal.query.spi;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.OrderedResultSet;
import com.nfsdb.journal.OrderedResultSetBuilder;
import com.nfsdb.journal.Partition;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.iterators.*;
import com.nfsdb.journal.query.api.QueryAll;
import com.nfsdb.journal.query.api.QueryAllBuilder;
import com.nfsdb.journal.utils.Interval;
import com.nfsdb.journal.utils.Rows;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QueryAllImpl<T> implements QueryAll<T> {

    private final Journal<T> journal;

    public QueryAllImpl(Journal<T> journal) {
        this.journal = journal;
    }

    @Override
    public OrderedResultSet<T> asResultSet() throws JournalException {
        return journal.iteratePartitions(new OrderedResultSetBuilder<T>() {
            @Override
            public void read(long lo, long hi) throws JournalException {
                result.addCapacity((int) (hi - lo + 1));
                for (long i = lo; i < hi + 1; i++) {
                    result.add(Rows.toRowID(partition.getPartitionIndex(), i));
                }
            }
        });
    }

    @Override
    public long size() {
        try {
            return journal.size();
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public QueryAllBuilder<T> withKeys(String... values) {
        return withSymValues(journal.getMetadata().getKey(), values);
    }

    @Override
    public QueryAllBuilder<T> withSymValues(String symbol, String... values) {
        QueryAllBuilderImpl<T> result = new QueryAllBuilderImpl<>(journal);
        result.setSymbol(symbol, values);
        return result;
    }

    @Override
    public Iterator<T> iterator() {
        return new JournalIteratorImpl<>(journal, createRanges());
    }

    @Override
    public JournalPeekingIterator<T> bufferedIterator() {
        return new JournalBufferedIterator<>(journal, createRanges());
    }

    public JournalRowBufferedIterator<T> bufferedRowIterator() {
        return new JournalRowBufferedIterator<>(journal, createRanges());
    }

    @Override
    public AbstractConcurrentIterator<T> concurrentIterator() {
        return new JournalConcurrentIterator<>(journal, createRanges(), 1024);
    }

    @Override
    public JournalIterator<T> iterator(Interval interval) {
        return new JournalIteratorImpl<>(journal, createRanges(interval));
    }

    @Override
    public JournalPeekingIterator<T> bufferedIterator(Interval interval) {
        return new JournalBufferedIterator<>(journal, createRanges(interval));
    }

    @Override
    public AbstractConcurrentIterator<T> concurrentIterator(Interval interval) {
        return new JournalConcurrentIterator<>(journal, createRanges(interval), 1024);
    }

    @Override
    public JournalPeekingIterator<T> iterator(long rowid) {
        return new JournalIteratorImpl<>(journal, createRanges(rowid));
    }

    @Override
    public JournalPeekingIterator<T> bufferedIterator(long rowid) {
        return new JournalBufferedIterator<>(journal, createRanges(rowid));
    }

    @Override
    public JournalPeekingIterator<T> incrementBufferedIterator() {
        try {
            long lo = journal.getMaxRowID();
            journal.refresh();
            return new JournalBufferedIterator<>(journal, createRanges(journal.incrementRowID(lo)));
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public JournalPeekingIterator<T> incrementIterator() {
        try {
            long lo = journal.getMaxRowID();
            journal.refresh();
            return new JournalIteratorImpl<>(journal, createRanges(journal.incrementRowID(lo)));
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public AbstractConcurrentIterator<T> concurrentIterator(long rowid) {
        return new JournalConcurrentIterator<>(journal, createRanges(rowid), 1024);
    }

    private List<JournalIteratorRange> createRanges() {
        final int partitionCount = journal.getPartitionCount();
        List<JournalIteratorRange> ranges = new ArrayList<>(partitionCount);
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

    private List<JournalIteratorRange> createRanges(Interval interval) {
        final List<JournalIteratorRange> ranges = new ArrayList<>();
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

    private List<JournalIteratorRange> createRanges(long lo) {
        List<JournalIteratorRange> ranges = new ArrayList<>();
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
