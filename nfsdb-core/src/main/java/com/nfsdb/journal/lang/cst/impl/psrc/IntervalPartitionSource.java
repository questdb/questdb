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

package com.nfsdb.journal.lang.cst.impl.psrc;

import com.nfsdb.journal.BinarySearch;
import com.nfsdb.journal.Journal;
import com.nfsdb.journal.Partition;
import com.nfsdb.journal.collections.AbstractImmutableIterator;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.lang.cst.PartitionSlice;
import com.nfsdb.journal.lang.cst.PartitionSource;
import com.nfsdb.journal.utils.Interval;

import java.util.NoSuchElementException;

public class IntervalPartitionSource extends AbstractImmutableIterator<PartitionSlice> implements PartitionSource {
    private final PartitionSource delegate;
    private final Interval interval;
    private final PartitionSlice slice = new PartitionSlice();
    private boolean calNextSlice = true;

    public IntervalPartitionSource(PartitionSource delegate, Interval interval) {
        this.delegate = delegate;
        this.interval = interval;
    }

    @Override
    public boolean hasNext() {
        try {

            if (!calNextSlice) {
                return true;
            }

            while (delegate.hasNext()) {
                PartitionSlice slice = delegate.next();
                Partition partition = slice.partition;
                if (partition.getInterval().overlaps(interval)) {

                    long hi = slice.calcHi ? partition.open().size() - 1 : slice.hi;
                    long lo = partition.indexOf(interval.getLo(), BinarySearch.SearchType.NEWER_OR_SAME, slice.lo, hi);

                    if (lo >= 0) {
                        hi = partition.indexOf(interval.getHi(), BinarySearch.SearchType.OLDER_OR_SAME, lo, hi);
                        if (hi >= 0) {
                            this.slice.partition = slice.partition;
                            this.slice.lo = lo;
                            this.slice.hi = hi;
                            this.calNextSlice = false;
                            return true;
                        }
                    }
                }
            }
            slice.partition = null;
            return false;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public PartitionSlice next() {
        if (slice.partition == null) {
            throw new NoSuchElementException();
        }
        this.calNextSlice = true;
        return slice;
    }

    @Override
    public void reset() {
        delegate.reset();
        calNextSlice = true;
    }

    @Override
    public Journal getJournal() {
        return delegate.getJournal();
    }
}
