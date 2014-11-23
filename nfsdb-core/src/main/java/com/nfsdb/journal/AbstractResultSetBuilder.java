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

package com.nfsdb.journal;

import com.nfsdb.journal.collections.LongArrayList;
import com.nfsdb.journal.exceptions.JournalException;
import org.joda.time.Interval;

public abstract class AbstractResultSetBuilder<T, X> {
    protected final LongArrayList result = new LongArrayList();
    protected Partition<T> partition;
    protected Journal<T> journal;
    private Interval interval = null;

    protected AbstractResultSetBuilder(Interval interval) {
        this.interval = interval;
    }

    protected AbstractResultSetBuilder() {
    }

    public void setJournal(Journal<T> journal) {
        this.journal = journal;
    }

    public boolean next(Partition<T> partition, boolean desc) throws JournalException {

        if (interval != null && partition.getInterval() != null
                &&
                (
                        partition.getInterval().getStartMillis() > interval.getEndMillis()
                                || partition.getInterval().getEndMillis() < interval.getStartMillis()
                )
                ) {

            return (partition.getInterval().getEndMillis() < interval.getStartMillis() && !desc) ||
                    (partition.getInterval().getStartMillis() > interval.getEndMillis() && desc);
        }

        switch (accept(partition)) {
            case SKIP:
                return false;
            case BREAK:
                return true;
        }

        long size = partition.open().size();

        if (size > 0) {

            long lo = 0;
            long hi = size - 1;

            if (interval != null && partition.getInterval() != null) {
                if (partition.getInterval().getStartMillis() < interval.getStartMillis()) {
                    long _lo = partition.indexOf(interval.getStartMillis(), BinarySearch.SearchType.NEWER_OR_SAME);

                    // there are no data with timestamp later then start date of interval, skip partition
                    if (_lo == -2) {
                        return false;
                    }

                    lo = _lo;
                }

                if (partition.getInterval().getEndMillis() > interval.getEndMillis()) {
                    long _hi = partition.indexOf(interval.getEndMillis(), BinarySearch.SearchType.OLDER_OR_SAME);

                    // there are no data with timestamp earlier then end date of interval, skip partition
                    if (_hi == -1) {
                        return false;
                    }

                    hi = _hi;
                }
            }

            if (lo <= hi) {
                read(lo, hi);
            }
        }
        return false;
    }

    public Accept accept(Partition<T> partition) throws JournalException {
        this.partition = partition;
        return Accept.CONTINUE;
    }

    public abstract void read(long lo, long hi) throws JournalException;

    public abstract X getResult();

    public enum Accept {
        CONTINUE, SKIP, BREAK
    }
}
