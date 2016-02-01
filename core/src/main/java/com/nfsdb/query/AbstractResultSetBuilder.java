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

package com.nfsdb.query;

import com.nfsdb.Journal;
import com.nfsdb.Partition;
import com.nfsdb.ex.JournalException;
import com.nfsdb.misc.Interval;
import com.nfsdb.std.LongList;
import com.nfsdb.store.BSearchType;

public abstract class AbstractResultSetBuilder<T, X> {
    protected final LongList result = new LongList();
    protected Partition<T> partition;
    Journal<T> journal;
    private Interval interval = null;

    AbstractResultSetBuilder(Interval interval) {
        this.interval = interval;
    }

    AbstractResultSetBuilder() {
    }

    public Accept accept(Partition<T> partition) throws JournalException {
        this.partition = partition;
        return Accept.CONTINUE;
    }

    public abstract X getResult();

    public boolean next(Partition<T> partition, boolean desc) throws JournalException {

        Interval that = partition.getInterval();
        if (interval != null && that != null
                &&
                (
                        that.getLo() > interval.getHi()
                                || that.getHi() < interval.getLo()
                )
                ) {

            return (that.getHi() < interval.getLo() && !desc) ||
                    (that.getLo() > interval.getHi() && desc);
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
                if (partition.getInterval().getLo() < interval.getLo()) {
                    long _lo = partition.indexOf(interval.getLo(), BSearchType.NEWER_OR_SAME);

                    // there are no data with timestamp later then start date of interval, skip partition
                    if (_lo == -2) {
                        return false;
                    }

                    lo = _lo;
                }

                if (partition.getInterval().getHi() > interval.getHi()) {
                    long _hi = partition.indexOf(interval.getHi(), BSearchType.OLDER_OR_SAME);

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

    public abstract void read(long lo, long hi) throws JournalException;

    public void setJournal(Journal<T> journal) {
        this.journal = journal;
    }

    public enum Accept {
        CONTINUE, SKIP, BREAK
    }
}
