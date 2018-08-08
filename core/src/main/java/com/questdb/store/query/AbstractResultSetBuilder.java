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

package com.questdb.store.query;

import com.questdb.std.LongList;
import com.questdb.std.ex.JournalException;
import com.questdb.store.BSearchType;
import com.questdb.store.Interval;
import com.questdb.store.Journal;
import com.questdb.store.Partition;

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
            default:
                break;
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
