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

package com.questdb.query;

import com.questdb.Journal;
import com.questdb.Partition;
import com.questdb.ex.JournalException;
import com.questdb.misc.Interval;
import com.questdb.std.LongList;
import com.questdb.store.BSearchType;

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
