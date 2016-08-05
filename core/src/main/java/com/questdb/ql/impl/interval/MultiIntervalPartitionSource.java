/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql.impl.interval;

import com.questdb.Partition;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.misc.Interval;
import com.questdb.ql.PartitionCursor;
import com.questdb.ql.PartitionSlice;
import com.questdb.ql.PartitionSource;
import com.questdb.ql.StorageFacade;
import com.questdb.std.AbstractImmutableIterator;
import com.questdb.std.CharSink;
import com.questdb.store.BSearchType;
import com.questdb.store.FixedColumn;

public class MultiIntervalPartitionSource extends AbstractImmutableIterator<PartitionSlice> implements PartitionSource, PartitionCursor {
    private final PartitionSource partitionSource;
    private final PartitionSlice result = new PartitionSlice();
    private final IntervalSource intervalSource;
    private PartitionCursor partitionCursor;
    private boolean needInterval = true;
    private boolean needPartition = true;
    private Interval interval;
    private PartitionSlice slice = null;
    private FixedColumn timestampColumn = null;
    private long nextRowLo;

    public MultiIntervalPartitionSource(PartitionSource partitionSource, IntervalSource intervalSource) {
        this.partitionSource = partitionSource;
        this.intervalSource = intervalSource;
    }

    @Override
    public JournalMetadata getMetadata() {
        return partitionSource.getMetadata();
    }

    @Override
    public PartitionCursor prepareCursor(JournalReaderFactory readerFactory) {
        intervalSource.reset();
        needInterval = true;
        needPartition = true;
        partitionCursor = partitionSource.prepareCursor(readerFactory);
        return this;
    }

    @Override
    public Partition getPartition(int index) {
        return partitionCursor.getPartition(index);
    }

    @Override
    public StorageFacade getStorageFacade() {
        return partitionCursor.getStorageFacade();
    }

    @Override
    public boolean hasNext() {
        long sliceRowLo;
        long sliceRowHi;
        long sliceLo;
        long sliceHi;

        while (true) {

            if (needInterval) {
                if (intervalSource.hasNext()) {
                    interval = intervalSource.next();
                } else {
                    return false;
                }
            }

            if (needPartition) {
                if (partitionCursor.hasNext()) {
                    slice = partitionCursor.next();
                    sliceRowLo = nextRowLo = slice.lo;
                    sliceRowHi = slice.calcHi ? slice.partition.size() - 1 : slice.hi;
                    if (sliceRowHi < 0) {
                        needInterval = false;
                        continue;
                    }
                    timestampColumn = slice.partition.getTimestampColumn();
                } else {
                    return false;
                }
            } else {
                sliceRowLo = nextRowLo;
                sliceRowHi = slice.calcHi ? slice.partition.size() - 1 : slice.hi;
            }

            // interval is fully above notional partition interval, skip to next interval
            if (interval.getHi() < slice.partition.getInterval().getLo() || interval.getHi() < (sliceLo = timestampColumn.getLong(sliceRowLo))) {
                needPartition = false;
                needInterval = true;
                continue;
            }

            // interval is below notional partition, skip to next partition
            if (interval.getLo() > slice.partition.getInterval().getHi() || interval.getLo() > (sliceHi = timestampColumn.getLong(sliceRowHi))) {
                needPartition = true;
                needInterval = false;
            } else {
                break;
            }
        }

        this.result.partition = slice.partition;

        if (interval.getLo() > sliceLo) {
            this.result.lo = slice.partition.indexOf(interval.getLo(), BSearchType.NEWER_OR_SAME);
        } else {
            this.result.lo = sliceRowLo;
        }

        if (interval.getHi() < sliceHi) {
            this.result.hi = slice.partition.indexOf(interval.getHi(), BSearchType.OLDER_OR_SAME, this.result.lo, sliceRowHi);
            needPartition = false;
            needInterval = true;
        } else {
            this.result.hi = sliceRowHi;
            needPartition = true;
            needInterval = interval.getHi() == sliceHi;
        }

        nextRowLo = result.hi + 1;

        return true;
    }

    @Override
    public PartitionSlice next() {
        return result;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("MultiIntervalPartitionSource").put(',');
        sink.putQuoted("psrc").put(':').put(partitionSource).put(',');
        sink.putQuoted("isrc").put(':').put(intervalSource);
        sink.put('}');
    }

}
