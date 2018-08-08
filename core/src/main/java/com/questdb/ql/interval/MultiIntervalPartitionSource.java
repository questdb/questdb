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

package com.questdb.ql.interval;

import com.questdb.parser.sql.IntervalCompiler;
import com.questdb.ql.PartitionCursor;
import com.questdb.ql.PartitionSlice;
import com.questdb.ql.PartitionSource;
import com.questdb.std.LongList;
import com.questdb.std.str.CharSink;
import com.questdb.store.BSearchType;
import com.questdb.store.FixedColumn;
import com.questdb.store.Partition;
import com.questdb.store.StorageFacade;
import com.questdb.store.factory.ReaderFactory;
import com.questdb.store.factory.configuration.JournalMetadata;

public class MultiIntervalPartitionSource implements PartitionSource, PartitionCursor, com.questdb.std.ImmutableIterator<PartitionSlice> {
    private final PartitionSource partitionSource;
    private final PartitionSlice result = new PartitionSlice();
    private final LongList intervals;
    private final int intervalCount;
    private PartitionCursor partitionCursor;
    private boolean needPartition = true;
    private PartitionSlice slice = null;
    private FixedColumn timestampColumn = null;
    private long nextRowLo;
    private int intervalIndex = 0;

    public MultiIntervalPartitionSource(PartitionSource partitionSource, LongList intervals) {
        this.partitionSource = partitionSource;
        this.intervals = intervals;
        this.intervalCount = intervals.size() / 2;
    }

    @Override
    public JournalMetadata getMetadata() {
        return partitionSource.getMetadata();
    }

    @Override
    public PartitionCursor prepareCursor(ReaderFactory readerFactory) {
        intervalIndex = 0;
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
    public void releaseCursor() {
        if (partitionCursor != null) {
            partitionCursor.releaseCursor();
        }
    }

    @Override
    public void toTop() {
        intervalIndex = 0;
        needPartition = true;
        partitionCursor.toTop();
    }

    @Override
    public boolean hasNext() {
        long sliceRowLo;
        long sliceRowHi;
        long sliceLo;
        long sliceHi;
        long lo;
        long hi;

        while (true) {
            if (intervalIndex == intervalCount) {
                return false;
            }

            if (needPartition) {
                if (partitionCursor.hasNext()) {
                    slice = partitionCursor.next();
                    sliceRowLo = nextRowLo = slice.lo;
                    sliceRowHi = slice.calcHi ? slice.partition.size() - 1 : slice.hi;
                    if (sliceRowHi < 0) {
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

            lo = IntervalCompiler.getIntervalLo(intervals, intervalIndex);
            hi = IntervalCompiler.getIntervalHi(intervals, intervalIndex);

            // interval is fully above notional partition interval, skip to next interval
            if (hi < slice.partition.getInterval().getLo() || hi < (sliceLo = timestampColumn.getLong(sliceRowLo))) {
                needPartition = false;
                intervalIndex++;
                continue;
            }

            // interval is below notional partition, skip to next partition
            if (lo > slice.partition.getInterval().getHi() || lo > (sliceHi = timestampColumn.getLong(sliceRowHi))) {
                needPartition = true;
            } else {
                break;
            }
        }

        this.result.partition = slice.partition;

        if (lo > sliceLo) {
            this.result.lo = slice.partition.indexOf(lo, BSearchType.NEWER_OR_SAME);
        } else {
            this.result.lo = sliceRowLo;
        }

        if (hi < sliceHi) {
            this.result.hi = slice.partition.indexOf(hi, BSearchType.OLDER_OR_SAME, this.result.lo, sliceRowHi);
            needPartition = false;
            intervalIndex++;
        } else {
            this.result.hi = sliceRowHi;
            needPartition = true;
            if (hi == sliceHi) {
                intervalIndex++;
            }
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
        sink.putQuoted("isrc").put(':').put(IntervalCompiler.asIntervalStr(intervals));
        sink.put('}');
    }
}
