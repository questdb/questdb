/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo;

import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;

public class SegmentCopyInfo implements QuietCloseable {
    private int distinctWalSegmentCount;
    private long maxSegmentRowCount;
    private long maxTimestamp;
    private long minTimestamp;
    private DirectLongList segments = new DirectLongList(4, MemoryTag.NATIVE_TABLE_WRITER);
    private DirectLongList txns = new DirectLongList(4, MemoryTag.NATIVE_TABLE_WRITER);
    private long totalRows;

    public void add(int walId, int segmentId, long segmentLo, long segmentHi, long minTimestamp, long maxTimestamp) {
        segments.add(walId);
        segments.add(segmentId);
        segments.add(segmentLo);
        segments.add(segmentHi);
        totalRows += segmentHi - segmentLo;
        maxSegmentRowCount = Math.max(maxSegmentRowCount, segmentHi - segmentLo);
        this.minTimestamp = Math.min(this.minTimestamp, minTimestamp);
        this.maxTimestamp = Math.max(this.maxTimestamp, maxTimestamp);
    }

    public void addTxn(long segmentRowOffset, int seqTxn, long committedRowsCount, int copyTaskCount) {
        txns.add(segmentRowOffset);
        txns.add(seqTxn);
        txns.add(committedRowsCount);
        txns.add(copyTaskCount);
    }

    public void clear() {
        segments.clear();
        txns.clear();
        totalRows = 0;
        maxSegmentRowCount = 0;
        minTimestamp = Long.MAX_VALUE;
        maxTimestamp = Long.MIN_VALUE;
    }

    @Override
    public void close() {
        segments = Misc.free(segments);
        txns = Misc.free(txns);
    }

    public long getRowHi(int i) {
        return segments.get(i * 4L + 3);
    }

    public long getRowLo(int i) {
        return segments.get(i * 4L + 2);
    }

    public long getMaxSegmentRowCount() {
        return maxSegmentRowCount;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public int getSegmentId(int i) {
        return (int) segments.get(i * 4L + 1);
    }

    public long getSegmentInfoAddress() {
        return segments.getAddress();
    }

    public long getTxnInfoAddress() {
        return txns.getAddress();
    }

    public int getWalId(int i) {
        return (int) segments.get(i * 4L);
    }

    public int size() {
        return (int) (segments.size() / 4);
    }
}
