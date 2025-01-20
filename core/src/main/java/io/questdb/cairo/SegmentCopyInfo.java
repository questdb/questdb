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
    private long maxTimestamp = Long.MIN_VALUE;
    private long maxTxnRowCount;
    private long minTimestamp = Long.MAX_VALUE;
    private DirectLongList segments = new DirectLongList(4, MemoryTag.NATIVE_TABLE_WRITER);
    private long startSeqTxn;
    private DirectLongList txns = new DirectLongList(4, MemoryTag.NATIVE_TABLE_WRITER);
    private long totalRows;

    public void clear() {
        segments.clear();
        txns.clear();
        totalRows = 0;
        maxTxnRowCount = 0;
        startSeqTxn = 0;
        minTimestamp = Long.MAX_VALUE;
        maxTimestamp = Long.MIN_VALUE;
    }

    public void addSegment(int walId, int segmentId, long segmentLo, long segmentHi) {
        segments.add(walId);
        segments.add(segmentId);
        segments.add(segmentLo);
        segments.add(segmentHi);
    }

    public void addTxn(long segmentRowOffset, int seqTxn, long committedRowsCount, int copyTaskCount, long minTimestamp, long maxTimestamp) {
        txns.add(segmentRowOffset);
        txns.add(seqTxn);
        txns.add(committedRowsCount);
        txns.add(copyTaskCount);
        maxTxnRowCount = Math.max(maxTxnRowCount, committedRowsCount);
        totalRows += committedRowsCount;
        this.minTimestamp = Math.min(this.minTimestamp, minTimestamp);
        this.maxTimestamp = Math.max(this.maxTimestamp, maxTimestamp);
    }

    public long getSeqTxn(long txnIndex) {
        return startSeqTxn + txns.get(txnIndex * 4L + 1);
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

    public long getMaxTxRowCount() {
        return maxTxnRowCount;
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

    public long getStartTxn() {
        return startSeqTxn;
    }

    public void setStartSeqTxn(long seqTxn) {
        startSeqTxn = seqTxn;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public long getTxnCount() {
        return txns.size() / 4;
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
