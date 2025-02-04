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

import io.questdb.cairo.vm.api.MemoryCMOR;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

import java.util.function.IntBinaryOperator;

public class SegmentCopyInfo implements QuietCloseable {
    private static final IntBinaryOperator assertSeqTxnOrderComparer = (a, b) -> a - b;
    private final IntList seqTxnOrder = new IntList();
    private int distinctWalSegmentCount;
    private long maxTimestamp = Long.MIN_VALUE;
    private long maxTxnRowCount;
    private long minTimestamp = Long.MAX_VALUE;
    private DirectLongList segments = new DirectLongList(4, MemoryTag.NATIVE_TABLE_WRITER);
    private long startSeqTxn;
    private long totalRows;
    private DirectLongList txns = new DirectLongList(4, MemoryTag.NATIVE_TABLE_WRITER);

    public void addSegment(int walId, int segmentId, long segmentLo, long segmentHi, boolean isLastSegmentUse) {
        segments.add(walId);
        segments.add(segmentId);
        segments.add(segmentLo);
        segments.add(isLastSegmentUse ? segmentHi : -segmentHi);
    }

    public void addTxn(long segmentRowOffset, int relativeSeqTxn, long committedRowsCount, int segmentIndex, long minTimestamp, long maxTimestamp) {
        txns.add(segmentRowOffset);
        txns.add(relativeSeqTxn);
        txns.add(committedRowsCount);
        txns.add(segmentIndex);

        if (seqTxnOrder.size() > 0) {
            seqTxnOrder.set(relativeSeqTxn, (int) (txns.size() / 4 - 1));
        }
        maxTxnRowCount = Math.max(maxTxnRowCount, committedRowsCount);
        totalRows += committedRowsCount;
        this.minTimestamp = Math.min(this.minTimestamp, minTimestamp);
        this.maxTimestamp = Math.max(this.maxTimestamp, maxTimestamp);
    }

    public void clear() {
        segments.clear();
        txns.clear();
        seqTxnOrder.clear();
        totalRows = 0;
        maxTxnRowCount = 0;
        startSeqTxn = 0;
        minTimestamp = Long.MAX_VALUE;
        maxTimestamp = Long.MIN_VALUE;
    }

    @Override
    public void close() {
        segments = Misc.free(segments);
        txns = Misc.free(txns);
    }

    public void createAddressBuffersPrimary(int columnIndex, int columnCount, ObjList<MemoryCMOR> walMappedColumns, long mappedAddrBuffPrimary) {
        int walColumnCountPerSegment = columnCount * 2;

        for (int i = 0, n = getSegmentCount(); i < n; i++) {
            var segmentColumnPrimary = walMappedColumns.get(walColumnCountPerSegment * i + 2 * columnIndex);
            Unsafe.getUnsafe().putLong(mappedAddrBuffPrimary + (long) i * Long.BYTES, segmentColumnPrimary.addressOf(0));
        }
    }

    public long createAddressBuffersSecondary(int columnIndex, int columnCount, ObjList<MemoryCMOR> walMappedColumns, long mappedAddrBuffSecondary, ColumnTypeDriver driver) {
        int walColumnCountPerSegment = columnCount * 2;

        long totalVarSize = 0;
        for (int i = 0, n = getSegmentCount(); i < n; i++) {
            var segmentColumnAux = walMappedColumns.get(walColumnCountPerSegment * i + 2 * columnIndex + 1);
            long segmentColumnAuxAddr = segmentColumnAux.addressOf(0);
            Unsafe.getUnsafe().putLong(mappedAddrBuffSecondary + (long) i * Long.BYTES, segmentColumnAuxAddr);
            totalVarSize += driver.getDataVectorSize(segmentColumnAuxAddr, getRowLo(i), getRowHi(i) - 1);
        }
        return totalVarSize;
    }

    public int getMappingOrder(long absoluteSeqTxn) {
        return seqTxnOrder.get((int) (absoluteSeqTxn - startSeqTxn));
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getMaxTxRowCount() {
        return maxTxnRowCount;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getRowHi(int segmentIndex) {
        return Math.abs(segments.get(segmentIndex * 4L + 3));
    }

    public long getRowLo(int segmentIndex) {
        return Math.abs(segments.get(segmentIndex * 4L + 2));
    }

    public int getSegmentCount() {
        return (int) (segments.size() / 4);
    }

    public int getSegmentId(int segmentIndex) {
        return (int) segments.get(segmentIndex * 4L + 1);
    }

    public long getStartTxn() {
        return startSeqTxn;
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

    public int getWalId(int segmentIndex) {
        return (int) segments.get(segmentIndex * 4L);
    }

    public void initBlock(long startSeqTxn, int txnCount, boolean hasSymbols) {
        this.startSeqTxn = startSeqTxn;
        if (hasSymbols) {
            seqTxnOrder.setPos(txnCount);
        }
    }

    public boolean isLastSegmentUse(int segmentIndex) {
        return segments.get(segmentIndex * 4L + 3) > 0;
    }
}
