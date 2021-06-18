/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.MessageBus;
import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.sql.DataFrame;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.std.BitmapIndexUtilsNative;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.Rows;
import io.questdb.tasks.LatestByTask;
import org.jetbrains.annotations.NotNull;

class LatestByAllIndexedRecordCursor extends AbstractRecordListCursor {
    private final int columnIndex;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    protected long indexShift = 0;
    protected long aIndex;
    protected long aLimit;

    public LatestByAllIndexedRecordCursor(int columnIndex, DirectLongList rows, @NotNull IntList columnIndexes) {
        super(rows, columnIndexes);
        this.columnIndex = columnIndex;
    }

    @Override
    public boolean hasNext() {
        if (aIndex < aLimit) {
            long row = rows.get(aIndex++) - 1; // we added 1 on cpp side
            recordA.jumpTo(Rows.toPartitionIndex(row), Rows.toLocalRowID(row));
            return true;
        }
        return false;
    }

    @Override
    public void toTop() {
        aIndex = indexShift;
    }

    @Override
    public long size() {
        return aLimit - indexShift;
    }

    @Override
    protected void buildTreeMap(SqlExecutionContext executionContext) {
        final MessageBus bus = executionContext.getMessageBus();
        assert bus != null;

        final RingQueue<LatestByTask> queue = bus.getLatestByQueue();
        final Sequence pubSeq = bus.getLatestByPubSeq();
        final Sequence subSeq = bus.getLatestBySubSeq();

        final int workerCount = executionContext.getWorkerCount();
        int keyCount = getSymbolTable(columnIndex).size() + 1;

        long keyLo = 0;
        long keyHi = keyCount;

        rows.extend(keyCount);
        rows.setPos(rows.getCapacity());
        rows.zero(0);

        final long argumentsAddress = LatestByArguments.allocateMemoryArray(workerCount);
        long rowCount = 0;
        DataFrame frame;
        // frame metadata is based on TableReader, which is "full" metadata
        // this cursor works with subset of columns, which warrants column index remap
        int frameColumnIndex = columnIndexes.getQuick(columnIndex);
        while ((frame = this.dataFrameCursor.next()) != null && rowCount < keyCount) {
            doneLatch.reset();
            final BitmapIndexReader indexReader = frame.getBitmapIndexReader(frameColumnIndex, BitmapIndexReader.DIR_BACKWARD);
            final long frameKeyCount = keyHi - keyLo;

            final long chunkSize = (frameKeyCount + workerCount - 1) / workerCount;
            final long taskCount = (frameKeyCount + chunkSize - 1) / chunkSize;

            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;

            final long keyBaseAddress = indexReader.getKeyBaseAddress();
            final long keysMemorySize = indexReader.getKeyMemorySize();
            final long valueBaseAddress = indexReader.getValueBaseAddress();
            final long valuesMemorySize = indexReader.getValueMemorySize();
            final int valueBlockCapacity = indexReader.getValueBlockCapacity();
            final long unIndexedNullCount = indexReader.getUnIndexedNullCount();
            final int partitionIndex = frame.getPartitionIndex();

            int queuedCount = 0;
            for (long i = 0; i < taskCount; ++i) {
                final long klo = i * chunkSize;
                final long khi = Long.min(klo + chunkSize, frameKeyCount);

                final long argsAddress = argumentsAddress + i * LatestByArguments.MEMORY_SIZE;
                LatestByArguments.setRowsAddress(argsAddress, rows.getAddress());
                LatestByArguments.setRowsCapacity(argsAddress, rows.getCapacity());
                LatestByArguments.setKeyLo(argsAddress, klo);
                LatestByArguments.setKeyHi(argsAddress, khi);
                LatestByArguments.setRowsSize(argsAddress, rows.size());

                final long seq = pubSeq.next();
                if (seq < 0) {
                    BitmapIndexUtilsNative.latestScanBackward(
                            keyBaseAddress,
                            keysMemorySize,
                            valueBaseAddress,
                            valuesMemorySize,
                            argsAddress,
                            unIndexedNullCount,
                            rowHi,
                            rowLo,
                            partitionIndex,
                            valueBlockCapacity
                    );
                } else {
                    queue.get(seq).of(
                            keyBaseAddress,
                            keysMemorySize,
                            valueBaseAddress,
                            valuesMemorySize,
                            argsAddress,
                            unIndexedNullCount,
                            rowHi,
                            rowLo,
                            partitionIndex,
                            valueBlockCapacity,
                            doneLatch
                    );
                    pubSeq.done(seq);
                    queuedCount++;
                }
            }

            // process our own queue
            // this should fix deadlock with 1 worker configuration
            while (doneLatch.getCount() > -queuedCount) {
                long seq = subSeq.next();
                if (seq > -1) {
                    queue.get(seq).run();
                    subSeq.done(seq);
                }
            }

            doneLatch.await(queuedCount);

            for (int i = 0; i < taskCount; i++) {
                final long addr = argumentsAddress + i * LatestByArguments.MEMORY_SIZE;
                rowCount += LatestByArguments.getRowsSize(addr);
                keyLo = Long.min(keyLo, LatestByArguments.getKeyLo(addr));
                keyHi = Long.max(keyHi, LatestByArguments.getKeyHi(addr) + 1);
            }
        }
        LatestByArguments.releaseMemoryArray(argumentsAddress, workerCount);
        postProcessRows();
    }

    protected void postProcessRows() {
        // we have to sort rows because multiple symbols
        // are liable to be looked up out of order
        rows.setPos(rows.getCapacity());
        rows.sortAsUnsigned();

        //skip "holes"
        while (rows.get(indexShift) <= 0) {
            indexShift++;
        }

        aLimit = rows.size();
        aIndex = indexShift;
    }
}
