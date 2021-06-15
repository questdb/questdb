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
import io.questdb.std.*;
import io.questdb.tasks.LatestByTask;
import org.jetbrains.annotations.NotNull;

class LatestByAllIndexedRecordCursor extends AbstractRecordListCursor {
    private final int columnIndex;

    private long indexShift = 0;
    private long aIndex;
    private long aLimit;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();

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
        final int BATCH_SIZE = 200;
        final int workerCount = executionContext.getWorkerCount();

        int keyCount = getSymbolTable(columnIndex).size() + 1;

        long keyLo = 0;
        long keyHi = keyCount;

        rows.extend(keyCount);
        rows.setPos(rows.getCapacity());
        rows.zero(0);

        final DirectLongList arguments = new DirectLongList(workerCount);
        long rowCount = 0;
        DataFrame frame;
        // frame metadata is based on TableReader, which is "full" metadata
        // this cursor works with subset of columns, which warrants column index remap
        int frameColumnIndex = columnIndexes.getQuick(columnIndex);
        while ((frame = this.dataFrameCursor.next()) != null && rowCount < keyCount) {
            doneLatch.reset();
            final BitmapIndexReader indexReader = frame.getBitmapIndexReader(frameColumnIndex, BitmapIndexReader.DIR_BACKWARD);
            final long currKeyCount = keyHi - keyLo;

            final long taskCount = currKeyCount <= BATCH_SIZE ? 0 : workerCount;
            final long batchSize = currKeyCount <= BATCH_SIZE ? currKeyCount : currKeyCount / taskCount; // taskCount != 0
            final long remaining = currKeyCount <= BATCH_SIZE ? currKeyCount : currKeyCount % workerCount;

            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;

            final long keyBaseAddress = indexReader.getKeyBaseAddress();
            final long keysMemorySize = indexReader.getKeyMemorySize();
            final long valueBaseAddress = indexReader.getValueBaseAddress();
            final long valuesMemorySize = indexReader.getValueMemorySize();
            final int valueBlockCapacity  = indexReader.getValueBlockCapacity();
            final long unIndexedNullCount = indexReader.getUnIndexedNullCount();
            final int partitionIndex = frame.getPartitionIndex();

            int queuedCount = 0;
            arguments.clear();
            for (long i = 0; i < taskCount; ++i) {
                final long klo = keyLo + i*batchSize;
                final long khi = klo + batchSize;
                final long argsAddress = LatestByArguments.allocateMemory();
                LatestByArguments.setRowsAddress(argsAddress, rows.getAddress());
                LatestByArguments.setRowsCapacity(argsAddress, rows.getCapacity());
                LatestByArguments.setKeyLo(argsAddress, klo);
                LatestByArguments.setKeyHi(argsAddress, khi);
                LatestByArguments.setRowsSize(argsAddress, rows.size());

                arguments.add(argsAddress);
                final long seq = pubSeq.next();
                if (seq < 0) {
                    BitmapIndexUtilsNative.latestScanBackward(keyBaseAddress, keysMemorySize, valueBaseAddress,
                            valuesMemorySize, argsAddress,
                            unIndexedNullCount, rowHi, rowLo, partitionIndex, valueBlockCapacity);
                } else {
                    queue.get(seq).of(keyBaseAddress, keysMemorySize, valueBaseAddress, valuesMemorySize, argsAddress,
                            unIndexedNullCount, rowHi, rowLo, partitionIndex, valueBlockCapacity, doneLatch);
                    pubSeq.done(seq);
                    queuedCount++;
                }
            }

            doneLatch.await(queuedCount);

            if (remaining > 0) {

                final long klo = keyLo + taskCount*batchSize;
                final long khi = klo + batchSize;

                final long argsAddress = LatestByArguments.allocateMemory();
                LatestByArguments.setRowsAddress(argsAddress, rows.getAddress());
                LatestByArguments.setRowsCapacity(argsAddress, rows.getCapacity());
                LatestByArguments.setKeyLo(argsAddress, klo);
                LatestByArguments.setKeyHi(argsAddress, khi);
                LatestByArguments.setRowsSize(argsAddress, rows.size());

                arguments.add(argsAddress);
                BitmapIndexUtilsNative.latestScanBackward(keyBaseAddress, keysMemorySize, valueBaseAddress,
                        valuesMemorySize, argsAddress,
                        unIndexedNullCount, rowHi, rowLo, partitionIndex, valueBlockCapacity);
            }

            for(int i = 0; i < arguments.size(); i++) {
                final long addr = arguments.get(i);
                rowCount += LatestByArguments.getRowsSize(addr);
                keyLo = Long.min(keyLo, LatestByArguments.getKeyLo(addr));
                keyHi = Long.max(keyHi, LatestByArguments.getKeyHi(addr) + 1);
                LatestByArguments.releaseMemory(addr);
            }
        }
        arguments.close();

        // we have to sort rows because multiple symbols
        // are liable to be looked up out of order
        rows.setPos(rows.getCapacity());
        rows.sortAsUnsigned();

        //skip "holes"
        while(rows.get(indexShift) <= 0) {
            indexShift++;
        }

        aLimit = rows.size();
        aIndex = indexShift;
    }
}