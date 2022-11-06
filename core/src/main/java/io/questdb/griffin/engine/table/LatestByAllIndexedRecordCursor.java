/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.cutlass.text.AtomicBooleanCircuitBreaker;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.geohash.GeoHashNative;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.Rows;
import io.questdb.std.Vect;
import io.questdb.tasks.LatestByTask;
import org.jetbrains.annotations.NotNull;

class LatestByAllIndexedRecordCursor extends AbstractRecordListCursor {
    protected final DirectLongList prefixes;
    private final int columnIndex;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    protected long indexShift = 0;
    protected long aIndex;
    protected long aLimit;
    private final AtomicBooleanCircuitBreaker sharedCircuitBreaker = new AtomicBooleanCircuitBreaker();

    public LatestByAllIndexedRecordCursor(
            int columnIndex,
            @NotNull DirectLongList rows,
            @NotNull IntList columnIndexes,
            @NotNull DirectLongList prefixes
    ) {
        super(rows, columnIndexes);
        this.columnIndex = columnIndex;
        this.prefixes = prefixes;
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
        SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        final MessageBus bus = executionContext.getMessageBus();

        final RingQueue<LatestByTask> queue = bus.getLatestByQueue();
        final Sequence pubSeq = bus.getLatestByPubSeq();
        final Sequence subSeq = bus.getLatestBySubSeq();

        int keyCount = getSymbolTable(columnIndex).getSymbolCount() + 1;
        rows.setCapacity(keyCount);
        GeoHashNative.iota(rows.getAddress(), rows.getCapacity(), 0);

        final int workerCount = executionContext.getSharedWorkerCount();

        final long chunkSize = (keyCount + workerCount - 1) / workerCount;
        final int taskCount = (int) ((keyCount + chunkSize - 1) / chunkSize);

        final long argumentsAddress = LatestByArguments.allocateMemoryArray(taskCount);
        for (long i = 0; i < taskCount; ++i) {
            final long klo = i * chunkSize;
            final long khi = Long.min(klo + chunkSize, keyCount);
            final long argsAddress = argumentsAddress + i * LatestByArguments.MEMORY_SIZE;
            LatestByArguments.setRowsAddress(argsAddress, rows.getAddress());
            LatestByArguments.setRowsCapacity(argsAddress, rows.getCapacity());
            LatestByArguments.setKeyLo(argsAddress, klo);
            LatestByArguments.setKeyHi(argsAddress, khi);
            LatestByArguments.setRowsSize(argsAddress, 0);
        }

        int hashColumnIndex = -1;
        int hashColumnType = ColumnType.UNDEFINED;

        sharedCircuitBreaker.reset();
        long prefixesAddress = 0;
        long prefixesCount = 0;

        if(this.prefixes.size() > 2) {
            hashColumnIndex = (int) prefixes.get(0);
            hashColumnType = (int) prefixes.get(1);
            prefixesAddress = prefixes.getAddress() + 2 * Long.BYTES;
            prefixesCount = prefixes.size() - 2;
        }

        DataFrame frame;
        // frame metadata is based on TableReader, which is "full" metadata
        // this cursor works with subset of columns, which warrants column index remap
        int frameColumnIndex = columnIndexes.getQuick(columnIndex);

        final TableReader reader = this.dataFrameCursor.getTableReader();

        long foundRowCount = 0;
        int queuedCount = 0;

        try {
            while ((frame = this.dataFrameCursor.next()) != null && foundRowCount < keyCount) {
                doneLatch.reset();
                final BitmapIndexReader indexReader = frame.getBitmapIndexReader(frameColumnIndex, BitmapIndexReader.DIR_BACKWARD);

                final long rowLo = frame.getRowLo();
                final long rowHi = frame.getRowHi() - 1;

                final long keyBaseAddress = indexReader.getKeyBaseAddress();
                final long keysMemorySize = indexReader.getKeyMemorySize();
                final long valueBaseAddress = indexReader.getValueBaseAddress();
                final long valuesMemorySize = indexReader.getValueMemorySize();
                final int valueBlockCapacity = indexReader.getValueBlockCapacity();
                final long unIndexedNullCount = indexReader.getUnIndexedNullCount();
                final int partitionIndex = frame.getPartitionIndex();

                long hashColumnAddress = 0;

                //hashColumnIndex can be -1 for latest by part only (no prefixes to match)
                if (hashColumnIndex > -1) {
                    final int columnBase = reader.getColumnBase(partitionIndex);
                    final int primaryColumnIndex = TableReader.getPrimaryColumnIndex(columnBase, hashColumnIndex);
                    final MemoryR column = reader.getColumn(primaryColumnIndex);
                    hashColumnAddress = column.getPageAddress(0);
                }

                // -1 must be dead case here
                final int hashesColumnSize = ColumnType.isGeoHash(hashColumnType) ? getPow2SizeOfGeoHashType(hashColumnType) : -1;

                queuedCount = 0;
                for (long i = 0; i < taskCount; ++i) {
                    final long argsAddress = argumentsAddress + i * LatestByArguments.MEMORY_SIZE;
                    final long found = LatestByArguments.getRowsSize(argsAddress);
                    final long keyHi = LatestByArguments.getKeyHi(argsAddress);
                    final long keyLo = LatestByArguments.getKeyLo(argsAddress);

                    // Skip range if all keys found
                    if (found >= keyHi - keyLo) {
                        continue;
                    }
                    // Update hash column address with current frame value
                    LatestByArguments.setHashesAddress(argsAddress, hashColumnAddress);

                    final long seq = pubSeq.next();
                    if (seq < 0) {
                        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                        GeoHashNative.latestByAndFilterPrefix(
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
                                hashColumnAddress,
                                hashesColumnSize,
                                prefixesAddress,
                                prefixesCount
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
                                hashColumnAddress,
                                hashesColumnSize,
                                prefixesAddress,
                                prefixesCount,
                                doneLatch,
                                sharedCircuitBreaker
                        );
                        pubSeq.done(seq);
                        queuedCount++;
                    }
                }

                // process our own queue
                // this should fix deadlock with 1 worker configuration
                while (doneLatch.getCount() > -queuedCount) {
                    circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                    long seq = subSeq.next();
                    if (seq > -1) {
                        queue.get(seq).run();
                        subSeq.done(seq);
                    }
                }

                doneLatch.await(queuedCount);

                foundRowCount = 0; // Reset found counter
                for (int i = 0; i < taskCount; i++) {
                    final long address = argumentsAddress + i * LatestByArguments.MEMORY_SIZE;
                    foundRowCount += LatestByArguments.getRowsSize(address);
                }
            }
        } catch (Throwable t) {
            sharedCircuitBreaker.cancel();
            throw t;
        } finally {
            processTasks(circuitBreaker, queue, subSeq, queuedCount);
            if (sharedCircuitBreaker.isCanceled()) {
                LatestByArguments.releaseMemoryArray(argumentsAddress, taskCount);
            }
        }

        final long rowCount = GeoHashNative.slideFoundBlocks(argumentsAddress, taskCount);
        LatestByArguments.releaseMemoryArray(argumentsAddress, taskCount);
        aLimit = rowCount;
        aIndex = indexShift;
        postProcessRows();
    }

    private void processTasks(SqlExecutionCircuitBreaker circuitBreaker, RingQueue<LatestByTask> queue, Sequence subSeq, int queuedCount) {
        while (doneLatch.getCount() > -queuedCount) {
            long seq = subSeq.next();
            if (seq > -1) {
                if (circuitBreaker.checkIfTripped()) {
                    sharedCircuitBreaker.cancel();
                }
                queue.get(seq).run();
                subSeq.done(seq);
            }
        }

        doneLatch.await(queuedCount);
    }

    private static int getPow2SizeOfGeoHashType(int type) {
        return 1 << ColumnType.pow2SizeOfBits(ColumnType.getGeoHashBits(type));
    }

    protected void postProcessRows() {
        Vect.sortULongAscInPlace(rows.getAddress(), aLimit);
    }

}
