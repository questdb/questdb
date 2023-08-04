/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.cutlass.text.AtomicBooleanCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.geohash.GeoHashNative;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.std.*;
import io.questdb.tasks.LatestByTask;
import org.jetbrains.annotations.NotNull;

class LatestByAllIndexedRecordCursor extends AbstractDataFrameRecordCursor {
    protected final long indexShift = 0;
    protected final DirectLongList prefixes;
    protected final DirectLongList rows;
    private final int columnIndex;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final AtomicBooleanCircuitBreaker sharedCircuitBreaker = new AtomicBooleanCircuitBreaker();
    protected long aIndex;
    protected long aLimit;
    protected SqlExecutionCircuitBreaker circuitBreaker;
    private long argumentsAddress;
    private MessageBus bus;
    private boolean isTreeMapBuilt;
    private int keyCount;
    private int workerCount;

    public LatestByAllIndexedRecordCursor(
            int columnIndex,
            @NotNull DirectLongList rows,
            @NotNull IntList columnIndexes,
            @NotNull DirectLongList prefixes
    ) {
        super(columnIndexes);
        this.rows = rows;
        this.columnIndex = columnIndex;
        this.prefixes = prefixes;
    }

    @Override
    public boolean hasNext() {
        if (!isTreeMapBuilt) {
            buildTreeMap();
            isTreeMapBuilt = true;
        }
        if (aIndex < aLimit) {
            long row = rows.get(aIndex++) - 1; // we added 1 on cpp side
            recordA.jumpTo(Rows.toPartitionIndex(row), Rows.toLocalRowID(row));
            return true;
        }
        return false;
    }

    @Override
    public void of(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        this.dataFrameCursor = dataFrameCursor;
        recordA.of(dataFrameCursor.getTableReader());
        recordB.of(dataFrameCursor.getTableReader());
        circuitBreaker = executionContext.getCircuitBreaker();
        bus = executionContext.getMessageBus();
        workerCount = executionContext.getSharedWorkerCount();
        rows.clear();
        keyCount = -1;
        argumentsAddress = 0;
        isTreeMapBuilt = false;
    }

    @Override
    public long size() {
        return isTreeMapBuilt ? aLimit - indexShift : -1;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index backward scan").meta("on").putColumnName(columnIndex);
        sink.meta("parallel").val(true);

        if (prefixes.size() > 2) {
            int hashColumnIndex = (int) prefixes.get(0);
            int hashColumnType = (int) prefixes.get(1);
            int geoHashBits = ColumnType.getGeoHashBits(hashColumnType);

            if (hashColumnIndex > -1 && ColumnType.isGeoHash(hashColumnType)) {
                sink.attr("filter")
                        .putColumnName(hashColumnIndex).val(" within(");

                for (long i = 2, n = prefixes.size(); i < n; i += 2) {
                    if (i > 2) {
                        sink.val(',');
                    }
                    sink.val(prefixes.get(i), geoHashBits);
                }
                sink.val(')');
            }
        }
    }

    @Override
    public void toTop() {
        aIndex = indexShift;
    }

    private static long getChunkSize(int keyCount, int workerCount) {
        return (keyCount + workerCount - 1) / workerCount;
    }

    private static int getPow2SizeOfGeoHashType(int type) {
        return 1 << ColumnType.pow2SizeOfBits(ColumnType.getGeoHashBits(type));
    }

    private static int getTaskCount(int keyCount, long chunkSize) {
        return (int) ((keyCount + chunkSize - 1) / chunkSize);
    }

    private void buildTreeMap() {
        int taskCount;
        if (keyCount < 0) {
            keyCount = getSymbolTable(columnIndex).getSymbolCount() + 1;
            final long chunkSize = getChunkSize(keyCount, workerCount);
            taskCount = getTaskCount(keyCount, chunkSize);
            rows.setCapacity(keyCount);
            GeoHashNative.iota(rows.getAddress(), rows.getCapacity(), 0);

            argumentsAddress = LatestByArguments.allocateMemoryArray(taskCount);
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

            sharedCircuitBreaker.reset();
        } else {
            final long chunkSize = getChunkSize(keyCount, workerCount);
            taskCount = getTaskCount(keyCount, chunkSize);
        }

        int hashColumnIndex = -1;
        int hashColumnType = ColumnType.UNDEFINED;
        long prefixesAddress = 0;
        long prefixesCount = 0;

        if (prefixes.size() > 2) {
            hashColumnIndex = (int) prefixes.get(0);
            hashColumnType = (int) prefixes.get(1);
            prefixesAddress = prefixes.getAddress() + 2 * Long.BYTES;
            prefixesCount = prefixes.size() - 2;
        }

        // frame metadata is based on TableReader, which is "full" metadata
        // this cursor works with subset of columns, which warrants column index remap
        int frameColumnIndex = columnIndexes.getQuick(columnIndex);

        final RingQueue<LatestByTask> queue = bus.getLatestByQueue();
        final Sequence pubSeq = bus.getLatestByPubSeq();
        final Sequence subSeq = bus.getLatestBySubSeq();
        final TableReader reader = dataFrameCursor.getTableReader();

        DataFrame frame;
        long foundRowCount = 0;
        int queuedCount = 0;
        try {
            while ((frame = dataFrameCursor.next()) != null && foundRowCount < keyCount) {
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

                // hashColumnIndex can be -1 for latest by part only (no prefixes to match)
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
                while (!doneLatch.done(queuedCount)) {
                    circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                    long seq = subSeq.next();
                    if (seq > -1) {
                        queue.get(seq).run();
                        subSeq.done(seq);
                    } else {
                        Os.pause();
                    }
                }

                foundRowCount = 0; // Reset found counter
                for (int i = 0; i < taskCount; i++) {
                    final long address = argumentsAddress + i * LatestByArguments.MEMORY_SIZE;
                    foundRowCount += LatestByArguments.getRowsSize(address);
                }
            }
        } catch (DataUnavailableException e) {
            // We're not yet done, so no need to cancel the circuit breaker. 
            throw e;
        } catch (Throwable t) {
            sharedCircuitBreaker.cancel();
            throw t;
        } finally {
            processTasks(queuedCount);
            if (sharedCircuitBreaker.isCanceled()) {
                LatestByArguments.releaseMemoryArray(argumentsAddress, taskCount);
                argumentsAddress = 0;
            }
        }

        long rowCount = 0;
        if (argumentsAddress > 0) {
            rowCount = GeoHashNative.slideFoundBlocks(argumentsAddress, taskCount);
            LatestByArguments.releaseMemoryArray(argumentsAddress, taskCount);
            argumentsAddress = 0;
        }
        aLimit = rowCount;
        aIndex = indexShift;
        postProcessRows();
    }

    private void postProcessRows() {
        Vect.sortULongAscInPlace(rows.getAddress(), aLimit);
    }

    private void processTasks(int queuedCount) {
        final RingQueue<LatestByTask> queue = bus.getLatestByQueue();
        final Sequence subSeq = bus.getLatestBySubSeq();
        while (!doneLatch.done(queuedCount)) {
            long seq = subSeq.next();
            if (seq > -1) {
                if (circuitBreaker.checkIfTripped()) {
                    sharedCircuitBreaker.cancel();
                }
                queue.get(seq).run();
                subSeq.done(seq);
            } else {
                Os.pause();
            }
        }
    }
}
