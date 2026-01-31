/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.idx.BitmapIndexReader;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.geohash.GeoHashNative;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.std.DirectLongList;
import io.questdb.std.Os;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import io.questdb.std.Vect;
import io.questdb.tasks.LatestByTask;
import org.jetbrains.annotations.NotNull;

class LatestByAllIndexedRecordCursor extends AbstractPageFrameRecordCursor {
    private final int columnIndex;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final long indexShift = 0;
    private final DirectLongList prefixes;
    private final DirectLongList rows;
    private final AtomicBooleanCircuitBreaker sharedCircuitBreaker;
    private long aIndex;
    private long aLimit;
    private long argumentsAddress;
    private MessageBus bus;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private boolean isFrameCacheBuilt;
    private boolean isTreeMapBuilt;
    private int keyCount;
    private int sharedQueryWorkerCount;

    public LatestByAllIndexedRecordCursor(
            CairoEngine engine,
            @NotNull CairoConfiguration configuration,
            @NotNull @Transient RecordMetadata metadata,
            int columnIndex,
            @NotNull DirectLongList rows,
            @NotNull DirectLongList prefixes
    ) {
        super(configuration, metadata);
        sharedCircuitBreaker = new AtomicBooleanCircuitBreaker(engine);
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
            // We added 1 on cpp side.
            final long rowId = rows.get(aIndex++) - 1;
            // We inverted frame indexes when posting tasks.
            final int frameIndex = Rows.MAX_SAFE_PARTITION_INDEX - Rows.toPartitionIndex(rowId);
            frameMemoryPool.navigateTo(frameIndex, recordA);
            recordA.setRowIndex(Rows.toLocalRowID(rowId));
            return true;
        }
        return false;
    }

    @Override
    public void of(PageFrameCursor pageFrameCursor, SqlExecutionContext executionContext) {
        this.frameCursor = pageFrameCursor;
        recordA.of(pageFrameCursor);
        recordB.of(pageFrameCursor);
        circuitBreaker = executionContext.getCircuitBreaker();
        bus = executionContext.getMessageBus();
        // If the worker count is 0
        sharedQueryWorkerCount = executionContext.getSharedQueryWorkerCount();
        rows.clear();
        keyCount = -1;
        argumentsAddress = 0;
        isFrameCacheBuilt = false;
        isTreeMapBuilt = false;
        // prepare for page frame iteration
        super.init();
    }

    @Override
    public long preComputedStateSize() {
        return isTreeMapBuilt ? 1 : 0;
    }

    @Override
    public long size() {
        return isTreeMapBuilt ? aLimit - indexShift : -1;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Async index backward scan").meta("on").putColumnName(columnIndex);
        sink.meta("workers").val(sharedQueryWorkerCount + 1);

        if (prefixes.size() > 2) {
            int geoHashColumnIndex = (int) prefixes.get(0);
            int geoHashColumnType = (int) prefixes.get(1);
            int geoHashBits = ColumnType.getGeoHashBits(geoHashColumnType);

            if (geoHashColumnIndex > -1 && ColumnType.isGeoHash(geoHashColumnType)) {
                sink.attr("filter").putColumnName(geoHashColumnIndex).val(" within(");
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

    private static long getChunkSize(int keyCount, int sharedWorkerCount) {
        return sharedWorkerCount > 0 ? (keyCount + sharedWorkerCount - 1) / sharedWorkerCount : keyCount;
    }

    private static int getTaskCount(int keyCount, long chunkSize) {
        return (int) ((keyCount + chunkSize - 1) / chunkSize);
    }

    private void buildTreeMap() {
        int taskCount;
        if (keyCount < 0) {
            keyCount = getSymbolTable(columnIndex).getSymbolCount() + 1;
            final long chunkSize = getChunkSize(keyCount, sharedQueryWorkerCount);
            taskCount = getTaskCount(keyCount, chunkSize);
            rows.setCapacity(keyCount);
            GeoHashNative.iota(rows.getAddress(), rows.getCapacity(), 0);

            argumentsAddress = LatestByArguments.allocateMemoryArray(taskCount);
            for (long i = 0; i < taskCount; ++i) {
                final long keyLo = i * chunkSize;
                final long keyHi = Long.min(keyLo + chunkSize, keyCount);
                final long argsAddress = argumentsAddress + i * LatestByArguments.MEMORY_SIZE;
                LatestByArguments.setRowsAddress(argsAddress, rows.getAddress());
                LatestByArguments.setRowsCapacity(argsAddress, rows.getCapacity());
                LatestByArguments.setKeyLo(argsAddress, keyLo);
                LatestByArguments.setKeyHi(argsAddress, keyHi);
                LatestByArguments.setRowsSize(argsAddress, 0);
            }

            sharedCircuitBreaker.reset();
        } else {
            final long chunkSize = getChunkSize(keyCount, sharedQueryWorkerCount);
            taskCount = getTaskCount(keyCount, chunkSize);
        }

        int geoHashColumnIndex = -1;
        int geoHashColumnType = ColumnType.UNDEFINED;
        long prefixesAddress = 0;
        long prefixesCount = 0;

        if (prefixes.size() > 2) {
            // Looks like we have WITHIN clause in the filter.
            geoHashColumnIndex = (int) prefixes.get(0);
            geoHashColumnType = (int) prefixes.get(1);
            prefixesAddress = prefixes.getAddress() + 2 * Long.BYTES;
            prefixesCount = prefixes.size() - 2;
        }

        final RingQueue<LatestByTask> queue = bus.getLatestByQueue();
        final Sequence pubSeq = bus.getLatestByPubSeq();
        final Sequence subSeq = bus.getLatestBySubSeq();


        int queuedCount = 0;
        long foundRowCount = 0;
        try {
            // First, build address cache as we'll be publishing it to other threads.
            PageFrame frame;
            if (!isFrameCacheBuilt) {
                while ((frame = frameCursor.next()) != null) {
                    frameAddressCache.add(frameCount++, frame);
                }
                isFrameCacheBuilt = true;
            }

            int frameIndex = 0;
            frameCursor.toTop();
            while ((frame = frameCursor.next()) != null && foundRowCount < keyCount) {
                final BitmapIndexReader indexReader = frame.getBitmapIndexReader(columnIndex, BitmapIndexReader.DIR_BACKWARD);
                final long partitionLo = frame.getPartitionLo();
                final long partitionHi = frame.getPartitionHi() - 1;

                final long keyBaseAddress = indexReader.getKeyBaseAddress();
                final long keysMemorySize = indexReader.getKeyMemorySize();
                final long valueBaseAddress = indexReader.getValueBaseAddress();
                final long valuesMemorySize = indexReader.getValueMemorySize();
                final int valueBlockCapacity = indexReader.getValueBlockCapacity();
                final long unIndexedNullCount = indexReader.getColumnTop();

                doneLatch.reset();

                queuedCount = 0;
                for (long i = 0; i < taskCount; i++) {
                    final long argsAddress = argumentsAddress + i * LatestByArguments.MEMORY_SIZE;
                    final long found = LatestByArguments.getRowsSize(argsAddress);
                    final long keyHi = LatestByArguments.getKeyHi(argsAddress);
                    final long keyLo = LatestByArguments.getKeyLo(argsAddress);

                    // Skip range if all keys found
                    if (found >= keyHi - keyLo) {
                        continue;
                    }

                    final long seq = pubSeq.next();
                    if (seq < 0) {
                        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                        GeoHashNative.latestByAndFilterPrefix(
                                frameMemoryPool,
                                keyBaseAddress,
                                keysMemorySize,
                                valueBaseAddress,
                                valuesMemorySize,
                                argsAddress,
                                unIndexedNullCount,
                                partitionHi,
                                partitionLo,
                                frameIndex,
                                valueBlockCapacity,
                                geoHashColumnIndex,
                                geoHashColumnType,
                                prefixesAddress,
                                prefixesCount
                        );
                    } else {
                        queue.get(seq).of(
                                frameAddressCache,
                                keyBaseAddress,
                                keysMemorySize,
                                valueBaseAddress,
                                valuesMemorySize,
                                argsAddress,
                                unIndexedNullCount,
                                partitionHi,
                                partitionLo,
                                frameIndex,
                                valueBlockCapacity,
                                geoHashColumnIndex,
                                geoHashColumnType,
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
                        try {
                            queue.get(seq).run();
                        } finally {
                            subSeq.done(seq);
                        }
                    } else {
                        Os.pause();
                    }
                }

                foundRowCount = 0; // Reset found counter
                for (int i = 0; i < taskCount; i++) {
                    final long address = argumentsAddress + i * LatestByArguments.MEMORY_SIZE;
                    foundRowCount += LatestByArguments.getRowsSize(address);
                }

                frameIndex++;
            }
        } catch (Throwable th) {
            sharedCircuitBreaker.cancel();
            throw th;
        } finally {
            processTasks(queuedCount);
            if (sharedCircuitBreaker.checkIfTripped()) {
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
                try {
                    queue.get(seq).run();
                } finally {
                    subSeq.done(seq);
                }
            } else {
                Os.pause();
            }
        }
    }
}
