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

package io.questdb.cairo;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;
import java.util.concurrent.locks.LockSupport;

import static io.questdb.cairo.TableUtils.TX_OFFSET_MIN_TIMESTAMP;

class TableReaderTransaction extends BaseRecordMetadata implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableReaderTransaction.class);
    private final CairoConfiguration configuration;
    private final Path path;
    private final ReadOnlyMemory txMem;
    private final FilesFacade ff;

    private final IntList symbolCountSnapshot;
    private final LongHashSet removedPartitions;
    private final TimestampFloorMethod timestampFloorMethod;
    private long minTimestamp = Long.MAX_VALUE;
    private long maxTimestamp = Numbers.LONG_NaN;
    private long transientRowCount;
    private long structVersion;
    private long dataVersion;
    private long partitionTableVersion;
    private long rowCount;
    private long txn = TableUtils.INITIAL_TXN;

    public TableReaderTransaction(CairoConfiguration configuration, Path path, IntList symbolCountSnapshot, LongHashSet removedPartitions, TimestampFloorMethod timestampFloorMethod) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.path = new Path().of(path).$();
        this.symbolCountSnapshot = symbolCountSnapshot;
        this.removedPartitions = removedPartitions;
        this.timestampFloorMethod = timestampFloorMethod;
        try {
            txMem = new ReadOnlyMemory(ff, path, ff.getPageSize(), TableUtils.getSymbolWriterIndexOffset(0));
            read();
        } catch (CairoException e) {
            close();
            throw e;
        }
    }

    public boolean read() {
        int count = 0;
        final long deadline = configuration.getMicrosecondClock().getTicks() + configuration.getSpinLockTimeoutUs();
        while (true) {
            long txn = txMem.getLong(TableUtils.TX_OFFSET_TXN);

            // exit if this is the same as we already have
            if (txn == this.txn) {
                return false;
            }

            // make sure this isn't re-ordered
            Unsafe.getUnsafe().loadFence();

            // do start and end sequences match? if so we have a chance at stable read
            if (txn == txMem.getLong(TableUtils.TX_OFFSET_TXN_CHECK)) {
                // great, we seem to have got stable read, lets do some reading
                // and check later if it was worth it

                Unsafe.getUnsafe().loadFence();
                final long transientRowCount = txMem.getLong(TableUtils.TX_OFFSET_TRANSIENT_ROW_COUNT);
                final long fixedRowCount = txMem.getLong(TableUtils.TX_OFFSET_FIXED_ROW_COUNT);
                final long minTimestamp = txMem.getLong(TX_OFFSET_MIN_TIMESTAMP);
                final long maxTimestamp = txMem.getLong(TableUtils.TX_OFFSET_MAX_TIMESTAMP);
                final long structVersion = txMem.getLong(TableUtils.TX_OFFSET_STRUCT_VERSION);
                final long dataVersion = txMem.getLong(TableUtils.TX_OFFSET_DATA_VERSION);
                final long partitionTableVersion = txMem.getLong(TableUtils.TX_OFFSET_PARTITION_TABLE_VERSION);

                if (symbolCountSnapshot != null) {
                    symbolCountSnapshot.clear();
                    int symbolMapCount = txMem.getInt(TableUtils.TX_OFFSET_MAP_WRITER_COUNT);
                    if (symbolMapCount > 0) {
                        txMem.grow(TableUtils.getSymbolWriterIndexOffset(symbolMapCount));
                        for (int i = 0; i < symbolMapCount; i++) {
                            symbolCountSnapshot.add(txMem.getInt(TableUtils.getSymbolWriterIndexOffset(i)));
                        }
                    }
                    txMem.grow(TableUtils.getPartitionTableIndexOffset(symbolMapCount, 0));

                    if (removedPartitions != null) {
                        removedPartitions.clear();
                        int partitionTableSize = txMem.getInt(TableUtils.getPartitionTableSizeOffset(symbolMapCount));
                        if (partitionTableSize > 0) {
                            txMem.grow(TableUtils.getPartitionTableIndexOffset(symbolMapCount, partitionTableSize));
                            for (int i = 0; i < partitionTableSize; i++) {
                                removedPartitions.add(txMem.getLong(TableUtils.getPartitionTableIndexOffset(symbolMapCount, i)));
                            }
                        }
                    }
                }

                Unsafe.getUnsafe().loadFence();
                // ok, we have snapshot, check if our snapshot is stable
                if (txn == txMem.getLong(TableUtils.TX_OFFSET_TXN)) {
                    // good, very stable, congrats
                    this.txn = txn;
                    this.transientRowCount = transientRowCount;
                    this.rowCount = fixedRowCount + transientRowCount;
                    if (minTimestamp == Long.MAX_VALUE) {
                        this.minTimestamp = Long.MAX_VALUE;
                    } else {
                        this.minTimestamp = timestampFloorMethod.floor(minTimestamp);
                    }
                    this.maxTimestamp = maxTimestamp;
                    this.structVersion = structVersion;
                    this.dataVersion = dataVersion;
                    this.partitionTableVersion = partitionTableVersion;
                    LOG.info()
                            .$("new transaction [txn=").$(txn)
                            .$(", transientRowCount=").$(transientRowCount)
                            .$(", fixedRowCount=").$(fixedRowCount)
                            .$(", maxTimestamp=").$(maxTimestamp)
                            .$(", attempts=").$(count)
                            .$(']').$();
                    return true;
                }
                // This is unlucky, sequences have changed while we were reading transaction data
                // We must discard and try again
            }
            count++;
            if (configuration.getMicrosecondClock().getTicks() > deadline) {
                LOG.error().$("tx read timeout [timeout=").$(configuration.getSpinLockTimeoutUs()).utf8("μs]").$();
                throw CairoException.instance(0).put("Transaction read timeout");
            }
            LockSupport.parkNanos(1);
        }
    }


    @Override
    public void close() {
        Misc.free(txMem);
        Misc.free(path);
    }

    public long getStructVersion() {
        return structVersion;
    }

    public long getDataVersion() {
        return dataVersion;
    }

    public long getPartitionTableVersion() {
        return partitionTableVersion;
    }

    public long getRowCount() { return rowCount; }

    public long getTransientRowCount() {
        return transientRowCount;
    }

    public long getTxn() { return txn; }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }
}
