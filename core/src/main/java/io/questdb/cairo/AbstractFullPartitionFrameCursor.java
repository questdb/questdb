/*+*****************************************************************************
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

package io.questdb.cairo;

import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Abstract base class for full partition frame cursors.
 */
public abstract class AbstractFullPartitionFrameCursor implements PartitionFrameCursor {
    protected final FullTablePartitionFrame frame = new FullTablePartitionFrame();
    // Task 5b: set by the owning factory (see PartitionFrameCursorFactory#setAllowedCellKeys) right
    // before this cursor is handed out; null means "no pruning" (every plain table, and every composite
    // query whose predicate was not resolved to a dimension cellKey set).
    protected @Nullable IntHashSet allowedCellKeys;
    // Partition high boundary.
    protected int partitionHi;
    // Current partition index.
    protected int partitionIndex;
    // The scan high boundary. Defaults to partitionHi but can be narrowed
    // by toPartition(int) to limit iteration to a single partition.
    protected int partitionScanHi;
    protected TableReader reader;

    @Override
    public void close() {
        // avoid double-close in case of cursor not closing the reader, query progress catching the leak
        // and then factory is trying to close the cursor and the reader
        if (reader != null && reader.isActive()) {
            reader = Misc.free(reader);
        }
    }

    @Override
    public SymbolMapReader getSymbolTable(int columnIndex) {
        return reader.getSymbolMapReader(columnIndex);
    }

    @Override
    public TableReader getTableReader() {
        return reader;
    }

    /**
     * Task 5b: {@code true} unless a composite dimension predicate was resolved to an allowed-cellKey
     * set AND this slot's cell is not in it. Both {@link FullFwdPartitionFrameCursor} and
     * {@link FullBwdPartitionFrameCursor} compose this with their existing empty-partition check --
     * never replace it. {@code allowedCellKeys == null} (no pruning attempted, or a plain table)
     * short-circuits to {@code true} unconditionally, so this is a zero-cost no-op for every case this
     * task does not touch.
     */
    protected boolean isCellAllowed(int partitionIndex) {
        return allowedCellKeys == null || allowedCellKeys.contains(reader.getPartitionCellKey(partitionIndex));
    }

    @Override
    public StaticSymbolTable newSymbolTable(int columnIndex) {
        return reader.newSymbolTable(columnIndex);
    }

    /**
     * Initializes the cursor with the given table reader.
     *
     * @param reader the table reader
     * @return this cursor
     */
    public PartitionFrameCursor of(TableReader reader) {
        partitionHi = reader.getPartitionCount();
        partitionScanHi = partitionHi;
        toTop();
        this.reader = reader;
        return this;
    }

    @TestOnly
    @Override
    public boolean reload() {
        boolean moreData = reader.reload();
        partitionHi = reader.getPartitionCount();
        toTop();
        return moreData;
    }

    /**
     * Task 5b: see {@link io.questdb.cairo.sql.PartitionFrameCursorFactory#setAllowedCellKeys}'s own doc.
     * Called by the owning factory on every {@code getCursor()}, not just once, since this cursor
     * instance is cached and reused across executions of the same compiled factory.
     */
    public void setAllowedCellKeys(@Nullable IntHashSet allowedCellKeys) {
        this.allowedCellKeys = allowedCellKeys;
    }

    @Override
    public long size() {
        return reader.size();
    }

    @Override
    public void toPartition(int targetPartitionIndex) {
        this.partitionIndex = targetPartitionIndex;
        this.partitionScanHi = targetPartitionIndex + 1;
    }

    /**
     * A partition frame representing a full table partition.
     */
    protected static class FullTablePartitionFrame implements PartitionFrame {
        /**
         * The partition format.
         */
        protected byte format;
        /**
         * The parquet-meta-backed Parquet decoder for table partitions.
         */
        protected ParquetPartitionDecoder parquetMetaDecoder;
        /**
         * The partition index.
         */
        protected int partitionIndex;
        /**
         * The high row boundary.
         */
        protected long rowHi;
        /**
         * The low row boundary.
         */
        protected long rowLo;

        @Override
        public ParquetPartitionDecoder getParquetMetaDecoder() {
            return parquetMetaDecoder;
        }

        @Override
        public byte getPartitionFormat() {
            return format;
        }

        @Override
        public int getPartitionIndex() {
            return partitionIndex;
        }

        @Override
        public long getRowHi() {
            return rowHi;
        }

        @Override
        public long getRowLo() {
            return rowLo;
        }
    }
}
