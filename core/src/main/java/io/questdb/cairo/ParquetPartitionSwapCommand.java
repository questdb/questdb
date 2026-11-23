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

package io.questdb.cairo;

import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.wal.MetadataService;
import io.questdb.std.IntList;
import io.questdb.tasks.TableWriterTask;

/**
 * Swaps in a Parquet partition compacted off a {@link TableReader} snapshot - see {@code PartitionCompactionScanJob}
 * and {@link O3PartitionJob#compactParquetPartition} - without ever holding the writer for the copy itself.
 */
public class ParquetPartitionSwapCommand implements CompactionSwapCommand {
    private final IntList staleColumnTops = new IntList();
    private long correlationId = -1L;
    private long expectedMetadataVersion;
    private long expectedParquetFileSize;
    private long expectedSrcNameTxn;
    private volatile boolean isApplied;
    private boolean isFullyMaterialized;
    private boolean isIndexRowMapDropped;
    private long newParquetFileSize;
    private long partitionTimestamp;
    private int tableId;
    private TableToken tableToken;

    @Override
    public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) {
        try {
            ((TableWriter) svc).swapCompactedParquetPartition(
                    partitionTimestamp,
                    expectedSrcNameTxn,
                    expectedParquetFileSize,
                    expectedMetadataVersion,
                    newParquetFileSize,
                    isFullyMaterialized,
                    staleColumnTops
            );
        } finally {
            isApplied = true;
        }
        return 0;
    }

    @Override
    public void close() {
    }

    @Override
    public AsyncWriterCommand deserialize(TableWriterTask task) {
        return this;
    }

    @Override
    public int getCmdType() {
        return TableWriterTask.CMD_PARQUET_PARTITION_SWAP;
    }

    @Override
    public String getCommandName() {
        return TableWriterTask.getCommandName(TableWriterTask.CMD_PARQUET_PARTITION_SWAP);
    }

    @Override
    public long getCorrelationId() {
        return correlationId;
    }

    public long getExpectedMetadataVersion() {
        return expectedMetadataVersion;
    }

    public long getExpectedParquetFileSize() {
        return expectedParquetFileSize;
    }

    public long getExpectedSrcNameTxn() {
        return expectedSrcNameTxn;
    }

    public long getNewParquetFileSize() {
        return newParquetFileSize;
    }

    public long getPartitionTimestamp() {
        return partitionTimestamp;
    }

    /**
     * The writer indexes of the columns whose stale {@code _cv} top the swap zeroes: every column the file carries
     * with a non-zero top, when QuestDB 9.3.4 - 10.0.1 left a value inside one of them. Empty for a folder whose
     * tops are sound. Filled by {@code PartitionCompactionScanJob} off the reader snapshot the copy is built from.
     */
    public IntList getStaleColumnTops() {
        return staleColumnTops;
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public int getTableNamePosition() {
        return 0;
    }

    @Override
    public TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public long getTableVersion() {
        return 0;
    }

    @Override
    public boolean isApplied() {
        return isApplied;
    }

    /**
     * Whether the build re-encoded every row group under the current table schema, materializing every
     * column from row 0 - in which case the swap zeroes the partition's column tops - as opposed to copying
     * the row groups verbatim, which leaves the tops as they are.
     */
    public boolean isFullyMaterialized() {
        return isFullyMaterialized;
    }

    /**
     * Whether the compacted file dropped the source's index row map, so its index files must be rebuilt.
     */
    public boolean isIndexRowMapDropped() {
        return isIndexRowMapDropped;
    }

    @Override
    public boolean isStructural() {
        return false;
    }

    public void of(
            TableToken tableToken,
            int tableId,
            long partitionTimestamp,
            long expectedSrcNameTxn,
            long expectedParquetFileSize,
            long expectedMetadataVersion
    ) {
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.partitionTimestamp = partitionTimestamp;
        this.expectedSrcNameTxn = expectedSrcNameTxn;
        this.expectedParquetFileSize = expectedParquetFileSize;
        this.expectedMetadataVersion = expectedMetadataVersion;
        this.newParquetFileSize = -1L;
        this.isFullyMaterialized = false;
        this.isIndexRowMapDropped = false;
        this.staleColumnTops.clear();
        this.isApplied = false;
    }

    @Override
    public void serialize(TableWriterTask task) {
        task.of(getCmdType(), tableId, tableToken);
        task.setInstance(correlationId);
        task.setAsyncWriterCommand(this);
        task.putLong(partitionTimestamp);
    }

    @Override
    public void setCommandCorrelationId(long correlationId) {
        this.correlationId = correlationId;
    }

    /**
     * Records the build's outcome: the size of the staged {@code data.parquet}, whether it was fully
     * re-encoded under the current schema, and whether the source footer carried an index row map the
     * compacted file drops. Set by {@link O3PartitionJob#compactParquetPartition}.
     */
    public void setResult(long newParquetFileSize, boolean isFullyMaterialized, boolean isIndexRowMapDropped) {
        this.newParquetFileSize = newParquetFileSize;
        this.isFullyMaterialized = isFullyMaterialized;
        this.isIndexRowMapDropped = isIndexRowMapDropped;
    }

    @Override
    public void startAsync() {
    }
}
