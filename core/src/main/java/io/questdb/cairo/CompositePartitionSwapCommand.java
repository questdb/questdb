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
import io.questdb.tasks.TableWriterTask;

/**
 * Swaps in a composite partition REWRITE built off a {@link TableReader} snapshot - see
 * {@code PartitionCompactionScanJob} - without ever holding the writer for the copy itself.
 * <p>
 * Published via {@link CairoEngine#getWriterOrPublishCommand}: an idle writer applies it directly, a busy
 * writer serializes it onto its own {@link TableWriterTask} command queue and applies it later on the
 * writer's own thread via {@link TableWriter#tick()} - same protocol
 * {@link ParquetPartitionCompactionCommand} already uses. Unlike that command, this one does not override
 * {@link #newInstance()}: the producer's own instance - already carrying the recorded column tops - is
 * what the writer applies, so {@link #deserialize} has nothing to reconstruct from the task buffer.
 * <p>
 * {@link #apply} throws {@link io.questdb.cairo.sql.TableReferenceOutOfDateException} when the source
 * partition's generation no longer matches what the build snapshot saw - the writer discards the staged
 * directory and the caller's next sweep starts over from a fresh snapshot. Non-structural (it never
 * changes table structure, only a partition's own storage layout), so it is safe to queue for a WAL table
 * too.
 */
public class CompositePartitionSwapCommand implements AsyncWriterCommand {
    private final ColumnTopRecorder columnTops = new ColumnTopRecorder();
    private long correlationId = -1L;
    private long expectedSrcNameTxn;
    private long expectedWriterTxn;
    private long liveRows;
    private long partitionTimestamp;
    private int tableId;
    private TableToken tableToken;

    @Override
    public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) {
        ((TableWriter) svc).swapCompactedCompositePartition(
                partitionTimestamp,
                expectedSrcNameTxn,
                expectedWriterTxn,
                liveRows,
                columnTops
        );
        return 0;
    }

    @Override
    public void close() {
    }

    @Override
    public AsyncWriterCommand deserialize(TableWriterTask task) {
        // newInstance() is not overridden (see class doc): this is always the producer's own instance,
        // already carrying every field including the recorded tops, so there is nothing to reconstruct
        // from the task buffer.
        return this;
    }

    @Override
    public int getCmdType() {
        return TableWriterTask.CMD_COMPOSITE_PARTITION_SWAP;
    }

    @Override
    public String getCommandName() {
        return TableWriterTask.getCommandName(TableWriterTask.CMD_COMPOSITE_PARTITION_SWAP);
    }

    public ColumnTopRecorder getColumnTops() {
        return columnTops;
    }

    @Override
    public long getCorrelationId() {
        return correlationId;
    }

    public long getExpectedSrcNameTxn() {
        return expectedSrcNameTxn;
    }

    public long getExpectedWriterTxn() {
        return expectedWriterTxn;
    }

    public long getLiveRows() {
        return liveRows;
    }

    public long getPartitionTimestamp() {
        return partitionTimestamp;
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
    public boolean isStructural() {
        return false;
    }

    public void of(
            TableToken tableToken,
            int tableId,
            long partitionTimestamp,
            long expectedSrcNameTxn,
            long expectedWriterTxn,
            long liveRows
    ) {
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.partitionTimestamp = partitionTimestamp;
        this.expectedSrcNameTxn = expectedSrcNameTxn;
        this.expectedWriterTxn = expectedWriterTxn;
        this.liveRows = liveRows;
        this.columnTops.clear();
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

    @Override
    public void startAsync() {
    }
}
