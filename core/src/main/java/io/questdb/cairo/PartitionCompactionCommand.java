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
import io.questdb.std.Unsafe;
import io.questdb.tasks.TableWriterTask;

/**
 * Idle-triggered async command wrapping {@link TableWriter#swapCompositePartition(CompositePartitionMerger.MergeResult)},
 * the swap half of a composite (multi-piece) partition's compaction. {@link CompositePartitionMerger#merge}
 * is the build half: it runs first, off a {@link TableReader} snapshot with no writer lock, and its result
 * is what this command carries across to the writer thread.
 * <p>
 * Published via {@link CairoEngine#getWriterOrPublishCommand}: an idle writer applies it directly, a busy
 * writer serializes it onto its own {@link TableWriterTask} command queue and applies it later on the
 * writer's own thread via {@link TableWriter#tick()}. Non-structural (it only replaces a composite
 * partition's on-disk pieces with their merged image; the table's structure - columns, symbol maps - never
 * changes), so it is safe to queue for a WAL table too.
 * <p>
 * Carries the full {@link CompositePartitionMerger.MergeResult} - column tops included - only when
 * constructed directly from one: {@link #apply} then calls the {@code MergeResult}-taking overload of
 * {@link TableWriter#swapCompositePartition}, which repairs column tops after the repack. The scan job that
 * builds this command is the only place that ever has a live {@code MergeResult} to hand it, and only on the
 * idle-writer path where {@link #apply} runs in-process, right after {@link CompositePartitionMerger#merge}
 * - no serialization involved. A command drained from the writer's own queue is reconstructed by
 * {@link #deserialize} from the five plain longs/int {@link #serialize} wrote - the queue slot has no room
 * for a {@link io.questdb.std.LongList} of column tops - so {@code mergeResult} stays {@code null} there and
 * {@link #apply} falls back to the primitive overload, which leaves column tops unrepaired for that path.
 */
public class PartitionCompactionCommand implements AsyncWriterCommand {
    private long correlationId = -1L;
    private long mergedRowCount;
    private CompositePartitionMerger.MergeResult mergeResult;
    private long oldNameTxn;
    private long partitionTimestamp;
    private int snapshotPieceCount;
    private long snapshotWriterTxn;
    private int tableId;
    private TableToken tableToken;

    public PartitionCompactionCommand() {
    }

    public PartitionCompactionCommand(
            TableToken tableToken,
            int tableId,
            long partitionTimestamp,
            long oldNameTxn,
            long snapshotWriterTxn,
            int snapshotPieceCount,
            long mergedRowCount
    ) {
        of(tableToken, tableId, partitionTimestamp, oldNameTxn, snapshotWriterTxn, snapshotPieceCount, mergedRowCount);
    }

    public PartitionCompactionCommand(TableToken tableToken, int tableId, CompositePartitionMerger.MergeResult mergeResult) {
        of(tableToken, tableId, mergeResult);
    }

    @Override
    public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) {
        final TableWriter writer = (TableWriter) svc;
        return mergeResult != null
                ? writer.swapCompositePartition(mergeResult)
                : writer.swapCompositePartition(partitionTimestamp, oldNameTxn, snapshotWriterTxn, snapshotPieceCount, mergedRowCount);
    }

    @Override
    public void close() {
    }

    @Override
    public AsyncWriterCommand deserialize(TableWriterTask task) {
        long p = task.getData();
        this.mergeResult = null;
        this.partitionTimestamp = Unsafe.getLong(p);
        this.oldNameTxn = Unsafe.getLong(p + Long.BYTES);
        this.snapshotWriterTxn = Unsafe.getLong(p + 2L * Long.BYTES);
        this.snapshotPieceCount = Unsafe.getInt(p + 3L * Long.BYTES);
        this.mergedRowCount = Unsafe.getLong(p + 3L * Long.BYTES + Integer.BYTES);
        return this;
    }

    @Override
    public int getCmdType() {
        return TableWriterTask.CMD_PARTITION_COMPACTION;
    }

    @Override
    public String getCommandName() {
        return TableWriterTask.getCommandName(TableWriterTask.CMD_PARTITION_COMPACTION);
    }

    @Override
    public long getCorrelationId() {
        return correlationId;
    }

    public long getMergedRowCount() {
        return mergedRowCount;
    }

    /**
     * The live {@link CompositePartitionMerger.MergeResult} this command was constructed from, or
     * {@code null} for a command reconstructed by {@link #deserialize} off the writer's command queue.
     */
    public CompositePartitionMerger.MergeResult getMergeResult() {
        return mergeResult;
    }

    public long getOldNameTxn() {
        return oldNameTxn;
    }

    public long getPartitionTimestamp() {
        return partitionTimestamp;
    }

    public int getSnapshotPieceCount() {
        return snapshotPieceCount;
    }

    public long getSnapshotWriterTxn() {
        return snapshotWriterTxn;
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

    @Override
    public AsyncWriterCommand newInstance() {
        return new PartitionCompactionCommand();
    }

    public void of(
            TableToken tableToken,
            int tableId,
            long partitionTimestamp,
            long oldNameTxn,
            long snapshotWriterTxn,
            int snapshotPieceCount,
            long mergedRowCount
    ) {
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.mergeResult = null;
        this.partitionTimestamp = partitionTimestamp;
        this.oldNameTxn = oldNameTxn;
        this.snapshotWriterTxn = snapshotWriterTxn;
        this.snapshotPieceCount = snapshotPieceCount;
        this.mergedRowCount = mergedRowCount;
    }

    public void of(TableToken tableToken, int tableId, CompositePartitionMerger.MergeResult mergeResult) {
        of(
                tableToken,
                tableId,
                mergeResult.getPartitionTimestamp(),
                mergeResult.getOldNameTxn(),
                mergeResult.getSnapshotWriterTxn(),
                mergeResult.getSnapshotPieceCount(),
                mergeResult.getMergedRowCount()
        );
        this.mergeResult = mergeResult;
    }

    @Override
    public void serialize(TableWriterTask task) {
        task.of(getCmdType(), tableId, tableToken);
        task.setInstance(correlationId);
        task.setAsyncWriterCommand(this);
        task.putLong(partitionTimestamp);
        task.putLong(oldNameTxn);
        task.putLong(snapshotWriterTxn);
        task.putInt(snapshotPieceCount);
        task.putLong(mergedRowCount);
    }

    @Override
    public void setCommandCorrelationId(long correlationId) {
        this.correlationId = correlationId;
    }

    @Override
    public void startAsync() {
    }
}
