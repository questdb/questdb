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
 * Idle-triggered async command wrapping {@link TableWriter#compactParquetPartition(long)}.
 * <p>
 * Published via {@link CairoEngine#getWriterOrPublishCommand}: an idle writer applies it directly,
 * a busy writer serializes it onto its own {@link TableWriterTask} command queue and applies it
 * later on the writer's own thread via {@link TableWriter#tick()}. Non-structural (it only rewrites
 * a Parquet partition's file layout, it never changes table structure), so it is safe to queue for
 * a WAL table too.
 */
public class ParquetPartitionCompactionCommand implements AsyncWriterCommand {
    private long correlationId = -1L;
    private long partitionTimestamp;
    private int tableId;
    private TableToken tableToken;

    public ParquetPartitionCompactionCommand() {
    }

    public ParquetPartitionCompactionCommand(TableToken tableToken, int tableId, long partitionTimestamp) {
        of(tableToken, tableId, partitionTimestamp);
    }

    @Override
    public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) {
        ((TableWriter) svc).compactParquetPartition(partitionTimestamp);
        return 0;
    }

    @Override
    public void close() {
    }

    @Override
    public AsyncWriterCommand deserialize(TableWriterTask task) {
        this.partitionTimestamp = Unsafe.getLong(task.getData());
        return this;
    }

    @Override
    public int getCmdType() {
        return TableWriterTask.CMD_PARQUET_COMPACTION;
    }

    @Override
    public String getCommandName() {
        return TableWriterTask.getCommandName(TableWriterTask.CMD_PARQUET_COMPACTION);
    }

    @Override
    public long getCorrelationId() {
        return correlationId;
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

    @Override
    public AsyncWriterCommand newInstance() {
        return new ParquetPartitionCompactionCommand();
    }

    public void of(TableToken tableToken, int tableId, long partitionTimestamp) {
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.partitionTimestamp = partitionTimestamp;
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
