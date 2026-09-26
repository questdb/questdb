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
import io.questdb.std.LongList;
import io.questdb.tasks.TableWriterTask;

/**
 * Carries the compaction sweep's decision for one composite partition to its writer - see {@code
 * PartitionCompactionScanJob} - in one of two modes:
 * <ul>
 *     <li>REWRITE, the default: swaps in a copy built off a {@link TableReader} snapshot, without ever holding the
 *     writer for the copy itself.</li>
 *     <li>MAKE-PLAIN ({@link #ofMakePlain}): for a partition already reduced to a single piece at row 0, where the
 *     copy buys nothing - the writer drops the dead space and trims the files in place. Nothing is staged, so
 *     {@code liveRows} and the recorded tops go unused.</li>
 *     <li>MERGE ({@link #ofMerge}): the whole LOGICAL partition - the main directory plus every MOVE-TAIL split -
 *     copied into one staging directory, which replaces the run of {@code _txn} entries it was built from.</li>
 * </ul>
 * All three are the same errand - compact this partition - so they share one command and one lock reason.
 */
public class CompositePartitionSwapCommand implements AsyncWriterCommand {
    /**
     * Longs per {@link #folders} entry: the folder's own start timestamp, its name txn, its generation
     * (a composite folder's writer txn, zero for a plain one), its live row count and the column version its
     * files carried when the copy read them. Together they are the state the writer re-checks each source
     * folder against before it swaps the merged copy in.
     */
    public static final int LONGS_PER_FOLDER = 5;
    private final ColumnTopRecorder columnTops = new ColumnTopRecorder();
    /**
     * MERGE only: the source folders the staging copy was built from, in {@code _txn} order.
     */
    private final LongList folders = new LongList();
    private long correlationId = -1L;
    private long expectedMetadataVersion;
    private long expectedSrcNameTxn;
    private long expectedWriterTxn;
    private boolean isMakePlain;
    private boolean isMerge;
    private long liveRows;
    private long partitionTimestamp;
    private int tableId;
    private TableToken tableToken;

    @Override
    public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) {
        if (isMerge) {
            ((TableWriter) svc).swapMergedLogicalPartition(
                    partitionTimestamp,
                    folders,
                    expectedSrcNameTxn,
                    expectedWriterTxn,
                    expectedMetadataVersion,
                    liveRows,
                    columnTops
            );
        } else if (isMakePlain) {
            ((TableWriter) svc).makePartitionPlainInPlace(
                    partitionTimestamp,
                    expectedSrcNameTxn,
                    expectedWriterTxn,
                    expectedMetadataVersion
            );
        } else {
            ((TableWriter) svc).swapCompactedCompositePartition(
                    partitionTimestamp,
                    expectedSrcNameTxn,
                    expectedWriterTxn,
                    expectedMetadataVersion,
                    liveRows,
                    columnTops
            );
        }
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

    public long getExpectedMetadataVersion() {
        return expectedMetadataVersion;
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

    public boolean isMakePlain() {
        return isMakePlain;
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
            long expectedMetadataVersion,
            long liveRows
    ) {
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.partitionTimestamp = partitionTimestamp;
        this.expectedSrcNameTxn = expectedSrcNameTxn;
        this.expectedWriterTxn = expectedWriterTxn;
        this.expectedMetadataVersion = expectedMetadataVersion;
        this.liveRows = liveRows;
        this.isMakePlain = false;
        this.isMerge = false;
        this.folders.clear();
        this.columnTops.clear();
    }

    /**
     * The MAKE-PLAIN mode: no staging directory, no copy, so no live row count and no recorded tops.
     */
    public void ofMakePlain(
            TableToken tableToken,
            int tableId,
            long partitionTimestamp,
            long expectedSrcNameTxn,
            long expectedWriterTxn,
            long expectedMetadataVersion
    ) {
        of(tableToken, tableId, partitionTimestamp, expectedSrcNameTxn, expectedWriterTxn, expectedMetadataVersion, 0);
        this.isMakePlain = true;
    }

    /**
     * The MERGE mode. {@code folders} carries {@link #LONGS_PER_FOLDER} longs per source folder, in {@code _txn}
     * order. The first folder's name txn names the staging directory - {@code
     * <logicalPartition>.<firstFolderNameTxn>.merging<folderCount>} - so the writer can find what the job built
     * without being told the path.
     *
     * @param logicalPartitionTimestamp the start of the logical partition, which is where the merged directory lands
     * @param folders                   the source folders, copied into this command
     * @param liveRows                  the live rows of the whole logical partition, which the staging copy holds
     */
    public void ofMerge(
            TableToken tableToken,
            int tableId,
            long logicalPartitionTimestamp,
            LongList folders,
            long expectedMetadataVersion,
            long liveRows
    ) {
        assert folders.size() >= 2 * LONGS_PER_FOLDER : "a merge needs at least two folders";
        of(
                tableToken,
                tableId,
                logicalPartitionTimestamp,
                folders.getQuick(1),
                folders.getQuick(2),
                expectedMetadataVersion,
                liveRows
        );
        this.isMerge = true;
        this.folders.add(folders);
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
