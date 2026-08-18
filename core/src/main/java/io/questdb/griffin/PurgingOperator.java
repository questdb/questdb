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

package io.questdb.griffin;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.BitmapIndexUtils;
import io.questdb.cairo.idx.IndexFactory;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.log.Log;
import io.questdb.mp.Sequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import org.jetbrains.annotations.Nullable;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import io.questdb.tasks.ColumnPurgeTask;

public final class PurgingOperator {
    public static final long TABLE_ROOT_PARTITION = Long.MIN_VALUE + 1;
    // SP2A: rendered cell segment per queued entry, parallel to cleanupColumnVersions' 4-long groups
    // (entry i occupies cleanupColumnVersions[i*4 .. i*4+3]; its segment is cellSegments[i]). null for
    // a plain table, and for the day-level TABLE_ROOT_PARTITION entry.
    //
    // A PARALLEL list rather than a wider stride, deliberately: cleanupColumnVersions is reused for
    // the async-reschedule tail below, which appends its own 4-long entries and hands them to
    // purgeColumnVersionAsync. Two strides in one list would be a trap for the next reader.
    private final ObjList<String> cellSegments = new ObjList<>();
    private final LongList cleanupColumnVersions = new LongList();
    private final ObjList<String> columnNames = new ObjList<>();
    private final FilesFacade ff;
    private final Log log;
    private final MessageBus messageBus;
    private final IntList updateColumnIndexes = new IntList();

    public PurgingOperator(
            Log log,
            CairoConfiguration configuration,
            MessageBus messageBus
    ) {
        this.log = log;
        this.messageBus = messageBus;
        this.ff = configuration.getFilesFacade();
    }

    /**
     * Day-level overload for callers whose operation is still GATED for composite tables
     * ({@code CONVERT PARTITION}, {@code DROP INDEX}, {@code UPDATE}, and the two table-root entries).
     * Passing no cell is correct for them precisely because a composite table cannot reach them.
     * <p>
     * <b>If you are enabling one of those operations for composite, do not call this.</b> A day-level
     * purge on a composite table silently leaves every cell's file on disk -- measured for
     * {@code DROP COLUMN} before SP2A: {@code E0/px.d}, {@code E1/px.d} and {@code E2/px.d} all
     * survived while the operation reported success.
     */
    public void add(
            int columnIndex,
            String columnName,
            int columnType,
            byte indexType,
            long columnVersion,
            long partitionTimestamp,
            long partitionNameTxn
    ) {
        add(columnIndex, columnName, columnType, indexType, columnVersion, partitionTimestamp, partitionNameTxn, null);
    }

    public void add(
            int columnIndex,
            String columnName,
            int columnType,
            byte indexType,
            long columnVersion,
            long partitionTimestamp,
            long partitionNameTxn,
            @Nullable String cellSegment
    ) {
        updateColumnIndexes.add(columnIndex);
        updateColumnIndexes.add(columnType);
        updateColumnIndexes.add(indexType);
        updateColumnIndexes.add(columnNames.size());
        columnNames.add(columnName);
        cleanupColumnVersions.add(columnIndex, columnVersion, partitionTimestamp, partitionNameTxn);
        cellSegments.add(cellSegment);
    }

    public void clear() {
        updateColumnIndexes.clear();
        cleanupColumnVersions.clear();
        cellSegments.clear();
    }

    public void purge(
            Path path,
            TableToken tableToken,
            int timestampType,
            int partitionBy,
            boolean asyncOnly,
            long truncateVersion,
            long txn
    ) {
        int rootLen = path.size();

        try {
            // Process updated column by column, one at the time
            int cleanupVersionSize = cleanupColumnVersions.size();
            int lastColumnIndex = -1;
            final int intsPerEntry = 4;
            updateColumnIndexes.sortGroups(intsPerEntry);
            for (int updatedCol = 0, nn = updateColumnIndexes.size(); updatedCol < nn; updatedCol += intsPerEntry) {
                int processColumnIndex = updateColumnIndexes.getQuick(updatedCol);
                if (processColumnIndex == lastColumnIndex) {
                    // Skip duplicate column index
                    continue;
                }

                lastColumnIndex = processColumnIndex;
                int columnType = updateColumnIndexes.getQuick(updatedCol + 1);
                byte indexType = (byte) updateColumnIndexes.getQuick(updatedCol + 2);
                int colNameIndex = updateColumnIndexes.getQuick(updatedCol + 3);
                String columnName = columnNames.getQuick(colNameIndex);

                for (int i = 0; i < cleanupVersionSize; i += 4) {
                    int columnIndex = (int) cleanupColumnVersions.getQuick(i);
                    long columnVersion = cleanupColumnVersions.getQuick(i + 1);
                    long partitionTimestamp = cleanupColumnVersions.getQuick(i + 2);
                    long partitionNameTxn = cleanupColumnVersions.getQuick(i + 3);

                    // Process updated column by column, one at a time
                    if (columnIndex == processColumnIndex) {
                        boolean columnPurged = !asyncOnly;
                        if (!asyncOnly) {
                            if (partitionTimestamp != TABLE_ROOT_PARTITION) {
                                path.trimTo(rootLen);
                                // SP2A: a composite partition is a CELL, so the column files live at
                                // <day>/<cell>/. The cell-blind form removed only the vestigial
                                // day-level file and left every cell's behind -- measured: after
                                // DROP COLUMN, E0/px.d, E1/px.d and E2/px.d all survived.
                                TableUtils.setPathForNativePartition(path, timestampType, partitionBy,
                                        partitionTimestamp, partitionNameTxn, cellSegments.getQuick(i / 4));
                                int pathPartitionLen = path.size();
                                TableUtils.dFile(path, columnName, columnVersion);
                                columnPurged = ff.removeQuiet(path.$());

                                if (ColumnType.isVarSize(columnType)) {
                                    TableUtils.iFile(path.trimTo(pathPartitionLen), columnName, columnVersion);
                                    columnPurged &= ff.removeQuiet(path.$());
                                }

                                if (indexType != IndexType.NONE) {
                                    if (IndexType.isPosting(indexType)) {
                                        // Enumerate every sealed .pv / .pc<N> for this column
                                        // instance across all on-disk sealTxn generations.
                                        // removeAllSealedFiles tolerates missing files, so
                                        // successive purge passes are idempotent. Returns true
                                        // if at least one removal failed and the file is still
                                        // on disk — fold that into columnPurged so the column
                                        // version is rescheduled for async purge.
                                        boolean sidecarRemovalFailed = PostingIndexUtils.removeAllSealedFiles(ff, path, pathPartitionLen, columnName, columnVersion);
                                        IndexFactory.keyFileName(indexType, path.trimTo(pathPartitionLen), columnName, columnVersion);
                                        columnPurged &= ff.removeQuiet(path.$());
                                        columnPurged &= !sidecarRemovalFailed;
                                    } else {
                                        // BITMAP keeps a single .v at columnVersion (no sealTxn axis).
                                        IndexFactory.valueFileName(indexType, path.trimTo(pathPartitionLen), columnName, columnVersion, columnVersion);
                                        columnPurged &= ff.removeQuiet(path.$());
                                        IndexFactory.keyFileName(indexType, path.trimTo(pathPartitionLen), columnName, columnVersion);
                                        columnPurged &= ff.removeQuiet(path.$());
                                    }
                                }
                            } else {
                                // This is removal of symbol files from the table root directory
                                // Symbol map files always use SYMBOL format (.k/.v)
                                TableUtils.charFileName(path.trimTo(rootLen), columnName, columnVersion);
                                columnPurged = ff.removeQuiet(path.$());
                                TableUtils.offsetFileName(path.trimTo(rootLen), columnName, columnVersion);
                                columnPurged &= ff.removeQuiet(path.$());
                                BitmapIndexUtils.keyFileName(path.trimTo(rootLen), columnName, columnVersion);
                                columnPurged &= ff.removeQuiet(path.$());
                                BitmapIndexUtils.valueFileName(path.trimTo(rootLen), columnName, columnVersion);
                                columnPurged &= ff.removeQuiet(path.$());
                            }
                        }

                        if (!columnPurged) {
                            // Schedule for async purge
                            cleanupColumnVersions.add(columnVersion, partitionTimestamp, partitionNameTxn, 0);
                        }
                    }
                }

                // if anything not purged, schedule async purge
                if (cleanupColumnVersions.size() > cleanupVersionSize) {
                    purgeColumnVersionAsync(
                            tableToken,
                            columnName,
                            tableToken.getTableId(),
                            (int) truncateVersion,
                            columnType,
                            indexType,
                            timestampType,
                            partitionBy,
                            txn,
                            cleanupColumnVersions,
                            cleanupVersionSize,
                            cleanupColumnVersions.size()
                    );
                    cleanupColumnVersions.setPos(cleanupVersionSize);

                    log.info().$("column purge scheduled [table=").$(tableToken)
                            .$(", column=").$safe(columnName)
                            .$(", updateTxn=").$(txn)
                            .I$();
                } else {
                    log.info().$("column purge complete [table=").$(tableToken)
                            .$(", column=").$safe(columnName)
                            .$(", newColumnVersion=").$(txn - 1)
                            .I$();
                }
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void purgeColumnVersionAsync(
            TableToken tableToken,
            String columnName,
            int tableId,
            int tableTruncateVersion,
            int columnType,
            byte indexType,
            int timestampType,
            int partitionBy,
            long updateTxn,
            @Transient LongList columnVersions,
            int columnVersionsLo,
            int columnVersionsHi
    ) {
        Sequence pubSeq = messageBus.getColumnPurgePubSeq();
        while (true) {
            long cursor = pubSeq.next();
            if (cursor > -1L) {
                ColumnPurgeTask task = messageBus.getColumnPurgeQueue().get(cursor);
                task.of(
                        tableToken,
                        columnName,
                        tableId,
                        tableTruncateVersion,
                        columnType,
                        indexType,
                        timestampType,
                        partitionBy,
                        updateTxn,
                        columnVersions,
                        columnVersionsLo,
                        columnVersionsHi
                );
                pubSeq.done(cursor);
                return;
            } else if (cursor == -1L) {
                // Queue overflow
                log.error().$("cannot schedule column purge, purge queue is full. Please run 'VACUUM TABLE \"").$safe(tableToken.getTableName())
                        .$("\"' [columnName=").$safe(columnName)
                        .$(", updateTxn=").$(updateTxn)
                        .I$();
                return;
            }
            Os.pause();
        }
    }

}
