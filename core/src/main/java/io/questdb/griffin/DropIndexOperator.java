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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.TableUtils.dFile;

public class DropIndexOperator {
    private static final Log LOG = LogFactory.getLog(DropIndexOperator.class);
    private final FilesFacade ff;
    private final Path other;
    private final Path path;
    private final PurgingOperator purgingOperator;
    private final LongList rollbackColumnVersions = new LongList();
    private final IntList rollbackCellKeys = new IntList();
    private final int rootLen;
    private final TableWriter tableWriter;

    public DropIndexOperator(
            CairoConfiguration configuration,
            TableWriter tableWriter,
            Path path,
            Path other,
            int rootLen,
            PurgingOperator purgingOperator
    ) {
        this.other = other;
        this.tableWriter = tableWriter;
        this.rootLen = rootLen;
        this.purgingOperator = purgingOperator;
        this.path = path;
        this.ff = configuration.getFilesFacade();
    }

    public void executeDropIndex(String columnName, int columnIndex) {
        int timestampType = tableWriter.getMetadata().getTimestampType();
        int partitionBy = tableWriter.getPartitionBy();
        int partitionCount = tableWriter.getPartitionCount();
        final boolean composite = tableWriter.isComposite();
        byte indexType = tableWriter.getMetadata().getColumnIndexType(columnIndex);
        try {
            purgingOperator.clear();
            rollbackColumnVersions.clear();
            rollbackCellKeys.clear();
            for (int pIndex = 0; pIndex < partitionCount; pIndex++) {
                long pTimestamp = tableWriter.getPartitionTimestamp(pIndex);
                long pVersion = tableWriter.getPartitionNameTxn(pIndex);
                // Per-CELL. On a composite table several cells share one raw partition timestamp, so
                // the by-timestamp lookups below answer for cellKey 0 and the bare path names the day
                // CONTAINER. That made src and hardLink resolve to the SAME file and DROP INDEX failed
                // with errno=17. A composite attached entry IS a cell, so resolving by pIndex is exact;
                // for a plain table cellKey is 0 and cellSegment null, i.e. unchanged behaviour.
                final int cellKey = tableWriter.getPartitionCellKey(pIndex);
                final String cellSegment = composite ? renderCellSegment(cellKey) : null;
                long columnVersion = tableWriter.getColumnNameTxn(pTimestamp, cellKey, columnIndex);
                // Cell-scoped, like the getColumnNameTxn above it. The 3-arg form answers for cellKey
                // 0, and this value is written straight back into THIS cell's _cv record by the
                // upsertColumnVersion below -- so on cells with differing column tops (ADD COLUMN while
                // cells hold different row counts) every cell inherited cell 0's top. MEASURED: after
                // DROP INDEX the ETH cell's tag values read back as NULL while the row count stayed
                // correct, i.e. silent data loss. Covered by CompositeDropIndexColumnTopTest.
                long columnTop = tableWriter.getColumnTop(pTimestamp, cellKey, columnIndex, -1);
                byte partitionFormat = tableWriter.getPartitionFormat(pIndex);

                if (columnTop != -1) {
                    // bump up column version, metadata will be updated later
                    // Cell-scoped: the 3-arg form bumps cellKey 0's version, so on a composite table the
                    // read below returned the UNCHANGED version for this cell and src == hardLink.
                    tableWriter.upsertColumnVersion(pTimestamp, cellKey, columnIndex, columnTop);

                    if (partitionFormat == PartitionFormat.NATIVE) {
                        final long columnDropIndexVersion = tableWriter.getColumnNameTxn(pTimestamp, cellKey, columnIndex);
                        // create hard link to column data
                        // src
                        partitionDFile(path, rootLen, timestampType, partitionBy, pTimestamp, pVersion, columnName, columnVersion, cellSegment);
                        // hard link
                        partitionDFile(other, rootLen, timestampType, partitionBy, pTimestamp, pVersion, columnName, columnDropIndexVersion, cellSegment);
                        if (ff.hardLink(path.$(), other.$()) == -1) {
                            throw CairoException.critical(ff.errno())
                                    .put("cannot hardLink [src=").put(path)
                                    .put(", hardLink=").put(other)
                                    .put(']');
                        }
                        rollbackColumnVersions.add(columnIndex, columnDropIndexVersion, pTimestamp, pVersion);
                        rollbackCellKeys.add(cellKey);
                    }

                    // add to cleanup tasks, the index will be removed in due time
                    purgingOperator.add(columnIndex, columnName, ColumnType.SYMBOL, indexType, columnVersion, pTimestamp, pVersion, cellSegment);
                }
            }
        } catch (Throwable th) {
            LOG.error().$("Could not DROP INDEX: ").$safe(th.getMessage()).$();
            purgingOperator.clear();

            // cleanup successful links prior to the failed link operation
            int limit = rollbackColumnVersions.size();
            if (limit / 4 < partitionCount) {
                for (int i = 0; i < limit; i += 4) {
                    final long columnDropIndexVersion = rollbackColumnVersions.getQuick(i + 1);
                    final long pTimestamp = rollbackColumnVersions.getQuick(i + 2);
                    final long partitionNameTxn = rollbackColumnVersions.getQuick(i + 3);
                    // Same cell the link was created under, or the rollback deletes nothing (or, worse,
                    // the wrong cell's file).
                    final String rollbackCell = composite ? renderCellSegment(rollbackCellKeys.getQuick(i / 4)) : null;
                    partitionDFile(other, rootLen, timestampType, partitionBy, pTimestamp, partitionNameTxn, columnName, columnDropIndexVersion, rollbackCell);
                    if (!ff.removeQuiet(other.$())) {
                        LOG.info().$("Please remove this file \"").$(other).$('"').I$();
                    }
                }
            }
            throw th;
        } finally {
            path.trimTo(rootLen);
            other.trimTo(rootLen);
        }
    }

    /**
     * Renders {@code cellKey}'s directory segment. A fresh String per call rather than the writer's
     * thread-local sink, because these values are STORED (in the purge queue and the rollback list)
     * and outlive the loop iteration that produced them.
     */
    private String renderCellSegment(int cellKey) {
        final StringSink sink = new StringSink();
        tableWriter.renderCellSegment(sink, cellKey);
        return sink.toString();
    }

    private static void partitionDFile(
            Path path,
            int rootLen,
            int timestampType,
            int partitionBy,
            long partitionTimestamp,
            long partitionNameTxn,
            CharSequence columnName,
            long columnNameTxn,
            @Nullable CharSequence cellSegment
    ) {
        TableUtils.setPathForNativePartition(
                path.trimTo(rootLen),
                timestampType,
                partitionBy,
                partitionTimestamp,
                partitionNameTxn,
                cellSegment
        );
        dFile(path, columnName, columnNameTxn);
    }
}
