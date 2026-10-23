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

package io.questdb.test.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;

/**
 * Drops the index of a symbol column, after the WAL drained like {@link FuzzAddCoveringIndexOperation},
 * and indexes the column again with {@code newIndexTypeName} when it is not null, which changes the
 * index type. Each change queues the purge of the index files of the column instance it replaces.
 */
public class FuzzDropIndexOperation implements FuzzTransactionOperation {
    private static final Log LOG = LogFactory.getLog(FuzzDropIndexOperation.class);
    // SQL name of the index type the column gets after DROP INDEX: "BITMAP", "POSTING", "POSTING DELTA"
    // or "POSTING EF"; null keeps the column without an index
    private final String newIndexTypeName;
    private final int symbolColumnIndex;

    public FuzzDropIndexOperation(int symbolColumnIndex, String newIndexTypeName) {
        this.symbolColumnIndex = symbolColumnIndex;
        this.newIndexTypeName = newIndexTypeName;
    }

    @Override
    public boolean apply(Rnd tempRnd, CairoEngine engine, TableWriterAPI wApi, int virtualTimestampIndex, LongList excludedTsIntervals) {
        return false;
    }

    public void executePostDrain(CairoEngine engine, String tableName) {
        final String columnName;
        try (TableRecordMetadata metadata = engine.getTableMetadata(engine.verifyTableName(tableName))) {
            if (symbolColumnIndex >= metadata.getColumnCount()) {
                return;
            }
            final int columnType = metadata.getColumnType(symbolColumnIndex);
            if (columnType < 0 || !ColumnType.isSymbol(columnType) || !metadata.isColumnIndexed(symbolColumnIndex)) {
                return;
            }
            // DROP INDEX of a column that a POSTING index INCLUDEs gives the column a new column name
            // txn, and the covering sidecars keep the old one, so covering queries read NULL for it.
            // The guard goes when DROP INDEX moves the sidecars of the indexes that cover the column.
            if (isCoveredByAnotherIndex(metadata, metadata.getWriterIndex(symbolColumnIndex))) {
                LOG.info().$("skipping drop index of a covered column [table=").$(tableName)
                        .$(", column=").$(metadata.getColumnName(symbolColumnIndex)).I$();
                return;
            }
            columnName = metadata.getColumnName(symbolColumnIndex);
        } catch (CairoException e) {
            return;
        }

        final String alterColumn = "ALTER TABLE \"" + tableName + "\" ALTER COLUMN \"" + columnName + "\" ";
        try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1)) {
            context.with(AllowAllSecurityContext.INSTANCE);
            engine.execute(alterColumn + "DROP INDEX", context);
            if (newIndexTypeName != null) {
                engine.execute(alterColumn + "ADD INDEX TYPE " + newIndexTypeName, context);
            }
        } catch (SqlException | CairoException e) {
            // The checks above read getTableMetadata(), which for WAL tables is the lagging applied
            // metadata, and the ALTER is validated against the sequencer metadata, the same races that
            // FuzzAddCoveringIndexOperation tolerates.
            if (Chars.contains(e.getMessage(), "not indexed")
                    || Chars.contains(e.getMessage(), "already indexed")
                    || Chars.contains(e.getMessage(), "does not exist")
                    || Chars.contains(e.getMessage(), "Invalid column")
                    || Chars.contains(e.getMessage(), "table busy")
                    || Chars.contains(e.getMessage(), "indexes are only supported for symbol type")) {
                return;
            }
            if (e instanceof CairoException ce && ce.isTableDropped()) {
                return;
            }
            throw e instanceof RuntimeException re ? re : new RuntimeException(e);
        }
    }

    public String getNewIndexTypeName() {
        return newIndexTypeName;
    }

    public int getSymbolColumnIndex() {
        return symbolColumnIndex;
    }

    private static boolean isCoveredByAnotherIndex(TableRecordMetadata metadata, int writerIndex) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            // a tombstone slot of a dropped covered column holds -1 and matches no writer index
            final IntList coveringColumnIndices = metadata.getColumnMetadata(i).getCoveringColumnIndices();
            if (coveringColumnIndices != null && coveringColumnIndices.contains(writerIndex)) {
                return true;
            }
        }
        return false;
    }
}
