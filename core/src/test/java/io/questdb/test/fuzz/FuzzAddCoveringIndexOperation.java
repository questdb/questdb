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
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;

public class FuzzAddCoveringIndexOperation implements FuzzTransactionOperation {

    private final IntList includeColumnIndices;
    // SQL name of the posting kind this operation indexes with: "POSTING", "POSTING DELTA" or
    // "POSTING EF". Drawn from the generator's Rnd, not here, so a seed replays the same kind.
    // The row-id encoding is a property of the index type, and the covering decoder reads it
    // back per kind; fuzzing only the default left the delta and EF layouts untested.
    private final String indexTypeName;
    private final int symbolColumnIndex;

    public FuzzAddCoveringIndexOperation(int symbolColumnIndex, IntList includeColumnIndices, String indexTypeName) {
        this.symbolColumnIndex = symbolColumnIndex;
        this.includeColumnIndices = includeColumnIndices;
        this.indexTypeName = indexTypeName;
    }

    private static boolean isTolerated(Throwable e) {
        final String message = e.getMessage();
        // A concurrent drop/re-add (column-name reuse) can turn the column into a non-symbol type, or remove it,
        // between the writer's metadata and the sequencer's that the ALTER is validated against.
        return Chars.contains(message, "already indexed")
                || Chars.contains(message, "does not exist")
                || Chars.contains(message, "table busy")
                || Chars.contains(message, "indexes are only supported for symbol type")
                || (e instanceof CairoException ce && ce.isTableDropped());
    }

    /**
     * Adds the covering index through the writer, in order with the other operations of the transaction stream,
     * the way {@link FuzzDropPartitionOperation} drops partitions.
     */
    @Override
    public boolean apply(Rnd tempRnd, CairoEngine engine, TableWriterAPI wApi, int virtualTimestampIndex, LongList excludedTsIntervals) {
        final String sql = toSql(wApi.getMetadata(), wApi.getTableToken().getTableName());
        if (sql == null) {
            return true;
        }
        try (
                SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1);
                SqlCompiler sqlCompiler = engine.getSqlCompiler()
        ) {
            context.with(AllowAllSecurityContext.INSTANCE);
            final CompiledQuery query = sqlCompiler.compile(sql, context);
            final AlterOperation alterOp = query.getAlterOperation();
            alterOp.withSqlStatement(sql);
            alterOp.withContext(context);
            wApi.apply(alterOp, false);
        } catch (SqlException | CairoException e) {
            if (!isTolerated(e)) {
                throw e instanceof RuntimeException re ? re : new RuntimeException(e);
            }
        }
        return true;
    }

    /**
     * The ALTER adding the index, resolved against the writer's metadata, or null when the column is no longer an
     * unindexed SYMBOL or none of the columns to include is left.
     */
    private String toSql(TableRecordMetadata metadata, String tableName) {
        if (symbolColumnIndex >= metadata.getColumnCount()) {
            return null;
        }
        final int colType = metadata.getColumnType(symbolColumnIndex);
        if (colType < 0 || !ColumnType.isSymbol(colType) || metadata.isColumnIndexed(symbolColumnIndex)) {
            return null;
        }
        final StringBuilder includeList = new StringBuilder();
        for (int i = 0, n = includeColumnIndices.size(); i < n; i++) {
            final int idx = includeColumnIndices.getQuick(i);
            if (idx >= metadata.getColumnCount() || metadata.getColumnType(idx) < 0) {
                continue;
            }
            if (!includeList.isEmpty()) {
                includeList.append(", ");
            }
            includeList.append('"').append(metadata.getColumnName(idx)).append('"');
        }
        if (includeList.isEmpty()) {
            return null;
        }
        return "ALTER TABLE \"" + tableName
                + "\" ALTER COLUMN \"" + metadata.getColumnName(symbolColumnIndex)
                + "\" ADD INDEX TYPE " + indexTypeName + " INCLUDE (" + includeList + ")";
    }
}
