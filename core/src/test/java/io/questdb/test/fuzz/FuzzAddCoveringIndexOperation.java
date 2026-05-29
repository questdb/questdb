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
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;

public class FuzzAddCoveringIndexOperation implements FuzzTransactionOperation {

    private final IntList includeColumnIndices;
    private final int symbolColumnIndex;

    public FuzzAddCoveringIndexOperation(int symbolColumnIndex, IntList includeColumnIndices) {
        this.symbolColumnIndex = symbolColumnIndex;
        this.includeColumnIndices = includeColumnIndices;
    }

    @Override
    public boolean apply(Rnd tempRnd, CairoEngine engine, TableWriterAPI wApi, int virtualTimestampIndex, LongList excludedTsIntervals) {
        return false;
    }

    public void executePostDrain(CairoEngine engine, String tableName) {
        TableRecordMetadata metadata;
        try {
            metadata = engine.getTableMetadata(engine.verifyTableName(tableName));
        } catch (CairoException e) {
            return;
        }

        try {
            if (symbolColumnIndex >= metadata.getColumnCount()) {
                return;
            }
            int colType = metadata.getColumnType(symbolColumnIndex);
            if (colType < 0 || !ColumnType.isSymbol(colType)) {
                return;
            }
            if (metadata.isColumnIndexed(symbolColumnIndex)) {
                return;
            }

            String symColName = metadata.getColumnName(symbolColumnIndex);
            StringBuilder includeList = new StringBuilder();
            for (int i = 0, n = includeColumnIndices.size(); i < n; i++) {
                int idx = includeColumnIndices.getQuick(i);
                if (idx >= metadata.getColumnCount()) {
                    continue;
                }
                int incType = metadata.getColumnType(idx);
                if (incType < 0) {
                    continue;
                }
                if (!includeList.isEmpty()) {
                    includeList.append(", ");
                }
                includeList.append('"').append(metadata.getColumnName(idx)).append('"');
            }
            if (includeList.isEmpty()) {
                return;
            }

            String sql = "ALTER TABLE \"" + tableName
                    + "\" ALTER COLUMN \"" + symColName
                    + "\" ADD INDEX TYPE POSTING INCLUDE (" + includeList + ")";

            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1)) {
                context.with(AllowAllSecurityContext.INSTANCE);
                engine.execute(sql, context);
            } catch (SqlException | CairoException e) {
                if (Chars.contains(e.getMessage(), "already indexed")
                        || Chars.contains(e.getMessage(), "does not exist")
                        || Chars.contains(e.getMessage(), "column is already indexed")
                        || Chars.contains(e.getMessage(), "table busy")) {
                    return;
                }
                if (e instanceof CairoException ce && ce.isTableDropped()) {
                    return;
                }
                throw e instanceof RuntimeException re ? re : new RuntimeException(e);
            }
        } finally {
            metadata.close();
        }
    }
}
