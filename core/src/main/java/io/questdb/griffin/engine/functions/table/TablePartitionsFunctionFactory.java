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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionRequirements;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.table.ShowPartitionsRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class TablePartitionsFunctionFactory implements FunctionFactory {
    @Override
    public int getExecutionRequirements() {
        // resolves the table against the caller or its enclosing view, see SqlExecutionRequirements
        return SqlExecutionRequirements.REQUIRES_ENTERPRISE_SECURITY_CONTEXT;
    }

    @Override
    public String getSignature() {
        return "table_partitions(s)";
    }

    @Override
    public boolean isCursor() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPos, CairoConfiguration config, SqlExecutionContext context) throws SqlException {
        final TableToken tt;
        final SqlExecutionContext.TableFunctionView view = context.getTableFunctionView();
        int timestampType;
        try {
            final CharSequence tableName = args.getQuick(0).getStrA(null);
            tt = context.getTableToken(tableName);
            // Outside a view, an invisible table fails like a missing one, echoing its SQL spelling.
            if (!context.isTableFunctionVisibleAtCompile(tt, view)) {
                throw CairoException.tableDoesNotExist(tableName);
            }
            try (TableMetadata metadata = context.getCairoEngine().getTableMetadata(tt)) {
                timestampType = metadata.getTimestampType();
            }
        } catch (CairoException e) {
            if (e.isAuthorizationError()) {
                // e.g. a reader of the enclosing view without SELECT on it, see
                // SqlExecutionContext.isTableFunctionVisible(): it stays an authorization error
                throw e;
            }
            throw SqlException.$(argPos.getQuick(0), e.getFlyweightMessage());
        }
        return new CursorFunction(new ShowPartitionsRecordCursorFactory(tt, timestampType, argPos.getQuick(0), view));
    }
}
