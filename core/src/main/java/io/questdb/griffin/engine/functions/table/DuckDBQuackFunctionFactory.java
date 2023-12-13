/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.pool.DuckDBConnectionPool;
import io.questdb.cairo.sql.*;
import io.questdb.duckdb.*;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.GcUtf8String;

public class DuckDBQuackFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "quack(S)";
    }

    @Override
    public boolean isCursor() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        assert args.size() == 1;
        assert args.getQuick(0).isConstant();
        CharSequence str = args.getQuick(0).getStr(null);
        DirectUtf8Sequence query = new GcUtf8String(str.toString());
        try(DuckDBConnectionPool.Connection conn = sqlExecutionContext.getCairoEngine().getDuckDBConnection()) {
            long stmt = conn.prepare(query);
            long error = DuckDB.preparedGetError(stmt);
            if (error != 0) {
                DirectUtf8StringZ err = new DirectUtf8StringZ();
                err.of(error);
                throw SqlException.$(0, err.toString());
            }
            DuckDBRecordCursorFactory factory = new DuckDBRecordCursorFactory(stmt);
            return new CursorFunction(factory) {
                @Override
                public boolean isRuntimeConstant() {
                    return false;
                }
            };
        }
    }
}
