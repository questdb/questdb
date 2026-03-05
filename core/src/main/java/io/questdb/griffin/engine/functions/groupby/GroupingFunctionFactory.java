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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * Factory for the GROUPING(col1, col2, ...) function. Returns an integer
 * bitmask indicating which columns are rolled up in the current grouping set.
 * For a single argument, returns 0 (actively grouped) or 1 (rolled up).
 * For multiple arguments, returns a multi-bit integer.
 *
 * <p>This function is only valid in queries that use GROUPING SETS, ROLLUP,
 * or CUBE. The {@link GroupingSetsRecordCursorFactory} wires up the function
 * at construction time with the actual grouping set layout.</p>
 */
public class GroupingFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "grouping(V)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (args == null || args.size() == 0) {
            throw SqlException.$(position, "GROUPING() requires at least one argument");
        }

        IntList columnIndices = new IntList(args.size());
        for (int i = 0, n = args.size(); i < n; i++) {
            Function arg = args.getQuick(i);
            if (!(arg instanceof ColumnFunction)) {
                throw SqlException.$(argPositions.getQuick(i), "GROUPING() arguments must be column references");
            }
            columnIndices.add(((ColumnFunction) arg).getColumnIndex());
        }

        return new GroupingFunction(columnIndices);
    }
}
