/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class SumDecimalGroupByFunctionFactory implements FunctionFactory {

    public static GroupByFunction newInstance(Function arg, int position) {
        int k = ColumnType.tagOf(arg.getType());
        return switch (k) {
            case ColumnType.DECIMAL8 -> new SumDecimal8GroupByFunction(arg);
            case ColumnType.DECIMAL16 -> new SumDecimal16GroupByFunction(arg);
            case ColumnType.DECIMAL32 -> new SumDecimal32GroupByFunction(arg, position);
            case ColumnType.DECIMAL64 -> new SumDecimal64GroupByFunction(arg, position);
            case ColumnType.DECIMAL128 -> new SumDecimal128GroupByFunction(arg, position);
            default -> new SumDecimal256GroupByFunction(arg, position);
        };
    }

    @Override
    public String getSignature() {
        return "sum(Ξ)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        Function arg = args.getQuick(0);
        return newInstance(arg, position);
    }
}
