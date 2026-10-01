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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class CountDecimalWindowFunctionFactory extends AbstractWindowFunctionFactory {

    private static final CountFunctionFactoryHelper.IsRecordNotNull DECIMAL16_NOT_NULL =
            (arg, record) -> arg.getDecimal16(record) != Decimals.DECIMAL16_NULL;
    private static final CountFunctionFactoryHelper.IsRecordNotNull DECIMAL32_NOT_NULL =
            (arg, record) -> arg.getDecimal32(record) != Decimals.DECIMAL32_NULL;
    private static final CountFunctionFactoryHelper.IsRecordNotNull DECIMAL64_NOT_NULL =
            (arg, record) -> arg.getDecimal64(record) != Decimals.DECIMAL64_NULL;
    private static final CountFunctionFactoryHelper.IsRecordNotNull DECIMAL8_NOT_NULL =
            (arg, record) -> arg.getDecimal8(record) != Decimals.DECIMAL8_NULL;

    @Override
    public String getSignature() {
        return "count(Ξ)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final int argType = args.get(0).getType();
        final int tag = ColumnType.tagOf(argType);
        final CountFunctionFactoryHelper.IsRecordNotNull predicate = switch (tag) {
            case ColumnType.DECIMAL8 -> DECIMAL8_NOT_NULL;
            case ColumnType.DECIMAL16 -> DECIMAL16_NOT_NULL;
            case ColumnType.DECIMAL32 -> DECIMAL32_NOT_NULL;
            case ColumnType.DECIMAL64 -> DECIMAL64_NOT_NULL;
            case ColumnType.DECIMAL128 -> {
                final Decimal128 scratch = new Decimal128();
                yield (arg, record) -> {
                    arg.getDecimal128(record, scratch);
                    return !scratch.isNull();
                };
            }
            default -> {
                final Decimal256 scratch = new Decimal256();
                yield (arg, record) -> {
                    arg.getDecimal256(record, scratch);
                    return !scratch.isNull();
                };
            }
        };
        return CountFunctionFactoryHelper.newCountWindowFunction(this, position, args, configuration, sqlExecutionContext, predicate);
    }
}
