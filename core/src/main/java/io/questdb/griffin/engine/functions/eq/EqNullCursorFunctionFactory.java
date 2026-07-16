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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.ScalarSubQueryUtils;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * Resolves equality between an untyped NULL literal and a scalar sub-query. The exact NULL/cursor
 * signature wins before the NULL literal can tie across the typed cursor overloads; this factory
 * then uses the sub-query metadata to select the same typed implementation as a typed left operand.
 */
public class EqNullCursorFunctionFactory implements FunctionFactory {
    private static final FunctionFactory DOUBLE_FACTORY = new EqDoubleCursorFunctionFactory();
    private static final FunctionFactory INT_FACTORY = new EqIntCursorFunctionFactory();
    private static final FunctionFactory LONG_FACTORY = new EqLongCursorFunctionFactory();
    private static final FunctionFactory TIMESTAMP_FACTORY = new EqTimestampCursorFunctionFactory();

    @Override
    public String getSignature() {
        return "=(oC)";
    }

    @Override
    public boolean isBoolean() {
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
        final RecordCursorFactory cursorFactory = args.getQuick(1).getRecordCursorFactory();
        final RecordMetadata metadata = ScalarSubQueryUtils.assertSingleColumn(cursorFactory, argPositions.getQuick(1));
        final FunctionFactory factory = switch (ColumnType.tagOf(metadata.getColumnType(0))) {
            case ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.NULL -> INT_FACTORY;
            case ColumnType.LONG -> LONG_FACTORY;
            case ColumnType.FLOAT, ColumnType.DOUBLE -> DOUBLE_FACTORY;
            default -> TIMESTAMP_FACTORY;
        };
        return factory.newInstance(position, args, argPositions, configuration, sqlExecutionContext);
    }
}
