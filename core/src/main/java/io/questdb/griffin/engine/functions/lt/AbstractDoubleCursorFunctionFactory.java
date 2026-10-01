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

package io.questdb.griffin.engine.functions.lt;

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
 * Shared cold-path implementation of the {@code double (< | >) (sub-query)} factories: validates
 * the scalar sub-query metadata and the left operand type, then delegates the direction-specific
 * function construction to the concrete factory.
 */
public abstract class AbstractDoubleCursorFunctionFactory implements FunctionFactory {

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
        final RecordCursorFactory factory = args.getQuick(1).getRecordCursorFactory();

        // verify that the factory has metadata we can support:
        // 1. the factory must provide only one column
        // 2. the column must be a numeric type we can widen to double (or null)
        final RecordMetadata metadata = ScalarSubQueryUtils.assertSingleColumn(factory, argPositions.getQuick(1));
        final Function arg0 = args.getQuick(0);
        // Accept DOUBLE, FLOAT and NULL left operands. FLOAT widens to double losslessly via
        // Function#getDouble, so the comparison stays exact; a bare NULL literal reads as NaN and
        // follows the sentinel-null comparison convention; other numeric types route to their
        // own (IC/LC) cursor factories and never reach this DOUBLE cursor factory.
        final int arg0Tag = ColumnType.tagOf(arg0.getType());
        if (arg0Tag != ColumnType.DOUBLE && arg0Tag != ColumnType.FLOAT && arg0Tag != ColumnType.NULL) {
            throw SqlException.$(argPositions.getQuick(0), "left operand must be a DOUBLE or FLOAT, found: ")
                    .put(ColumnType.nameOf(arg0.getType()));
        }
        final int cursorTag = ColumnType.tagOf(metadata.getColumnType(0));
        return switch (cursorTag) {
            case ColumnType.DOUBLE, ColumnType.FLOAT, ColumnType.LONG, ColumnType.INT, ColumnType.SHORT,
                 ColumnType.BYTE, ColumnType.NULL ->
                    newDoubleFunc(factory, arg0, args.getQuick(1), cursorTag, argPositions.getQuick(1));
            default ->
                    throw SqlException.$(argPositions.getQuick(1), "cannot compare DOUBLE and ").put(ColumnType.nameOf(metadata.getColumnType(0)));
        };
    }

    protected abstract Function newDoubleFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int cursorTag, int rightPos);
}
