/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.*;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.constants.*;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;

public class SampleByFillNullRecordCursorFactory extends AbstractSampleByRecordCursorFactory {

    private static final SampleByCursorLambda CURSOR_LAMBDA = SampleByFillNullRecordCursorFactory::createCursor;

    public SampleByFillNullRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @NotNull TimestampSampler timestampSampler,
            @Transient @NotNull QueryModel model,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull FunctionParser functionParser,
            @Transient @NotNull SqlExecutionContext executionContext,
            @Transient @NotNull BytecodeAssembler asm,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes
    ) throws SqlException {
        super(
                configuration,
                base,
                timestampSampler,
                model,
                listColumnFilter,
                functionParser,
                executionContext,
                asm,
                CURSOR_LAMBDA,
                keyTypes,
                valueTypes
        );
    }

    @NotNull
    public static SampleByFillValueRecordCursor createCursor(
            Map map,
            RecordSink mapSink,
            @NotNull TimestampSampler timestampSampler,
            int timestampIndex,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            IntIntHashMap symbolTableIndex,
            int keyCount
    ) throws SqlException {
        try {
            return new SampleByFillValueRecordCursor(
                    map,
                    mapSink,
                    groupByFunctions,
                    recordFunctions,
                    createPlaceholderFunctions(recordFunctions),
                    timestampIndex,
                    timestampSampler,
                    symbolTableIndex
            );
        } catch (SqlException e) {
            GroupByUtils.closeGroupByFunctions(groupByFunctions);
            throw e;
        }
    }

    @NotNull
    private static ObjList<Function> createPlaceholderFunctions(ObjList<Function> recordFunctions) throws SqlException {
        final ObjList<Function> placeholderFunctions = new ObjList<>();
        for (int i = 0, n = recordFunctions.size(); i < n; i++) {
            Function function = recordFunctions.getQuick(i);
            if (function instanceof GroupByFunction) {
                switch (function.getType()) {
                    case ColumnType.INT:
                        placeholderFunctions.add(new IntConstant(function.getPosition(), Numbers.INT_NaN));
                        break;
                    case ColumnType.LONG:
                        placeholderFunctions.add(new LongConstant(function.getPosition(), Numbers.LONG_NaN));
                        break;
                    case ColumnType.FLOAT:
                        placeholderFunctions.add(new FloatConstant(function.getPosition(), Float.NaN));
                        break;
                    case ColumnType.DOUBLE:
                        placeholderFunctions.add(new DoubleConstant(function.getPosition(), Double.NaN));
                        break;
                    case ColumnType.BYTE:
                        placeholderFunctions.add(new ByteConstant(function.getPosition(), (byte) 0));
                        break;
                    case ColumnType.SHORT:
                        placeholderFunctions.add(new ShortConstant(function.getPosition(), (short) 0));
                        break;
                    default:
                        throw SqlException.$(function.getPosition(), "Unsupported type: ").put(ColumnType.nameOf(function.getType()));
                }
            } else {
                placeholderFunctions.add(function);
            }
        }
        return placeholderFunctions;
    }
}
