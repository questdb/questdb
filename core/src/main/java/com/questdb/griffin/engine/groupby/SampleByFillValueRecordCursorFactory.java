/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.groupby;

import com.questdb.cairo.*;
import com.questdb.cairo.map.Map;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.griffin.FunctionParser;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.SqlExecutionContext;
import com.questdb.griffin.engine.functions.GroupByFunction;
import com.questdb.griffin.engine.functions.constants.DoubleConstant;
import com.questdb.griffin.engine.functions.constants.FloatConstant;
import com.questdb.griffin.engine.functions.constants.IntConstant;
import com.questdb.griffin.engine.functions.constants.LongConstant;
import com.questdb.griffin.model.ExpressionNode;
import com.questdb.griffin.model.QueryModel;
import com.questdb.std.*;
import org.jetbrains.annotations.NotNull;

public class SampleByFillValueRecordCursorFactory extends AbstractSampleByRecordCursorFactory {
    public SampleByFillValueRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @NotNull TimestampSampler timestampSampler,
            @Transient @NotNull QueryModel model,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull FunctionParser functionParser,
            @Transient @NotNull SqlExecutionContext executionContext,
            @Transient @NotNull BytecodeAssembler asm,
            @Transient @NotNull ObjList<ExpressionNode> fillValues,
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
                (
                        map,
                        sink,
                        sampler,
                        timestampIndex,
                        groupByFunctions,
                        recordFunctions,
                        symbolTableIndex

                ) -> createCursor(
                        map,
                        sink,
                        sampler,
                        timestampIndex,
                        groupByFunctions,
                        recordFunctions,
                        symbolTableIndex,
                        fillValues
                ),
                keyTypes,
                valueTypes
        );
    }

    public static SampleByFillValueRecordCursor createCursor(
            Map map,
            RecordSink sink,
            @NotNull TimestampSampler timestampSampler,
            int timestampIndex,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            IntIntHashMap symbolTableIndex,
            @NotNull ObjList<ExpressionNode> fillValues
    ) throws SqlException {
        return new SampleByFillValueRecordCursor(
                map,
                sink,
                groupByFunctions,
                recordFunctions,
                createPlaceholderFunctions(recordFunctions, fillValues),
                timestampIndex,
                timestampSampler,
                symbolTableIndex
        );
    }

    @NotNull
    private static ObjList<Function> createPlaceholderFunctions(
            ObjList<Function> recordFunctions,
            @NotNull @Transient ObjList<ExpressionNode> fillValues
    ) throws SqlException {

        final ObjList<Function> placeholderFunctions = new ObjList<>();
        int fillIndex = 0;
        final int fillValueCount = fillValues.size();
        for (int i = 0, n = recordFunctions.size(); i < n; i++) {
            Function function = recordFunctions.getQuick(i);
            if (function instanceof GroupByFunction) {
                if (fillIndex == fillValueCount) {
                    throw SqlException.position(0).put("not enough values");
                }

                ExpressionNode fillNode = fillValues.getQuick(fillIndex++);

                try {
                    switch (function.getType()) {
                        case ColumnType.INT:
                            placeholderFunctions.add(new IntConstant(function.getPosition(), Numbers.parseInt(fillNode.token)));
                            break;
                        case ColumnType.LONG:
                            placeholderFunctions.add(new LongConstant(function.getPosition(), Numbers.parseLong(fillNode.token)));
                            break;
                        case ColumnType.FLOAT:
                            placeholderFunctions.add(new FloatConstant(function.getPosition(), Numbers.parseFloat(fillNode.token)));
                            break;
                        case ColumnType.DOUBLE:
                            placeholderFunctions.add(new DoubleConstant(function.getPosition(), Numbers.parseDouble(fillNode.token)));
                            break;
                        // todo: support and test all types
                        default:
                            assert false;
                    }
                } catch (NumericException e) {
                    throw SqlException.position(fillNode.position).put("invalid number: ").put(fillNode.token);
                }
            } else {
                placeholderFunctions.add(function);
            }
        }
        return placeholderFunctions;
    }
}
