/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.*;
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
        try {
            final ObjList<Function> placeholderFunctions = createPlaceholderFunctions(recordFunctions, fillValues);
            return new SampleByFillValueRecordCursor(
                    map,
                    sink,
                    groupByFunctions,
                    recordFunctions,
                    placeholderFunctions,
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
                        case ColumnType.SHORT:
                            placeholderFunctions.add(new ShortConstant(function.getPosition(), (short) Numbers.parseInt(fillNode.token)));
                            break;
                        case ColumnType.BYTE:
                            placeholderFunctions.add(new ByteConstant(function.getPosition(), (byte) Numbers.parseInt(fillNode.token)));
                            break;
                        default:
                            throw SqlException.$(function.getPosition(), "Unsupported type: ").put(ColumnType.nameOf(function.getType()));
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
