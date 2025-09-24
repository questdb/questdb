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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.FloatConstant;
import io.questdb.griffin.engine.functions.constants.IPv4Constant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.ShortConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.groupby.InterpolationGroupByFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

import static io.questdb.griffin.SqlKeywords.*;

public class SampleByFillValueRecordCursorFactory extends AbstractSampleByFillRecordCursorFactory {
    private final SampleByFillValueRecordCursor cursor;

    public SampleByFillValueRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @NotNull TimestampSampler timestampSampler,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull ObjList<ExpressionNode> fillValues,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            RecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            @Transient IntList recordFunctionPositions,
            int timestampIndex,
            int timestampType,
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos,
            Function sampleFromFunc,
            int sampleFromFuncPos,
            Function sampleToFunc,
            int sampleToFuncPos
    ) throws SqlException {
        super(
                asm,
                configuration,
                base,
                listColumnFilter,
                keyTypes,
                valueTypes,
                groupByMetadata,
                groupByFunctions,
                recordFunctions
        );
        try {
            final ObjList<Function> placeholderFunctions = createPlaceholderFunctions(
                    ColumnType.getTimestampDriver(base.getMetadata().getTimestampType()),
                    groupByFunctions,
                    recordFunctions,
                    recordFunctionPositions,
                    fillValues,
                    false
            );
            final GroupByFunctionsUpdater updater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
            cursor = new SampleByFillValueRecordCursor(
                    configuration,
                    map,
                    mapSink,
                    groupByFunctions,
                    updater,
                    recordFunctions,
                    placeholderFunctions,
                    timestampIndex,
                    timestampType,
                    timestampSampler,
                    timezoneNameFunc,
                    timezoneNameFuncPos,
                    offsetFunc,
                    offsetFuncPos,
                    sampleFromFunc,
                    sampleFromFuncPos,
                    sampleToFunc,
                    sampleToFuncPos
            );
        } catch (Throwable e) {
            Misc.freeObjList(recordFunctions);
            Misc.free(map);
            throw e;
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Sample By");
        sink.attr("fill").val("value");
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        sink.optAttr("values", cursor.groupByFunctions, true);
        sink.child(base);
    }

    static Function createPlaceHolderFunction(
            TimestampDriver timestampDriver,
            IntList recordFunctionPositions,
            int index,
            int type,
            ExpressionNode fillNode
    ) throws SqlException {
        try {
            switch (ColumnType.tagOf(type)) {
                case ColumnType.INT:
                    return IntConstant.newInstance(Numbers.parseInt(fillNode.token));
                case ColumnType.IPv4:
                    return IPv4Constant.newInstance(Numbers.parseIPv4(fillNode.token));
                case ColumnType.LONG:
                    return LongConstant.newInstance(Numbers.parseLong(fillNode.token));
                case ColumnType.FLOAT:
                    return FloatConstant.newInstance(Numbers.parseFloat(fillNode.token));
                case ColumnType.DOUBLE:
                    return DoubleConstant.newInstance(Numbers.parseDouble(fillNode.token));
                case ColumnType.SHORT:
                    return ShortConstant.newInstance((short) Numbers.parseInt(fillNode.token));
                case ColumnType.BYTE:
                    return ByteConstant.newInstance((byte) Numbers.parseInt(fillNode.token));
                case ColumnType.TIMESTAMP:
                    if (!Chars.isQuoted(fillNode.token)) {
                        throw SqlException.position(fillNode.position).put("Invalid fill value: '").put(fillNode.token).put("'. Timestamp fill value must be in quotes. Example: '2019-01-01T00:00:00.000Z'");
                    }
                    return TimestampConstant.newInstance(timestampDriver.parseQuotedLiteral(fillNode.token), type);
                default:
                    throw SqlException.$(recordFunctionPositions.getQuick(index), "Unsupported type: ").put(ColumnType.nameOf(type));
            }
        } catch (NumericException e) {
            throw SqlException.position(fillNode.position).put("invalid fill value: ").put(fillNode.token);
        }
    }

    @NotNull
    static ObjList<Function> createPlaceholderFunctions(
            TimestampDriver timestampDriver,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            @Transient IntList recordFunctionPositions,
            @NotNull @Transient ObjList<ExpressionNode> fillValues,
            boolean linearSupported
    ) throws SqlException {
        final ObjList<Function> placeholderFunctions = new ObjList<>();
        int fillIndex = 0;
        final int fillValueCount = fillValues.size();
        for (int i = 0, n = recordFunctions.size(); i < n; i++) {
            Function function = recordFunctions.getQuick(i);
            if (function instanceof GroupByFunction) {
                if (fillIndex == fillValueCount) {
                    throw SqlException.position(fillValues.getQuick(fillIndex - 1).position)
                            .put("insufficient fill values for SAMPLE BY FILL: expected ")
                            .put(groupByFunctions.size())
                            .put(" values but only ")
                            .put(fillValueCount)
                            .put(" provided");
                }
                ExpressionNode fillNode = fillValues.getQuick(fillIndex++);
                if (isNullKeyword(fillNode.token)) {
                    placeholderFunctions.add(SampleByFillNullRecordCursorFactory.createPlaceHolderFunction(recordFunctionPositions, i, function.getType()));
                } else if (isPrevKeyword(fillNode.token)) {
                    placeholderFunctions.add(function);
                } else if (isLinearKeyword(fillNode.token)) {
                    if (!linearSupported) {
                        throw SqlException.position(0).put("linear interpolation is not supported when using fill values for keyed sample by expression");
                    }
                    GroupByFunction interpolation = InterpolationGroupByFunction.newInstance((GroupByFunction) function);
                    placeholderFunctions.add(interpolation);
                    groupByFunctions.set(fillIndex - 1, interpolation);
                    recordFunctions.set(i, interpolation);
                } else {
                    placeholderFunctions.add(createPlaceHolderFunction(timestampDriver, recordFunctionPositions, i, function.getType(), fillNode));
                }
            } else {
                placeholderFunctions.add(function);
            }
        }
        return placeholderFunctions;
    }

    @Override
    protected AbstractNoRecordSampleByCursor getRawCursor() {
        return cursor;
    }
}
