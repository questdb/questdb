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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.FloatConstant;
import io.questdb.griffin.engine.functions.constants.GeoByteConstant;
import io.questdb.griffin.engine.functions.constants.GeoIntConstant;
import io.questdb.griffin.engine.functions.constants.GeoLongConstant;
import io.questdb.griffin.engine.functions.constants.GeoShortConstant;
import io.questdb.griffin.engine.functions.constants.IPv4Constant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.ShortConstant;
import io.questdb.griffin.engine.functions.constants.UuidConstant;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class SampleByFillNullRecordCursorFactory extends AbstractSampleByFillRecordCursorFactory {
    private final SampleByFillValueRecordCursor cursor;

    public SampleByFillNullRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @NotNull TimestampSampler timestampSampler,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            RecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            @Transient @NotNull IntList recordFunctionPositions,
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
            final GroupByFunctionsUpdater updater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
            cursor = new SampleByFillValueRecordCursor(
                    configuration,
                    map,
                    mapSink,
                    groupByFunctions,
                    updater,
                    recordFunctions,
                    createPlaceholderFunctions(recordFunctions, recordFunctionPositions),
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
    public AbstractNoRecordSampleByCursor getRawCursor() {
        return cursor;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Sample By");
        sink.attr("fill").val("null");
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        sink.optAttr("values", cursor.groupByFunctions, true);
        sink.child(base);
    }

    static Function createPlaceHolderFunction(IntList recordFunctionPositions, int index, int type) throws SqlException {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.INT:
                return IntConstant.NULL;
            case ColumnType.IPv4:
                return IPv4Constant.NULL;
            case ColumnType.LONG:
                return LongConstant.NULL;
            case ColumnType.FLOAT:
                return FloatConstant.NULL;
            case ColumnType.DOUBLE:
                return DoubleConstant.NULL;
            case ColumnType.BYTE:
                return ByteConstant.ZERO;
            case ColumnType.SHORT:
                return ShortConstant.ZERO;
            case ColumnType.GEOBYTE:
                return GeoByteConstant.NULL;
            case ColumnType.GEOSHORT:
                return GeoShortConstant.NULL;
            case ColumnType.GEOINT:
                return GeoIntConstant.NULL;
            case ColumnType.GEOLONG:
                return GeoLongConstant.NULL;
            case ColumnType.UUID:
                return UuidConstant.NULL;
            case ColumnType.TIMESTAMP:
                return ColumnType.getTimestampDriver(type).getTimestampConstantNull();
            default:
                if (ColumnType.isDecimal(type)) {
                    return DecimalUtil.createNullDecimalConstant(
                            ColumnType.getDecimalPrecision(type),
                            ColumnType.getDecimalScale(type)
                    );
                }
                throw SqlException.$(recordFunctionPositions.getQuick(index), "Unsupported type: ").put(ColumnType.nameOf(type));
        }
    }

    @NotNull
    static ObjList<Function> createPlaceholderFunctions(
            ObjList<Function> recordFunctions,
            IntList recordFunctionPositions
    ) throws SqlException {
        final ObjList<Function> placeholderFunctions = new ObjList<>();
        for (int i = 0, n = recordFunctions.size(); i < n; i++) {
            Function function = recordFunctions.getQuick(i);
            if (function instanceof GroupByFunction) {
                placeholderFunctions.add(createPlaceHolderFunction(recordFunctionPositions, i, function.getType()));
            } else {
                placeholderFunctions.add(function);
            }
        }
        return placeholderFunctions;
    }
}
