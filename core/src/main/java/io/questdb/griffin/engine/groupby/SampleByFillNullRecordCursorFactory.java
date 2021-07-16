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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.constants.*;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;

public class SampleByFillNullRecordCursorFactory extends AbstractSampleByFillRecordCursorFactory {
    private final AbstractNoRecordSampleByCursor cursor;

    public SampleByFillNullRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @NotNull TimestampSampler timestampSampler,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull BytecodeAssembler asm,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            RecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            @Transient @NotNull IntList recordFunctionPositions,
            int timestampIndex,
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos
    ) throws SqlException {

        super(
                configuration,
                base,
                listColumnFilter,
                asm,
                keyTypes,
                valueTypes,
                groupByMetadata,
                groupByFunctions,
                recordFunctions
        );
        try {
            this.cursor = new SampleByFillValueRecordCursor(
                    map,
                    mapSink,
                    groupByFunctions,
                    recordFunctions,
                    createPlaceholderFunctions(recordFunctions, recordFunctionPositions),
                    timestampIndex,
                    timestampSampler,
                    timezoneNameFunc,
                    timezoneNameFuncPos,
                    offsetFunc,
                    offsetFuncPos
            );
        } catch (Throwable e) {
            Misc.freeObjList(recordFunctions);
            Misc.free(map);
            throw e;
        }
    }

    @NotNull
    public static ObjList<Function> createPlaceholderFunctions(
            ObjList<Function> recordFunctions,
            IntList recordFunctionPositions
    ) throws SqlException {
        final ObjList<Function> placeholderFunctions = new ObjList<>();
        for (int i = 0, n = recordFunctions.size(); i < n; i++) {
            Function function = recordFunctions.getQuick(i);
            if (function instanceof GroupByFunction) {
                switch (function.getType()) {
                    case ColumnType.INT:
                        placeholderFunctions.add(IntConstant.NULL);
                        break;
                    case ColumnType.LONG:
                        placeholderFunctions.add(LongConstant.NULL);
                        break;
                    case ColumnType.FLOAT:
                        placeholderFunctions.add(FloatConstant.NULL);
                        break;
                    case ColumnType.DOUBLE:
                        placeholderFunctions.add(DoubleConstant.NULL);
                        break;
                    case ColumnType.BYTE:
                        placeholderFunctions.add(ByteConstant.ZERO);
                        break;
                    case ColumnType.SHORT:
                        placeholderFunctions.add(ShortConstant.ZERO);
                        break;
                    default:
                        throw SqlException.$(recordFunctionPositions.getQuick(i), "Unsupported type: ").put(ColumnType.nameOf(function.getType()));
                }
            } else {
                placeholderFunctions.add(function);
            }
        }
        return placeholderFunctions;
    }

    @Override
    public AbstractNoRecordSampleByCursor getRawCursor() {
        return cursor;
    }
}
