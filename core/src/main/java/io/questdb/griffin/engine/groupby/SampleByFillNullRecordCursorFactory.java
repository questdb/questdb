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

import org.jetbrains.annotations.NotNull;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRecordCursor;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.FloatConstant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.ShortConstant;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class SampleByFillNullRecordCursorFactory implements RecordCursorFactory {

    protected final RecordCursorFactory base;
    protected final Map map;
    private final DelegatingRecordCursor cursor;
    private final ObjList<Function> recordFunctions;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final RecordSink mapSink;
    private final RecordMetadata metadata;

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
            IntList symbolTableSkewIndex,
            int timestampIndex
    ) throws SqlException {

        this.recordFunctions = recordFunctions;
        this.groupByFunctions = groupByFunctions;

        // sink will be storing record columns to map key
        this.mapSink = RecordSinkFactory.getInstance(asm, base.getMetadata(), listColumnFilter, false);
        // this is the map itself, which we must not forget to free when factory closes
        this.map = MapFactory.createMap(configuration, keyTypes, valueTypes);
        try {
            this.base = base;
            this.metadata = groupByMetadata;
            this.cursor = new SampleByFillValueRecordCursor(
                    map,
                    mapSink,
                    groupByFunctions,
                    recordFunctions,
                    createPlaceholderFunctions(recordFunctions),
                    timestampIndex,
                    timestampSampler,
                    symbolTableSkewIndex
            );
        } catch (SqlException | CairoException e) {
            Misc.freeObjList(recordFunctions);
            Misc.free(map);
            throw e;
        }
    }

    @NotNull
    public static ObjList<Function> createPlaceholderFunctions(ObjList<Function> recordFunctions) throws SqlException {
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

    @Override
    public void close() {
        Misc.freeObjList(recordFunctions);
        Misc.free(map);
        Misc.free(base);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            map.clear();
            long maxInMemoryRows = executionContext.getCairoSecurityContext().getMaxInMemoryRows();
            if (maxInMemoryRows > baseCursor.size() || baseCursor.size() < 0) {
                map.setMaxSize(maxInMemoryRows);

                // This factory fills gaps in data. To do that we
                // have to know all possible key values. Essentially, every time
                // we sample we return same set of key values with different
                // aggregation results and timestamp

                int n = groupByFunctions.size();
                final Record baseCursorRecord = baseCursor.getRecord();
                while (baseCursor.hasNext()) {
                    MapKey key = map.withKey();
                    mapSink.copy(baseCursorRecord, key);
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        // timestamp is always stored in value field 0
                        value.putLong(0, Numbers.LONG_NaN);
                        // have functions reset their columns to "zero" state
                        // this would set values for when keys are not found right away
                        for (int i = 0; i < n; i++) {
                            groupByFunctions.getQuick(i).setNull(value);
                        }
                    }
                }

                // empty map? this means that base cursor was empty
                if (map.size() == 0) {
                    baseCursor.close();
                    return EmptyTableRecordCursor.INSTANCE;
                }

                // because we pass base cursor twice we have to go back to top
                // for the second run
                baseCursor.toTop();
                boolean next = baseCursor.hasNext();
                // we know base cursor has value
                assert next;
                return initFunctionsAndCursor(executionContext, baseCursor);
            } else {
                throw LimitOverflowException.instance(maxInMemoryRows);
            }
        } catch (CairoException ex) {
            baseCursor.close();
            throw ex;
        }
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @NotNull
    private RecordCursor initFunctionsAndCursor(SqlExecutionContext executionContext, RecordCursor baseCursor) {
        cursor.of(baseCursor);
        // init all record function for this cursor, in case functions require metadata and/or symbol tables
        for (int i = 0, m = recordFunctions.size(); i < m; i++) {
            recordFunctions.getQuick(i).init(cursor, executionContext);
        }
        return cursor;
    }
}
