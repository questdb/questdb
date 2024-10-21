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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class LastGeoHashGroupByFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "last(G)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        Function function = args.getQuick(0);
        int type = function.getType();

        // Reuse first implementation overriding computeNext() method inline
        switch (ColumnType.tagOf(type)) {
            case ColumnType.GEOBYTE:
                return new LastGeoHashGroupByFunctionByte(type, function);
            case ColumnType.GEOSHORT:
                return new LastGeoHashGroupByFunctionShort(type, function);
            case ColumnType.GEOINT:
                return new LastGeoHashGroupByFunctionInt(type, function);
            default:
                return new LastGeoHashGroupByFunctionLong(type, function);
        }
    }

    private static class LastGeoHashGroupByFunctionByte extends FirstGeoHashGroupByFunctionByte {
        LastGeoHashGroupByFunctionByte(int type, Function arg) {
            super(type, arg);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            computeFirst(mapValue, record, rowId);
        }

        @Override
        public String getName() {
            return "last";
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            long srcRowId = srcValue.getLong(valueIndex);
            long destRowId = destValue.getLong(valueIndex);
            if (srcRowId > destRowId) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putByte(valueIndex + 1, srcValue.getGeoByte(valueIndex + 1));
            }
        }

        @Override
        public Function newInstance(final Function arg) {
            return new LastGeoHashGroupByFunctionByte(type, arg);
        }
    }

    private static class LastGeoHashGroupByFunctionShort extends FirstGeoHashGroupByFunctionShort {
        public LastGeoHashGroupByFunctionShort(int type, Function arg) {
            super(type, arg);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            computeFirst(mapValue, record, rowId);
        }

        @Override
        public String getName() {
            return "last";
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            long srcRowId = srcValue.getLong(valueIndex);
            long destRowId = destValue.getLong(valueIndex);
            if (srcRowId > destRowId) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putShort(valueIndex + 1, srcValue.getGeoShort(valueIndex + 1));
            }
        }

        @Override
        public Function newInstance(final Function arg) {
            return new LastGeoHashGroupByFunctionShort(type, arg);
        }
    }

    private static class LastGeoHashGroupByFunctionInt extends FirstGeoHashGroupByFunctionInt {
        public LastGeoHashGroupByFunctionInt(int type, Function arg) {
            super(type, arg);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            computeFirst(mapValue, record, rowId);
        }

        @Override
        public String getName() {
            return "last";
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            long srcRowId = srcValue.getLong(valueIndex);
            long destRowId = destValue.getLong(valueIndex);
            if (srcRowId > destRowId) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putInt(valueIndex + 1, srcValue.getGeoInt(valueIndex + 1));
            }
        }

        @Override
        public Function newInstance(final Function arg) {
            return new LastGeoHashGroupByFunctionInt(type, arg);
        }
    }

    private static class LastGeoHashGroupByFunctionLong extends FirstGeoHashGroupByFunctionLong {
        public LastGeoHashGroupByFunctionLong(int type, Function arg) {
            super(type, arg);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            computeFirst(mapValue, record, rowId);
        }

        @Override
        public String getName() {
            return "last";
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            long srcRowId = srcValue.getLong(valueIndex);
            long destRowId = destValue.getLong(valueIndex);
            if (srcRowId > destRowId) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putLong(valueIndex + 1, srcValue.getGeoLong(valueIndex + 1));
            }
        }

        @Override
        public Function newInstance(final Function arg) {
            return new LastGeoHashGroupByFunctionLong(type, arg);
        }
    }
}
