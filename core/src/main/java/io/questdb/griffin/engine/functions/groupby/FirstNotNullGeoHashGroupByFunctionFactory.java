/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class FirstNotNullGeoHashGroupByFunctionFactory implements FunctionFactory {

    public static final String NAME = "first_not_null";

    @Override
    public String getSignature() {
        return "first_not_null(G)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        Function function = args.getQuick(0);
        int type = function.getType();

        switch (ColumnType.tagOf(type)) {
            case ColumnType.GEOBYTE:
                return new FirstNotNullGeoHashGroupByFunctionByte(type, function);
            case ColumnType.GEOSHORT:
                return new FirstNotNullGeoHashGroupByFunctionShort(type, function);
            case ColumnType.GEOINT:
                return new FirstNotNullGeoHashGroupByFunctionInt(type, function);
            default:
                return new FirstNotNullGeoHashGroupByFunctionLong(type, function);
        }
    }

    private static class FirstNotNullGeoHashGroupByFunctionByte extends FirstGeoHashGroupByFunctionByte {
        public FirstNotNullGeoHashGroupByFunctionByte(int type, Function function) {
            super(type, function);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record) {
            if (mapValue.getGeoByte(valueIndex) == GeoHashes.BYTE_NULL) {
                computeFirst(mapValue, record);
            }
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    private static class FirstNotNullGeoHashGroupByFunctionInt extends FirstGeoHashGroupByFunctionInt {
        public FirstNotNullGeoHashGroupByFunctionInt(int type, Function function) {
            super(type, function);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record) {
            if (mapValue.getGeoInt(valueIndex) == GeoHashes.INT_NULL) {
                computeFirst(mapValue, record);
            }
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    private static class FirstNotNullGeoHashGroupByFunctionLong extends FirstGeoHashGroupByFunctionLong {
        public FirstNotNullGeoHashGroupByFunctionLong(int type, Function function) {
            super(type, function);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record) {
            if (mapValue.getGeoLong(valueIndex) == GeoHashes.NULL) {
                computeFirst(mapValue, record);
            }
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    private static class FirstNotNullGeoHashGroupByFunctionShort extends FirstGeoHashGroupByFunctionShort {
        public FirstNotNullGeoHashGroupByFunctionShort(int type, Function function) {
            super(type, function);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record) {
            if (mapValue.getGeoShort(valueIndex) == GeoHashes.SHORT_NULL) {
                computeFirst(mapValue, record);
            }
        }

        @Override
        public String getName() {
            return NAME;
        }
    }
}