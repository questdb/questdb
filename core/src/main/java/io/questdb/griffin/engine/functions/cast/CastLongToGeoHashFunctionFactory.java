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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.*;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class CastLongToGeoHashFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        // GeoHashes are of different lengths
        // and can be cast to lower precision
        // for example cast(cast('questdb' as geohash(6c)) as geohash(5c))
        return "cast(Lg)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final int targetType = args.getQuick(1).getType();
        switch (ColumnType.tagOf(targetType)) {
            case ColumnType.GEOBYTE:
                return new CastGeoByteFunc(targetType, args.getQuick(0));
            case ColumnType.GEOSHORT:
                return new CastGeoShortFunc(targetType, args.getQuick(0));
            case ColumnType.GEOINT:
                return new CastGeoIntFunc(targetType, args.getQuick(0));
            default:
                return new CastGeoLongFunc(targetType, args.getQuick(0));
        }
    }

    private static class CastGeoByteFunc extends GeoByteFunction implements UnaryFunction {
        private final Function value;

        public CastGeoByteFunc(int targetType, Function value) {
            super(targetType);
            this.value = value;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public byte getGeoByte(Record rec) {
            final long value = this.value.getLong(rec);
            return value != Numbers.LONG_NULL ? (byte) value : GeoHashes.BYTE_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value).val("::geobyte");
        }
    }

    private static class CastGeoIntFunc extends GeoIntFunction implements UnaryFunction {
        private final Function value;

        public CastGeoIntFunc(int targetType, Function value) {
            super(targetType);
            this.value = value;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public int getGeoInt(Record rec) {
            final long value = this.value.getLong(rec);
            return value != Numbers.LONG_NULL ? (int) value : GeoHashes.INT_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value).val("::geoint");
        }
    }

    private static class CastGeoLongFunc extends GeoLongFunction implements UnaryFunction {
        private final Function value;

        public CastGeoLongFunc(int targetType, Function value) {
            super(targetType);
            this.value = value;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public long getGeoLong(Record rec) {
            final long value = this.value.getLong(rec);
            return value != Numbers.LONG_NULL ? value : GeoHashes.NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value).val("::geoshort");
        }
    }

    private static class CastGeoShortFunc extends GeoShortFunction implements UnaryFunction {
        private final Function value;

        public CastGeoShortFunc(int targetType, Function value) {
            super(targetType);
            this.value = value;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public short getGeoShort(Record rec) {
            final long value = this.value.getLong(rec);
            return value != Numbers.LONG_NULL ? (short) value : GeoHashes.SHORT_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value).val("::geolong");
        }
    }
}
