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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GeoByteFunction;
import io.questdb.griffin.engine.functions.GeoIntFunction;
import io.questdb.griffin.engine.functions.GeoLongFunction;
import io.questdb.griffin.engine.functions.GeoShortFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndGeoHashFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "rnd_geohash(i)";
    }

    @Override
    public Function newInstance(int position,
                                ObjList<Function> args,
                                IntList argPositions,
                                CairoConfiguration configuration,
                                SqlExecutionContext sqlExecutionContext) throws SqlException {
        int bits = args.getQuick(0).getInt(null);
        if (bits < 1 || bits > ColumnType.GEOLONG_MAX_BITS) {
            throw SqlException.$(argPositions.getQuick(0), "precision must be in [1..60] range");
        }
        final int type = ColumnType.getGeoHashTypeWithBits(bits);
        switch (ColumnType.tagOf(type)) {
            case ColumnType.GEOBYTE:
                return new RndByteFunction(type);
            case ColumnType.GEOSHORT:
                return new RndShortFunction(type);
            case ColumnType.GEOINT:
                return new RndIntFunction(type);
            default:
                return new RndLongFunction(type);
        }
    }

    private static class RndByteFunction extends GeoByteFunction implements Function {

        private final int bits;
        private Rnd rnd;

        public RndByteFunction(int type) {
            super(type);
            this.bits = ColumnType.getGeoHashBits(type);
        }

        @Override
        public byte getGeoByte(Record rec) {
            return (byte) rnd.nextGeoHash(bits);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("rnd_geohash(").val(bits).val(')');
        }
    }

    private static class RndIntFunction extends GeoIntFunction implements Function {

        private final int bits;
        private Rnd rnd;

        public RndIntFunction(int type) {
            super(type);
            this.bits = ColumnType.getGeoHashBits(type);
        }

        @Override
        public int getGeoInt(Record rec) {
            return (int) rnd.nextGeoHash(bits);
        }

        @Override
        public short getGeoShort(Record rec) {
            return (byte) rnd.nextGeoHash(bits);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("rnd_geohash(").val(bits).val(')');
        }
    }

    private static class RndLongFunction extends GeoLongFunction implements Function {

        private final int bits;
        private Rnd rnd;

        public RndLongFunction(int type) {
            super(type);
            this.bits = ColumnType.getGeoHashBits(type);
        }

        @Override
        public long getGeoLong(Record rec) {
            return rnd.nextGeoHash(bits);
        }

        @Override
        public short getGeoShort(Record rec) {
            return (byte) rnd.nextGeoHash(bits);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("rnd_geohash(").val(bits).val(')');
        }
    }

    private static class RndShortFunction extends GeoShortFunction implements Function {

        private final int bits;
        private Rnd rnd;

        public RndShortFunction(int type) {
            super(type);
            this.bits = ColumnType.getGeoHashBits(type);
        }

        @Override
        public short getGeoShort(Record rec) {
            return (short) rnd.nextGeoHash(bits);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("rnd_geohash(").val(bits).val(')');
        }
    }
}
