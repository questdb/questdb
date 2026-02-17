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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import static io.questdb.cairo.GeoHashes.getGeoLong;

public class EqGeoHashGeoHashFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(GG)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function geohash1 = args.getQuick(0);
        Function geohash2 = args.getQuick(1);
        int type1p = geohash1.getType();
        int type2p = geohash2.getType();

        if (ColumnType.isNull(type1p) && ColumnType.isNull(type2p)) {
            return BooleanConstant.of(true);
        }
        if (ColumnType.isNull(type1p)) {
            type1p = type2p;
            geohash1 = Constants.getNullConstant(type2p);
        }
        if (ColumnType.isNull(type2p)) {
            type2p = type1p;
            geohash2 = Constants.getNullConstant(type1p);
        }

        if (geohash1.isConstant()) {
            final long hash1 = getGeoLong(type1p, geohash1, null);
            if (geohash2.isConstant()) {
                // both constants, we do not need to do null check
                // null values across types are equal as it is
                long hash2 = getGeoLong(type2p, geohash2, null);
                return BooleanConstant.of((type1p == type2p && hash1 == hash2) || (hash1 == GeoHashes.NULL && hash2 == GeoHashes.NULL));
            }

            if (hash1 == GeoHashes.NULL || type1p == type2p) {
                // continue only if types match or we have constant NULL
                return createConstCheckFunc(geohash2, hash1, type1p);
            }

            return BooleanConstant.of(false);
        }

        if (geohash2.isConstant()) {
            final long hash2 = getGeoLong(type2p, geohash2, null);
            if (hash2 == GeoHashes.NULL || type1p == type2p) {
                return createConstCheckFunc(geohash1, hash2, type1p);
            }
            return BooleanConstant.of(false);
        }

        if (type1p == type2p) {
            return createBinaryFunc(geohash1, geohash2, type1p);
        }

        return BooleanConstant.of(false);
    }

    private static Function createBinaryFunc(Function geohash1, Function geohash2, int valType) {
        switch (ColumnType.tagOf(valType)) {
            case ColumnType.GEOBYTE:
                return new GeoEqFunc(geohash1, geohash2) {
                    @Override
                    public boolean getBool(Record rec) {
                        return negated != (geohash1.getGeoByte(rec) == geohash2.getGeoByte(rec));
                    }
                };
            case ColumnType.GEOSHORT:
                return new GeoEqFunc(geohash1, geohash2) {
                    @Override
                    public boolean getBool(Record rec) {
                        return negated != (geohash1.getGeoShort(rec) == geohash2.getGeoShort(rec));
                    }
                };
            case ColumnType.GEOINT:
                return new GeoEqFunc(geohash1, geohash2) {
                    @Override
                    public boolean getBool(Record rec) {
                        return negated != (geohash1.getGeoInt(rec) == geohash2.getGeoInt(rec));
                    }
                };
            default:
                return new GeoEqFunc(geohash1, geohash2) {
                    @Override
                    public boolean getBool(Record rec) {
                        return negated != (geohash1.getGeoLong(rec) == geohash2.getGeoLong(rec));
                    }
                };
        }
    }

    private static Function createConstCheckFunc(Function function, long value, int valType) {
        switch (ColumnType.tagOf(valType)) {
            case ColumnType.GEOBYTE:
                return new ConstCheckFuncByte(function, (byte) value);
            case ColumnType.GEOSHORT:
                return new ConstCheckFuncShort(function, (short) value);
            case ColumnType.GEOINT:
                return new ConstCheckFuncInt(function, (int) value);
            default:
                return new ConstCheckFuncLong(function, value);
        }
    }

    private static abstract class ConstCheckFunc extends NegatableBooleanFunction implements UnaryFunction {
        protected final Function arg;

        protected ConstCheckFunc(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
            if (negated) {
                sink.val('!');
            }
            sink.val('=').val(getHash()).val("::geohash");
        }

        protected abstract long getHash();
    }

    private static class ConstCheckFuncByte extends ConstCheckFunc {
        private final byte hash;

        public ConstCheckFuncByte(Function arg, byte hash) {
            super(arg);
            this.hash = hash;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (hash == arg.getGeoByte(rec));
        }

        @Override
        protected long getHash() {
            return hash;
        }
    }

    private static class ConstCheckFuncInt extends ConstCheckFunc {
        private final int hash;

        public ConstCheckFuncInt(Function arg, int hash) {
            super(arg);
            this.hash = hash;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (hash == arg.getGeoInt(rec));
        }

        @Override
        protected long getHash() {
            return hash;
        }
    }

    private static class ConstCheckFuncLong extends ConstCheckFunc {
        private final long hash;

        public ConstCheckFuncLong(Function arg, long hash) {
            super(arg);
            this.hash = hash;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (hash == arg.getGeoLong(rec));
        }

        @Override
        protected long getHash() {
            return hash;
        }
    }

    private static class ConstCheckFuncShort extends ConstCheckFunc {
        private final short hash;

        public ConstCheckFuncShort(Function arg, short hash) {
            super(arg);
            this.hash = hash;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (hash == arg.getGeoShort(rec));
        }

        @Override
        protected long getHash() {
            return hash;
        }
    }

    private static abstract class GeoEqFunc extends NegatableBooleanFunction implements BinaryFunction {
        protected final Function left;
        protected final Function right;

        public GeoEqFunc(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public String getName() {
            if (negated) {
                return "!=";
            }
            return "=";
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public boolean isOperator() {
            return true;
        }
    }
}
