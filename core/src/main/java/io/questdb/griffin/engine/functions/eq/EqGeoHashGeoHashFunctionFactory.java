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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
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
    public Function newInstance(int position,
                                ObjList<Function> args,
                                IntList argPositions,
                                CairoConfiguration configuration,
                                SqlExecutionContext sqlExecutionContext) {
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

        // Make sure we don't compare apples with pears once null type is ruled out
        if (type1p != type2p) {
            return BooleanConstant.of(false);
        }

        if (geohash1.isConstant()) {
            long hash1 = getGeoLong(geohash1.getType(), geohash1, null);
            if (geohash2.isConstant()) {
                long hash2 = getGeoLong(type1p, geohash2, null);
                return BooleanConstant.of(hash1 == hash2);
            }

            return createConstCheckFunc(geohash2, hash1, type1p);
        }
        return geohash2.isConstant() ?
                createConstCheckFunc(geohash1, getGeoLong(type1p, geohash2, null), type1p) :
                crateBinaryFunc(geohash1, geohash2, type1p);
    }

    private Function crateBinaryFunc(Function geohash1, Function geohash2, int valType) {
        switch (ColumnType.sizeOf(valType)) {
            default:
                return new GeoEqFunc(geohash1, geohash2) {
                    @Override
                    public boolean getBool(Record rec) {
                        return negated != (geohash1.getGeoHashLong(rec) == geohash2.getGeoHashLong(rec));
                    }
                };
            case Integer.BYTES:
                return new GeoEqFunc(geohash1, geohash2) {
                    @Override
                    public boolean getBool(Record rec) {
                        return negated != (geohash1.getGeoHashInt(rec) == geohash2.getGeoHashInt(rec));
                    }
                };
            case Short.BYTES:
                return new GeoEqFunc(geohash1, geohash2) {
                    @Override
                    public boolean getBool(Record rec) {
                        return negated != (geohash1.getGeoHashShort(rec) == geohash2.getGeoHashShort(rec));
                    }
                };
            case Byte.BYTES:
                return new GeoEqFunc(geohash1, geohash2) {
                    @Override
                    public boolean getBool(Record rec) {
                        return negated != (geohash1.getGeoHashByte(rec) == geohash2.getGeoHashByte(rec));
                    }
                };
        }
    }

    private Function createConstCheckFunc(Function function, long value, int valType) {
        switch (ColumnType.sizeOf(valType)) {
            default:
                return new ConstCheckFuncLong(function, value);
            case Integer.BYTES:
                return new ConstCheckFuncInt(function, (int) value);
            case Short.BYTES:
                return new ConstCheckFuncShort(function, (short) value);
            case Byte.BYTES:
                return new ConstCheckFuncByte(function, (byte) value);
        }
    }

    private static class ConstCheckFuncByte extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;
        private final byte hash;

        public ConstCheckFuncByte(Function arg, byte hash) {
            this.arg = arg;
            this.hash = hash;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (hash == arg.getGeoHashByte(rec));
        }
    }

    private static class ConstCheckFuncShort extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;
        private final short hash;

        public ConstCheckFuncShort(Function arg, short hash) {
            this.arg = arg;
            this.hash = hash;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (hash == arg.getGeoHashShort(rec));
        }
    }

    private static class ConstCheckFuncInt extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;
        private final int hash;

        public ConstCheckFuncInt(Function arg, int hash) {
            this.arg = arg;
            this.hash = hash;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (hash == arg.getGeoHashInt(rec));
        }
    }

    private static class ConstCheckFuncLong extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;
        private final long hash;

        public ConstCheckFuncLong(Function arg, long hash) {
            this.arg = arg;
            this.hash = hash;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (hash == arg.getGeoHashLong(rec));
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
        public Function getRight() {
            return right;
        }
    }
}
