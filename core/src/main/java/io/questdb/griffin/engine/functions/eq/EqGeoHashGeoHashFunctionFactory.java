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
import io.questdb.cairo.GeoHashExtra;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class EqGeoHashGeoHashFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(GG)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    // TODO: refactor this so that it is nicer code and add a truckload of tests

    @Override
    public Function newInstance(int position,
                                ObjList<Function> args,
                                IntList argPositions,
                                CairoConfiguration configuration,
                                SqlExecutionContext sqlExecutionContext) {
        Function geohash1 = args.getQuick(0);
        Function geohash2 = args.getQuick(1);
        if (geohash1.isConstant()) {
            long hash1 = geohash1.getLong(null);
            if (geohash2.isConstant()) {
                long hash2 = geohash2.getLong(null);
                if (geohash1.getType() == geohash2.getType() ||
                        hash1 == GeoHashExtra.NULL ||
                        hash2 == GeoHashExtra.NULL) {
                    return BooleanConstant.of(hash1 == hash2);
                } else {
                    return BooleanConstant.of(false);
                }
            } else {
                return createHalfConstantFunc(geohash1, geohash2);
            }
        } else if (geohash2.isConstant()) {
            return createHalfConstantFunc(geohash2, geohash1);
        }
        return new Func(geohash1, geohash2);
    }

    private Function createHalfConstantFunc(Function constFunc, Function varFunc) {
        if (ColumnType.tagOf(constFunc.getType()) == ColumnType.NULL ||
                constFunc.getLong(null) == GeoHashExtra.NULL) {
            return new NullCheckFunc(varFunc);
        }
        return new ConstCheckFunc(varFunc, constFunc.getLong(null), constFunc.getType());
    }

    private static class NullCheckFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;

        public NullCheckFunc(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (arg.getLong(rec) == GeoHashExtra.NULL);
        }
    }

    private static class ConstCheckFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;
        private final long hash;
        private final int typep;

        public ConstCheckFunc(Function arg, long hash, int typep) {
            this.arg = arg;
            this.hash = hash;
            this.typep = typep;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (typep == arg.getType() && hash == arg.getGeoHash(rec));
        }
    }

    private static class Func extends NegatableBooleanFunction implements BinaryFunction {
        private final Function left;
        private final Function right;

        public Func(Function left, Function right) {
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

        @Override
        public boolean getBool(Record rec) {
            final long a = left.getLong(rec);
            final int aType = left.getType();
            final long b = right.getLong(rec);
            final int bType = right.getType();

            if (aType == bType) {
                return negated != (a == b);
            }
            if (a == b && a == GeoHashExtra.NULL) {
                return negated;
            }
            return false;
        }
    }
}
