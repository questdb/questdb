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

            return new ConstCheckFunc(geohash2, hash1, type1p);
        }
        return geohash2.isConstant() ?
                new ConstCheckFunc(geohash1, getGeoLong(type1p, geohash2, null), type1p) :
                new Func(geohash1, geohash2, type1p);
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
            return negated != (typep == arg.getType() && hash == getGeoLong(typep, arg, rec));
        }
    }

    private static class Func extends NegatableBooleanFunction implements BinaryFunction {
        private final Function left;
        private final Function right;
        private final int typep;

        public Func(Function left, Function right, int typep) {
            this.left = left;
            this.right = right;
            this.typep = typep;
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
            final long hash1 = getGeoLong(typep, left, rec);
            final long hash2 = getGeoLong(typep, right, rec);
            return hash1 == hash2;
        }
    }
}
