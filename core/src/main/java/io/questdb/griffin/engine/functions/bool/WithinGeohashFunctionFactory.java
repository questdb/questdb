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

package io.questdb.griffin.engine.functions.bool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;


public class WithinGeohashFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "within(GV)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        int constCount = 0;
        int runtimeConstCount = 0;
        final Function firstArg = args.getQuick(0);
        final int argCount = args.size() - 1;
        for (int i = 1, n = args.size(); i < n; i++) {
            Function func = args.getQuick(i);
            if (!ColumnType.isGeoHash(func.getType())) {
                throw SqlException.position(argPositions.getQuick(i)).put("cannot compare GEOHASH with type ").put(ColumnType.nameOf(func.getType()));
            }

            if (func.isConstant()) {
                constCount++;
            }

            if (func.isRuntimeConstant()) {
                runtimeConstCount++;
            }
        }

        // have to copy, args is mutable
        if (firstArg.isConstant() && constCount == 0 && runtimeConstCount == 0) {
            return new WithinGeohashConstVarFunction(new ObjList<>(args));
        }

        // don't have to copy, references are not kept to args
        if (firstArg.isConstant() && constCount == argCount && runtimeConstCount == 0) {
            return new WithinGeohashConstConstFunction(args);
        }

        if (constCount == argCount && runtimeConstCount == 0) {
            try {
                return new WithinGeohashVarConstFunction(new ObjList<>(args));
            } catch (NumericException ex) {
                // fall back to default impl
                return new WithinGeohashVarVarFunction(new ObjList<>(args));
            }
        }

        // todo(nwoolmer): WithinGeohashRuntimeConstFunction

        // have to copy, args is mutable
        return new WithinGeohashVarVarFunction(new ObjList<>(args));
    }

    private static long getGeoHashAsLong(Record rec, Function geoHashFunc, int geoHashType) {
        assert ColumnType.isGeoHash(geoHashType);

        switch (ColumnType.tagOf(geoHashType)) {
            case ColumnType.GEOBYTE:
                return geoHashFunc.getGeoByte(rec);
            case ColumnType.GEOSHORT:
                return geoHashFunc.getGeoShort(rec);
            case ColumnType.GEOINT:
                return geoHashFunc.getGeoInt(rec);
            case ColumnType.GEOLONG:
                return geoHashFunc.getGeoLong(rec);
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static abstract class AbstractWithinGeohashFunction extends NegatableBooleanFunction implements MultiArgFunction {
        protected final ObjList<Function> args;

        public AbstractWithinGeohashFunction(ObjList<Function> args) {
            this.args = args;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(args.getQuick(0));
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in ");
            sink.val(args, 1);
        }
    }

    // Const LHS
    private static class WithinGeohashConstConstFunction extends AbstractWithinGeohashFunction {
        boolean result;

        public WithinGeohashConstConstFunction(ObjList<Function> args) {
            super(args);
            final Function geoHashFunc = args.getQuick(0);
            final int geoHashType = geoHashFunc.getType();
            final long geoHashValue = getGeoHashAsLong(null, geoHashFunc, geoHashType);
            for (int i = 1, n = args.size(); i < n; i++) {
                final Function prefixFunc = args.getQuick(i);
                final int prefixFuncType = prefixFunc.getType();
                final long prefixValue = getGeoHashAsLong(null, prefixFunc, prefixFuncType);
                final long convertedGeoHashValue = SqlUtil.implicitCastGeoHashAsGeoHash(geoHashValue, geoHashType, prefixFuncType);
                if (prefixValue == convertedGeoHashValue) {
                    result = !negated;
                    break;
                }
            }
        }

        // all hashes must be higher or same precision as comparator
        @Override
        public boolean getBool(Record rec) {
            return result;
        }
    }

    // Const LHS
    private static class WithinGeohashConstVarFunction extends AbstractWithinGeohashFunction {
        final Function geoHashFunc;
        final int geoHashType;
        final long geoHashValue;

        public WithinGeohashConstVarFunction(ObjList<Function> args) {
            super(args);
            geoHashFunc = args.getQuick(0);
            geoHashType = geoHashFunc.getType();
            geoHashValue = getGeoHashAsLong(null, geoHashFunc, geoHashType);
        }

        // all hashes must be higher or same precision as comparator
        @Override
        public boolean getBool(Record rec) {
            for (int i = 1, n = args.size(); i < n; i++) {
                final Function prefixFunc = args.getQuick(i);
                final int prefixFuncType = prefixFunc.getType();
                final long prefixValue = getGeoHashAsLong(rec, prefixFunc, prefixFuncType);
                final long convertedGeoHashValue = SqlUtil.implicitCastGeoHashAsGeoHash(geoHashValue, geoHashType, prefixFuncType);
                if (prefixValue == convertedGeoHashValue) {
                    return !negated;
                }
            }

            return negated;
        }
    }

    private static class WithinGeohashVarConstFunction extends AbstractWithinGeohashFunction {
        final LongList hashesAndMasks;

        public WithinGeohashVarConstFunction(ObjList<Function> args) throws NumericException {
            super(args);
            hashesAndMasks = new LongList(args.size() - 1);
            for (int i = 1, n = args.size(); i < n; i++) {
                final Function prefixFunc = args.getQuick(i);
                final int prefixFuncType = prefixFunc.getType();
                final long prefixValue = getGeoHashAsLong(null, prefixFunc, prefixFuncType);
                GeoHashes.addNormalizedGeoPrefix(prefixValue, prefixFuncType, args.getQuick(0).getType(), hashesAndMasks);

            }
        }

        // all hashes must be higher or same precision as comparator
        @Override
        public boolean getBool(Record rec) {
            final Function geoHashFunc = args.getQuick(0);
            final int geoHashType = geoHashFunc.getType();
            final long geoHashValue = getGeoHashAsLong(rec, geoHashFunc, geoHashType);

            for (int i = 0, n = hashesAndMasks.size(); i < n; i += 2) {
                final long prefixValue = hashesAndMasks.getQuick(i);
                final long prefixMask = hashesAndMasks.getQuick(i + 1);
                if ((geoHashValue & prefixMask) == prefixValue) {
                    return !negated;
                }
            }
            return negated;
        }
    }

    private static class WithinGeohashVarVarFunction extends AbstractWithinGeohashFunction {

        public WithinGeohashVarVarFunction(ObjList<Function> args) {
            super(args);
        }

        // all hashes must be higher or same precision as comparator
        @Override
        public boolean getBool(Record rec) {
            final Function geoHashFunc = args.getQuick(0);
            final int geoHashType = geoHashFunc.getType();
            final long geoHashValue = getGeoHashAsLong(rec, geoHashFunc, geoHashType);

            for (int i = 1, n = args.size(); i < n; i++) {
                final Function prefixFunc = args.getQuick(i);
                final int prefixFuncType = prefixFunc.getType();
                final long prefixValue = getGeoHashAsLong(rec, prefixFunc, prefixFuncType);
                final long convertedGeoHashValue = SqlUtil.implicitCastGeoHashAsGeoHash(geoHashValue, geoHashType, prefixFuncType);
                if (prefixValue == convertedGeoHashValue) {
                    return !negated;
                }
            }
            return negated;
        }
    }
}

