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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
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
    public static long getGeoHashAsLong(Record rec, Function geoHashFunc, int geoHashType) {
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

        for (int i = 1; i <= argCount; i++) {
            Function func = args.getQuick(i);
            int funcType = ColumnType.tagOf(func.getType());
            if (!(funcType >= ColumnType.GEOBYTE && funcType <= ColumnType.GEOLONG) && func.getType() != ColumnType.UNDEFINED) {
                throw SqlException.position(argPositions.getQuick(i))
                        .put("cannot compare GEOHASH with type ")
                        .put(ColumnType.nameOf(funcType));
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
            return new WithinGeohashConstConstFunction(new ObjList<>(args));
        }

        if (constCount == argCount && runtimeConstCount == 0) {
            try {
                return new WithinGeohashVarConstFunction(new ObjList<>(args));
            } catch (NumericException ex) {
                // fall back to default impl
                return new WithinGeohashVarVarFunction(new ObjList<>(args));
            }
        }

        if (runtimeConstCount == argCount || runtimeConstCount + constCount == argCount) {
            final IntList positions = new IntList();
            positions.addAll(argPositions);
            return new WithinGeohashRuntimeConstFunction(new ObjList<>(args), positions);
        }


        // have to copy, args is mutable
        return new WithinGeohashVarVarFunction(new ObjList<>(args));
    }

    private static abstract class AbstractWithinGeohashFunction extends NegatableBooleanFunction implements MultiArgFunction {
        protected final int argCount;
        protected final ObjList<Function> args;
        protected final Function geoHashFunc;
        protected final int geoHashType;
        protected long geoHashValue;

        public AbstractWithinGeohashFunction(ObjList<Function> args) {
            this.args = args;
            this.argCount = args.size() - 1;
            this.geoHashFunc = args.getQuick(0);
            this.geoHashType = geoHashFunc.getType();
        }

        @Override
        public ObjList<Function> args() {
            return new ObjList<>(args);
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
            geoHashValue = getGeoHashAsLong(null, geoHashFunc, geoHashType);
            for (int i = 1; i <= argCount; i++) {
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
        final long geoHashValue;

        public WithinGeohashConstVarFunction(ObjList<Function> args) {
            super(args);
            geoHashValue = getGeoHashAsLong(null, geoHashFunc, geoHashType);
        }

        // all hashes must be higher or same precision as comparator
        @Override
        public boolean getBool(Record rec) {
            for (int i = 1; i <= argCount; i++) {
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

    private static class WithinGeohashRuntimeConstFunction extends AbstractWithinGeohashFunction {
        IntList argPositions;
        long geoHashValue;
        ObjList<Function> prefixFuncs;
        LongList prefixList;
        int prefixListSize;

        public WithinGeohashRuntimeConstFunction(ObjList<Function> args, IntList argPositions) {
            super(args);
            this.prefixFuncs = args;
            this.argPositions = argPositions;
        }

        // all hashes must be higher or same precision as comparator
        @Override
        public boolean getBool(Record rec) {
            geoHashValue = getGeoHashAsLong(rec, geoHashFunc, geoHashType);
            for (int i = 0; i < prefixListSize; i += 2) {
                final long prefixValue = prefixList.getQuick(i);
                final long prefixMask = prefixList.getQuick(i + 1);
                if ((geoHashValue & prefixMask) == prefixValue) {
                    return !negated;
                }
            }
            return negated;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            prefixList = new LongList((argCount) * 2);
            for (int i = 1; i <= argCount; i++) {
                final Function prefixFunc = args.getQuick(i);
                final int prefixFuncType = prefixFunc.getType();
                final long prefixValue = getGeoHashAsLong(null, prefixFunc, prefixFuncType);
                try {
                    GeoHashes.addNormalizedGeoPrefix(prefixValue, prefixFuncType, geoHashType, prefixList);
                } catch (NumericException ex) {
                    throw CairoException.nonCritical()
                            .position(argPositions.getQuick(0))
                            .put("could not process geohash: ")
                            .put(GeoHashes.toString0(prefixValue, prefixFuncType));
                }
            }
            prefixListSize = prefixList.size();
        }
    }

    private static class WithinGeohashVarConstFunction extends AbstractWithinGeohashFunction {
        final LongList prefixList;
        final int preflixListSize;

        public WithinGeohashVarConstFunction(ObjList<Function> args) throws NumericException {
            super(args);
            prefixList = new LongList((args.size() - 1) * 2);
            for (int i = 1; i <= argCount; i++) {
                final Function prefixFunc = args.getQuick(i);
                final int prefixFuncType = prefixFunc.getType();
                final long prefixValue = getGeoHashAsLong(null, prefixFunc, prefixFuncType);
                GeoHashes.addNormalizedGeoPrefix(prefixValue, prefixFuncType, args.getQuick(0).getType(), prefixList);
            }
            preflixListSize = prefixList.size();
        }

        // all hashes must be higher or same precision as comparator
        @Override
        public boolean getBool(Record rec) {
            geoHashValue = getGeoHashAsLong(rec, geoHashFunc, geoHashType);
            for (int i = 0; i < preflixListSize; i += 2) {
                final long prefixValue = prefixList.getQuick(i);
                final long prefixMask = prefixList.getQuick(i + 1);
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
            geoHashValue = getGeoHashAsLong(rec, geoHashFunc, geoHashType);
            for (int i = 1; i <= argCount; i++) {
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

