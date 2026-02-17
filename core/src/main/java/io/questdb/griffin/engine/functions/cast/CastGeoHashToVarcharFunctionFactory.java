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
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bool.WithinGeohashFunctionFactory;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;

import java.util.Objects;

public class CastGeoHashToVarcharFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Gø)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        Function geoHashFunc = args.getQuick(0);
        if (geoHashFunc.isConstant()) {
            int geoHashFuncType = geoHashFunc.getType();
            final StringSink sink = Misc.getThreadLocalSink();
            long hash = WithinGeohashFunctionFactory.getGeoHashAsLong(null, geoHashFunc, geoHashFuncType);
            GeoHashes.appendNoQuotes(hash, GeoHashes.getBitFlags(geoHashFuncType), sink);
            return new VarcharConstant(Chars.toString(sink));
        }
        return new Func(args.getQuick(0));
    }

    public static class Func extends AbstractCastToVarcharFunction {
        private final int argType;
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();

        public Func(Function arg) {
            super(arg);
            argType = arg.getType();
        }

        public void getVarchar(Record rec, Utf8StringSink sink) {
            long hash = WithinGeohashFunctionFactory.getGeoHashAsLong(rec, arg, argType);
            sink.clear();
            if (hash != GeoHashes.NULL) {
                GeoHashes.appendNoQuotes(hash, GeoHashes.getBitFlags(argType), sink);
            }
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            getVarchar(rec, sinkA);
            return sinkA;
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            getVarchar(rec, sinkB);
            return sinkB;
        }

        @Override
        public int getVarcharSize(Record rec) {
            return Objects.requireNonNull(getVarcharA(rec)).size();
        }
    }
}
