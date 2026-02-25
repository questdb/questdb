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

package io.questdb.griffin.engine.functions.array;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class DoubleArrayElemAvgFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "array_elem_avg(D[]V)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (args == null || args.size() < 2) {
            throw SqlException.$(position, "array_elem_avg requires at least 2 arguments");
        }
        for (int i = 0, n = args.size(); i < n; i++) {
            int t = args.getQuick(i).getType();
            if (!ColumnType.isArray(t) || ColumnType.decodeArrayElementType(t) != ColumnType.DOUBLE) {
                throw SqlException.$(argPositions.getQuick(i), "expected DOUBLE[] argument");
            }
        }
        return new Func(configuration, new ObjList<>(args));
    }

    private static class Func extends ArrayFunction implements MultiArgFunction {
        private final DirectArray arrayOut;
        private final ObjList<Function> args;
        private final IntList counts = new IntList();

        public Func(CairoConfiguration configuration, ObjList<Function> args) {
            this.args = args;
            this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
            this.arrayOut = new DirectArray(configuration);
            this.arrayOut.setType(type);
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public void close() {
            MultiArgFunction.super.close();
            Misc.free(arrayOut);
        }

        @Override
        public ArrayView getArray(Record rec) {
            int maxLen = 0;
            for (int i = 0, n = args.size(); i < n; i++) {
                ArrayView a = args.getQuick(i).getArray(rec);
                if (a != null && !a.isNull()) {
                    maxLen = Math.max(maxLen, a.getFlatViewLength());
                }
            }
            if (maxLen == 0) {
                return ArrayConstant.NULL;
            }
            arrayOut.setDimLen(0, maxLen);
            arrayOut.applyShape();
            for (int i = 0; i < maxLen; i++) {
                arrayOut.putDouble(i, Double.NaN);
            }
            counts.clear();
            counts.setPos(maxLen);
            counts.zero(0);
            for (int a = 0, n = args.size(); a < n; a++) {
                ArrayView arr = args.getQuick(a).getArray(rec);
                if (arr == null || arr.isNull()) {
                    continue;
                }
                int len = arr.getFlatViewLength();
                for (int i = 0; i < len; i++) {
                    double val = arr.getDouble(i);
                    if (Numbers.isFinite(val)) {
                        double cur = arrayOut.getDouble(i);
                        if (Numbers.isFinite(cur)) {
                            arrayOut.putDouble(i, cur + val);
                        } else {
                            arrayOut.putDouble(i, val);
                        }
                        counts.increment(i);
                    }
                }
            }
            for (int i = 0; i < maxLen; i++) {
                int c = counts.getQuick(i);
                if (c > 0) {
                    arrayOut.putDouble(i, arrayOut.getDouble(i) / c);
                }
            }
            return arrayOut;
        }

        @Override
        public String getName() {
            return "array_elem_avg";
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }
}
