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
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;

public class CastArrayToVarcharFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(Aø)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext) {
        Function arrayFunc = args.getQuick(0);
        if (arrayFunc.isConstant()) {
            final StringSink sink = Misc.getThreadLocalSink();
            ArrayView view = arrayFunc.getArray(null);
            printArray(view, sink);
            return new VarcharConstant(Chars.toString(sink));
        }
        return new Func(args.getQuick(0));
    }

    private static void printArray(ArrayView view, CharSink<?> sink) {
        if (view == null || view.isNull()) {
            return;
        }
        printRecursive(view, 0, 0, sink);
    }

    private static void printRecursive(ArrayView view, int dim, int baseFlatIndex, CharSink<?> sink) {
        int len = view.getDimLen(dim);
        int stride = view.getStride(dim);
        boolean isLeaf = (dim == view.getDimCount() - 1);

        sink.put('[');
        for (int i = 0; i < len; i++) {
            if (i > 0) {
                sink.put(',');
            }
            int currentFlatIndex = baseFlatIndex + i * stride;

            if (isLeaf) {
                switch (view.getElemType()) {
                    case ColumnType.DOUBLE:
                        double d = view.getDouble(currentFlatIndex);
                        if (Numbers.isNull(d)) {
                            sink.put("null");
                        } else {
                            sink.put(d);
                        }
                        break;
                    case ColumnType.LONG:
                        long l = view.getLong(currentFlatIndex);
                        if (l == Numbers.LONG_NULL) {
                            sink.put("null");
                        } else {
                            sink.put(l);
                        }
                        break;
                    default:
                        sink.put("?");
                        break;
                }
            } else {
                printRecursive(view, dim + 1, currentFlatIndex, sink);
            }
        }
        sink.put(']');
    }

    public static class Func extends AbstractCastToVarcharFunction {
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();

        public Func(Function arg) {
            super(arg);
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            ArrayView view = arg.getArray(rec);
            if (view == null) {
                return null;
            }
            sinkA.clear();
            printArray(view, sinkA);
            return sinkA;
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            ArrayView view = arg.getArray(rec);
            if (view == null) {
                return null;
            }
            sinkB.clear();
            printArray(view, sinkB);
            return sinkB;
        }
    }
}
