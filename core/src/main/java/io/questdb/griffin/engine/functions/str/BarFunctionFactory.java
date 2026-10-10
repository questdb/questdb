/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.QuaternaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.Nullable;

public class BarFunctionFactory implements FunctionFactory {
    private static final String SIGNATURE = "bar(DDDI)";
    // ▏▎▍▌▋▊▉█ - fractional blocks in increasing fill order
    private static final char[] BAR_CHARS = {'▏', '▎', '▍', '▌', '▋', '▊', '▉', '█'};

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        // Each bar character is 3 bytes in UTF-8
        final int maxWidth = configuration.getStrFunctionMaxBufferLength() / 3;
        return new BarFunction(args.getQuick(0), args.getQuick(1), args.getQuick(2), args.getQuick(3), maxWidth, argPositions.getQuick(3));
    }

    private static class BarFunction extends VarcharFunction implements QuaternaryFunction {
        private final Function maxFunc;
        private final int maxWidth;
        private final Function minFunc;
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();
        private final Function valueFunc;
        private final Function widthFunc;
        private final int widthPosition;

        private BarFunction(Function valueFunc, Function minFunc, Function maxFunc, Function widthFunc, int maxWidth, int widthPosition) {
            this.valueFunc = valueFunc;
            this.minFunc = minFunc;
            this.maxFunc = maxFunc;
            this.widthFunc = widthFunc;
            this.maxWidth = maxWidth;
            this.widthPosition = widthPosition;
        }

        @Override
        public Function getFunc0() {
            return valueFunc;
        }

        @Override
        public Function getFunc1() {
            return minFunc;
        }

        @Override
        public Function getFunc2() {
            return maxFunc;
        }

        @Override
        public Function getFunc3() {
            return widthFunc;
        }

        @Override
        public String getName() {
            return "bar";
        }

        @Override
        public @Nullable Utf8Sequence getVarcharA(Record rec) {
            return renderBar(rec, sinkA);
        }

        @Override
        public @Nullable Utf8Sequence getVarcharB(Record rec) {
            return renderBar(rec, sinkB);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        private @Nullable Utf8Sequence renderBar(Record rec, Utf8StringSink sink) {
            final double value = valueFunc.getDouble(rec);
            // Non-finite values (NaN as DOUBLE null and +/-Infinity as stored IEEE 754
            // values on NOT NULL columns) have no sensible bar rendering — return null.
            if (!Double.isFinite(value)) {
                return null;
            }

            final double min = minFunc.getDouble(rec);
            final double max = maxFunc.getDouble(rec);
            final int width = widthFunc.getInt(rec);

            if (!Double.isFinite(min) || !Double.isFinite(max) || width <= 0 || min >= max) {
                return null;
            }

            if (width > maxWidth) {
                throw CairoException.nonCritical().position(widthPosition)
                        .put("breached memory limit set for ").put(SIGNATURE)
                        .put(" [maxWidth=").put(maxWidth)
                        .put(", requestedWidth=").put(width).put(']');
            }

            sink.clear();

            final double clamped = Math.max(min, Math.min(max, value));
            final double proportion = (clamped - min) / (max - min);
            final double filled = proportion * width;

            final int wholeChars = (int) filled;
            final double fractional = filled - wholeChars;

            // Render full blocks
            for (int i = 0; i < wholeChars; i++) {
                sink.put(BAR_CHARS[7]);
            }

            // Render fractional block if there's remaining space
            if (wholeChars < width) {
                final int fracIndex = (int) (fractional * 8);
                if (fracIndex > 0) {
                    sink.put(BAR_CHARS[fracIndex - 1]);
                }
            }

            return sink;
        }
    }
}
