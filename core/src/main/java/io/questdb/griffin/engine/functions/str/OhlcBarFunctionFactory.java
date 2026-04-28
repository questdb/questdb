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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.Nullable;

/**
 * Scalar function that renders a horizontal OHLC candlestick bar from
 * precomputed open, high, low, close values with explicit scale bounds.
 * <p>
 * {@code ohlc_bar(open, high, low, close, min, max, width)}
 * <p>
 * All parameters accept column references, window functions, and expressions
 * (uppercase D/I signatures). This enables composition with SAMPLE BY
 * subqueries and window functions for global scaling:
 * <pre>
 * SELECT ts, ohlc_bar(o, h, l, c, min(l) OVER (), max(h) OVER (), 40)
 * FROM (
 *     SELECT ts, first(price) o, max(price) h, min(price) l, last(price) c
 *     FROM trades SAMPLE BY 1h
 * )
 * </pre>
 */
public class OhlcBarFunctionFactory implements FunctionFactory {
    private static final int DEFAULT_WIDTH = 40;
    private static final String SIGNATURE = "ohlc_bar(DDDDDDI)";

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
        int maxWidth = configuration.getStrFunctionMaxBufferLength() / 3;
        return new OhlcBarScalarFunction(
                "ohlc_bar",
                new ObjList<>(args),
                new IntList(argPositions),
                false,
                maxWidth
        );
    }

    static class OhlcBarScalarFunction extends VarcharFunction implements MultiArgFunction {
        private static final char BLANK = '\u2800';
        private static final char BODY_BEAR = '\u2591';
        private static final char BODY_BULL = '\u2588';
        private static final char DOJI = '\u2502';
        private static final char WICK = '\u2500';
        private final ObjList<Function> args;
        private final int maxBufferLength;
        private final int maxWidth;
        private final String name;
        private final int minArgPosition;
        private final boolean showLabels;
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();
        private final int widthArgIndex;
        private final int widthPosition;

        OhlcBarScalarFunction(
                String name,
                ObjList<Function> args,
                IntList argPositions,
                boolean showLabels,
                int maxWidth
        ) {
            this.name = name;
            this.args = args;
            this.showLabels = showLabels;
            this.maxBufferLength = maxWidth * 3;
            this.maxWidth = showLabels ? Math.max(1, (maxWidth * 3 - 120) / 3) : maxWidth;
            this.minArgPosition = argPositions.getQuick(4);
            this.widthArgIndex = args.size() > 6 ? 6 : -1;
            this.widthPosition = widthArgIndex >= 0 ? argPositions.getQuick(6) : 0;
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public String getName() {
            return name;
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

        private int effectiveWidth(Record rec) {
            int w;
            if (widthArgIndex >= 0) {
                w = args.getQuick(widthArgIndex).getInt(rec);
                if (w < 1) {
                    throw CairoException.nonCritical().position(widthPosition)
                            .put("width must be a positive integer");
                }
            } else {
                w = DEFAULT_WIDTH;
            }
            if (w > maxWidth) {
                throw CairoException.nonCritical().position(widthPosition)
                        .put("breached memory limit set for ").put(name)
                        .put(" [maxWidth=").put(maxWidth)
                        .put(", requestedWidth=").put(w).put(']');
            }
            return w;
        }

        private int mapPosition(double value, double low, double range, int width) {
            if (range == 0.0) {
                return width / 2;
            }
            double proportion = (value - low) / range;
            int pos = (int) Math.round(proportion * (width - 1));
            return Math.max(0, Math.min(width - 1, pos));
        }

        private @Nullable Utf8Sequence renderBar(Record rec, Utf8StringSink sink) {
            double open = args.getQuick(0).getDouble(rec);
            double high = args.getQuick(1).getDouble(rec);
            double low = args.getQuick(2).getDouble(rec);
            double close = args.getQuick(3).getDouble(rec);
            double scaleMin = args.getQuick(4).getDouble(rec);
            double scaleMax = args.getQuick(5).getDouble(rec);

            if (Double.isNaN(open) || Double.isNaN(high) || Double.isNaN(low) || Double.isNaN(close)
                    || Double.isNaN(scaleMin) || Double.isNaN(scaleMax)) {
                return null;
            }

            if (scaleMin > scaleMax) {
                throw CairoException.nonCritical().position(minArgPosition)
                        .put(name).put("() min must not exceed max [min=")
                        .put(scaleMin).put(", max=").put(scaleMax).put(']');
            }

            int width = effectiveWidth(rec);
            double scaleRange = scaleMax - scaleMin;

            int lowPos = mapPosition(low, scaleMin, scaleRange, width);
            int highPos = mapPosition(high, scaleMin, scaleRange, width);
            int openPos = mapPosition(open, scaleMin, scaleRange, width);
            int closePos = mapPosition(close, scaleMin, scaleRange, width);

            int bodyStart = Math.min(openPos, closePos);
            int bodyEnd = Math.max(openPos, closePos);
            boolean isDoji = open == close;
            boolean isBullish = close >= open;

            sink.clear();

            for (int i = 0; i < width; i++) {
                char c;
                if (i < lowPos || i > highPos) {
                    c = BLANK;
                } else if (isDoji && i == bodyStart) {
                    c = DOJI;
                } else if (i >= bodyStart && i <= bodyEnd) {
                    c = isBullish ? BODY_BULL : BODY_BEAR;
                } else {
                    c = WICK;
                }
                sink.put(c);
            }

            if (showLabels) {
                sink.putAscii(" O:");
                Numbers.append(sink, open);
                sink.putAscii(" H:");
                Numbers.append(sink, high);
                sink.putAscii(" L:");
                Numbers.append(sink, low);
                sink.putAscii(" C:");
                Numbers.append(sink, close);
            }

            if (sink.size() > maxBufferLength) {
                throw CairoException.nonCritical().position(widthPosition)
                        .put("breached memory limit set for ").put(name)
                        .put(" [maxBytes=").put(maxBufferLength)
                        .put(", actualBytes=").put(sink.size()).put(']');
            }

            return sink;
        }
    }
}
