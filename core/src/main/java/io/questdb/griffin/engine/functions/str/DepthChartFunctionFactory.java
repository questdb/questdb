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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
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
 * Scalar function that renders a market depth profile from bid/ask volume
 * arrays as a horizontal sparkline-style chart.
 * <p>
 * Bids are rendered on the left (worst-to-best, left-to-right), asks on
 * the right (best-to-worst). A spread marker separates them. Uses log
 * scale ({@code Math.log1p}) internally to handle the exponential growth
 * of cumulative volumes.
 * <p>
 * {@code depth_chart(bid_volumes[], ask_volumes[] [, width])}
 */
public class DepthChartFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "depth_chart(D[]D[])";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        for (int i = 0; i < 2; i++) {
            final int dims = ColumnType.decodeWeakArrayDimensionality(args.getQuick(i).getType());
            if (dims > 0 && dims != 1) {
                throw SqlException.$(argPositions.getQuick(i), "not a one-dimensional array");
            }
        }
        int maxWidth = configuration.getStrFunctionMaxBufferLength() / 3;
        return new DepthChartFunction(
                "depth_chart",
                new ObjList<>(args),
                new IntList(argPositions),
                false,
                position,
                maxWidth,
                configuration.getStrFunctionMaxBufferLength()
        );
    }

    static class DepthChartFunction extends VarcharFunction implements MultiArgFunction {
        private static final char[] SPARK_CHARS = {'\u2581', '\u2582', '\u2583', '\u2584', '\u2585', '\u2586', '\u2587', '\u2588'};
        private static final char SPREAD = '\u254E';
        private final ObjList<Function> args;
        private final int askArgPosition;
        private final int bidArgPosition;
        private final boolean hasLabels;
        private final int maxBufferLength;
        private final int maxWidth;
        private final String name;
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();
        private final int widthArgIndex;
        private final int widthPosition;
        private double[] scratchAsks;
        private double[] scratchBids;

        DepthChartFunction(
                String name,
                ObjList<Function> args,
                IntList argPositions,
                boolean hasLabels,
                int functionPosition,
                int maxWidth,
                int maxBufferLength
        ) {
            this.name = name;
            this.args = args;
            this.hasLabels = hasLabels;
            this.askArgPosition = argPositions.getQuick(1);
            this.bidArgPosition = argPositions.getQuick(0);
            this.maxWidth = maxWidth;
            this.maxBufferLength = maxBufferLength;
            this.widthArgIndex = args.size() > 2 ? 2 : -1;
            this.widthPosition = widthArgIndex >= 0 ? argPositions.getQuick(2) : functionPosition;
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
            return renderChart(rec, sinkA);
        }

        @Override
        public @Nullable Utf8Sequence getVarcharB(Record rec) {
            return renderChart(rec, sinkB);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        private char charForValue(double logValue, double logMin, double logRange) {
            if (logRange == 0.0) {
                // All values identical. If logMin > 0 (positive depth), use
                // highest char. If logMin == 0 (all zeros), use lowest char.
                return logMin > 0.0 ? SPARK_CHARS[SPARK_CHARS.length - 1] : SPARK_CHARS[0];
            }
            double clamped = Math.max(logMin, Math.min(logMin + logRange, logValue));
            int idx = (int) ((clamped - logMin) / logRange * (SPARK_CHARS.length - 1));
            return SPARK_CHARS[Math.min(idx, SPARK_CHARS.length - 1)];
        }

        private double computeCumulative(ArrayView arr, int arrLen, int targetChars, double[] out, int argPosition) {
            double cumSum = 0.0;

            if (targetChars >= arrLen) {
                // No subsampling needed. One char per level, pad remainder.
                for (int i = 0; i < arrLen; i++) {
                    double v = arr.getDouble(i);
                    validateVolume(v, argPosition);
                    cumSum += v;
                    validateCumulative(cumSum, argPosition);
                    out[i] = cumSum;
                }
                // Pad remaining positions with 0 (will render as lowest char)
                for (int i = arrLen; i < targetChars; i++) {
                    out[i] = 0.0;
                }
            } else {
                // Subsample: streaming bucket-average of the cumulative curve.
                // Single pass, O(targetChars) memory - no scratch proportional
                // to input length.
                // Bucket j covers source indexes [from, to) where:
                //   from = floor(j * arrLen / targetChars)
                //   to   = floor((j+1) * arrLen / targetChars)
                int bucketIdx = 0;
                int bucketFrom = 0;
                int bucketTo = (int) ((long) (bucketIdx + 1) * arrLen / targetChars);
                double bucketSum = 0.0;

                for (int i = 0; i < arrLen; i++) {
                    double v = arr.getDouble(i);
                    validateVolume(v, argPosition);
                    cumSum += v;
                    validateCumulative(cumSum, argPosition);

                    if (i >= bucketFrom && i < bucketTo) {
                        bucketSum += cumSum;
                    }

                    if (i == bucketTo - 1 || i == arrLen - 1) {
                        out[bucketIdx] = bucketSum / (bucketTo - bucketFrom);
                        bucketIdx++;
                        if (bucketIdx < targetChars) {
                            bucketFrom = bucketTo;
                            bucketTo = (int) ((long) (bucketIdx + 1) * arrLen / targetChars);
                            bucketSum = 0.0;
                        }
                    }
                }
            }

            return cumSum;
        }

        private double[] ensureScratch(double[] scratch, int needed) {
            int cap = Math.min(needed, maxWidth);
            if (scratch == null || scratch.length < cap) {
                return new double[cap];
            }
            return scratch;
        }

        private @Nullable Utf8Sequence renderChart(Record rec, Utf8StringSink sink) {
            ArrayView bidArr = args.getQuick(0).getArray(rec);
            ArrayView askArr = args.getQuick(1).getArray(rec);

            if (bidArr == null || askArr == null || bidArr.isNull() || askArr.isNull()) {
                return null;
            }

            int bidLen = bidArr.getDimLen(0);
            int askLen = askArr.getDimLen(0);

            if (bidLen == 0 || askLen == 0) {
                return null;
            }

            // Determine width per side
            int bidChars, askChars;
            if (widthArgIndex >= 0) {
                int width = args.getQuick(widthArgIndex).getInt(rec);
                if (width < 3) {
                    throw CairoException.nonCritical().position(widthPosition)
                            .put("width must be at least 3");
                }
                if (width > maxWidth) {
                    throw CairoException.nonCritical().position(widthPosition)
                            .put("breached memory limit set for ").put(name)
                            .put(" [maxWidth=").put(maxWidth)
                            .put(", requestedWidth=").put(width).put(']');
                }
                askChars = width / 2;
                bidChars = width - 1 - askChars;
            } else {
                bidChars = bidLen;
                askChars = askLen;
                int totalWidth = bidChars + 1 + askChars;
                if (totalWidth > maxWidth) {
                    throw CairoException.nonCritical().position(bidArgPosition)
                            .put("breached memory limit set for ").put(name)
                            .put(" [maxWidth=").put(maxWidth)
                            .put(", actualWidth=").put(totalWidth).put(']');
                }
            }

            // Compute cumulative sums and validate
            scratchBids = ensureScratch(scratchBids, bidChars);
            scratchAsks = ensureScratch(scratchAsks, askChars);

            double bidTotal = computeCumulative(bidArr, bidLen, bidChars, scratchBids, bidArgPosition);
            double askTotal = computeCumulative(askArr, askLen, askChars, scratchAsks, askArgPosition);

            // Apply log1p and find global range
            double logMin = Double.POSITIVE_INFINITY;
            double logMax = Double.NEGATIVE_INFINITY;

            for (int i = 0; i < bidChars; i++) {
                double v = Math.log1p(scratchBids[i]);
                scratchBids[i] = v;
                if (v < logMin) logMin = v;
                if (v > logMax) logMax = v;
            }
            for (int i = 0; i < askChars; i++) {
                double v = Math.log1p(scratchAsks[i]);
                scratchAsks[i] = v;
                if (v < logMin) logMin = v;
                if (v > logMax) logMax = v;
            }

            double logRange = logMax - logMin;

            // Render
            sink.clear();

            // Bids reversed: index 0 = worst bid (leftmost)
            for (int i = 0; i < bidChars; i++) {
                int srcIdx = bidChars - 1 - i;
                sink.put(charForValue(scratchBids[srcIdx], logMin, logRange));
            }

            // Spread marker
            sink.put(SPREAD);

            // Asks normal: index 0 = best ask (leftmost, nearest spread)
            for (int i = 0; i < askChars; i++) {
                sink.put(charForValue(scratchAsks[i], logMin, logRange));
            }

            // Labels
            if (hasLabels) {
                double bestBid = bidArr.getDouble(0);
                double bestAsk = askArr.getDouble(0);
                sink.putAscii(" bb:");
                Numbers.append(sink, bestBid);
                sink.putAscii(" ba:");
                Numbers.append(sink, bestAsk);
                sink.putAscii(" tb:");
                Numbers.append(sink, bidTotal);
                sink.putAscii(" ta:");
                Numbers.append(sink, askTotal);
            }

            // Validate total output size
            if (sink.size() > maxBufferLength) {
                throw CairoException.nonCritical().position(widthPosition)
                        .put("breached memory limit set for ").put(name)
                        .put(" [maxBytes=").put(maxBufferLength)
                        .put(", actualBytes=").put(sink.size()).put(']');
            }

            return sink;
        }

        private void validateCumulative(double cumSum, int argPosition) {
            if (!Double.isFinite(cumSum)) {
                throw CairoException.nonCritical().position(argPosition)
                        .put(name).put("() volume must be finite and non-negative");
            }
        }

        private void validateVolume(double v, int argPosition) {
            if (!Double.isFinite(v) || v < 0.0) {
                throw CairoException.nonCritical().position(argPosition)
                        .put(name).put("() volume must be finite and non-negative");
            }
        }
    }
}
