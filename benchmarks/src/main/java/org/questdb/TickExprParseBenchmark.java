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

package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.CompiledTickExpression;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.StringSink;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Parses tick strings whose bracket expansion leaves 480 separate ten-second blocks, more than
 * the incremental merge threshold (256), in one merge range: the 480,000 leaves of 10 ms of
 * {@code 2024-01-01T[09..16]:[00..59]:[00..09].[0..9][0..9]}. A query compiles a tick string
 * literal once, and parses a bind value on every execution and a string column on every row.
 * "now" is 2024-01-08T01:00Z, a Monday.
 * <ul>
 *   <li>{@code COVERED_ZONED}: the blocks as the zoned second element of a date list whose first
 *   element, the New York day 2024-01-01, covers them</li>
 *   <li>{@code COVERED_ZONED_DESCENDING}: the same with the hours in descending order, so the
 *   leaves of each hour sort before the blocks of the later hours</li>
 *   <li>{@code ZONED}: the zoned element alone</li>
 *   <li>{@code ZONELESS}: the element alone without the time zone</li>
 *   <li>{@code COMPILED_COVERED_ZONED}: {@code compileTickExpr} of a date list of {@code $today}
 *   and the zoned element, which it expands at compile time</li>
 * </ul>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 2)
@Fork(1)
public class TickExprParseBenchmark {
    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir"));
    private static final long NOW_MICROS = Micros.toMicros(2024, 1, 8, 1, 0);
    @Param({"COVERED_ZONED", "COVERED_ZONED_DESCENDING", "ZONED", "ZONELESS", "COMPILED_COVERED_ZONED"})
    public TickExpr tickExpr;
    private final LongList out = new LongList();
    private final StringSink sink = new StringSink();
    private final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_MICRO);

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(TickExprParseBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public int testTickExpr() throws SqlException {
        final String expression = tickExpr.expression;
        if (tickExpr.isCompiled) {
            try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                    timestampDriver, configuration, expression, 0, expression.length(), 0)) {
                return compiled.getElementCount();
            }
        }
        out.clear();
        IntervalUtils.parseTickExpr(timestampDriver, configuration, expression, 0, expression.length(), 0, out, sink, NOW_MICROS);
        return out.size();
    }

    public enum TickExpr {
        COVERED_ZONED(false, "[2024-01-01,2024-01-01T[09..16]:[00..59]:[00..09].[0..9][0..9]]@America/New_York"),
        COVERED_ZONED_DESCENDING(false, "[2024-01-01,2024-01-01T[16,15,14,13,12,11,10,09]:[00..59]:[00..09].[0..9][0..9]]@America/New_York"),
        ZONED(false, "2024-01-01T[09..16]:[00..59]:[00..09].[0..9][0..9]@America/New_York"),
        ZONELESS(false, "2024-01-01T[09..16]:[00..59]:[00..09].[0..9][0..9]"),
        COMPILED_COVERED_ZONED(true, "[$today,2024-01-01T[09..16]:[00..59]:[00..09].[0..9][0..9]]@America/New_York");

        private final String expression;
        private final boolean isCompiled;

        TickExpr(boolean isCompiled, String expression) {
            this.isCompiled = isCompiled;
            this.expression = expression;
        }
    }
}
