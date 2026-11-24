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
import io.questdb.std.LongList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.time.LocalDate;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks one evaluation of the compiled form of a tick expression with long date ranges, whose
 * days merge into the intervals so far once there are as many as the merge threshold, with the
 * days in order, with elements that lie later in time before the range, with days without an
 * interval, with two time slots a day, across days when the clocks change, with days that overlap,
 * and with a limit of 100,000 intervals. The expression compiles once; each evaluation appends to
 * a cleared list at a fixed "now".
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CompiledTickExpressionBenchmark {

    @Param({
            "belowThreshold",
            "inOrder",
            "inOrderNanos",
            "saturdays",
            "laterDaysFirst",
            "elevenLaterDaysFirst",
            "twoSlotsReversed",
            "londonDst",
            "laterDatesFirst",
            "overlappingDays",
            "laterDaysFirstLimit100000"
    })
    public String shape;

    private CompiledTickExpression expression;
    private long now;
    private LongList out;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CompiledTickExpressionBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(5)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public int evaluate() {
        out.clear();
        expression.evaluate(out, now);
        return out.size();
    }

    @Setup(Level.Trial)
    public void setup() throws SqlException {
        int timestampType = ColumnType.TIMESTAMP_MICRO;
        String today = "2025-01-24";
        int mergeThreshold = 256;
        int maxIntervals = 1024;
        final String tickExpr;
        switch (shape) {
            case "belowThreshold":
                tickExpr = "[$today-100d..$today]T09:00;1h";
                break;
            case "inOrder":
                tickExpr = "[$today-1000d..$today]T09:00;1h";
                break;
            case "inOrderNanos":
                tickExpr = "[$today-1000d..$today]T09:00;1h";
                timestampType = ColumnType.TIMESTAMP_NANO;
                break;
            case "saturdays":
                tickExpr = "[$today-7000d..$today]T09:00#Sat;1h";
                break;
            case "laterDaysFirst":
                tickExpr = "[$today+1d..$today+500d, $today-500d..$today]T09:00;1h";
                break;
            case "elevenLaterDaysFirst":
                tickExpr = "[$today-10d..$today, $today-1000d..$today-11d]T09:00";
                break;
            case "twoSlotsReversed":
                tickExpr = "[$today-500d..$today]T[15:00,09:00];1h";
                break;
            case "londonDst":
                tickExpr = "[$today-400d..$today]T01:30@Europe/London;1h";
                today = "2024-12-01";
                break;
            case "laterDatesFirst": {
                // 1,000 dates in 2400, every other day, then 10,001 days from today that merge into one
                final StringBuilder sb = new StringBuilder("[");
                for (LocalDate day = LocalDate.of(2400, 1, 1), last = day.plusDays(1998); !day.isAfter(last); day = day.plusDays(2)) {
                    sb.append(day).append(", ");
                }
                tickExpr = sb.append("$today..$today+10000d]").toString();
                break;
            }
            case "overlappingDays":
                tickExpr = "[$today-1000d..$today]T09:00;3d";
                break;
            case "laterDaysFirstLimit100000":
                tickExpr = "[$today+1d..$today+5000d, $today-5000d..$today]T09:00;1h";
                maxIntervals = 100_000;
                break;
            default:
                throw new IllegalArgumentException(shape);
        }
        final int threshold = mergeThreshold;
        final int limit = maxIntervals;
        final CairoConfiguration configuration = new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir")) {
            @Override
            public int getSqlIntervalIncrementalMergeThreshold() {
                return threshold;
            }

            @Override
            public int getSqlIntervalMaxIntervalsAfterMerge() {
                return limit;
            }
        };
        final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        expression = IntervalUtils.compileTickExpr(driver, configuration, tickExpr, 0, tickExpr.length(), 0);
        now = driver.parseFloorLiteral(today + "T12:00:00.000000Z");
        out = new LongList();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        expression.close();
    }
}
