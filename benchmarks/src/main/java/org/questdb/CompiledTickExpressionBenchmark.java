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

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TickCalendarServiceFactory;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.model.CompiledTickExpression;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.Micros;
import org.jetbrains.annotations.NotNull;
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

import java.io.IOException;
import java.nio.file.Files;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks one evaluation of the compiled form of a tick expression with long date ranges, whose
 * days merge into the intervals so far once there are as many as the merge threshold, with the
 * days in order, with elements that lie later in time before the range, with days without an
 * interval, with two time slots a day, across days when the clocks change, with days that overlap,
 * and with a limit of 100,000 intervals. The expression compiles once; each evaluation appends to
 * a cleared list at a fixed "now".
 * <p>
 * The shapes from staticDateItem on are lists with fixed dates, a tick calendar, XNYS, whose
 * sessions the benchmark builds, and a duration: the fixed dates keep their own extents, many of
 * which stay apart in the carried shapes, and a fixed date whose own extents alone exceed the
 * interval limit makes the list parse as its bind variable on each evaluation, in the overflow
 * shapes, where earlier elements hold them, or fail, or, in a lenient evaluation, fall back to the IR.
 * {@link #compile} compiles and validates the expression, as SQL does for each statement and each
 * worker of a parallel filter.
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
            "laterDaysFirstLimit100000",
            "staticDateItem",
            "staticDateManyDays",
            "staticDateCarried780",
            "staticDateCarried780x3",
            "staticDateCarried1020",
            "staticDateCarried1020Descending",
            "staticDateCarried1020NewYork",
            "staticDateOverflowBridged",
            "staticDateOverflowBridged10080",
            "staticDateOverflowBridged44640",
            "staticDateOverflowError",
            "staticDateLenientFallback"
    })
    public String shape;

    private CairoConfiguration configuration;
    private TimestampDriver driver;
    // a context of SQL that the engine runs on its own, for staticDateLenientFallback
    private CairoEngine engine;
    private CompiledTickExpression expression;
    private long now;
    private LongList out;
    private String tickExpr;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CompiledTickExpressionBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(5)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    /**
     * Returns one session a weekday of 2024-12 to 2025-03 from 14:30 to 21:00Z, but on the holidays of
     * XNYS in that time, as TestTickCalendarService has them for 2025.
     */
    private static LongList buildSessions() {
        final LongList sessions = new LongList();
        for (LocalDate day = LocalDate.of(2024, 12, 1); day.isBefore(LocalDate.of(2025, 4, 1)); day = day.plusDays(1)) {
            if (day.getDayOfWeek() == DayOfWeek.SATURDAY || day.getDayOfWeek() == DayOfWeek.SUNDAY) {
                continue;
            }
            if (day.equals(LocalDate.of(2024, 12, 25)) || day.equals(LocalDate.of(2025, 1, 1))
                    || day.equals(LocalDate.of(2025, 1, 9)) || day.equals(LocalDate.of(2025, 1, 20))
                    || day.equals(LocalDate.of(2025, 2, 17))) {
                continue;
            }
            final long start = day.toEpochDay() * Micros.DAY_MICROS;
            sessions.add(start + 14 * Micros.HOUR_MICROS + 30 * Micros.MINUTE_MICROS, start + 21 * Micros.HOUR_MICROS - 1);
        }
        return sessions;
    }

    /**
     * Compiles the expression and validates it at "now", as SQL does for each statement, and for each
     * worker of a parallel filter.
     */
    @Benchmark
    public int compile() {
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, tickExpr, 0, tickExpr.length(), 0)) {
            compiled.validate(now);
            return 1;
        } catch (SqlException e) {
            return -1;
        }
    }

    @Benchmark
    public int evaluate() {
        out.clear();
        try {
            expression.evaluate(out, now);
        } catch (CairoException e) {
            return -1;
        }
        return out.size();
    }

    @Setup(Level.Trial)
    public void setup() throws SqlException {
        int timestampType = ColumnType.TIMESTAMP_MICRO;
        String today = "2025-01-24";
        int mergeThreshold = 256;
        int maxIntervals = 1024;
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
            case "staticDateItem":
                tickExpr = "[$today, 2025-01-24]#XNYS;1h";
                today = "2025-01-27";
                break;
            case "staticDateManyDays":
                // the windows of an hour from 00:00 hold no session, the days 336 of them
                tickExpr = "[$today, 2025-[01..03]-[01..28]]#XNYS;1h";
                today = "2025-01-27";
                break;
            case "staticDateCarried780":
                // 780 seconds that stay apart, all before the session
                tickExpr = "[$today, 2025-01-24T[00..12]:[00..59]:00]#XNYS;1m";
                today = "2025-01-27";
                break;
            case "staticDateCarried780x3":
                tickExpr = "[$today, 2025-01-22T[00..12]:[00..59]:00, 2025-01-23T[00..12]:[00..59]:00, 2025-01-24T[00..12]:[00..59]:00]#XNYS;1m";
                today = "2025-01-27";
                break;
            case "staticDateCarried1020":
                // 1,020 seconds that stay apart, 150 of them in the session
                tickExpr = "[$today, 2025-01-24T[00..16]:[00..59]:00]#XNYS;1m";
                today = "2025-01-27";
                break;
            case "staticDateCarried1020Descending":
                tickExpr = "[$today, 2025-01-24T[16,15,14,13,12,11,10,09,08,07,06,05,04,03,02,01,00]:[00..59]:00]#XNYS;1m";
                today = "2025-01-27";
                break;
            case "staticDateCarried1020NewYork":
                tickExpr = "[$today, 2025-01-24T[00..16]:[00..59]:00]@America/New_York#XNYS;1m";
                today = "2025-01-27";
                break;
            case "staticDateOverflowBridged":
                // 1,440 seconds, which today holds
                tickExpr = "[$today, 2025-01-27T[00..23]:[00..59]:00]#XNYS;1m";
                today = "2025-01-27";
                break;
            case "staticDateOverflowBridged10080":
                // the seconds of 7 days, which the range holds
                tickExpr = "[$today-40d..$today, 2025-01-[01..07]T[00..23]:[00..59]:00]#XNYS;1m";
                today = "2025-01-31";
                break;
            case "staticDateOverflowBridged44640":
                tickExpr = "[$today-40d..$today, 2025-01-[01..31]T[00..23]:[00..59]:00]#XNYS;1m";
                today = "2025-01-31";
                break;
            case "staticDateOverflowError":
            case "staticDateLenientFallback":
                // 1,440 seconds that nothing holds
                tickExpr = "[$today, 2025-01-24T[00..23]:[00..59]:00]#XNYS;1m";
                today = "2025-01-27";
                break;
            default:
                throw new IllegalArgumentException(shape);
        }
        final int threshold = mergeThreshold;
        final int limit = maxIntervals;
        final LongList sessions = buildSessions();
        final FactoryProvider factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull TickCalendarServiceFactory getTickCalendarServiceFactory() {
                return () -> exchange -> Chars.equalsIgnoreCase("XNYS", exchange) ? sessions : null;
            }
        };
        // the engine of the lenient evaluation writes to its root
        final boolean isLenient = shape.equals("staticDateLenientFallback");
        final String root;
        try {
            root = isLenient ? Files.createTempDirectory("tick-bench").toString() : System.getProperty("java.io.tmpdir");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        configuration = new DefaultCairoConfiguration(root) {
            @Override
            public @NotNull FactoryProvider getFactoryProvider() {
                return factoryProvider;
            }

            @Override
            public int getSqlIntervalIncrementalMergeThreshold() {
                return threshold;
            }

            @Override
            public int getSqlIntervalMaxIntervalsAfterMerge() {
                return limit;
            }
        };
        driver = ColumnType.getTimestampDriver(timestampType);
        expression = IntervalUtils.compileTickExpr(driver, configuration, tickExpr, 0, tickExpr.length(), 0);
        now = driver.parseFloorLiteral(today + "T12:00:00.000000Z");
        out = new LongList();
        if (isLenient) {
            engine = new CairoEngine(configuration);
            try (SqlExecutionContextImpl backgroundJob = new SqlExecutionContextImpl(engine, 1) {
                @Override
                public boolean isBackgroundJob() {
                    return true;
                }
            }) {
                backgroundJob.with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
                backgroundJob.setNowAndFixClock(now, timestampType);
                expression.init(null, backgroundJob);
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        expression.close();
        engine = Misc.free(engine);
    }
}
