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
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalOperation;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.str.StringSink;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;

/**
 * Parses a tick expression the way SQL parses a bind variable once per execution, and the value of
 * a string column once per row, as in {@code ts IN $1} and {@code ts IN column}: a range between two
 * points in time, alone and with a time part, which the parser checks although the range ignores it,
 * a point in time with a timezone of its own, and a day, which reads its time part.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class TickExprParseBenchmark {
    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir"));
    private final LongList intervals = new LongList();
    private final StringSink sink = new StringSink();
    private final TimestampDriver timestampDriver = MicrosTimestampDriver.INSTANCE;
    private final long now = timestampDriver.parseFloorLiteral("2024-01-02T10:00:00.000000Z");
    @Param({
            "[$now - 1h..$now]",
            "[$now - 1h..$now]T09:00",
            "[$now - 1h..$now]T[09..10]:00",
            "[$now@+01:00]T09:00",
            "[$today]T09:00"
    })
    public String expression;

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder()
                .include(TickExprParseBenchmark.class.getSimpleName())
                .jvmArgsAppend("--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED")
                .warmupIterations(3)
                .warmupTime(TimeValue.seconds(1))
                .measurementIterations(5)
                .measurementTime(TimeValue.seconds(1))
                .forks(1)
                .build();
        new Runner(options).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public int testParseTickExpr() throws SqlException {
        intervals.clear();
        IntervalUtils.parseTickExpr(timestampDriver, configuration, expression, 0, expression.length(), 0, intervals, IntervalOperation.INTERSECT, sink, true, now);
        return intervals.size();
    }
}
