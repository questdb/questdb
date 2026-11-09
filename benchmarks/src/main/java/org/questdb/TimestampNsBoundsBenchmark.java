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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.bool.BetweenTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.eq.EqTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.lt.LtTimestampFunctionFactory;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Text to TIMESTAMP_NS parsing, and the per-row cost of timestamp comparisons with a constant and between two
 * columns, the Java filter path. Each operation runs over the same 1024 values; the result is per value.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class TimestampNsBoundsBenchmark {
    private static final long DAY_MICROS = 86_400_000_000L;
    private static final int N = 1024;
    private static final long YEAR_2020_MICROS = 1_577_836_800_000_000L;
    private final ColumnFunc micros = new ColumnFunc(ColumnType.TIMESTAMP_MICRO);
    private final ColumnFunc nanos = new ColumnFunc(ColumnType.TIMESTAMP_NANO);
    private final ObjList<String> texts = new ObjList<>();
    private final ObjList<Utf8String> utf8Texts = new ObjList<>();
    // random rows make the filter result unpredictable, sorted rows make it predictable
    @Param({"random", "sorted"})
    public String order;
    private Function betweenMicrosConst;
    private Function eqMicrosConst;
    private Function eqNanosConstOnMicros;
    private Function ltColumns;
    private Function ltMicrosConst;
    private Function ltMicrosConstOnNanos;
    private Function ltNanosConstOnMicros;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(TimestampNsBoundsBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public int betweenMicrosConst() {
        return count(betweenMicrosConst);
    }

    @Benchmark
    public int eqMicrosConst() {
        return count(eqMicrosConst);
    }

    @Benchmark
    public int eqNanosConstOnMicros() {
        return count(eqNanosConstOnMicros);
    }

    @Benchmark
    public int ltColumns() {
        return count(ltColumns);
    }

    @Benchmark
    public int ltMicrosConst() {
        return count(ltMicrosConst);
    }

    @Benchmark
    public int ltMicrosConstOnNanos() {
        return count(ltMicrosConstOnNanos);
    }

    @Benchmark
    public int ltNanosConstOnMicros() {
        return count(ltNanosConstOnMicros);
    }

    @Benchmark
    public long parseString() throws NumericException {
        long sum = 0;
        for (int i = 0; i < N; i++) {
            final String text = texts.getQuick(i);
            sum += NanosTimestampDriver.INSTANCE.parseFloor(text, 0, text.length());
        }
        return sum;
    }

    @Benchmark
    public long parseVarchar() throws NumericException {
        long sum = 0;
        for (int i = 0; i < N; i++) {
            final Utf8String text = utf8Texts.getQuick(i);
            sum += NanosTimestampDriver.INSTANCE.parseFloor(text, 0, text.size());
        }
        return sum;
    }

    @Setup(Level.Trial)
    public void setup() throws SqlException {
        final Rnd rnd = new Rnd();
        final StringSink sink = new StringSink();
        for (int i = 0; i < N; i++) {
            // a year of values from 2020, the micros and nanos columns hold the same instants
            final long value = YEAR_2020_MICROS + rnd.nextPositiveLong() % (365 * DAY_MICROS);
            micros.values[i] = value;
            nanos.values[i] = value * 1000 + rnd.nextPositiveInt() % 1000;
            sink.clear();
            NanosTimestampDriver.INSTANCE.append(sink, nanos.values[i]);
            texts.add(sink.toString());
            utf8Texts.add(new Utf8String(sink.toString()));
        }
        if ("sorted".equals(order)) {
            Arrays.sort(micros.values);
            Arrays.sort(nanos.values);
        }
        final long midMicros = YEAR_2020_MICROS + 180 * DAY_MICROS;
        ltMicrosConst = newFunction(new LtTimestampFunctionFactory(), micros, new TimestampConstant(midMicros, ColumnType.TIMESTAMP_MICRO));
        ltNanosConstOnMicros = newFunction(new LtTimestampFunctionFactory(), micros, new TimestampConstant(midMicros * 1000 + 123, ColumnType.TIMESTAMP_NANO));
        ltMicrosConstOnNanos = newFunction(new LtTimestampFunctionFactory(), nanos, new TimestampConstant(midMicros, ColumnType.TIMESTAMP_MICRO));
        ltColumns = newFunction(new LtTimestampFunctionFactory(), micros, nanos);
        eqMicrosConst = newFunction(new EqTimestampFunctionFactory(), micros, new TimestampConstant(micros.values[7], ColumnType.TIMESTAMP_MICRO));
        eqNanosConstOnMicros = newFunction(new EqTimestampFunctionFactory(), micros, new TimestampConstant(micros.values[7] * 1000, ColumnType.TIMESTAMP_NANO));
        final ObjList<Function> args = new ObjList<>();
        args.add(micros);
        args.add(new TimestampConstant(midMicros, ColumnType.TIMESTAMP_MICRO));
        args.add(new TimestampConstant(midMicros + 90 * DAY_MICROS, ColumnType.TIMESTAMP_MICRO));
        betweenMicrosConst = new BetweenTimestampFunctionFactory().newInstance(0, args, null, null, null);
    }

    private static Function newFunction(FunctionFactory factory, Function left, Function right) throws SqlException {
        final ObjList<Function> args = new ObjList<>();
        args.add(left);
        args.add(right);
        return factory.newInstance(0, args, null, null, null);
    }

    private int count(Function filter) {
        int count = 0;
        for (int i = 0; i < N; i++) {
            micros.index = i;
            nanos.index = i;
            if (filter.getBool(null)) {
                count++;
            }
        }
        return count;
    }

    // a column read row by row: index selects the row
    private static class ColumnFunc extends TimestampFunction {
        private final long[] values = new long[N];
        private int index;

        private ColumnFunc(int timestampType) {
            super(timestampType);
        }

        @Override
        public long getTimestamp(Record rec) {
            return values[index];
        }
    }
}
