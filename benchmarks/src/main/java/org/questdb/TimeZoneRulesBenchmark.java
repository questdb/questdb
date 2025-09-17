/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.Micros;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class TimeZoneRulesBenchmark {
    private static final int ITERATIONS = 10_000;
    private final long initialTs;
    private final TimeZoneRules rules;
    private final TimestampDriver timestampDriver = MicrosTimestampDriver.INSTANCE;

    public TimeZoneRulesBenchmark() {
        try {
            this.initialTs = timestampDriver.parseFloorLiteral("2024-01-01T00:00:00.000000Z");
            this.rules = timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin");
        } catch (NumericException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(TimeZoneRulesBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public long testGetLocalOffset() {
        long s = 0;
        for (long ts = initialTs, maxTs = initialTs + ITERATIONS * Micros.SECOND_MICROS; ts < maxTs; ts += Micros.SECOND_MICROS) {
            s += rules.getLocalOffset(ts);
        }
        return s;
    }

    @Benchmark
    public long testGetOffset() {
        long s = 0;
        for (long ts = initialTs, maxTs = initialTs + ITERATIONS * Micros.SECOND_MICROS; ts < maxTs; ts += Micros.SECOND_MICROS) {
            s += rules.getOffset(ts);
        }
        return s;
    }
}
