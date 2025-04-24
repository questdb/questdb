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

import io.questdb.cairo.mv.FixedOffsetIntervalIterator;
import io.questdb.cairo.mv.TimeZoneIntervalIterator;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.std.datetime.microtime.TimeZoneRulesMicros;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SampleByIntervalIteratorBenchmark {
    private final FixedOffsetIntervalIterator fixedOffsetIterator = new FixedOffsetIntervalIterator();
    private final TimeZoneIntervalIterator tzIterator = new TimeZoneIntervalIterator();
    @Param({"1", "15", "30", "60"})
    private int step;

    public SampleByIntervalIteratorBenchmark() {
        try {
            final TimestampSampler sampler = TimestampSamplerFactory.getInstance(1, 'm', 0);
            final TimeZoneRulesMicros rules = new TimeZoneRulesMicros(ZoneId.of("Europe/Berlin").getRules());

            final long minTs = TimestampFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            final long maxTs = TimestampFormatUtils.parseTimestamp("2030-01-01T00:00:00.000000Z");

            fixedOffsetIterator.of(
                    sampler,
                    2 * Timestamps.HOUR_MICROS,
                    null,
                    minTs,
                    maxTs,
                    step
            );
            tzIterator.of(
                    sampler,
                    rules,
                    0,
                    null,
                    minTs,
                    maxTs,
                    step
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(SampleByIntervalIteratorBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public long testFixedOffsetIterator() {
        fixedOffsetIterator.toTop(step);
        long s = 0;
        while (fixedOffsetIterator.next()) {
            s += fixedOffsetIterator.getTimestampLo();
            s += fixedOffsetIterator.getTimestampHi();
        }
        return s;
    }

    @Benchmark
    public long testTimeZoneIterator() {
        tzIterator.toTop(step);
        long s = 0;
        while (tzIterator.next()) {
            s += tzIterator.getTimestampLo();
            s += tzIterator.getTimestampHi();
        }
        return s;
    }
}
