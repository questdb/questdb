/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUnboundedByteSink;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class LineTcpParserBenchmark {

    private static final long BUFFER_SIZE = 32768;

    private final LineTcpParser parser;
    private final long bufLo;
    private final long bufHi;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(LineTcpParserBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(3)
                // Uncomment to collect a flame graph via async-profiler:
                //.addProfiler(AsyncProfiler.class, "output=flamegraph")
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    public LineTcpParserBenchmark() {
        this.bufLo = Unsafe.malloc(BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        this.parser = new LineTcpParser(false, false);

        Rnd rnd = new Rnd();
        long lineLenEstimate = 0;
        DirectUnboundedByteSink sink = new DirectUnboundedByteSink();
        sink.of(bufLo);
        while (sink.length() < (BUFFER_SIZE - lineLenEstimate)) {
            sink.put("cpu")
                    .put(",hostname=host_").put(String.valueOf(rnd.nextInt(1000)))
                    .put(",region=central_").put(rnd.nextString(32))
                    .put(",rack=").put(String.valueOf(rnd.nextInt(16)))
                    .put(",os=").put(rnd.nextString(3))
                    .put(",arch=").put(rnd.nextString(3))
                    .put(",team=").put(rnd.nextString(3))
                    .put(",service=").put(String.valueOf(rnd.nextInt(100)))
                    .put(",service_version=").put(String.valueOf(rnd.nextInt(10)))
                    .put(",service_environment=").put(rnd.nextString(5))
                    .put(" ")
                    .put("usage_user=").put(String.valueOf(rnd.nextInt(100))).put("i")
                    .put(",usage_system=").put(String.valueOf(rnd.nextInt(100))).put("i")
                    .put(",usage_idle=").put(String.valueOf(rnd.nextInt(100))).put("i")
                    .put(",usage_nice=").put(String.valueOf(rnd.nextInt(100))).put("i")
                    .put(",usage_iowait=").put(String.valueOf(rnd.nextInt(100))).put("i")
                    .put(",usage_irq=").put(String.valueOf(rnd.nextInt(100))).put("i")
                    .put(",usage_softirq=").put(String.valueOf(rnd.nextInt(100))).put("i")
                    .put(",usage_steal=").put(String.valueOf(rnd.nextInt(100))).put("i")
                    .put(",usage_guest=").put(String.valueOf(rnd.nextInt(100))).put("i")
                    .put(",usage_guest_nice=").put(String.valueOf(rnd.nextInt(100))).put("i")
                    .put(" 1451606400000000000\n");
            if (lineLenEstimate == 0) {
                lineLenEstimate = 3L * sink.length();
            }
        }

        this.bufHi = bufLo + sink.length();
    }

    @Benchmark
    public void testParse(Blackhole bh) {
        long bufPos = bufLo;
        while (bufPos < bufHi) {
            parser.of(bufPos);
            if (parser.parseMeasurement(bufHi) != LineTcpParser.ParseResult.MEASUREMENT_COMPLETE) {
                break;
            }
            bh.consume(parser.getMeasurementName());
            bh.consume(parser.getTimestamp());
            for (int nEntity = 0, n = parser.getEntityCount(); nEntity < n; nEntity++) {
                LineTcpParser.ProtoEntity entity = parser.getEntity(nEntity);
                byte entityType = entity.getType();
                switch (entityType) {
                    case LineTcpParser.ENTITY_TYPE_TAG:
                        bh.consume(entity.getValue());
                        break;
                    case LineTcpParser.ENTITY_TYPE_INTEGER:
                        bh.consume(entity.getLongValue());
                        break;
                    default:
                        break;
                }
            }
            bufPos = parser.getBufferAddress();
        }
    }
}
