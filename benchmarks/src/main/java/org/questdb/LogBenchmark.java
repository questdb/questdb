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

import io.questdb.log.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class LogBenchmark {

    private static final Log LOG;
    private long counter = 0;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(LogBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .addProfiler("gc")
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void testLogOneInt() {
        LOG.info().$("brown fox jumped over ").$(counter).$(" fence").$();
    }

    @Benchmark
    public void testLogOneIntDisabled() {
        LOG.debug().$("brown fox jumped over ").$(counter).$(" fence").$();
    }

    @Benchmark
    public void testBaseline() {
    }

    static {
        LogFactory.INSTANCE.add(new LogWriterConfig(LogLevel.INFO, (queue, subSeq, level) -> {
            LogRollingFileWriter w = new LogRollingFileWriter(queue, subSeq, level);
            w.setLocation("log-bench1.log");
            return w;
        }));
        LogFactory.INSTANCE.bind();
        LogFactory.INSTANCE.startThread();

        LOG = LogFactory.getLog(LogBenchmark.class);
    }
}
