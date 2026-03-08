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

package org.questdb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class Log4jBenchmark {

    private static final Logger LOG = LogManager.getLogger(Log4jBenchmark.class);
    private static final boolean USE_ASYNC_LOGGER = false;
    private long counter = 0;

    public static void main(String[] args) throws RunnerException {
        ChainedOptionsBuilder builder = new OptionsBuilder()
                .include(Log4jBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .addProfiler("gc")
                .forks(1);

        if (USE_ASYNC_LOGGER) {
            builder = builder.jvmArgsAppend("-Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector", "-Dlog4j2.asyncLoggerWaitStrategy=Yield");
        }

        Options opt = builder.build();
        new Runner(opt).run();
    }

    @Benchmark
    public void testBaseline() {
    }

    @Benchmark
    public void testLogOneInt() {
        LOG.info("brown fox jumped over {} fence", counter++);
    }

    @Benchmark
    public void testLogOneIntDisabled() {
        LOG.debug("brown fox jumped over {} fence", counter++);
    }
}
