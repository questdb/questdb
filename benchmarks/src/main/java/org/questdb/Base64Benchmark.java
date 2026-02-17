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

import io.questdb.std.Chars;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Threads(1)
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class Base64Benchmark {

    @Param({"16", "512", "65536"})
    public int size;
    private ByteBuffer bb;
    private String data;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Base64Benchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .addProfiler("gc")
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public byte[] jdkBaseline() {
        return Base64.getDecoder().decode(data);
    }

    @Setup
    public void prepareData() {
        Random random = new Random();

        byte[] b = new byte[size];
        random.nextBytes(b);
        data = Base64.getEncoder().encodeToString(b);
        bb = ByteBuffer.allocate(size);
    }

    @Benchmark
    public ByteBuffer questDB() {
        bb.clear();
        Chars.base64Decode(data, bb);
        return bb;
    }

}
