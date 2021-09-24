/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cutlass.line.CachedCharSequence;
import io.questdb.cutlass.line.CharSequenceCache;
import io.questdb.cutlass.line.LineProtoLexer;
import io.questdb.cutlass.line.LineProtoParser;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CutlassLexerTest {

    private final static LineProtoLexer lpLexer = new LineProtoLexer(4096);
    private static final byte[] bytes = "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n".getBytes();
    private static final long mem = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CutlassLexerTest.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void baseline() {
        // do nothing, this is a baseline
    }

    @Benchmark
    public void testSelectColumns() {
        lpLexer.parse(mem, mem + bytes.length);
    }

    static {
        int len = bytes.length;
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(mem + i, bytes[i]);
        }
    }

    static {
        lpLexer.withParser(new LineProtoParser() {
            @Override
            public void onError(int position, int state, int code) {

            }

            @Override
            public void onEvent(CachedCharSequence token, int type, CharSequenceCache cache) {

            }

            @Override
            public void onLineEnd(CharSequenceCache cache) {

            }
        });
    }
}
