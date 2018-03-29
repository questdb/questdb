/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package org.questdb;

import com.questdb.cutlass.receiver.parser.CachedCharSequence;
import com.questdb.cutlass.receiver.parser.CharSequenceCache;
import com.questdb.cutlass.receiver.parser.LineProtoLexer;
import com.questdb.cutlass.receiver.parser.LineProtoParser;
import com.questdb.std.Unsafe;
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
    private static final long mem = Unsafe.malloc(bytes.length);

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
