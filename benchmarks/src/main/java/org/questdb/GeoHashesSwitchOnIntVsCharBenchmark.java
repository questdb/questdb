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

import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class GeoHashesSwitchOnIntVsCharBenchmark {

    @Benchmark
    public static void isValidBits0() {
        Rnd rnd = new Rnd();
        StringSink sink = Misc.getThreadLocalSink();
        for (int j = 0, m = 10_000_000; j < m; j++) {
            if (!isValidBits0(rnd_geobits(rnd, sink), 0)) {
                throw new AssertionError();
            }
        }
    }

    @Benchmark
    public static void isValidBits1() {
        Rnd rnd = new Rnd();
        StringSink sink = Misc.getThreadLocalSink();
        for (int j = 0, m = 10_000_000; j < m; j++) {
            if (!isValidBits1(rnd_geobits(rnd, sink), 0)) {
                throw new AssertionError();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new Runner(new OptionsBuilder()
                .include(GeoHashesSwitchOnIntVsCharBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(1)
                .build()).run();
    }

    private static boolean isValidBits0(CharSequence tok, int start) {
        int idx;
        for (int i = start, n = tok.length(); i < n; i++) {
            idx = tok.charAt(i);
            if (idx < 48 || idx > 49) { // '0', '1'
                return false;
            }
        }
        return true;
    }

    private static boolean isValidBits1(CharSequence tok, int start) { // slightly faster method
        int idx;
        for (int i = start, n = tok.length(); i < n; i++) {
            idx = tok.charAt(i);
            if (idx < '0' || idx > '1') {
                return false;
            }
        }
        return true;
    }

    private static CharSequence rnd_geobits(Rnd rnd, StringSink sink) {
        sink.clear();
        for (int bits = 1, limit = 1 + rnd.nextPositiveInt() % 61; bits < limit; bits++) {
            sink.put(rnd.nextBoolean() ? '1' : '0');
        }
        return sink.toString();
    }
}
