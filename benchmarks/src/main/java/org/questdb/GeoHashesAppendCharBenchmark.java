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

import io.questdb.cairo.GeoHashes;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class GeoHashesAppendCharBenchmark {

    private static final char[] base32 = {
            '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'b', 'c', 'd', 'e', 'f', 'g',
            'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
            's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
    };
    private static final byte[] base32Indexes = {
            0, 1, 2, 3, 4, 5, 6, 7,         // 30-37, '0'..'7'
            8, 9, -1, -1, -1, -1, -1, -1,   // 38-2F, '8','9'
            -1, -1, 10, 11, 12, 13, 14, 15, // 40-47, 'B'..'G'
            16, -1, 17, 18, -1, 19, 20, -1, // 48-4F, 'H','J','K','M','N'
            21, 22, 23, 24, 25, 26, 27, 28, // 50-57, 'P'..'W'
            29, 30, 31, -1, -1, -1, -1, -1, // 58-5F, 'X'..'Z'
            -1, -1, 10, 11, 12, 13, 14, 15, // 60-67, 'b'..'g'
            16, -1, 17, 18, -1, 19, 20, -1, // 68-6F, 'h','j','k','m','n'
            21, 22, 23, 24, 25, 26, 27, 28, // 70-77, 'p'..'w'
            29, 30, 31                      // 78-7A, 'x'..'z'
    };

    public static void main(String[] args) throws Exception {
        new Runner(new OptionsBuilder()
                .include(GeoHashesAppendCharBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(1)
                .build()).run();
    }

    @Benchmark
    public void appendChar0() throws NumericException {
        Rnd rnd = new Rnd();
        long geohash = 0;
        for (int j = 0, m = 12; j < m; j++) {
            geohash = appendChar0(geohash, rnd_geochar(rnd));
        }
        if (geohash != 592262567632380556L) {
            throw new AssertionError();
        }
    }

    @Benchmark
    public void appendChar1() throws NumericException {
        Rnd rnd = new Rnd();
        long geohash = 0;
        for (int j = 0, m = 12; j < m; j++) {
            geohash = appendChar1(geohash, rnd_geochar(rnd));
        }
        if (geohash != 592262567632380556L) {
            throw new AssertionError();
        }
    }

    @Benchmark
    public void appendChar2() throws NumericException {
        Rnd rnd = new Rnd();
        long geohash = 0;
        for (int j = 0, m = 12; j < m; j++) {
            geohash = appendChar2(geohash, rnd_geochar(rnd));
        }
        if (geohash != 592262567632380556L) {
            throw new AssertionError();
        }
    }

    private static long appendChar0(long geohash, char c) throws NumericException { // faster execution
        byte idx;
        if (c >= 48 && c < 123) { // 123 = base32Indexes.length + 48
            idx = base32Indexes[c - 48];
            if (idx >= 0) {
                return (geohash << 5) | idx;
            }
        }
        throw NumericException.INSTANCE;
    }

    private static long appendChar1(long geohash, char c) throws NumericException {
        int idx = c - 48;
        if (idx >= 0 && idx < 75) { // base32Indexes.length
            idx = base32Indexes[idx];
            if (idx >= 0) {
                return (geohash << 5) | idx;
            }
        }
        throw NumericException.INSTANCE;
    }

    private static long appendChar2(long geohash, char c) throws NumericException { // faster execution
        byte idx;
        idx = GeoHashes.encodeChar(c);
        if (idx > -1) {
            return (geohash << 5) | idx;
        }
        throw NumericException.INSTANCE;
    }

    private static char rnd_geochar(Rnd rnd) {
        return base32[rnd.nextPositiveInt() % base32.length];
    }
}
