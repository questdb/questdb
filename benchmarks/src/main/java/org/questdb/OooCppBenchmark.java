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

import io.questdb.griffin.SqlException;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class OooCppBenchmark {
    private static final long BUFFER_MAX_SIZE = 256 * 1024 * 1024L;
    private static long buffer;
    private static final long MB = 1024 * 1024L;

    public static void main(String[] args) throws RunnerException, SqlException, IOException {

        Os.init();
        System.out.println("MemorySuffle");
        testSetMemoryShuffleToCsv();

//        System.out.println("SetMemoryVanilla");
//        testSetMemoryVanillaToCsv();
    }


    private static void testSetMemoryShuffleToCsv() {
        var r = new OooCppBenchmark();
        long index = Unsafe.getUnsafe().allocateMemory(BUFFER_MAX_SIZE * 2);
        long src = Unsafe.getUnsafe().allocateMemory(BUFFER_MAX_SIZE);
        long dest = Unsafe.getUnsafe().allocateMemory(BUFFER_MAX_SIZE);

        System.out.println(String.format("src=%d, dest=%d, index=%d\n", src, dest, index));
        Rnd random = new Rnd();
        long size = BUFFER_MAX_SIZE / Long.BYTES;
        for (int i = 0; i < size; i++) {
            Unsafe.getUnsafe().putLong(index + (i + 1) * Long.BYTES, Math.abs(random.nextLong()) %  size);
        }

        try {
            // warmup
            Vect.indexReshuffle64Bit(src, dest, index, size);

            int iterations = 100;
            for (int i = 1; i < 50; i += 1) {
                var timeout1 = runReshufle64(iterations, i, index, src, dest);
                var timeout2 = runReshufle32(iterations, i, index, src, dest);
                var timeout3 = runReshufle16(iterations, i, index, src, dest);
                System.out.println("" + i + ", " + timeout1 + ", " + timeout2 + ", " + timeout3);
            }
        } finally {
            Unsafe.free(index, BUFFER_MAX_SIZE * 2);
            Unsafe.free(src, BUFFER_MAX_SIZE);
            Unsafe.free(dest, BUFFER_MAX_SIZE);
        }
    }

    private static double runReshufle64(int iterations, int mb, long index, long src, long dest) {
        var nt = System.nanoTime();
        long size = MB * mb / 8;

        for (int j = 0; j < iterations; j++) {
            Vect.indexReshuffle64Bit(src, dest, index, size);
        }
        var timeout = System.nanoTime() - nt;
        return Math.round(timeout * 1E-1 / iterations) / 100.0;
    }

    private static double runReshufle32(int iterations, int mb, long index, long src, long dest) {
        var nt = System.nanoTime();
        long size = MB * mb / 4;

        for (int j = 0; j < iterations; j++) {
            Vect.indexReshuffle32Bit(src, dest, index, size);
        }
        var timeout = System.nanoTime() - nt;
        return Math.round(timeout * 1E-1 / iterations) / 100.0;
    }

    private static double runReshufle16(int iterations, int mb, long index, long src, long dest) {
        var nt = System.nanoTime();
        long size = MB * mb / 2;

        for (int j = 0; j < iterations; j++) {
            Vect.indexReshuffle16Bit(src, dest, index, size);
        }
        var timeout = System.nanoTime() - nt;
        return Math.round(timeout * 1E-1 / iterations) / 100.0;
    }

    private static void testSetMemoryVanillaToCsv() {
        var r = new OooCppBenchmark();
        r.mallocBuffer();

        // warmup
        Vect.setMemoryDouble(
                buffer,
                -1L,
                1_000L * Double.BYTES
        );

        int iterations = 500;
        for (int i = 1; i < 50; i += 1) {
            var timeout1 = runDoubleKs(iterations, i);
            var timeout2 = runLongsKs(iterations, i, Long.MIN_VALUE);
            var timeout3 = runLongsKs(iterations, i, -1L);
            System.out.println("" + i + ", " + timeout1 + ", " + timeout2 + ", " + timeout3);
        }
        r.freeBuffer();
    }

    private static double runDoubleKs(int iterations, int i) {
        var nt = System.nanoTime();
        for (int j = 0; j < iterations; j++) {
            Vect.setMemoryDouble(
                    buffer,
                    -1.0,
                    i * MB / Double.BYTES
            );
        }
        var timeout = System.nanoTime() - nt;
        return Math.round(timeout * 1E-1 / iterations) / 100.0;
    }

    private static double runLongsKs(int iterations, int i, long value) {
        var nt = System.nanoTime();
        for (int j = 0; j < iterations; j++) {
            Vect.setMemoryLong(
                    buffer,
                    value,
                    i * MB / Long.BYTES
            );
        }
        var timeout = System.nanoTime() - nt;
        return Math.round(timeout * 1E-1 / iterations) / 100.0;
    }

    public void mallocBuffer() {
        buffer = Unsafe.getUnsafe().allocateMemory(BUFFER_MAX_SIZE);
    }

    public void freeBuffer() {
        Unsafe.free(buffer, BUFFER_MAX_SIZE);
    }
}
