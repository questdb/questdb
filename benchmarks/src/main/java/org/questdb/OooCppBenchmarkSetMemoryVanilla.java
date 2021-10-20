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

import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public class OooCppBenchmarkSetMemoryVanilla {
    private static final long BUFFER_MAX_SIZE = 256 * 1024 * 1024L;
    private static long buffer;
    private static final long MB = 1024 * 1024L;

    public static void main(String[] args){
        Os.init();
        System.out.println("SetMemoryVanilla");
        testSetMemoryVanillaToCsv();
    }

    private static void testSetMemoryVanillaToCsv() {
        OooCppBenchmarkSetMemoryVanilla r = new OooCppBenchmarkSetMemoryVanilla();
        r.mallocBuffer();

        // warmup
        Vect.setMemoryDouble(
                buffer,
                -1L,
                1_000L * Double.BYTES
        );

        int iterations = 500;
        for (int i = 1; i < 50; i += 1) {
            double timeout1 = runDoubleKs(iterations, i);
            double timeout2 = runLongsKs(iterations, i, Long.MIN_VALUE);
            double timeout3 = runLongsKs(iterations, i, -1L);
            System.out.println("" + i + ", " + timeout1 + ", " + timeout2 + ", " + timeout3);
        }
        r.freeBuffer();
    }

    private static double runDoubleKs(int iterations, int i) {
        long nt = System.nanoTime();
        for (int j = 0; j < iterations; j++) {
            Vect.setMemoryDouble(
                    buffer,
                    -1.0,
                    i * MB / Double.BYTES
            );
        }
        long timeout = System.nanoTime() - nt;
        return Math.round(timeout * 1E-1 / iterations) / 100.0;
    }

    private static double runLongsKs(int iterations, int i, long value) {
        long nt = System.nanoTime();
        for (int j = 0; j < iterations; j++) {
            Vect.setMemoryLong(
                    buffer,
                    value,
                    i * MB / Long.BYTES
            );
        }
        long timeout = System.nanoTime() - nt;
        return Math.round(timeout * 1E-1 / iterations) / 100.0;
    }

    public void mallocBuffer() {
        buffer = Unsafe.getUnsafe().allocateMemory(BUFFER_MAX_SIZE);
    }

    public void freeBuffer() {
        Unsafe.free(buffer, BUFFER_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
    }
}
