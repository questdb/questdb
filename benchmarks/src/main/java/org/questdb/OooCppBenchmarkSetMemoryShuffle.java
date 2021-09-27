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

import io.questdb.std.*;

public class OooCppBenchmarkSetMemoryShuffle {
    private static final long BUFFER_MAX_SIZE = 256 * 1024 * 1024L;
    private static final long MB = 1024 * 1024L;

    public static void main(String[] args){

        Os.init();
        System.out.println("MemoryShuffle");
        testSetMemoryShuffleToCsv();
    }

    private static void testSetMemoryShuffleToCsv() {
        long index = Unsafe.getUnsafe().allocateMemory(BUFFER_MAX_SIZE * 2);
        long src = Unsafe.getUnsafe().allocateMemory(BUFFER_MAX_SIZE);
        long dest = Unsafe.getUnsafe().allocateMemory(BUFFER_MAX_SIZE);

        System.out.printf("src=%d, dest=%d, index=%d\n%n", src, dest, index);
        Rnd random = new Rnd();
        long size = BUFFER_MAX_SIZE / Long.BYTES;
        for (int i = 0; i < size; i++) {
            Unsafe.getUnsafe().putLong(index + (i + 1L) * Long.BYTES, Math.abs(random.nextLong()) %  size);
        }

        try {
            // warmup
            Vect.indexReshuffle64Bit(src, dest, index, size);

            int iterations = 100;
            for (int i = 1; i < 50; i += 1) {
                double timeout1 = runReshufle64(iterations, i, index, src, dest);
                double timeout2 = runReshufle32(iterations, i, index, src, dest);
                double timeout3 = runReshufle16(iterations, i, index, src, dest);
                System.out.println("" + i + ", " + timeout1 + ", " + timeout2 + ", " + timeout3);
            }
        } finally {
            Unsafe.free(index, BUFFER_MAX_SIZE * 2, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(src, BUFFER_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(dest, BUFFER_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static double runReshufle64(int iterations, int mb, long index, long src, long dest) {
        long nt = System.nanoTime();
        long size = MB * mb / 8;

        for (int j = 0; j < iterations; j++) {
            Vect.indexReshuffle64Bit(src, dest, index, size);
        }
        long timeout = System.nanoTime() - nt;
        return Math.round(timeout * 1E-1 / iterations) / 100.0;
    }

    private static double runReshufle32(int iterations, int mb, long index, long src, long dest) {
        long nt = System.nanoTime();
        long size = MB * mb / 4;

        for (int j = 0; j < iterations; j++) {
            Vect.indexReshuffle32Bit(src, dest, index, size);
        }
        long timeout = System.nanoTime() - nt;
        return Math.round(timeout * 1E-1 / iterations) / 100.0;
    }

    private static double runReshufle16(int iterations, int mb, long index, long src, long dest) {
        long nt = System.nanoTime();
        long size = MB * mb / 2;

        for (int j = 0; j < iterations; j++) {
            Vect.indexReshuffle16Bit(src, dest, index, size);
        }
        long timeout = System.nanoTime() - nt;
        return Math.round(timeout * 1E-1 / iterations) / 100.0;
    }
}
