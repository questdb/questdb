/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This code is a port of xxHash algorithm by Yann Collet.
 *
 * Original code can be found here:
 *
 * https://code.google.com/p/lz4/source/browse/trunk/xxhash.c
 *
 */
package com.nfsdb.journal.utils;

public class Hash {

    private static final int PRIME32_1 = (int) 2654435761L;
    private static final int PRIME32_2 = (int) 2246822519L;
    private static final int PRIME32_3 = (int) 3266489917L;
    private static final int PRIME32_4 = (int) 668265263L;
    private static final int PRIME32_5 = (int) 374761393L;

    public static int rotl(int value, int n) {
        return (value >>> n) | (value << (32 - n));
    }

    public static int hashXX(MemoryBuffer data, int seed) {
        int i32;
        int p = 0;
        int len = data.length();

        if (len >= 16) {
            int limit = len - 16;
            int v1 = seed + PRIME32_1 + PRIME32_2;
            int v2 = seed + PRIME32_2;
            int v3 = seed;
            int v4 = seed - PRIME32_1;

            do {
                v1 += data.getInt(p) * PRIME32_2;
                v1 = rotl(v1, 13);
                v1 *= PRIME32_1;
                p += 4;
                v2 += data.getInt(p) * PRIME32_2;
                v2 = rotl(v2, 13);
                v2 *= PRIME32_1;
                p += 4;
                v3 += data.getInt(p) * PRIME32_2;
                v3 = rotl(v3, 13);
                v3 *= PRIME32_1;
                p += 4;

                int i = data.getInt(p);
                v4 += i * PRIME32_2;
                v4 = rotl(v4, 13);
                v4 *= PRIME32_1;
                p += 4;
            }
            while (p <= limit);

            i32 = rotl(v1, 1) + rotl(v2, 7) + rotl(v3, 12) + rotl(v4, 18);
        } else {
            i32 = seed + PRIME32_5;
        }

        i32 += len;

        while (p + 4 <= len) {
            i32 += data.getInt(p) * PRIME32_3;
            i32 = rotl(i32, 17) * PRIME32_4;
            p += 4;
        }

        while (p < len) {
            i32 += data.getByte(p) * PRIME32_5;
            i32 = rotl(i32, 11) * PRIME32_1;
            p++;
        }

        i32 ^= i32 >> 15;
        i32 *= PRIME32_2;
        i32 ^= i32 >> 13;
        i32 *= PRIME32_3;
        i32 ^= i32 >> 16;

        return i32;
    }

    public static int hashXX(long address, int len, int seed) {
        int i32;
        long p = address;

        if (len >= 16) {
            int limit = len - 16;
            int v1 = seed + PRIME32_1 + PRIME32_2;
            int v2 = seed + PRIME32_2;
            int v3 = seed;
            int v4 = seed - PRIME32_1;

            do {
                v1 += Unsafe.getUnsafe().getInt(p) * PRIME32_2;
                v1 = rotl(v1, 13);
                v1 *= PRIME32_1;
                p += 4;
                v2 += Unsafe.getUnsafe().getInt(p) * PRIME32_2;
                v2 = rotl(v2, 13);
                v2 *= PRIME32_1;
                p += 4;
                v3 += Unsafe.getUnsafe().getInt(p) * PRIME32_2;
                v3 = rotl(v3, 13);
                v3 *= PRIME32_1;
                p += 4;

                int i = Unsafe.getUnsafe().getInt(p);
                v4 += i * PRIME32_2;
                v4 = rotl(v4, 13);
                v4 *= PRIME32_1;
                p += 4;
            }
            while (p <= limit);

            i32 = rotl(v1, 1) + rotl(v2, 7) + rotl(v3, 12) + rotl(v4, 18);
        } else {
            i32 = seed + PRIME32_5;
        }

        i32 += len;

        while (p + 4 <= len) {
            i32 += Unsafe.getUnsafe().getInt(p) * PRIME32_3;
            i32 = rotl(i32, 17) * PRIME32_4;
            p += 4;
        }

        while (p < len) {
            i32 += Unsafe.getUnsafe().getByte(p) * PRIME32_5;
            i32 = rotl(i32, 11) * PRIME32_1;
            p++;
        }

        i32 ^= i32 >> 15;
        i32 *= PRIME32_2;
        i32 ^= i32 >> 13;
        i32 *= PRIME32_3;
        i32 ^= i32 >> 16;

        return i32;
    }
}
