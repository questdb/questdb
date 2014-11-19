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

package com.nfsdb.journal.utils;

public class Rnd {
    private long s0;
    private long s1;

    public Rnd(long s0, long s1) {
        this.s0 = s0;
        this.s1 = s1;
    }

    public Rnd() {
        this.s0 = 0xdeadbeef;
        this.s1 = 0xdee4c0ed;
    }

    public long nextLong() {
        long l1 = s0;
        long l0 = s1;
        s0 = l0;
        l1 ^= l1 << 23;
        return (s1 = l1 ^ l0 ^ (l1 >> 17) ^ (l0 >> 26)) + l0;
    }

    public long nextLong(int bits) {
        return nextLong() >>> (64 - bits);
    }

    public double nextDouble() {
        return (nextLong(26) << 27 + nextLong(27)) / (double) (1L << 53);
    }

    public int nextInt() {
        return (int) nextLong();
    }

    public int nextPositiveInt() {
        int n = (int) nextLong();
        return n > 0 ? n : -n;
    }

    public String nextString(int len) {
        char chars[] = new char[len];
        for (int i = 0; i < len; i++) {
            chars[i] = (char) (nextPositiveInt() % 25 + 66);
        }
        return new String(chars);
    }

    public byte[] nextBytes(int len) {
        byte bytes[] = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) (nextPositiveInt() % 25 + 66);
        }
        return bytes;
    }

    public boolean nextBoolean() {
        return nextLong(1) != 0;
    }
}
