/*+*****************************************************************************
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

import io.questdb.std.Hash;
import io.questdb.std.Rnd;

import java.util.function.LongUnaryOperator;

/**
 * Compares hash quality of fmix64 (Hash.hashLong64), hashLong64Simd, and hashInt64Simd
 * via avalanche, distribution, and collision metrics.
 */
public class HashFunctionQualityComparison {

    public static void main(String[] args) {
        System.out.println("Hash function quality comparison");
        System.out.println("==================================\n");

        LongUnaryOperator fmix64 = Hash::hashLong64;
        LongUnaryOperator simdLong = Hash::hashLong64Simd;
        LongUnaryOperator simdInt = k -> Hash.hashInt64Simd((int) k);

        runAvalancheTest("fmix64       ", fmix64, false);
        runAvalancheTest("hashLong64Simd", simdLong, false);
        runAvalancheTest("hashInt64Simd ", simdInt, true); // 32-bit input
        System.out.println();

        // Distribution: random 64-bit keys, hash table sizes 256/4K/64K/1M.
        System.out.println("Distribution (chi-squared, lower=better, ideal ~ slots-1):");
        int[] slotCounts = {256, 4096, 65536, 1024 * 1024};
        for (int slots : slotCounts) {
            System.out.printf("  slots=%-7d (random 64-bit keys, %d samples)%n", slots, slots * 16);
            runDistributionTest("    fmix64       ", fmix64, slots, slots * 16, false);
            runDistributionTest("    hashLong64Simd", simdLong, slots, slots * 16, false);
            runDistributionTest("    hashInt64Simd ", simdInt, slots, slots * 16, true);
        }
        System.out.println();

        // Sequential keys: 1..N. Tests how well hashes spread tight ranges.
        System.out.println("Sequential keys 1..N (chi-squared, lower=better):");
        for (int slots : slotCounts) {
            System.out.printf("  slots=%-7d (sequential keys 1..%d)%n", slots, slots * 16);
            runSequentialTest("    fmix64       ", fmix64, slots, slots * 16);
            runSequentialTest("    hashLong64Simd", simdLong, slots, slots * 16);
            runSequentialTest("    hashInt64Simd ", simdInt, slots, slots * 16);
        }
        System.out.println();

        // Sharding: top 8 bits → 256 shards.
        System.out.println("Shard distribution (top 8 bits, chi-squared, ideal ~255):");
        runShardTest("  fmix64       ", fmix64, false);
        runShardTest("  hashLong64Simd", simdLong, false);
        runShardTest("  hashInt64Simd ", simdInt, true);
        runShardTestSequential("  fmix64        (seq)", fmix64);
        runShardTestSequential("  hashLong64Simd (seq)", simdLong);
        runShardTestSequential("  hashInt64Simd  (seq)", simdInt);
    }

    /**
     * Avalanche test: for each input bit, flip it and count how many output bits change.
     * Average across many random keys; ideal ratio is 0.5 per bit pair.
     * The reported "max bias" is the largest deviation from 0.5 observed across all
     * (input_bit, output_bit) pairs (smaller is better).
     */
    private static void runAvalancheTest(String name, LongUnaryOperator hashFn, boolean input32) {
        final int trials = 200_000;
        final int inputBits = input32 ? 32 : 64;
        final int outputBits = 64;
        final int[][] flips = new int[inputBits][outputBits];
        final Rnd rnd = new Rnd();

        for (int t = 0; t < trials; t++) {
            long k = input32 ? (rnd.nextInt() & 0xFFFFFFFFL) : rnd.nextLong();
            long h = hashFn.applyAsLong(k);
            for (int b = 0; b < inputBits; b++) {
                long kFlipped = k ^ (1L << b);
                long hFlipped = hashFn.applyAsLong(kFlipped);
                long diff = h ^ hFlipped;
                for (int ob = 0; ob < outputBits; ob++) {
                    if (((diff >>> ob) & 1L) != 0) {
                        flips[b][ob]++;
                    }
                }
            }
        }

        double maxBias = 0;
        double avgBias = 0;
        int pairs = 0;
        for (int b = 0; b < inputBits; b++) {
            for (int ob = 0; ob < outputBits; ob++) {
                double ratio = flips[b][ob] / (double) trials;
                double bias = Math.abs(ratio - 0.5);
                maxBias = Math.max(maxBias, bias);
                avgBias += bias;
                pairs++;
            }
        }
        avgBias /= pairs;
        System.out.printf("Avalanche %s: avgBias=%.5f maxBias=%.5f (ideal: 0.0)%n",
                name, avgBias, maxBias);
    }

    /**
     * Distribution test: hash N random keys into M slots. Returns chi-squared.
     */
    private static void runDistributionTest(String name, LongUnaryOperator hashFn, int slots, int samples, boolean input32) {
        long[] buckets = new long[slots];
        long mask = slots - 1;
        Rnd rnd = new Rnd();
        for (int i = 0; i < samples; i++) {
            long k = input32 ? (rnd.nextInt() & 0xFFFFFFFFL) : rnd.nextLong();
            buckets[(int) (hashFn.applyAsLong(k) & mask)]++;
        }
        double chi = chiSquared(buckets, samples);
        System.out.printf("%s chi=%-12.2f (ideal ~%d)%n", name, chi, slots - 1);
    }

    private static void runSequentialTest(String name, LongUnaryOperator hashFn, int slots, int samples) {
        long[] buckets = new long[slots];
        long mask = slots - 1;
        for (int i = 1; i <= samples; i++) {
            buckets[(int) (hashFn.applyAsLong(i) & mask)]++;
        }
        double chi = chiSquared(buckets, samples);
        System.out.printf("%s chi=%-12.2f (ideal ~%d)%n", name, chi, slots - 1);
    }

    private static void runShardTest(String name, LongUnaryOperator hashFn, boolean input32) {
        long[] shards = new long[256];
        int samples = 1_000_000;
        Rnd rnd = new Rnd();
        for (int i = 0; i < samples; i++) {
            long k = input32 ? (rnd.nextInt() & 0xFFFFFFFFL) : rnd.nextLong();
            shards[(int) (hashFn.applyAsLong(k) >>> 56)]++;
        }
        double chi = chiSquared(shards, samples);
        System.out.printf("%s chi=%-12.2f (ideal ~255)%n", name, chi);
    }

    private static void runShardTestSequential(String name, LongUnaryOperator hashFn) {
        long[] shards = new long[256];
        int samples = 1_000_000;
        for (int i = 1; i <= samples; i++) {
            shards[(int) (hashFn.applyAsLong(i) >>> 56)]++;
        }
        double chi = chiSquared(shards, samples);
        System.out.printf("%s chi=%-12.2f (ideal ~255)%n", name, chi);
    }

    private static double chiSquared(long[] buckets, long samples) {
        double expected = samples / (double) buckets.length;
        double chi = 0;
        for (long count : buckets) {
            double diff = count - expected;
            chi += (diff * diff) / expected;
        }
        return chi;
    }
}
