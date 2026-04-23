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

package io.questdb.test.std;

import io.questdb.std.Files;
import io.questdb.std.Hash;
import io.questdb.std.LongHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.function.LongUnaryOperator;
import java.util.zip.ZipFile;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.assertEquals;

public class HashTest {

    @Test
    public void testHashInt64RandomDistribution() {
        final int N = 100_000;
        Rnd rnd = new Rnd();
        LongHashSet hashes = new LongHashSet(N);
        for (int i = 0; i < N; i++) {
            // Only the 32 LSBs matter - that's what fixed-size maps mask to.
            hashes.add((int) Hash.hashInt64(rnd.nextInt()));
        }
        Assert.assertTrue("hashInt64 distribution dropped", hashes.size() > 99_990);
    }

    @Test
    public void testHashLong128_64RandomDistribution() {
        final int N = 100_000;
        Rnd rnd = new Rnd();
        LongHashSet hashes = new LongHashSet(N);
        for (int i = 0; i < N; i++) {
            hashes.add((int) Hash.hashLong128_64(rnd.nextLong(), rnd.nextLong()));
        }
        Assert.assertTrue("hashLong128_64 distribution dropped", hashes.size() > 99_990);
    }

    @Test
    public void testHashLong256_64RandomDistribution() {
        final int N = 100_000;
        Rnd rnd = new Rnd();
        LongHashSet hashes = new LongHashSet(N);
        for (int i = 0; i < N; i++) {
            hashes.add((int) Hash.hashLong256_64(rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong()));
        }
        Assert.assertTrue("hashLong256_64 distribution dropped", hashes.size() > 99_990);
    }

    @Test
    public void testHashLong64RandomDistribution() {
        final int N = 100_000;
        Rnd rnd = new Rnd();
        LongHashSet hashes = new LongHashSet(N);
        for (int i = 0; i < N; i++) {
            hashes.add((int) Hash.hashLong64(rnd.nextLong()));
        }
        Assert.assertTrue("hashLong64 distribution dropped", hashes.size() > 99_990);
    }

    @Test
    public void testHashLong64SequentialShardDistribution() {
        assertShardBalance("sequential", Hash::hashLong64);
    }

    @Test
    public void testHashLong64ShiftedLow20ShardDistribution() {
        // FxHasher zeroed the low 20 bits on this pattern and collapsed all keys
        // into a single bucket. xxh3 must spread them across shards.
        assertShardBalance("i << 20", i -> Hash.hashLong64(i << 20));
    }

    @Test
    public void testHashLong64ShiftedLow40ShardDistribution() {
        assertShardBalance("i << 40", i -> Hash.hashLong64(i << 40));
    }

    @Test
    public void testHashMem64SequentialIntKeyShardDistribution() throws Exception {
        // 4-byte keys hit only the int-tail branch (no 8-byte polynomial chunk).
        assertMemShardBalance("hashMem64 4-byte sequential", 4, (addr, i) -> {
            Unsafe.getUnsafe().putInt(addr, i);
            return 4;
        });
    }

    @Test
    public void testHashMem64SequentialLongIntKeyShardDistribution() throws Exception {
        // 12-byte keys exercise one 8-byte polynomial iteration plus the 4-byte int tail.
        assertMemShardBalance("hashMem64 12-byte sequential", 12, (addr, i) -> {
            Unsafe.getUnsafe().putLong(addr, i);
            Unsafe.getUnsafe().putInt(addr + 8, ~i);
            return 12;
        });
    }

    @Test
    public void testHashMemEnglishWordsCorpus_hashMem64() throws IOException {
        testHashMemEnglishWordsCorpus(Hash::hashMem64);
    }

    @Test
    public void testHashMemRandomCorpus_hashMem64() {
        testHashMemRandomCorpus(Hash::hashMem64);
    }

    @Test
    public void testMurmur3ToLongForIntKey() {
        // The expected values have been obtained from the original implementation of MurmurHash3,
        // available at: https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
        assertEquals(-1633616987925480281L, Hash.murmur3ToLong(-1205269188));
        assertEquals(-5166452714297686332L, Hash.murmur3ToLong(287961467));
        assertEquals(370098364460170807L, Hash.murmur3ToLong(43976175));
        assertEquals(337284429664094377L, Hash.murmur3ToLong(1071024900));
        assertEquals(-7391378269516181578L, Hash.murmur3ToLong(-46715208));
    }

    @Test
    public void testMurmur3ToLongForLongKey() {
        // The expected values have been obtained from the original implementation of MurmurHash3,
        // available at: https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
        assertEquals(1586830184839932339L, Hash.murmur3ToLong(2769845405872435875L));
        assertEquals(5667271150751524839L, Hash.murmur3ToLong(8467001914150166941L));
        assertEquals(-6164039929522353948L, Hash.murmur3ToLong(3116016319545714670L));
        assertEquals(5404083732375145584L, Hash.murmur3ToLong(-3505607450965693221L));
        assertEquals(-2748674767479114199L, Hash.murmur3ToLong(-1442442454180049685L));
    }

    private static void assertMemShardBalance(String desc, int maxKeyLen, MemKeyEncoder encoder) throws Exception {
        assertMemoryLeak(() -> {
            final int N = 100_000;
            final int shards = 256;
            final int mask = shards - 1;
            final int[] buckets = new int[shards];
            long addr = Unsafe.malloc(maxKeyLen, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < N; i++) {
                    int len = encoder.encode(addr, i);
                    buckets[(int) (Hash.hashMem64(addr, len) & mask)]++;
                }
            } finally {
                Unsafe.free(addr, maxKeyLen, MemoryTag.NATIVE_DEFAULT);
            }
            assertShardsBalanced(desc, buckets, N);
        });
    }

    private static void assertShardBalance(String desc, LongUnaryOperator hasher) {
        final int N = 100_000;
        final int shards = 256;
        final int mask = shards - 1;
        final int[] buckets = new int[shards];
        for (int i = 0; i < N; i++) {
            buckets[(int) (hasher.applyAsLong(i) & mask)]++;
        }
        assertShardsBalanced(desc, buckets, N);
    }

    private static void assertShardsBalanced(String desc, int[] buckets, int N) {
        final int expected = N / buckets.length;
        final int tolerance = expected / 3;
        for (int s = 0; s < buckets.length; s++) {
            int c = buckets[s];
            Assert.assertTrue(
                    desc + ": bucket " + s + " imbalance, got " + c + " expected ~" + expected,
                    Math.abs(c - expected) < tolerance
            );
        }
    }

    private void testHashMemEnglishWordsCorpus(HashFunction hashFunction) throws IOException {
        final int maxLen = 128;
        LongHashSet hashes = new LongHashSet(500000);

        String file = Files.getResourcePath(getClass().getResource("/hash/words.zip"));
        long address = Unsafe.malloc(maxLen, MemoryTag.NATIVE_DEFAULT);
        try (
                ZipFile zipFile = new ZipFile(file);
                InputStream input = zipFile.getInputStream(zipFile.entries().nextElement());
                BufferedReader br = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))
        ) {
            String line;
            while ((line = br.readLine()) != null) {
                byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
                for (int i = 0; i < bytes.length; i++) {
                    Unsafe.getUnsafe().putByte(address + i, bytes[i]);
                }
                // Use only 32 LSBs for the unique value check since that's where we want entropy.
                hashes.add((int) hashFunction.hash(address, bytes.length));
            }
            // 466189 is the number of unique values of String#hashCode() on the same corpus.
            Assert.assertTrue("hash function distribution on English words corpus dropped", hashes.size() >= 466189);
        } finally {
            Unsafe.free(address, maxLen, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void testHashMemRandomCorpus(HashFunction hashFunction) {
        final int len = 15;
        Rnd rnd = new Rnd();
        LongHashSet hashes = new LongHashSet(100000);

        long address = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < 100000; i++) {
                rnd.nextChars(address, len / 2);
                // Use only 32 LSBs for the unique value check since that's where we want entropy.
                hashes.add((int) hashFunction.hash(address, len));
            }
            Assert.assertTrue("Hash function distribution dropped", hashes.size() > 99990);
        } finally {
            Unsafe.free(address, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private interface HashFunction {
        long hash(long p, long len);
    }

    @FunctionalInterface
    private interface MemKeyEncoder {
        // Writes the i-th key starting at addr and returns the key length in bytes.
        int encode(long addr, int i);
    }
}
