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

package io.questdb.test.cairo.map;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.MapBatchProber;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.map.Unordered4Map;
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.cairo.map.UnorderedVarcharMap;
import io.questdb.std.Hash;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.AsciiCharSequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

public class MapBatchProberTest extends AbstractCairoTest {

    @Test
    public void testOrderedMapBatchInsertOneByOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.LONG);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    OrderedMap map = new OrderedMap(256, keyTypes, valueTypes, 16, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(1)
            ) {
                Assert.assertNotNull(prober);
                // Insert 20 unique keys, then 20 duplicates — forces rehash
                for (int round = 0; round < 2; round++) {
                    for (int k = 0; k < 20; k++) {
                        prober.resetBatch();
                        prober.putLong(k);
                        prober.computeHashes(1);
                        MapValue value = prober.probeWithHash(0);
                        if (value.isNew()) {
                            Assert.assertEquals("round " + round + " key " + k, 0, round);
                            value.putLong(0, 1);
                        } else {
                            Assert.assertEquals("round " + round + " key " + k, 1, round);
                            value.addLong(0, 1);
                        }
                    }
                }

                Assert.assertEquals(20, map.size());
                for (int k = 0; k < 20; k++) {
                    MapKey key = map.withKey();
                    key.putLong(k);
                    MapValue v = key.findValue();
                    Assert.assertNotNull("key not found: " + k, v);
                    Assert.assertEquals("value mismatch for key " + k, 2, v.getLong(0));
                }
            }
        });
    }

    @Test
    public void testOrderedMapBatchVsPerKey() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.LONG);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    OrderedMap batchMap = new OrderedMap(4096, keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    OrderedMap perKeyMap = new OrderedMap(4096, keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = batchMap.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                final int N = 10_000;
                long[] keys = new long[N];
                for (int i = 0; i < N; i++) {
                    keys[i] = rnd.nextLong() % 5_000; // some duplicates
                }

                // Populate perKeyMap using standard API.
                for (int i = 0; i < N; i++) {
                    MapKey key = perKeyMap.withKey();
                    key.putLong(keys[i]);
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        value.putLong(0, 1);
                    } else {
                        value.addLong(0, 1);
                    }
                }

                // Populate batchMap using batch prober.
                for (int offset = 0; offset < N; offset += 256) {
                    int batchSize = Math.min(256, N - offset);
                    prober.resetBatch();
                    for (int i = 0; i < batchSize; i++) {
                        prober.putLong(keys[offset + i]);
                    }
                    prober.computeHashes(batchSize);
                    for (int i = 0; i < batchSize; i++) {
                        MapValue value = prober.probeWithHash(i);
                        if (value.isNew()) {
                            value.putLong(0, 1);
                        } else {
                            value.addLong(0, 1);
                        }
                    }
                }

                Assert.assertEquals(perKeyMap.size(), batchMap.size());
                assertOrderedMapsEqual(keys, perKeyMap, batchMap);
            }
        });
    }

    @Test
    public void testOrderedMapBatchVsPerKeySmall() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.LONG);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    OrderedMap batchMap = new OrderedMap(4096, keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    OrderedMap perKeyMap = new OrderedMap(4096, keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = batchMap.createBatchProber(4)
            ) {
                Assert.assertNotNull(prober);
                long[] keys = {1, 2, 1, 2};

                // Per-key
                for (long k : keys) {
                    MapKey key = perKeyMap.withKey();
                    key.putLong(k);
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        value.putLong(0, 1);
                    } else {
                        value.addLong(0, 1);
                    }
                }

                // Batch
                prober.resetBatch();
                for (long k : keys) {
                    prober.putLong(k);
                }
                prober.computeHashes(keys.length);
                for (int i = 0; i < keys.length; i++) {
                    MapValue value = prober.probeWithHash(i);
                    if (value.isNew()) {
                        value.putLong(0, 1);
                    } else {
                        value.addLong(0, 1);
                    }
                }

                Assert.assertEquals(2, batchMap.size());
                Assert.assertEquals(perKeyMap.size(), batchMap.size());

                MapKey k1 = batchMap.withKey();
                k1.putLong(1);
                Assert.assertEquals(2, k1.findValue().getLong(0));

                MapKey k2 = batchMap.withKey();
                k2.putLong(2);
                Assert.assertEquals(2, k2.findValue().getLong(0));
            }
        });
    }

    @Test
    public void testOrderedMapBatchVsPerKeyWithRehash() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.LONG);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            // Small capacity to force rehash.
            try (
                    OrderedMap batchMap = new OrderedMap(256, keyTypes, valueTypes, 16, 0.8, Integer.MAX_VALUE);
                    OrderedMap perKeyMap = new OrderedMap(256, keyTypes, valueTypes, 16, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = batchMap.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                // 100 unique keys in a batch of 256 (many duplicates), forces rehash
                long[] keys = new long[256];
                for (int i = 0; i < keys.length; i++) {
                    keys[i] = i % 100;
                }

                // Per-key
                for (long k : keys) {
                    MapKey key = perKeyMap.withKey();
                    key.putLong(k);
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        value.putLong(0, 1);
                    } else {
                        value.addLong(0, 1);
                    }
                }

                // Batch
                prober.resetBatch();
                for (long k : keys) {
                    prober.putLong(k);
                }
                prober.computeHashes(keys.length);
                for (int i = 0; i < keys.length; i++) {
                    MapValue value = prober.probeWithHash(i);
                    if (value.isNew()) {
                        value.putLong(0, 1);
                    } else {
                        value.addLong(0, 1);
                    }
                }

                Assert.assertEquals(perKeyMap.size(), batchMap.size());
                assertOrderedMapsEqual(keys, perKeyMap, batchMap);
            }
        });
    }

    @Test
    public void testOrderedMapBatchWithRehashDuringProbe() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.LONG);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            // capacity 16, loadFactor 0.8 → 12 free slots. Batch of 20 unique keys forces rehash mid-batch.
            try (
                    OrderedMap map = new OrderedMap(256, keyTypes, valueTypes, 16, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                // 20 unique keys + 20 duplicates in one batch
                int batchSize = 40;
                prober.resetBatch();
                for (int i = 0; i < batchSize; i++) {
                    prober.putLong(i % 20);
                }
                prober.computeHashes(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    MapValue value = prober.probeWithHash(i);
                    if (value.isNew()) {
                        value.putLong(0, 1);
                    } else {
                        value.addLong(0, 1);
                    }
                }

                Assert.assertEquals(20, map.size());
                for (int k = 0; k < 20; k++) {
                    MapKey key = map.withKey();
                    key.putLong(k);
                    MapValue v = key.findValue();
                    Assert.assertNotNull("key not found: " + k, v);
                    Assert.assertEquals("value mismatch for key " + k, 2, v.getLong(0));
                }
            }
        });
    }

    @Test
    public void testOrderedMapHashMatchesJava16B() throws Exception {
        // Covers: case 16 in OrderedMap_computeHashes switch (hashMem64_16).
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.LONG).add(ColumnType.LONG);
            try (
                    OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                prober.resetBatch();
                prober.putLong(-1);
                prober.putLong(Long.MAX_VALUE);
                prober.putLong(0);
                prober.putLong(0);
                prober.computeHashes(2);

                long keyBuf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putLong(keyBuf, -1);
                    Unsafe.getUnsafe().putLong(keyBuf + 8, Long.MAX_VALUE);
                    Assert.assertEquals(Hash.hashMem64(keyBuf, 16), prober.getHash(0));

                    Unsafe.getUnsafe().putLong(keyBuf, 0);
                    Unsafe.getUnsafe().putLong(keyBuf + 8, 0);
                    Assert.assertEquals(Hash.hashMem64(keyBuf, 16), prober.getHash(1));
                } finally {
                    Unsafe.free(keyBuf, 16, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testOrderedMapHashMatchesJava1B() throws Exception {
        // Covers: default branch in OrderedMap_computeHashes switch,
        // and the byte-tail-only path in hashMem64 (len=1, no 8-byte or 4-byte chunks).
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.BYTE);
            try (
                    OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                byte[] keys = {0, 1, -1, Byte.MIN_VALUE, Byte.MAX_VALUE, 42};
                prober.resetBatch();
                for (byte key : keys) {
                    prober.putByte(key);
                }
                prober.computeHashes(keys.length);

                long keyBuf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < keys.length; i++) {
                        Unsafe.getUnsafe().putByte(keyBuf, keys[i]);
                        long expected = Hash.hashMem64(keyBuf, 1);
                        long actual = prober.getHash(i);
                        Assert.assertEquals("hash mismatch for key " + keys[i], expected, actual);
                    }
                } finally {
                    Unsafe.free(keyBuf, 1, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testOrderedMapHashMatchesJava20B() throws Exception {
        // Covers: default branch (keySize=20), hashMem64 general with
        // 8-byte loop (2 iterations) + 4-byte tail (1 int).
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes()
                    .add(ColumnType.LONG).add(ColumnType.LONG).add(ColumnType.INT);
            try (
                    OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                prober.resetBatch();
                prober.putLong(1);
                prober.putLong(2);
                prober.putInt(3);
                prober.computeHashes(1);

                long keyBuf = Unsafe.malloc(20, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putLong(keyBuf, 1);
                    Unsafe.getUnsafe().putLong(keyBuf + 8, 2);
                    Unsafe.getUnsafe().putInt(keyBuf + 16, 3);
                    Assert.assertEquals(Hash.hashMem64(keyBuf, 20), prober.getHash(0));
                } finally {
                    Unsafe.free(keyBuf, 20, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testOrderedMapHashMatchesJava2B() throws Exception {
        // Covers: default branch (keySize=2), byte-tail with 2 remaining bytes.
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.SHORT);
            try (
                    OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                short[] keys = {0, 1, -1, Short.MIN_VALUE, Short.MAX_VALUE, 1000};
                prober.resetBatch();
                for (short key : keys) {
                    prober.putShort(key);
                }
                prober.computeHashes(keys.length);

                long keyBuf = Unsafe.malloc(2, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < keys.length; i++) {
                        Unsafe.getUnsafe().putShort(keyBuf, keys[i]);
                        long expected = Hash.hashMem64(keyBuf, 2);
                        long actual = prober.getHash(i);
                        Assert.assertEquals("hash mismatch for key " + keys[i], expected, actual);
                    }
                } finally {
                    Unsafe.free(keyBuf, 2, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testOrderedMapHashMatchesJava3B() throws Exception {
        // Covers: default branch (keySize=3), hashMem64 general with
        // byte-tail-only loop (3 iterations, no 8-byte or 4-byte chunks).
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes()
                    .add(ColumnType.SHORT).add(ColumnType.BYTE);
            try (
                    OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                prober.resetBatch();
                prober.putShort((short) -1);
                prober.putByte((byte) 127);
                prober.computeHashes(1);

                long keyBuf = Unsafe.malloc(3, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putShort(keyBuf, (short) -1);
                    Unsafe.getUnsafe().putByte(keyBuf + 2, (byte) 127);
                    Assert.assertEquals(Hash.hashMem64(keyBuf, 3), prober.getHash(0));
                } finally {
                    Unsafe.free(keyBuf, 3, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testOrderedMapHashMatchesJava4B() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.INT);
            try (
                    OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                int[] keys = {0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, 42, -100_000};
                prober.resetBatch();
                for (int key : keys) {
                    prober.putInt(key);
                }
                prober.computeHashes(keys.length);

                long keyBuf = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < keys.length; i++) {
                        Unsafe.getUnsafe().putInt(keyBuf, keys[i]);
                        long expected = Hash.hashMem64(keyBuf, 4);
                        long actual = prober.getHash(i);
                        Assert.assertEquals("hash mismatch for key " + keys[i], expected, actual);
                    }
                } finally {
                    Unsafe.free(keyBuf, 4, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testOrderedMapHashMatchesJava5B() throws Exception {
        // Covers: default branch (keySize=5), hashMem64 general with
        // 4-byte tail + 1-byte tail (no 8-byte loop).
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes()
                    .add(ColumnType.INT).add(ColumnType.BYTE);
            try (
                    OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                prober.resetBatch();
                prober.putInt(-1);
                prober.putByte((byte) 42);
                prober.computeHashes(1);

                long keyBuf = Unsafe.malloc(5, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putInt(keyBuf, -1);
                    Unsafe.getUnsafe().putByte(keyBuf + 4, (byte) 42);
                    Assert.assertEquals(Hash.hashMem64(keyBuf, 5), prober.getHash(0));
                } finally {
                    Unsafe.free(keyBuf, 5, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testOrderedMapHashMatchesJava7B() throws Exception {
        // Covers: default branch (keySize=7), hashMem64 general with
        // 4-byte tail + 3-byte tail loop (no 8-byte loop).
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes()
                    .add(ColumnType.INT).add(ColumnType.SHORT).add(ColumnType.BYTE);
            try (
                    OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                prober.resetBatch();
                prober.putInt(100);
                prober.putShort((short) -200);
                prober.putByte((byte) 77);
                prober.computeHashes(1);

                long keyBuf = Unsafe.malloc(7, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putInt(keyBuf, 100);
                    Unsafe.getUnsafe().putShort(keyBuf + 4, (short) -200);
                    Unsafe.getUnsafe().putByte(keyBuf + 6, (byte) 77);
                    Assert.assertEquals(Hash.hashMem64(keyBuf, 7), prober.getHash(0));
                } finally {
                    Unsafe.free(keyBuf, 7, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testOrderedMapHashMatchesJava8B() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.LONG);
            try (
                    OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                long[] keys = {0, 1, -1, Long.MIN_VALUE, Long.MAX_VALUE, 123_456_789L, -999_999_999_999L};
                prober.resetBatch();
                for (long key : keys) {
                    prober.putLong(key);
                }
                prober.computeHashes(keys.length);

                long keyBuf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < keys.length; i++) {
                        Unsafe.getUnsafe().putLong(keyBuf, keys[i]);
                        long expected = Hash.hashMem64(keyBuf, 8);
                        long actual = prober.getHash(i);
                        Assert.assertEquals("hash mismatch for key " + keys[i], expected, actual);
                    }
                } finally {
                    Unsafe.free(keyBuf, 8, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testOrderedMapHashMatchesJavaMultiColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.INT).add(ColumnType.LONG);
            try (
                    OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                // 12-byte key: INT + LONG
                prober.resetBatch();
                prober.putInt(-1);
                prober.putLong(Long.MAX_VALUE);
                prober.putInt(0);
                prober.putLong(0);
                prober.computeHashes(2);

                long keyBuf = Unsafe.malloc(12, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putInt(keyBuf, -1);
                    Unsafe.getUnsafe().putLong(keyBuf + 4, Long.MAX_VALUE);
                    Assert.assertEquals(Hash.hashMem64(keyBuf, 12), prober.getHash(0));

                    Unsafe.getUnsafe().putInt(keyBuf, 0);
                    Unsafe.getUnsafe().putLong(keyBuf + 4, 0);
                    Assert.assertEquals(Hash.hashMem64(keyBuf, 12), prober.getHash(1));
                } finally {
                    Unsafe.free(keyBuf, 12, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testOrderedMapHashMatchesJavaRandom() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.LONG);
            try (
                    OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                long[] keys = new long[256];
                prober.resetBatch();
                for (int i = 0; i < keys.length; i++) {
                    keys[i] = rnd.nextLong();
                    prober.putLong(keys[i]);
                }
                prober.computeHashes(keys.length);

                long keyBuf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < keys.length; i++) {
                        Unsafe.getUnsafe().putLong(keyBuf, keys[i]);
                        long expected = Hash.hashMem64(keyBuf, 8);
                        long actual = prober.getHash(i);
                        Assert.assertEquals("hash mismatch at index " + i, expected, actual);
                    }
                } finally {
                    Unsafe.free(keyBuf, 8, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testOrderedMapVarSizeBatchInsertOneByOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.STRING);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    OrderedMap map = new OrderedMap(256, keyTypes, valueTypes, 16, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(1)
            ) {
                Assert.assertNotNull(prober);
                for (int round = 0; round < 2; round++) {
                    for (int k = 0; k < 20; k++) {
                        prober.resetBatch();
                        prober.beginKey();
                        prober.putStr("key_" + k);
                        prober.endKey();
                        prober.computeHashes(1);
                        MapValue value = prober.probeWithHash(0);
                        if (value.isNew()) {
                            Assert.assertEquals("round " + round + " key_" + k, 0, round);
                            value.putLong(0, 1);
                        } else {
                            Assert.assertEquals("round " + round + " key_" + k, 1, round);
                            value.addLong(0, 1);
                        }
                    }
                }

                Assert.assertEquals(20, map.size());
                for (int k = 0; k < 20; k++) {
                    MapKey key = map.withKey();
                    key.putStr("key_" + k);
                    MapValue v = key.findValue();
                    Assert.assertNotNull("key not found: key_" + k, v);
                    Assert.assertEquals("value mismatch for key_" + k, 2, v.getLong(0));
                }
            }
        });
    }

    @Test
    public void testOrderedMapVarSizeBatchVsPerKey() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.STRING);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    OrderedMap batchMap = new OrderedMap(4096, keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    OrderedMap perKeyMap = new OrderedMap(4096, keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = batchMap.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                final int N = 10_000;
                String[] keys = new String[N];
                for (int i = 0; i < N; i++) {
                    keys[i] = "key_" + (rnd.nextInt() % 2_000);
                }

                // Per-key
                for (String k : keys) {
                    MapKey key = perKeyMap.withKey();
                    key.putStr(k);
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        value.putLong(0, 1);
                    } else {
                        value.addLong(0, 1);
                    }
                }

                // Batch
                for (int offset = 0; offset < N; offset += 256) {
                    int batchSize = Math.min(256, N - offset);
                    prober.resetBatch();
                    for (int i = 0; i < batchSize; i++) {
                        prober.beginKey();
                        prober.putStr(keys[offset + i]);
                        prober.endKey();
                    }
                    prober.computeHashes(batchSize);
                    for (int i = 0; i < batchSize; i++) {
                        MapValue value = prober.probeWithHash(i);
                        if (value.isNew()) {
                            value.putLong(0, 1);
                        } else {
                            value.addLong(0, 1);
                        }
                    }
                }

                Assert.assertEquals(perKeyMap.size(), batchMap.size());
                assertOrderedStringMapsEqual(keys, perKeyMap, batchMap);
            }
        });
    }

    @Test
    public void testOrderedMapVarSizeBatchWithRehashDuringProbe() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.STRING);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            // Small capacity to force rehash mid-batch.
            try (
                    OrderedMap map = new OrderedMap(256, keyTypes, valueTypes, 16, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                // 20 unique keys + 20 duplicates in one batch
                int batchSize = 40;
                prober.resetBatch();
                for (int i = 0; i < batchSize; i++) {
                    prober.beginKey();
                    prober.putStr("key_" + (i % 20));
                    prober.endKey();
                }
                prober.computeHashes(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    MapValue value = prober.probeWithHash(i);
                    if (value.isNew()) {
                        value.putLong(0, 1);
                    } else {
                        value.addLong(0, 1);
                    }
                }

                Assert.assertEquals(20, map.size());
                for (int k = 0; k < 20; k++) {
                    MapKey key = map.withKey();
                    key.putStr("key_" + k);
                    MapValue v = key.findValue();
                    Assert.assertNotNull("key not found: key_" + k, v);
                    Assert.assertEquals("value mismatch for key_" + k, 2, v.getLong(0));
                }
            }
        });
    }

    @Test
    public void testOrderedMapVarSizeHashMatchesJava() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.STRING);
            try (
                    OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                String[] keys = {"hello", "", "a", "test string with spaces", null};
                prober.resetBatch();
                for (String key : keys) {
                    prober.beginKey();
                    prober.putStr(key);
                    prober.endKey();
                }
                prober.computeHashes(keys.length);

                // Compute expected hashes using the standard MapKey path.
                for (int i = 0; i < keys.length; i++) {
                    MapKey mapKey = map.withKey();
                    mapKey.putStr(keys[i]);
                    mapKey.commit(); // commit() sets the length field needed by hash()
                    long expected = mapKey.hash();
                    long actual = prober.getHash(i);
                    Assert.assertEquals("hash mismatch for key '" + keys[i] + "'", expected, actual);
                }
            }
        });
    }

    @Test
    public void testOrderedMapVarSizeKeyReturnsProber() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.STRING);
            try (
                    OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
            }
        });
    }

    @Test
    public void testUnordered4MapBatchInsertOneByOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    Unordered4Map map = new Unordered4Map(ColumnType.INT, valueTypes, 16, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(1)
            ) {
                Assert.assertNotNull(prober);
                // Insert 20 unique keys (including 0 and negatives), then 20 duplicates — forces rehash
                for (int round = 0; round < 2; round++) {
                    for (int k = -5; k < 15; k++) {
                        prober.resetBatch();
                        prober.putInt(k);
                        prober.computeHashes(1);
                        MapValue value = prober.probeWithHash(0);
                        if (value.isNew()) {
                            Assert.assertEquals("round " + round + " key " + k, 0, round);
                            value.putLong(0, 1);
                        } else {
                            Assert.assertEquals("round " + round + " key " + k, 1, round);
                            value.addLong(0, 1);
                        }
                    }
                }

                Assert.assertEquals(20, map.size());
                for (int k = -5; k < 15; k++) {
                    MapKey key = map.withKey();
                    key.putInt(k);
                    MapValue v = key.findValue();
                    Assert.assertNotNull("key not found: " + k, v);
                    Assert.assertEquals("value mismatch for key " + k, 2, v.getLong(0));
                }
            }
        });
    }

    @Test
    public void testUnordered4MapBatchVsPerKey() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    Unordered4Map batchMap = new Unordered4Map(ColumnType.INT, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    Unordered4Map perKeyMap = new Unordered4Map(ColumnType.INT, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = batchMap.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                final int N = 10_000;
                int[] keys = new int[N];
                for (int i = 0; i < N; i++) {
                    keys[i] = rnd.nextInt() % 5_000; // some duplicates, includes negatives and zero
                }

                // Populate perKeyMap using standard API.
                for (int i = 0; i < N; i++) {
                    MapKey key = perKeyMap.withKey();
                    key.putInt(keys[i]);
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        value.putLong(0, 1);
                    } else {
                        value.addLong(0, 1);
                    }
                }

                // Populate batchMap using batch prober.
                for (int offset = 0; offset < N; offset += 256) {
                    int batchSize = Math.min(256, N - offset);
                    prober.resetBatch();
                    for (int i = 0; i < batchSize; i++) {
                        prober.putInt(keys[offset + i]);
                    }
                    prober.computeHashes(batchSize);
                    for (int i = 0; i < batchSize; i++) {
                        MapValue value = prober.probeWithHash(i);
                        if (value.isNew()) {
                            value.putLong(0, 1);
                        } else {
                            value.addLong(0, 1);
                        }
                    }
                }

                Assert.assertEquals(perKeyMap.size(), batchMap.size());
                assertUnordered4MapsEqual(keys, perKeyMap, batchMap);
            }
        });
    }

    @Test
    public void testUnordered4MapBatchWithRehashDuringProbe() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            // capacity 16, loadFactor 0.8 → 12 free slots. 20 unique keys forces rehash mid-batch.
            try (
                    Unordered4Map map = new Unordered4Map(ColumnType.INT, valueTypes, 16, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                // 20 unique keys (including 0 and negatives) + 20 duplicates in one batch
                int batchSize = 40;
                prober.resetBatch();
                for (int i = 0; i < batchSize; i++) {
                    prober.putInt((i % 20) - 5);
                }
                prober.computeHashes(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    MapValue value = prober.probeWithHash(i);
                    if (value.isNew()) {
                        value.putLong(0, 1);
                    } else {
                        value.addLong(0, 1);
                    }
                }

                Assert.assertEquals(20, map.size());
                for (int k = -5; k < 15; k++) {
                    MapKey key = map.withKey();
                    key.putInt(k);
                    MapValue v = key.findValue();
                    Assert.assertNotNull("key not found: " + k, v);
                    Assert.assertEquals("value mismatch for key " + k, 2, v.getLong(0));
                }
            }
        });
    }

    @Test
    public void testUnordered4MapHashMatchesJava() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    Unordered4Map map = new Unordered4Map(ColumnType.INT, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                int[] keys = {0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, 42, -100_000};
                prober.resetBatch();
                for (int key : keys) {
                    prober.putInt(key);
                }
                prober.computeHashes(keys.length);

                for (int i = 0; i < keys.length; i++) {
                    long expected = Hash.hashInt64Simd(keys[i]);
                    long actual = prober.getHash(i);
                    Assert.assertEquals("hash mismatch for key " + keys[i], expected, actual);
                }
            }
        });
    }

    @Test
    public void testUnordered4MapHashMatchesJavaRandom() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            try (
                    Unordered4Map map = new Unordered4Map(ColumnType.INT, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                int[] keys = new int[256];
                prober.resetBatch();
                for (int i = 0; i < keys.length; i++) {
                    keys[i] = rnd.nextInt();
                    prober.putInt(keys[i]);
                }
                prober.computeHashes(keys.length);

                for (int i = 0; i < keys.length; i++) {
                    long expected = Hash.hashInt64Simd(keys[i]);
                    long actual = prober.getHash(i);
                    Assert.assertEquals("hash mismatch at index " + i, expected, actual);
                }
            }
        });
    }

    @Test
    public void testUnordered8MapBatchInsertOneByOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    Unordered8Map map = new Unordered8Map(ColumnType.LONG, valueTypes, 16, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(1)
            ) {
                Assert.assertNotNull(prober);
                // Insert 20 unique keys (including 0 and negatives), then 20 duplicates — forces rehash
                for (int round = 0; round < 2; round++) {
                    for (int k = -5; k < 15; k++) {
                        prober.resetBatch();
                        prober.putLong(k);
                        prober.computeHashes(1);
                        MapValue value = prober.probeWithHash(0);
                        if (value.isNew()) {
                            Assert.assertEquals("round " + round + " key " + k, 0, round);
                            value.putLong(0, 1);
                        } else {
                            Assert.assertEquals("round " + round + " key " + k, 1, round);
                            value.addLong(0, 1);
                        }
                    }
                }

                Assert.assertEquals(20, map.size());
                for (int k = -5; k < 15; k++) {
                    MapKey key = map.withKey();
                    key.putLong(k);
                    MapValue v = key.findValue();
                    Assert.assertNotNull("key not found: " + k, v);
                    Assert.assertEquals("value mismatch for key " + k, 2, v.getLong(0));
                }
            }
        });
    }

    @Test
    public void testUnordered8MapBatchVsPerKey() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    Unordered8Map batchMap = new Unordered8Map(ColumnType.LONG, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    Unordered8Map perKeyMap = new Unordered8Map(ColumnType.LONG, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = batchMap.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                final int N = 10_000;
                long[] keys = new long[N];
                for (int i = 0; i < N; i++) {
                    keys[i] = rnd.nextLong() % 5_000; // some duplicates, includes negatives and zero
                }

                // Populate perKeyMap using standard API.
                for (int i = 0; i < N; i++) {
                    MapKey key = perKeyMap.withKey();
                    key.putLong(keys[i]);
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        value.putLong(0, 1);
                    } else {
                        value.addLong(0, 1);
                    }
                }

                // Populate batchMap using batch prober.
                for (int offset = 0; offset < N; offset += 256) {
                    int batchSize = Math.min(256, N - offset);
                    prober.resetBatch();
                    for (int i = 0; i < batchSize; i++) {
                        prober.putLong(keys[offset + i]);
                    }
                    prober.computeHashes(batchSize);
                    for (int i = 0; i < batchSize; i++) {
                        MapValue value = prober.probeWithHash(i);
                        if (value.isNew()) {
                            value.putLong(0, 1);
                        } else {
                            value.addLong(0, 1);
                        }
                    }
                }

                Assert.assertEquals(perKeyMap.size(), batchMap.size());
                assertUnordered8MapsEqual(keys, perKeyMap, batchMap);
            }
        });
    }

    @Test
    public void testUnordered8MapBatchWithRehashDuringProbe() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            // capacity 16, loadFactor 0.8 → 12 free slots. 20 unique keys forces rehash mid-batch.
            try (
                    Unordered8Map map = new Unordered8Map(ColumnType.LONG, valueTypes, 16, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                // 20 unique keys (including 0 and negatives) + 20 duplicates in one batch
                int batchSize = 40;
                prober.resetBatch();
                for (int i = 0; i < batchSize; i++) {
                    prober.putLong((i % 20) - 5);
                }
                prober.computeHashes(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    MapValue value = prober.probeWithHash(i);
                    if (value.isNew()) {
                        value.putLong(0, 1);
                    } else {
                        value.addLong(0, 1);
                    }
                }

                Assert.assertEquals(20, map.size());
                for (int k = -5; k < 15; k++) {
                    MapKey key = map.withKey();
                    key.putLong(k);
                    MapValue v = key.findValue();
                    Assert.assertNotNull("key not found: " + k, v);
                    Assert.assertEquals("value mismatch for key " + k, 2, v.getLong(0));
                }
            }
        });
    }

    @Test
    public void testUnordered8MapHashMatchesJava() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    Unordered8Map map = new Unordered8Map(ColumnType.LONG, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                long[] keys = {0, 1, -1, Long.MIN_VALUE, Long.MAX_VALUE, 123_456_789L, -999_999_999_999L};
                prober.resetBatch();
                for (long key : keys) {
                    prober.putLong(key);
                }
                prober.computeHashes(keys.length);

                for (int i = 0; i < keys.length; i++) {
                    long expected = Hash.hashLong64Simd(keys[i]);
                    long actual = prober.getHash(i);
                    Assert.assertEquals("hash mismatch for key " + keys[i], expected, actual);
                }
            }
        });
    }

    @Test
    public void testUnorderedVarcharMapBatchInsertOneByOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    UnorderedVarcharMap map = new UnorderedVarcharMap(valueTypes, 16, 0.8, Integer.MAX_VALUE, 128 * 1024, 4 * Numbers.SIZE_1GB);
                    MapBatchProber prober = map.createBatchProber(1);
                    DirectUtf8Sink sink = new DirectUtf8Sink(64)
            ) {
                Assert.assertNotNull(prober);
                // Insert 20 unique keys, then 20 duplicates — forces rehash
                for (int round = 0; round < 2; round++) {
                    for (int k = 0; k < 20; k++) {
                        sink.clear();
                        sink.put("key_" + k);
                        prober.resetBatch();
                        prober.beginKey();
                        prober.putVarchar(sink);
                        prober.endKey();
                        prober.computeHashes(1);
                        MapValue value = prober.probeWithHash(0);
                        if (value.isNew()) {
                            Assert.assertEquals("round " + round + " key_" + k, 0, round);
                            value.putLong(0, 1);
                        } else {
                            Assert.assertEquals("round " + round + " key_" + k, 1, round);
                            value.addLong(0, 1);
                        }
                    }
                }

                Assert.assertEquals(20, map.size());
                for (int k = 0; k < 20; k++) {
                    sink.clear();
                    sink.put("key_" + k);
                    MapKey key = map.withKey();
                    key.putVarchar(sink);
                    MapValue v = key.findValue();
                    Assert.assertNotNull("key not found: key_" + k, v);
                    Assert.assertEquals("value mismatch for key_" + k, 2, v.getLong(0));
                }
            }
        });
    }

    @Test
    public void testUnorderedVarcharMapBatchNullAndEmpty() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    UnorderedVarcharMap map = new UnorderedVarcharMap(valueTypes, 64, 0.8, Integer.MAX_VALUE, 128 * 1024, 4 * Numbers.SIZE_1GB);
                    MapBatchProber prober = map.createBatchProber(256);
                    DirectUtf8Sink sink = new DirectUtf8Sink(64)
            ) {
                Assert.assertNotNull(prober);
                // Insert null, empty, and "abc" in one batch of 3
                prober.resetBatch();

                // null
                prober.beginKey();
                prober.putVarchar((Utf8Sequence) null);
                prober.endKey();

                // empty
                sink.clear();
                prober.beginKey();
                prober.putVarchar(sink);
                prober.endKey();

                // "abc"
                sink.clear();
                sink.put("abc");
                prober.beginKey();
                prober.putVarchar(sink);
                prober.endKey();

                prober.computeHashes(3);
                for (int i = 0; i < 3; i++) {
                    MapValue value = prober.probeWithHash(i);
                    Assert.assertTrue("expected new entry at index " + i, value.isNew());
                    value.putLong(0, i + 1);
                }

                Assert.assertEquals(3, map.size());

                // Verify null
                MapKey nullKey = map.withKey();
                nullKey.putVarchar((Utf8Sequence) null);
                MapValue nullValue = nullKey.findValue();
                Assert.assertNotNull("null key not found", nullValue);
                Assert.assertEquals(1, nullValue.getLong(0));

                // Verify empty
                sink.clear();
                MapKey emptyKey = map.withKey();
                emptyKey.putVarchar(sink);
                MapValue emptyValue = emptyKey.findValue();
                Assert.assertNotNull("empty key not found", emptyValue);
                Assert.assertEquals(2, emptyValue.getLong(0));

                // Verify "abc"
                sink.clear();
                sink.put("abc");
                MapKey abcKey = map.withKey();
                abcKey.putVarchar(sink);
                MapValue abcValue = abcKey.findValue();
                Assert.assertNotNull("'abc' key not found", abcValue);
                Assert.assertEquals(3, abcValue.getLong(0));
            }
        });
    }

    @Test
    public void testUnorderedVarcharMapBatchStablePointers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    UnorderedVarcharMap map = new UnorderedVarcharMap(valueTypes, 64, 0.8, Integer.MAX_VALUE, 128 * 1024, 4 * Numbers.SIZE_1GB);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                byte[] data = "stable_key".getBytes();
                long stableMem = Unsafe.malloc(data.length, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < data.length; i++) {
                        Unsafe.getUnsafe().putByte(stableMem + i, data[i]);
                    }
                    StableUtf8Wrapper stableKey = new StableUtf8Wrapper(stableMem, data.length, true);

                    prober.resetBatch();
                    prober.beginKey();
                    prober.putVarchar(stableKey);
                    prober.endKey();
                    prober.computeHashes(1);

                    // Verify hash matches standard MapKey path
                    MapKey mapKey = map.withKey();
                    mapKey.putVarchar(stableKey);
                    long expected = mapKey.hash();
                    long actual = prober.getHash(0);
                    Assert.assertEquals("hash mismatch for stable key", expected, actual);

                    // Insert via prober and verify lookup
                    MapValue value = prober.probeWithHash(0);
                    Assert.assertTrue(value.isNew());
                    value.putLong(0, 42);

                    MapKey lookupKey = map.withKey();
                    lookupKey.putVarchar(stableKey);
                    MapValue lookupValue = lookupKey.findValue();
                    Assert.assertNotNull("stable key not found", lookupValue);
                    Assert.assertEquals(42, lookupValue.getLong(0));
                } finally {
                    Unsafe.free(stableMem, data.length, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testUnorderedVarcharMapBatchVsPerKey() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    UnorderedVarcharMap batchMap = new UnorderedVarcharMap(valueTypes, 64, 0.8, Integer.MAX_VALUE, 128 * 1024, 4 * Numbers.SIZE_1GB);
                    UnorderedVarcharMap perKeyMap = new UnorderedVarcharMap(valueTypes, 64, 0.8, Integer.MAX_VALUE, 128 * 1024, 4 * Numbers.SIZE_1GB);
                    MapBatchProber prober = batchMap.createBatchProber(256);
                    DirectUtf8Sink sink = new DirectUtf8Sink(64)
            ) {
                Assert.assertNotNull(prober);
                final int N = 10_000;
                String[] keys = new String[N];
                for (int i = 0; i < N; i++) {
                    keys[i] = "key_" + (rnd.nextInt() % 2_000);
                }

                // Populate perKeyMap using standard API.
                for (int i = 0; i < N; i++) {
                    sink.clear();
                    sink.put(keys[i]);
                    MapKey key = perKeyMap.withKey();
                    key.putVarchar(sink);
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        value.putLong(0, 1);
                    } else {
                        value.addLong(0, 1);
                    }
                }

                // Populate batchMap using batch prober.
                for (int offset = 0; offset < N; offset += 256) {
                    int batchSize = Math.min(256, N - offset);
                    prober.resetBatch();
                    for (int i = 0; i < batchSize; i++) {
                        sink.clear();
                        sink.put(keys[offset + i]);
                        prober.beginKey();
                        prober.putVarchar(sink);
                        prober.endKey();
                    }
                    prober.computeHashes(batchSize);
                    for (int i = 0; i < batchSize; i++) {
                        MapValue value = prober.probeWithHash(i);
                        if (value.isNew()) {
                            value.putLong(0, 1);
                        } else {
                            value.addLong(0, 1);
                        }
                    }
                }

                Assert.assertEquals(perKeyMap.size(), batchMap.size());
                assertUnorderedVarcharMapsEqual(keys, sink, perKeyMap, batchMap);
            }
        });
    }

    @Test
    public void testUnorderedVarcharMapBatchWithRehashDuringProbe() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            // Small capacity to force rehash mid-batch.
            try (
                    UnorderedVarcharMap map = new UnorderedVarcharMap(valueTypes, 16, 0.8, Integer.MAX_VALUE, 128 * 1024, 4 * Numbers.SIZE_1GB);
                    MapBatchProber prober = map.createBatchProber(256);
                    DirectUtf8Sink sink = new DirectUtf8Sink(64)
            ) {
                Assert.assertNotNull(prober);
                // 20 unique keys + 20 duplicates in one batch
                int batchSize = 40;
                prober.resetBatch();
                for (int i = 0; i < batchSize; i++) {
                    sink.clear();
                    sink.put("key_" + (i % 20));
                    prober.beginKey();
                    prober.putVarchar(sink);
                    prober.endKey();
                }
                prober.computeHashes(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    MapValue value = prober.probeWithHash(i);
                    if (value.isNew()) {
                        value.putLong(0, 1);
                    } else {
                        value.addLong(0, 1);
                    }
                }

                Assert.assertEquals(20, map.size());
                for (int k = 0; k < 20; k++) {
                    sink.clear();
                    sink.put("key_" + k);
                    MapKey key = map.withKey();
                    key.putVarchar(sink);
                    MapValue v = key.findValue();
                    Assert.assertNotNull("key not found: key_" + k, v);
                    Assert.assertEquals("value mismatch for key_" + k, 2, v.getLong(0));
                }
            }
        });
    }

    @Test
    public void testUnorderedVarcharMapHashMatchesJava() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    UnorderedVarcharMap map = new UnorderedVarcharMap(valueTypes, 64, 0.8, Integer.MAX_VALUE, 128 * 1024, 4 * Numbers.SIZE_1GB);
                    MapBatchProber prober = map.createBatchProber(256);
                    DirectUtf8Sink sink = new DirectUtf8Sink(128)
            ) {
                Assert.assertNotNull(prober);
                String[] keys = {"hello", "", "a", "test string with spaces", "longer key that exceeds eight bytes for multi-chunk hashing"};
                prober.resetBatch();
                for (String key : keys) {
                    sink.clear();
                    sink.put(key);
                    prober.beginKey();
                    prober.putVarchar(sink);
                    prober.endKey();
                }
                prober.computeHashes(keys.length);

                // Compute expected hashes using the standard MapKey path.
                for (int i = 0; i < keys.length; i++) {
                    sink.clear();
                    sink.put(keys[i]);
                    MapKey mapKey = map.withKey();
                    mapKey.putVarchar(sink);
                    long expected = mapKey.hash();
                    long actual = prober.getHash(i);
                    Assert.assertEquals("hash mismatch for key '" + keys[i] + "'", expected, actual);
                }
            }
        });
    }

    @Test
    public void testUnorderedVarcharMapHashMatchesJavaNullKey() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    UnorderedVarcharMap map = new UnorderedVarcharMap(valueTypes, 64, 0.8, Integer.MAX_VALUE, 128 * 1024, 4 * Numbers.SIZE_1GB);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                prober.resetBatch();
                prober.beginKey();
                prober.putVarchar((Utf8Sequence) null);
                prober.endKey();
                prober.computeHashes(1);

                long expected = Hash.hashMem64(0, 0);
                long actual = prober.getHash(0);
                Assert.assertEquals("hash mismatch for null key", expected, actual);
            }
        });
    }

    @Test
    public void testUnorderedVarcharMapHashMatchesJavaRandom() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    UnorderedVarcharMap map = new UnorderedVarcharMap(valueTypes, 64, 0.8, Integer.MAX_VALUE, 128 * 1024, 4 * Numbers.SIZE_1GB);
                    MapBatchProber prober = map.createBatchProber(256);
                    DirectUtf8Sink sink = new DirectUtf8Sink(128)
            ) {
                Assert.assertNotNull(prober);
                String[] keys = new String[256];
                prober.resetBatch();
                for (int i = 0; i < keys.length; i++) {
                    keys[i] = "rnd_" + rnd.nextLong();
                    sink.clear();
                    sink.put(keys[i]);
                    prober.beginKey();
                    prober.putVarchar(sink);
                    prober.endKey();
                }
                prober.computeHashes(keys.length);

                // Verify hashes match standard MapKey path.
                for (int i = 0; i < keys.length; i++) {
                    sink.clear();
                    sink.put(keys[i]);
                    MapKey mapKey = map.withKey();
                    mapKey.putVarchar(sink);
                    long expected = mapKey.hash();
                    long actual = prober.getHash(i);
                    Assert.assertEquals("hash mismatch at index " + i, expected, actual);
                }
            }
        });
    }

    @Test
    public void testUnorderedVarcharMapHashMatchesJavaVarLengths() throws Exception {
        // Covers: hashMem64 general function via UnorderedVarcharMap with key sizes
        // exercising all tail paths: 0B (null), 1B, 3B, 4B, 5B, 7B, 8B, 9B, 15B, 16B.
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);
            try (
                    UnorderedVarcharMap map = new UnorderedVarcharMap(valueTypes, 64, 0.8, Integer.MAX_VALUE, 128 * 1024, 4 * Numbers.SIZE_1GB);
                    DirectUtf8Sink sink = new DirectUtf8Sink(64);
                    MapBatchProber prober = map.createBatchProber(256)
            ) {
                Assert.assertNotNull(prober);
                // Keys of sizes: 1, 3, 4, 5, 7, 8, 9, 15, 16
                String[] keys = {"a", "abc", "abcd", "abcde", "abcdefg", "abcdefgh",
                        "abcdefghi", "abcdefghijklmno", "abcdefghijklmnop"};
                prober.resetBatch();
                for (String key : keys) {
                    sink.clear();
                    sink.put(key);
                    prober.beginKey();
                    prober.putVarchar(sink);
                    prober.endKey();
                }
                prober.computeHashes(keys.length);

                for (int i = 0; i < keys.length; i++) {
                    sink.clear();
                    sink.put(keys[i]);
                    MapKey mapKey = map.withKey();
                    mapKey.putVarchar(sink);
                    long expected = mapKey.hash();
                    long actual = prober.getHash(i);
                    Assert.assertEquals("hash mismatch for key '" + keys[i] + "' (len=" + keys[i].length() + ")", expected, actual);
                }
            }
        });
    }

    private static void assertOrderedMapsEqual(long[] keys, OrderedMap expected, OrderedMap actual) {
        for (long k : keys) {
            MapKey expectedKey = expected.withKey();
            expectedKey.putLong(k);
            MapValue expectedValue = expectedKey.findValue();
            Assert.assertNotNull("key not found in expected map: " + k, expectedValue);

            MapKey actualKey = actual.withKey();
            actualKey.putLong(k);
            MapValue actualValue = actualKey.findValue();
            Assert.assertNotNull("key not found in batch map: " + k, actualValue);
            Assert.assertEquals("value mismatch for key " + k, expectedValue.getLong(0), actualValue.getLong(0));
        }
    }

    private static void assertOrderedStringMapsEqual(String[] keys, OrderedMap expected, OrderedMap actual) {
        for (String k : keys) {
            MapKey expectedKey = expected.withKey();
            expectedKey.putStr(k);
            MapValue expectedValue = expectedKey.findValue();
            Assert.assertNotNull("key not found in expected map: " + k, expectedValue);

            MapKey actualKey = actual.withKey();
            actualKey.putStr(k);
            MapValue actualValue = actualKey.findValue();
            Assert.assertNotNull("key not found in batch map: " + k, actualValue);
            Assert.assertEquals("value mismatch for key " + k, expectedValue.getLong(0), actualValue.getLong(0));
        }
    }

    private static void assertUnordered4MapsEqual(int[] keys, Unordered4Map expected, Unordered4Map actual) {
        for (int k : keys) {
            MapKey expectedKey = expected.withKey();
            expectedKey.putInt(k);
            MapValue expectedValue = expectedKey.findValue();
            Assert.assertNotNull("key not found in expected map: " + k, expectedValue);

            MapKey actualKey = actual.withKey();
            actualKey.putInt(k);
            MapValue actualValue = actualKey.findValue();
            Assert.assertNotNull("key not found in batch map: " + k, actualValue);
            Assert.assertEquals("value mismatch for key " + k, expectedValue.getLong(0), actualValue.getLong(0));
        }
    }

    private static void assertUnordered8MapsEqual(long[] keys, Unordered8Map expected, Unordered8Map actual) {
        for (long k : keys) {
            MapKey expectedKey = expected.withKey();
            expectedKey.putLong(k);
            MapValue expectedValue = expectedKey.findValue();
            Assert.assertNotNull("key not found in expected map: " + k, expectedValue);

            MapKey actualKey = actual.withKey();
            actualKey.putLong(k);
            MapValue actualValue = actualKey.findValue();
            Assert.assertNotNull("key not found in batch map: " + k, actualValue);
            Assert.assertEquals("value mismatch for key " + k, expectedValue.getLong(0), actualValue.getLong(0));
        }
    }

    private static void assertUnorderedVarcharMapsEqual(String[] keys, DirectUtf8Sink sink, UnorderedVarcharMap expected, UnorderedVarcharMap actual) {
        for (String k : keys) {
            sink.clear();
            sink.put(k);
            MapKey expectedKey = expected.withKey();
            expectedKey.putVarchar(sink);
            MapValue expectedValue = expectedKey.findValue();
            Assert.assertNotNull("key not found in expected map: " + k, expectedValue);

            MapKey actualKey = actual.withKey();
            actualKey.putVarchar(k);
            MapValue actualValue = actualKey.findValue();
            Assert.assertNotNull("key not found in batch map: " + k, actualValue);
            Assert.assertEquals("value mismatch for key " + k, expectedValue.getLong(0), actualValue.getLong(0));
        }
    }

    private static class StableUtf8Wrapper implements Utf8Sequence {
        private final boolean ascii;
        private final AsciiCharSequence asciiCharSequence = new AsciiCharSequence();
        private final long ptr;
        private final int size;

        StableUtf8Wrapper(long ptr, int size, boolean ascii) {
            this.ptr = ptr;
            this.size = size;
            this.ascii = ascii;
        }

        @Override
        public @NotNull CharSequence asAsciiCharSequence() {
            return asciiCharSequence.of(this);
        }

        @Override
        public byte byteAt(int offset) {
            return Unsafe.getUnsafe().getByte(ptr + offset);
        }

        @Override
        public boolean isAscii() {
            return ascii;
        }

        @Override
        public boolean isStable() {
            return true;
        }

        @Override
        public long ptr() {
            return ptr;
        }

        @Override
        public int size() {
            return size;
        }
    }
}
