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
import io.questdb.std.Hash;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MapBatchProberTest extends AbstractCairoTest {

    @Test
    public void testOrderedMapHashMatchesJava4B() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.INT);
            try (OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE)) {
                MapBatchProber prober = map.createBatchProber(256);
                Assert.assertNotNull(prober);
                try {
                    int[] keys = {0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, 42, -100_000};
                    prober.resetBatch();
                    for (int key : keys) {
                        prober.putInt(key);
                    }
                    prober.hashAndPrefetch(keys.length);

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
                } finally {
                    prober.close();
                }
            }
        });
    }

    @Test
    public void testOrderedMapHashMatchesJava8B() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.LONG);
            try (OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE)) {
                MapBatchProber prober = map.createBatchProber(256);
                Assert.assertNotNull(prober);
                try {
                    long[] keys = {0, 1, -1, Long.MIN_VALUE, Long.MAX_VALUE, 123_456_789L, -999_999_999_999L};
                    prober.resetBatch();
                    for (long key : keys) {
                        prober.putLong(key);
                    }
                    prober.hashAndPrefetch(keys.length);

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
                } finally {
                    prober.close();
                }
            }
        });
    }

    @Test
    public void testOrderedMapHashMatchesJavaMultiColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.INT).add(ColumnType.LONG);
            try (OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE)) {
                MapBatchProber prober = map.createBatchProber(256);
                Assert.assertNotNull(prober);
                try {
                    // 12-byte key: INT + LONG
                    prober.resetBatch();
                    prober.putInt(-1);
                    prober.putLong(Long.MAX_VALUE);
                    prober.putInt(0);
                    prober.putLong(0);
                    prober.hashAndPrefetch(2);

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
                } finally {
                    prober.close();
                }
            }
        });
    }

    @Test
    public void testUnordered4MapHashMatchesJava() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Unordered4Map map = new Unordered4Map(ColumnType.INT, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE)) {
                MapBatchProber prober = map.createBatchProber(256);
                Assert.assertNotNull(prober);
                try {
                    int[] keys = {0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, 42, -100_000};
                    prober.resetBatch();
                    for (int key : keys) {
                        prober.putInt(key);
                    }
                    prober.hashAndPrefetch(keys.length);

                    for (int i = 0; i < keys.length; i++) {
                        long expected = Hash.hashInt64(keys[i]);
                        long actual = prober.getHash(i);
                        Assert.assertEquals("hash mismatch for key " + keys[i], expected, actual);
                    }
                } finally {
                    prober.close();
                }
            }
        });
    }

    @Test
    public void testUnordered8MapHashMatchesJava() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Unordered8Map map = new Unordered8Map(ColumnType.LONG, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE)) {
                MapBatchProber prober = map.createBatchProber(256);
                Assert.assertNotNull(prober);
                try {
                    long[] keys = {0, 1, -1, Long.MIN_VALUE, Long.MAX_VALUE, 123_456_789L, -999_999_999_999L};
                    prober.resetBatch();
                    for (long key : keys) {
                        prober.putLong(key);
                    }
                    prober.hashAndPrefetch(keys.length);

                    for (int i = 0; i < keys.length; i++) {
                        long expected = Hash.hashLong64(keys[i]);
                        long actual = prober.getHash(i);
                        Assert.assertEquals("hash mismatch for key " + keys[i], expected, actual);
                    }
                } finally {
                    prober.close();
                }
            }
        });
    }

    @Test
    public void testOrderedMapHashMatchesJavaRandom() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.LONG);
            try (OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE)) {
                MapBatchProber prober = map.createBatchProber(256);
                Assert.assertNotNull(prober);
                try {
                    long[] keys = new long[256];
                    prober.resetBatch();
                    for (int i = 0; i < keys.length; i++) {
                        keys[i] = rnd.nextLong();
                        prober.putLong(keys[i]);
                    }
                    prober.hashAndPrefetch(keys.length);

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
                } finally {
                    prober.close();
                }
            }
        });
    }

    @Test
    public void testUnordered4MapHashMatchesJavaRandom() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            try (Unordered4Map map = new Unordered4Map(ColumnType.INT, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE)) {
                MapBatchProber prober = map.createBatchProber(256);
                Assert.assertNotNull(prober);
                try {
                    int[] keys = new int[256];
                    prober.resetBatch();
                    for (int i = 0; i < keys.length; i++) {
                        keys[i] = rnd.nextInt();
                        prober.putInt(keys[i]);
                    }
                    prober.hashAndPrefetch(keys.length);

                    for (int i = 0; i < keys.length; i++) {
                        long expected = Hash.hashInt64(keys[i]);
                        long actual = prober.getHash(i);
                        Assert.assertEquals("hash mismatch at index " + i, expected, actual);
                    }
                } finally {
                    prober.close();
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
                    OrderedMap perKeyMap = new OrderedMap(4096, keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE)
            ) {
                MapBatchProber prober = batchMap.createBatchProber(256);
                Assert.assertNotNull(prober);
                try {
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
                        prober.hashAndPrefetch(batchSize);
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
                } finally {
                    prober.close();
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
                    Unordered4Map perKeyMap = new Unordered4Map(ColumnType.INT, valueTypes, 64, 0.8, Integer.MAX_VALUE)
            ) {
                MapBatchProber prober = batchMap.createBatchProber(256);
                Assert.assertNotNull(prober);
                try {
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
                        prober.hashAndPrefetch(batchSize);
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
                } finally {
                    prober.close();
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
                    Unordered8Map perKeyMap = new Unordered8Map(ColumnType.LONG, valueTypes, 64, 0.8, Integer.MAX_VALUE)
            ) {
                MapBatchProber prober = batchMap.createBatchProber(256);
                Assert.assertNotNull(prober);
                try {
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
                        prober.hashAndPrefetch(batchSize);
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
                } finally {
                    prober.close();
                }
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
                    OrderedMap perKeyMap = new OrderedMap(4096, keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE)
            ) {
                MapBatchProber prober = batchMap.createBatchProber(4);
                Assert.assertNotNull(prober);
                try {
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
                    prober.hashAndPrefetch(keys.length);
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
                } finally {
                    prober.close();
                }
            }
        });
    }

    @Test
    public void testOrderedMapBatchInsertOneByOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.LONG);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (OrderedMap map = new OrderedMap(256, keyTypes, valueTypes, 16, 0.8, Integer.MAX_VALUE)) {
                MapBatchProber prober = map.createBatchProber(1);
                Assert.assertNotNull(prober);
                try {
                    // Insert 20 unique keys, then 20 duplicates — forces rehash
                    for (int round = 0; round < 2; round++) {
                        for (int k = 0; k < 20; k++) {
                            prober.resetBatch();
                            prober.putLong(k);
                            prober.hashAndPrefetch(1);
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
                } finally {
                    prober.close();
                }
            }
        });
    }

    @Test
    public void testOrderedMapBatchWithRehashDuringProbe() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.LONG);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            // capacity 16, loadFactor 0.8 → 12 free slots. Batch of 20 unique keys forces rehash mid-batch.
            try (OrderedMap map = new OrderedMap(256, keyTypes, valueTypes, 16, 0.8, Integer.MAX_VALUE)) {
                MapBatchProber prober = map.createBatchProber(256);
                Assert.assertNotNull(prober);
                try {
                    // 20 unique keys + 20 duplicates in one batch
                    int batchSize = 40;
                    prober.resetBatch();
                    for (int i = 0; i < batchSize; i++) {
                        prober.putLong(i % 20);
                    }
                    prober.hashAndPrefetch(batchSize);
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
                } finally {
                    prober.close();
                }
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
                    OrderedMap perKeyMap = new OrderedMap(256, keyTypes, valueTypes, 16, 0.8, Integer.MAX_VALUE)
            ) {
                MapBatchProber prober = batchMap.createBatchProber(256);
                Assert.assertNotNull(prober);
                try {
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
                    prober.hashAndPrefetch(keys.length);
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
                } finally {
                    prober.close();
                }
            }
        });
    }

    @Test
    public void testUnordered4MapBatchInsertOneByOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (Unordered4Map map = new Unordered4Map(ColumnType.INT, valueTypes, 16, 0.8, Integer.MAX_VALUE)) {
                MapBatchProber prober = map.createBatchProber(1);
                Assert.assertNotNull(prober);
                try {
                    // Insert 20 unique keys (including 0 and negatives), then 20 duplicates — forces rehash
                    for (int round = 0; round < 2; round++) {
                        for (int k = -5; k < 15; k++) {
                            prober.resetBatch();
                            prober.putInt(k);
                            prober.hashAndPrefetch(1);
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
                } finally {
                    prober.close();
                }
            }
        });
    }

    @Test
    public void testUnordered4MapBatchWithRehashDuringProbe() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            // capacity 16, loadFactor 0.8 → 12 free slots. 20 unique keys forces rehash mid-batch.
            try (Unordered4Map map = new Unordered4Map(ColumnType.INT, valueTypes, 16, 0.8, Integer.MAX_VALUE)) {
                MapBatchProber prober = map.createBatchProber(256);
                Assert.assertNotNull(prober);
                try {
                    // 20 unique keys (including 0 and negatives) + 20 duplicates in one batch
                    int batchSize = 40;
                    prober.resetBatch();
                    for (int i = 0; i < batchSize; i++) {
                        prober.putInt((i % 20) - 5);
                    }
                    prober.hashAndPrefetch(batchSize);
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
                } finally {
                    prober.close();
                }
            }
        });
    }

    @Test
    public void testUnordered8MapBatchInsertOneByOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (Unordered8Map map = new Unordered8Map(ColumnType.LONG, valueTypes, 16, 0.8, Integer.MAX_VALUE)) {
                MapBatchProber prober = map.createBatchProber(1);
                Assert.assertNotNull(prober);
                try {
                    // Insert 20 unique keys (including 0 and negatives), then 20 duplicates — forces rehash
                    for (int round = 0; round < 2; round++) {
                        for (int k = -5; k < 15; k++) {
                            prober.resetBatch();
                            prober.putLong(k);
                            prober.hashAndPrefetch(1);
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
                } finally {
                    prober.close();
                }
            }
        });
    }

    @Test
    public void testUnordered8MapBatchWithRehashDuringProbe() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            // capacity 16, loadFactor 0.8 → 12 free slots. 20 unique keys forces rehash mid-batch.
            try (Unordered8Map map = new Unordered8Map(ColumnType.LONG, valueTypes, 16, 0.8, Integer.MAX_VALUE)) {
                MapBatchProber prober = map.createBatchProber(256);
                Assert.assertNotNull(prober);
                try {
                    // 20 unique keys (including 0 and negatives) + 20 duplicates in one batch
                    int batchSize = 40;
                    prober.resetBatch();
                    for (int i = 0; i < batchSize; i++) {
                        prober.putLong((i % 20) - 5);
                    }
                    prober.hashAndPrefetch(batchSize);
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
                } finally {
                    prober.close();
                }
            }
        });
    }

    @Test
    public void testOrderedMapVarSizeKeyReturnsProber() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.STRING);
            try (OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE)) {
                MapBatchProber prober = map.createBatchProber(256);
                Assert.assertNotNull(prober);
                prober.close();
            }
        });
    }

    @Test
    public void testOrderedMapVarSizeHashMatchesJava() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.STRING);
            try (OrderedMap map = new OrderedMap(1024, keyTypes, new SingleColumnType(ColumnType.LONG), 64, 0.8, Integer.MAX_VALUE)) {
                MapBatchProber prober = map.createBatchProber(256);
                Assert.assertNotNull(prober);
                try {
                    String[] keys = {"hello", "", "a", "test string with spaces", null};
                    prober.resetBatch();
                    for (String key : keys) {
                        prober.beginKey();
                        prober.putStr(key);
                        prober.endKey();
                    }
                    prober.hashAndPrefetch(keys.length);

                    // Compute expected hashes using the standard MapKey path.
                    for (int i = 0; i < keys.length; i++) {
                        MapKey mapKey = map.withKey();
                        mapKey.putStr(keys[i]);
                        mapKey.commit(); // commit() sets the length field needed by hash()
                        long expected = mapKey.hash();
                        long actual = prober.getHash(i);
                        Assert.assertEquals("hash mismatch for key '" + keys[i] + "'", expected, actual);
                    }
                } finally {
                    prober.close();
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
                    OrderedMap perKeyMap = new OrderedMap(4096, keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE)
            ) {
                MapBatchProber prober = batchMap.createBatchProber(256);
                Assert.assertNotNull(prober);
                try {
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
                        prober.hashAndPrefetch(batchSize);
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
                } finally {
                    prober.close();
                }
            }
        });
    }

    @Test
    public void testOrderedMapVarSizeBatchWithRehashDuringProbe() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.STRING);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            // Small capacity to force rehash mid-batch.
            try (OrderedMap map = new OrderedMap(256, keyTypes, valueTypes, 16, 0.8, Integer.MAX_VALUE)) {
                MapBatchProber prober = map.createBatchProber(256);
                Assert.assertNotNull(prober);
                try {
                    // 20 unique keys + 20 duplicates in one batch
                    int batchSize = 40;
                    prober.resetBatch();
                    for (int i = 0; i < batchSize; i++) {
                        prober.beginKey();
                        prober.putStr("key_" + (i % 20));
                        prober.endKey();
                    }
                    prober.hashAndPrefetch(batchSize);
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
                } finally {
                    prober.close();
                }
            }
        });
    }

    @Test
    public void testOrderedMapVarSizeBatchInsertOneByOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.STRING);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (OrderedMap map = new OrderedMap(256, keyTypes, valueTypes, 16, 0.8, Integer.MAX_VALUE)) {
                MapBatchProber prober = map.createBatchProber(1);
                Assert.assertNotNull(prober);
                try {
                    for (int round = 0; round < 2; round++) {
                        for (int k = 0; k < 20; k++) {
                            prober.resetBatch();
                            prober.beginKey();
                            prober.putStr("key_" + k);
                            prober.endKey();
                            prober.hashAndPrefetch(1);
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
                } finally {
                    prober.close();
                }
            }
        });
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
}
