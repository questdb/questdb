/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cutlass.http.qwp;

import io.questdb.cutlass.qwp.protocol.QwpSchemaHash;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class QwpSchemaHashTest {

    @Test
    public void testEmptySchema() {
        String[] names = {};
        byte[] types = {};
        long hash = QwpSchemaHash.computeSchemaHash(names, types);
        // Empty input should produce the same hash consistently
        Assert.assertEquals(hash, QwpSchemaHash.computeSchemaHash(names, types));
    }

    @Test
    public void testSingleColumn() {
        String[] names = {"price"};
        byte[] types = {0x07}; // DOUBLE
        long hash = QwpSchemaHash.computeSchemaHash(names, types);
        Assert.assertNotEquals(0, hash);
    }

    @Test
    public void testMultipleColumns() {
        String[] names = {"symbol", "price", "timestamp"};
        byte[] types = {0x09, 0x07, 0x0A}; // SYMBOL, DOUBLE, TIMESTAMP
        long hash = QwpSchemaHash.computeSchemaHash(names, types);
        Assert.assertNotEquals(0, hash);
    }

    @Test
    public void testColumnOrderMatters() {
        // Order 1
        String[] names1 = {"price", "symbol"};
        byte[] types1 = {0x07, 0x09};

        // Order 2 (different order)
        String[] names2 = {"symbol", "price"};
        byte[] types2 = {0x09, 0x07};

        long hash1 = QwpSchemaHash.computeSchemaHash(names1, types1);
        long hash2 = QwpSchemaHash.computeSchemaHash(names2, types2);

        Assert.assertNotEquals("Column order should affect hash", hash1, hash2);
    }

    @Test
    public void testTypeAffectsHash() {
        // Same name, different type
        String[] names = {"value"};

        byte[] types1 = {0x04}; // INT
        byte[] types2 = {0x05}; // LONG

        long hash1 = QwpSchemaHash.computeSchemaHash(names, types1);
        long hash2 = QwpSchemaHash.computeSchemaHash(names, types2);

        Assert.assertNotEquals("Type should affect hash", hash1, hash2);
    }

    @Test
    public void testNameAffectsHash() {
        // Different names, same type
        byte[] types = {0x07}; // DOUBLE

        String[] names1 = {"price"};
        String[] names2 = {"value"};

        long hash1 = QwpSchemaHash.computeSchemaHash(names1, types);
        long hash2 = QwpSchemaHash.computeSchemaHash(names2, types);

        Assert.assertNotEquals("Name should affect hash", hash1, hash2);
    }

    @Test
    public void testDeterministic() {
        String[] names = {"col1", "col2", "col3"};
        byte[] types = {0x01, 0x02, 0x03};

        long hash1 = QwpSchemaHash.computeSchemaHash(names, types);
        long hash2 = QwpSchemaHash.computeSchemaHash(names, types);
        long hash3 = QwpSchemaHash.computeSchemaHash(names, types);

        Assert.assertEquals("Hash should be deterministic", hash1, hash2);
        Assert.assertEquals("Hash should be deterministic", hash2, hash3);
    }

    @Test
    public void testXXHash64KnownValue() {
        // Test against a known XXHash64 value
        // "abc" with seed 0 should produce a specific value
        byte[] data = "abc".getBytes(StandardCharsets.UTF_8);
        long hash = QwpSchemaHash.hash(data);

        // XXH64("abc", 0) = 0x44BC2CF5AD770999
        Assert.assertEquals(0x44BC2CF5AD770999L, hash);
    }

    @Test
    public void testXXHash64Empty() {
        byte[] data = new byte[0];
        long hash = QwpSchemaHash.hash(data);
        // XXH64("", 0) = 0xEF46DB3751D8E999
        Assert.assertEquals(0xEF46DB3751D8E999L, hash);
    }

    @Test
    public void testXXHash64LongerString() {
        // Test with a longer string to exercise the main loop
        byte[] data = "Hello, World! This is a test string for XXHash64.".getBytes(StandardCharsets.UTF_8);
        long hash1 = QwpSchemaHash.hash(data);
        long hash2 = QwpSchemaHash.hash(data);
        Assert.assertEquals(hash1, hash2);
        Assert.assertNotEquals(0, hash1);
    }

    @Test
    public void testXXHash64DirectMemory() {
        byte[] data = "test data".getBytes(StandardCharsets.UTF_8);
        long addr = Unsafe.malloc(data.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < data.length; i++) {
                Unsafe.getUnsafe().putByte(addr + i, data[i]);
            }

            long hashFromBytes = QwpSchemaHash.hash(data);
            long hashFromMem = QwpSchemaHash.hash(addr, data.length);

            Assert.assertEquals("Direct memory hash should match byte array hash", hashFromBytes, hashFromMem);
        } finally {
            Unsafe.free(addr, data.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testHasherStreaming() {
        // Test that streaming hasher produces same result as one-shot
        byte[] data = "streaming test data for the hasher".getBytes(StandardCharsets.UTF_8);

        // One-shot
        long oneShot = QwpSchemaHash.hash(data);

        // Streaming - byte by byte
        QwpSchemaHash.Hasher hasher = new QwpSchemaHash.Hasher();
        hasher.reset(0);
        for (byte b : data) {
            hasher.update(b);
        }
        long streaming = hasher.getValue();

        Assert.assertEquals("Streaming should match one-shot", oneShot, streaming);
    }

    @Test
    public void testHasherStreamingChunks() {
        // Test streaming with various chunk sizes
        byte[] data = "This is a longer test string to verify chunked hashing works correctly!".getBytes(StandardCharsets.UTF_8);

        long oneShot = QwpSchemaHash.hash(data);

        // Streaming - in chunks
        QwpSchemaHash.Hasher hasher = new QwpSchemaHash.Hasher();
        hasher.reset(0);

        int pos = 0;
        int[] chunkSizes = {5, 10, 3, 20, 7, 15};
        for (int chunkSize : chunkSizes) {
            int toAdd = Math.min(chunkSize, data.length - pos);
            if (toAdd > 0) {
                hasher.update(data, pos, toAdd);
                pos += toAdd;
            }
        }
        // Add remaining
        if (pos < data.length) {
            hasher.update(data, pos, data.length - pos);
        }

        Assert.assertEquals("Chunked streaming should match one-shot", oneShot, hasher.getValue());
    }

    @Test
    public void testHasherReset() {
        QwpSchemaHash.Hasher hasher = new QwpSchemaHash.Hasher();

        byte[] data1 = "first".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "second".getBytes(StandardCharsets.UTF_8);

        // Hash first data
        hasher.reset(0);
        hasher.update(data1);
        long hash1 = hasher.getValue();

        // Reset and hash second data
        hasher.reset(0);
        hasher.update(data2);
        long hash2 = hasher.getValue();

        // Should be different
        Assert.assertNotEquals(hash1, hash2);

        // Reset and hash first again - should be same as original
        hasher.reset(0);
        hasher.update(data1);
        Assert.assertEquals(hash1, hasher.getValue());
    }

    @Test
    public void testSchemaHashWithUtf8Names() {
        // Test UTF-8 column names
        String[] names = {"prix", "日時", "価格"}; // French, Japanese for datetime, Japanese for price
        byte[] types = {0x07, 0x0A, 0x07};

        long hash1 = QwpSchemaHash.computeSchemaHash(names, types);
        long hash2 = QwpSchemaHash.computeSchemaHash(names, types);

        Assert.assertEquals("UTF-8 names should hash consistently", hash1, hash2);
        Assert.assertNotEquals(0, hash1);
    }

    @Test
    public void testNullableFlagAffectsHash() {
        String[] names = {"value"};

        // Non-nullable
        byte[] types1 = {0x05}; // LONG
        // Nullable (high bit set)
        byte[] types2 = {(byte) 0x85}; // LONG | 0x80

        long hash1 = QwpSchemaHash.computeSchemaHash(names, types1);
        long hash2 = QwpSchemaHash.computeSchemaHash(names, types2);

        Assert.assertNotEquals("Nullable flag should affect hash", hash1, hash2);
    }

    @Test
    public void testLargeSchema() {
        // Test with many columns
        int columnCount = 100;
        String[] names = new String[columnCount];
        byte[] types = new byte[columnCount];

        for (int i = 0; i < columnCount; i++) {
            names[i] = "column_" + i;
            types[i] = (byte) ((i % 15) + 1); // Cycle through types 1-15
        }

        long hash1 = QwpSchemaHash.computeSchemaHash(names, types);
        long hash2 = QwpSchemaHash.computeSchemaHash(names, types);

        Assert.assertEquals("Large schema should hash consistently", hash1, hash2);
    }

    @Test
    public void testXXHash64Over32Bytes() {
        // Test data longer than 32 bytes to exercise the main processing loop
        byte[] data = new byte[100];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }

        long hash = QwpSchemaHash.hash(data);
        Assert.assertNotEquals(0, hash);

        // Verify deterministic
        Assert.assertEquals(hash, QwpSchemaHash.hash(data));
    }

    @Test
    public void testXXHash64Exactly32Bytes() {
        // Edge case: exactly 32 bytes
        byte[] data = new byte[32];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }

        long hash = QwpSchemaHash.hash(data);
        Assert.assertNotEquals(0, hash);
        Assert.assertEquals(hash, QwpSchemaHash.hash(data));
    }

    @Test
    public void testXXHash64WithSeed() {
        byte[] data = "test".getBytes(StandardCharsets.UTF_8);

        long hash0 = QwpSchemaHash.hash(data, 0, data.length, 0);
        long hash1 = QwpSchemaHash.hash(data, 0, data.length, 1);
        long hash42 = QwpSchemaHash.hash(data, 0, data.length, 42);

        // Different seeds should produce different hashes
        Assert.assertNotEquals(hash0, hash1);
        Assert.assertNotEquals(hash1, hash42);
        Assert.assertNotEquals(hash0, hash42);
    }
}
