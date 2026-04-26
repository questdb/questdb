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

package io.questdb.test.cairo;

import io.questdb.cairo.idx.PostingIndexNative;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PostingIndexNativeTest {

    @Test
    public void testFallbackBitWidth63() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int count = 10;
            long minValue = 0;
            int bitWidth = 63;

            long valuesAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            int packedBytes = (count * bitWidth + 7) / 8 + 8;
            long packedAddr = Unsafe.malloc(packedBytes, MemoryTag.NATIVE_DEFAULT);
            long unpackedAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(valuesAddr + (long) i * Long.BYTES, (long) i * 1_000_000_000L);
                }

                PostingIndexNative.packValuesNativeFallback(valuesAddr, count, minValue, bitWidth, packedAddr);
                PostingIndexNative.unpackAllValuesNativeFallback(packedAddr, count, bitWidth, minValue, unpackedAddr);

                for (int i = 0; i < count; i++) {
                    long expected = (long) i * 1_000_000_000L;
                    long actual = Unsafe.getUnsafe().getLong(unpackedAddr + (long) i * Long.BYTES);
                    Assert.assertEquals("mismatch at index " + i, expected, actual);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(packedAddr, packedBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(unpackedAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFallbackMatchesNative() throws Exception {
        // Compare fallback output with native output for identical input.
        if (!PostingIndexNative.isNativeAvailable()) {
            return;
        }

        TestUtils.assertMemoryLeak(() -> {
            int count = 200;
            long minValue = 500;
            int bitWidth = 13;
            int meaningfulPackedBytes = (count * bitWidth + 7) / 8;
            int allocBytes = meaningfulPackedBytes + 8;

            long valuesAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long nativePackedAddr = Unsafe.calloc(allocBytes, MemoryTag.NATIVE_DEFAULT);
            long fallbackPackedAddr = Unsafe.calloc(allocBytes, MemoryTag.NATIVE_DEFAULT);
            long nativeUnpackedAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long fallbackUnpackedAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(valuesAddr + (long) i * Long.BYTES, minValue + i * 3L);
                }

                // Pack with native
                PostingIndexNative.packValuesNative(valuesAddr, count, minValue, bitWidth, nativePackedAddr);
                // Pack with fallback
                PostingIndexNative.packValuesNativeFallback(valuesAddr, count, minValue, bitWidth, fallbackPackedAddr);

                // Packed output should be identical (only meaningful bytes)
                for (int b = 0; b < meaningfulPackedBytes; b++) {
                    Assert.assertEquals("packed byte mismatch at " + b,
                            Unsafe.getUnsafe().getByte(nativePackedAddr + b),
                            Unsafe.getUnsafe().getByte(fallbackPackedAddr + b));
                }

                // Unpack with native
                PostingIndexNative.unpackAllValuesNative(nativePackedAddr, count, bitWidth, minValue, nativeUnpackedAddr);
                // Unpack with fallback
                PostingIndexNative.unpackAllValuesNativeFallback(nativePackedAddr, count, bitWidth, minValue, fallbackUnpackedAddr);

                // Unpacked output should be identical
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals("unpack mismatch at " + i,
                            Unsafe.getUnsafe().getLong(nativeUnpackedAddr + (long) i * Long.BYTES),
                            Unsafe.getUnsafe().getLong(fallbackUnpackedAddr + (long) i * Long.BYTES));
                }
            } finally {
                Unsafe.free(valuesAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nativePackedAddr, allocBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(fallbackPackedAddr, allocBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nativeUnpackedAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(fallbackUnpackedAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFallbackPackUnpackRoundTrip() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int count = 100;
            long minValue = 1000;
            int bitWidth = 20;

            long valuesAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long packedAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long unpackedAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(valuesAddr + (long) i * Long.BYTES, minValue + i * 7L);
                }

                PostingIndexNative.packValuesNativeFallback(valuesAddr, count, minValue, bitWidth, packedAddr);
                PostingIndexNative.unpackAllValuesNativeFallback(packedAddr, count, bitWidth, minValue, unpackedAddr);

                for (int i = 0; i < count; i++) {
                    long expected = minValue + i * 7L;
                    long actual = Unsafe.getUnsafe().getLong(unpackedAddr + (long) i * Long.BYTES);
                    Assert.assertEquals("mismatch at index " + i, expected, actual);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(packedAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(unpackedAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFallbackSingleValue() throws Exception {
        // Edge case: single value
        TestUtils.assertMemoryLeak(() -> {
            long valuesAddr = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long packedAddr = Unsafe.malloc(Long.BYTES + 8, MemoryTag.NATIVE_DEFAULT);
            long unpackedAddr = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putLong(valuesAddr, 42);

                PostingIndexNative.packValuesNativeFallback(valuesAddr, 1, 42, 1, packedAddr);
                PostingIndexNative.unpackAllValuesNativeFallback(packedAddr, 1, 1, 42, unpackedAddr);

                Assert.assertEquals(42, Unsafe.getUnsafe().getLong(unpackedAddr));
            } finally {
                Unsafe.free(valuesAddr, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(packedAddr, Long.BYTES + 8, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(unpackedAddr, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFallbackWideBitWidth() throws Exception {
        // Test bit widths > 32 which trigger the overflow recovery path
        // (oldBufferBits + bitWidth > 64) in packValuesNativeFallback.
        TestUtils.assertMemoryLeak(() -> {
            int count = 50;
            long minValue = 0;
            int bitWidth = 48;

            long valuesAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            int packedBytes = (count * bitWidth + 7) / 8 + 8;
            long packedAddr = Unsafe.malloc(packedBytes, MemoryTag.NATIVE_DEFAULT);
            long unpackedAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(valuesAddr + (long) i * Long.BYTES, (long) i * 100_000_000L);
                }

                PostingIndexNative.packValuesNativeFallback(valuesAddr, count, minValue, bitWidth, packedAddr);
                PostingIndexNative.unpackAllValuesNativeFallback(packedAddr, count, bitWidth, minValue, unpackedAddr);

                for (int i = 0; i < count; i++) {
                    long expected = (long) i * 100_000_000L;
                    long actual = Unsafe.getUnsafe().getLong(unpackedAddr + (long) i * Long.BYTES);
                    Assert.assertEquals("mismatch at index " + i, expected, actual);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(packedAddr, packedBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(unpackedAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }
}
