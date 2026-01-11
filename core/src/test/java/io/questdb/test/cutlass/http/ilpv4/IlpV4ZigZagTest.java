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

package io.questdb.test.cutlass.http.ilpv4;

import io.questdb.cutlass.http.ilpv4.IlpV4ZigZag;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class IlpV4ZigZagTest {

    @Test
    public void testEncodeDecodeZero() {
        Assert.assertEquals(0, IlpV4ZigZag.encode(0L));
        Assert.assertEquals(0, IlpV4ZigZag.decode(0L));
    }

    @Test
    public void testEncodePositive() {
        // ZigZag encoding maps:
        // 0 -> 0
        // 1 -> 2
        // 2 -> 4
        // n -> 2n
        Assert.assertEquals(2, IlpV4ZigZag.encode(1L));
        Assert.assertEquals(4, IlpV4ZigZag.encode(2L));
        Assert.assertEquals(6, IlpV4ZigZag.encode(3L));
        Assert.assertEquals(200, IlpV4ZigZag.encode(100L));
    }

    @Test
    public void testEncodeNegative() {
        // ZigZag encoding maps:
        // -1 -> 1
        // -2 -> 3
        // -n -> 2n - 1
        Assert.assertEquals(1, IlpV4ZigZag.encode(-1L));
        Assert.assertEquals(3, IlpV4ZigZag.encode(-2L));
        Assert.assertEquals(5, IlpV4ZigZag.encode(-3L));
        Assert.assertEquals(199, IlpV4ZigZag.encode(-100L));
    }

    @Test
    public void testEncodeMinLong() {
        long encoded = IlpV4ZigZag.encode(Long.MIN_VALUE);
        Assert.assertEquals(-1L, encoded); // All bits set (unsigned max)
        Assert.assertEquals(Long.MIN_VALUE, IlpV4ZigZag.decode(encoded));
    }

    @Test
    public void testEncodeMaxLong() {
        long encoded = IlpV4ZigZag.encode(Long.MAX_VALUE);
        Assert.assertEquals(-2L, encoded); // 0xFFFFFFFFFFFFFFFE (all bits except LSB)
        Assert.assertEquals(Long.MAX_VALUE, IlpV4ZigZag.decode(encoded));
    }

    @Test
    public void testSymmetry() {
        // Test that encode then decode returns the original value
        long[] testValues = {
                0, 1, -1, 2, -2,
                100, -100,
                1000000, -1000000,
                Long.MAX_VALUE, Long.MIN_VALUE,
                Long.MAX_VALUE / 2, Long.MIN_VALUE / 2
        };

        for (long value : testValues) {
            long encoded = IlpV4ZigZag.encode(value);
            long decoded = IlpV4ZigZag.decode(encoded);
            Assert.assertEquals("Failed for value: " + value, value, decoded);
        }
    }

    @Test
    public void testRoundTripRandomValues() {
        Random random = new Random(42); // Fixed seed for reproducibility

        for (int i = 0; i < 1000; i++) {
            long value = random.nextLong();
            long encoded = IlpV4ZigZag.encode(value);
            long decoded = IlpV4ZigZag.decode(encoded);
            Assert.assertEquals("Failed for value: " + value, value, decoded);
        }
    }

    @Test
    public void testEncodeDecodeInt() {
        // Test 32-bit version
        Assert.assertEquals(0, IlpV4ZigZag.encode(0));
        Assert.assertEquals(0, IlpV4ZigZag.decode(0));

        Assert.assertEquals(2, IlpV4ZigZag.encode(1));
        Assert.assertEquals(1, IlpV4ZigZag.decode(2));

        Assert.assertEquals(1, IlpV4ZigZag.encode(-1));
        Assert.assertEquals(-1, IlpV4ZigZag.decode(1));

        int minInt = Integer.MIN_VALUE;
        int encoded = IlpV4ZigZag.encode(minInt);
        Assert.assertEquals(minInt, IlpV4ZigZag.decode(encoded));

        int maxInt = Integer.MAX_VALUE;
        encoded = IlpV4ZigZag.encode(maxInt);
        Assert.assertEquals(maxInt, IlpV4ZigZag.decode(encoded));
    }

    @Test
    public void testEncodingPattern() {
        // Verify the exact encoding pattern matches the formula:
        // zigzag(n) = (n << 1) ^ (n >> 63)
        // This means:
        // - Non-negative n: zigzag(n) = 2 * n
        // - Negative n: zigzag(n) = -2 * n - 1

        for (int n = -100; n <= 100; n++) {
            long encoded = IlpV4ZigZag.encode((long) n);
            long expected = (n >= 0) ? (2L * n) : (-2L * n - 1);
            Assert.assertEquals("Encoding mismatch for n=" + n, expected, encoded);
        }
    }

    @Test
    public void testSmallValuesHaveSmallEncodings() {
        // The point of ZigZag is that small absolute values produce small encoded values
        // which then encode efficiently as varints

        // -1 encodes to 1 (small, 1 byte as varint)
        Assert.assertTrue(IlpV4ZigZag.encode(-1L) < 128);

        // Small positive and negative values should encode to small values
        // Values in [-63, 63] all encode to values < 128 (1 byte varint)
        // 63 encodes to 126, -63 encodes to 125
        for (int n = -63; n <= 63; n++) {
            long encoded = IlpV4ZigZag.encode(n);
            Assert.assertTrue("Value " + n + " encoded to " + encoded,
                    encoded < 128); // Fits in 1 byte varint
        }

        // 64 encodes to 128, which requires 2 bytes as varint
        Assert.assertEquals(128, IlpV4ZigZag.encode(64L));
    }
}
