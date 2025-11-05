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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cutlass.line.tcp.DecimalBinaryFormatParser;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

public class DecimalBinaryFormatParserTest {
    @Test
    public void testBasic() throws DecimalBinaryFormatParser.ParseException {
        Decimal256 decimal256 = parse(new byte[]{
                0,
                2, // Length
                1, 8, // 2⁸ + 8
        });
        Assert.assertNotNull(decimal256);
        Assert.assertEquals(0, decimal256.getScale());
        Assert.assertEquals("264", decimal256.toString());
    }

    @Test
    public void testInvalidScale() {
        try {
            parse(new byte[]{
                    Decimals.MAX_SCALE + 1,
                    0, // Length
            });
            Assert.fail();
        } catch (DecimalBinaryFormatParser.ParseException ex) {
            Assert.assertEquals(LineTcpParser.ErrorCode.DECIMAL_INVALID_SCALE, ex.errorCode());
        }
    }

    @Test
    public void testOverflow() throws DecimalBinaryFormatParser.ParseException {
        Decimal256 decimal256 = parse(new byte[]{
                0, // Scale
                36, // Length
                1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 // Unscaled value
        });
        Assert.assertNull(decimal256);
    }

    @Test
    public void testZeroLength() throws DecimalBinaryFormatParser.ParseException {
        Decimal256 decimal256 = parse(new byte[]{
                5, // Scale
                0, // Zero length means NULL decimal
        });
        Assert.assertNotNull(decimal256);
        Assert.assertTrue(decimal256.isNull());
    }

    @Test
    public void testNegativeValue() throws DecimalBinaryFormatParser.ParseException {
        // Test -10 with scale 2 = -0.10
        Decimal256 decimal256 = parse(new byte[]{
                2, // Scale
                2, // Length
                (byte) 0b11111111, (byte) 0b11110110, // -00 in two's complement big-endian
        });
        Assert.assertNotNull(decimal256);
        Assert.assertEquals(2, decimal256.getScale());
        Assert.assertEquals("-0.10", decimal256.toString());
    }

    @Test
    public void testPositiveWithScale() throws DecimalBinaryFormatParser.ParseException {
        // Test 12345 with scale 3 = 12.345
        Decimal256 decimal256 = parse(new byte[]{
                3, // Scale
                2, // Length
                0x30, 0x39, // 12345 in big-endian
        });
        Assert.assertNotNull(decimal256);
        Assert.assertEquals(3, decimal256.getScale());
        Assert.assertEquals("12.345", decimal256.toString());
    }

    @Test
    public void testSingleByte() throws DecimalBinaryFormatParser.ParseException {
        // Test single byte value
        Decimal256 decimal256 = parse(new byte[]{
                0, // Scale
                1, // Length
                127, // Max positive byte value
        });
        Assert.assertNotNull(decimal256);
        Assert.assertEquals(0, decimal256.getScale());
        Assert.assertEquals("127", decimal256.toString());
    }

    @Test
    public void testNegativeSingleByte() throws DecimalBinaryFormatParser.ParseException {
        // Test negative single byte value
        Decimal256 decimal256 = parse(new byte[]{
                1, // Scale
                1, // Length
                (byte) 0xFF, // -1 in two's complement
        });
        Assert.assertNotNull(decimal256);
        Assert.assertEquals(1, decimal256.getScale());
        Assert.assertEquals("-0.1", decimal256.toString());
    }

    @Test
    public void testFourByteValue() throws DecimalBinaryFormatParser.ParseException {
        // Test 4-byte integer value
        Decimal256 decimal256 = parse(new byte[]{
                0, // Scale
                4, // Length
                0x00, 0x00, 0x00, 0x64, // 100 in big-endian
        });
        Assert.assertNotNull(decimal256);
        Assert.assertEquals(0, decimal256.getScale());
        Assert.assertEquals("100", decimal256.toString());
    }

    @Test
    public void testEightByteValue() throws DecimalBinaryFormatParser.ParseException {
        // Test 8-byte long value
        Decimal256 decimal256 = parse(new byte[]{
                0, // Scale
                8, // Length
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, (byte) 0xE8, // 1000 in big-endian
        });
        Assert.assertNotNull(decimal256);
        Assert.assertEquals(0, decimal256.getScale());
        Assert.assertEquals("1000", decimal256.toString());
    }

    @Test
    public void testMaxScale() throws DecimalBinaryFormatParser.ParseException {
        // Test maximum allowed scale
        Decimal256 decimal256 = parse(new byte[]{
                Decimals.MAX_SCALE, // Maximum scale (76)
                1, // Length
                1, // Value 1
        });
        Assert.assertNotNull(decimal256);
        Assert.assertEquals(Decimals.MAX_SCALE, decimal256.getScale());
        // Value 1 with scale 76 = 0.0000...0001 (75 zeros after decimal point)
        String expected = "0." + "0".repeat(75) + "1";
        Assert.assertEquals(expected, decimal256.toString());
    }

    @Test
    public void testNegativeScale() {
        // Test negative scale (invalid)
        try {
            parse(new byte[]{
                    (byte) -1, // Negative scale
                    0,
            });
            Assert.fail();
        } catch (DecimalBinaryFormatParser.ParseException ex) {
            Assert.assertEquals(LineTcpParser.ErrorCode.DECIMAL_INVALID_SCALE, ex.errorCode());
        }
    }

    @Test
    public void testThreeByteValue() throws DecimalBinaryFormatParser.ParseException {
        // Test 3-byte value (not aligned to 4 bytes)
        Decimal256 decimal256 = parse(new byte[]{
                2, // Scale
                3, // Length
                0x01, (byte) 0x86, (byte) 0xA0, // 100000 in big-endian
        });
        Assert.assertNotNull(decimal256);
        Assert.assertEquals(2, decimal256.getScale());
        Assert.assertEquals("1000.00", decimal256.toString());
    }

    @Test
    public void testFiveByteValue() throws DecimalBinaryFormatParser.ParseException {
        // Test 5-byte value
        Decimal256 decimal256 = parse(new byte[]{
                0, // Scale
                5, // Length
                0x00, 0x00, 0x00, 0x01, 0x00, // 256 in big-endian
        });
        Assert.assertNotNull(decimal256);
        Assert.assertEquals(0, decimal256.getScale());
        Assert.assertEquals("256", decimal256.toString());
    }

    @Test
    public void testMaximumDecimal256() throws DecimalBinaryFormatParser.ParseException {
        // Test maximum 256-bit value (32 bytes)
        byte[] bytes = new byte[34]; // 1 scale + 1 length + 32 data
        bytes[0] = 0; // Scale
        bytes[1] = 32; // Length
        bytes[2] = 0x7F; // Positive sign bit
        for (int i = 3; i < 34; i++) {
            bytes[i] = (byte) 0xFF;
        }
        Decimal256 decimal256 = parse(bytes);
        Assert.assertNotNull(decimal256);
        Assert.assertEquals(0, decimal256.getScale());
    }

    @Test
    public void testMinimumDecimal256() throws DecimalBinaryFormatParser.ParseException {
        // Test minimum 256-bit value (negative)
        byte[] bytes = new byte[34]; // 1 scale + 1 length + 32 data
        bytes[0] = 0; // Scale
        bytes[1] = 32; // Length
        bytes[2] = (byte) 0x80; // Negative sign bit
        for (int i = 3; i < 34; i++) {
            bytes[i] = 0x00;
        }
        Decimal256 decimal256 = parse(bytes);
        Assert.assertNotNull(decimal256);
        Assert.assertEquals(0, decimal256.getScale());
    }

    @Test
    public void testLargeScaleWithSmallValue() throws DecimalBinaryFormatParser.ParseException {
        // Test small value with large scale
        Decimal256 decimal256 = parse(new byte[]{
                50, // Large scale
                2, // Length
                0x00, 0x64, // 100 in big-endian
        });
        Assert.assertNotNull(decimal256);
        Assert.assertEquals(50, decimal256.getScale());
        String expected = "0.00000000000000000000000000000000000000000000000100";
        Assert.assertEquals(expected, decimal256.toString());
    }

    @Test
    public void testResetAndReuse() throws DecimalBinaryFormatParser.ParseException {
        long mem = Unsafe.malloc(100, MemoryTag.NATIVE_DEFAULT);
        try (DecimalBinaryFormatParser parser = new DecimalBinaryFormatParser()) {
            // First parse
            parser.reset();
            byte[] bytes1 = new byte[]{2, 2, 0x00, 0x64}; // 100 with scale 2
            for (int i = 0; i < bytes1.length; i++) {
                Unsafe.getUnsafe().putByte(mem + i, bytes1[i]);
            }

            long start = mem;
            boolean finish;
            do {
                long size = parser.getNextExpectSize();
                finish = parser.processNextBinaryPart(start);
                start += size;
            } while (!finish);

            Decimal256 decimal1 = new Decimal256();
            Assert.assertTrue(parser.load(decimal1));
            Assert.assertEquals("1.00", decimal1.toString());

            // Reset and parse again with different value
            parser.reset();
            byte[] bytes2 = new byte[]{0, 1, 0x0A}; // 10 with scale 0
            for (int i = 0; i < bytes2.length; i++) {
                Unsafe.getUnsafe().putByte(mem + i, bytes2[i]);
            }

            start = mem;
            do {
                long size = parser.getNextExpectSize();
                finish = parser.processNextBinaryPart(start);
                start += size;
            } while (!finish);

            Decimal256 decimal2 = new Decimal256();
            Assert.assertTrue(parser.load(decimal2));
            Assert.assertEquals("10", decimal2.toString());
        } finally {
            Unsafe.free(mem, 100, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testGetNextExpectSize() throws DecimalBinaryFormatParser.ParseException {
        long mem = Unsafe.malloc(100, MemoryTag.NATIVE_DEFAULT);
        try (DecimalBinaryFormatParser parser = new DecimalBinaryFormatParser()) {
            parser.reset();

            // Initially expects 1 byte for scale
            Assert.assertEquals(1, parser.getNextExpectSize());

            // Set scale
            Unsafe.getUnsafe().putByte(mem, (byte) 2);
            parser.processNextBinaryPart(mem);

            // Now expects 1 byte for length
            Assert.assertEquals(1, parser.getNextExpectSize());

            // Set length to 10
            Unsafe.getUnsafe().putByte(mem + 1, (byte) 10);
            parser.processNextBinaryPart(mem + 1);

            // Now expects 10 bytes for values
            Assert.assertEquals(10, parser.getNextExpectSize());
        } finally {
            Unsafe.free(mem, 100, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCloseResetsState() throws DecimalBinaryFormatParser.ParseException {
        try (DecimalBinaryFormatParser parser = new DecimalBinaryFormatParser()) {
            parser.reset();

            // Process some data
            long mem = Unsafe.malloc(10, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(mem, (byte) 2); // scale
                parser.processNextBinaryPart(mem);
                Assert.assertEquals(1, parser.getNextExpectSize());

                // Close should reset state
                parser.close();
                Assert.assertEquals(1, parser.getNextExpectSize()); // Back to expecting scale
            } finally {
                Unsafe.free(mem, 10, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private @Nullable Decimal256 parse(byte[] bytes) throws DecimalBinaryFormatParser.ParseException {
        long mem = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try (DecimalBinaryFormatParser parser = new DecimalBinaryFormatParser()) {
            parser.reset();

            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(mem + i, bytes[i]);
            }

            boolean finish;
            long start = mem;
            do {
                long size = parser.getNextExpectSize();
                finish = parser.processNextBinaryPart(start);
                start += size;
            } while (!finish);

            Decimal256 decimal = new Decimal256();
            if (!parser.load(decimal)) {
                return null;
            }
            return decimal;
        }
    }
}
