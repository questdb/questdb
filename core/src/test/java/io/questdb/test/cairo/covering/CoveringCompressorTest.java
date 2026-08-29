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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.idx.BitpackUtils;
import io.questdb.cairo.idx.CoveringCompressor;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class CoveringCompressorTest extends AbstractCairoTest {

    /**
     * Words allocated past the decode target and workspace so a test can prove
     * a checked decode wrote neither past {@code expectedCount} nor at all when
     * it rejected the block.
     */
    private static final int GUARD_WORDS = 4;
    private static final long GUARD_WORD = 0x5a5a5a5a5a5a5a5aL;
    private static final int RAW_BLOCK_FLAG = 0x80000000;

    @Test
    public void testAllExceptionsBlock() throws Exception {
        // When ALL values are ALP exceptions (irrational numbers), fillValue=0,
        // bw=0, and all values are stored in the exception list
        assertMemoryLeak(() -> {
            double[] input = {Math.PI, Math.E, Math.sqrt(2), Math.sqrt(3), Math.log(2),
                    Math.log(10), Math.sin(1), Math.cos(1)};
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                compressDoubles(srcAddr, count, 3, destAddr);
                double[] output = new double[count];
                decompressDoubles(destAddr, output);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals("value " + i + " (" + input[i] + ")",
                            Double.doubleToRawLongBits(input[i]), Double.doubleToRawLongBits(output[i]));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testAllIdenticalValues() throws Exception {
        assertMemoryLeak(() -> {
            int count = 64;
            double[] input = new double[count];
            Arrays.fill(input, 42.5);
            long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                int compressedSize = compressDoubles(srcAddr, count, 3, destAddr);
                Assert.assertEquals(CoveringCompressor.DOUBLE_HEADER_SIZE, compressedSize);
                double[] output = new double[count];
                decompressDoubles(destAddr, output);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals(42.5, output[i], 0.0);
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testAlpEncodeDecode() throws Exception {
        assertMemoryLeak(() -> {
            double[] values = {10.5, 20.5, 11.5, 30.5, 21.5, 12.5};
            int params = findParams(values);
            int e = params >>> 16;
            int f = params & 0xFFFF;

            for (double val : values) {
                long enc = CoveringCompressor.alpEncode(val, e, f);
                double dec = CoveringCompressor.alpDecode(enc, e, f);
                Assert.assertEquals(Double.doubleToRawLongBits(val), Double.doubleToRawLongBits(dec));
            }
        });
    }

    @Test
    public void testAlpTightPriceRange() throws Exception {
        assertMemoryLeak(() -> {
            double[] values = new double[256];
            for (int i = 0; i < 256; i++) {
                values[i] = 99.00 + i * 0.01;
            }
            int params = findParams(values);
            int e = params >>> 16;
            int f = params & 0xFFFF;

            Assert.assertTrue("exponent should be > 0", e > 0);
            for (double val : values) {
                long enc = CoveringCompressor.alpEncode(val, e, f);
                double dec = CoveringCompressor.alpDecode(enc, e, f);
                Assert.assertEquals(Double.doubleToRawLongBits(val), Double.doubleToRawLongBits(dec));
            }
        });
    }

    @Test
    public void testCheckedDecodeStatusNames() {
        final int[] statuses = {
                CoveringCompressor.DECODE_OK,
                CoveringCompressor.DECODE_ERR_ALP_EXPONENT,
                CoveringCompressor.DECODE_ERR_ARGUMENTS,
                CoveringCompressor.DECODE_ERR_BIT_WIDTH,
                CoveringCompressor.DECODE_ERR_COUNT_MISMATCH,
                CoveringCompressor.DECODE_ERR_EXCEPTION_COUNT,
                CoveringCompressor.DECODE_ERR_EXCEPTION_POSITION,
                CoveringCompressor.DECODE_ERR_LENGTH_MISMATCH,
                CoveringCompressor.DECODE_ERR_TARGET_CAPACITY,
                CoveringCompressor.DECODE_ERR_TRUNCATED_HEADER,
                CoveringCompressor.DECODE_ERR_WORKSPACE_CAPACITY,
        };
        final ObjHashSet<String> names = new ObjHashSet<>();
        for (int status : statuses) {
            final String name = CoveringCompressor.decodeStatusName(status);
            Assert.assertNotEquals("status " + status + " has no name", "unknown status", name);
            Assert.assertTrue("duplicate name for status " + status, names.add(name));
        }
        Assert.assertEquals("unknown status", CoveringCompressor.decodeStatusName(-999));
    }

    @Test
    public void testCheckedDoublesRejectsBadAlpExponents() throws Exception {
        assertMemoryLeak(() -> {
            final double[] input = {1.5, 2.25, 3.125, 4.0625};
            withDoubleBlock(input, (block, storedLength, outAddr, wsAddr) -> {
                // e and f index the F10/IF10 power tables, which hold exponents 0-18.
                Unsafe.putByte(block + 4, (byte) 19);
                Assert.assertEquals(CoveringCompressor.DECODE_ERR_ALP_EXPONENT,
                        checkedDoubles(block, storedLength, input.length, outAddr, wsAddr));
                Unsafe.putByte(block + 4, (byte) 0xFF);
                Assert.assertEquals(CoveringCompressor.DECODE_ERR_ALP_EXPONENT,
                        checkedDoubles(block, storedLength, input.length, outAddr, wsAddr));
                Unsafe.putByte(block + 4, (byte) 0);
                Unsafe.putByte(block + 5, (byte) 19);
                Assert.assertEquals(CoveringCompressor.DECODE_ERR_ALP_EXPONENT,
                        checkedDoubles(block, storedLength, input.length, outAddr, wsAddr));
                assertUntouched(outAddr, input.length);
                assertUntouched(wsAddr, input.length);
            });
        });
    }

    @Test
    public void testCheckedDoublesRejectsBadArguments() throws Exception {
        assertMemoryLeak(() -> {
            final double[] input = {1.5, 2.25, 3.125, 4.0625};
            final int count = input.length;
            withDoubleBlock(input, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals("null source", CoveringCompressor.DECODE_ERR_ARGUMENTS,
                        CoveringCompressor.decompressDoublesToAddrChecked(
                                0L, storedLength, count, outAddr, count, wsAddr, count));
                Assert.assertEquals("negative count", CoveringCompressor.DECODE_ERR_ARGUMENTS,
                        CoveringCompressor.decompressDoublesToAddrChecked(
                                block, storedLength, -1, outAddr, count, wsAddr, count));
                Assert.assertEquals("negative target capacity", CoveringCompressor.DECODE_ERR_ARGUMENTS,
                        CoveringCompressor.decompressDoublesToAddrChecked(
                                block, storedLength, count, outAddr, -1, wsAddr, count));
                Assert.assertEquals("negative workspace capacity", CoveringCompressor.DECODE_ERR_ARGUMENTS,
                        CoveringCompressor.decompressDoublesToAddrChecked(
                                block, storedLength, count, outAddr, count, wsAddr, -1));
                Assert.assertEquals("null target", CoveringCompressor.DECODE_ERR_ARGUMENTS,
                        CoveringCompressor.decompressDoublesToAddrChecked(
                                block, storedLength, count, 0L, count, wsAddr, count));
                Assert.assertEquals("target too small", CoveringCompressor.DECODE_ERR_TARGET_CAPACITY,
                        CoveringCompressor.decompressDoublesToAddrChecked(
                                block, storedLength, count, outAddr, count - 1, wsAddr, count));
                assertUntouched(outAddr, count);
                assertUntouched(wsAddr, count);
            });
        });
    }

    @Test
    public void testCheckedDoublesRejectsBadBitWidth() throws Exception {
        assertMemoryLeak(() -> {
            final double[] input = {1.5, 2.25, 3.125, 4.0625};
            withDoubleBlock(input, (block, storedLength, outAddr, wsAddr) -> {
                Unsafe.putByte(block + 6, (byte) 65);
                Assert.assertEquals(CoveringCompressor.DECODE_ERR_BIT_WIDTH,
                        checkedDoubles(block, storedLength, input.length, outAddr, wsAddr));
                Unsafe.putByte(block + 6, (byte) 0xFF);
                Assert.assertEquals(CoveringCompressor.DECODE_ERR_BIT_WIDTH,
                        checkedDoubles(block, storedLength, input.length, outAddr, wsAddr));
                assertUntouched(outAddr, input.length);
                assertUntouched(wsAddr, input.length);
            });
        });
    }

    @Test
    public void testCheckedDoublesRejectsBadCount() throws Exception {
        assertMemoryLeak(() -> {
            final double[] input = {1.5, 2.25, 3.125, 4.0625};
            final int count = input.length;
            withDoubleBlock(input, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals(CoveringCompressor.DECODE_ERR_COUNT_MISMATCH,
                        checkedDoubles(block, storedLength, count + 1, outAddr, wsAddr));
                Assert.assertEquals(CoveringCompressor.DECODE_ERR_COUNT_MISMATCH,
                        checkedDoubles(block, storedLength, count - 1, outAddr, wsAddr));
                // The raw block layout carries the count with RAW_BLOCK_FLAG set, so its
                // stored count is negative and can never match a checkpoint page's count.
                Unsafe.putInt(block, count | RAW_BLOCK_FLAG);
                Assert.assertEquals(CoveringCompressor.DECODE_ERR_COUNT_MISMATCH,
                        checkedDoubles(block, storedLength, count, outAddr, wsAddr));
                assertUntouched(outAddr, count);
                assertUntouched(wsAddr, count);
            });
        });
    }

    @Test
    public void testCheckedDoublesRejectsBadExceptionCount() throws Exception {
        assertMemoryLeak(() -> {
            final double[] input = {1.5, Double.NaN, 2.5, 3.5};
            withDoubleBlock(input, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals("premise: one exception", 1, Unsafe.getInt(block + 7));
                Unsafe.putInt(block + 7, input.length + 1);
                Assert.assertEquals(CoveringCompressor.DECODE_ERR_EXCEPTION_COUNT,
                        checkedDoubles(block, storedLength, input.length, outAddr, wsAddr));
                Unsafe.putInt(block + 7, -1);
                Assert.assertEquals(CoveringCompressor.DECODE_ERR_EXCEPTION_COUNT,
                        checkedDoubles(block, storedLength, input.length, outAddr, wsAddr));
                // A count that stays in range but no longer matches the stored table
                // extent is caught by the length check.
                Unsafe.putInt(block + 7, 0);
                Assert.assertEquals(CoveringCompressor.DECODE_ERR_LENGTH_MISMATCH,
                        checkedDoubles(block, storedLength, input.length, outAddr, wsAddr));
                assertUntouched(outAddr, input.length);
                assertUntouched(wsAddr, input.length);
            });
        });
    }

    @Test
    public void testCheckedDoublesRejectsBadExceptionPositions() throws Exception {
        assertMemoryLeak(() -> {
            final double[] input = {1.5, Double.NaN, 2.5, Double.POSITIVE_INFINITY, 3.5};
            final int count = input.length;
            withDoubleBlock(input, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals("premise: two exceptions", 2, Unsafe.getInt(block + 7));
                final int bw = Unsafe.getByte(block + 6) & 0xFF;
                final long positions = block + CoveringCompressor.DOUBLE_HEADER_SIZE
                        + BitpackUtils.packedDataSize(count, bw);
                Assert.assertEquals(1, Unsafe.getInt(positions));
                Assert.assertEquals(3, Unsafe.getInt(positions + Integer.BYTES));

                Unsafe.putInt(positions, count);
                Assert.assertEquals("out of range", CoveringCompressor.DECODE_ERR_EXCEPTION_POSITION,
                        checkedDoubles(block, storedLength, count, outAddr, wsAddr));
                Unsafe.putInt(positions, -1);
                Assert.assertEquals("negative", CoveringCompressor.DECODE_ERR_EXCEPTION_POSITION,
                        checkedDoubles(block, storedLength, count, outAddr, wsAddr));
                Unsafe.putInt(positions, 3);
                Assert.assertEquals("duplicated", CoveringCompressor.DECODE_ERR_EXCEPTION_POSITION,
                        checkedDoubles(block, storedLength, count, outAddr, wsAddr));
                Unsafe.putInt(positions, 4);
                Assert.assertEquals("unsorted", CoveringCompressor.DECODE_ERR_EXCEPTION_POSITION,
                        checkedDoubles(block, storedLength, count, outAddr, wsAddr));
                assertUntouched(outAddr, count);
                assertUntouched(wsAddr, count);

                // Restoring the position makes the same block decode, which proves the
                // rejections above came from the position table and nothing else.
                Unsafe.putInt(positions, 1);
                Assert.assertEquals(CoveringCompressor.DECODE_OK,
                        checkedDoubles(block, storedLength, count, outAddr, wsAddr));
                assertExactDoubles(input, outAddr);
            });
        });
    }

    @Test
    public void testCheckedDoublesRejectsBadFraming() throws Exception {
        assertMemoryLeak(() -> {
            final double[] input = {1.5, 2.25, 3.125, 4.0625};
            final int count = input.length;
            withDoubleBlock(input, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals("truncated header", CoveringCompressor.DECODE_ERR_TRUNCATED_HEADER,
                        checkedDoubles(block, CoveringCompressor.DOUBLE_HEADER_SIZE - 1, count, outAddr, wsAddr));
                Assert.assertEquals("truncated data", CoveringCompressor.DECODE_ERR_LENGTH_MISMATCH,
                        checkedDoubles(block, storedLength - 1, count, outAddr, wsAddr));
                Assert.assertEquals("trailing data", CoveringCompressor.DECODE_ERR_LENGTH_MISMATCH,
                        checkedDoubles(block, storedLength + 1, count, outAddr, wsAddr));
                assertUntouched(outAddr, count);
                assertUntouched(wsAddr, count);
            });
        });
    }

    @Test
    public void testCheckedDoublesRejectsMissingWorkspace() throws Exception {
        assertMemoryLeak(() -> {
            final double[] input = {1.5, 2.25, 3.125, 4.0625};
            final int count = input.length;
            withDoubleBlock(input, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals("null workspace", CoveringCompressor.DECODE_ERR_WORKSPACE_CAPACITY,
                        CoveringCompressor.decompressDoublesToAddrChecked(
                                block, storedLength, count, outAddr, count, 0L, count));
                Assert.assertEquals("workspace too small", CoveringCompressor.DECODE_ERR_WORKSPACE_CAPACITY,
                        CoveringCompressor.decompressDoublesToAddrChecked(
                                block, storedLength, count, outAddr, count, wsAddr, count - 1));
                assertUntouched(outAddr, count);
                assertUntouched(wsAddr, count);
            });
        });
    }

    @Test
    public void testCheckedDoublesRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            final double[] input = new double[512];
            for (int i = 0; i < input.length; i++) {
                input[i] = 99.00 + i * 0.01;
            }
            withDoubleBlock(input, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertTrue("ALP block must be smaller than raw",
                        storedLength < input.length * Double.BYTES);
                Assert.assertEquals(CoveringCompressor.DECODE_OK,
                        checkedDoubles(block, storedLength, input.length, outAddr, wsAddr));
                assertExactDoubles(input, outAddr);
                assertGuardWords(outAddr, input.length);
            });
        });
    }

    @Test
    public void testCheckedDoublesRoundTripSpecialValues() throws Exception {
        assertMemoryLeak(() -> {
            // A NaN payload, both infinities and signed zero must come back bit-exact:
            // the encoder cannot represent them, so they travel in the exception table.
            final double[] special = {
                    1.5,
                    Double.longBitsToDouble(0x7ff8000000000123L),
                    Double.POSITIVE_INFINITY,
                    -0.0,
                    Double.NEGATIVE_INFINITY,
                    0.0,
                    Double.MIN_VALUE,
                    -Double.MAX_VALUE,
            };
            withDoubleBlock(special, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals(CoveringCompressor.DECODE_OK,
                        checkedDoubles(block, storedLength, special.length, outAddr, wsAddr));
                assertExactDoubles(special, outAddr);
                assertGuardWords(outAddr, special.length);
            });

            final double[] identical = new double[64];
            Arrays.fill(identical, 42.5);
            withDoubleBlock(identical, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals(CoveringCompressor.DOUBLE_HEADER_SIZE, storedLength);
                Assert.assertEquals(CoveringCompressor.DECODE_OK,
                        checkedDoubles(block, storedLength, identical.length, outAddr, wsAddr));
                assertExactDoubles(identical, outAddr);
                assertGuardWords(outAddr, identical.length);
            });

            // An empty block is header-only and needs neither target nor workspace.
            withDoubleBlock(new double[0], (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals(CoveringCompressor.DOUBLE_HEADER_SIZE, storedLength);
                Assert.assertEquals(CoveringCompressor.DECODE_OK,
                        CoveringCompressor.decompressDoublesToAddrChecked(
                                block, storedLength, 0, 0L, 0, 0L, 0));
                assertUntouched(outAddr, 0);
            });
        });
    }

    @Test
    public void testCheckedLongsRejectsBadArguments() throws Exception {
        assertMemoryLeak(() -> {
            final long[] input = {1000L, 1005L, 1002L, 1008L};
            final int count = input.length;
            withLongBlock(input, false, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals("null source", CoveringCompressor.DECODE_ERR_ARGUMENTS,
                        CoveringCompressor.decompressLongsToAddrChecked(
                                0L, storedLength, count, outAddr, count, wsAddr, count));
                Assert.assertEquals("negative count", CoveringCompressor.DECODE_ERR_ARGUMENTS,
                        CoveringCompressor.decompressLongsToAddrChecked(
                                block, storedLength, -1, outAddr, count, wsAddr, count));
                Assert.assertEquals("negative target capacity", CoveringCompressor.DECODE_ERR_ARGUMENTS,
                        CoveringCompressor.decompressLongsToAddrChecked(
                                block, storedLength, count, outAddr, -1, wsAddr, count));
                Assert.assertEquals("negative workspace capacity", CoveringCompressor.DECODE_ERR_ARGUMENTS,
                        CoveringCompressor.decompressLongsToAddrChecked(
                                block, storedLength, count, outAddr, count, wsAddr, -1));
                Assert.assertEquals("null target", CoveringCompressor.DECODE_ERR_ARGUMENTS,
                        CoveringCompressor.decompressLongsToAddrChecked(
                                block, storedLength, count, 0L, count, wsAddr, count));
                Assert.assertEquals("target too small", CoveringCompressor.DECODE_ERR_TARGET_CAPACITY,
                        CoveringCompressor.decompressLongsToAddrChecked(
                                block, storedLength, count, outAddr, count - 1, wsAddr, count));
                assertUntouched(outAddr, count);
                assertUntouched(wsAddr, count);
            });
        });
    }

    @Test
    public void testCheckedLongsRejectsBadBitWidth() throws Exception {
        assertMemoryLeak(() -> {
            final long[] input = {1000L, 1005L, 1002L, 1008L};
            withLongBlock(input, false, (block, storedLength, outAddr, wsAddr) -> {
                Unsafe.putByte(block + 4, (byte) 65);
                Assert.assertEquals("plain width above 64", CoveringCompressor.DECODE_ERR_BIT_WIDTH,
                        checkedLongs(block, storedLength, input.length, outAddr, wsAddr));
                // Only the top flag bit set is not a linear-prediction block, so the byte
                // reads as a plain width of 128.
                Unsafe.putByte(block + 4, (byte) 0x80);
                Assert.assertEquals("half-set flag", CoveringCompressor.DECODE_ERR_BIT_WIDTH,
                        checkedLongs(block, storedLength, input.length, outAddr, wsAddr));
                assertUntouched(outAddr, input.length);
                assertUntouched(wsAddr, input.length);
            });
        });
    }

    @Test
    public void testCheckedLongsRejectsBadCount() throws Exception {
        assertMemoryLeak(() -> {
            final long[] input = {1000L, 1005L, 1002L, 1008L};
            final int count = input.length;
            withLongBlock(input, false, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals(CoveringCompressor.DECODE_ERR_COUNT_MISMATCH,
                        checkedLongs(block, storedLength, count + 1, outAddr, wsAddr));
                Assert.assertEquals(CoveringCompressor.DECODE_ERR_COUNT_MISMATCH,
                        checkedLongs(block, storedLength, count - 1, outAddr, wsAddr));
                Unsafe.putInt(block, -1);
                Assert.assertEquals(CoveringCompressor.DECODE_ERR_COUNT_MISMATCH,
                        checkedLongs(block, storedLength, count, outAddr, wsAddr));
                assertUntouched(outAddr, count);
                assertUntouched(wsAddr, count);
            });
        });
    }

    @Test
    public void testCheckedLongsRejectsBadFraming() throws Exception {
        assertMemoryLeak(() -> {
            final long[] input = {1000L, 1005L, 1002L, 1008L};
            final int count = input.length;
            withLongBlock(input, false, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals("truncated header", CoveringCompressor.DECODE_ERR_TRUNCATED_HEADER,
                        checkedLongs(block, CoveringCompressor.LONG_HEADER_SIZE - 1, count, outAddr, wsAddr));
                Assert.assertEquals("truncated data", CoveringCompressor.DECODE_ERR_LENGTH_MISMATCH,
                        checkedLongs(block, storedLength - 1, count, outAddr, wsAddr));
                Assert.assertEquals("trailing data", CoveringCompressor.DECODE_ERR_LENGTH_MISMATCH,
                        checkedLongs(block, storedLength + 1, count, outAddr, wsAddr));
                assertUntouched(outAddr, count);
                assertUntouched(wsAddr, count);
            });
        });
    }

    @Test
    public void testCheckedLongsRejectsTruncatedLinearPredHeader() throws Exception {
        assertMemoryLeak(() -> {
            final long[] input = new long[64];
            for (int i = 0; i < input.length; i++) {
                input[i] = 1_700_000_000_000_000L + i * 1_000_000L;
            }
            withLongBlock(input, true, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals("premise: linear-prediction block",
                        CoveringCompressor.LONG_LINEAR_PRED_HEADER_SIZE, storedLength);
                Assert.assertEquals(CoveringCompressor.DECODE_ERR_TRUNCATED_HEADER,
                        checkedLongs(block, CoveringCompressor.LONG_LINEAR_PRED_HEADER_SIZE - 1,
                                input.length, outAddr, wsAddr));
                assertUntouched(outAddr, input.length);
                assertUntouched(wsAddr, input.length);
            });
        });
    }

    @Test
    public void testCheckedLongsRejectsUnusableLinearPredWorkspace() throws Exception {
        assertMemoryLeak(() -> {
            // Jitter keeps the residual width above zero, so the decode really does
            // need the workspace rather than tolerating a missing one.
            final long[] input = new long[64];
            for (int i = 0; i < input.length; i++) {
                input[i] = 1_700_000_000_000_000L + i * 1_000_000L + (i % 7);
            }
            final int count = input.length;
            withLongBlock(input, true, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals("premise: linear-prediction block",
                        0xC0, Unsafe.getByte(block + 4) & 0xC0);
                Assert.assertEquals("null workspace", CoveringCompressor.DECODE_ERR_WORKSPACE_CAPACITY,
                        CoveringCompressor.decompressLongsToAddrChecked(
                                block, storedLength, count, outAddr, count, 0L, count));
                Assert.assertEquals("workspace too small", CoveringCompressor.DECODE_ERR_WORKSPACE_CAPACITY,
                        CoveringCompressor.decompressLongsToAddrChecked(
                                block, storedLength, count, outAddr, count, wsAddr, count - 1));
                assertUntouched(outAddr, count);
                assertUntouched(wsAddr, count);
            });
        });
    }

    @Test
    public void testCheckedLongsRoundTripEdgeValues() throws Exception {
        assertMemoryLeak(() -> {
            // All-equal values pack to nothing, so the block is header-only.
            final long[] identical = {7L, 7L, 7L, 7L};
            withLongBlock(identical, false, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals(CoveringCompressor.LONG_HEADER_SIZE, storedLength);
                Assert.assertEquals(CoveringCompressor.DECODE_OK,
                        checkedLongs(block, storedLength, identical.length, outAddr, wsAddr));
                assertExactLongs(identical, outAddr);
                assertGuardWords(outAddr, identical.length);
            });

            // A span that overflows signed arithmetic forces the full 64-bit width.
            final long[] extremes = {Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1L, 1L};
            withLongBlock(extremes, false, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals("premise: 64-bit width", 64, Unsafe.getByte(block + 4) & 0xFF);
                Assert.assertEquals(CoveringCompressor.LONG_HEADER_SIZE + extremes.length * Long.BYTES,
                        storedLength);
                Assert.assertEquals(CoveringCompressor.DECODE_OK,
                        checkedLongs(block, storedLength, extremes.length, outAddr, wsAddr));
                assertExactLongs(extremes, outAddr);
                assertGuardWords(outAddr, extremes.length);
            });

            withLongBlock(new long[0], false, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals(CoveringCompressor.LONG_HEADER_SIZE, storedLength);
                Assert.assertEquals(CoveringCompressor.DECODE_OK,
                        CoveringCompressor.decompressLongsToAddrChecked(
                                block, storedLength, 0, 0L, 0, 0L, 0));
                assertUntouched(outAddr, 0);
            });
        });
    }

    @Test
    public void testCheckedLongsRoundTripLinearPred() throws Exception {
        assertMemoryLeak(() -> {
            final long[] input = new long[512];
            for (int i = 0; i < input.length; i++) {
                input[i] = 1_700_000_000_000_000L + i * 1_000_000L + (i % 11);
            }
            withLongBlock(input, true, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals("premise: linear-prediction block",
                        0xC0, Unsafe.getByte(block + 4) & 0xC0);
                Assert.assertTrue("linear residuals must be smaller than raw",
                        storedLength < input.length * Long.BYTES);
                Assert.assertEquals(CoveringCompressor.DECODE_OK,
                        checkedLongs(block, storedLength, input.length, outAddr, wsAddr));
                assertExactLongs(input, outAddr);
                assertGuardWords(outAddr, input.length);
                assertGuardWords(wsAddr, input.length);
            });
        });
    }

    @Test
    public void testCheckedLongsRoundTripPlain() throws Exception {
        assertMemoryLeak(() -> {
            final long[] input = {1000L, 1005L, 1002L, 1008L, 1001L, 1003L, 1007L, 1004L};
            withLongBlock(input, false, (block, storedLength, outAddr, wsAddr) -> {
                Assert.assertEquals(CoveringCompressor.DECODE_OK,
                        checkedLongs(block, storedLength, input.length, outAddr, wsAddr));
                assertExactLongs(input, outAddr);
                assertGuardWords(outAddr, input.length);
                // A plain block unpacks straight into the target, so it accepts no
                // workspace at all.
                Unsafe.setMemory(outAddr, (long) input.length * Long.BYTES, (byte) 0);
                Assert.assertEquals(CoveringCompressor.DECODE_OK,
                        CoveringCompressor.decompressLongsToAddrChecked(
                                block, storedLength, input.length, outAddr, input.length, 0L, 0));
                assertExactLongs(input, outAddr);
                assertGuardWords(outAddr, input.length);
            });
        });
    }

    @Test
    public void testCompressDecompressDoubles() throws Exception {
        assertMemoryLeak(() -> {
            double[] input = {10.5, 20.5, 11.5, 30.5, 21.5, 12.5, 15.0, 25.0};
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                int compressedSize = compressDoubles(srcAddr, count, 3, destAddr);
                Assert.assertTrue("compressed should be smaller", compressedSize < count * Double.BYTES);
                double[] output = new double[count];
                decompressDoubles(destAddr, output);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals(Double.doubleToRawLongBits(input[i]), Double.doubleToRawLongBits(output[i]));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCompressDecompressFloatAsInt() throws Exception {
        // FLOAT is compressed via compressInts (raw int bits) and reconstructed with Float.intBitsToFloat
        assertMemoryLeak(() -> {
            float[] input = {1.5f, Float.NaN, -0.0f, Float.MAX_VALUE, Float.MIN_VALUE, Float.MIN_NORMAL,
                    Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 0.0f, -42.75f};
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.INT), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putFloat(srcAddr + (long) i * Float.BYTES, input[i]);
                }
                compressInts(srcAddr, count, destAddr);
                int[] output = new int[count];
                decompressInts(destAddr, output);
                for (int i = 0; i < count; i++) {
                    float recovered = Float.intBitsToFloat(output[i]);
                    Assert.assertEquals("float " + i + " (" + input[i] + ")",
                            Float.floatToRawIntBits(input[i]), Float.floatToRawIntBits(recovered));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.INT), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCompressDecompressInts() throws Exception {
        assertMemoryLeak(() -> {
            int[] input = {100, 200, 150, 300, 250, 120, 180, 280};
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.INT), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putInt(srcAddr + (long) i * Integer.BYTES, input[i]);
                }
                int compressedSize = compressInts(srcAddr, count, destAddr);
                Assert.assertTrue("compressed should be smaller", compressedSize < count * Integer.BYTES);
                int[] output = new int[count];
                decompressInts(destAddr, output);
                Assert.assertArrayEquals(input, output);
            } finally {
                Unsafe.free(srcAddr, (long) count * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.INT), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCompressDecompressLongs() throws Exception {
        assertMemoryLeak(() -> {
            long[] input = {1000L, 1005L, 1002L, 1008L, 1001L, 1003L, 1007L, 1004L};
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.LONG), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putLong(srcAddr + (long) i * Long.BYTES, input[i]);
                }
                int compressedSize = CoveringCompressor.compressLongs(srcAddr, count, destAddr);
                Assert.assertTrue("compressed should be smaller", compressedSize < count * Long.BYTES);
                long[] output = new long[count];
                decompressLongs(destAddr, output);
                Assert.assertArrayEquals(input, output);
            } finally {
                Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.LONG), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCompressFloatsRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            float[] input = {
                    1.5f, 2.25f, 3.125f, -4.0625f, 0.0f, -0.0f,
                    1e-3f, 1e3f, 42.0f, 42.0f, 42.0f, Float.MIN_NORMAL
            };
            int count = input.length;
            int destCap = CoveringCompressor.maxCompressedSize(count, ColumnType.FLOAT);
            long srcAddr = Unsafe.malloc((long) count * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(destCap, MemoryTag.NATIVE_DEFAULT);
            long encAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long excAddr = Unsafe.malloc(count, MemoryTag.NATIVE_DEFAULT);
            long decAddr = Unsafe.malloc((long) count * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
            long decodeWsAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putFloat(srcAddr + (long) i * Float.BYTES, input[i]);
                }
                int sz = CoveringCompressor.compressFloats(srcAddr, count, destAddr, encAddr, excAddr);
                Assert.assertTrue("compressed size must be positive", sz > 0);

                CoveringCompressor.decompressFloatsToAddr(destAddr, decAddr, decodeWsAddr);
                for (int i = 0; i < count; i++) {
                    float actual = Unsafe.getFloat(decAddr + (long) i * Float.BYTES);
                    Assert.assertEquals("value " + i,
                            Float.floatToRawIntBits(input[i]), Float.floatToRawIntBits(actual));
                }
            } finally {
                Unsafe.free(decodeWsAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(decAddr, (long) count * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(excAddr, count, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(encAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, destCap, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(srcAddr, (long) count * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCompressionRatio() throws Exception {
        assertMemoryLeak(() -> {
            int count = 256;
            double[] input = new double[count];
            Random rng = new Random(123);
            for (int i = 0; i < count; i++) {
                input[i] = 10.0 + rng.nextInt(2000) * 0.01;
            }
            long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                int compressedSize = compressDoubles(srcAddr, count, 3, destAddr);
                double ratio = (double) (count * Double.BYTES) / compressedSize;
                Assert.assertTrue("prices should compress at least 2x, got " + ratio + "x", ratio >= 2.0);
                double[] output = new double[count];
                decompressDoubles(destAddr, output);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals(Double.doubleToRawLongBits(input[i]), Double.doubleToRawLongBits(output[i]));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }


    @Test
    public void testEmptyBlockBwIsZero() throws Exception {
        assertMemoryLeak(() -> {
            int destCap = CoveringCompressor.maxCompressedSize(1, ColumnType.LONG);
            long destAddr = Unsafe.malloc(destCap, MemoryTag.NATIVE_DEFAULT);
            try {
                int sz = CoveringCompressor.compressLongs(0L, 0, destAddr);
                Assert.assertTrue("header must fit", sz > 0);
                Assert.assertEquals(0, Unsafe.getInt(destAddr));
                Assert.assertEquals(0, Unsafe.getByte(destAddr + 4));
            } finally {
                Unsafe.free(destAddr, destCap, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testLinearPredFallbackOnBwOverflow() throws Exception {
        assertMemoryLeak(() -> {
            long[] input = {0L, Long.MIN_VALUE, Long.MAX_VALUE, 0L};
            int count = input.length;
            int destCap = CoveringCompressor.maxCompressedSize(count, ColumnType.TIMESTAMP);
            long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(destCap, MemoryTag.NATIVE_DEFAULT);
            long workAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putLong(srcAddr + (long) i * Long.BYTES, input[i]);
                }
                int sz = CoveringCompressor.compressLongsLinearPred(srcAddr, count, destAddr, workAddr);
                Assert.assertTrue("compressed size must be positive", sz > 0);

                int flagByte = Unsafe.getByte(destAddr + 4) & 0xFF;
                Assert.assertNotEquals("expected plain FoR after fallback, got linear-pred",
                        0xC0, flagByte & 0xC0);

                long[] output = new long[count];
                decompressLongs(destAddr, output);
                Assert.assertArrayEquals(input, output);
            } finally {
                Unsafe.free(workAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, destCap, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMaxCompressedSizeRejectsUnsupportedTypes() {
        int[] unsupported = {ColumnType.VARCHAR, ColumnType.STRING, ColumnType.BINARY};
        for (int type : unsupported) {
            Assert.assertThrows("column type " + type,
                    AssertionError.class,
                    () -> CoveringCompressor.maxCompressedSize(100, type));
        }
    }

    @Test
    public void testNaNAndInfAreExceptions() throws Exception {
        assertMemoryLeak(() -> {
            double[] input = {10.5, Double.NaN, 11.5, Double.POSITIVE_INFINITY, 12.5,
                    Double.NEGATIVE_INFINITY, -0.0, 13.5};
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                compressDoubles(srcAddr, count, 3, destAddr);
                double[] output = new double[count];
                decompressDoubles(destAddr, output);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals(Double.doubleToRawLongBits(input[i]), Double.doubleToRawLongBits(output[i]));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRandomDoublesLossless() throws Exception {
        assertMemoryLeak(() -> {
            Random rng = new Random(42);
            int count = 256;
            double[] input = new double[count];
            for (int i = 0; i < count; i++) {
                input[i] = rng.nextDouble() * 1000.0;
            }
            long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                compressDoubles(srcAddr, count, 3, destAddr);
                double[] output = new double[count];
                decompressDoubles(destAddr, output);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals(Double.doubleToRawLongBits(input[i]), Double.doubleToRawLongBits(output[i]));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReadLongAtBitWidth63HighBitsRoundTrip() throws Exception {
        // BitpackUtils.unpackValue uses a per-byte OR loop. When bitShift + bitWidth > 64,
        // the loop iterates 9 times, and the 9th iteration shifts a byte by 64 (Java treats
        // as shift-by-0), OR'ing the value's high bits into the low byte where they get
        // discarded by the subsequent right-shift. For bw=63 this drops bits 57..62 of the
        // offset for any index where (index * 63) % 8 >= 2, i.e. indices {1, 2, 3, 4, 5, 6}
        // out of every 8.
        assertMemoryLeak(() -> {
            // forBase=0, forMax=(1<<62)|3 -> span requires 63 bits.
            long[] input = {
                    0L,
                    1L << 62,
                    1L,
                    (1L << 62) | 1L,
                    2L,
                    (1L << 62) | 2L,
                    3L,
                    (1L << 62) | 3L,
            };
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.LONG), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putLong(srcAddr + (long) i * Long.BYTES, input[i]);
                }
                CoveringCompressor.compressLongs(srcAddr, count, destAddr);
                // Assert the compressor really picked bw=63, otherwise the test premise is invalid.
                int bw = Unsafe.getByte(destAddr + 4) & 0xFF;
                Assert.assertEquals("expected bw=63", 63, bw);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals("readLongAt at index " + i, input[i], CoveringCompressor.readLongAt(destAddr, i));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.LONG), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSubnormalsAndExtremes() throws Exception {
        // Edge case doubles: subnormals, max/min values, zero
        assertMemoryLeak(() -> {
            double[] input = {Double.MIN_VALUE, Double.MIN_NORMAL, Double.MAX_VALUE,
                    -Double.MAX_VALUE, 0.0, -0.0, 1e308, 1e-308, -1e-308};
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                compressDoubles(srcAddr, count, 3, destAddr);
                double[] output = new double[count];
                decompressDoubles(destAddr, output);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals("value " + i + " (" + input[i] + ")",
                            Double.doubleToRawLongBits(input[i]), Double.doubleToRawLongBits(output[i]));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void assertExactDoubles(double[] expected, long addr) {
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals("value " + i + " (" + expected[i] + ")",
                    Double.doubleToRawLongBits(expected[i]),
                    Double.doubleToRawLongBits(Unsafe.getDouble(addr + (long) i * Double.BYTES)));
        }
    }

    private static void assertExactLongs(long[] expected, long addr) {
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals("value " + i, expected[i], Unsafe.getLong(addr + (long) i * Long.BYTES));
        }
    }

    private static void assertGuardWords(long addr, int count) {
        for (int i = count; i < count + GUARD_WORDS; i++) {
            Assert.assertEquals("decode wrote past element " + count,
                    GUARD_WORD, Unsafe.getLong(addr + (long) i * Long.BYTES));
        }
    }

    private static void assertUntouched(long addr, int count) {
        for (int i = 0; i < count + GUARD_WORDS; i++) {
            Assert.assertEquals("rejected decode wrote element " + i,
                    GUARD_WORD, Unsafe.getLong(addr + (long) i * Long.BYTES));
        }
    }

    private static int checkedDoubles(long blockAddr, int storedLength, int expectedCount, long outAddr, long wsAddr) {
        return CoveringCompressor.decompressDoublesToAddrChecked(blockAddr, storedLength, expectedCount,
                outAddr, expectedCount + GUARD_WORDS, wsAddr, expectedCount + GUARD_WORDS);
    }

    private static int checkedLongs(long blockAddr, int storedLength, int expectedCount, long outAddr, long wsAddr) {
        return CoveringCompressor.decompressLongsToAddrChecked(blockAddr, storedLength, expectedCount,
                outAddr, expectedCount + GUARD_WORDS, wsAddr, expectedCount + GUARD_WORDS);
    }

    private static int compressDoubles(long srcAddr, int count, int valueShift, long destAddr) {
        long encAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long excAddr = Unsafe.malloc(count, MemoryTag.NATIVE_DEFAULT);
        try {
            return CoveringCompressor.compressDoubles(srcAddr, count, valueShift, destAddr, encAddr, excAddr);
        } finally {
            Unsafe.free(excAddr, count, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(encAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static int compressInts(long srcAddr, int count, long destAddr) {
        long wsAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            return CoveringCompressor.compressInts(srcAddr, count, destAddr, wsAddr);
        } finally {
            Unsafe.free(wsAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void decompressDoubles(long srcAddr, double[] output) {
        int count = output.length;
        long outAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        long wsAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            CoveringCompressor.decompressDoublesToAddr(srcAddr, outAddr, wsAddr);
            for (int i = 0; i < count; i++) {
                output[i] = Unsafe.getDouble(outAddr + (long) i * Double.BYTES);
            }
        } finally {
            Unsafe.free(wsAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void decompressInts(long srcAddr, int[] output) {
        int count = output.length;
        long outAddr = Unsafe.malloc((long) count * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        long wsAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            CoveringCompressor.decompressIntsToAddr(srcAddr, outAddr, wsAddr);
            for (int i = 0; i < count; i++) {
                output[i] = Unsafe.getInt(outAddr + (long) i * Integer.BYTES);
            }
        } finally {
            Unsafe.free(wsAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outAddr, (long) count * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void decompressLongs(long srcAddr, long[] output) {
        int count = output.length;
        long outAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long wsAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            CoveringCompressor.decompressLongsToAddr(srcAddr, outAddr, wsAddr);
            for (int i = 0; i < count; i++) {
                output[i] = Unsafe.getLong(outAddr + (long) i * Long.BYTES);
            }
        } finally {
            Unsafe.free(wsAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void fillGuardWords(long addr, int count) {
        for (int i = 0; i < count + GUARD_WORDS; i++) {
            Unsafe.putLong(addr + (long) i * Long.BYTES, GUARD_WORD);
        }
    }

    private static int findParams(double[] values) {
        long addr = Unsafe.malloc((long) values.length * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < values.length; i++) {
                Unsafe.putDouble(addr + (long) i * Double.BYTES, values[i]);
            }
            return CoveringCompressor.findBestAlpParams(addr, values.length, 3);
        } finally {
            Unsafe.free(addr, (long) values.length * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Compresses {@code input} into an ALP block and runs {@code body} against it
     * with a decode target and workspace that both carry {@link #GUARD_WORDS}
     * sentinel words past their declared capacity.
     */
    private static void withDoubleBlock(double[] input, BlockTest body) {
        final int count = input.length;
        final long srcSize = Math.max(1, (long) count * Double.BYTES);
        final long encodedSize = Math.max(1, (long) count * Long.BYTES);
        final long exceptionSize = Math.max(1, count);
        final long guardedSize = (long) (count + GUARD_WORDS) * Long.BYTES;
        final int destCapacity = CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE);
        final long srcAddr = Unsafe.malloc(srcSize, MemoryTag.NATIVE_DEFAULT);
        final long destAddr = Unsafe.malloc(destCapacity, MemoryTag.NATIVE_DEFAULT);
        final long encodedAddr = Unsafe.malloc(encodedSize, MemoryTag.NATIVE_DEFAULT);
        final long exceptionAddr = Unsafe.malloc(exceptionSize, MemoryTag.NATIVE_DEFAULT);
        final long outAddr = Unsafe.malloc(guardedSize, MemoryTag.NATIVE_DEFAULT);
        final long workspaceAddr = Unsafe.malloc(guardedSize, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < count; i++) {
                Unsafe.putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
            }
            final int storedLength = CoveringCompressor.compressDoubles(
                    srcAddr, count, 3, destAddr, encodedAddr, exceptionAddr);
            Assert.assertTrue("compressed size must fit the destination",
                    storedLength > 0 && storedLength <= destCapacity);
            fillGuardWords(outAddr, count);
            fillGuardWords(workspaceAddr, count);
            body.run(destAddr, storedLength, outAddr, workspaceAddr);
        } finally {
            Unsafe.free(workspaceAddr, guardedSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outAddr, guardedSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(exceptionAddr, exceptionSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(encodedAddr, encodedSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(destAddr, destCapacity, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(srcAddr, srcSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Compresses {@code input} into a plain or linear-prediction FoR block and
     * runs {@code body} against it. See {@link #withDoubleBlock(double[], BlockTest)}
     * for the guarded target and workspace.
     */
    private static void withLongBlock(long[] input, boolean linearPred, BlockTest body) {
        final int count = input.length;
        final long srcSize = Math.max(1, (long) count * Long.BYTES);
        final long guardedSize = (long) (count + GUARD_WORDS) * Long.BYTES;
        final int destCapacity = CoveringCompressor.maxCompressedSize(
                count, linearPred ? ColumnType.TIMESTAMP : ColumnType.LONG);
        final long srcAddr = Unsafe.malloc(srcSize, MemoryTag.NATIVE_DEFAULT);
        final long destAddr = Unsafe.malloc(destCapacity, MemoryTag.NATIVE_DEFAULT);
        final long residualAddr = Unsafe.malloc(srcSize, MemoryTag.NATIVE_DEFAULT);
        final long outAddr = Unsafe.malloc(guardedSize, MemoryTag.NATIVE_DEFAULT);
        final long workspaceAddr = Unsafe.malloc(guardedSize, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < count; i++) {
                Unsafe.putLong(srcAddr + (long) i * Long.BYTES, input[i]);
            }
            final int storedLength = linearPred
                    ? CoveringCompressor.compressLongsLinearPred(srcAddr, count, destAddr, residualAddr)
                    : CoveringCompressor.compressLongs(srcAddr, count, destAddr);
            Assert.assertTrue("compressed size must fit the destination",
                    storedLength > 0 && storedLength <= destCapacity);
            fillGuardWords(outAddr, count);
            fillGuardWords(workspaceAddr, count);
            body.run(destAddr, storedLength, outAddr, workspaceAddr);
        } finally {
            Unsafe.free(workspaceAddr, guardedSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outAddr, guardedSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(residualAddr, srcSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(destAddr, destCapacity, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(srcAddr, srcSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private interface BlockTest {
        void run(long blockAddr, int storedLength, long outAddr, long workspaceAddr);
    }

    /**
     * {@link CoveringCompressor#arithmeticStart} and
     * {@link CoveringCompressor#arithmeticStride} read the block header at fixed
     * offsets, so they are pinned against the decoder that owns the layout: if
     * the header ever gains a field, this fails rather than silently returning
     * the wrong sequence.
     * <p>
     * The closed form matters because a cursor uses it to generate row ids
     * WITHOUT decoding -- a wrong start or stride would produce plausible row
     * ids that are simply not the ones stored.
     */
    @Test
    public void testArithmeticAccessorsAgreeWithTheDecoder() throws Exception {
        assertMemoryLeak(() -> {
            final int count = 257;
            for (long stride : new long[]{1, 7, 2000, 1L << 20}) {
                final long first = 12_345L;
                final long srcSize = (long) count * Long.BYTES;
                final long src = Unsafe.malloc(srcSize, MemoryTag.NATIVE_DEFAULT);
                final int bound = CoveringCompressor.maxCompressedSize(count, ColumnType.TIMESTAMP);
                final long block = Unsafe.calloc(bound, MemoryTag.NATIVE_DEFAULT);
                final long ws = Unsafe.malloc(srcSize, MemoryTag.NATIVE_DEFAULT);
                final long out = Unsafe.malloc(srcSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < count; i++) {
                        Unsafe.getUnsafe().putLong(src + ((long) i << 3), first + i * stride);
                    }
                    CoveringCompressor.compressLongsLinearPred(src, count, block, ws);

                    Assert.assertTrue("an exact progression must encode with zero residuals [stride=" + stride + ']',
                            CoveringCompressor.isArithmeticBlock(block));
                    Assert.assertEquals("stride", stride, CoveringCompressor.arithmeticStride(block));

                    // The decoder is the authority: the accessors must reproduce
                    // exactly what it produces, value for value.
                    CoveringCompressor.readLongsInto(block, 0, count, out);
                    final long start = CoveringCompressor.arithmeticStart(block);
                    for (int i = 0; i < count; i++) {
                        final long decoded = Unsafe.getUnsafe().getLong(out + ((long) i << 3));
                        Assert.assertEquals("value " + i + " [stride=" + stride + ']',
                                decoded, start + (long) i * stride);
                        Assert.assertEquals("round trip " + i, first + i * stride, decoded);
                    }
                } finally {
                    Unsafe.free(out, srcSize, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(ws, srcSize, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(block, bound, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(src, srcSize, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }
}
