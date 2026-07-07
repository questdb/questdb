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

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8SplitString;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
import static io.questdb.cairo.VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL;

public class VarcharTypeDriverSliceTest extends AbstractCairoTest {

    @Test
    public void testGetSliceValueAsciiFlag() throws Exception {
        assertMemoryLeak(() -> {
            // "abc" = 3 bytes, ASCII
            byte[] data = new byte[]{'a', 'b', 'c'};
            int length = data.length;
            long dataAddr = Unsafe.malloc(length, MemoryTag.NATIVE_DEFAULT);
            long auxAddr = Unsafe.malloc(VARCHAR_AUX_WIDTH_BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < length; i++) {
                    Unsafe.putByte(dataAddr + i, data[i]);
                }

                // header with ASCII flag set (bit 1): length << 4 | 2
                int headerAscii = (length << 4) | 2;
                Unsafe.putInt(auxAddr, headerAscii);
                Unsafe.putInt(auxAddr + 4, 0); // reserved
                Unsafe.putLong(auxAddr + 8, dataAddr);

                Utf8SplitString utf8View = new Utf8SplitString();
                Utf8Sequence result = VarcharTypeDriver.getSliceValue(auxAddr, 0, utf8View);

                Assert.assertNotNull(result);
                Assert.assertTrue(result.isAscii());

                // Now write header without ASCII flag
                int headerNonAscii = (length << 4);
                Unsafe.putInt(auxAddr, headerNonAscii);

                result = VarcharTypeDriver.getSliceValue(auxAddr, 0, utf8View);

                Assert.assertNotNull(result);
                Assert.assertFalse(result.isAscii());
            } finally {
                Unsafe.free(auxAddr, VARCHAR_AUX_WIDTH_BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dataAddr, length, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testGetSliceValueMultipleRows() throws Exception {
        assertMemoryLeak(() -> {
            // Row 0: "hello" (5 bytes, ASCII)
            // Row 1: null
            // Row 2: "xy" (2 bytes, non-ASCII flag)
            byte[] data0 = new byte[]{'h', 'e', 'l', 'l', 'o'};
            byte[] data2 = new byte[]{'x', 'y'};

            long dataAddr0 = Unsafe.malloc(data0.length, MemoryTag.NATIVE_DEFAULT);
            long dataAddr2 = Unsafe.malloc(data2.length, MemoryTag.NATIVE_DEFAULT);
            long auxAddr = Unsafe.malloc(3L * VARCHAR_AUX_WIDTH_BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < data0.length; i++) {
                    Unsafe.putByte(dataAddr0 + i, data0[i]);
                }
                for (int i = 0; i < data2.length; i++) {
                    Unsafe.putByte(dataAddr2 + i, data2[i]);
                }

                // Row 0: non-null, ASCII, length=5
                Unsafe.putInt(auxAddr, (data0.length << 4) | 2);
                Unsafe.putInt(auxAddr + 4, 0);
                Unsafe.putLong(auxAddr + 8, dataAddr0);

                // Row 1: null
                long row1 = auxAddr + VARCHAR_AUX_WIDTH_BYTES;
                Unsafe.putInt(row1, VARCHAR_HEADER_FLAG_NULL);
                Unsafe.putInt(row1 + 4, 0);
                Unsafe.putLong(row1 + 8, 0);

                // Row 2: non-null, non-ASCII, length=2
                long row2 = auxAddr + 2L * VARCHAR_AUX_WIDTH_BYTES;
                Unsafe.putInt(row2, (data2.length << 4));
                Unsafe.putInt(row2 + 4, 0);
                Unsafe.putLong(row2 + 8, dataAddr2);

                Utf8SplitString utf8View = new Utf8SplitString();

                // Read row 0
                Utf8Sequence result0 = VarcharTypeDriver.getSliceValue(auxAddr, 0, utf8View);
                Assert.assertNotNull(result0);
                Assert.assertEquals(5, result0.size());
                Assert.assertTrue(result0.isAscii());
                Assert.assertEquals("hello", result0.toString());

                // Read row 1
                Utf8Sequence result1 = VarcharTypeDriver.getSliceValue(auxAddr, 1, utf8View);
                Assert.assertNull(result1);

                // Read row 2
                Utf8Sequence result2 = VarcharTypeDriver.getSliceValue(auxAddr, 2, utf8View);
                Assert.assertNotNull(result2);
                Assert.assertEquals(2, result2.size());
                Assert.assertFalse(result2.isAscii());
                Assert.assertEquals("xy", result2.toString());
            } finally {
                Unsafe.free(auxAddr, 3L * VARCHAR_AUX_WIDTH_BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dataAddr0, data0.length, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dataAddr2, data2.length, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testGetSliceValueNonNull() throws Exception {
        assertMemoryLeak(() -> {
            // Write "test" (4 bytes, ASCII) to a data buffer
            byte[] data = new byte[]{'t', 'e', 's', 't'};
            int length = data.length;
            long dataAddr = Unsafe.malloc(length, MemoryTag.NATIVE_DEFAULT);
            long auxAddr = Unsafe.malloc(VARCHAR_AUX_WIDTH_BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < length; i++) {
                    Unsafe.putByte(dataAddr + i, data[i]);
                }

                // header: length << 4 | ascii flag (bit 1)
                int header = (length << 4) | 2;
                Unsafe.putInt(auxAddr, header);
                Unsafe.putInt(auxAddr + 4, 0); // reserved
                Unsafe.putLong(auxAddr + 8, dataAddr);

                Utf8SplitString utf8View = new Utf8SplitString();
                Utf8Sequence result = VarcharTypeDriver.getSliceValue(auxAddr, 0, utf8View);

                Assert.assertNotNull(result);
                Assert.assertEquals(length, result.size());
                Assert.assertTrue(result.isAscii());
                Assert.assertEquals("test", result.toString());
            } finally {
                Unsafe.free(auxAddr, VARCHAR_AUX_WIDTH_BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dataAddr, length, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testGetSliceValueNull() throws Exception {
        assertMemoryLeak(() -> {
            long auxAddr = Unsafe.malloc(VARCHAR_AUX_WIDTH_BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putInt(auxAddr, VARCHAR_HEADER_FLAG_NULL);
                Unsafe.putInt(auxAddr + 4, 0); // reserved
                Unsafe.putLong(auxAddr + 8, 0); // ptr = 0

                Utf8SplitString utf8View = new Utf8SplitString();
                Utf8Sequence result = VarcharTypeDriver.getSliceValue(auxAddr, 0, utf8View);

                Assert.assertNull(result);
            } finally {
                Unsafe.free(auxAddr, VARCHAR_AUX_WIDTH_BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testGetSliceValueSizeNegativeRow() throws Exception {
        assertMemoryLeak(() -> {
            long auxAddr = Unsafe.malloc(VARCHAR_AUX_WIDTH_BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                // Content does not matter since rowNum < 0 short-circuits
                Unsafe.putInt(auxAddr, (10 << 4) | 2);
                Unsafe.putInt(auxAddr + 4, 0);
                Unsafe.putLong(auxAddr + 8, 0);

                Assert.assertEquals(TableUtils.NULL_LEN, VarcharTypeDriver.getSliceValueSize(auxAddr, -1));
            } finally {
                Unsafe.free(auxAddr, VARCHAR_AUX_WIDTH_BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testGetSliceValueSizeNonNull() throws Exception {
        assertMemoryLeak(() -> {
            long auxAddr = Unsafe.malloc(VARCHAR_AUX_WIDTH_BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                int length = 7;
                int header = (length << 4) | 2; // ASCII flag
                Unsafe.putInt(auxAddr, header);
                Unsafe.putInt(auxAddr + 4, 0);
                Unsafe.putLong(auxAddr + 8, 0);

                Assert.assertEquals(length, VarcharTypeDriver.getSliceValueSize(auxAddr, 0));
            } finally {
                Unsafe.free(auxAddr, VARCHAR_AUX_WIDTH_BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testGetSliceValueSizeNull() throws Exception {
        assertMemoryLeak(() -> {
            long auxAddr = Unsafe.malloc(VARCHAR_AUX_WIDTH_BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putInt(auxAddr, VARCHAR_HEADER_FLAG_NULL);
                Unsafe.putInt(auxAddr + 4, 0);
                Unsafe.putLong(auxAddr + 8, 0);

                Assert.assertEquals(TableUtils.NULL_LEN, VarcharTypeDriver.getSliceValueSize(auxAddr, 0));
            } finally {
                Unsafe.free(auxAddr, VARCHAR_AUX_WIDTH_BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }
}
