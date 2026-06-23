/*******************************************************************************
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import static io.questdb.cairo.arr.ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES;

/**
 * Unit tests for {@link ArrayTypeDriver#getDataVectorSizeAtFromFd(io.questdb.std.FilesFacade, long, long)}.
 * <p>
 * Focuses on the monotonicity-based torn-write guard:
 * <ol>
 *   <li>Healthy null-prefix: [NULL, NULL, real, real] must NOT throw — the real entries
 *       have offset=0 because the two NULLs wrote no data bytes, so data-end(row-1)=0
 *       and 0 &lt; 0 is false.</li>
 *   <li>Torn tail: zeroing the last entry's offset bytes must throw with a message
 *       containing "Invalid data offset read from array aux file".</li>
 * </ol>
 */
public class ArrayFdAccessorTest extends AbstractTest {

    @Test
    public void testNullPrefixDoesNotThrowAndTornTailDoes() throws Exception {
        final var ff = TestFilesFacadeImpl.INSTANCE;
        TestUtils.assertMemoryLeak(() -> {
            try (Path auxPath = new Path().of(temp.newFile().getAbsolutePath())) {

                // --- Build a 1D double[] aux+data file with [NULL, NULL, ARRAY[1,2,3], ARRAY[4,5,6]] ---
                //
                // Null entries: offset=dataMem.getAppendOffset()=0, size=0. The two nulls both
                // produce offset=0,size=0 → data-end=0. The first real entry (row 2) gets offset=0
                // because no data bytes were written for the nulls; that is VALID and must not throw.
                //
                // ARRAY[1.0,2.0,3.0] data layout written by writeDataEntry:
                //   writeShape:       4 bytes  (dim count=3 as int, 1D so 1 int)
                //   padTo(8):         4 bytes  (align for double)
                //   appendDataToMem:  24 bytes (3 doubles)
                //   padTo(4):         0 bytes  (32 already aligned)
                //   size = 32
                //
                // ARRAY[4.0,5.0,6.0] - identical layout, size = 32
                //
                // So:
                //   row 0 (NULL):  offset=0, size=0  → end=0
                //   row 1 (NULL):  offset=0, size=0  → end=0
                //   row 2 (real):  offset=0, size=32 → end=32
                //   row 3 (real):  offset=32, size=32 → end=64

                final int arraySize;  // bytes per non-null entry (computed after building)

                try (MemoryCMARW auxMem = Vm.getSmallCMARWInstance(ff, auxPath.$(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                     MemoryCARW dataMem = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                     DirectArray arr1 = new DirectArray();
                     DirectArray arr2 = new DirectArray()) {

                    // Append NULL, NULL
                    ArrayTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
                    ArrayTypeDriver.INSTANCE.appendNull(auxMem, dataMem);

                    // Append ARRAY[1.0, 2.0, 3.0]
                    arr1.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
                    arr1.setDimLen(0, 3);
                    arr1.applyShape();
                    MemoryA mem1 = arr1.startMemoryA();
                    mem1.putDouble(1.0);
                    mem1.putDouble(2.0);
                    mem1.putDouble(3.0);
                    ArrayTypeDriver.appendValue(auxMem, dataMem, arr1);

                    // Append ARRAY[4.0, 5.0, 6.0]
                    arr2.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
                    arr2.setDimLen(0, 3);
                    arr2.applyShape();
                    MemoryA mem2 = arr2.startMemoryA();
                    mem2.putDouble(4.0);
                    mem2.putDouble(5.0);
                    mem2.putDouble(6.0);
                    long dataEndBefore = dataMem.getAppendOffset();
                    ArrayTypeDriver.appendValue(auxMem, dataMem, arr2);
                    long dataEndAfter = dataMem.getAppendOffset();
                    arraySize = (int) (dataEndAfter - dataEndBefore);  // should be 32
                }

                // --- Healthy read: rows 2 and 3 must NOT throw ---
                long auxFdClean = ff.openRO(auxPath.$());
                try {
                    // Row 2 (first real after null prefix): offset=0, prevEnd=0, 0<0 is false → no throw
                    long sz2 = ArrayTypeDriver.INSTANCE.getDataVectorSizeAtFromFd(ff, auxFdClean, 2);
                    Assert.assertEquals("row 2 data end", arraySize, sz2);

                    // Row 3 (second real): offset=arraySize, prevEnd=arraySize, arraySize<arraySize is false → no throw
                    long sz3 = ArrayTypeDriver.INSTANCE.getDataVectorSizeAtFromFd(ff, auxFdClean, 3);
                    Assert.assertEquals("row 3 data end", (long) 2 * arraySize, sz3);
                } finally {
                    ff.close(auxFdClean);
                }

                // --- Torn tail: zero the offset bytes of row 3 (bytes [0,8) of aux entry 3) ---
                // After zeroing: row 3 has offset=0, prevEnd=arraySize → 0 < arraySize → THROW.
                // Use RandomAccessFile so we do an in-place partial overwrite without truncating.
                long row3AuxOffset = (long) 3 * ARRAY_AUX_WIDTH_BYTES;
                try (RandomAccessFile raf = new RandomAccessFile(auxPath.toString(), "rw")) {
                    raf.seek(row3AuxOffset);
                    raf.write(new byte[Long.BYTES]);  // zero 8 bytes (the offset field)
                }

                long auxFdTorn = ff.openRO(auxPath.$());
                try {
                    ArrayTypeDriver.INSTANCE.getDataVectorSizeAtFromFd(ff, auxFdTorn, 3);
                    Assert.fail("expected CairoException for torn aux entry");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Invalid data offset read from array aux file");
                } finally {
                    ff.close(auxFdTorn);
                }
            }
        });
    }
}
