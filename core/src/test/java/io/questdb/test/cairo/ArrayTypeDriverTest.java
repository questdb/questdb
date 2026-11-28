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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.vm.MemoryPMARImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.arr.ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES;

public class ArrayTypeDriverTest extends AbstractTest {
    private static final int DOUBLE_ARRAY = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);

    @Test
    public void testRewindAppendPosition() throws Exception {
        // A row was cancelled after it moved a DOUBLE[] column, or did not move it. rewindAppendPosition() moves both
        // vectors back to the start of the row: the data vector keeps no byte of the cancelled row, so the next values
        // land right after the committed ones, with no gap between. It maps no page that the files lack.
        TestUtils.assertMemoryLeak(() -> {
            for (CancelledRow cancelled : CancelledRow.values()) {
                assertRewind(cancelled);
            }
        });
    }

    private static void append(MemoryMA auxMem, MemoryMA dataMem, DirectArray array, double[] value) {
        if (value == null) {
            ArrayTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
            return;
        }
        array.setType(DOUBLE_ARRAY);
        array.setDimLen(0, value.length);
        array.applyShape();
        for (int i = 0; i < value.length; i++) {
            array.putDouble(i, value[i]);
        }
        ArrayTypeDriver.appendValue(auxMem, dataMem, array);
    }

    // n doubles from first, step 0.5
    private static double[] arrayOf(double first, int n) {
        final double[] value = new double[n];
        for (int i = 0; i < n; i++) {
            value[i] = first + i * 0.5;
        }
        return value;
    }

    private static void assertRewind(CancelledRow cancelled) throws Exception {
        final String context = cancelled.toString();
        final AllocationCountingFacade ff = new AllocationCountingFacade();
        final long pageSize = Files.PAGE_SIZE;
        final ObjList<double[]> values = new ObjList<>();
        final long committedRows = switch (cancelled) {
            case NULL_AT_ROW_ZERO, VALUE_AT_ROW_ZERO, VALUE_CUT_AT_ROW_ZERO -> 0;
            // arrays of 12 doubles, 104 bytes in the data vector: the next one crosses the end of its first page
            case VALUE_CUT_BY_FAILED_PAGE -> pageSize / 104;
            // the aux entries end the first page of the aux file
            case NONE_AT_AUX_PAGE_END, VALUE_WITH_FAILED_AUX_PAGE, NULL_OPENS_AUX_PAGE, VALUE_OPENS_AUX_PAGE ->
                    pageSize / ARRAY_AUX_WIDTH_BYTES;
            // arrays of 7 doubles, 64 bytes in the data vector, end the first page of the data file
            case NONE_AT_DATA_PAGE_END, VALUE_OPENS_DATA_PAGE -> pageSize / 64;
            default -> 10;
        };
        for (int i = 0; i < committedRows; i++) {
            values.add(arrayOf(i, switch (cancelled) {
                case VALUE_CUT_BY_FAILED_PAGE -> 12;
                case NONE_AT_DATA_PAGE_END, VALUE_OPENS_DATA_PAGE -> 7;
                default -> 1 + i % 3;
            }));
        }
        try (
                Path auxPath = new Path().of(temp.newFile().getAbsolutePath());
                Path dataPath = new Path().of(temp.newFile().getAbsolutePath());
                DirectArray array = new DirectArray()
        ) {
            try (
                    MemoryPMARImpl auxMem = new MemoryPMARImpl(ff, auxPath.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                    MemoryPMARImpl dataMem = new MemoryPMARImpl(ff, dataPath.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)
            ) {
                for (int i = 0, n = values.size(); i < n; i++) {
                    append(auxMem, dataMem, array, values.getQuick(i));
                }
                Assert.assertEquals(context, committedRows * ARRAY_AUX_WIDTH_BYTES, auxMem.getAppendOffset());
                if (cancelled == CancelledRow.VALUE_CUT_AT_ROW_ZERO) {
                    // the first page of the data file, which the value does not fit
                    dataMem.jumpTo(0);
                }
                final long committedDataSize = dataMem.getAppendOffset();
                if (cancelled == CancelledRow.NONE_AT_DATA_PAGE_END || cancelled == CancelledRow.VALUE_OPENS_DATA_PAGE) {
                    Assert.assertEquals(context, pageSize, committedDataSize);
                }

                // the writes of the cancelled row
                switch (cancelled) {
                    case NULL, NULL_AT_ROW_ZERO, NULL_OPENS_AUX_PAGE -> ArrayTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
                    case VALUE, VALUE_AT_ROW_ZERO, VALUE_OPENS_AUX_PAGE, VALUE_OPENS_DATA_PAGE ->
                            append(auxMem, dataMem, array, arrayOf(-1, 2));
                    case VALUE_CUT_BY_FAILED_PAGE -> failAppend(context, ff, dataMem.getFd(), auxMem, dataMem, array, arrayOf(-1, 12));
                    // its shape lands, its doubles take a page
                    case VALUE_CUT_AT_ROW_ZERO -> failAppend(context, ff, dataMem.getFd(), auxMem, dataMem, array, arrayOf(-1, (int) pageSize / Double.BYTES));
                    case VALUE_WITH_FAILED_AUX_PAGE -> failAppend(context, ff, auxMem.getFd(), auxMem, dataMem, array, arrayOf(-1, 2));
                    default -> {
                    }
                }
                if (cancelled == CancelledRow.NULL_OPENS_AUX_PAGE || cancelled == CancelledRow.VALUE_OPENS_AUX_PAGE) {
                    // the entry of the row starts the second page of the aux file
                    Assert.assertEquals(context, pageSize + ARRAY_AUX_WIDTH_BYTES, auxMem.getAppendOffset());
                }
                if (cancelled == CancelledRow.VALUE_OPENS_DATA_PAGE) {
                    // the value starts the second page of the data file
                    Assert.assertEquals(context, pageSize + 24, dataMem.getAppendOffset());
                }

                final int allocations = ff.allocations.get();
                ArrayTypeDriver.INSTANCE.rewindAppendPosition(committedRows, auxMem, dataMem);
                Assert.assertEquals(context, allocations, ff.allocations.get());
                Assert.assertEquals(context, committedRows * ARRAY_AUX_WIDTH_BYTES, auxMem.getAppendOffset());
                Assert.assertEquals(context, committedDataSize, dataMem.getAppendOffset());

                // the next values: one longer than half a page, NULL, and a short one
                values.add(arrayOf(100, (int) pageSize / 16));
                values.add(null);
                values.add(arrayOf(200, 1));
                for (int i = (int) committedRows, n = values.size(); i < n; i++) {
                    append(auxMem, dataMem, array, values.getQuick(i));
                }
            }
            try (
                    MemoryCMR auxMem = Vm.getCMRInstance(ff, auxPath.$(), ff.length(auxPath.$()), MemoryTag.MMAP_DEFAULT);
                    MemoryCMR dataMem = Vm.getCMRInstance(ff, dataPath.$(), ff.length(dataPath.$()), MemoryTag.MMAP_DEFAULT)
            ) {
                final BorrowedArray actual = new BorrowedArray();
                final long auxAddr = auxMem.addressOf(0);
                final long dataAddr = dataMem.addressOf(0);
                // each value starts where the value before it ends
                Assert.assertFalse(context, ArrayTypeDriver.INSTANCE.isSparseDataVector(auxAddr, dataAddr, values.size()));
                for (int i = 0, n = values.size(); i < n; i++) {
                    actual.of(DOUBLE_ARRAY, auxAddr, auxAddr + auxMem.size(), dataAddr, dataAddr + dataMem.size(), i);
                    final double[] value = values.getQuick(i);
                    if (value == null) {
                        Assert.assertTrue(context + ", row " + i, actual.isNull());
                    } else {
                        Assert.assertEquals(context + ", row " + i, value.length, actual.getDimLen(0));
                        for (int j = 0; j < value.length; j++) {
                            Assert.assertEquals(context + ", row " + i, value[j], actual.getDouble(j), 0);
                        }
                    }
                }
            }
        }
    }

    // the put of the value fails where it needs a page of the file of fd, which cannot be allocated
    private static void failAppend(String context, AllocationCountingFacade ff, long fd, MemoryMA auxMem, MemoryMA dataMem, DirectArray array, double[] value) {
        ff.failingFd.set(fd);
        try {
            append(auxMem, dataMem, array, value);
            Assert.fail(context);
        } catch (CairoException e) {
            TestUtils.assertContains(context, e.getFlyweightMessage(), "No space left");
        }
        Assert.assertEquals(context, -1, ff.failingFd.get());
    }

    // what the cancelled row wrote
    private enum CancelledRow {
        // nothing
        NONE,
        // nothing, the aux entries end the first page of the aux file: a jump to the end of row k - 1 would map the
        // next page
        NONE_AT_AUX_PAGE_END,
        // a NULL
        NULL,
        // a NULL in the first row
        NULL_AT_ROW_ZERO,
        // a NULL, whose aux entry takes the second page of the aux file
        NULL_OPENS_AUX_PAGE,
        // nothing, the values end the first page of the data file: a jump to the end of row k - 1 would map the next
        // page
        NONE_AT_DATA_PAGE_END,
        // a value
        VALUE,
        // a value in the first row
        VALUE_AT_ROW_ZERO,
        // a value, whose aux entry takes the second page of the aux file
        VALUE_OPENS_AUX_PAGE,
        // a value, which takes the second page of the data file
        VALUE_OPENS_DATA_PAGE,
        // the start of a value that crosses the end of the first page of the data file, whose next page cannot be
        // allocated
        VALUE_CUT_BY_FAILED_PAGE,
        // the start of a value in the first row, which the first page of the data file cannot hold, and whose next
        // page cannot be allocated
        VALUE_CUT_AT_ROW_ZERO,
        // a value, whose aux entry needs the next page of the aux file, which cannot be allocated
        VALUE_WITH_FAILED_AUX_PAGE
    }
}
