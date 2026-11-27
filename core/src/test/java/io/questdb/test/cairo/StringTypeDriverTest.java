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

import io.questdb.cairo.BinaryTypeDriver;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.StringTypeDriver;
import io.questdb.cairo.vm.MemoryPMARImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.BinarySequence;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import io.questdb.test.griffin.engine.TestBinarySequence;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class StringTypeDriverTest extends AbstractTest {

    @Test
    public void testRewindAppendPosition() throws Exception {
        // A row was cancelled after it moved a STRING or BINARY column, or did not move it. rewindAppendPosition()
        // moves the column back to the start of the row: the aux vector to the end of row k - 1, with aux[k], the
        // offset that the row before put, as it was, and the data vector to aux[k]. It maps no page that the files
        // lack, and the next values land after the committed ones.
        TestUtils.assertMemoryLeak(() -> {
            for (CancelledRow cancelled : CancelledRow.values()) {
                assertRewind(StringTypeDriver.INSTANCE, false, cancelled);
                assertRewind(BinaryTypeDriver.INSTANCE, true, cancelled);
            }
        });
    }

    private static void append(MemoryMA auxMem, MemoryMA dataMem, boolean isBinary, Object value) {
        if (isBinary) {
            auxMem.putLong(dataMem.putBin(value == null ? null : new TestBinarySequence().of((byte[]) value)));
        } else {
            StringTypeDriver.appendValue(auxMem, dataMem, (CharSequence) value);
        }
    }

    private static void assertRewind(StringTypeDriver driver, boolean isBinary, CancelledRow cancelled) throws Exception {
        final String context = (isBinary ? "BINARY, " : "STRING, ") + cancelled;
        final AllocationCountingFacade ff = new AllocationCountingFacade();
        final long pageSize = Files.PAGE_SIZE;
        // the committed values
        final ObjList<Object> values = new ObjList<>();
        // a value of 100 bytes: 4 + 2 * 48 for STRING, 8 + 92 for BINARY
        final int longLen = isBinary ? 92 : 48;
        switch (cancelled) {
            case NULL_AT_ROW_ZERO -> {
            }
            // values of 100 bytes: the next one crosses the end of the first page of the data file
            case VALUE_CUT_BY_FAILED_PAGE -> {
                for (int i = 0, n = (int) pageSize / 100; i < n; i++) {
                    values.add(valueOf((char) ('a' + i % 26), longLen, isBinary));
                }
            }
            // values of 100 bytes, and one of the rest, end the first page of the data file; a BINARY value that ends
            // a page makes its put map the next page as well, so a NULL, which does not, ends it
            case NONE_AT_DATA_PAGE_END -> {
                final long valuesEnd = isBinary ? pageSize - Long.BYTES : pageSize;
                final int n = (int) (valuesEnd - Long.BYTES) / 100;
                for (int i = 0; i < n; i++) {
                    values.add(valueOf((char) ('a' + i % 26), longLen, isBinary));
                }
                final long rest = valuesEnd - 100L * n;
                values.add(valueOf('r', (int) (isBinary ? rest - Long.BYTES : (rest - Integer.BYTES) / 2), isBinary));
                if (isBinary) {
                    values.add(null);
                }
            }
            // the offsets end the first page of the aux file
            case NONE_AT_AUX_PAGE_END, VALUE_WITH_FAILED_AUX_PAGE, NULL_OPENS_AUX_PAGE -> {
                for (int i = 0, n = (int) pageSize / Long.BYTES - 1; i < n; i++) {
                    values.add(valueOf((char) ('a' + i % 26), 1 + i % 3, isBinary));
                }
            }
            default -> {
                for (int i = 0; i < 10; i++) {
                    values.add(valueOf((char) ('a' + i % 26), 1 + i % 3, isBinary));
                }
            }
        }
        final long committedRows = values.size();
        try (
                Path auxPath = new Path().of(temp.newFile().getAbsolutePath());
                Path dataPath = new Path().of(temp.newFile().getAbsolutePath())
        ) {
            try (
                    MemoryPMARImpl auxMem = new MemoryPMARImpl(ff, auxPath.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                    MemoryPMARImpl dataMem = new MemoryPMARImpl(ff, dataPath.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)
            ) {
                driver.configureAuxMemMA(auxMem);
                long dataSize = 0;
                for (int i = 0, n = values.size(); i < n; i++) {
                    append(auxMem, dataMem, isBinary, values.getQuick(i));
                    dataSize += sizeOf(values.getQuick(i), isBinary);
                }
                Assert.assertEquals(context, (committedRows + 1) * Long.BYTES, auxMem.getAppendOffset());
                Assert.assertEquals(context, dataSize, dataMem.getAppendOffset());
                if (cancelled == CancelledRow.NONE_AT_DATA_PAGE_END) {
                    Assert.assertEquals(context, pageSize, dataSize);
                }

                // the writes of the cancelled row
                switch (cancelled) {
                    case NULL, NULL_AT_ROW_ZERO, NULL_OPENS_AUX_PAGE -> driver.appendNull(auxMem, dataMem);
                    case VALUE_CUT_BY_FAILED_PAGE -> failAppend(context, ff, dataMem.getFd(), auxMem, dataMem, isBinary, valueOf('F', longLen, isBinary));
                    case VALUE_WITH_FAILED_AUX_PAGE -> failAppend(context, ff, auxMem.getFd(), auxMem, dataMem, isBinary, valueOf('F', 2, isBinary));
                    default -> {
                    }
                }
                if (cancelled == CancelledRow.NULL_OPENS_AUX_PAGE) {
                    // the offset of the NULL starts the second page of the aux file
                    Assert.assertEquals(context, pageSize + Long.BYTES, auxMem.getAppendOffset());
                }

                final int allocations = ff.allocations.get();
                driver.rewindAppendPosition(committedRows, auxMem, dataMem);
                Assert.assertEquals(context, allocations, ff.allocations.get());
                Assert.assertEquals(context, (committedRows + 1) * Long.BYTES, auxMem.getAppendOffset());
                Assert.assertEquals(context, dataSize, dataMem.getAppendOffset());

                // the next values: one longer than half a page, NULL, and a short one
                values.add(valueOf('L', (int) pageSize / 2, isBinary));
                values.add(null);
                values.add(valueOf('z', 1, isBinary));
                for (int i = (int) committedRows, n = values.size(); i < n; i++) {
                    append(auxMem, dataMem, isBinary, values.getQuick(i));
                }
            }
            try (
                    MemoryCMR auxMem = Vm.getCMRInstance(ff, auxPath.$(), ff.length(auxPath.$()), MemoryTag.MMAP_DEFAULT);
                    MemoryCMR dataMem = Vm.getCMRInstance(ff, dataPath.$(), ff.length(dataPath.$()), MemoryTag.MMAP_DEFAULT)
            ) {
                long offset = 0;
                for (int i = 0, n = values.size(); i < n; i++) {
                    final Object value = values.getQuick(i);
                    Assert.assertEquals(context + ", row " + i, offset, auxMem.getLong((long) i * Long.BYTES));
                    if (isBinary) {
                        final BinarySequence actual = dataMem.getBin(offset);
                        if (value == null) {
                            Assert.assertNull(context + ", row " + i, actual);
                        } else {
                            final byte[] bytes = new byte[(int) actual.length()];
                            for (int j = 0; j < bytes.length; j++) {
                                bytes[j] = actual.byteAt(j);
                            }
                            Assert.assertArrayEquals(context + ", row " + i, (byte[]) value, bytes);
                        }
                    } else {
                        final CharSequence actual = dataMem.getStrA(offset);
                        if (value == null) {
                            Assert.assertNull(context + ", row " + i, actual);
                        } else {
                            TestUtils.assertEquals(context + ", row " + i, (String) value, actual);
                        }
                    }
                    offset += sizeOf(value, isBinary);
                }
                Assert.assertEquals(context, offset, auxMem.getLong((long) values.size() * Long.BYTES));
            }
        }
    }

    // the put of the value fails where it needs a page of the file of fd, which cannot be allocated
    private static void failAppend(String context, AllocationCountingFacade ff, long fd, MemoryMA auxMem, MemoryMA dataMem, boolean isBinary, Object value) {
        ff.failingFd.set(fd);
        try {
            append(auxMem, dataMem, isBinary, value);
            Assert.fail(context);
        } catch (CairoException e) {
            TestUtils.assertContains(context, e.getFlyweightMessage(), "No space left");
        }
        Assert.assertEquals(context, -1, ff.failingFd.get());
    }

    // bytes that a STRING or BINARY value takes in the data vector
    private static long sizeOf(Object value, boolean isBinary) {
        if (value == null) {
            return isBinary ? Long.BYTES : Integer.BYTES;
        }
        return isBinary ? Long.BYTES + ((byte[]) value).length : Integer.BYTES + 2L * ((String) value).length();
    }

    // a STRING of len chars c, or a BINARY of len bytes c
    private static Object valueOf(char c, int len, boolean isBinary) {
        if (isBinary) {
            final byte[] bytes = new byte[len];
            Arrays.fill(bytes, (byte) c);
            return bytes;
        }
        return String.valueOf(c).repeat(len);
    }

    // what the cancelled row wrote
    private enum CancelledRow {
        // nothing
        NONE,
        // nothing, the offsets end the first page of the aux file: a jump to the end of row k - 1 would map the next
        // page
        NONE_AT_AUX_PAGE_END,
        // nothing, the values end the first page of the data file: a jump there would map the next page
        NONE_AT_DATA_PAGE_END,
        // a NULL
        NULL,
        // a NULL in the first row
        NULL_AT_ROW_ZERO,
        // a NULL, whose offset takes the second page of the aux file
        NULL_OPENS_AUX_PAGE,
        // the start of a value that crosses the end of the first page of the data file, whose next page cannot be
        // allocated
        VALUE_CUT_BY_FAILED_PAGE,
        // a value, whose offset needs the next page of the aux file, which cannot be allocated
        VALUE_WITH_FAILED_AUX_PAGE
    }
}
