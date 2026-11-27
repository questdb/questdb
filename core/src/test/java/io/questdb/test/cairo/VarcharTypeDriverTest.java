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
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cairo.vm.MemoryPMARImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.*;
import io.questdb.std.str.*;
import io.questdb.test.AbstractTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
import static io.questdb.cairo.VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED;

public class VarcharTypeDriverTest extends AbstractTest {

    @Test
    public void testAppendPlainEmptyNonAsciiVarchar() throws Exception {
        // An empty varchar is ASCII by definition, but the Utf8Sequence contract lets a producer
        // report isAscii() == false for it. In the single-vector "plain" format the length prefix
        // encodes the ASCII flag in its top bit, so an empty non-ASCII value used to write a 0
        // header -- which getPlainValue rejects via "assert header != 0" (it is the NULL / empty
        // sentinel). The value must round-trip as an empty ASCII varchar instead.
        TestUtils.assertMemoryLeak(() -> {
            try (MemoryCARW dataMem = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                Utf8Sequence emptyNonAscii = new Utf8String(new byte[0], false);
                Assert.assertEquals(0, emptyNonAscii.size());
                Assert.assertFalse(emptyNonAscii.isAscii());

                VarcharTypeDriver.appendPlainValue(dataMem, emptyNonAscii);

                // The header must be non-zero, otherwise getPlainValue mistakes it for a missing entry.
                Assert.assertNotEquals(0, dataMem.getInt(0));

                Utf8Sequence read = VarcharTypeDriver.getPlainValue(dataMem, 0);
                Assert.assertNotNull(read);
                Assert.assertEquals(0, read.size());
                Assert.assertTrue(read.isAscii());
            }
        });
    }

    @Test
    public void testAppendPlainValueAddressEmptyNonAsciiVarchar() throws Exception {
        // Same empty => ASCII invariant for the address-based appendPlainValue(long, value, false)
        // overload used by RecordChain: an empty non-ASCII value must write a non-zero header, not 0.
        TestUtils.assertMemoryLeak(() -> {
            try (MemoryCARW dataMem = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                long addr = dataMem.appendAddressFor(16);
                Utf8Sequence emptyNonAscii = new Utf8String(new byte[0], false);
                Assert.assertEquals(0, emptyNonAscii.size());
                Assert.assertFalse(emptyNonAscii.isAscii());

                VarcharTypeDriver.appendPlainValue(addr, emptyNonAscii, false);

                // The header must be non-zero, otherwise the asserting getPlainValue overload rejects it.
                Assert.assertNotEquals(0, Unsafe.getInt(addr));

                Utf8Sequence read = VarcharTypeDriver.getPlainValue(addr, new DirectUtf8String());
                Assert.assertNotNull(read);
                Assert.assertEquals(0, read.size());
                Assert.assertTrue(read.isAscii());
            }
        });
    }

    @Test
    public void testGetDataVectorSize() throws Exception {
        final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        final VarcharTypeDriver driver = new VarcharTypeDriver();
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
                try (
                        MemoryCMARW auxMem = Vm.getSmallCMARWInstance(ff, path.$(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                        MemoryCARW dataMem = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
                ) {
                    VarcharTypeDriver.appendValue(auxMem, dataMem, null);
                    String testStr = "foobarbaz - bazbarfoo";
                    int length = testStr.length();
                    VarcharTypeDriver.appendValue(auxMem, dataMem, new Utf8String(testStr));

                    Assert.assertEquals(length, driver.getDataVectorSize(auxMem.addressOf(0), 0, 1));

                    Assert.assertEquals(0, driver.getDataVectorSizeAt(auxMem.addressOf(0), 0));
                    Assert.assertEquals(length, driver.getDataVectorSizeAt(auxMem.addressOf(0), 1));

                    Assert.assertEquals(0, driver.getDataVectorSizeAtFromFd(ff, auxMem.getFd(), -1));
                    Assert.assertEquals(0, driver.getDataVectorSizeAtFromFd(ff, auxMem.getFd(), 0));
                    Assert.assertEquals(length, driver.getDataVectorSizeAtFromFd(ff, auxMem.getFd(), 1));
                }
            }
        });
    }

    @Test
    public void testO3setColumnRefs() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final VarcharTypeDriver driver = new VarcharTypeDriver();
            for (int n = 1; n < 50; n++) {
                try (
                        MemoryCARW auxMem = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryCARW dataMem = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
                ) {
                    long auxOffset = n / 4;
                    long dataOffset = n / 2;
                    auxMem.resize((n + auxOffset) * 2 * Long.BYTES);
                    dataMem.resize(dataOffset);

                    driver.setPartAuxVectorNull(auxMem.addressOf(auxOffset * 2 * Long.BYTES), dataOffset, n);

                    for (int i = 0; i < n; i++) {
                        Assert.assertNull(VarcharTypeDriver.getSplitValue(auxMem, dataMem, (i + auxOffset), 1));
                        Assert.assertEquals(dataOffset, VarcharTypeDriver.getDataVectorSize(auxMem,
                                (i + auxOffset) * VARCHAR_AUX_WIDTH_BYTES));
                    }
                }
            }
        });
    }

    @Test
    public void testO3shiftCopyAuxVector() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final VarcharTypeDriver driver = new VarcharTypeDriver();
            final Utf8StringSink utf8Sink = new Utf8StringSink();
            final LongList expectedOffsets = new LongList();
            final int auxLoBase = 3;
            for (int n = auxLoBase; n < 50; n++) {
                try (
                        MemoryCARW auxMemA = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryCARW dataMemA = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryCARW auxMemB = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryCARW dataMemB = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
                ) {
                    final int len = n % 16;
                    final int auxLo = n % auxLoBase;
                    final int shift = -n;
                    expectedOffsets.clear();
                    for (int i = 0; i < n; i++) {
                        utf8Sink.clear();
                        utf8Sink.repeat("a", len);
                        VarcharTypeDriver.appendValue(auxMemA, dataMemA, utf8Sink);
                        if (i >= auxLo) {
                            expectedOffsets.add(dataMemA.getAppendOffset());
                        }
                    }

                    utf8Sink.clear();
                    utf8Sink.repeat("a", len);
                    final String expectedStr = utf8Sink.toString();

                    for (int i = auxLo; i < n; i++) {
                        Utf8Sequence varchar = VarcharTypeDriver.getSplitValue(auxMemA, dataMemA, i, 1);
                        Assert.assertEquals(
                                expectedOffsets.getQuick(i - auxLo),
                                VarcharTypeDriver.getDataVectorSize(auxMemA, i * VARCHAR_AUX_WIDTH_BYTES));
                        Assert.assertNotNull(varchar);
                        TestUtils.assertEquals(expectedStr, varchar.asAsciiCharSequence());
                        Assert.assertTrue(varchar.isAscii());
                    }

                    dataMemB.extend(dataMemA.size() - shift);
                    Vect.memcpy(dataMemB.addressOf(-shift), dataMemA.addressOf(0), dataMemA.size());
                    auxMemB.extend(auxMemA.size());

                    driver.shiftCopyAuxVector(shift, auxMemA.addressOf(0), auxLo, n, auxMemB.addressOf(0), auxMemB.size());

                    for (int i = 0; i < n - auxLo; i++) {
                        Utf8Sequence varchar = VarcharTypeDriver.getSplitValue(auxMemB, dataMemB, i, 1);
                        Assert.assertEquals("offset mismatch: i=" + i + ", n=" + n,
                                expectedOffsets.getQuick(i) - shift,
                                VarcharTypeDriver.getDataVectorSize(auxMemB, i * VARCHAR_AUX_WIDTH_BYTES));
                        Assert.assertNotNull(varchar);
                        TestUtils.assertEquals(expectedStr, varchar.asAsciiCharSequence());
                        Assert.assertTrue(varchar.isAscii());
                    }
                }
            }
        });
    }

    @Test
    public void testO3shiftCopyAuxVectorNulls() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final VarcharTypeDriver driver = new VarcharTypeDriver();
            final LongList expectedOffsets = new LongList();
            final int auxLoBase = 3;
            for (int n = auxLoBase; n < 50; n++) {
                try (
                        MemoryCARW auxMemA = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryCARW dataMemA = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryCARW auxMemB = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryCARW dataMemB = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
                ) {
                    final int auxLo = n % auxLoBase;
                    final int shift = -42;
                    expectedOffsets.clear();
                    for (int i = 0; i < n; i++) {
                        VarcharTypeDriver.appendValue(auxMemA, dataMemA, null);
                        if (i >= auxLo) {
                            expectedOffsets.add(dataMemA.getAppendOffset());
                        }
                    }

                    for (int i = auxLo; i < n; i++) {
                        Assert.assertNull(VarcharTypeDriver.getSplitValue(auxMemA, dataMemA, i, 1));
                        Assert.assertEquals(expectedOffsets.getQuick(i - auxLo),
                                VarcharTypeDriver.getDataVectorSize(auxMemA, i * VARCHAR_AUX_WIDTH_BYTES));
                    }

                    dataMemB.extend(dataMemA.size() - shift);
                    Vect.memcpy(dataMemB.addressOf(-shift), dataMemA.addressOf(0), dataMemA.size());
                    auxMemB.extend(auxMemA.size());

                    driver.shiftCopyAuxVector(shift, auxMemA.addressOf(0), auxLo, n, auxMemB.addressOf(0), auxMemB.size());

                    for (int i = 0; i < n - auxLo; i++) {
                        Assert.assertNull(VarcharTypeDriver.getSplitValue(auxMemB, dataMemB, i, 1));
                        Assert.assertEquals("offset mismatch: i=" + i + ", n=" + n,
                                expectedOffsets.getQuick(i) - shift,
                                VarcharTypeDriver.getDataVectorSize(auxMemB, i * VARCHAR_AUX_WIDTH_BYTES));
                    }
                }
            }
        });
    }

    @Test
    public void testO3sortReverseOrder() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    MemoryCARW tsIndexMem = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARW auxMemA = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARW dataMemA = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARW auxMemB = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARW dataMemB = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
            ) {
                final VarcharTypeDriver driver = new VarcharTypeDriver();
                final Utf8StringSink utf8Sink = new Utf8StringSink();
                final Rnd rnd = new Rnd();

                final int n = 1000;
                for (int i = 0; i < n; i++) {
                    tsIndexMem.putLong128(0, n - i - 1);

                    utf8Sink.clear();
                    int len = rnd.nextInt(32);
                    switch (rnd.nextInt(3)) {
                        case 0: // null
                            VarcharTypeDriver.appendValue(auxMemA, dataMemA, null);
                            break;
                        case 1: // ascii
                            utf8Sink.repeat("a", len);
                            VarcharTypeDriver.appendValue(auxMemA, dataMemA, utf8Sink);
                            break;
                        case 2: // non-ascii
                            utf8Sink.repeat("ы", len);
                            VarcharTypeDriver.appendValue(auxMemA, dataMemA, utf8Sink);
                            break;
                        default:
                            assert false;
                    }
                }

                driver.o3sort(tsIndexMem.addressOf(0), n, dataMemA, auxMemA, dataMemB, auxMemB);

                for (int i = 0; i < n; i++) {
                    Utf8Sequence varcharA = VarcharTypeDriver.getSplitValue(auxMemA, dataMemA, (n - i - 1), 1);
                    Utf8Sequence varcharB = VarcharTypeDriver.getSplitValue(auxMemB, dataMemB, i, 1);
                    Assert.assertTrue((varcharA != null && varcharB != null) || (varcharA == null && varcharB == null));
                    if (varcharA != null) {
                        Assert.assertEquals(varcharA.isAscii(), varcharB.isAscii());
                        TestUtils.assertEquals(varcharA, varcharB);
                    }
                }
            }
        });
    }

    @Test
    public void testO3sortSameOrder() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    MemoryCARW tsIndexMem = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARW auxMemA = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARW dataMemA = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARW auxMemB = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARW dataMemB = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
            ) {
                final VarcharTypeDriver driver = new VarcharTypeDriver();
                final Utf8StringSink utf8Sink = new Utf8StringSink();
                final Rnd rnd = new Rnd();

                final int n = 1000;
                for (int i = 0; i < n; i++) {
                    tsIndexMem.putLong128(0, i);

                    utf8Sink.clear();
                    int len = rnd.nextInt(32);
                    switch (rnd.nextInt(3)) {
                        case 0: // null
                            VarcharTypeDriver.appendValue(auxMemA, dataMemA, null);
                            break;
                        case 1: // ascii
                            utf8Sink.repeat("a", len);
                            VarcharTypeDriver.appendValue(auxMemA, dataMemA, utf8Sink);
                            break;
                        case 2: // non-ascii
                            utf8Sink.repeat("ы", len);
                            VarcharTypeDriver.appendValue(auxMemA, dataMemA, utf8Sink);
                            break;
                        default:
                            assert false;
                    }
                }

                driver.o3sort(tsIndexMem.addressOf(0), n, dataMemA, auxMemA, dataMemB, auxMemB);

                for (int i = 0; i < n; i++) {
                    Utf8Sequence varcharA = VarcharTypeDriver.getSplitValue(auxMemA, dataMemA, i, 1);
                    Utf8Sequence varcharB = VarcharTypeDriver.getSplitValue(auxMemB, dataMemB, i, 1);
                    // offsets should match since the data is in-order
                    Assert.assertEquals(
                            VarcharTypeDriver.getDataVectorSize(auxMemA, i * VARCHAR_AUX_WIDTH_BYTES),
                            VarcharTypeDriver.getDataVectorSize(auxMemB, i * VARCHAR_AUX_WIDTH_BYTES));
                    Assert.assertTrue((varcharA != null && varcharB != null) || (varcharA == null && varcharB == null));
                    if (varcharA != null) {
                        Assert.assertEquals(varcharA.isAscii(), varcharB.isAscii());
                        TestUtils.assertEquals(varcharA, varcharB);
                    }
                }
            }
        });
    }

    @Test
    public void testRewindAppendPosition() throws Exception {
        // A row was cancelled after it moved a VARCHAR column, or did not move it. rewindAppendPosition() moves the
        // aux vector back to the start of the row, and only when the row moved it: each aux entry holds the data
        // offset of its row, so the data vector keeps the bytes of the cancelled row, which no row points at. It maps
        // no page that the files lack, and the next values land after the committed ones.
        TestUtils.assertMemoryLeak(() -> {
            for (CancelledRow cancelled : CancelledRow.values()) {
                assertRewind(cancelled);
            }
        });
    }

    @Test
    public void testSetAppendPosition() throws Exception {
        FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        long pageSize = Files.PAGE_SIZE;
        final Rnd rnd = TestUtils.generateRandom(null);

        try (Path auxPath = new Path().of(temp.newFile().getAbsolutePath());
             Path dataPath = new Path().of(temp.newFile().getAbsolutePath());
             MemoryCMARW auxMem = new MemoryCMARWImpl(ff, auxPath.$(), pageSize, -1, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE);
             MemoryCMARW dataMem = new MemoryCMARWImpl(ff, dataPath.$(), pageSize, -1, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE)) {

            Utf8StringSink utf8Sink = new Utf8StringSink();
            int n = 10000;
            LongList usedSpace = new LongList();
            long expectedUsedSize = 0;
            for (int i = 0; i < n; i++) {
                expectedUsedSize += VARCHAR_AUX_WIDTH_BYTES;
                if (i % 15 == 0) {
                    VarcharTypeDriver.appendValue(auxMem, dataMem, null);
                } else {
                    utf8Sink.clear();
                    final int len = i % 20;
                    char ch = (char) ('a' + i);
                    utf8Sink.repeat(ch, len);
                    if (utf8Sink.size() > VARCHAR_MAX_BYTES_FULLY_INLINED) {
                        expectedUsedSize += utf8Sink.size();
                    }
                    VarcharTypeDriver.appendValue(auxMem, dataMem, utf8Sink);
                }
                long actuallyUsedSize = (dataMem.getAppendOffset() + auxMem.getAppendOffset());
                Assert.assertEquals(expectedUsedSize, actuallyUsedSize);
                usedSpace.add(actuallyUsedSize);
            }

            StringSink stringSink = new StringSink();
            int minCut = Integer.MAX_VALUE;
            for (int z = 0; z < 10; z++) {
                int cut = rnd.nextInt(n);
                long actualUsedSize = VarcharTypeDriver.INSTANCE.setAppendPosition(cut, auxMem, dataMem);
                expectedUsedSize = (cut == 0) ? 0 : usedSpace.getQuick(cut - 1);
                Assert.assertEquals(expectedUsedSize, actualUsedSize);
                long expectedAuxUsedSize = VARCHAR_AUX_WIDTH_BYTES * (long) cut;
                long expectedDataUsedSize = expectedUsedSize - expectedAuxUsedSize;
                Assert.assertEquals(expectedAuxUsedSize, auxMem.getAppendOffset());
                Assert.assertEquals(expectedDataUsedSize, dataMem.getAppendOffset());

                for (int i = cut; i < n; i++) {
                    expectedUsedSize += 16;
                    if (i % 15 == 0) {
                        VarcharTypeDriver.appendValue(auxMem, dataMem, null);
                    } else {
                        utf8Sink.clear();
                        final int len = i % 40;
                        char ch = (char) ('A' + i);
                        utf8Sink.repeat(ch, len);
                        if (utf8Sink.size() > VARCHAR_MAX_BYTES_FULLY_INLINED) {
                            expectedUsedSize += utf8Sink.size();
                        }
                        VarcharTypeDriver.appendValue(auxMem, dataMem, utf8Sink);
                    }
                    long actuallyUsedSize = (dataMem.getAppendOffset() + auxMem.getAppendOffset());
                    Assert.assertEquals(expectedUsedSize, actuallyUsedSize);
                    usedSpace.add(i, actuallyUsedSize);
                }

                minCut = Math.min(minCut, cut);
                for (int i = 0; i < n; i++) {
                    stringSink.clear();
                    Utf8Sequence varchar = VarcharTypeDriver.getSplitValue(auxMem, dataMem, i, 1);
                    if (i % 15 == 0) {
                        Assert.assertNull(varchar);
                    } else {
                        Assert.assertNotNull(varchar);
                        stringSink.put(varchar);

                        int expectedLen = i < minCut ? i % 20 : i % 40;
                        char ch = i < minCut ? (char) ('a' + i) : (char) ('A' + i);
                        Assert.assertEquals("error in the string no. " + i, expectedLen, stringSink.length());
                        for (int j = 0; j < stringSink.length(); j++) {
                            Assert.assertEquals("error in the string no. " + i, ch, stringSink.charAt(j));
                        }
                    }
                }
            }

            VarcharTypeDriver.INSTANCE.setAppendPosition(0, auxMem, dataMem);
            Assert.assertEquals(0, dataMem.getAppendOffset());
            Assert.assertEquals(0, auxMem.getAppendOffset());
        }
    }

    private static void assertRewind(CancelledRow cancelled) throws Exception {
        final String context = cancelled.toString();
        final AllocationCountingFacade ff = new AllocationCountingFacade();
        final long pageSize = Files.PAGE_SIZE;
        final ObjList<String> values = new ObjList<>();
        final long committedRows = switch (cancelled) {
            case NULL_AT_ROW_ZERO -> 0;
            // values of 100 bytes in the data vector: the next one crosses the end of its first page
            case VALUE_CUT_BY_FAILED_PAGE -> pageSize / 100;
            // the aux entries end the first page of the aux file
            case NONE_AT_AUX_PAGE_END, VALUE_WITH_FAILED_AUX_PAGE, NULL_OPENS_AUX_PAGE -> pageSize / VARCHAR_AUX_WIDTH_BYTES;
            default -> 10;
        };
        for (int i = 0; i < committedRows; i++) {
            final char c = (char) ('a' + i % 26);
            // inlined in the aux entry, or longer than an entry holds
            values.add(cancelled == CancelledRow.VALUE_CUT_BY_FAILED_PAGE ? String.valueOf(c).repeat(100) : i % 2 == 0 ? String.valueOf(c) : String.valueOf(c).repeat(20));
        }
        try (
                Path auxPath = new Path().of(temp.newFile().getAbsolutePath());
                Path dataPath = new Path().of(temp.newFile().getAbsolutePath())
        ) {
            try (
                    MemoryPMARImpl auxMem = new MemoryPMARImpl(ff, auxPath.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                    MemoryPMARImpl dataMem = new MemoryPMARImpl(ff, dataPath.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)
            ) {
                for (int i = 0, n = values.size(); i < n; i++) {
                    VarcharTypeDriver.appendValue(auxMem, dataMem, utf8Of(values.getQuick(i)));
                }
                Assert.assertEquals(context, committedRows * VARCHAR_AUX_WIDTH_BYTES, auxMem.getAppendOffset());

                // the writes of the cancelled row
                switch (cancelled) {
                    case NULL, NULL_AT_ROW_ZERO, NULL_OPENS_AUX_PAGE -> VarcharTypeDriver.INSTANCE.appendNull(auxMem, dataMem);
                    case VALUE_CUT_BY_FAILED_PAGE -> failAppend(context, ff, dataMem.getFd(), auxMem, dataMem, "F".repeat(100));
                    case VALUE_WITH_FAILED_AUX_PAGE -> failAppend(context, ff, auxMem.getFd(), auxMem, dataMem, "F".repeat(20));
                    default -> {
                    }
                }
                if (cancelled == CancelledRow.NULL_OPENS_AUX_PAGE) {
                    // the entry of the NULL starts the second page of the aux file
                    Assert.assertEquals(context, pageSize + VARCHAR_AUX_WIDTH_BYTES, auxMem.getAppendOffset());
                }

                final int allocations = ff.allocations.get();
                final long dataOffset = dataMem.getAppendOffset();
                VarcharTypeDriver.INSTANCE.rewindAppendPosition(committedRows, auxMem, dataMem);
                Assert.assertEquals(context, allocations, ff.allocations.get());
                Assert.assertEquals(context, committedRows * VARCHAR_AUX_WIDTH_BYTES, auxMem.getAppendOffset());
                Assert.assertEquals(context, dataOffset, dataMem.getAppendOffset());

                // the next values: one longer than half a page, NULL, and a short one
                values.add("L".repeat((int) pageSize / 2));
                values.add(null);
                values.add("z");
                for (int i = (int) committedRows, n = values.size(); i < n; i++) {
                    VarcharTypeDriver.appendValue(auxMem, dataMem, utf8Of(values.getQuick(i)));
                }
            }
            try (
                    MemoryCMR auxMem = Vm.getCMRInstance(ff, auxPath.$(), ff.length(auxPath.$()), MemoryTag.MMAP_DEFAULT);
                    MemoryCMR dataMem = Vm.getCMRInstance(ff, dataPath.$(), ff.length(dataPath.$()), MemoryTag.MMAP_DEFAULT)
            ) {
                for (int i = 0, n = values.size(); i < n; i++) {
                    final Utf8Sequence actual = VarcharTypeDriver.getSplitValue(auxMem, dataMem, i, 1);
                    if (values.getQuick(i) == null) {
                        Assert.assertNull(context + ", row " + i, actual);
                    } else {
                        Assert.assertNotNull(context + ", row " + i, actual);
                        Assert.assertEquals(context + ", row " + i, values.getQuick(i), Utf8s.stringFromUtf8Bytes(actual));
                    }
                }
            }
        }
    }

    // the put of the value fails where it needs a page of the file of fd, which cannot be allocated
    private static void failAppend(String context, AllocationCountingFacade ff, long fd, MemoryMA auxMem, MemoryMA dataMem, String value) {
        ff.failingFd.set(fd);
        try {
            VarcharTypeDriver.appendValue(auxMem, dataMem, utf8Of(value));
            Assert.fail(context);
        } catch (CairoException e) {
            TestUtils.assertContains(context, e.getFlyweightMessage(), "No space left");
        }
        Assert.assertEquals(context, -1, ff.failingFd.get());
    }

    private static Utf8String utf8Of(String value) {
        return value == null ? null : new Utf8String(value);
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
        // the start of a value that crosses the end of the first page of the data file, whose next page cannot be
        // allocated
        VALUE_CUT_BY_FAILED_PAGE,
        // a value, whose aux entry needs the next page of the aux file, which cannot be allocated
        VALUE_WITH_FAILED_AUX_PAGE
    }
}
