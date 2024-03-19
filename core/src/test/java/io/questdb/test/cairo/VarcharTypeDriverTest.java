/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryAR;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCMARW;
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
    public void testGetDataVectorSize() throws Exception {
        final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        final VarcharTypeDriver driver = new VarcharTypeDriver();
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(temp.newFile().getAbsolutePath()).$()) {
                try (
                        MemoryCMARW auxMem = Vm.getSmallCMARWInstance(ff, path, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                        MemoryCARW dataMem = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
                ) {
                    VarcharTypeDriver.appendValue(dataMem, auxMem, null);
                    String testStr = "foobarbaz - bazbarfoo";
                    int length = testStr.length();
                    VarcharTypeDriver.appendValue(dataMem, auxMem, new Utf8String(testStr));

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

                    driver.setColumnRefs(auxMem.addressOf(auxOffset * 2 * Long.BYTES), dataOffset, n);

                    for (int i = 0; i < n; i++) {
                        Assert.assertNull(VarcharTypeDriver.getValue((i + auxOffset), dataMem, auxMem, 1));
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
                        MemoryARW auxMemA = Vm.getARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryAR dataMemA = Vm.getARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryARW auxMemB = Vm.getARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryAR dataMemB = Vm.getARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
                ) {
                    final int len = n % 16;
                    final int auxLo = n % auxLoBase;
                    final int shift = -n;
                    expectedOffsets.clear();
                    for (int i = 0; i < n; i++) {
                        utf8Sink.clear();
                        utf8Sink.repeat("a", len);
                        VarcharTypeDriver.appendValue(dataMemA, auxMemA, utf8Sink);
                        if (i >= auxLo) {
                            expectedOffsets.add(dataMemA.getAppendOffset());
                        }
                    }

                    utf8Sink.clear();
                    utf8Sink.repeat("a", len);
                    final String expectedStr = utf8Sink.toString();

                    for (int i = auxLo; i < n; i++) {
                        Utf8Sequence varchar = VarcharTypeDriver.getValue(i, dataMemA, auxMemA, 1);
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

                    driver.shiftCopyAuxVector(shift, auxMemA.addressOf(0), auxLo, n, auxMemB.addressOf(0));

                    for (int i = 0; i < n - auxLo; i++) {
                        Utf8Sequence varchar = VarcharTypeDriver.getValue(i, dataMemB, auxMemB, 1);
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
                        MemoryARW auxMemA = Vm.getARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryAR dataMemA = Vm.getARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryARW auxMemB = Vm.getARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryAR dataMemB = Vm.getARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
                ) {
                    final int auxLo = n % auxLoBase;
                    final int shift = -42;
                    expectedOffsets.clear();
                    for (int i = 0; i < n; i++) {
                        VarcharTypeDriver.appendValue(dataMemA, auxMemA, null);
                        if (i >= auxLo) {
                            expectedOffsets.add(dataMemA.getAppendOffset());
                        }
                    }

                    for (int i = auxLo; i < n; i++) {
                        Assert.assertNull(VarcharTypeDriver.getValue(i, dataMemA, auxMemA, 1));
                        Assert.assertEquals(expectedOffsets.getQuick(i - auxLo),
                                VarcharTypeDriver.getDataVectorSize(auxMemA, i * VARCHAR_AUX_WIDTH_BYTES));
                    }

                    dataMemB.extend(dataMemA.size() - shift);
                    Vect.memcpy(dataMemB.addressOf(-shift), dataMemA.addressOf(0), dataMemA.size());
                    auxMemB.extend(auxMemA.size());

                    driver.shiftCopyAuxVector(shift, auxMemA.addressOf(0), auxLo, n, auxMemB.addressOf(0));

                    for (int i = 0; i < n - auxLo; i++) {
                        Assert.assertNull(VarcharTypeDriver.getValue(i, dataMemB, auxMemB, 1));
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
                            VarcharTypeDriver.appendValue(dataMemA, auxMemA, null);
                            break;
                        case 1: // ascii
                            utf8Sink.repeat("a", len);
                            VarcharTypeDriver.appendValue(dataMemA, auxMemA, utf8Sink);
                            break;
                        case 2: // non-ascii
                            utf8Sink.repeat("ы", len);
                            VarcharTypeDriver.appendValue(dataMemA, auxMemA, utf8Sink);
                            break;
                        default:
                            assert false;
                    }
                }

                driver.o3sort(tsIndexMem.addressOf(0), n, dataMemA, auxMemA, dataMemB, auxMemB);

                for (int i = 0; i < n; i++) {
                    Utf8Sequence varcharA = VarcharTypeDriver.getValue((n - i - 1), dataMemA, auxMemA, 1);
                    Utf8Sequence varcharB = VarcharTypeDriver.getValue(i, dataMemB, auxMemB, 1);
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
                            VarcharTypeDriver.appendValue(dataMemA, auxMemA, null);
                            break;
                        case 1: // ascii
                            utf8Sink.repeat("a", len);
                            VarcharTypeDriver.appendValue(dataMemA, auxMemA, utf8Sink);
                            break;
                        case 2: // non-ascii
                            utf8Sink.repeat("ы", len);
                            VarcharTypeDriver.appendValue(dataMemA, auxMemA, utf8Sink);
                            break;
                        default:
                            assert false;
                    }
                }

                driver.o3sort(tsIndexMem.addressOf(0), n, dataMemA, auxMemA, dataMemB, auxMemB);

                for (int i = 0; i < n; i++) {
                    Utf8Sequence varcharA = VarcharTypeDriver.getValue(i, dataMemA, auxMemA, 1);
                    Utf8Sequence varcharB = VarcharTypeDriver.getValue(i, dataMemB, auxMemB, 1);
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
    public void testSetAppendPosition() throws Exception {
        FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        long pageSize = Files.PAGE_SIZE;
        final Rnd rnd = TestUtils.generateRandom(null);

        try (Path auxPath = new Path().of(temp.newFile().getAbsolutePath()).$();
             Path dataPath = new Path().of(temp.newFile().getAbsolutePath()).$();
             MemoryCMARW auxMem = new MemoryCMARWImpl(ff, auxPath, pageSize, -1, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE);
             MemoryCMARW dataMem = new MemoryCMARWImpl(ff, dataPath, pageSize, -1, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE)) {

            Utf8StringSink utf8Sink = new Utf8StringSink();
            int n = 10000;
            LongList usedSpace = new LongList();
            long expectedUsedSize = 0;
            for (int i = 0; i < n; i++) {
                expectedUsedSize += VARCHAR_AUX_WIDTH_BYTES;
                if (i % 15 == 0) {
                    VarcharTypeDriver.appendValue(dataMem, auxMem, null);
                } else {
                    utf8Sink.clear();
                    final int len = i % 20;
                    char ch = (char) ('a' + i);
                    utf8Sink.repeat(ch, len);
                    if (utf8Sink.size() > VARCHAR_MAX_BYTES_FULLY_INLINED) {
                        expectedUsedSize += utf8Sink.size();
                    }
                    VarcharTypeDriver.appendValue(dataMem, auxMem, utf8Sink);
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
                        VarcharTypeDriver.appendValue(dataMem, auxMem, null);
                    } else {
                        utf8Sink.clear();
                        final int len = i % 40;
                        char ch = (char) ('A' + i);
                        utf8Sink.repeat(ch, len);
                        if (utf8Sink.size() > VARCHAR_MAX_BYTES_FULLY_INLINED) {
                            expectedUsedSize += utf8Sink.size();
                        }
                        VarcharTypeDriver.appendValue(dataMem, auxMem, utf8Sink);
                    }
                    long actuallyUsedSize = (dataMem.getAppendOffset() + auxMem.getAppendOffset());
                    Assert.assertEquals(expectedUsedSize, actuallyUsedSize);
                    usedSpace.add(i, actuallyUsedSize);
                }

                minCut = Math.min(minCut, cut);
                for (int i = 0; i < n; i++) {
                    stringSink.clear();
                    Utf8Sequence varchar = VarcharTypeDriver.getValue(i, dataMem, auxMem, 1);
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
}
