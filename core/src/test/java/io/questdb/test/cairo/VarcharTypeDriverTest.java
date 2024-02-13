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
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.*;
import io.questdb.std.str.*;
import io.questdb.test.AbstractTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class VarcharTypeDriverTest extends AbstractTest {

    @Test
    public void testO3shiftCopyAuxVector() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final VarcharTypeDriver driver = new VarcharTypeDriver();
            final Utf8StringSink utf8Sink = new Utf8StringSink();
            final LongList expectedOffsets = new LongList();
            for (int n = 1; n < 100; n++) {
                try (
                        MemoryARW auxMemA = Vm.getARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryAR dataMemA = Vm.getARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryARW auxMemB = Vm.getARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryAR dataMemB = Vm.getARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
                ) {
                    final int len = n % 16;
                    final int shift = -n;
                    expectedOffsets.clear();
                    for (int i = 0; i < n; i++) {
                        utf8Sink.clear();
                        utf8Sink.repeat("a", len);
                        Utf8s.varcharAppend(dataMemA, auxMemA, utf8Sink);
                        expectedOffsets.add(dataMemA.getAppendOffset());
                    }

                    utf8Sink.clear();
                    utf8Sink.repeat("a", len);
                    final String expectedStr = utf8Sink.toString();

                    for (int i = 0; i < n; i++) {
                        Utf8Sequence varchar = Utf8s.varcharRead(i * 16L, dataMemA, auxMemA, 1);
                        Assert.assertEquals(expectedOffsets.getQuick(i), VarcharTypeDriver.varcharGetDataVectorSize(auxMemA, i * 16L));
                        Assert.assertNotNull(varchar);
                        TestUtils.assertEquals(expectedStr, varchar.asAsciiCharSequence());
                        Assert.assertTrue(varchar.isAscii());
                    }

                    dataMemB.extend(dataMemA.size() - shift);
                    Vect.memcpy(dataMemB.addressOf(-shift), dataMemA.addressOf(0), dataMemA.size());
                    auxMemB.extend(auxMemA.size());

                    driver.o3shiftCopyAuxVector(shift, auxMemA.addressOf(0), 0, n, auxMemB.addressOf(0));

                    for (int i = 0; i < n; i++) {
                        Utf8Sequence varchar = Utf8s.varcharRead(i * 16L, dataMemB, auxMemB, 1);
                        Assert.assertEquals("offset mismatch: i=" + i + ", n=" + n, expectedOffsets.getQuick(i) - shift, VarcharTypeDriver.varcharGetDataVectorSize(auxMemB, i * 16L));
                        Assert.assertNotNull(varchar);
                        TestUtils.assertEquals(expectedStr, varchar.asAsciiCharSequence());
                        Assert.assertTrue(varchar.isAscii());
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
                expectedUsedSize += 16; // aux vector entry size
                if (i % 15 == 0) {
                    Utf8s.varcharAppend(dataMem, auxMem, null);
                } else {
                    utf8Sink.clear();
                    final int len = i % 20;
                    char ch = (char) ('a' + i);
                    utf8Sink.repeat(ch, len);
                    if (utf8Sink.size() > Utf8s.UTF8_STORAGE_INLINE_BYTES) {
                        expectedUsedSize += (utf8Sink.size() - Utf8s.UTF8_STORAGE_SPLIT_BYTE);
                    }
                    Utf8s.varcharAppend(dataMem, auxMem, utf8Sink);
                }
                long actuallyUsedSize = (dataMem.getAppendOffset() + auxMem.getAppendOffset());
                Assert.assertEquals(expectedUsedSize, actuallyUsedSize);
                usedSpace.add(actuallyUsedSize);
            }

            StringSink stringSink = new StringSink();
            int minCut = Integer.MAX_VALUE;
            for (int z = 0; z < 10; z++) {
                int cut = rnd.nextInt(n);
                long actualUsedSize = VarcharTypeDriver.INSTANCE.setAppendPosition(cut, auxMem, dataMem, true);
                expectedUsedSize = (cut == 0) ? 0 : usedSpace.getQuick(cut - 1);
                Assert.assertEquals(expectedUsedSize, actualUsedSize);

                long expectedAuxUsedSize = cut * 16L;
                long expectedDataUsedSize = expectedUsedSize - expectedAuxUsedSize;
                Assert.assertEquals(expectedAuxUsedSize, auxMem.getAppendOffset());
                Assert.assertEquals(expectedDataUsedSize, dataMem.getAppendOffset());

                for (int i = cut; i < n; i++) {
                    expectedUsedSize += 16;
                    if (i % 15 == 0) {
                        Utf8s.varcharAppend(dataMem, auxMem, null);
                    } else {
                        utf8Sink.clear();
                        final int len = i % 40;
                        char ch = (char) ('A' + i);
                        utf8Sink.repeat(ch, len);
                        if (utf8Sink.size() > Utf8s.UTF8_STORAGE_INLINE_BYTES) {
                            expectedUsedSize += (utf8Sink.size() - Utf8s.UTF8_STORAGE_SPLIT_BYTE);
                        }
                        Utf8s.varcharAppend(dataMem, auxMem, utf8Sink);
                    }
                    long actuallyUsedSize = (dataMem.getAppendOffset() + auxMem.getAppendOffset());
                    Assert.assertEquals(expectedUsedSize, actuallyUsedSize);
                    usedSpace.add(i, actuallyUsedSize);
                }

                minCut = Math.min(minCut, cut);
                for (int i = 0; i < n; i++) {
                    stringSink.clear();
                    Utf8Sequence varchar = Utf8s.varcharRead(i * 16L, dataMem, auxMem, 1);
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

            VarcharTypeDriver.INSTANCE.setAppendPosition(0, auxMem, dataMem, false);
            Assert.assertEquals(0, dataMem.getAppendOffset());
            Assert.assertEquals(0, auxMem.getAppendOffset());
        }
    }
}
