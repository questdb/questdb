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


import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryAR;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Vect;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class VarcharTypeDriverTest extends AbstractTest {

    @Test
    public void testSmoke() throws Exception {
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
}
