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
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;

public class VarcharFdAccessorTest extends AbstractTest {
    // 20 bytes -> longer than VARCHAR_MAX_BYTES_FULLY_INLINED (9), so stored as split
    private static final String SPLIT = "AAAABBBBCCCCDDDDEEEE";

    @Test
    public void testDeepColumnNoStackOverflow() throws Exception {
        // Previously the recursive getDataVectorSizeAtFromFd would StackOverflow on large row counts.
        // This test writes 10000 split rows and calls the accessor on the last row to prove it
        // returns the correct total without any recursion/overflow.
        final var ff = TestFilesFacadeImpl.INSTANCE;
        TestUtils.assertMemoryLeak(() -> {
            try (Path auxPath = new Path().of(temp.newFile().getAbsolutePath())) {
                final int rows = 10000;
                try (MemoryCMARW auxMem = Vm.getSmallCMARWInstance(ff, auxPath.$(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                     MemoryCARW dataMem = Vm.getCARWInstance(rows * SPLIT.length() + 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    for (int i = 0; i < rows; i++) {
                        VarcharTypeDriver.appendValue(auxMem, dataMem, new Utf8String(SPLIT));
                    }
                }
                long auxFd = ff.openRO(auxPath.$());
                try {
                    long sz = VarcharTypeDriver.INSTANCE.getDataVectorSizeAtFromFd(ff, auxFd, rows - 1);
                    Assert.assertEquals((long) rows * SPLIT.length(), sz);
                } finally {
                    ff.close(auxFd);
                }
            }
        });
    }

    @Test
    public void testTornZeroOffsetThrows() throws Exception {
        final var ff = TestFilesFacadeImpl.INSTANCE;
        TestUtils.assertMemoryLeak(() -> {
            try (Path auxPath = new Path().of(temp.newFile().getAbsolutePath());
                 Path dataPath = new Path().of(temp.newFile().getAbsolutePath())) {
                final int rows = 5;
                try (MemoryCMARW auxMem = Vm.getSmallCMARWInstance(ff, auxPath.$(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                     MemoryCARW dataMem = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    for (int i = 0; i < rows; i++) {
                        VarcharTypeDriver.appendValue(auxMem, dataMem, new Utf8String(SPLIT));
                    }

                    // clean read: must return correct size and must NOT throw for row 0
                    long auxFdClean = ff.openRO(auxPath.$());
                    try {
                        long sz = VarcharTypeDriver.INSTANCE.getDataVectorSizeAtFromFd(ff, auxFdClean, rows - 1);
                        // last row ends at offset (rows-1)*SPLIT.length() + SPLIT.length() = rows * SPLIT.length()
                        Assert.assertEquals((long) rows * SPLIT.length(), sz);
                        // row 0: offset is 0 for first split entry, must NOT throw
                        VarcharTypeDriver.INSTANCE.getDataVectorSizeAtFromFd(ff, auxFdClean, 0);
                    } finally {
                        ff.close(auxFdClean);
                    }

                    // corrupt: zero bytes 8-15 of the last aux entry (prefix + offset fields)
                    // making the decoded data offset come out to 0 while the header is still valid
                    auxMem.putLong((long) (rows - 1) * VARCHAR_AUX_WIDTH_BYTES + 8L, 0L);
                }

                // reopen the corrupt aux file and assert the accessor throws
                long auxFd = ff.openRO(auxPath.$());
                try {
                    VarcharTypeDriver.INSTANCE.getDataVectorSizeAtFromFd(ff, auxFd, rows - 1);
                    Assert.fail("expected CairoException on torn zero offset");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Invalid data offset read from varchar aux file");
                } finally {
                    ff.close(auxFd);
                }
            }
        });
    }
}
