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

package io.questdb.test.cairo.vm;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.cairo.vm.MemoryPMRImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MemoryPMRImplTest extends AbstractTest {
    private static final FilesFacade FF = TestFilesFacadeImpl.INSTANCE;

    @Test
    public void testCrossPageGetLong() throws Exception {
        try (Path path = new Path().of(root).concat(testName.getMethodName())) {
            createLongFile(path, 3L * Files.PAGE_SIZE / Long.BYTES);
            TestUtils.assertMemoryLeak(() -> {
                try (
                        MemoryCMRImpl contiguous = new MemoryCMRImpl();
                        MemoryPMRImpl paged = new MemoryPMRImpl(Files.PAGE_SIZE, 2)
                ) {
                    contiguous.of(FF, path.$(), Files.PAGE_SIZE, -1L, MemoryTag.NATIVE_DEFAULT);
                    paged.of(FF, path.$(), Files.PAGE_SIZE, -1L, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE, -1);

                    final long from = Files.PAGE_SIZE - 16;
                    final long to = Files.PAGE_SIZE + 16;
                    for (long offset = from; offset <= to; offset++) {
                        Assert.assertEquals("offset=" + offset, contiguous.getLong(offset), paged.getLong(offset));
                    }
                }
            });
        }
    }

    @Test
    public void testEvictionRespectsMaxMappedPages() throws Exception {
        try (Path path = new Path().of(root).concat(testName.getMethodName())) {
            createLongFile(path, 6L * Files.PAGE_SIZE / Long.BYTES);
            TestUtils.assertMemoryLeak(() -> {
                try (MemoryPMRImpl paged = new MemoryPMRImpl(Files.PAGE_SIZE, 2)) {
                    paged.of(FF, path.$(), Files.PAGE_SIZE, -1L, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE, -1);

                    paged.getLong(0);
                    paged.getLong(Files.PAGE_SIZE);
                    Assert.assertEquals(2, paged.getPageCount());
                    Assert.assertNotEquals(0, paged.getPageAddress(0));
                    Assert.assertNotEquals(0, paged.getPageAddress(1));

                    paged.getLong(2L * Files.PAGE_SIZE);
                    Assert.assertEquals(2, paged.getPageCount());
                    Assert.assertEquals(0, paged.getPageAddress(0));
                    Assert.assertNotEquals(0, paged.getPageAddress(1));
                    Assert.assertNotEquals(0, paged.getPageAddress(2));
                }
            });
        }
    }

    @Test
    public void testLogicalBoundsEnforced() throws Exception {
        try (Path path = new Path().of(root).concat(testName.getMethodName())) {
            createLongFile(path, 2L * Files.PAGE_SIZE / Long.BYTES);
            TestUtils.assertMemoryLeak(() -> {
                try (MemoryPMRImpl paged = new MemoryPMRImpl(Files.PAGE_SIZE, 2)) {
                    paged.of(FF, path.$(), Files.PAGE_SIZE, -1L, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE, -1);

                    try {
                        paged.getLong(paged.size() - 7);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "outside of file boundary");
                    }

                    try {
                        paged.addressOf(paged.size() + 1);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "outside of file boundary");
                    }
                }
            });
        }
    }

    @Test
    public void testPinnedPageIsNotEvicted() throws Exception {
        try (Path path = new Path().of(root).concat(testName.getMethodName())) {
            createLongFile(path, 6L * Files.PAGE_SIZE / Long.BYTES);
            TestUtils.assertMemoryLeak(() -> {
                try (MemoryPMRImpl paged = new MemoryPMRImpl(Files.PAGE_SIZE, 2)) {
                    paged.of(FF, path.$(), Files.PAGE_SIZE, -1L, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE, -1);

                    final int pin = paged.pin(0);
                    paged.getLong(Files.PAGE_SIZE);
                    paged.getLong(2L * Files.PAGE_SIZE);

                    Assert.assertNotEquals(0, paged.getPageAddress(0));
                    Assert.assertEquals(2, paged.getPageCount());

                    paged.unpin(pin);
                    paged.getLong(3L * Files.PAGE_SIZE);
                    Assert.assertEquals(0, paged.getPageAddress(0));
                    Assert.assertEquals(2, paged.getPageCount());
                }
            });
        }
    }

    private void createLongFile(Path path, long longCount) {
        if (!FF.touch(path.$())) {
            Assert.fail("failed to create file [errno=" + FF.errno() + ']');
        }

        final long fd = TableUtils.openFileRWOrFail(FF, path.$(), CairoConfiguration.O_NONE);
        try (MemoryMARW mem = Vm.getCMARWInstance()) {
            mem.of(FF, fd, null, Files.PAGE_SIZE, MemoryTag.NATIVE_DEFAULT);
            for (long i = 0; i < longCount; i++) {
                mem.putLong(i * 11 + 7);
            }
        }
    }
}
