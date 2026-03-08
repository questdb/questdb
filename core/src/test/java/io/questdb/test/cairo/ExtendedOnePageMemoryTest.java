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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExtendedOnePageMemoryTest extends AbstractTest {
    private static final AtomicBoolean FILE_MAP_FAIL = new AtomicBoolean(false);
    private static final int FILE_SIZE = 1024;
    private static final Path path = new Path(4096);
    private static FilesFacade ff;

    @AfterClass
    public static void afterClass() {
        path.close();
    }

    @BeforeClass
    public static void beforeClass() {
        ff = new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (FILE_MAP_FAIL.compareAndSet(true, false)) {
                    return FilesFacade.MAP_FAILED;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
                if (FILE_MAP_FAIL.compareAndSet(true, false)) {
                    return FilesFacade.MAP_FAILED;
                }
                return super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
            }
        };
    }

    @Test
    public void testFailOnGrow() throws IOException {
        createFile();
        try (MemoryMR mem = new MemoryCMRImpl()) {
            int sz = FILE_SIZE / 2;
            mem.of(ff, path.$(), sz, sz, MemoryTag.MMAP_DEFAULT);
            FILE_MAP_FAIL.set(true);
            sz *= 2;
            try {
                mem.extend(sz);
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not remap");
            }
        }
    }

    @Test
    public void testFailOnInitialMap() throws IOException {
        createFile();
        try (MemoryMR mem = new MemoryCMRImpl()) {
            FILE_MAP_FAIL.set(true);
            try {
                mem.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not mmap");
            }
        }
    }

    private void createFile() throws IOException {
        File f = temp.newFile();
        try (FileOutputStream fos = new FileOutputStream(f)) {
            for (int i = 0; i < ExtendedOnePageMemoryTest.FILE_SIZE; i++) {
                fos.write(0);
            }
        }
        path.of(f.getCanonicalPath()).$();
    }
}
