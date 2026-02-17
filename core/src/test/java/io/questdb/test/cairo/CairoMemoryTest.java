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
import io.questdb.cairo.vm.MemoryCMORImpl;
import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.cairo.vm.MemoryPMARImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CairoMemoryTest extends AbstractTest {
    private static final FilesFacade FF = TestFilesFacadeImpl.INSTANCE;
    private static final int N = 1000000;

    @Test
    public void testAppendAfterMMapFailure() throws Exception {
        long used = Unsafe.getMemUsed();
        Rnd rnd = new Rnd();

        class X extends FilesFacadeImpl {
            boolean force = true;

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (force || rnd.nextBoolean()) {
                    force = false;
                    return super.mmap(fd, len, offset, flags, memoryTag);
                } else {
                    return -1;
                }
            }
        }

        X ff = new X();

        long openFileCount = ff.getOpenFileCount();
        int failureCount = 0;
        try (Path path = new Path()) {
            path.of(temp.newFile().getAbsolutePath());
            try (MemoryMA mem = Vm.getPMARInstance(null)) {
                mem.of(ff, path.$(), ff.getPageSize() * 2, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                int i = 0;
                while (i < N) {
                    try {
                        mem.putLong(i);
                        i++;
                    } catch (CairoException ignore) {
                        failureCount++;
                    }
                }
                Assert.assertEquals(N * 8, mem.getAppendOffset());
            }
        }
        Assert.assertTrue(failureCount > 0);
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(openFileCount, ff.getOpenFileCount());
    }

    @Test
    public void testAppendAllocateError() throws Exception {
        long used = Unsafe.getMemUsed();

        class X extends FilesFacadeImpl {
            boolean allClear = false;
            int count = 2;

            @Override
            public boolean allocate(long fd, long size) {
                if (allClear || --count > 0) {
                    return super.allocate(fd, size);
                }
                allClear = true;
                return false;
            }
        }

        X ff = new X();

        long openFileCount = ff.getOpenFileCount();
        try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
            try (MemoryPMARImpl mem = new MemoryPMARImpl(ff, path.$(), 2 * ff.getPageSize(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                try {
                    for (int i = 0; i < N * 10; i++) {
                        mem.putLong(i);
                    }
                    Assert.fail();
                } catch (CairoException ignore) {

                }
                Assert.assertTrue(mem.getAppendOffset() > 0);
            }
        }

        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(openFileCount, ff.getOpenFileCount());
    }

    @Test
    public void testAppendCannotOpenFile() {
        long used = Unsafe.getMemUsed();

        class X extends FilesFacadeImpl {
            @Override
            public long openRW(LPSZ name, int opts) {
                int n = name.size();
                if (n > 5 && Utf8s.equalsAscii(".fail", name, n - 5, n)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        }

        X ff = new X();

        long openFileCount = ff.getOpenFileCount();
        int successCount = 0;
        int failCount = 0;
        try (Path path = new Path()) {
            path.of(temp.getRoot().getAbsolutePath());
            int prefixLen = path.size();
            try (MemoryMA mem = Vm.getPMARInstance(null)) {
                Rnd rnd = new Rnd();
                for (int k = 0; k < 10; k++) {
                    path.trimTo(prefixLen).concat(rnd.nextString(10));

                    boolean fail = rnd.nextBoolean();
                    if (fail) {
                        path.put(".fail").$();
                        failCount++;
                    } else {
                        path.put(".data").$();
                        successCount++;
                    }

                    if (fail) {
                        try {
                            mem.of(ff, path.$(), 2 * ff.getPageSize(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                            Assert.fail();
                        } catch (CairoException ignored) {
                        }
                    } else {
                        mem.of(ff, path.$(), 2 * ff.getPageSize(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                        for (int i = 0; i < N; i++) {
                            mem.putLong(i);
                        }
                        Assert.assertEquals(N * 8, mem.getAppendOffset());
                    }
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(openFileCount, ff.getOpenFileCount());
        Assert.assertTrue(failCount > 0);
        Assert.assertTrue(successCount > 0);
    }

    @Test
    public void testAppendMemoryJump() throws Exception {
        testVirtualMemoryJump(path -> new MemoryPMARImpl(
                        FF,
                        path.$(),
                        FF.getPageSize(),
                        MemoryTag.MMAP_DEFAULT,
                        CairoConfiguration.O_NONE
                )
        );
    }

    @Test
    public void testCMARWImplLeavesFdsOpenOnDetachFdClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long used = Unsafe.getMemUsed();
            try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
                MemoryCMARW mem = Vm.getCMARWInstance(
                        FF,
                        path.$(),
                        2 * FF.getPageSize(),
                        -1,
                        MemoryTag.MMAP_DEFAULT,
                        CairoConfiguration.O_NONE
                );
                for (int i = 0; i < N; i++) {
                    mem.putLong(i);
                }
                Assert.assertEquals(8L * N, mem.getAppendOffset());
                long fd = mem.getFd();
                mem.detachFdClose();

                MemoryCMORImpl memR = new MemoryCMORImpl();
                memR.ofOffset(FF, fd, false, null, 0, 8 * N, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, memR.getLong(i * 8));
                }
                memR.detachFdClose();

                FF.close(fd);
            }
            Assert.assertEquals(used, Unsafe.getMemUsed());
        });
    }

    @Test
    public void testPMARImplLeavesFdsOpenOnDetachFdClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long used = Unsafe.getMemUsed();
            try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
                MemoryPMARImpl mem = new MemoryPMARImpl(FF, path.$(), 2 * FF.getPageSize(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                for (int i = 0; i < N; i++) {
                    mem.putLong(i);
                }
                long fd = mem.getFd();
                mem.detachFdClose();

                MemoryCMORImpl memR = new MemoryCMORImpl();
                memR.ofOffset(FF, fd, false, null, 0, 8 * N, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, memR.getLong(i * 8));
                }
                memR.detachFdClose();

                FF.close(fd);
            }
            Assert.assertEquals(used, Unsafe.getMemUsed());
        });
    }

    @Test
    public void testReadWriteCannotOpenFile() {
        long used = Unsafe.getMemUsed();

        class X extends FilesFacadeImpl {
            @Override
            public long openRW(LPSZ name, int opts) {
                int n = name.size();
                if (n > 5 && Utf8s.equalsAscii(".fail", name, n - 5, n)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        }

        X ff = new X();

        long openFileCount = ff.getOpenFileCount();
        int successCount = 0;
        int failCount = 0;
        try (Path path = new Path()) {
            path.of(temp.getRoot().getAbsolutePath());
            int prefixLen = path.size();
            try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                Rnd rnd = new Rnd();
                for (int k = 0; k < 10; k++) {
                    path.trimTo(prefixLen).concat(rnd.nextString(10));

                    boolean fail = rnd.nextBoolean();
                    if (fail) {
                        path.put(".fail").$();
                        failCount++;
                    } else {
                        path.put(".data").$();
                        successCount++;
                    }

                    if (fail) {
                        try {
                            mem.of(ff, path.$(), 2 * ff.getPageSize(), -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                            Assert.fail();
                        } catch (CairoException ignored) {
                        }
                    } else {
                        mem.of(ff, path.$(), 2 * ff.getPageSize(), -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                        for (int i = 0; i < N; i++) {
                            mem.putLong(i);
                        }
                        Assert.assertEquals(N * 8, mem.getAppendOffset());
                    }
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(openFileCount, ff.getOpenFileCount());
        Assert.assertTrue(failCount > 0);
        Assert.assertTrue(successCount > 0);
    }

    @Test
    public void testReadWriteMemoryTruncate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
                int pageSize = 1024 * 1024;
                try (MemoryCMARW mem = Vm.getSmallCMARWInstance(FF, path.$(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                    int count = 2 * pageSize / Long.BYTES;
                    for (int i = 0; i < count; i++) {
                        mem.putLong(i);
                    }

                    long fileSize = FF.length(path.$());

                    // read the whole file
                    long addr = FF.mmap(mem.getFd(), fileSize, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
                    try {
                        for (int i = 0; i < count; i++) {
                            Assert.assertEquals(i, Unsafe.getUnsafe().getLong(addr + i * Long.BYTES));
                        }
                    } finally {
                        FF.munmap(addr, fileSize, MemoryTag.MMAP_DEFAULT);
                    }
                    // truncate
                    mem.truncate();

                    // ensure that entire file is zeroed out
                    fileSize = FF.length(path.$());
                    addr = FF.mmap(mem.getFd(), fileSize, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
                    try {
                        for (int i = 0; i < fileSize / Long.BYTES; i++) {
                            Assert.assertEquals(0, Unsafe.getUnsafe().getLong(addr + i * 8L));
                        }
                    } finally {
                        FF.munmap(addr, fileSize, MemoryTag.MMAP_DEFAULT);
                    }
                }
            }
        });
    }

    @Test
    public void testWriteAndRead() throws Exception {
        long used = Unsafe.getMemUsed();
        try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
            try (
                    MemoryCMARW mem = Vm.getCMARWInstance(
                            FF,
                            path.$(),
                            2 * FF.getPageSize(),
                            -1,
                            MemoryTag.MMAP_DEFAULT,
                            CairoConfiguration.O_NONE
                    )
            ) {
                for (int i = 0; i < N; i++) {
                    mem.putLong(i);
                }
                // read in place
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8));
                }

                Assert.assertEquals(8L * N, mem.getAppendOffset());
            }
            try (MemoryCMARW mem = Vm.getSmallCMARWInstance(FF, path.$(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                final int M = (int) (mem.size() / Long.BYTES);
                for (int i = 0; i < M; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8L));
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testWriteAndReadWithReadOnlyMem() throws Exception {
        long used = Unsafe.getMemUsed();
        try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
            try (
                    MemoryCMARW mem = Vm.getCMARWInstance(
                            FF,
                            path.$(),
                            2 * FF.getPageSize(),
                            -1,
                            MemoryTag.MMAP_DEFAULT,
                            CairoConfiguration.O_NONE
                    )
            ) {
                for (int i = 0; i < N; i++) {
                    mem.putLong(i);
                }
                Assert.assertEquals(8L * N, mem.getAppendOffset());
            }
            try (MemoryMR mem = new MemoryCMRImpl(FF, path.$(), 8L * N, MemoryTag.MMAP_DEFAULT)) {
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8));
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testWriteOverMapFailuresAndRead() throws Exception {
        Rnd rnd = new Rnd();
        class X extends FilesFacadeImpl {
            @Override
            public long getMapPageSize() {
                return super.getPageSize();
            }

            @Override
            public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
                if (rnd.nextBoolean()) {
                    return -1;
                }
                return super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
            }
        }

        final X ff = new X();
        TestUtils.assertMemoryLeak(() -> {
            int writeFailureCount = 0;
            try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
                try (MemoryCMARW mem = Vm.getSmallCMARWInstance(ff, path.$(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                    int i = 0;
                    while (i < N) {
                        try {
                            mem.putLong(i);
                            i++;
                        } catch (CairoException ignore) {
                            writeFailureCount++;
                            break;
                        }
                    }
                }
            }
            Assert.assertTrue(writeFailureCount > 0);
        });
    }

    private void testVirtualMemoryJump(VirtualMemoryFactory factory) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
                try (MemoryARW mem = factory.newInstance(path)) {
                    for (int i = 0; i < 100; i++) {
                        mem.putLong(i);
                    }
                    mem.jumpTo(0);
                    for (int i = 0; i < 50; i++) {
                        mem.putLong(50 - i);
                    }
                    // keep previously written data
                    mem.jumpTo(800);
                }

                try (MemoryMR roMem = new MemoryCMRImpl(FF, path.$(), 800, MemoryTag.MMAP_DEFAULT)) {
                    for (int i = 0; i < 50; i++) {
                        Assert.assertEquals(50 - i, roMem.getLong(i * 8));
                    }

                    for (int i = 50; i < 100; i++) {
                        Assert.assertEquals(i, roMem.getLong(i * 8));
                    }
                }
            }
        });
    }

    @FunctionalInterface
    private interface VirtualMemoryFactory {
        MemoryARW newInstance(Path path);
    }
}
