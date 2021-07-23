/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo;

import io.questdb.cairo.vm.*;
import io.questdb.cairo.vm.api.CMARWMemory;
import io.questdb.cairo.vm.api.MRMemory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CairoMemoryTest {
    private static final int N = 1000000;
    private static final Log LOG = LogFactory.getLog(CairoMemoryTest.class);
    private static final FilesFacade FF = FilesFacadeImpl.INSTANCE;

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @BeforeClass
    public static void setUp() {
        LOG.info().$("Begin test").$();
    }

    @Test
    public void testAppendAfterMMapFailure() throws Exception {
        long used = Unsafe.getMemUsed();
        Rnd rnd = new Rnd();

        class X extends FilesFacadeImpl {
            boolean force = true;

            @Override
            public long mmap(long fd, long len, long offset, int flags) {
                if (force || rnd.nextBoolean()) {
                    force = false;
                    return super.mmap(fd, len, offset, flags);
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
            try (MAMemoryImpl mem = new MAMemoryImpl()) {
                mem.of(ff, path.$(), ff.getPageSize() * 2);
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
    public void testAppendCannotOpenFile() {
        long used = Unsafe.getMemUsed();

        class X extends FilesFacadeImpl {
            @Override
            public long openRW(LPSZ name) {
                int n = name.length();
                if (n > 5 && Chars.equals(".fail", name, n - 5, n)) {
                    return -1;
                }
                return super.openRW(name);
            }
        }

        X ff = new X();

        long openFileCount = ff.getOpenFileCount();
        int successCount = 0;
        int failCount = 0;
        try (Path path = new Path()) {
            path.of(temp.getRoot().getAbsolutePath());
            int prefixLen = path.length();
            try (MAMemoryImpl mem = new MAMemoryImpl()) {
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
                            mem.of(ff, path, 2 * ff.getPageSize());
                            Assert.fail();
                        } catch (CairoException ignored) {
                        }
                    } else {
                        mem.of(ff, path, 2 * ff.getPageSize());
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
        testVirtualMemoryJump(path -> new MAMemoryImpl(FF, path, FF.getPageSize()));
    }

    @Test
    public void testAppendAllocateError() throws Exception {
        long used = Unsafe.getMemUsed();

        class X extends FilesFacadeImpl {
            int count = 2;
            boolean allClear = false;

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
        try (Path path = new Path().of(temp.newFile().getAbsolutePath()).$()) {
            try (MAMemoryImpl mem = new MAMemoryImpl(ff, path, 2 * ff.getPageSize())) {
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
    public void testReadWriteCannotOpenFile() {
        long used = Unsafe.getMemUsed();

        class X extends FilesFacadeImpl {
            @Override
            public long openRW(LPSZ name) {
                int n = name.length();
                if (n > 5 && Chars.equals(".fail", name, n - 5, n)) {
                    return -1;
                }
                return super.openRW(name);
            }
        }

        X ff = new X();

        long openFileCount = ff.getOpenFileCount();
        int successCount = 0;
        int failCount = 0;
        try (Path path = new Path()) {
            path.of(temp.getRoot().getAbsolutePath());
            int prefixLen = path.length();
            try (CMARWMemory mem = new CMARWMemoryImpl()) {
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
                            mem.of(ff, path, 2 * ff.getPageSize(), Long.MAX_VALUE);
                            Assert.fail();
                        } catch (CairoException ignored) {
                        }
                    } else {
                        mem.of(ff, path, 2 * ff.getPageSize(), Long.MAX_VALUE);
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
    public void testReadWriteMemoryJump() throws Exception {
        testVirtualMemoryJump(path -> new PagedMappedReadWriteMemory(FF, path, FF.getPageSize()));
    }

    @Test
    public void testReadWriteMemoryTruncate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(temp.newFile().getAbsolutePath()).$()) {
                int pageSize = 1024 * 1024;
                try (CMARWMemory mem = new CMARWMemoryImpl(FF, path, pageSize, Long.MAX_VALUE)) {
                    int count = 2 * pageSize / Long.BYTES;
                    for (int i = 0; i < count; i++) {
                        mem.putLong(i);
                    }

                    long fileSize = FF.length(path);

                    // read the whole file
                    long addr = FF.mmap(mem.getFd(), fileSize, 0, Files.MAP_RO);
                    try {
                        for (int i = 0; i < count; i++) {
                            Assert.assertEquals(i, Unsafe.getUnsafe().getLong(addr + i * Long.BYTES));
                        }
                    } finally {
                        FF.munmap(addr, fileSize);
                    }
                    // truncate
                    mem.truncate();

                    // ensure that entire file is zeroed out
                    fileSize = FF.length(path);
                    addr = FF.mmap(mem.getFd(), fileSize, 0, Files.MAP_RO);
                    try {
                        for (int i = 0; i < fileSize / Long.BYTES; i++) {
                            Assert.assertEquals(0, Unsafe.getUnsafe().getLong(addr + i * 8L));
                        }
                    } finally {
                        FF.munmap(addr, fileSize);
                    }

                }
            }
        });
    }

    @Test
    public void testSlidingWindowMemory() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                path.of(temp.getRoot().getAbsolutePath());
                final int N = 100000;
                final Rnd rnd = new Rnd();
                try (MAMemoryImpl mem = new MAMemoryImpl()) {
                    mem.of(FF, path.concat("x.dat").$(), FF.getPageSize());


                    for (int i = 0; i < N; i++) {
                        mem.putLong(rnd.nextLong());
                    }

                    try (PagedSlidingReadOnlyMemory mem2 = new PagedSlidingReadOnlyMemory()) {
                        mem2.of(mem);

                        // try to read outside of original page bounds
                        try {
                            mem2.getLong(N * 16);
                            Assert.fail();
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "Trying to map read-only page outside");
                        }

                        // make sure jump() is reported
                        try {
                            mem2.jumpTo(1024);
                            Assert.fail();
                        } catch (UnsupportedOperationException e) {
                            TestUtils.assertContains(e.getMessage(), "Cannot jump() read-only memory");
                        }

                        rnd.reset();
                        for (int i = 0; i < N; i++) {
                            Assert.assertEquals(rnd.nextLong(), mem2.getLong(i * 8));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSlidingWindowMemoryCannotMap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                path.of(temp.getRoot().getAbsolutePath());
                final int N = 100000;
                final Rnd rnd = new Rnd();

                FilesFacade ff = new FilesFacadeImpl() {
                    int counter = 2;

                    @Override
                    public long mmap(long fd, long len, long offset, int flags) {
                        if (flags == Files.MAP_RO && --counter == 0) {
                            return -1;
                        }
                        return super.mmap(fd, len, offset, flags);
                    }
                };
                try (MAMemoryImpl mem = new MAMemoryImpl()) {
                    mem.of(ff, path.concat("x.dat").$(), ff.getPageSize());

                    for (int i = 0; i < N; i++) {
                        mem.putLong(rnd.nextLong());
                    }

                    try (PagedSlidingReadOnlyMemory mem2 = new PagedSlidingReadOnlyMemory()) {
                        mem2.of(mem);

                        try {
                            rnd.reset();
                            for (int i = 0; i < N; i++) {
                                Assert.assertEquals(rnd.nextLong(), mem2.getLong(i * 8));
                            }
                            Assert.fail();
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getMessage(), "could not mmap");
                        }

                        rnd.reset();
                        for (int i = 0; i < N; i++) {
                            Assert.assertEquals(rnd.nextLong(), mem2.getLong(i * 8));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testWriteAndRead() throws Exception {
        long used = Unsafe.getMemUsed();
        try (Path path = new Path().of(temp.newFile().getAbsolutePath()).$()) {
            try (CMARWMemory mem = new CMARWMemoryImpl(FF, path, 2 * FF.getPageSize(), Long.MAX_VALUE)) {
                for (int i = 0; i < N; i++) {
                    mem.putLong(i);
                }
                // read in place
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8));
                }

                Assert.assertEquals(8L * N, mem.getAppendOffset());
            }
            try (CMARWMemory mem = new CMARWMemoryImpl(FF, path, FF.getPageSize(), Long.MAX_VALUE)) {
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8));
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testWriteAndReadWithReadOnlyMem() throws Exception {
        long used = Unsafe.getMemUsed();
        try (Path path = new Path().of(temp.newFile().getAbsolutePath()).$()) {
            try (CMARWMemory mem = new CMARWMemoryImpl(FF, path, 2 * FF.getPageSize(), Long.MAX_VALUE)) {
                for (int i = 0; i < N; i++) {
                    mem.putLong(i);
                }
                Assert.assertEquals(8L * N, mem.getAppendOffset());
            }
            try (MRMemory mem = new CMRMemoryImpl(FF, path, 8L * N)) {
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
            public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode) {
                if (rnd.nextBoolean()) {
                    return -1;
                }
                return super.mremap(fd, addr, previousSize, newSize, offset, mode);
            }
        }

        final X ff = new X();
        TestUtils.assertMemoryLeak(() -> {
            int writeFailureCount = 0;
            try (Path path = new Path().of(temp.newFile().getAbsolutePath()).$()) {
                try (CMARWMemory mem = new CMARWMemoryImpl(ff, path, ff.getPageSize(), Long.MAX_VALUE)) {
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
            try (Path path = new Path().of(temp.newFile().getAbsolutePath()).$()) {
                try (PagedVirtualMemory mem = factory.newInstance(path)) {
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

                try (MRMemory roMem = new CMRMemoryImpl(FF, path, 800)) {
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
        PagedVirtualMemory newInstance(Path path);
    }
}