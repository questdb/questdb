/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.*;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.Path;
import com.questdb.test.tools.TestUtils;
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
            public long mmap(long fd, long len, long offset, int mode) {
                if (force || rnd.nextBoolean()) {
                    force = false;
                    return super.mmap(fd, len, offset, mode);
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
            try (AppendMemory mem = new AppendMemory()) {
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
    public void testAppendAndCannotMap() throws Exception {
        long used = Unsafe.getMemUsed();

        Rnd rnd = new Rnd();
        class X extends FilesFacadeImpl {
            @Override
            public long mmap(long fd, long len, long offset, int mode) {
                if (rnd.nextBoolean()) {
                    return -1;
                }
                return super.mmap(fd, len, offset, mode);
            }
        }

        X ff = new X();

        try (Path path = new Path().of(temp.newFile().getAbsolutePath()).$()) {
            try (AppendMemory mem = new AppendMemory(FF, path, 2 * FF.getPageSize())) {
                for (int i = 0; i < N; i++) {
                    mem.putLong(i);
                }
                Assert.assertEquals(8L * N, mem.getAppendOffset());
            }

            int failureCount = 0;
            try (ReadOnlyMemory mem = new ReadOnlyMemory()) {
                mem.of(ff, path, ff.getPageSize(), 8L * N);
                int i = 0;
                while (i < N) {
                    try {
                        Assert.assertEquals(i, mem.getLong(i * 8));
                        i++;
                    } catch (CairoException ignore) {
                        failureCount++;
                    }
                }
                Assert.assertTrue(failureCount > 0);
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testAppendAndCannotRead() throws Exception {
        long used = Unsafe.getMemUsed();

        class X extends FilesFacadeImpl {
            int count = 2;

            @Override
            public long openRO(LPSZ name) {
                return --count > 0 ? -1 : super.openRO(name);
            }
        }

        X ff = new X();

        try (Path path = new Path().of(temp.newFile().getAbsolutePath()).$()) {
            try (AppendMemory mem = new AppendMemory(FF, path, 2 * FF.getPageSize())) {
                for (int i = 0; i < N; i++) {
                    mem.putLong(i);
                }
                Assert.assertEquals(8L * N, mem.getAppendOffset());
            }

            try (ReadOnlyMemory mem = new ReadOnlyMemory()) {

                // open non-existing
                try {
                    mem.of(ff, path, ff.getPageSize(), 8L * N);
                    Assert.fail();
                } catch (CairoException ignore) {
                }

                mem.of(ff, path, ff.getPageSize(), 8L * N);

                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8));
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testAppendAndReadWithReadOnlyMem() throws Exception {
        long used = Unsafe.getMemUsed();
        try (Path path = new Path().of(temp.newFile().getAbsolutePath()).$()) {
            try (AppendMemory mem = new AppendMemory(FF, path, 2 * FF.getPageSize())) {
                for (int i = 0; i < N; i++) {
                    mem.putLong(i);
                }
                Assert.assertEquals(8L * N, mem.getAppendOffset());
            }

            try (ReadOnlyMemory mem = new ReadOnlyMemory()) {

                // open non-existing
                try {
                    mem.of(FF, null, FF.getPageSize(), 8L * N);
                    Assert.fail();
                } catch (CairoException ignore) {
                }

                mem.of(FF, path, FF.getPageSize(), 8L * N);

                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8));
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
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
            try (AppendMemory mem = new AppendMemory()) {
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
        testVirtualMemoryJump(path -> new AppendMemory(FF, path, FF.getPageSize()));
    }

    @Test
    public void testAppendMemoryReuse() throws Exception {
        long used = Unsafe.getMemUsed();
        try (AppendMemory mem = new AppendMemory()) {
            for (int j = 0; j < 10; j++) {
                try (Path path = new Path().of(temp.newFile().getAbsolutePath()).$()) {
                    mem.of(FF, path, 2 * FF.getPageSize());
                    for (int i = 0; i < N; i++) {
                        mem.putLong(i);
                    }
                    Assert.assertEquals(8L * N, mem.getAppendOffset());

                    try (ReadOnlyMemory ro = new ReadOnlyMemory(FF, path, FF.getPageSize(), 8L * N)) {
                        for (int i = 0; i < N; i++) {
                            Assert.assertEquals(i, ro.getLong(i * 8));
                        }
                    }
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testAppendTruncateError() throws Exception {
        long used = Unsafe.getMemUsed();

        class X extends FilesFacadeImpl {
            int count = 2;
            boolean allClear = false;

            @Override
            public boolean truncate(long fd, long size) {
                if (allClear || --count > 0) {
                    return super.truncate(fd, size);
                }
                allClear = true;
                return false;
            }
        }

        X ff = new X();

        long openFileCount = ff.getOpenFileCount();
        try (Path path = new Path().of(temp.newFile().getAbsolutePath()).$()) {
            try (AppendMemory mem = new AppendMemory(ff, path, 2 * ff.getPageSize())) {
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
    public void testReadOnlyMemoryJump() {
        try (ReadOnlyMemory mem = new ReadOnlyMemory()) {
            try {
                mem.jumpTo(100);
                Assert.fail();
            } catch (UnsupportedOperationException e) {
                Assert.assertTrue(Chars.contains(e.getMessage(), "Use grow"));
            }
        }
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
            try (ReadWriteMemory mem = new ReadWriteMemory()) {
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
    public void testReadWriteMemoryJump() throws Exception {
        testVirtualMemoryJump(path -> new ReadWriteMemory(FF, path, FF.getPageSize()));
    }

    @Test
    public void testSlidingWindowMemory() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                path.of(temp.getRoot().getAbsolutePath());
                final int N = 100000;
                final Rnd rnd = new Rnd();
                try (AppendMemory mem = new AppendMemory()) {
                    mem.of(FF, path.concat("x.dat").$(), FF.getPageSize());


                    for (int i = 0; i < N; i++) {
                        mem.putLong(rnd.nextLong());
                    }

                    try (SlidingWindowMemory mem2 = new SlidingWindowMemory()) {
                        mem2.of(mem);

                        // try to read outside of original page bounds
                        try {
                            mem2.getLong(N * 16);
                            Assert.fail();
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getMessage(), "Trying to map read-only page outside");
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
                    public long mmap(long fd, long len, long offset, int mode) {
                        if (mode == Files.MAP_RO && --counter == 0) {
                            return -1;
                        }
                        return super.mmap(fd, len, offset, mode);
                    }
                };
                try (AppendMemory mem = new AppendMemory()) {
                    mem.of(ff, path.concat("x.dat").$(), ff.getPageSize());

                    for (int i = 0; i < N; i++) {
                        mem.putLong(rnd.nextLong());
                    }

                    try (SlidingWindowMemory mem2 = new SlidingWindowMemory()) {
                        mem2.of(mem);

                        try {
                            rnd.reset();
                            for (int i = 0; i < N; i++) {
                                Assert.assertEquals(rnd.nextLong(), mem2.getLong(i * 8));
                            }
                            Assert.fail();
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getMessage(), "Cannot map read-only page");
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
    public void testWindowsTruncateRaceCondition() throws Exception {
        TestUtils.assertMemoryLeak(new TestUtils.LeakProneCode() {
            @Override
            public void run() throws Exception {
                try (Path path = new Path().of(temp.newFile().getAbsolutePath()).$()) {
                    try (AppendMemory mem = new AppendMemory(FF, path, FF.getMapPageSize())) {
                        mem.putLong(1);
                        mem.putDouble(0.123456);

                        final long size = mem.getAppendOffset();

                        TestFilesFacade ff = new TestFilesFacade() {

                            int errno = 0;
                            boolean wasCalled = false;

                            @Override
                            public int errno() {
                                return errno;
                            }

                            @Override
                            public boolean wasCalled() {
                                return wasCalled;
                            }

                            @Override
                            public long mmap(long fd, long len, long offset, int mode) {
                                if (len > size) {
                                    errno = 8;
                                    wasCalled = true;
                                    return -1L;
                                }
                                return super.mmap(fd, len, offset, mode);
                            }

                            @Override
                            public boolean isRestrictedFileSystem() {
                                return true;
                            }


                        };

                        try (ReadOnlyMemory roMem = new ReadOnlyMemory(ff, path, ff.getMapPageSize(), size)) {
                            Assert.assertEquals(1, roMem.getLong(0));
                            Assert.assertEquals(0.123456, roMem.getDouble(8), 0.000001d);
                        }
                        Assert.assertTrue(ff.wasCalled());
                    }
                }
            }
        });
    }

    @Test
    public void testWriteAndRead() throws Exception {
        long used = Unsafe.getMemUsed();
        try (Path path = new Path().of(temp.newFile().getAbsolutePath()).$()) {
            try (ReadWriteMemory mem = new ReadWriteMemory(FF, path, 2 * FF.getPageSize())) {
                for (int i = 0; i < N; i++) {
                    mem.putLong(i);
                }
                // read in place
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8));
                }

                Assert.assertEquals(8L * N, mem.getAppendOffset());
            }
            try (ReadWriteMemory mem = new ReadWriteMemory(FF, path, FF.getPageSize())) {
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
            try (ReadWriteMemory mem = new ReadWriteMemory(FF, path, 2 * FF.getPageSize())) {
                for (int i = 0; i < N; i++) {
                    mem.putLong(i);
                }
                Assert.assertEquals(8L * N, mem.getAppendOffset());
            }
            try (ReadOnlyMemory mem = new ReadOnlyMemory(FF, path, FF.getPageSize(), 8L * N)) {
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8));
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testWriteOverMapFailuresAndRead() throws Exception {
        long used = Unsafe.getMemUsed();
        Rnd rnd = new Rnd();
        class X extends FilesFacadeImpl {
            @Override
            public long mmap(long fd, long len, long offset, int mode) {
                if (rnd.nextBoolean()) {
                    return -1;
                }
                return super.mmap(fd, len, offset, mode);
            }
        }

        int writeFailureCount = 0;

        final X ff = new X();

        try (Path path = new Path().of(temp.newFile().getAbsolutePath()).$()) {
            try (ReadWriteMemory mem = new ReadWriteMemory(ff, path, 2 * ff.getPageSize())) {
                int i = 0;
                while (i < N) {
                    try {
                        mem.putLong(i);
                        i++;
                    } catch (CairoException ignore) {
                        writeFailureCount++;
                    }
                }
                // read in place
                for (i = 0; i < N; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8));
                }

                Assert.assertEquals(8L * N, mem.getAppendOffset());
            }
        }
        Assert.assertTrue(writeFailureCount > 0);
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    private void testVirtualMemoryJump(VirtualMemoryFactory factory) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(temp.newFile().getAbsolutePath()).$()) {
                try (VirtualMemory mem = factory.newInstance(path)) {
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

                try (ReadOnlyMemory roMem = new ReadOnlyMemory(FF, path, FF.getPageSize(), 800)) {
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
        VirtualMemory newInstance(Path path);
    }
}