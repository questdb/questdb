/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo.vm;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class ContinuousOffsetMappedMemoryTest {
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    private static String root;
    private final FilesFacade ff = FilesFacadeImpl.INSTANCE;
    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void setUpClass() throws Exception {
        root = temp.newFolder("dbRoot").getAbsolutePath();
    }

    @Test
    public void testExtendAfterZeroSizeOpen() throws Exception {
        try (Path path = new Path().of(root)) {
            path.concat(testName.getMethodName());

            long appendCount = 3 * Files.PAGE_SIZE / 8L;
            createFile(ff, path, appendCount, false);

            TestUtils.assertMemoryLeak(() -> {
                try (MemoryCMORImpl memoryROffset = new MemoryCMORImpl()) {
                    memoryROffset.ofOffset(ff, path, Files.PAGE_SIZE, Files.PAGE_SIZE, MemoryTag.NATIVE_DEFAULT);
                    memoryROffset.extend(Files.PAGE_SIZE);
                    Assert.assertEquals(memoryROffset.size(), Files.PAGE_SIZE);
                    Assert.assertEquals(memoryROffset.getOffset(), 0);
                }
            });
        }
    }

    @Test
    public void testMappingFails() throws Exception {
        try (Path path = new Path().of(root)) {
            path.concat(testName.getMethodName());

            long appendCount = 5 * Files.PAGE_SIZE / 8L - 1;
            createFile(ff, path, appendCount, false);

            TestUtils.assertMemoryLeak(() -> {
                try (
                        MemoryCMORImpl memoryROffset = new MemoryCMORImpl()
                ) {
                    FilesFacade ff = new FilesFacadeImpl() {
                        @Override
                        public long length(long fd) {
                            return -1;
                        }

                        @Override
                        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                            return -1;
                        }
                    };

                    // Fail to get file size
                    try {
                        memoryROffset.of(ff, path, Files.PAGE_SIZE, -1L, MemoryTag.NATIVE_DEFAULT);
                        Assert.fail();
                    } catch (CairoException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "could not get length");
                    }
                    Assert.assertEquals(-1, memoryROffset.getFd());

                    // Fail to map
                    try {
                        memoryROffset.of(ff, path, Files.PAGE_SIZE, 1234, MemoryTag.NATIVE_DEFAULT);
                        Assert.fail();
                    } catch (CairoException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "could not mmap");
                    }
                    Assert.assertEquals(-1, memoryROffset.getFd());

                    // Failed to remap
                    ff = new FilesFacadeImpl() {
                        @Override
                        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
                            return -1;
                        }
                    };

                    memoryROffset.ofOffset(ff, path, Files.PAGE_SIZE - 10, 2 * Files.PAGE_SIZE + 10, MemoryTag.NATIVE_DEFAULT);
                    try {
                        memoryROffset.growToFileSize();
                        Assert.fail();
                    } catch (CairoException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "could not remap file");
                    }
                    Assert.assertEquals(-1, memoryROffset.getFd());

                    // Cannot get length to grow to file size
                    ff = new FilesFacadeImpl() {
                        @Override
                        public long length(long fd) {
                            return -1;
                        }
                    };
                    memoryROffset.of(ff, path, Files.PAGE_SIZE, 1234, MemoryTag.NATIVE_DEFAULT);
                    try {
                        memoryROffset.growToFileSize();
                        Assert.fail();
                    } catch (CairoException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "could not get length");
                    }
                }
            });
        }
    }

    @Test
    public void testOffsetMappedFileExtend() throws Exception {
        try (Path path = new Path().of(root)) {
            path.concat(testName.getMethodName());

            long appendCount = 5 * Files.PAGE_SIZE / 8L - 1;
            createFile(ff, path, appendCount, true);

            TestUtils.assertMemoryLeak(() -> {
                try (
                        MemoryCMRImpl memoryR = new MemoryCMRImpl();
                        MemoryCMORImpl memoryROffset = new MemoryCMORImpl()
                ) {
                    memoryR.of(ff, path, Files.PAGE_SIZE, -1L, MemoryTag.NATIVE_DEFAULT);
                    long fileSize = memoryR.size();


                    Rnd rnd = new Rnd();
                    for (int i = 0; i < 10; i++) {
                        long lo = rnd.nextLong(appendCount / 2) * 8L;

                        memoryROffset.ofOffset(ff, path, lo, fileSize / 2, MemoryTag.NATIVE_DEFAULT);
                        Assert.assertEquals(fileSize / 2, memoryROffset.size() + memoryROffset.getOffset());

                        memoryROffset.extend(fileSize - lo);
                        Assert.assertEquals(fileSize, memoryROffset.getOffset() + memoryROffset.size());

                        Assert.assertEquals(lo, memoryR.getLong(lo));
                        Assert.assertEquals(lo, memoryROffset.getLong(lo));
                    }
                }
            });
        }
    }

    @Test
    public void testOffsetMappedFileExtendBelowLoIsIgnored() throws Exception {
        try (Path path = new Path().of(root)) {
            path.concat(testName.getMethodName());

            long appendCount = 3 * Files.PAGE_SIZE / 8L;
            createFile(ff, path, appendCount, false);

            TestUtils.assertMemoryLeak(() -> {
                try (MemoryCMORImpl memoryROffset = new MemoryCMORImpl()) {
                    memoryROffset.ofOffset(ff, path, Files.PAGE_SIZE, 2 * Files.PAGE_SIZE, MemoryTag.NATIVE_DEFAULT);
                    memoryROffset.extend(Files.PAGE_SIZE / 2);
                    Assert.assertEquals(Files.PAGE_SIZE, memoryROffset.size());

                    memoryROffset.growToFileSize();
                    Assert.assertEquals(2 * Files.PAGE_SIZE, memoryROffset.size());
                    Assert.assertEquals(memoryROffset.size(), 2 * Files.PAGE_SIZE);
                    Assert.assertEquals(memoryROffset.getOffset() + memoryROffset.size(), 3 * Files.PAGE_SIZE);
                }
            });
        }
    }

    @Test
    public void testOffsetMappings() throws Exception {
        try (Path path = new Path().of(root)) {
            path.concat(testName.getMethodName());

            long appendCount = 3 * Files.PAGE_SIZE / 8L;
            createFile(ff, path, appendCount, true);

            TestUtils.assertMemoryLeak(() -> {
                try (
                        MemoryCMRImpl memoryR = new MemoryCMRImpl();
                        MemoryCMORImpl memoryROffset = new MemoryCMORImpl()
                ) {
                    memoryR.of(ff, path, Files.PAGE_SIZE, -1L, MemoryTag.NATIVE_DEFAULT);

                    memoryROffset.of(ff, path, Files.PAGE_SIZE, -1L, MemoryTag.NATIVE_DEFAULT);
                    Assert.assertEquals(memoryR.size(), memoryROffset.size());

                    for (long pos = 8L; pos < appendCount; pos += 8L) {
                        Assert.assertEquals(pos, memoryR.getLong(pos));
                        Assert.assertEquals(pos, memoryROffset.getLong(pos));
                    }

                    Rnd rnd = new Rnd();
                    for (int i = 0; i < 10; i++) {
                        long lo = rnd.nextLong(appendCount) * 8L;
                        memoryROffset.ofOffset(ff, path, lo, memoryR.size(), MemoryTag.NATIVE_DEFAULT);
                        Assert.assertEquals(memoryR.size(), memoryROffset.size() + memoryROffset.getOffset());

                        Assert.assertEquals(lo, memoryR.getLong(lo));
                        Assert.assertEquals(lo, memoryROffset.getLong(lo));
                    }
                }
            });
        }
    }

    private void createFile(FilesFacade ff, Path path, long appendCount, boolean writeData) {
        if (!ff.touch(path.$())) {
            Assert.fail("Failed to create file " + ff.errno());
        } else {
            System.out.println("Created file " + path.$());
        }
        long fd = ff.openRW(path, CairoConfiguration.O_NONE);
        Assert.assertTrue(fd > 0);

        try (MemoryMARW memoryW = Vm.getMARWInstance()) {
            memoryW.of(ff, fd, testName.getMethodName(), 16, 0);

            if (writeData) {
                memoryW.jumpTo(0);


                for (long i = 0; i < appendCount; i++) {
                    memoryW.putLong(i * 8L);
                }
            } else {
                memoryW.jumpTo(appendCount * 8L);
            }
        }
    }
}
