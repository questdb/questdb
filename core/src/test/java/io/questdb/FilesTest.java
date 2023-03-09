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

package io.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableUtils;
import io.questdb.log.LogError;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

public class FilesTest {
    private static final String EOL = System.lineSeparator();
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testAllocate() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            TestUtils.writeStringToFile(temp, "abcde");
            try (Path path = new Path().of(temp.getAbsolutePath()).$()) {
                Assert.assertTrue(Files.exists(path));
                Assert.assertEquals(5, Files.length(path));

                int fd = Files.openRW(path);
                try {
                    Files.allocate(fd, 10);
                    Assert.assertEquals(10, Files.length(path));
                    Files.allocate(fd, 120);
                    Assert.assertEquals(120, Files.length(path));
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testAllocateConcurrent() throws IOException, InterruptedException {
        // This test allocates (but doesn't write to) potentially very large files
        // size of which will depend on free disk space of the host OS.
        // I found that on OSX (m1) with large disk allocate() call is painfully slow and
        // for that reason (until we understood the problem better) we won't run this test
        // on OSX
        Assume.assumeTrue(Os.type != Os.OSX_ARM64 && Os.type != Os.OSX_AMD64);
        FilesFacade ff = TestFilesFacadeImpl.INSTANCE;

        String tmpFolder = temporaryFolder.newFolder("allocate").getAbsolutePath();
        AtomicInteger errors = new AtomicInteger();

        for (int i = 0; i < 10; i++) {
            Thread th1 = new Thread(() -> testAllocateConcurrent0(ff, tmpFolder, 1, errors));
            Thread th2 = new Thread(() -> testAllocateConcurrent0(ff, tmpFolder, 2, errors));
            th1.start();
            th2.start();
            th1.join();
            th2.join();
            Assert.assertEquals(0, errors.get());
        }
    }

    @Test
    public void testAllocateLoop() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            TestUtils.writeStringToFile(temp, "abcde");
            try (Path path = new Path().of(temp.getAbsolutePath()).$()) {
                Assert.assertTrue(Files.exists(path));
                Assert.assertEquals(5, Files.length(path));
                int fd = Files.openRW(path);

                long M50 = 100 * 1024L * 1024L;
                try {
                    // If allocate tries to allocate by the given size
                    // instead of to the size this will allocate 2TB and suppose to fail
                    for (int i = 0; i < 20000; i++) {
                        Files.allocate(fd, M50 + i);
                        Assert.assertEquals(M50 + i, Files.length(path));
                    }
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testCopy() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            TestUtils.writeStringToFile(temp, "abcde");
            try (Path path = new Path().of(temp.getAbsolutePath()).$()) {
                Assert.assertTrue(Files.exists(path));
                try (Path copyPath = new Path().of(temp.getAbsolutePath()).put("-copy").$()) {
                    Files.copy(path, copyPath);

                    Assert.assertEquals(5, Files.length(copyPath));
                    TestUtils.assertFileContentsEquals(path, copyPath);
                }
            }
        });
    }

    @Test
    public void testCopyBigFile() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            int fileSize = 2 * 1024 * 1024; // in MB
            byte[] page = new byte[1024 * 64];
            Rnd rnd = new Rnd();

            for (int i = 0; i < page.length; i++) {
                page[i] = rnd.nextByte();
            }

            try (FileOutputStream fos = new FileOutputStream(temp)) {
                for (int i = 0; i < fileSize / page.length; i++) {
                    fos.write(page);
                }
            }

            try (Path path = new Path().of(temp.getAbsolutePath()).$()) {
                Assert.assertTrue(Files.exists(path));
                try (Path copyPath = new Path().of(temp.getAbsolutePath()).put("-copy").$()) {
                    Files.copy(path, copyPath);

                    Assert.assertEquals(fileSize, Files.length(copyPath));
                    TestUtils.assertFileContentsEquals(path, copyPath);
                }
            }
        });
    }

    @Test
    public void testCopyRecursive() throws Exception {
        assertMemoryLeak(() -> {
            int mkdirMode = 509;
            try (Path src = new Path().of(temporaryFolder.getRoot().getAbsolutePath())) {
                File f1 = new File(Chars.toString(src.concat("file")));
                TestUtils.writeStringToFile(f1, "abcde");

                src.parent();
                src.concat("subdir");
                Assert.assertEquals(0, Files.mkdir(src.$(), mkdirMode));

                File f2 = new File(Chars.toString(src.concat("file2")));
                TestUtils.writeStringToFile(f2, "efgh");

                src.of(temporaryFolder.getRoot().getAbsolutePath());
                try (
                        Path dst = new Path().of(temporaryFolder.getRoot().getPath()).put("copy").$();
                        Path p2 = new Path().of(dst).slash$()
                ) {
                    try {
                        Assert.assertEquals(0, TestFilesFacadeImpl.INSTANCE.copyRecursive(src, dst, mkdirMode));
                        dst.concat("file");
                        src.concat("file");
                        TestUtils.assertFileContentsEquals(src, dst);
                        dst.parent();
                        src.parent();

                        src.concat("subdir").concat("file2");
                        dst.concat("subdir").concat("file2");
                        TestUtils.assertFileContentsEquals(src, dst);
                    } finally {
                        Files.rmdir(p2);
                    }
                }
            }
        });
    }

    @Test
    public void testDeleteDir2() throws Exception {
        assertMemoryLeak(() -> {
            File r = temporaryFolder.newFolder("to_delete");
            Assert.assertTrue(new File(r, "a/b/c").mkdirs());
            Assert.assertTrue(new File(r, "d/e/f").mkdirs());
            touch(new File(r, "d/1.txt"));
            touch(new File(r, "a/b/2.txt"));
            try (Path path = new Path().of(r.getAbsolutePath()).$()) {
                Assert.assertEquals(0, Files.rmdir(path));
                Assert.assertFalse(r.exists());
            }
        });
    }

    @Test
    public void testDeleteOpenFile() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                File f = temporaryFolder.newFile();
                int fd = Files.openRW(path.of(f.getAbsolutePath()).$());
                Assert.assertTrue(Files.exists(fd));
                Assert.assertTrue(Files.remove(path));
                Assert.assertFalse(Files.exists(fd));
                Files.close(fd);
            }
        });
    }

    @Test
    public void testFailsToAllocateWhenNotEnoughSpace() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            TestUtils.writeStringToFile(temp, "abcde");
            try (Path path = new Path().of(temp.getAbsolutePath()).$()) {
                Assert.assertTrue(Files.exists(path));
                int fd = Files.openRW(path);
                Assert.assertEquals(5, Files.length(path));

                try {
                    long tb10 = 1024L * 1024L * 1024L * 1024L * 10; // 10TB
                    boolean success = Files.allocate(fd, tb10);
                    Assert.assertFalse("Allocation should fail on reasonable hard disk size", success);
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testHardLinkAsciiName() throws Exception {
        assertHardLinkPreservesFileContent("some_column.d");
    }

    @Test
    public void testHardLinkFailuresSrcDoesNotExist() throws Exception {
        assertMemoryLeak(() -> {
            File dbRoot = temporaryFolder.newFolder("dbRoot");
            Path srcFilePath = null;
            Path hardLinkFilePath = null;
            try {
                srcFilePath = new Path().of(dbRoot.getAbsolutePath()).concat("some_column.d").$();
                hardLinkFilePath = new Path().of(srcFilePath).put(".1").$();
                Assert.assertEquals(-1, Files.hardLink(srcFilePath, hardLinkFilePath));
            } finally {
                Misc.free(srcFilePath);
                Misc.free(hardLinkFilePath);
            }
        });
    }

    @Test
    public void testHardLinkNonAsciiName() throws Exception {
        assertHardLinkPreservesFileContent("いくつかの列.d");
    }

    @Test
    public void testHardLinkRecursive() throws Exception {
        assertMemoryLeak(() -> {
            int mkdirMode = 509;
            try (Path src = new Path().of(temporaryFolder.getRoot().getAbsolutePath())) {
                File f1 = new File(Chars.toString(src.concat("file")));
                TestUtils.writeStringToFile(f1, "abcde");

                src.parent();
                src.concat("subdir");
                Assert.assertEquals(0, Files.mkdir(src.$(), mkdirMode));

                File f2 = new File(Chars.toString(src.concat("file2")));
                TestUtils.writeStringToFile(f2, "efgh");

                src.of(temporaryFolder.getRoot().getAbsolutePath());
                try (
                        Path dst = new Path().of(temporaryFolder.getRoot().getPath()).put("copy").$();
                        Path p2 = new Path().of(dst).slash$()
                ) {
                    try {
                        Assert.assertEquals(0, TestFilesFacadeImpl.INSTANCE.hardLinkDirRecursive(src, dst, mkdirMode));

                        dst.concat("file");
                        src.concat("file");
                        TestUtils.assertFileContentsEquals(src, dst);
                        dst.parent();
                        src.parent();

                        src.concat("subdir").concat("file2");
                        dst.concat("subdir").concat("file2");
                        TestUtils.assertFileContentsEquals(src, dst);
                    } finally {
                        Files.rmdir(p2);
                    }
                }
            }
        });
    }

    @Test
    public void testLastModified() throws Exception {
        LogFactory.getLog(FilesTest.class); // so that it is not accounted in assertMemoryLeak
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                assertLastModified(path, DateFormatUtils.parseUTCDate("2015-10-17T10:00:00.000Z"));
                assertLastModified(path, 122222212222L);
            }
        });
    }

    @Test
    public void testListDir() throws Exception {
        assertMemoryLeak(() -> {
            String temp = temporaryFolder.getRoot().getAbsolutePath();
            ObjList<String> names = new ObjList<>();
            try (Path path = new Path().of(temp).$()) {
                try (Path cp = new Path()) {
                    Assert.assertTrue(Files.touch(cp.of(temp).concat("a.txt").$()));
                    StringSink nameSink = new StringSink();
                    long pFind = Files.findFirst(path);
                    Assert.assertTrue(pFind != 0);
                    try {
                        do {
                            nameSink.clear();
                            Chars.utf8DecodeZ(Files.findName(pFind), nameSink);
                            names.add(nameSink.toString());
                        } while (Files.findNext(pFind) > 0);
                    } finally {
                        Files.findClose(pFind);
                    }
                }
            }

            names.sort(Chars::compare);

            Assert.assertEquals("[.,..,a.txt]", names.toString());
        });
    }

    @Test
    public void testListNonExistingDir() throws Exception {
        assertMemoryLeak(() -> {
            String temp = temporaryFolder.getRoot().getAbsolutePath();
            try (Path path = new Path().of(temp).concat("xyz").$()) {
                long pFind = Files.findFirst(path);
                Assert.assertEquals("failed os=" + Os.errno(), 0, pFind);
            }
        });
    }

    @Test
    public void testMkdirs() throws Exception {
        assertMemoryLeak(() -> {
            File r = temporaryFolder.newFolder("to_delete");
            try (Path path = new Path().of(r.getAbsolutePath())) {
                path.concat("a").concat("b").concat("c").concat("f.text").$();
                Assert.assertEquals(0, Files.mkdirs(path, 509));
            }

            try (Path path = new Path().of(r.getAbsolutePath())) {
                path.concat("a").concat("b").concat("c").$();
                Assert.assertTrue(Files.exists(path));
            }
        });
    }

    @Test
    public void testOpenCleanRWAllocatesToSize() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.getRoot();
            try (Path path = new Path().of(temp.getAbsolutePath()).concat("openCleanRWParallel").$()) {
                int fd = Files.openCleanRW(path, 1024);
                Assert.assertTrue(Files.exists(path));
                Assert.assertEquals(1024, Files.length(path));

                int fd2 = Files.openCleanRW(path, 2048);
                Assert.assertEquals(2048, Files.length(path));

                Files.close(fd);
                Files.close(fd2);
            }
        });
    }

    @Test
    public void testOpenCleanRWFailsWhenCalledOnDir() throws Exception {
        assertMemoryLeak(() -> {
            int fd = -1;
            try (Path path = new Path()) {
                fd = Files.openCleanRW(path.of(temporaryFolder.getRoot().getAbsolutePath()).$(), 32);
                Assert.assertTrue(fd < 0);
            } finally {
                Files.close(fd);
            }
        });
    }

    @Test
    public void testOpenCleanRWParallel() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.getRoot();
            int threadCount = 15;
            int iteration = 10;
            int fileSize = 1024;

            try (Path path = new Path().of(temp.getAbsolutePath()).concat("openCleanRWParallel").$()) {
                Assert.assertFalse(Files.exists(path));

                final CyclicBarrier barrier = new CyclicBarrier(threadCount);
                final CountDownLatch halt = new CountDownLatch(threadCount);
                final AtomicInteger errors = new AtomicInteger();

                for (int k = 0; k < threadCount; k++) {
                    new Thread(() -> {
                        try {
                            barrier.await();
                            for (int i = 0; i < iteration; i++) {
                                int fd = Files.openCleanRW(path, fileSize);
                                if (fd < 0) {
                                    errors.incrementAndGet();
                                }
                                int error = Files.close(fd);
                                if (error != 0) {
                                    errors.incrementAndGet();
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            errors.incrementAndGet();
                        } finally {
                            halt.countDown();
                        }
                    }).start();
                }

                halt.await();
                Assert.assertEquals(0, errors.get());
                Assert.assertTrue(Files.exists(path));
                Assert.assertEquals(fileSize, Files.length(path));
            }
        });
    }

    @Test
    public void testOpenRWFailsWhenCalledOnDir() throws Exception {
        assertMemoryLeak(() -> {
            int fd = -1;
            try (Path path = new Path()) {
                fd = Files.openRW(path.of(temporaryFolder.getRoot().getAbsolutePath()).$());
                Assert.assertTrue(fd < 0);
            } finally {
                Files.close(fd);
            }
        });
    }

    @Test
    public void testReadFails() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();

            try (Path path = new Path().of(temp.getAbsolutePath())) {
                int fd1 = Files.openRW(path.$());
                long fileSize = 4096;
                long mem = Unsafe.malloc(fileSize, MemoryTag.NATIVE_DEFAULT);

                long testValue = 0x1234567890ABCDEFL;
                Unsafe.getUnsafe().putLong(mem, testValue);

                try {
                    Files.truncate(fd1, fileSize);

                    MatcherAssert.assertThat(Files.read(fd1, mem, 8L, 0), is(8L));
                    MatcherAssert.assertThat(Files.read(fd1, mem, fileSize, -1), lessThan(0L));
                    MatcherAssert.assertThat(Files.read(fd1, mem, fileSize, fileSize), is(0L));
                    MatcherAssert.assertThat(Files.read(fd1, mem, fileSize + 8, fileSize), is(0L));

                } finally {
                    // Release mem, fd
                    Files.close(fd1);
                    Unsafe.free(mem, fileSize, MemoryTag.NATIVE_DEFAULT);

                    // Delete files
                    Files.remove(path);
                }
            }
        });
    }

    @Test
    public void testReadOver2GB() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();

            try (Path path = new Path().of(temp.getAbsolutePath())) {
                int fd1 = Files.openRW(path.$());
                long size2Gb = (2L << 30) + 4096;
                long mem = Unsafe.malloc(size2Gb, MemoryTag.NATIVE_DEFAULT);

                long testValue = 0x1234567890ABCDEFL;
                Unsafe.getUnsafe().putLong(mem, testValue);

                try {
                    Files.truncate(fd1, size2Gb);

                    Files.write(fd1, mem, 8, 0);
                    Files.write(fd1, mem, 8, size2Gb - 8);

                    // Check read call works
                    // Check written data
                    Assert.assertEquals(size2Gb, Files.read(fd1, mem, size2Gb, 0));
                    byte byte1 = Files.readNonNegativeByte(fd1, 0L);
                    short short1 = Files.readNonNegativeShort(fd1, 0L);
                    int int1 = Files.readNonNegativeInt(fd1, 0L);
                    long long1 = Files.readNonNegativeLong(fd1, 0L);
                    Assert.assertEquals((byte) testValue, byte1);
                    Assert.assertEquals((short) testValue, short1);
                    Assert.assertEquals((int) testValue, int1);
                    Assert.assertEquals(testValue, long1);

                    byte byte2 = Files.readNonNegativeByte(fd1, size2Gb - 8);
                    short short2 = Files.readNonNegativeShort(fd1, size2Gb - 8);
                    int int2 = Files.readNonNegativeInt(fd1, size2Gb - 8);
                    long long2 = Files.readNonNegativeLong(fd1, size2Gb - 8);
                    Assert.assertEquals((byte) testValue, byte2);
                    Assert.assertEquals((short) testValue, short2);
                    Assert.assertEquals((int) testValue, int2);
                    Assert.assertEquals(testValue, long2);

                } finally {
                    // Release mem, fd
                    Files.close(fd1);
                    Unsafe.free(mem, size2Gb, MemoryTag.NATIVE_DEFAULT);

                    // Delete files
                    Files.remove(path);
                }
            }
        });
    }

    @Test
    public void testRemove() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(temporaryFolder.newFile().getAbsolutePath()).$()) {
                Assert.assertTrue(Files.touch(path));
                Assert.assertTrue(Files.exists(path));
                Assert.assertTrue(Files.remove(path));
                Assert.assertFalse(Files.exists(path));
            }
        });
    }

    @Test
    public void testSendFileOver2GB() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();

            try (
                    Path path1 = new Path().of(temp.getAbsolutePath());
                    Path path2 = new Path().of(temp.getAbsolutePath())
            ) {
                int fd1 = Files.openRW(path1.$());
                path2.put(".2").$();
                int fd2 = 0;

                long mem = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);

                long testValue = 0x1234567890ABCDEFL;
                Unsafe.getUnsafe().putLong(mem, testValue);
                long size2Gb = (2L << 30) + 4096;

                try {
                    Files.truncate(fd1, size2Gb);

                    Files.write(fd1, mem, 8, 0);
                    Files.write(fd1, mem, 8, size2Gb - 8);

                    // Check copy call works
                    int result = Files.copy(path1, path2);

                    // Check written data
                    // Windows return 1 but Linux and others return 0 on success
                    // All return negative in case of error.
                    MatcherAssert.assertThat(result, greaterThanOrEqualTo(0));

                    fd2 = Files.openRO(path2.$());
                    long long1 = Files.readNonNegativeLong(fd2, 0L);
                    Assert.assertEquals(testValue, long1);
                    long long2 = Files.readNonNegativeLong(fd2, size2Gb - 8);
                    Assert.assertEquals(testValue, long2);

                } finally {
                    // Release mem, fd
                    Files.close(fd1);
                    if (fd2 != 0) {
                        Files.close(fd2);
                    }
                    Unsafe.free(mem, 8, MemoryTag.NATIVE_DEFAULT);

                    // Delete files
                    Files.remove(path1);
                    Files.remove(path2);
                }
            }
        });
    }

    @Test
    public void testSendFileOver2GBWithOffset() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();

            try (
                    Path path1 = new Path().of(temp.getAbsolutePath());
                    Path path2 = new Path().of(temp.getAbsolutePath())
            ) {
                int fd1 = Files.openRW(path1.$());
                path2.put(".2").$();
                int fd2 = Files.openRW(path2);

                long mem = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);

                long testValue = 0x1234567890ABCDEFL;
                Unsafe.getUnsafe().putLong(mem, testValue);
                long fileSize = (2L << 30) + 2 * 4096;

                try {
                    Files.truncate(fd1, fileSize);

                    Files.write(fd1, mem, 8, 0);
                    Files.write(fd1, mem, 8, fileSize - 8);

                    // Check copy call works
                    int offset = 2058;
                    long copiedLen = Files.copyData(fd1, fd2, offset, -1);

                    MatcherAssert.assertThat("errno: " + Os.errno(), copiedLen, is(fileSize - offset));

                    long long1 = Files.readNonNegativeLong(fd2, fileSize - offset - 8);
                    Assert.assertEquals(testValue, long1);

                    // Copy with set length
                    Files.close(fd2);
                    Files.remove(path2);
                    fd2 = Files.openRW(path2.$());

                    // Check copy call works
                    offset = 3051;
                    copiedLen = Files.copyData(fd1, fd2, offset, fileSize - offset);
                    MatcherAssert.assertThat("errno: " + Os.errno(), copiedLen, is(fileSize - offset));

                    long1 = Files.readNonNegativeLong(fd2, fileSize - offset - 8);
                    Assert.assertEquals(testValue, long1);
                } finally {
                    // Release mem, fd
                    Files.close(fd1);
                    Files.close(fd2);
                    Unsafe.free(mem, 8, MemoryTag.NATIVE_DEFAULT);

                    // Delete files
                    Files.remove(path1);
                    Files.remove(path2);
                }
            }
        });
    }

    @Test
    public void testSoftLinkAsciiName() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertSoftLinkDoesNotPreserveFileContent("some_column.d");
    }

    @Test
    public void testSoftLinkDoesNotFailWhenSrcDoesNotExist() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);

        assertMemoryLeak(() -> {
            File tmpFolder = temporaryFolder.newFolder("soft");
            String fileName = "いくつかの列.d";
            try (
                    Path srcFilePath = new Path().of(tmpFolder.getAbsolutePath()).concat(fileName).$();
                    Path softLinkFilePath = new Path().of(tmpFolder.getAbsolutePath()).concat(fileName).put(".1").$()
            ) {
                Assert.assertEquals(0, Files.softLink(srcFilePath, softLinkFilePath));

                // check that when listing it we can actually find it
                File link = new File(softLinkFilePath.toString());
                File[] fileArray = link.getParentFile().listFiles();
                Assert.assertNotNull(fileArray);
                List<File> files = Arrays.asList(fileArray);
                Assert.assertEquals(fileName + ".1", files.get(0).getName());

                // however
                Assert.assertFalse(link.exists());
                Assert.assertFalse(link.canRead());
                Assert.assertEquals(-1, Files.openRO(softLinkFilePath));
                Assert.assertTrue(Files.remove(softLinkFilePath));
            } finally {
                temporaryFolder.delete();
            }
        });
    }

    @Test
    public void testSoftLinkNonAsciiName() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertSoftLinkDoesNotPreserveFileContent("いくつかの列.d");
    }

    @Test
    public void testTruncate() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            TestUtils.writeStringToFile(temp, "abcde");
            try (Path path = new Path().of(temp.getAbsolutePath()).$()) {
                Assert.assertTrue(Files.exists(path));
                Assert.assertEquals(5, Files.length(path));

                int fd = Files.openRW(path);
                try {
                    Files.truncate(fd, 3);
                    Assert.assertEquals(3, Files.length(path));
                    Files.truncate(fd, 0);
                    Assert.assertEquals(0, Files.length(path));
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testTypeOfDirAndSoftLinkAreTheSame() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            File folder = temporaryFolder.newFolder("folder");
            try (
                    Path path = new Path().of(folder.getAbsolutePath()).$();
                    Path link = new Path().of(folder.getParentFile().toString()).concat("link").$()
            ) {
                Assert.assertTrue(Files.exists(path));
                int pathType = Files.findType(Files.findFirst(path));
                Assert.assertEquals(Files.DT_DIR, pathType);

                Assert.assertEquals(0, Files.softLink(path, link));
                Assert.assertTrue(Files.exists(link));
                int linkType = Files.findType(Files.findFirst(link));
                Assert.assertEquals(Files.DT_DIR, linkType);

                Assert.assertEquals(pathType, linkType);
            }
        });
    }

    @Test
    public void testUnlink() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            File tmpFolder = temporaryFolder.newFolder("unlink");
            final String fileName = "いくつかの列.d";
            final String fileContent = "**unlink** deletes a name from the filesystem." + EOL +
                    "If the name IS a symbolic link, the link is removed." + EOL +
                    "If the name is the last link to the file, and no processes have" + EOL +
                    "the file open, the file is deleted and the space it was using is " + EOL +
                    "made available for reuse. Otherwise, if any process maintains the" + EOL +
                    "file open, it will remain in existence until the last file descriptor" + EOL +
                    "referring to it is closed." + EOL;

            try (
                    Path srcPath = new Path().of(tmpFolder.getAbsolutePath());
                    Path coldRoot = new Path().of(srcPath).concat("S3").slash$(); // does not exist yet
                    Path linkPath = new Path().of(coldRoot).concat(fileName).put(TableUtils.ATTACHABLE_DIR_MARKER).$()
            ) {
                createTempFile(srcPath, fileName, fileContent); // updates srcFilePath

                // create the soft link
                createSoftLink(coldRoot, srcPath, linkPath);

                // check contents are the same
                assertEqualsFileContent(linkPath, fileContent);

                // unlink soft link
                Assert.assertEquals(0, Files.unlink(linkPath));

                // check original file still exists and contents are the same
                assertEqualsFileContent(srcPath, fileContent);

                // however the link no longer exists
                File link = new File(linkPath.toString());
                Assert.assertFalse(link.exists());
                Assert.assertFalse(link.canRead());
                Assert.assertEquals(-1, Files.openRO(linkPath));
            }
        });
    }

    @Test
    public void testWriteFails() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();

            try (Path path = new Path().of(temp.getAbsolutePath()).$()) {
                int fd1 = Files.openRW(path.$());
                long mem = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);

                long testValue = 0x1234567890ABCDEFL;
                Unsafe.getUnsafe().putLong(mem, testValue);
                long fileSize = (2L << 30) + 4096;

                try {
                    Files.truncate(fd1, fileSize);

                    MatcherAssert.assertThat(Files.write(fd1, mem, 8, 0), is(8L));
                    MatcherAssert.assertThat(Files.write(fd1, mem, -1, fileSize), is(-1L));
                    MatcherAssert.assertThat(Files.write(-1, mem, 8, fileSize), is(-1L));

                } finally {
                    // Release mem, fd
                    Files.close(fd1);
                    Unsafe.free(mem, 8, MemoryTag.NATIVE_DEFAULT);

                    // Delete files
                    Files.remove(path);
                }
            }
        });
    }

    @Test
    public void testWriteOver2GB() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();

            try (Path path = new Path().of(temp.getAbsolutePath()).$()) {
                int fd1 = Files.openRW(path.$());
                int fd2 = Files.openRW(path.put(".2").$());
                long mem = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                long mmap = 0;

                long testValue = 0x1234567890ABCDEFL;
                Unsafe.getUnsafe().putLong(mem, testValue);
                long size2Gb = (2L << 30) + 4096;

                try {
                    Files.truncate(fd1, size2Gb);

                    Files.write(fd1, mem, 8, 0);
                    Files.write(fd1, mem, 8, size2Gb - 8);

                    // Check write call works
                    mmap = Files.mmap(fd1, size2Gb, 0, Files.MAP_RO, MemoryTag.NATIVE_DEFAULT);
                    Files.truncate(fd2, size2Gb);

                    // Check written data
                    Assert.assertEquals(size2Gb, Files.write(fd2, mmap, size2Gb, 0));
                    long long1 = Files.readNonNegativeLong(fd2, 0L);
                    Assert.assertEquals(testValue, long1);

                    long long2 = Files.readNonNegativeLong(fd2, size2Gb - 8);
                    Assert.assertEquals(testValue, long2);

                } finally {
                    // Release mem, fd
                    Files.close(fd1);
                    Files.close(fd2);
                    Files.munmap(mmap, size2Gb, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(mem, 8, MemoryTag.NATIVE_DEFAULT);

                    // Delete files
                    Files.remove(path);
                    Files.remove(path.of(temp.getAbsolutePath()).$());
                }
            }
        });
    }

    private static void assertEqualsFileContent(Path path, String fileContent) {
        final int buffSize = 2048;
        final long buffPtr = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);
        int fd = -1;
        try {
            fd = Files.openRO(path);
            Assert.assertTrue(Files.exists(fd));
            long size = Files.length(fd);
            if (size > buffSize) {
                throw new LogError("File is too big: " + path);
            }
            if (size < 0 || size != Files.read(fd, buffPtr, size, 0)) {
                throw new LogError(String.format(
                        "Cannot read %s [errno=%d, size=%d]",
                        path,
                        Os.errno(),
                        size
                ));
            }
            StringSink sink = Misc.getThreadLocalBuilder();
            Chars.utf8Decode(buffPtr, buffPtr + size, sink);
            TestUtils.assertEquals(fileContent, sink.toString());
        } finally {
            Files.close(fd);
            Unsafe.free(buffPtr, buffSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void createSoftLink(Path coldRoot, Path srcFilePath, Path softLinkFilePath) {
        Assert.assertEquals(0, Files.mkdirs(coldRoot, 509));
        Assert.assertEquals(0, Files.softLink(srcFilePath, softLinkFilePath));
        Assert.assertTrue(Os.isWindows() || Files.isSoftLink(softLinkFilePath)); // TODO: isSoftLink is not working on Windows
    }

    private static void createTempFile(Path path, String fileName, String fileContent) {
        final int buffSize = fileContent.length() * 3;
        final long buffPtr = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);
        final byte[] bytes = fileContent.getBytes(Files.UTF_8);
        long p = buffPtr;
        for (int i = 0, n = bytes.length; i < n; i++) {
            Unsafe.getUnsafe().putByte(p++, bytes[i]);
        }
        Unsafe.getUnsafe().putByte(p, (byte) 0);
        int fd = -1;
        try {
            fd = Files.openAppend(path.concat(fileName).$());
            if (fd > -1) {
                Files.truncate(fd, 0);
                Files.append(fd, buffPtr, bytes.length);
                Files.sync();
            }
            Assert.assertTrue(Files.exists(fd));
        } finally {
            Files.close(fd);
            Unsafe.free(buffPtr, buffSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void testAllocateConcurrent0(FilesFacade ff, String pathName, int index, AtomicInteger errors) {
        try (
                Path path = new Path().of(pathName).concat("hello.").put(index).put(".txt").$();
                Path p2 = new Path().of(pathName).$()
        ) {
            int fd = -1;
            long mem = -1;
            long diskSize = ff.getDiskSize(p2);
            Assert.assertNotEquals(-1, diskSize);
            long fileSize = diskSize / 3 * 2;
            try {
                fd = ff.openRW(path, CairoConfiguration.O_NONE);
                assert fd != -1;
                if (ff.allocate(fd, fileSize)) {
                    mem = ff.mmap(fd, fileSize, 0, Files.MAP_RW, 0);
                    Unsafe.getUnsafe().putLong(mem, 123455);
                }
            } finally {
                if (mem != -1) {
                    ff.munmap(mem, fileSize, 0);
                }

                if (fd != -1) {
                    ff.close(fd);
                }

                ff.remove(path);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            errors.incrementAndGet();
        }
    }

    private static void touch(File file) throws IOException {
        FileOutputStream fos = new FileOutputStream(file);
        fos.close();
    }

    private void assertHardLinkPreservesFileContent(String fileName) throws Exception {
        assertMemoryLeak(() -> {
            File dbRoot = temporaryFolder.newFolder("dbRoot");
            final String fileContent = "The theoretical tightest upper bound on the information rate of" + EOL +
                    "data that can be communicated at an arbitrarily low error rate using an average" + EOL +
                    "received signal power S through an analog communication channel subject to" + EOL +
                    "additive white Gaussian noise (AWGN) of power N:" + EOL + EOL +
                    "C = B * log_2(1 + S/N)" + EOL + EOL +
                    "where" + EOL + EOL +
                    "C is the channel capacity in bits per second, a theoretical upper bound on the net " + EOL +
                    "  bit rate (information rate, sometimes denoted I) excluding error-correction codes;" + EOL +
                    "B is the bandwidth of the channel in hertz (passband bandwidth in case of a bandpass " + EOL +
                    "signal);" + EOL +
                    "S is the average received signal power over the bandwidth (in case of a carrier-modulated " + EOL +
                    "passband transmission, often denoted C), measured in watts (or volts squared);" + EOL +
                    "N is the average power of the noise and interference over the bandwidth, measured in " + EOL +
                    "watts (or volts squared); and" + EOL +
                    "S/N is the signal-to-noise ratio (SNR) or the carrier-to-noise ratio (CNR) of the " + EOL +
                    "communication signal to the noise and interference at the receiver (expressed as a linear" + EOL +
                    "power ratio, not aslogarithmic decibels)." + EOL;
            try (
                    Path srcFilePath = new Path().of(dbRoot.getAbsolutePath());
                    Path hardLinkFilePath = new Path().of(dbRoot.getAbsolutePath()).concat(fileName).put(".1").$()
            ) {
                createTempFile(srcFilePath, fileName, fileContent);

                // perform the hard link
                Assert.assertEquals(0, Files.hardLink(srcFilePath, hardLinkFilePath));

                // check content are the same
                assertEqualsFileContent(hardLinkFilePath, fileContent);

                // delete source file
                Assert.assertTrue(Files.remove(srcFilePath));

                // check linked file still exists and content are the same
                assertEqualsFileContent(hardLinkFilePath, fileContent);

                Files.remove(srcFilePath);
                Assert.assertTrue(Files.remove(hardLinkFilePath));
            }
        });
    }

    private void assertLastModified(Path path, long t) throws IOException {
        File f = temporaryFolder.newFile();
        Assert.assertTrue(Files.touch(path.of(f.getAbsolutePath()).$()));
        Assert.assertTrue(Files.setLastModified(path, t));
        Assert.assertEquals(t, Files.getLastModified(path));
    }

    private void assertSoftLinkDoesNotPreserveFileContent(String fileName) throws Exception {
        assertMemoryLeak(() -> {
            File tmpFolder = temporaryFolder.newFolder("soft");
            final String fileContent = "A soft link is automatically interpreted and followed by the OS as " + EOL +
                    "a path to another file, or directory, called the 'target'. The soft link is itself a " + EOL +
                    "file that exists independently of its target. If the soft link is deleted, its target " + EOL +
                    "remains unaffected. If a soft link points to a target, and the target is moved, renamed, " + EOL +
                    "or deleted, the soft link is not automatically updated/deleted and continues to point" + EOL +
                    "to the old target, now a non-existing file, or directory." + EOL +
                    "Soft links may point to any file, or directory, irrespective of the volumes on which " + EOL +
                    "both the link and the target reside." + EOL;

            try (
                    Path srcFilePath = new Path().of(tmpFolder.getAbsolutePath());
                    Path coldRoot = new Path().of(srcFilePath).concat("S3").slash$();
                    Path softLinkRenamedFilePath = new Path().of(coldRoot).concat(fileName).$();
                    Path softLinkFilePath = new Path().of(softLinkRenamedFilePath).put(TableUtils.ATTACHABLE_DIR_MARKER).$()
            ) {
                createTempFile(srcFilePath, fileName, fileContent); // updates srcFilePath

                // create the soft link
                createSoftLink(coldRoot, srcFilePath, softLinkFilePath);

                // check contents are the same
                assertEqualsFileContent(softLinkFilePath, fileContent);

                // delete soft link
                Assert.assertTrue(Files.remove(softLinkFilePath));

                // check original file still exists and contents are the same
                assertEqualsFileContent(srcFilePath, fileContent);

                // create the soft link again
                createSoftLink(coldRoot, srcFilePath, softLinkFilePath);

                // rename the link
                Assert.assertEquals(0, Files.rename(softLinkFilePath, softLinkRenamedFilePath));

                // check contents are the same
                assertEqualsFileContent(softLinkRenamedFilePath, fileContent);

                // delete original file
                Assert.assertTrue(Files.remove(srcFilePath));

                // check that when listing the folder where the link is, we can actually find it
                File link = new File(softLinkRenamedFilePath.toString());
                File[] fileArray = link.getParentFile().listFiles();
                Assert.assertNotNull(fileArray);
                List<File> files = Arrays.asList(fileArray);
                Assert.assertEquals(fileName, files.get(0).getName());

                // however, OS checks do check the existence of the file pointed to
                Assert.assertFalse(link.exists());
                Assert.assertFalse(link.canRead());
                Assert.assertEquals(-1, Files.openRO(softLinkFilePath));
                Assert.assertTrue(Files.remove(softLinkRenamedFilePath));
            }
        });
    }
}
