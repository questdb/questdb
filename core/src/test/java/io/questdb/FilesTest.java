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

package io.questdb;

import io.questdb.log.LogError;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class FilesTest {
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

                long fd = Files.openRW(path);
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
    public void testAllocateLoop() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            TestUtils.writeStringToFile(temp, "abcde");
            try (Path path = new Path().of(temp.getAbsolutePath()).$()) {
                Assert.assertTrue(Files.exists(path));
                Assert.assertEquals(5, Files.length(path));
                long fd = Files.openRW(path);

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
                src.chop$();

                File f2 = new File(Chars.toString(src.concat("file2")));
                TestUtils.writeStringToFile(f2, "efgh");

                src.of(temporaryFolder.getRoot().getAbsolutePath());
                try (Path dst = new Path().of(temporaryFolder.getRoot().getPath()).put("copy").$()) {
                    Assert.assertEquals(0, FilesFacadeImpl.INSTANCE.copyRecursive(src, dst, mkdirMode));

                    dst.concat("file");
                    src.concat("file");
                    TestUtils.assertFileContentsEquals(src, dst);
                    dst.parent();
                    src.parent();

                    src.concat("subdir").concat("file2");
                    dst.concat("subdir").concat("file2");
                    TestUtils.assertFileContentsEquals(src, dst);
                }
            }
        });
    }

    @Test
    public void testHardLinkAsciiName() throws Exception {
        assertHardLinkPreservesFileContent("some_column.d");
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
                long fd = Files.openRW(path.of(f.getAbsolutePath()).$());
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
                long fd = Files.openRW(path);
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
                temporaryFolder.delete();
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
                src.chop$();

                File f2 = new File(Chars.toString(src.concat("file2")));
                TestUtils.writeStringToFile(f2, "efgh");

                src.of(temporaryFolder.getRoot().getAbsolutePath());
                try (Path dst = new Path().of(temporaryFolder.getRoot().getPath()).put("copy").$()) {
                    Assert.assertEquals(0, FilesFacadeImpl.INSTANCE.hardLinkDirRecursive(src, dst, mkdirMode));

                    dst.concat("file");
                    src.concat("file");
                    TestUtils.assertFileContentsEquals(src, dst);
                    dst.parent();
                    src.parent();

                    src.concat("subdir").concat("file2");
                    dst.concat("subdir").concat("file2");
                    TestUtils.assertFileContentsEquals(src, dst);
                }
            }
        });
    }

    @Test
    public void testLastModified() throws Exception {
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
                long fd = Files.openCleanRW(path, 1024);
                Assert.assertTrue(Files.exists(path));
                Assert.assertEquals(1024, Files.length(path));

                long fd2 = Files.openCleanRW(path, 2048);
                Assert.assertEquals(2048, Files.length(path));

                Files.close(fd);
                Files.close(fd2);
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
                                long fd = Files.openCleanRW(path, fileSize);
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
    public void testTruncate() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            TestUtils.writeStringToFile(temp, "abcde");
            try (Path path = new Path().of(temp.getAbsolutePath()).$()) {
                Assert.assertTrue(Files.exists(path));
                Assert.assertEquals(5, Files.length(path));

                long fd = Files.openRW(path);
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

    private static void touch(File file) throws IOException {
        FileOutputStream fos = new FileOutputStream(file);
        fos.close();
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
        long fd = -1L;
        try {
            fd = Files.openAppend(path.concat(fileName).$());
            if (fd > -1L) {
                Files.truncate(fd, 0);
                Files.append(fd, buffPtr, bytes.length);
                Files.sync();
            }
            Assert.assertTrue(Files.exists(fd));
        } finally {
            if (fd != -1L) {
                Files.close(fd);
            }
            Unsafe.free(buffPtr, buffSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertEqualsLinkedFileContent(Path path, String fileContent) {
        final int buffSize = 2048;
        final long buffPtr = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);
        long fd = -1L;
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
            if (fd != -1L) {
                Files.close(fd);
            }
            Unsafe.free(buffPtr, buffSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertHardLinkPreservesFileContent(String fileName) throws Exception {
        assertMemoryLeak(() -> {
            File dbRoot = temporaryFolder.newFolder("dbRoot");
            Path srcFilePath = new Path().of(dbRoot.getAbsolutePath());
            Path hardLinkFilePath = null;
            try {
                final String EOL = System.lineSeparator();
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

                createTempFile(srcFilePath, fileName, fileContent);

                // perform the hard link
                hardLinkFilePath = new Path().of(srcFilePath).put(".1").$();
                Assert.assertEquals(0, Files.hardLink(srcFilePath, hardLinkFilePath));

                // check content are the same
                assertEqualsLinkedFileContent(hardLinkFilePath, fileContent);

                // delete source file
                Assert.assertTrue(Files.remove(srcFilePath));

                // check linked file still exists and content are the same
                assertEqualsLinkedFileContent(hardLinkFilePath, fileContent);
            } finally {
                Files.remove(srcFilePath);
                Misc.free(srcFilePath);
                if (null != hardLinkFilePath) {
                    Assert.assertTrue(Files.remove(hardLinkFilePath));
                }
                Misc.free(hardLinkFilePath);
                temporaryFolder.delete();
            }
        });
    }

    private void assertLastModified(Path path, long t) throws IOException {
        File f = temporaryFolder.newFile();
        Assert.assertTrue(Files.touch(path.of(f.getAbsolutePath()).$()));
        Assert.assertTrue(Files.setLastModified(path, t));
        Assert.assertEquals(t, Files.getLastModified(path));
    }
}
