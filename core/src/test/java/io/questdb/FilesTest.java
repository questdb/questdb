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

public class FilesTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testAllocate() throws Exception {
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
    }

    @Test
    public void testAllocateLoop() throws Exception {
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
    }

    @Test
    public void testCopy() throws Exception {
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
    }

    @Test
    public void testCopyBigFile() throws Exception {
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
    }

    @Test
    public void testDeleteDir2() throws Exception {
        File r = temporaryFolder.newFolder("to_delete");
        Assert.assertTrue(new File(r, "a/b/c").mkdirs());
        Assert.assertTrue(new File(r, "d/e/f").mkdirs());
        touch(new File(r, "d/1.txt"));
        touch(new File(r, "a/b/2.txt"));
        try (Path path = new Path().of(r.getAbsolutePath()).$()) {
            Assert.assertEquals(0, Files.rmdir(path));
            Assert.assertFalse(r.exists());
        }
    }

    @Test
    public void testDeleteOpenFile() throws Exception {
        try (Path path = new Path()) {
            File f = temporaryFolder.newFile();
            long fd = Files.openRW(path.of(f.getAbsolutePath()).$());
            Assert.assertTrue(Files.exists(fd));
            Assert.assertTrue(Files.remove(path));
            Assert.assertFalse(Files.exists(fd));
            Files.close(fd);
        }
    }

    @Test
    public void testFailsToAllocateWhenNotEnoughSpace() throws Exception {
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
    }

    @Test
    public void testLastModified() throws IOException, NumericException {
        try (Path path = new Path()) {
            assertLastModified(path, DateFormatUtils.parseUTCDate("2015-10-17T10:00:00.000Z"));
            assertLastModified(path, 122222212222L);
        }
    }

    @Test
    public void testListDir() {
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
    }

    @Test
    public void testListNonExistingDir() {
        String temp = temporaryFolder.getRoot().getAbsolutePath();
        try (Path path = new Path().of(temp).concat("xyz").$()) {
            long pFind = Files.findFirst(path);
            Assert.assertEquals("failed os=" + Os.errno(), 0, pFind);
        }
    }

    @Test
    public void testMkdirs() throws Exception {
        File r = temporaryFolder.newFolder("to_delete");
        try (Path path = new Path().of(r.getAbsolutePath())) {
            path.concat("a").concat("b").concat("c").concat("f.text").$();
            Assert.assertEquals(0, Files.mkdirs(path, 509));
        }

        try (Path path = new Path().of(r.getAbsolutePath())) {
            path.concat("a").concat("b").concat("c").$();
            Assert.assertTrue(Files.exists(path));
        }
    }

    @Test
    public void testOpenCleanRWAllocatesToSize() {
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
    }

    @Test
    public void testOpenCleanRWParallel() throws Exception {
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
    }

    @Test
    public void testRemove() throws Exception {
        try (Path path = new Path().of(temporaryFolder.newFile().getAbsolutePath()).$()) {
            Assert.assertTrue(Files.touch(path));
            Assert.assertTrue(Files.exists(path));
            Assert.assertTrue(Files.remove(path));
            Assert.assertFalse(Files.exists(path));
        }
    }

    @Test
    public void testTruncate() throws Exception {
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
    }

    private static void touch(File file) throws IOException {
        FileOutputStream fos = new FileOutputStream(file);
        fos.close();
    }

    private void assertLastModified(Path path, long t) throws IOException {
        File f = temporaryFolder.newFile();
        Assert.assertTrue(Files.touch(path.of(f.getAbsolutePath()).$()));
        Assert.assertTrue(Files.setLastModified(path, t));
        Assert.assertEquals(t, Files.getLastModified(path));
    }
}
