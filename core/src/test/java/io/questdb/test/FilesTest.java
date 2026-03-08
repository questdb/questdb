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

package io.questdb.test;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.log.Log;
import io.questdb.log.LogError;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.AssumptionViolatedException;
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

import static io.questdb.test.tools.TestUtils.assertContains;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class FilesTest {
    private static final String EOL = System.lineSeparator();
    private static final Log LOG = LogFactory.getLog(FilesTest.class);
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testAllocate() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            TestUtils.writeStringToFile(temp, "abcde");
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                Assert.assertTrue(Files.exists(path.$()));
                Assert.assertEquals(5, Files.length(path.$()));

                long fd = Files.openRW(path.$());
                try {
                    Files.allocate(fd, 10);
                    Assert.assertEquals(10, Files.length(path.$()));
                    Files.allocate(fd, 120);
                    Assert.assertEquals(120, Files.length(path.$()));
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
        Assume.assumeTrue(Os.type != Os.DARWIN);
        FilesFacade ff = TestFilesFacadeImpl.INSTANCE;

        String tmpFolder = temporaryFolder.newFolder("allocate").getAbsolutePath();
        assumeIsNotTmpFs(tmpFolder);
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
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                Assert.assertTrue(Files.exists(path.$()));
                Assert.assertEquals(5, Files.length(path.$()));
                long fd = Files.openRW(path.$());

                long M100 = 100 * 1024L * 1024L;
                try {
                    // If allocate tries to allocate by the given size
                    // instead of to the size this will allocate 2TB and suppose to fail
                    for (int i = 0; i < 20000; i++) {
                        Files.allocate(fd, M100 + i);
                        Assert.assertEquals(M100 + i, Files.length(path.$()));
                    }
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testConcurrentRemove() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            TestUtils.writeStringToFile(temp, "abcde");
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                Assert.assertTrue(Files.exists(path.$()));
                Assert.assertEquals(5, Files.length(path.$()));

                int threadCount = 4;
                CyclicBarrier barrier = new CyclicBarrier(threadCount);
                AtomicInteger errorCounter = new AtomicInteger();
                ObjList<Thread> threads = new ObjList<>();

                for (int i = 0; i < threadCount; i++) {
                    threads.add(new Thread(() -> {
                        try {
                            barrier.await();
                            ff.remove(path.$());
                        } catch (Throwable e) {
                            e.printStackTrace(System.out);
                            LOG.error().$(e).$();
                            errorCounter.incrementAndGet();
                        }
                    }));
                }

                for (int i = 0; i < threadCount; i++) {
                    threads.getQuick(i).start();
                }

                for (int i = 0; i < threadCount; i++) {
                    threads.getQuick(i).join();
                }

                Assert.assertEquals(0, errorCounter.get());
            }
        });
    }

    @Test
    public void testCopy() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            TestUtils.writeStringToFile(temp, "abcde");
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                Assert.assertTrue(Files.exists(path.$()));
                try (Path copyPath = new Path().of(temp.getAbsolutePath()).put("-copy")) {
                    Files.copy(path.$(), copyPath.$());

                    Assert.assertEquals(5, Files.length(copyPath.$()));
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

            try (Path path = new Path().of(temp.getAbsolutePath())) {
                Assert.assertTrue(Files.exists(path.$()));
                try (Path copyPath = new Path().of(temp.getAbsolutePath()).put("-copy")) {
                    Files.copy(path.$(), copyPath.$());

                    Assert.assertEquals(fileSize, Files.length(copyPath.$()));
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
                File f1 = new File(Utf8s.toString(src.concat("file")));
                TestUtils.writeStringToFile(f1, "abcde");

                src.parent();
                src.concat("subdir");
                Assert.assertEquals(0, Files.mkdir(src.$(), mkdirMode));

                File f2 = new File(Utf8s.toString(src.concat("file2")));
                TestUtils.writeStringToFile(f2, "efgh");

                src.of(temporaryFolder.getRoot().getAbsolutePath());
                try (
                        Path dst = new Path().of(temporaryFolder.getRoot().getPath()).put("copy");
                        Path p2 = new Path().of(dst).slash()
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
                        Files.rmdir(p2, true);
                    }
                }
            }
        });
    }

    @Test
    public void testDeatch() throws Exception {
        assertMemoryLeak(() -> {
            int fdFake = 123;
            long fd = Files.createUniqueFd(fdFake);
            Files.detach(fd);
        });
    }

    @Test
    public void testDeleteDir() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            File r = temporaryFolder.newFolder("to_delete");
            Assert.assertTrue(new File(r, "a" + Files.SEPARATOR + "b" + Files.SEPARATOR + "c" + Files.SEPARATOR + "d").mkdirs());
            Assert.assertTrue(new File(r, "d" + Files.SEPARATOR + "e" + Files.SEPARATOR + "f").mkdirs());
            touch(new File(r, "a" + Files.SEPARATOR + "1.txt"));
            touch(new File(r, "a" + Files.SEPARATOR + "b" + Files.SEPARATOR + "2.txt"));
            touch(new File(r, "a" + Files.SEPARATOR + "b" + Files.SEPARATOR + "c" + Files.SEPARATOR + "3.txt"));
            touch(new File(r, "d" + Files.SEPARATOR + "1.txt"));
            touch(new File(r, "d" + Files.SEPARATOR + "e" + Files.SEPARATOR + "2.txt"));
            touch(new File(r, "d" + Files.SEPARATOR + "e" + Files.SEPARATOR + "f" + Files.SEPARATOR + "3.txt"));

            try (
                    Path targetPath = new Path().of(r.getAbsolutePath()).concat("d");
                    Path linkPath = new Path().of(r.getAbsolutePath()).concat("a").concat("link_to_d")
            ) {
                Assert.assertEquals(0, Files.softLink(targetPath.$(), linkPath.$()));
                Assert.assertTrue(Files.isSoftLink(linkPath.$()));

                targetPath.of(r.getAbsolutePath());
                linkPath.of(r.getParentFile().getAbsolutePath()).concat("start_here");
                Assert.assertEquals(0, Files.softLink(targetPath.$(), linkPath.$()));
                Assert.assertTrue(Files.isSoftLink(linkPath.$()));

                Assert.assertTrue(Files.rmdir(linkPath, true));
                Assert.assertFalse(new File(linkPath.toString()).exists());
                Assert.assertTrue(r.exists());
                Assert.assertEquals(0L, Files.getDirSize(targetPath));
                Assert.assertTrue(Files.rmdir(targetPath.slash(), true));
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
                Assert.assertTrue(TestUtils.remove(path.$()));
                Assert.assertFalse(Files.exists(fd));
                Files.close(fd);
            }
        });
    }

    @Test
    public void testDirectoryContentSize() throws Exception {
        String content = "Disk partitioning is the creation of one or more regions on secondary storage," + System.lineSeparator() +
                "so that each region can be managed separately. These regions are called partitions." + System.lineSeparator() +
                "The disk stores the information about the partitions' locations and sizes in an area" + System.lineSeparator() +
                "known as the partition table that the operating system reads before any other part of" + System.lineSeparator() +
                "the disk. Each partition then appears to the operating system as a distinct 'logical'" + System.lineSeparator() +
                "disk that uses part of the actual disk." + System.lineSeparator();
        assertMemoryLeak(() -> {
            File noDir = temporaryFolder.newFolder("данные", "no");
            try (Path path = new Path().of(noDir.getParentFile().getAbsolutePath())) {
                int rootLen = path.size();
                createTempFile(path, "prose.txt", content);
                long baseSize = Files.getDirSize(path.parent());
                createTempFile(path, "dinner.txt", content);
                createTempFile(path.parent(), "apple.txt", content);
                createTempFile(path.parent().concat("no"), "lunch.txt", content);
                createTempFile(path.parent(), "ребенок.txt", content);
                Assert.assertEquals(5 * baseSize, Files.getDirSize(path.trimTo(rootLen)));
                Assert.assertEquals(2 * baseSize, Files.getDirSize(path.concat("no")));
                Assert.assertEquals(0L, Files.getDirSize(path.concat("ребенок.txt")));
            }
        });
    }

    @Test
    public void testDirectoryContentSizeNonExistingDirectory() {
        try (Path path = new Path().of("banana")) {
            Assert.assertEquals(0L, Files.getDirSize(path));
        }
    }

    @Test
    public void testDirectoryContentSizeOfFile() throws IOException {
        String content = "nothing to report";
        File dir = temporaryFolder.newFolder("banana");
        try (Path path = new Path().of(dir.getAbsolutePath())) {
            createTempFile(path, "small.txt", content);
            Assert.assertEquals(0L, Files.getDirSize(path));
        }
    }

    @Test
    public void testDirectoryContentSizeWithLinks() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        String content = "RDBMSs favor consistency over availability and performance which" + System.lineSeparator() +
                "complicates scaling the system horizontally with efficiency in big data" + System.lineSeparator() +
                "scenarios. As a result, new DBMSs were developed to relax some consistency " + System.lineSeparator() +
                "constraints and provide better scalability and performance. Many new technologies," + System.lineSeparator() +
                "therefore, were introduced including wide-column stores Google Bigtable, " + System.lineSeparator() +
                "Apache Cassandra, Apache HBase; key-value stores DynamoDB, LevelDB, and RocksDB;" + System.lineSeparator() +
                "document-based stores AsterixDB, ArangoDB, and MongoDB; column-oriented" + System.lineSeparator() +
                "stores Apache Druid and ClickHouse; graph stores Neo4j. However, the evolution" + System.lineSeparator() +
                "of time-series applications in big data environments like large-scale scientific" + System.lineSeparator() +
                "experiments, Internet of Things (IoT), IT infrastructure monitoring, industrial" + System.lineSeparator() +
                "control systems, and forecasting and financial trends allowed the" + System.lineSeparator() +
                "emergence of many Time-Series Databases (TSDB) technologies." + System.lineSeparator();
        assertMemoryLeak(() -> {
            File dbDir = temporaryFolder.newFolder("db", "table", "partition");
            File backupDir = temporaryFolder.newFolder("backup", "table", "partition" + TableUtils.ATTACHABLE_DIR_MARKER);
            File tmpDir = temporaryFolder.newFolder("tmp");
            try (
                    Path dbPath = new Path().of(dbDir.getParentFile().getParentFile().getAbsolutePath());
                    Path backupPath = new Path().of(backupDir.getParentFile().getParentFile().getAbsolutePath());
                    Path auxPath = new Path()
            ) {
                int dbPathLen = dbPath.size();
                int backupPathLen = backupPath.size();
                createTempFile(auxPath.of(tmpDir.getAbsolutePath()), "for_size.d", content);
                long baseSize = Files.getDirSize(auxPath.parent());

                // create files at table level
                createTempFile(dbPath.concat("table"), "_meta", content);
                createTempFile(dbPath.parent(), "_txn", content);
                createTempFile(dbPath.parent(), "_cv", content);

                // create files at partition level
                createTempFile(dbPath.parent().concat("partition"), "timestamp.d", content);
                createTempFile(dbPath.parent(), "column0.d", content);
                createTempFile(dbPath.parent(), "column1.d", content);

                // create a link to a file
                Assert.assertEquals(0, Files.softLink(
                        dbPath.parent().concat("timestamp.d").$(),
                        auxPath.of(dbPath).parent().concat("timestamp_copy.d").$())
                );

                // create files in backup
                createTempFile(backupPath.concat("table"), "_meta", content);
                createTempFile(backupPath.parent(), "_txn", content);
                createTempFile(backupPath.parent(), "_cv", content);
                createTempFile(backupPath.parent().concat("partition" + TableUtils.ATTACHABLE_DIR_MARKER), "timestamp.d", content);
                createTempFile(backupPath.parent(), "column0.d", content);
                createTempFile(backupPath.parent(), "column1.d", content);

                // create an "attachable" partition
                Assert.assertEquals(0, Files.softLink(
                        backupPath.parent().$(),
                        dbPath.parent().parent().concat("partitionB").$()));

                // structure:
                // db/table/_meta
                // db/table/_txn
                // db/table/_cv
                // db/table/partition
                // db/table/partition/timestamp.d
                // db/table/partition/timestamp_copy.d -> db/table/partition/timestamp.d
                // db/table/partition/column0.d
                // db/table/partition/column1.d
                // db/table/partitionB -> backup/table/partition.attachable/
                // backup/table/_meta
                // backup/table/_txn
                // backup/table/_cv
                // backup/table/partition.attachable/
                // backup/table/partition.attachable/timestamp.d
                // backup/table/partition.attachable/column0.d
                // backup/table/partition.attachable/column1.d

                Assert.assertEquals(9 * baseSize, Files.getDirSize(dbPath.trimTo(dbPathLen)));
                Assert.assertEquals(9 * baseSize, Files.getDirSize(dbPath.concat("table")));
                Assert.assertEquals(3 * baseSize, Files.getDirSize(dbPath.concat("partition")));
                Assert.assertEquals(3 * baseSize, Files.getDirSize(dbPath.parent().concat("partitionB")));
                Assert.assertEquals(0L, Files.getDirSize(dbPath.concat("timestamp.d")));
                Assert.assertEquals(baseSize, Files.length(dbPath.$()));
                Assert.assertEquals(6 * baseSize, Files.getDirSize(backupPath.trimTo(backupPathLen)));
                Assert.assertEquals(6 * baseSize, Files.getDirSize(backupPath.concat("table")));
                Assert.assertEquals(3 * baseSize, Files.getDirSize(backupPath.concat("partition" + TableUtils.ATTACHABLE_DIR_MARKER)));
                Assert.assertEquals(15 * baseSize,
                        Files.getDirSize(dbPath.trimTo(dbPathLen)) + Files.getDirSize(backupPath.trimTo(backupPathLen))
                );
            }
        });
    }

    @Test
    public void testFailsToAllocateWhenNotEnoughSpace() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            TestUtils.writeStringToFile(temp, "abcde");
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                Assert.assertTrue(Files.exists(path.$()));
                long fd = Files.openRW(path.$());
                Assert.assertEquals(5, Files.length(path.$()));

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
    public void testFdCache() {
        for (int index = 0; index < 128; index++) {
            int NON_CACHED = (2 << 30);
            long fd = Numbers.encodeLowHighInts(index | NON_CACHED, 3342);
            int fdKind = (Numbers.decodeLowInt(fd) >>> 30) & 3;
            Assert.assertTrue(fdKind > 1);

            int RO_MASK = 0;
            fd = Numbers.encodeLowHighInts(index | RO_MASK, 78234);
            fdKind = (Numbers.decodeLowInt(fd) >>> 30) & 3;
            Assert.assertEquals(0, fdKind);
        }
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
                srcFilePath = new Path().of(dbRoot.getAbsolutePath()).concat("some_column.d");
                hardLinkFilePath = new Path().of(srcFilePath).put(".1");
                Assert.assertEquals(-1, Files.hardLink(srcFilePath.$(), hardLinkFilePath.$()));
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
                File f1 = new File(Utf8s.toString(src.concat("file")));
                TestUtils.writeStringToFile(f1, "abcde");

                src.parent();
                src.concat("subdir");
                Assert.assertEquals(0, Files.mkdir(src.$(), mkdirMode));

                File f2 = new File(Utf8s.toString(src.concat("file2")));
                TestUtils.writeStringToFile(f2, "efgh");

                src.of(temporaryFolder.getRoot().getAbsolutePath());
                try (
                        Path dst = new Path().of(temporaryFolder.getRoot().getPath()).put("copy");
                        Path p2 = new Path().of(dst).slash()
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
                        Files.rmdir(p2, true);
                    }
                }
            }
        });
    }

    @Test
    public void testIsDirOrSoftLinkDir() throws Exception {
        // Technically, the code _should_ (and is) supported on Windows too.
        // That said it requires Admin or Developer mode enabled on Windows. It runs just fine on CI, but fails to run
        // on most desktop machines. On Windows this is thus hidden behind an environment variable.
        final boolean testSymlinks = !Os.isWindows() || "1".equals(System.getenv("QDB_TEST_WINDOWS_SYMLINKS"));
        final File baseDir = temporaryFolder.newFolder();

        setupPath(baseDir, "empty_dir/");
        setupPath(baseDir, "file");
        setupPath(baseDir, "dir_with_a_file/file");
        setupPath(baseDir, "dir_with_an_empty_dir/dir/");

        if (testSymlinks) {
            setupPath(baseDir, "link_to_file -> file");
            setupPath(baseDir, "link_to_empty_dir -> empty_dir/");
            setupPath(baseDir, "link_to_dir_with_a_file -> dir_with_a_file/");
            setupPath(baseDir, "link_to_dir_with_an_empty_dir -> dir_with_an_empty_dir/");
            setupPath(baseDir, "nonexistent"); // deleted later
            setupPath(baseDir, "link_to_nonexistent -> nonexistent");
            setupPath(baseDir, "link_to_link_to_file -> link_to_file");
            setupPath(baseDir, "link_to_link_to_empty_dir -> link_to_empty_dir");
            setupPath(baseDir, "link_to_link_to_dir_with_a_file -> link_to_dir_with_a_file");
            setupPath(baseDir, "link_to_link_to_dir_with_an_empty_dir -> link_to_dir_with_an_empty_dir");
            setupPath(baseDir, "link_to_link_to_nonexistent -> link_to_nonexistent");

            final File nonexistent = new File(baseDir, "nonexistent");
            Assert.assertTrue(nonexistent.delete());
            Assert.assertFalse(nonexistent.exists());
        }

        assertMemoryLeak(() -> {
            Assert.assertFalse(isDirOrSoftLinkDir(baseDir, "something/that/does/not/exist"));
            Assert.assertTrue(isDirOrSoftLinkDir(baseDir, "empty_dir/"));
            Assert.assertTrue(isDirOrSoftLinkDir(baseDir, "empty_dir/."));
            Assert.assertFalse(isDirOrSoftLinkDir(baseDir, "file"));
            Assert.assertTrue(isDirOrSoftLinkDir(baseDir, "dir_with_a_file/"));
            Assert.assertTrue(isDirOrSoftLinkDir(baseDir, "dir_with_an_empty_dir/"));
            Assert.assertTrue(isDirOrSoftLinkDir(baseDir, "dir_with_an_empty_dir/dir/.."));

            if (testSymlinks) {
                Assert.assertFalse(isDirOrSoftLinkDir(baseDir, "link_to_file"));
                Assert.assertTrue(isDirOrSoftLinkDir(baseDir, "link_to_empty_dir"));
                Assert.assertTrue(isDirOrSoftLinkDir(baseDir, "link_to_dir_with_a_file"));
                Assert.assertTrue(isDirOrSoftLinkDir(baseDir, "link_to_dir_with_an_empty_dir"));
                Assert.assertFalse(isDirOrSoftLinkDir(baseDir, "link_to_nonexistent"));
                Assert.assertFalse(isDirOrSoftLinkDir(baseDir, "link_to_link_to_file"));
                Assert.assertTrue(isDirOrSoftLinkDir(baseDir, "link_to_link_to_empty_dir"));
                Assert.assertTrue(isDirOrSoftLinkDir(baseDir, "link_to_link_to_dir_with_a_file"));
                Assert.assertTrue(isDirOrSoftLinkDir(baseDir, "link_to_link_to_dir_with_an_empty_dir"));
                Assert.assertFalse(isDirOrSoftLinkDir(baseDir, "link_to_link_to_nonexistent"));
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
            try (Path path = new Path().of(temp)) {
                try (Path cp = new Path()) {
                    Assert.assertTrue(Files.touch(cp.of(temp).concat("a.txt").$()));
                    StringSink nameSink = new StringSink();
                    long pFind = Files.findFirst(path.$());
                    Assert.assertTrue(pFind != 0);
                    try {
                        do {
                            nameSink.clear();
                            Utf8s.utf8ToUtf16Z(Files.findName(pFind), nameSink);
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
            try (Path path = new Path().of(temp).concat("xyz")) {
                long pFind = Files.findFirst(path.$());
                Assert.assertEquals("failed os=" + Os.errno(), 0, pFind);
            }
        });
    }

    @Test
    public void testLongFd() {
        long unuqFd = Numbers.encodeLowHighInts(1000, -1);
        Assert.assertTrue(unuqFd < 0);
    }

    @Test
    public void testLongFd2() {
        long unuqFd = Numbers.encodeLowHighInts(Integer.MAX_VALUE, 1000);
        Assert.assertTrue(unuqFd > 0);
    }

    @Test
    public void testMixedIOConcurrent() throws Exception {
        final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;

        // This test aims to follow write pattern possible when handling O3 tasks.
        // Concurrent mmap-based writes and pwrite() may break read-your-write
        // guarantee on certain file systems.
        Assume.assumeTrue(ff.allowMixedIO(temporaryFolder.getRoot().getAbsolutePath()));

        File file = temporaryFolder.newFile();
        final int fileSize = 2 * 1024 * 1024; // in MB
        final long valueInMem = 42;

        AtomicInteger errors = new AtomicInteger();

        long srcMem = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().putLong(srcMem, valueInMem);

        long fd = -1;
        long mmapMem = 0;
        try (Path dstPath = new Path().of(file.getAbsolutePath())) {
            Assert.assertTrue(Files.exists(dstPath.$()));
            fd = Files.openRW(dstPath.$());
            mmapMem = TableUtils.mapRW(ff, fd, fileSize, MemoryTag.MMAP_DEFAULT);

            final long finalFd = fd;
            long finalMmapMem = mmapMem;
            Thread th1 = new Thread(() -> {
                for (long offset = 0; offset < fileSize; offset += 2 * Long.BYTES) {
                    long res = Files.write(finalFd, srcMem, Long.BYTES, offset);
                    if (res < 0) {
                        errors.incrementAndGet();
                        break;
                    }
                    long valueOnDisk = Files.readNonNegativeLong(finalFd, offset);
                    if (valueInMem != valueOnDisk) {
                        errors.incrementAndGet();
                        break;
                    }
                }
            });
            Thread th2 = new Thread(() -> {
                for (long offset = Long.BYTES; offset < fileSize; offset += 2 * Long.BYTES) {
                    Unsafe.getUnsafe().putLong(finalMmapMem + offset, valueInMem);
                    long valueOnDisk = Files.readNonNegativeLong(finalFd, offset);
                    if (valueInMem != valueOnDisk) {
                        errors.incrementAndGet();
                        break;
                    }
                }
            });

            th1.start();
            th2.start();
            th1.join();
            th2.join();

            Assert.assertEquals(0, errors.get());
        } finally {
            Files.close(fd);
            Unsafe.free(srcMem, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.munmap(mmapMem, fileSize, MemoryTag.MMAP_DEFAULT);
        }
    }

    @Test
    public void testMkdirs() throws Exception {
        assertMemoryLeak(() -> {
            File r = temporaryFolder.newFolder("to_delete");
            try (Path path = new Path().of(r.getAbsolutePath())) {
                path.concat("a").concat("b").concat("c").concat("f.text");
                Assert.assertEquals(0, Files.mkdirs(path, 509));
            }

            try (Path path = new Path().of(r.getAbsolutePath())) {
                path.concat("a").concat("b").concat("c");
                Assert.assertTrue(Files.exists(path.$()));
            }
        });
    }

    @Test
    public void testMmapInvalid() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                Assert.assertTrue(Files.exists(path.$()));
                long fdrw = Files.openRW(path.$());
                try {
                    if (!Files.allocate(fdrw, 1024)) {
                        Assert.fail("Files.allocate() failed with errno " + Os.errno());
                    }
                } finally {
                    Files.close(fdrw);
                }
                long fdro = Files.openRO(path.$());
                try {
                    long mmapAddr = Files.mmap(fdro, 0, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
                    Files.munmap(mmapAddr, 0, MemoryTag.MMAP_DEFAULT);
                    Assert.fail("mmap with zero len should have failed");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "invalid len");
                } finally {
                    Files.close(fdro);
                }
            }
        });
    }

    @Test
    public void testMremapInvalid() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                Assert.assertTrue(Files.exists(path.$()));
                long fdrw = Files.openRW(path.$());
                try {
                    if (!Files.allocate(fdrw, 1024)) {
                        Assert.fail("Files.allocate() failed with errno " + Os.errno());
                    }
                } finally {
                    Files.close(fdrw);
                }
                long fdro = Files.openRO(path.$());
                try {
                    long mmapAddr = Files.mmap(fdro, 64, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
                    Assert.assertNotEquals("mmap should have succeeded", FilesFacade.MAP_FAILED, mmapAddr);
                    try {
                        mmapAddr = Files.mremap(fdro, mmapAddr, 64, 0, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
                        Files.munmap(mmapAddr, 0, MemoryTag.MMAP_DEFAULT);
                        Assert.fail("mremap with len 0 should have failed");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "invalid newSize");
                    } finally {
                        Files.munmap(mmapAddr, 64, MemoryTag.MMAP_DEFAULT);
                    }
                } finally {
                    Files.close(fdro);
                }
            }
        });
    }

    @Test
    public void testMunmapInvalidZeroAddress() throws Exception {
        assertMemoryLeak(() -> {
            try {
                Files.munmap(0, 64, MemoryTag.MMAP_DEFAULT);
                Assert.fail("Expected CairoException");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "invalid address");
            }
        });
    }

    @Test
    public void testMunmapInvalidZeroLength() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                Assert.assertTrue(Files.exists(path.$()));

                long fdrw = Files.openRW(path.$());
                long fdro = Files.openRO(path.$());

                if (Files.allocate(fdrw, 1024)) {
                    long addr1 = Files.mmap(fdro, 64, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
                    try {
                        Files.munmap(addr1, 0, MemoryTag.MMAP_DEFAULT);
                    } catch (CairoException e) {
                        assertContains(e.getFlyweightMessage(), "invalid address or length");
                    } finally {
                        Files.munmap(addr1, 64, MemoryTag.MMAP_DEFAULT);
                    }
                }

                Files.close(fdrw);
                Files.close(fdro);
            }
        });
    }

    @Test
    public void testOpenCleanRWAllocatesToSize() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.getRoot();
            try (Path path = new Path().of(temp.getAbsolutePath()).concat("openCleanRWParallel")) {
                long fd = Files.openCleanRW(path.$(), 1024);
                Assert.assertTrue(Files.exists(path.$()));
                Assert.assertEquals(1024, Files.length(path.$()));

                long fd2 = Files.openCleanRW(path.$(), 2048);
                Assert.assertEquals(2048, Files.length(path.$()));

                Files.close(fd);
                Files.close(fd2);
            }
        });
    }

    @Test
    public void testOpenCleanRWFailsWhenCalledOnDir() throws Exception {
        assertMemoryLeak(() -> {
            long fd = -1;
            try (Path path = new Path()) {
                fd = Files.openCleanRW(path.of(temporaryFolder.getRoot().getAbsolutePath()).$(), 32);
                Assert.assertTrue(fd < 0);
            } finally {
                Files.close(fd);
            }
        });
    }

    @Test
    public void testOpenCleanRWLoop() throws Exception {
        // Emulates the syscall sequence from TxnScoreboard's initialization and close.
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                Assert.assertTrue(Files.exists(path.$()));
                long sizeInLongs = 64;
                long sizeInBytes = sizeInLongs * Long.BYTES;

                for (int i = 0; i < 100; i++) {
                    long fd = Files.openCleanRW(path.$(), sizeInBytes);
                    long mem = 0;
                    try {
                        Assert.assertTrue(Files.truncate(fd, sizeInBytes));
                        mem = Files.mmap(fd, sizeInBytes, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);

                        for (long j = 0; j < sizeInLongs; j++) {
                            Assert.assertEquals(0, Unsafe.getUnsafe().getLong(mem + j * Long.BYTES));
                            Unsafe.getUnsafe().putLong(mem + j * Long.BYTES, i);
                        }
                    } finally {
                        if (mem != 0) {
                            Files.munmap(mem, sizeInBytes, MemoryTag.MMAP_DEFAULT);
                        }

                        Assert.assertTrue(Files.close(fd) > -1);
                    }
                }
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

            try (Path path = new Path().of(temp.getAbsolutePath()).concat("openCleanRWParallel")) {
                Assert.assertFalse(Files.exists(path.$()));

                final CyclicBarrier barrier = new CyclicBarrier(threadCount);
                final CountDownLatch halt = new CountDownLatch(threadCount);
                final AtomicInteger errors = new AtomicInteger();

                for (int k = 0; k < threadCount; k++) {
                    new Thread(() -> {
                        try {
                            barrier.await();
                            for (int i = 0; i < iteration; i++) {
                                long fd = Files.openCleanRW(path.$(), fileSize);
                                if (fd < 0) {
                                    errors.incrementAndGet();
                                }
                                int error = Files.close(fd);
                                if (error != 0) {
                                    errors.incrementAndGet();
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace(System.out);
                            errors.incrementAndGet();
                        } finally {
                            halt.countDown();
                        }
                    }).start();
                }

                halt.await();
                Assert.assertEquals(0, errors.get());
                Assert.assertTrue(Files.exists(path.$()));
                Assert.assertEquals(fileSize, Files.length(path.$()));
            }
        });
    }

    @Test
    public void testOpenRWFailsWhenCalledOnDir() throws Exception {
        assertMemoryLeak(() -> {
            long fd = -1;
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
                long fd1 = Files.openRW(path.$());
                long fileSize = 4096;
                long mem = Unsafe.malloc(fileSize, MemoryTag.NATIVE_DEFAULT);

                long testValue = 0x1234567890ABCDEFL;
                Unsafe.getUnsafe().putLong(mem, testValue);

                try {
                    Files.truncate(fd1, fileSize);

                    Assert.assertEquals(8L, Files.read(fd1, mem, 8L, 0));
                    Assert.assertTrue(Files.read(fd1, mem, fileSize, -1) < 0);
                    Assert.assertEquals(0, Files.read(fd1, mem, fileSize, fileSize));
                    Assert.assertEquals(0, Files.read(fd1, mem, fileSize + 8, fileSize));

                } finally {
                    // Release mem, fd
                    Files.close(fd1);
                    Unsafe.free(mem, fileSize, MemoryTag.NATIVE_DEFAULT);

                    // Delete files
                    TestUtils.remove(path.$());
                }
            }
        });
    }

    @Test
    public void testReadOver2GB() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();

            try (Path path = new Path().of(temp.getAbsolutePath())) {
                long fd1 = Files.openRW(path.$());
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
                    long intAsLong1 = Files.readIntAsUnsignedLong(fd1, 0L);
                    long long1 = Files.readNonNegativeLong(fd1, 0L);
                    Assert.assertEquals((byte) testValue, byte1);
                    Assert.assertEquals((short) testValue, short1);
                    Assert.assertEquals((int) testValue, int1);
                    Assert.assertEquals(Integer.toUnsignedLong((int) testValue), intAsLong1);
                    Assert.assertEquals(testValue, long1);

                    byte byte2 = Files.readNonNegativeByte(fd1, size2Gb - 8);
                    short short2 = Files.readNonNegativeShort(fd1, size2Gb - 8);
                    int int2 = Files.readNonNegativeInt(fd1, size2Gb - 8);
                    long intAsLong2 = Files.readIntAsUnsignedLong(fd1, size2Gb - 8);
                    long long2 = Files.readNonNegativeLong(fd1, size2Gb - 8);
                    Assert.assertEquals((byte) testValue, byte2);
                    Assert.assertEquals((short) testValue, short2);
                    Assert.assertEquals((int) testValue, int2);
                    Assert.assertEquals(Integer.toUnsignedLong((int) testValue), intAsLong2);
                    Assert.assertEquals(testValue, long2);

                } finally {
                    // Release mem, fd
                    Files.close(fd1);
                    Unsafe.free(mem, size2Gb, MemoryTag.NATIVE_DEFAULT);

                    // Delete files
                    TestUtils.remove(path.$());
                }
            }
        });
    }

    @Test
    public void testRecursiveRmdirLimit() throws IOException {
        var ff = new FilesFacadeImpl();
        temporaryFolder.newFolder("a", "b");

        try (Path path = new Path().of(temporaryFolder.getRoot().getAbsolutePath()).concat("a")) {
            temporaryFolder.newFolder("a", ".download", "table", "wal", "segment");

            Assert.assertTrue(ff.rmdir(path));

            temporaryFolder.newFolder("a", ".download", "table", "wal", "segment", "extra");
            Assert.assertFalse(ff.rmdir(path));
        }
    }

    @Test
    public void testRemove() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(temporaryFolder.newFile().getAbsolutePath())) {
                Assert.assertTrue(Files.touch(path.$()));
                Assert.assertTrue(Files.exists(path.$()));
                Assert.assertTrue(TestUtils.remove(path.$()));
                Assert.assertFalse(Files.exists(path.$()));
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
                long fd1 = Files.openRW(path1.$());
                path2.put(".2");
                long fd2 = 0;

                long mem = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);

                long testValue = 0x1234567890ABCDEFL;
                Unsafe.getUnsafe().putLong(mem, testValue);
                long size2Gb = (2L << 30) + 4096;

                try {
                    Files.truncate(fd1, size2Gb);

                    Files.write(fd1, mem, 8, 0);
                    Files.write(fd1, mem, 8, size2Gb - 8);

                    // Check copy call works
                    int result = Files.copy(path1.$(), path2.$());

                    // Check written data
                    // Windows return 1 but Linux and others return 0 on success
                    // All return negative in case of error.
                    Assert.assertTrue("error: " + Os.errno(), result >= 0);

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
                    TestUtils.remove(path1.$());
                    TestUtils.remove(path2.$());
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
                long fd1 = Files.openRW(path1.$());
                path2.put(".2");
                long fd2 = Files.openRW(path2.$());

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

                    Assert.assertEquals("errno: " + Os.errno(), fileSize - offset, copiedLen);

                    long long1 = Files.readNonNegativeLong(fd2, fileSize - offset - 8);
                    Assert.assertEquals(testValue, long1);

                    // Copy with set length
                    Files.close(fd2);
                    TestUtils.remove(path2.$());
                    fd2 = Files.openRW(path2.$());

                    // Check copy call works
                    offset = 3051;
                    copiedLen = Files.copyData(fd1, fd2, offset, fileSize - offset);
                    Assert.assertEquals("errno: " + Os.errno(), fileSize - offset, copiedLen);

                    long1 = Files.readNonNegativeLong(fd2, fileSize - offset - 8);
                    Assert.assertEquals(testValue, long1);

                    // Copy with destination offset
                    Files.close(fd2);
                    TestUtils.remove(path2.$());
                    fd2 = Files.openRW(path2.$());

                    // Check copy with offset call works
                    long destOffset = 1057;
                    copiedLen = Files.copyDataToOffset(fd1, fd2, offset, destOffset, fileSize - offset);
                    Assert.assertEquals(fileSize - offset, copiedLen);

                    long1 = Files.readNonNegativeLong(fd2, destOffset + fileSize - offset - 8);
                    Assert.assertEquals(testValue, long1);

                    // Check subsequent copy call with zero offset works
                    long anotherTestValue = 0x0987654321FEDCBAL;
                    Unsafe.getUnsafe().putLong(mem, anotherTestValue);
                    Files.write(fd1, mem, 8, 0);

                    copiedLen = Files.copyDataToOffset(fd1, fd2, 0, 0, 8);
                    Assert.assertEquals(8, copiedLen);

                    long1 = Files.readNonNegativeLong(fd2, 0);
                    Assert.assertEquals(anotherTestValue, long1);
                } finally {
                    // Release mem, fd
                    Files.close(fd1);
                    Files.close(fd2);
                    Unsafe.free(mem, 8, MemoryTag.NATIVE_DEFAULT);

                    // Delete files
                    TestUtils.remove(path1.$());
                    TestUtils.remove(path2.$());
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
                    Path srcFilePath = new Path().of(tmpFolder.getAbsolutePath()).concat(fileName);
                    Path softLinkFilePath = new Path().of(tmpFolder.getAbsolutePath()).concat(fileName).put(".1")
            ) {
                Assert.assertEquals(0, Files.softLink(srcFilePath.$(), softLinkFilePath.$()));

                // check that when listing it we can actually find it
                File link = new File(softLinkFilePath.toString());
                File[] fileArray = link.getParentFile().listFiles();
                Assert.assertNotNull(fileArray);
                List<File> files = Arrays.asList(fileArray);
                Assert.assertEquals(fileName + ".1", files.get(0).getName());

                // however
                Assert.assertFalse(link.exists());
                Assert.assertFalse(link.canRead());
                Assert.assertEquals(-1, Files.openRO(softLinkFilePath.$()));
                Assert.assertTrue(TestUtils.remove(softLinkFilePath.$()));
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
    public void testSoftLinkRead() throws IOException {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        File dir = temporaryFolder.newFolder();
        try (
                Path path = new Path();
                Path path2 = new Path();
                Path path3 = new Path()
        ) {
            path.of(dir.getAbsolutePath());
            int plen = path.size();

            path.concat("text.txt");

            Files.touch(path.$());

            Assert.assertTrue(Files.exists(path.$()));

            path2.of("text.txt");//
            path.trimTo(plen).concat("rel_link");
            // relative link rel_link -> text.txt
            Assert.assertEquals(0, Files.softLink(path2.$(), path.$()));
            Assert.assertTrue(Files.isSoftLink(path.$()));
            Assert.assertTrue(Files.readLink(path, path3));
            TestUtils.assertEquals(dir.getAbsolutePath() + Files.SEPARATOR + "text.txt", path3.toString());
            // reset link sink
            path3.of("");

            // absolute link
            path2.prefix(path, plen + 1);
            path.trimTo(plen).concat("abs_link");
            Assert.assertEquals(0, Files.softLink(path2.$(), path.$()));
            Assert.assertTrue(Files.isSoftLink(path.$()));
            Assert.assertTrue(Files.readLink(path, path3));
            TestUtils.assertEquals(dir.getAbsolutePath() + Files.SEPARATOR + "text.txt", path3.toString());

            // test reading non-symbolic link, path2 is a file
            path3.of("");
            Assert.assertFalse(Files.readLink(path2, path3));
            Assert.assertEquals(0, path3.size());

            // test non-existing file
            path2.trimTo(plen).concat("hello.txt");
            Assert.assertFalse(Files.readLink(path2, path3));
            Assert.assertEquals(0, path3.size());

            // test directory
            path2.trimTo(plen);
            Assert.assertFalse(Files.readLink(path2, path3));
            Assert.assertEquals(0, path3.size());
        }
    }

    @Test
    public void testTruncate() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            TestUtils.writeStringToFile(temp, "abcde");
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                Assert.assertTrue(Files.exists(path.$()));
                Assert.assertEquals(5, Files.length(path.$()));

                long fd = Files.openRW(path.$());
                try {
                    Files.truncate(fd, 3);
                    Assert.assertEquals(3, Files.length(path.$()));
                    Files.truncate(fd, 0);
                    Assert.assertEquals(0, Files.length(path.$()));
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
                    Path path = new Path().of(folder.getAbsolutePath());
                    Path link = new Path().of(folder.getParentFile().toString()).concat("link")
            ) {
                Assert.assertTrue(Files.exists(path.$()));
                int pathType = Files.findType(Files.findFirst(path.$()));
                Assert.assertEquals(Files.DT_DIR, pathType);

                Assert.assertEquals(0, Files.softLink(path.$(), link.$()));
                Assert.assertTrue(Files.exists(link.$()));
                int linkType = Files.findType(Files.findFirst(link.$()));
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
                    Path coldRoot = new Path().of(srcPath).concat("S3").slash(); // does not exist yet
                    Path linkPath = new Path().of(coldRoot).concat(fileName).put(TableUtils.ATTACHABLE_DIR_MARKER)
            ) {
                createTempFile(srcPath, fileName, fileContent); // updates srcFilePath

                // create the soft link
                createSoftLink(coldRoot, srcPath, linkPath);

                // check contents are the same
                assertEqualsFileContent(linkPath, fileContent);

                // unlink soft link
                Assert.assertEquals(0, Files.unlink(linkPath.$()));

                // check original file still exists and contents are the same
                assertEqualsFileContent(srcPath, fileContent);

                // however the link no longer exists
                File link = new File(linkPath.toString());
                Assert.assertFalse(link.exists());
                Assert.assertFalse(link.canRead());
                Assert.assertEquals(-1, Files.openRO(linkPath.$()));
            }
        });
    }

    @Test
    public void testWriteFails() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();

            try (Path path = new Path().of(temp.getAbsolutePath())) {
                long fd1 = Files.openRW(path.$());
                long mem = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);

                long testValue = 0x1234567890ABCDEFL;
                Unsafe.getUnsafe().putLong(mem, testValue);
                long fileSize = (2L << 30) + 4096;

                try {
                    Files.truncate(fd1, fileSize);

                    Assert.assertEquals(8L, Files.write(fd1, mem, 8, 0));
                    Assert.assertEquals(-1L, Files.write(fd1, mem, -1, fileSize));
                    Assert.assertEquals(-1L, Files.write(-1, mem, 8, fileSize));

                } finally {
                    // Release mem, fd
                    Files.close(fd1);
                    Unsafe.free(mem, 8, MemoryTag.NATIVE_DEFAULT);

                    // Delete files
                    TestUtils.remove(path.$());
                }
            }
        });
    }

    @Test
    public void testWriteOver2GB() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();

            try (Path path = new Path().of(temp.getAbsolutePath())) {
                long fd1 = Files.openRW(path.$());
                long fd2 = Files.openRW(path.put(".2").$());
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
                    TestUtils.remove(path.$());
                    TestUtils.remove(path.of(temp.getAbsolutePath()).$());
                }
            }
        });
    }

    private static void assertEqualsFileContent(Path path, String fileContent) {
        final int buffSize = 2048;
        final long buffPtr = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);
        long fd = -1;
        try {
            fd = Files.openRO(path.$());
            Assert.assertTrue(Files.exists(fd));
            long size = Files.length(fd);
            if (size > buffSize) {
                throw new LogError("File is too big: " + path.$());
            }
            if (size < 0 || size != Files.read(fd, buffPtr, size, 0)) {
                throw new LogError(String.format(
                        "Cannot read %s [errno=%d, size=%d]",
                        path,
                        Os.errno(),
                        size
                ));
            }
            StringSink sink = Misc.getThreadLocalSink();
            Utf8s.utf8ToUtf16(buffPtr, buffPtr + size, sink);
            TestUtils.assertEquals(fileContent, sink);
        } finally {
            Files.close(fd);
            Unsafe.free(buffPtr, buffSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assumeIsNotTmpFs(String path) {
        // Assumption to skip a test if we run on 'tmpfs' filesystem.
        // Background: tmpfs doesn't support sparse files - when posix_fallocate() is called,
        // tmpfs immediately materializes the entire allocation in RAM rather than
        // just reserving space like ext4 and other filesystems do. This can cause
        // memory exhaustion for tests that allocate large files.
        if (!Os.isLinux()) {
            return;
        }
        if (Files.getFileSystemStatus(Path.getThreadLocal(path).$()) == Files.TMPFS_MAGIC) {
            throw new AssumptionViolatedException("Path is on tmpfs: " + path);
        }
    }

    private static void createSoftLink(Path coldRoot, Path srcFilePath, Path softLinkFilePath) {
        Assert.assertEquals(0, Files.mkdirs(coldRoot, 509));
        Assert.assertEquals(0, Files.softLink(srcFilePath.$(), softLinkFilePath.$()));
        Assert.assertTrue(Os.isWindows() || Files.isSoftLink(softLinkFilePath.$())); // TODO: isSoftLink is not working on Windows
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
        long fd = -1;
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
                Path path = new Path().of(pathName).concat("hello.").put(index).put(".txt");
                Path p2 = new Path().of(pathName)
        ) {
            long fd = -1;
            long mem = -1;
            long diskSize = ff.getDiskFreeSpace(p2.$());
            Assert.assertNotEquals(-1, diskSize);
            long fileSize = diskSize / 3 * 2;
            try {
                fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
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

                ff.remove(path.$());
            }
        } catch (Throwable e) {
            e.printStackTrace(System.out);
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
                    Path hardLinkFilePath = new Path().of(dbRoot.getAbsolutePath()).concat(fileName).put(".1")
            ) {
                createTempFile(srcFilePath, fileName, fileContent);

                // perform the hard link
                Assert.assertEquals(0, Files.hardLink(srcFilePath.$(), hardLinkFilePath.$()));

                // check content are the same
                assertEqualsFileContent(hardLinkFilePath, fileContent);

                // delete source file
                Assert.assertTrue(TestUtils.remove(srcFilePath.$()));

                // check linked file still exists and content are the same
                assertEqualsFileContent(hardLinkFilePath, fileContent);

                TestUtils.remove(srcFilePath.$());
                Assert.assertTrue(TestUtils.remove(hardLinkFilePath.$()));
            }
        });
    }

    private void assertLastModified(Path path, long t) throws IOException {
        File f = temporaryFolder.newFile();
        Assert.assertTrue(Files.touch(path.of(f.getAbsolutePath()).$()));
        Assert.assertTrue(Files.setLastModified(path.$(), t));
        Assert.assertEquals(t, Files.getLastModified(path.$()));
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
                    Path coldRoot = new Path().of(srcFilePath).concat("S3").slash();
                    Path softLinkRenamedFilePath = new Path().of(coldRoot).concat(fileName);
                    Path softLinkFilePath = new Path().of(softLinkRenamedFilePath).put(TableUtils.ATTACHABLE_DIR_MARKER)
            ) {
                createTempFile(srcFilePath, fileName, fileContent); // updates srcFilePath

                // create the soft link
                createSoftLink(coldRoot, srcFilePath, softLinkFilePath);

                // check contents are the same
                assertEqualsFileContent(softLinkFilePath, fileContent);

                // delete soft link
                Assert.assertTrue(TestUtils.remove(softLinkFilePath.$()));

                // check original file still exists and contents are the same
                assertEqualsFileContent(srcFilePath, fileContent);

                // create the soft link again
                createSoftLink(coldRoot, srcFilePath, softLinkFilePath);

                // rename the link
                Assert.assertEquals(0, Files.rename(softLinkFilePath.$(), softLinkRenamedFilePath.$()));

                // check contents are the same
                assertEqualsFileContent(softLinkRenamedFilePath, fileContent);

                // delete original file
                Assert.assertTrue(TestUtils.remove(srcFilePath.$()));

                // check that when listing the folder where the link is, we can actually find it
                File link = new File(softLinkRenamedFilePath.toString());
                File[] fileArray = link.getParentFile().listFiles();
                Assert.assertNotNull(fileArray);
                List<File> files = Arrays.asList(fileArray);
                Assert.assertEquals(fileName, files.get(0).getName());

                // however, OS checks do check the existence of the file pointed to
                Assert.assertFalse(link.exists());
                Assert.assertFalse(link.canRead());
                Assert.assertEquals(-1, Files.openRO(softLinkFilePath.$()));
                Assert.assertTrue(TestUtils.remove(softLinkRenamedFilePath.$()));
            }
        });
    }

    private boolean isDirOrSoftLinkDir(File basePath, String path) {
        try (Path p = new Path().of(new File(basePath, path).toString())) {
            return Files.isDirOrSoftLinkDir(p.$());
        }
    }

    private void setupPath(File baseDir, String scenario) throws IOException {
        // Under the base dir:
        //   - create dirs for any scenario ending in /
        //   - create symlinks for any scenario containing arrows (i.e. "LINK -> TARGET")
        //   - create files for any other scenario
        if (scenario.contains(" -> ")) {
            final String[] parts = scenario.split(" -> ");
            final String targetPathString = parts[1].replaceAll("/$", "");
            final File target = new File(baseDir, targetPathString);
            final File link = new File(baseDir, parts[0]);
            Assert.assertTrue(link.getParentFile().exists() || link.getParentFile().mkdirs());
            try (
                    Path targetPath = new Path().of(target.getAbsolutePath());
                    Path linkPath = new Path().of(link.getAbsolutePath())
            ) {
                Files.softLink(targetPath.$(), linkPath.$());
                Assert.assertTrue(
                        "Could not set up scenario: " + scenario,
                        Files.exists(linkPath.$()));
            }

        } else if (scenario.endsWith("/")) {
            final File file = new File(baseDir, scenario.replaceAll("/$", ""));
            Assert.assertTrue(file.mkdirs());
            Assert.assertTrue(
                    "Could not set up scenario: " + scenario,
                    file.exists());
        } else {
            final File file = new File(baseDir, scenario);
            Assert.assertTrue(file.getParentFile().exists() || file.getParentFile().mkdirs());
            touch(file);
            Assert.assertTrue(
                    "Could not set up scenario: " + scenario,
                    file.exists());
        }
    }
}
