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

package io.questdb.test.std.str;

import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PathTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();
    private final char separator = System.getProperty("file.separator").charAt(0);
    private Path path;

    @Before
    public void setUp() {
        path = new Path();
    }

    @After
    public void tearDown() {
        path = Misc.free(path);
    }

    @Test
    public void testCheckClosed() {
        try (Path p0 = new Path().put("root")) {
            p0.close();
            p0.of("pigeon");
            Assert.assertEquals("pigeon", p0.toString());
        }
    }

    @Test
    public void testConcatNoSlash() {
        TestUtils.assertEquals("xyz" + separator + "123", path.of("xyz").concat("123").$());
    }

    @Test
    public void testConcatWithExtend() {
        try (Path p0 = new Path().put("sumerians").$();
             Path p1 = new Path(1)) {
            p1.concat(p0.address());
            Assert.assertEquals(p0.toString(), p1.toString());
        }
    }

    @Test
    public void testConcatWithSlash() {
        TestUtils.assertEquals("xyz" + separator + "123", path.of("xyz/").concat("123").$());
    }

    @Test
    public void testDollarAt() {
        String name = "header";
        try (Path p = new Path(1)) {
            p.put(name).concat("footer").flush();
            Assert.assertEquals(name + Files.SEPARATOR + "footer", p.toString());
            for (int i = name.length(); i < p.length(); i++) {
                p.$at(i);
            }
            Assert.assertEquals(name + "\u0000\u0000\u0000\u0000\u0000\u0000\u0000", p.toString());
            Assert.assertEquals(name.length() + 7, p.length());
        }
    }

    @Test
    public void testDollarIdempotent() {
        final CharSequence tableName = "table_name";
        final AtomicInteger extendCount = new AtomicInteger();
        try (Path path = new Path(1) {
            @Override
            public void extend(int len) {
                super.extend(len);
                extendCount.incrementAndGet();
            }
        }) {
            path.of(tableName).$();
            for (int i = 0; i < 5; i++) {
                path.$();
                Assert.assertEquals(1, extendCount.get());
            }
        }
    }

    @Test
    public void testLpszConcat() {
        try (Path p1 = new Path()) {
            p1.of("abc").concat("123").$();
            try (Path p = new Path()) {
                p.of("/xyz/").concat(p1.address()).$();
                Assert.assertEquals(separator + "xyz" + separator + "abc" + separator + "123", p.toString());
            }
        }
    }

    @Test
    public void testOfCharSequence() {
        try (Path p0 = new Path().of("sumerians", 2, 7).$()) {
            Assert.assertEquals("meria", p0.toString());
        }
    }

    @Test
    public void testOverflow() {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 256; i++) {
            b.append('9');
        }

        try (Path p = new Path()) {
            TestUtils.assertEquals("9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999" + System.getProperty("file.separator") + "xyz",
                    p.of(b).concat("xyz").$());
        }
    }

    @Test
    public void testParent() {
        try (
                Path path = new Path();
                Path expected = new Path()
        ) {
            Assert.assertEquals("", path.parent().toString());
            Assert.assertEquals("" + Files.SEPARATOR, path.put(Files.SEPARATOR).parent().toString());

            expected.concat("A").concat("B").concat("C").$();
            path.of(expected).concat("D").$();
            Assert.assertEquals(expected.toString(), path.parent().toString());
            path.of(expected).concat("D").slash$();
            Assert.assertEquals(expected.toString(), path.parent().toString());
        }
    }

    @Test
    public void testPathOfPathUtf8() {
        Os.init();

        path.of("пути неисповедимы");
        Path path2 = new Path();
        path2.of(path);
        TestUtils.assertEquals(path, path2);

        // Reduce
        path.of("пути");
        path2.of(path);
        TestUtils.assertEquals(path, path2);

        // Extend
        path.of(Chars.repeat("пути неисповедимы", 50)).$();
        path2.of(path);
        TestUtils.assertEquals(path, path2);

        // Clear
        path.of("").$();
        path2.of(path);
        TestUtils.assertEquals(path, path2);

        // Destination closed
        path.of("1").$();
        path2.close();
        path2.of(path);
        TestUtils.assertEquals(path, path2);

        // Self copy
        path2.of(path2);
        TestUtils.assertEquals(path, path2);
    }

    @Test
    public void testPathThreadLocalDoesNotAllocateOnRelease() {
        final long count = Unsafe.getMallocCount();
        Path.clearThreadLocals();
        Assert.assertEquals(count, Unsafe.getMallocCount());
    }

    @Test
    public void testPutWithExtension0() {
        try (Path p0 = new Path(1)) {
            p0.put("sumerians".toCharArray(), 2, 5);
            p0.$();
            Assert.assertEquals("meria", p0.toString());
        }
    }

    @Test
    public void testPutWithExtension1() {
        try (Path p0 = new Path(1)) {
            p0.put("sumerians", 2, 7);
            p0.$();
            Assert.assertEquals("meria", p0.toString());
        }
    }

    @Test
    public void testSeekZ() {
        try (Path path = new Path()) {
            path.of("12345656788990").$();

            Assert.assertEquals(14, path.length());

            String inject = "hello\0";
            Chars.asciiStrCpy(inject, 0, inject.length(), path.address());

            Assert.assertSame(path, path.seekZ());
            TestUtils.assertEquals("hello", path);

            path.concat("next");
            TestUtils.assertEquals("hello" + Files.SEPARATOR + "next", path);
        }
    }

    @Test
    public void testSelfPath() {
        try (Path p0 = new Path().put("root")) {
            p0.flush();
            p0.of((CharSequence) p0);
            Assert.assertEquals("root", p0.toString());
        }
    }

    @Test
    public void testSimple() {
        TestUtils.assertEquals("xyz", path.of("xyz").$());
    }

    @Test
    public void testThreadLocal() {
        String root = "" + Files.SEPARATOR;
        Path path = Path.getThreadLocal(root);
        path.concat("banana");
        Assert.assertEquals(7, path.length());
        Assert.assertEquals("" + Files.SEPARATOR + "banana", path.toString());
        path.$();
        Assert.assertEquals(7, path.length());
        Assert.assertEquals("" + Files.SEPARATOR + "banana", path.toString());
    }

    @Test
    public void testThreadLocalMultiThreaded() {
        int numThreads = 9;
        SOCountDownLatch started = new SOCountDownLatch(numThreads);
        SOCountDownLatch completed = new SOCountDownLatch(numThreads);
        AtomicBoolean keepRunning = new AtomicBoolean(true);
        AtomicInteger failCount = new AtomicInteger();
        ExecutorService executor;
        ConcurrentHashMap<Integer, AtomicLong> stats = new ConcurrentHashMap<>();
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        executor = Executors.newFixedThreadPool(numThreads, runnable -> {
            Thread thread = threadFactory.newThread(runnable);
            thread.setDaemon(true);
            return thread;
        });
        for (int i = 0; i < numThreads; i++) {
            int threadId = i;
            executor.submit(() -> {
                String threadName = "thread" + threadId;
                Thread.currentThread().setName(threadName);
                String root = Files.SEPARATOR + threadName + Files.SEPARATOR + "dbRoot"; // 15
                String expected1 = root + Files.SEPARATOR + "table" + Files.SEPARATOR; // 22
                String expected2 = expected1 + "partition" + Files.SEPARATOR; // 32
                started.countDown();
                try {
                    while (keepRunning.get()) {
                        Path path = Path.getThreadLocal(root);
                        path.concat("table").slash$();
                        Assert.assertEquals(expected1, path.toString());
                        Assert.assertEquals(22, path.length());
                        Assert.assertFalse(Files.exists(path));
                        path.concat("partition").slash$();
                        Assert.assertEquals(expected2, path.toString());
                        Assert.assertEquals(32, path.length());
                        AtomicLong count = stats.get(threadId);
                        if (count == null) {
                            stats.put(threadId, count = new AtomicLong());
                        }
                        count.incrementAndGet();
                        Os.pause();
                    }
                } catch (Throwable err) {
                    failCount.incrementAndGet();
                    err.printStackTrace();
                    Assert.fail(err.getMessage());
                } finally {
                    completed.countDown();
                    Path.clearThreadLocals();
                }
            });
        }
        started.await();

        try {
            String root = "" + Files.SEPARATOR;
            String expected1 = root + "banana" + Files.SEPARATOR;
            String expected2 = expected1 + "party" + Files.SEPARATOR;
            for (int i = 0; i < 10; i++) {
                Path path = Path.getThreadLocal(root);
                path.concat("banana").slash$();
                Assert.assertEquals(expected1, path.toString());
                Assert.assertEquals(8, path.length());
                Assert.assertFalse(Files.exists(path));
                path.concat("party").slash$();
                Assert.assertEquals(expected2, path.toString());
                Assert.assertEquals(14, path.length());
                Os.sleep(20L);
            }
        } finally {
            keepRunning.set(false);
            completed.await();
            executor.shutdown();
            Assert.assertEquals(0, failCount.get());
            for (int i = 0; i < numThreads; i++) {
                AtomicLong count = stats.get(i);
                Assert.assertNotNull(count);
                Assert.assertTrue(count.get() > 0);
            }
        }
    }

    @Test
    public void testToStringOfClosedPath() {
        try (Path p0 = new Path(1)) {
            p0.close();
            Assert.assertEquals("", p0.toString());
        }
    }

    @Test
    public void testZeroEnd() throws Exception {
        File dir = temp.newFolder("a", "b", "c");
        File f = new File(dir, "f.txt");
        Assert.assertTrue(f.createNewFile());

        Assert.assertTrue(Files.exists(path.of(temp.getRoot().getAbsolutePath()).concat("a").concat("b").concat("c").concat("f.txt").$()));
    }
}
