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

package io.questdb.test.std.str;

import io.questdb.cairo.TableToken;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.*;
import io.questdb.std.str.*;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PathTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();
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
    public void testAsAsciiCharSequence() {
        try (Path p0 = new Path(1)) {
            p0.putAscii("foobar").$();
            Assert.assertTrue(p0.isAscii());
            TestUtils.assertEquals("foobar", p0.asAsciiCharSequence());
        }
    }

    @Test
    public void testCapacity() {
        try (Path p0 = new Path(4)) {
            Assert.assertEquals(4, p0.capacity());
            p0.putAscii("foobar").$();
            Assert.assertEquals(7, p0.capacity()); // 6 + 1
        }
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
        TestUtils.assertEquals("xyz" + Files.SEPARATOR + "123", path.of("xyz").concat("123").$());
        Assert.assertTrue(path.isAscii());
    }

    @Test
    public void testConcatNoSlashNonAscii() {
        TestUtils.assertEquals("xyz" + Files.SEPARATOR + "раздватри", path.of("xyz").concat("раздватри").$());
        Assert.assertFalse(path.isAscii());
    }

    @Test
    public void testConcatSlash() {
        TestUtils.assertEquals("xyz" + Files.SEPARATOR + "123", path.of("xyz").slash().concat("123").$());
    }

    @Test
    public void testConcatTableToken() {
        path.concat(new TableToken("root", "root", null, 0, false, false, false)).$();
        Assert.assertEquals("root", path.toString());
    }

    @Test
    public void testConcatUtf8Sequence() {
        path.concat(new Utf8String("root")).$();
        Assert.assertTrue(path.isAscii());
        Assert.assertEquals("root", path.toString());
    }

    @Test
    public void testConcatUtf8SequenceNonAscii() {
        path.concat(new Utf8String("грут")).$();
        Assert.assertFalse(path.isAscii());
        Assert.assertEquals("грут", path.toString());
    }

    @Test
    public void testConcatWithExtend() {
        try (
                Path p0 = new Path().put("sumerians");
                Path p1 = new Path(1)
        ) {
            p1.concat(p0.$().ptr());
            Assert.assertTrue(p0.isAscii());
            Assert.assertFalse(p1.isAscii());
            Assert.assertEquals(p0.toString(), p1.toString());
        }
    }

    @Test
    public void testConcatWithExtendNonAscii() {
        try (
                Path p0 = new Path().put("тест");
                Path p1 = new Path(1)
        ) {
            p1.concat(p0.$().ptr());
            Assert.assertFalse(p0.isAscii());
            Assert.assertFalse(p1.isAscii());
            Assert.assertEquals(p0.toString(), p1.toString());
        }
    }

    @Test
    public void testConcatWithSlash() {
        TestUtils.assertEquals("xyz" + Files.SEPARATOR + "123", path.of("xyz/").concat("123").$());
    }

    @Test
    public void testDollarAt() {
        String name = "header";
        try (Path p = new Path(1)) {
            p.put(name).concat("footer").flush();
            Assert.assertEquals(name + Files.SEPARATOR + "footer", p.toString());
            for (int i = name.length(); i < p.size(); i++) {
                p.$at(i);
            }
            Assert.assertEquals(name + "\u0000\u0000\u0000\u0000\u0000\u0000\u0000", p.toString());
            Assert.assertEquals(name.length() + 7, p.size());
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
    public void testHugeAppend() {
        final long threeGiB = 3L * 1024 * 1024 * 1024;
        final long src = 0;
        try {
            try (Path p0 = new Path()) {
                p0.putNonAscii(src, src + threeGiB);
                Assert.fail("Expected exception");
            }
        } catch (IllegalArgumentException iae) {
            TestUtils.assertContains(iae.getMessage(), "size exceeds 2GiB limit");
        } finally {
            Unsafe.free(src, threeGiB, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testLpszConcat() {
        try (Path p1 = new Path()) {
            p1.of("abc").concat("123").$();
            try (Path p = new Path()) {
                p.of("/xyz/").concat(p1.ptr()).$();
                Assert.assertFalse(p.isAscii());
                Assert.assertEquals(Files.SEPARATOR + "xyz" + Files.SEPARATOR + "abc" + Files.SEPARATOR + "123", p.toString());
            }
        }
    }

    @Test
    public void testOfAnotherPath() {
        try (Path p0 = new Path().of("root")) {
            path.of(p0).$();
            Assert.assertTrue(p0.isAscii());
            Assert.assertTrue(path.isAscii());
            Assert.assertEquals("root", path.toString());
        }
    }

    @Test
    public void testOfAnotherPathAsUtf8Sequence() {
        try (Path p0 = new Path().put("root")) {
            path.of((Utf8Sequence) p0);
            Assert.assertTrue(p0.isAscii());
            Assert.assertTrue(path.isAscii());
            Assert.assertEquals("root", p0.toString());
        }
    }

    @Test
    public void testOfAnotherPathNonAscii() {
        try (Path p0 = new Path().of("грут")) {
            path.of(p0).$();
            Assert.assertFalse(p0.isAscii());
            Assert.assertFalse(path.isAscii());
            Assert.assertEquals("грут", path.toString());
        }
    }

    @Test
    public void testOfCharSequence() {
        try (Path p0 = new Path().of("sumerians", 2, 7)) {
            Assert.assertEquals("meria", p0.toString());
            Assert.assertTrue(p0.isAscii());
        }
    }

    @Test
    public void testOfCharSequenceNonAscii() {
        try (Path p0 = new Path().of("грут", 1, 4)) {
            Assert.assertEquals("рут", p0.toString());
            Assert.assertFalse(p0.isAscii());
        }
    }

    @Test
    public void testOfSelf() {
        try (Path p0 = new Path().put("root")) {
            p0.flush();
            p0.of((Utf8Sequence) p0);
            Assert.assertTrue(p0.isAscii());
            Assert.assertEquals("root", p0.toString());
        }
    }

    @Test
    public void testOfSelfNonAscii() {
        try (Path p0 = new Path().put("грут")) {
            p0.flush();
            p0.of((Utf8Sequence) p0);
            Assert.assertFalse(p0.isAscii());
            Assert.assertEquals("грут", p0.toString());
        }
    }

    @Test
    public void testOverflow() {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 256; i++) {
            b.append('9');
        }

        try (Path p = new Path()) {
            TestUtils.assertEquals(
                    "9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999" + Files.SEPARATOR + "xyz",
                    p.of(b).concat("xyz").$()
            );
            Assert.assertTrue(p.isAscii());
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
    public void testPathOfPathNonAscii() {
        Os.init();

        path.of("пути неисповедимы");
        try (Path path2 = new Path()) {
            path2.of(path);
            Assert.assertFalse(path.isAscii());
            Assert.assertFalse(path2.isAscii());
            TestUtils.assertEquals(path, path2);

            // Reduce
            path.of("пути");
            path2.of(path);
            Assert.assertFalse(path2.isAscii());
            TestUtils.assertEquals(path, path2);

            // Extend
            path.of(Chars.repeat("пути неисповедимы", 50)).$();
            path2.of(path);
            Assert.assertFalse(path2.isAscii());
            TestUtils.assertEquals(path, path2);

            // Clear
            path.of("").$();
            path2.of(path);
            Assert.assertTrue(path2.isAscii());
            TestUtils.assertEquals(path, path2);

            // Destination closed
            path.of("1").$();
            path2.close();
            path2.of(path);
            Assert.assertTrue(path2.isAscii());
            TestUtils.assertEquals(path, path2);

            // Self copy
            path2.of(path2);
            Assert.assertTrue(path2.isAscii());
            TestUtils.assertEquals(path, path2);
        }
    }

    @Test
    public void testPrefix() {
        try (Path p0 = new Path(4).putAscii("foobar")) {
            path.of("baz").prefix(p0, p0.size()).$();
            Assert.assertTrue(p0.isAscii());
            Assert.assertTrue(path.isAscii());
            TestUtils.assertEquals("foobarbaz", path.toString());
        }
    }

    @Test
    public void testPrefixNonAscii() {
        try (Path p0 = new Path(4).put("раздва")) {
            path.of("три").prefix(p0, p0.size()).$();
            Assert.assertFalse(p0.isAscii());
            Assert.assertFalse(path.isAscii());
            TestUtils.assertEquals("раздватри", path.toString());
        }
    }

    @Test
    public void testPutDirectUtf8Sequence() {
        try (Path p0 = new Path(16); DirectUtf8Sink sink = new DirectUtf8Sink(32)) {
            Assert.assertEquals(16, p0.capacity());
            final String payload1 = "Moo: 🐄";
            sink.put(payload1);
            p0.put(sink);
            Assert.assertFalse(p0.isAscii());
            Assert.assertEquals(p0.capacity(), 16);
            Assert.assertEquals(payload1, p0.toString());
            final String payload2 = ", mooooooooooooooooooooo: 🐮!";
            sink.clear();
            sink.put(payload2);
            p0.put(sink);
            Assert.assertFalse(p0.isAscii());
            Assert.assertEquals(255, path.capacity());
            Assert.assertEquals(payload1 + payload2, p0.toString());
        }
    }

    @Test
    public void testPutPositioned() {
        path.of("foobar").$();
        path.put(0, (byte) 'b');
        Assert.assertFalse(path.isAscii());
        Assert.assertEquals("boobar", path.toString());
    }

    @Test
    public void testPutUtf8Sequence() {
        try (Path p0 = new Path(4)) {
            p0.put(new Utf8String("foobar")).$();
            Assert.assertTrue(p0.isAscii());
            Assert.assertEquals("foobar", p0.toString());
        }
    }

    @Test
    public void testPutUtf8SequenceNonAscii() {
        try (Path p0 = new Path(4)) {
            p0.put(new Utf8String("тест")).$();
            Assert.assertFalse(p0.isAscii());
            Assert.assertEquals("тест", p0.toString());
        }
    }

    @Test
    public void testPutWithExtension0() {
        try (Path p0 = new Path(1)) {
            p0.putAscii("sumerians".toCharArray(), 2, 5).$();
            Assert.assertTrue(p0.isAscii());
            Assert.assertEquals("meria", p0.toString());
        }
    }

    @Test
    public void testPutWithExtension1() {
        try (Path p0 = new Path(1)) {
            p0.put("sumerians", 2, 7).$();
            Assert.assertTrue(p0.isAscii());
            Assert.assertEquals("meria", p0.toString());
        }
    }

    @Test
    public void testSeekZ() {
        try (Path path = new Path()) {
            path.of("12345656788990").$();

            Assert.assertEquals(14, path.size());

            String inject = "hello\0";
            Utf8s.strCpyAscii(inject, 0, inject.length(), path.ptr());

            Assert.assertSame(path, path.seekZ());
            TestUtils.assertEquals("hello", path);

            path.concat("next");
            TestUtils.assertEquals("hello" + Files.SEPARATOR + "next", path);
        }
    }

    @Test
    public void testSimple() {
        TestUtils.assertEquals("xyz", path.of("xyz").$());
        Assert.assertTrue(path.isAscii());
    }

    @Test
    public void testThreadLocal() {
        String root = "" + Files.SEPARATOR;
        Path path = Path.getThreadLocal(root);
        path.concat("banana");
        Assert.assertEquals(7, path.size());
        Assert.assertEquals(Files.SEPARATOR + "banana", path.toString());
        path.$();
        Assert.assertEquals(7, path.size());
        Assert.assertEquals(Files.SEPARATOR + "banana", path.toString());
    }

    @Test
    public void testThreadLocalMultiThreaded() throws InterruptedException {
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

        try {
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
                            Assert.assertEquals(22, path.size());
                            Assert.assertFalse(Files.exists(path.$()));
                            path.concat("partition").slash$();
                            Assert.assertEquals(expected2, path.toString());
                            Assert.assertEquals(32, path.size());
                            AtomicLong count = stats.computeIfAbsent(threadId, k -> new AtomicLong());
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
            String root = "" + Files.SEPARATOR;
            String expected1 = root + "banana" + Files.SEPARATOR;
            String expected2 = expected1 + "party" + Files.SEPARATOR;
            for (int i = 0; i < 10; i++) {
                Path path = Path.getThreadLocal(root);
                path.concat("banana").slash$();
                Assert.assertEquals(expected1, path.toString());
                Assert.assertEquals(8, path.size());
                Assert.assertFalse(Files.exists(path.$()));
                path.concat("party").slash$();
                Assert.assertEquals(expected2, path.toString());
                Assert.assertEquals(14, path.size());
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
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.MINUTES));
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
    public void testTrimTo() {
        path.of("раз").$();
        int len = path.size();
        path.put("два").put("три").$();
        Assert.assertFalse(path.isAscii());
        Assert.assertEquals("раздватри", path.toString());
        path.trimTo(len);
        Assert.assertFalse(path.isAscii());
        Assert.assertEquals("раз", path.toString());
    }

    @Test
    public void testZeroEnd() throws Exception {
        File dir = temp.newFolder("a", "b", "c");
        File f = new File(dir, "f.txt");
        Assert.assertTrue(f.createNewFile());

        Assert.assertTrue(Files.exists(path.of(temp.getRoot().getAbsolutePath()).concat("a").concat("b").concat("c").concat("f.txt").$()));
    }

    @Test
    public void testZeroPad() {
        try (Path p0 = new Path(1).of("a")) {
            final int len = 16;
            p0.zeroPad(len);
            Assert.assertEquals(17, p0.capacity());
            for (int i = 0; i < len; i++) {
                Assert.assertEquals(0, Unsafe.getUnsafe().getByte(p0.hi() + i));
            }
        }
    }
}
