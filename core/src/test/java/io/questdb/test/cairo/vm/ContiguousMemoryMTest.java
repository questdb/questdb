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

package io.questdb.test.cairo.vm;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class ContiguousMemoryMTest extends AbstractCairoTest {

    private static final Log LOG = LogFactory.getLog(ContiguousMemoryMTest.class);
    private final long _4M = 4 * 1024 * 1024;
    private final long _8M = 2 * _4M;
    private final Rnd rnd = new Rnd();

    @Test
    public void testBoolAppend() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 10_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putBool(rnd.nextBoolean());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertBool(rwMem, N);
            assertBool(roMem, N);
        });
    }

    @Test
    public void testBoolAppendAtOffset() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 4_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putBool(i, rnd.nextBoolean());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertBool(rwMem, N);
            assertBool(roMem, N);
        });
    }

    @Test
    public void testBoolRandomWrite() throws Exception {
        withMem((rwMem, roMem) -> {
            final long MAX = 4 * _8M;
            final int N = 1_000;
            for (int i = 0; i < N; i++) {
                rwMem.putBool(rnd.nextLong(MAX), rnd.nextBoolean());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertRandomBool(rwMem, MAX, N);
            assertRandomBool(roMem, MAX, N);
        });
    }

    @Test
    public void testByteAppend() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 10_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putByte(rnd.nextByte());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertByte(rwMem, N);
            assertByte(roMem, N);
        });
    }

    @Test
    public void testByteAppendAtOffset() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 4_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putByte(i, rnd.nextByte());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertByte(rwMem, N);
            assertByte(roMem, N);
        });
    }

    @Test
    public void testByteRandomWrite() throws Exception {
        withMem((rwMem, roMem) -> {
            final long MAX = 4 * _8M;
            final int N = 1_000;
            for (int i = 0; i < N; i++) {
                rwMem.putByte(rnd.nextLong(MAX), rnd.nextByte());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertRandomByte(rwMem, MAX, N);
            assertRandomByte(roMem, MAX, N);
        });
    }

    @Test
    public void testCMARWToleratesLengthFailureOnClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Path path = Path.getThreadLocal(root).concat("t.d");
            AtomicBoolean trigger = new AtomicBoolean();

            FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public long length(long fd) {
                    long r = Files.length(fd);
                    if (r < 0 || trigger.get()) {
                        throw CairoException.critical(Os.errno()).put("Checking file size failed");
                    }
                    return r;
                }
            };

            MemoryCMARW rwMem1 = Vm.getCMARWInstance(
                    ff,
                    path.$(),
                    Files.PAGE_SIZE,
                    (long) (Files.PAGE_SIZE * 3.5),
                    MemoryTag.MMAP_DEFAULT,
                    configuration.getWriterFileOpenOpts()
            );

            rwMem1.jumpTo((long) (Files.PAGE_SIZE * 2.5));
            trigger.set(true);
            rwMem1.close();

            rwMem1.of(TestFilesFacadeImpl.INSTANCE, path.$(), Files.PAGE_SIZE,
                    (long) (Files.PAGE_SIZE * 1.5),
                    MemoryTag.MMAP_DEFAULT);
            rwMem1.putLong(123);

            rwMem1.close();
        });
    }

    @Test
    public void testCharAppend() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 10_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putChar(rnd.nextChar());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertChar(rwMem, N);
            assertChar(roMem, N);
        });
    }

    @Test
    public void testCharAppendAtOffset() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 4_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putChar(i * 2, rnd.nextChar());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertChar(rwMem, N);
            assertChar(roMem, N);
        });
    }

    @Test
    public void testCharRandomWrite() throws Exception {
        withMem((rwMem, roMem) -> {
            final long MAX = 4 * _8M / 2;
            final int N = 1_000;
            for (int i = 0; i < N; i++) {
                rwMem.putChar(rnd.nextLong(MAX) * 2L, rnd.nextChar());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertRandomChar(rwMem, MAX, N);
            assertRandomChar(roMem, MAX, N);
        });
    }

    @Test
    public void testDoubleAppend() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 10_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putDouble(rnd.nextDouble());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertDouble(rwMem, N);
            assertDouble(roMem, N);
        });
    }

    @Test
    public void testDoubleAppendAtOffset() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 4_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putDouble(i * 8, rnd.nextDouble());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertDouble(rwMem, N);
            assertDouble(roMem, N);
        });
    }

    @Test
    public void testDoubleRandomWrite() throws Exception {
        withMem((rwMem, roMem) -> {
            final long MAX = 4 * _8M / Double.BYTES;
            final int N = 500;
            for (int i = 0; i < N; i++) {
                rwMem.putDouble(rnd.nextLong(MAX) * 8L, rnd.nextDouble());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertRandomDouble(rwMem, MAX, N);
            assertRandomDouble(roMem, MAX, N);
        });
    }

    @Test
    public void testFloatAppend() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 10_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putFloat(rnd.nextFloat());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertFloat(rwMem, N);
            assertFloat(roMem, N);
        });
    }

    @Test
    public void testFloatAppendAtOffset() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 4_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putFloat(i * 4, rnd.nextFloat());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertFloat(rwMem, N);
            assertFloat(roMem, N);
        });
    }

    @Test
    public void testFloatRandomWrite() throws Exception {
        withMem((rwMem, roMem) -> {
            final long MAX = 4 * _8M / Float.BYTES;
            final int N = 500;
            for (int i = 0; i < N; i++) {
                rwMem.putFloat(rnd.nextLong(MAX) * 4L, rnd.nextFloat());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertRandomFloat(rwMem, MAX, N);
            assertRandomFloat(roMem, MAX, N);
        });
    }

    @Test
    public void testForcedExtend() {
        FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        try (Path path = new Path().of(root).concat("tmp1")) {
            ff.touch(path.$());
            final long fd = TableUtils.openRW(ff, path.$(), LOG, configuration.getWriterFileOpenOpts());
            try (MemoryMARW mem = Vm.getCMARWInstance()) {
                mem.of(ff, fd, null, -1, MemoryTag.MMAP_DEFAULT);

                mem.extend(ff.getMapPageSize() * 2);

                mem.putLong(ff.getMapPageSize(), 999);
                Assert.assertEquals(999, mem.getLong(ff.getMapPageSize()));
            }
        }
    }

    @Test
    public void testIntAppend() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 10_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putInt(rnd.nextInt());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertInt(rwMem, N);
            assertInt(roMem, N);
        });
    }

    @Test
    public void testIntAppendAtOffset() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 4_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putInt(i * 4, rnd.nextInt());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertInt(rwMem, N);
            assertInt(roMem, N);
        });
    }

    @Test
    public void testIntRandomWrite() throws Exception {
        withMem((rwMem, roMem) -> {
            final long MAX = 4 * _8M / Integer.BYTES;
            final int N = 1_000;
            for (int i = 0; i < N; i++) {
                rwMem.putInt(rnd.nextLong(MAX) * 4L, rnd.nextInt());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertRandomInt(rwMem, MAX, N);
            assertRandomInt(roMem, MAX, N);
        });
    }

    @Test
    public void testJumpToSetAppendPosition() {
        FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        try (Path path = new Path().of(root).concat("tmp3")) {
            ff.touch(path.$());
            try {
                try (MemoryMARW mem = Vm.getCMARWInstance()) {
                    mem.of(ff, TableUtils.openRW(ff, path.$(), LOG, configuration.getWriterFileOpenOpts()), null, -1, MemoryTag.MMAP_DEFAULT);

                    mem.extend(ff.getMapPageSize() * 2);

                    mem.putLong(ff.getMapPageSize(), 999);
                    Assert.assertEquals(999, mem.getLong(ff.getMapPageSize()));

                    mem.jumpTo(1024);
                }

                Assert.assertEquals(Files.PAGE_SIZE, ff.length(path.$()));
            } finally {
                Assert.assertTrue(ff.removeQuiet(path.$()));
            }
        }
    }

    @Test
    public void testLong128Append() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 10_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putLong128(rnd.nextLong(), rnd.nextLong());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertLong128(rwMem, N);
            assertLong128(roMem, N);
        });
    }

    @Test
    public void testLong256Append1() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 1_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putLong256(
                        rnd.nextLong(),
                        rnd.nextLong(),
                        rnd.nextLong(),
                        rnd.nextLong()
                );
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertLong256(rwMem, N);
            assertLong256(roMem, N);
        });
    }

    @Test
    public void testLong256Append2() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 1_000_000;
            Long256Impl value = new Long256Impl();
            for (int i = 0; i < N; i++) {
                value.fromRnd(rnd);
                rwMem.putLong256(value);
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertLong256(rwMem, N);
            assertLong256(roMem, N);
        });
    }

    @Test
    public void testLong256Append3() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 1_000_000;
            StringSink sink = new StringSink();
            for (int i = 0; i < N; i++) {
                Numbers.appendLong256(
                        rnd.nextLong(),
                        rnd.nextLong(),
                        rnd.nextLong(),
                        rnd.nextLong(),
                        sink
                );
                rwMem.putLong256(sink);
                sink.clear();
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertLong256(rwMem, N);
            assertLong256(roMem, N);
        });
    }

    @Test
    public void testLong256AppendAtOffset1() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 1_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putLong256(
                        i * 32
                        , rnd.nextLong()
                        , rnd.nextLong()
                        , rnd.nextLong()
                        , rnd.nextLong()
                );
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertLong256(rwMem, N);
            assertLong256(roMem, N);
        });
    }

    @Test
    public void testLong256AppendAtOffset2() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 1_000_000;
            Long256Impl value = new Long256Impl();
            for (int i = 0; i < N; i++) {
                value.fromRnd(rnd);
                rwMem.putLong256(i * 32, value);
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertLong256(rwMem, N);
            assertLong256(roMem, N);
        });
    }

    @Test
    public void testLongAppend() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 10_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putLong(rnd.nextLong());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertLong(rwMem, N);
            assertLong(roMem, N);
        });
    }

    @Test
    public void testLongAppendAtOffset() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 4_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putLong(i * 8, rnd.nextLong());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertLong(rwMem, N);
            assertLong(roMem, N);
        });
    }

    @Test
    public void testLongRandomWrite() throws Exception {
        withMem((rwMem, roMem) -> {
            final long MAX = 4 * _8M / Long.BYTES;
            final int N = 1_000;
            for (int i = 0; i < N; i++) {
                rwMem.putLong(rnd.nextLong(MAX) * 8L, rnd.nextLong());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertRandomLong(rwMem, MAX, N);
            assertRandomLong(roMem, MAX, N);
        });
    }

    @Test
    public void testPageCountAPI() throws Exception {
        withMem(TestFilesFacadeImpl.INSTANCE.getMapPageSize(), 0, (rwMem, roMem) -> {

            Assert.assertEquals(0, roMem.getPageCount());
            // read-write memory will always have one page unless it is closed
            Assert.assertEquals(1, rwMem.getPageCount());

            final int N = 10_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putBool(rnd.nextBoolean());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertBool(rwMem, N);
            assertBool(roMem, N);

            Assert.assertEquals(1, roMem.getPageCount());
            Assert.assertEquals(1, rwMem.getPageCount());

        });
    }

    @Test
    public void testPageCountIsZeroAfterClose() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (final Path path = Path.getThreadLocal(root).concat("t.d")) {
                rnd.reset();
                MemoryMARW rwMem = Vm.getCMARWInstance(
                        ff,
                        path.$(),
                        ff.getMapPageSize(),
                        0,
                        MemoryTag.MMAP_DEFAULT,
                        configuration.getWriterFileOpenOpts()
                );
                rwMem.close();
                Assert.assertEquals(0, rwMem.getPageCount());
            }
        });
    }

    @Test
    public void testShortAppend() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 10_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putShort(rnd.nextShort());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertShort(rwMem, N);
            assertShort(roMem, N);
        });
    }

    @Test
    public void testShortAppendAtOffset() throws Exception {
        withMem((rwMem, roMem) -> {
            final int N = 4_000_000;
            for (int i = 0; i < N; i++) {
                rwMem.putShort(i * 2, rnd.nextShort());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertShort(rwMem, N);
            assertShort(roMem, N);
        });
    }

    @Test
    public void testShortRandomWrite() throws Exception {
        withMem((rwMem, roMem) -> {
            final long MAX = 4 * _8M / Integer.BYTES;
            final int N = 1_000;
            for (int i = 0; i < N; i++) {
                rwMem.putShort(rnd.nextLong(MAX), rnd.nextShort());
            }

            roMem.extend(rwMem.size());

            // read these values back from
            assertRandomShort(rwMem, MAX, N);
            assertRandomShort(roMem, MAX, N);
        });
    }

    @Test
    public void testTruncate() {
        FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        try (Path path = new Path().of(root).concat("tmp1")) {
            ff.touch(path.$());
            try {
                try (MemoryMARW mem = Vm.getCMARWInstance()) {
                    mem.of(ff, path.$(), FilesFacadeImpl._16M, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                    // this is larger than page size
                    for (int i = 0; i < 3_000_000; i++) {
                        mem.putLong(i * 8, i + 1);
                    }

                    mem.truncate();
                    Assert.assertEquals(FilesFacadeImpl._16M, mem.size());
                    Assert.assertEquals(0, mem.getAppendOffset());
                }

                Assert.assertEquals(0, ff.length(path.$()));
            } finally {
                Assert.assertTrue(ff.removeQuiet(path.$()));
            }
        }
    }

    @Test
    public void testTruncateRemapFailed() {
        FilesFacade ff = new TestFilesFacadeImpl() {
            int counter = 1;
            boolean failTruncate = false;

            @Override
            public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
                if (--counter < 0) {
                    failTruncate = true;
                    return -1;
                }
                return super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
            }

            @Override
            public boolean truncate(long fd, long size) {
                if (failTruncate) {
                    return false;
                }
                return super.truncate(fd, size);
            }
        };

        try (Path path = new Path().of(root).concat("tmp4")) {
            ff.touch(path.$());
            try {
                try (MemoryMARW mem = Vm.getCMARWInstance()) {
                    mem.of(ff, path.$(), FilesFacadeImpl._16M, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                    // this is larger than page size
                    for (int i = 0; i < 3_000_000; i++) {
                        mem.putLong(i * 8, i + 1);
                    }

                    try {
                        mem.truncate();
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not remap file");
                    }
                }

                long fileLen = ff.length(path.$());
                Assert.assertNotEquals(0, fileLen);

                // we expect memory to zero out the file, which failed to truncate

                try (MemoryMR roMem = new MemoryCMRImpl(ff, path.$(), fileLen, MemoryTag.MMAP_DEFAULT)) {
                    Assert.assertEquals(fileLen, roMem.size());

                    for (int i = 0; i < fileLen; i++) {
                        Assert.assertEquals(0, roMem.getByte(i));
                    }
                }
            } finally {
                Assert.assertTrue(ff.removeQuiet(path.$()));
            }
        }
    }

    @Test
    public void testTruncateSameFileWithDifferentFd() {
        final Path path = Path.getThreadLocal(root).concat("t.d");

        MemoryCMARW rwMem1 = Vm.getCMARWInstance(
                TestFilesFacadeImpl.INSTANCE,
                path.$(),
                Files.PAGE_SIZE,
                (long) (Files.PAGE_SIZE * 1.5),
                MemoryTag.MMAP_DEFAULT,
                configuration.getWriterFileOpenOpts()
        );

        MemoryCMARW rwMem2 = Vm.getCMARWInstance(
                TestFilesFacadeImpl.INSTANCE,
                path.$(),
                Files.PAGE_SIZE,
                (long) (Files.PAGE_SIZE * 3.5),
                MemoryTag.MMAP_DEFAULT,
                configuration.getWriterFileOpenOpts()
        );

        rwMem1.close();

        rwMem2.jumpTo((long) (Files.PAGE_SIZE * 2.5));
        // Assert no errors on close
        rwMem2.close();
    }

    private void assertBool(MemoryR rwMem, int count) {
        rnd.reset();
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(rnd.nextBoolean(), rwMem.getBool(i));
        }
    }

    private void assertByte(MemoryR rwMem, int count) {
        rnd.reset();
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(rnd.nextByte(), rwMem.getByte(i));
        }
    }

    private void assertChar(MemoryR rwMem, int count) {
        rnd.reset();
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(rnd.nextChar(), rwMem.getChar(i * 2L));
        }
    }

    private void assertDouble(MemoryR rwMem, int count) {
        rnd.reset();
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(rnd.nextDouble(), rwMem.getDouble(i * 8L), 0.000001);
        }
    }

    private void assertFloat(MemoryR rwMem, int count) {
        rnd.reset();
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(rnd.nextFloat(), rwMem.getFloat(i * 4L), 0.000001);
        }
    }

    private void assertInt(MemoryR rwMem, int count) {
        rnd.reset();
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(rnd.nextInt(), rwMem.getInt(i * 4L));
        }
    }

    private void assertLong(MemoryR rwMem, int count) {
        rnd.reset();
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(rnd.nextLong(), rwMem.getLong(i * 8L));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void assertLong128(MemoryR rwMem, int count) {
        rnd.reset();
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(rnd.nextLong(), rwMem.getLong(i * 16L));
            Assert.assertEquals(rnd.nextLong(), rwMem.getLong(i * 16L + 8L));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void assertLong256(MemoryR rwMem, int count) {
        rnd.reset();
        for (int i = 0; i < count; i++) {
            long l0 = rnd.nextLong();
            long l1 = rnd.nextLong();
            long l2 = rnd.nextLong();
            long l3 = rnd.nextLong();
            Assert.assertEquals(l0, rwMem.getLong(i * 32L));
            Assert.assertEquals(l1, rwMem.getLong(i * 32L + 8L));
            Assert.assertEquals(l2, rwMem.getLong(i * 32L + 16L));
            Assert.assertEquals(l3, rwMem.getLong(i * 32L + 24L));

            Long256 valueA = rwMem.getLong256A(i * 32L);
            Assert.assertEquals(l0, valueA.getLong0());
            Assert.assertEquals(l1, valueA.getLong1());
            Assert.assertEquals(l2, valueA.getLong2());
            Assert.assertEquals(l3, valueA.getLong3());

            Long256 valueB = rwMem.getLong256B(i * 32L);
            Assert.assertEquals(l0, valueB.getLong0());
            Assert.assertEquals(l1, valueB.getLong1());
            Assert.assertEquals(l2, valueB.getLong2());
            Assert.assertEquals(l3, valueB.getLong3());
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void assertRandomBool(MemoryR rwMem, long MAX, int N) {
        rnd.reset();
        for (int i = 0; i < N; i++) {
            long offset = rnd.nextLong(MAX);
            Assert.assertEquals(rnd.nextBoolean(), rwMem.getBool(offset));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void assertRandomByte(MemoryR rwMem, long MAX, int N) {
        rnd.reset();
        for (int i = 0; i < N; i++) {
            long offset = rnd.nextLong(MAX);
            Assert.assertEquals(rnd.nextByte(), rwMem.getByte(offset));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void assertRandomChar(MemoryR rwMem, long MAX, int N) {
        rnd.reset();
        for (int i = 0; i < N; i++) {
            long offset = rnd.nextLong(MAX) * 2L;
            Assert.assertEquals(rnd.nextChar(), rwMem.getChar(offset));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void assertRandomDouble(MemoryR rwMem, long MAX, int N) {
        rnd.reset();
        for (int i = 0; i < N; i++) {
            long offset = rnd.nextLong(MAX) * 8L;
            Assert.assertEquals(rnd.nextDouble(), rwMem.getDouble(offset), 0.000001);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void assertRandomFloat(MemoryR rwMem, long MAX, int N) {
        rnd.reset();
        for (int i = 0; i < N; i++) {
            long offset = rnd.nextLong(MAX) * 4L;
            Assert.assertEquals(rnd.nextFloat(), rwMem.getFloat(offset), 0.000001);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void assertRandomInt(MemoryR rwMem, long MAX, int N) {
        rnd.reset();
        for (int i = 0; i < N; i++) {
            final long offset = rnd.nextLong(MAX) * 4L;
            Assert.assertEquals(rnd.nextInt(), rwMem.getInt(offset));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void assertRandomLong(MemoryR rwMem, long MAX, int N) {
        rnd.reset();
        for (int i = 0; i < N; i++) {
            long offset = rnd.nextLong(MAX) * 8L;
            Assert.assertEquals(rnd.nextLong(), rwMem.getLong(offset));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void assertRandomShort(MemoryR rwMem, long MAX, int N) {
        rnd.reset();
        for (int i = 0; i < N; i++) {
            long offset = rnd.nextLong(MAX);
            Assert.assertEquals(rnd.nextShort(), rwMem.getShort(offset));
        }
    }

    private void assertShort(MemoryR rwMem, int count) {
        rnd.reset();
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(rnd.nextShort(), rwMem.getShort(i * 2L));
        }
    }

    private void withMem(MemTestCode code) throws Exception {
        withMem(_4M, _4M, code);
    }

    private void withMem(long appendSz, long sz, MemTestCode code) throws Exception {
        assertMemoryLeak(() -> {
            final Path path = Path.getThreadLocal(root).concat("t.d");
            rnd.reset();
            try (
                    MemoryCMARW rwMem = Vm.getCMARWInstance(
                            TestFilesFacadeImpl.INSTANCE,
                            path.$(),
                            appendSz,
                            -1,
                            MemoryTag.MMAP_DEFAULT,
                            configuration.getWriterFileOpenOpts()
                    );
                    MemoryCMR roMem = new MemoryCMRImpl(
                            TestFilesFacadeImpl.INSTANCE,
                            path.$(),
                            sz,
                            MemoryTag.MMAP_DEFAULT
                    )
            ) {
                code.run(rwMem, roMem);
            } finally {
                Path.clearThreadLocals();
            }
        });

    }

    @FunctionalInterface
    private interface MemTestCode {
        void run(MemoryARW rwMem, MemoryR roMem);
    }
}
