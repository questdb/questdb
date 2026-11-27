/*+*****************************************************************************
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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.cairo.vm.MemoryPARWImpl;
import io.questdb.cairo.vm.MemoryPMARImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryM;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8String;
import io.questdb.test.cairo.AllocationCountingFacade;
import io.questdb.test.cairo.TestRecord;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class MemoryPMARImplTest {
    private static final Log LOG = LogFactory.getLog(MemoryPMARImplTest.class);

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    /**
     * {@link MemoryPMARImpl#release(long)} msyncs completed pages per the commit mode threaded in via
     * {@code setCommitMode()}, not the instance-global mode, and falls back to the global configuration only for
     * {@link CommitMode#UNSET}. A {@code null} configuration (a NOSYNC global) isolates the threaded mode: with
     * SYNC threaded in, the completed pages are msync'd. UNSET and explicit NOSYNC skip the msync, ASYNC uses
     * {@code msync(async=true)}, and an adaptive {@code applyLazy} column skips it even with SYNC threaded in,
     * since the epoch and WAL replay make it durable.
     */
    @Test
    public void testReleaseHonorsThreadedCommitModeOverGlobalNosync() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = Files.PAGE_SIZE;
            // (a) SYNC threaded in, global NOSYNC (null config): completed pages msync.
            assertReleaseMsync(pageSize, CommitMode.SYNC, false, true, false);
            // (b) never threaded (UNSET) defers to the global mode, NOSYNC with a null config: no msync.
            assertReleaseMsync(pageSize, CommitMode.UNSET, false, false, false);
            // (c) NOSYNC threaded in: no msync.
            assertReleaseMsync(pageSize, CommitMode.NOSYNC, false, false, false);
            // (d) ASYNC threaded in: msync(async=true).
            assertReleaseMsync(pageSize, CommitMode.ASYNC, false, true, true);
            // (e) adaptive lazy-apply column still skips even with SYNC threaded in.
            assertReleaseMsync(pageSize, CommitMode.SYNC, true, false, false);
        });
    }

    // The memory holds the pattern up to the start of the put, which needs the next page. The page switch fails;
    // a jump back to the start and the same put write the value; close truncates the file at its end.
    private static void assertFailedPageSwitchPut(
            AllocationCountingFacade ff,
            long pattern,
            PutCase putCase,
            long left,
            int commitMode
    ) throws Exception {
        final long pageSize = Files.PAGE_SIZE;
        final long start = pageSize - left;
        final String context = putCase.name + " [left=" + left + ", commitMode=" + commitMode + ']';
        final File file = temp.newFile();
        try (Path path = new Path().of(file.getAbsolutePath())) {
            try (MemoryPMARImpl mem = new MemoryPMARImpl((CairoConfiguration) null)) {
                mem.of(ff, path.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                mem.setCommitMode(commitMode);
                mem.jumpTo(0);
                mem.putBlockOfBytes(pattern, start);
                ff.failingFd.set(mem.getFd());
                try {
                    putCase.put.accept(mem);
                    Assert.fail(context);
                } catch (CairoException e) {
                    TestUtils.assertContains(context, e.getFlyweightMessage(), "No space left");
                }
                Assert.assertEquals(context, -1, ff.failingFd.get());
                Assert.assertEquals(context, 0, mem.getPageAddress(0));
                if (left == 0) {
                    Assert.assertEquals(context, start, mem.getAppendOffset());
                }
                mem.jumpTo(start);
                putCase.put.accept(mem);
                Assert.assertEquals(context, start + putCase.value.length, mem.getAppendOffset());
                mem.close(true, Vm.TRUNCATE_TO_POINTER);
            }
        }
        final byte[] expected = new byte[(int) start + putCase.value.length];
        for (int i = 0; i < start; i++) {
            expected[i] = patternByte(i);
        }
        System.arraycopy(putCase.value, 0, expected, (int) start, putCase.value.length);
        Assert.assertArrayEquals(context, expected, java.nio.file.Files.readAllBytes(file.toPath()));
        Assert.assertTrue(file.delete());
    }

    // the value of a BINARY: its length, then its bytes
    private static byte[] binValue(byte[] bytes) {
        return le(Long.BYTES + bytes.length).putLong(bytes.length).put(bytes).array();
    }

    // a jump to a page that cannot be mapped: it releases the mapped page and leaves the append offset
    private static void failJump(MemoryPMARImpl mem, AllocationCountingFacade ff, long appendOffset) {
        ff.failingFd.set(mem.getFd());
        try {
            mem.jumpTo(3 * Files.PAGE_SIZE);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
        }
        Assert.assertEquals(-1, ff.failingFd.get());
        Assert.assertEquals(appendOffset, mem.getAppendOffset());
        Assert.assertEquals(0, mem.getPageAddress(0));
    }

    // a put of a long at the end of the page: the page switch cannot allocate the next page
    private static void failPut(MemoryPMARImpl mem, AllocationCountingFacade ff) {
        final long appendOffset = mem.getAppendOffset();
        ff.failingFd.set(mem.getFd());
        try {
            mem.putLong(-1);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
        }
        Assert.assertEquals(-1, ff.failingFd.get());
        Assert.assertEquals(appendOffset, mem.getAppendOffset());
        Assert.assertEquals(0, mem.getPageAddress(0));
    }

    // appends the first n bytes of the pattern
    private static void fillPage(MemoryPMARImpl mem, long n) {
        for (int i = 0; i < n; i++) {
            mem.putByte(patternByte(i));
        }
    }

    private static ByteBuffer le(int capacity) {
        return ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
    }

    private static byte patternByte(int offset) {
        return (byte) (offset * 7 + 3);
    }

    // the value of a STRING: its length in chars, then its UTF-16 chars
    private static byte[] utf16(String value) {
        final ByteBuffer buffer = le(Integer.BYTES + value.length() * Character.BYTES).putInt(value.length());
        for (int i = 0; i < value.length(); i++) {
            buffer.putChar(value.charAt(i));
        }
        return buffer.array();
    }

    private void assertReleaseMsync(
            long pageSize,
            int commitMode,
            boolean applyLazy,
            boolean expectMsync,
            boolean expectAsync
    ) throws Exception {
        final int[] msyncCount = {0};
        final boolean[] lastAsync = {false};
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public void msync(long addr, long len, boolean async) {
                msyncCount[0]++;
                lastAsync[0] = async;
                super.msync(addr, len, async);
            }
        };
        try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
            // null configuration => release()'s global fallback resolves to NOSYNC (a nosync instance).
            try (MemoryPMARImpl mem = new MemoryPMARImpl((CairoConfiguration) null)) {
                mem.of(ff, path.$(), pageSize, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE);
                mem.setApplyLazy(applyLazy);
                mem.setCommitMode(commitMode);
                // Cross >1 page boundary so completed pages are released (msync'd) during append; close()
                // then releases the final active page too.
                for (long i = 0, n = pageSize * 2 + 64; i < n; i += Long.BYTES) {
                    mem.putLong(i);
                }
                mem.close();
            }
        }
        final String ctx = " [commitMode=" + commitMode + ", applyLazy=" + applyLazy + ']';
        if (expectMsync) {
            Assert.assertTrue("expected >=1 release msync but got 0" + ctx, msyncCount[0] > 0);
            Assert.assertEquals("release msync async flag mismatch" + ctx, expectAsync, lastAsync[0]);
        } else {
            Assert.assertEquals("expected ZERO release msync" + ctx, 0, msyncCount[0]);
        }
    }

    @Test
    public void testFailedJumpKeepsAppendOffset() throws Exception {
        // A jump to another page that cannot map it releases the current page and stays where it was. A put
        // without a jump maps the page back and writes at the append offset, also an int across the page end.
        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = Files.PAGE_SIZE;
            final AllocationCountingFacade ff = new AllocationCountingFacade();
            try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
                try (MemoryPMARImpl mem = new MemoryPMARImpl(ff, path.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                    for (long i = 0, n = pageSize / Long.BYTES - 1; i < n; i++) {
                        mem.putLong(i);
                    }
                    mem.putShort((short) 7);
                    Assert.assertEquals(pageSize - 6, mem.getAppendOffset());
                    failJump(mem, ff, pageSize - 6);
                    // mid-page: the put maps the page and writes at the append offset, not at the page start
                    mem.putShort((short) 8);
                    mem.putShort((short) 9);
                    Assert.assertEquals(pageSize - 2, mem.getAppendOffset());
                    failJump(mem, ff, pageSize - 2);
                    // 2 bytes before the page end: the int crosses into the second page
                    mem.putInt(0x0A0B0C0D);
                    mem.putByte((byte) 10);
                    Assert.assertEquals(pageSize + 3, mem.getAppendOffset());
                }
                try (MemoryCMRImpl file = new MemoryCMRImpl(ff, path.$(), pageSize + 3, MemoryTag.MMAP_DEFAULT)) {
                    Assert.assertEquals(pageSize + 3, file.size());
                    Assert.assertEquals(pageSize / Long.BYTES - 2, file.getLong(pageSize - 16));
                    Assert.assertEquals(7, file.getShort(pageSize - 8));
                    Assert.assertEquals(8, file.getShort(pageSize - 6));
                    Assert.assertEquals(9, file.getShort(pageSize - 4));
                    Assert.assertEquals(0x0A0B0C0D, file.getInt(pageSize - 2));
                    Assert.assertEquals(10, file.getByte(pageSize + 2));
                }
            }
        });
    }

    @Test
    public void testFailedPageReleaseThenJumpBack() throws Exception {
        // The page switch msyncs the full page as it releases it, and the msync fails: release() unmaps the page all
        // the same, and nothing must point into it. A jump back into the page maps it again, and the put after the
        // jump writes into the file, not through the unmapped page.
        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = Files.PAGE_SIZE;
            final AtomicBoolean isFailing = new AtomicBoolean();
            final FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public void msync(long addr, long len, boolean async) {
                    if (isFailing.compareAndSet(true, false)) {
                        throw CairoException.critical(5).put("could not msync");
                    }
                    super.msync(addr, len, async);
                }
            };
            final File file = temp.newFile();
            try (Path path = new Path().of(file.getAbsolutePath())) {
                try (MemoryPMARImpl mem = new MemoryPMARImpl((CairoConfiguration) null)) {
                    mem.of(ff, path.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                    // the page release msyncs under every commit mode but NOSYNC
                    mem.setCommitMode(CommitMode.ASYNC);
                    fillPage(mem, pageSize - 3);
                    isFailing.set(true);
                    try {
                        mem.putLong(0x0102030405060708L);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not msync");
                    }
                    Assert.assertFalse(isFailing.get());
                    Assert.assertEquals(0, mem.getPageAddress(0));
                    Assert.assertEquals(pageSize, mem.getAppendOffset());
                    mem.jumpTo(pageSize - 3);
                    mem.putLong(0x1112131415161718L);
                    Assert.assertEquals(pageSize + 5, mem.getAppendOffset());
                    mem.close(true, Vm.TRUNCATE_TO_POINTER);
                }
            }
            // the value of the put after the jump replaces the 3 bytes the failed put wrote into the page
            final ByteBuffer expected = le((int) pageSize + 5);
            for (int i = 0; i < pageSize - 3; i++) {
                expected.put(patternByte(i));
            }
            expected.putLong(0x1112131415161718L);
            Assert.assertArrayEquals(expected.array(), java.nio.file.Files.readAllBytes(file.toPath()));
        });
    }

    @Test
    public void testFailedPageSwitchEveryPutKind() throws Exception {
        // Every kind of put that needs the next page, with no byte, one byte, or all bytes but one of its value left
        // in the page, under each commit mode the page release follows. The page switch fails; a jump back to the
        // start of the value and the same put then write the value whole. The file holds the bytes before the
        // value and the value, as a little-endian ByteBuffer models them.
        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = Files.PAGE_SIZE;
            final AllocationCountingFacade ff = new AllocationCountingFacade();
            final long pattern = Unsafe.malloc(pageSize, MemoryTag.NATIVE_DEFAULT);
            final byte[] bin = {1, -2, 3, -4, 5, -6, 7, -8, 9, -10, 11};
            final long binAddress = Unsafe.malloc(bin.length, MemoryTag.NATIVE_DEFAULT);
            final ObjList<DirectUtf8Sink> utf8Sinks = new ObjList<>();
            try {
                for (int i = 0; i < pageSize; i++) {
                    Unsafe.putByte(pattern + i, patternByte(i));
                }
                for (int i = 0; i < bin.length; i++) {
                    Unsafe.putByte(binAddress + i, bin[i]);
                }
                final ObjList<PutCase> cases = new ObjList<>();
                for (byte v : new byte[]{0, -1, Byte.MIN_VALUE, Byte.MAX_VALUE}) {
                    cases.add(new PutCase("putByte(" + v + ")", mem -> mem.putByte(v), new byte[]{v}));
                }
                cases.add(new PutCase("putBool(true)", mem -> mem.putBool(true), new byte[]{1}));
                cases.add(new PutCase("putBool(false)", mem -> mem.putBool(false), new byte[]{0}));
                for (short v : new short[]{0, -1, Short.MIN_VALUE, Short.MAX_VALUE}) {
                    cases.add(new PutCase("putShort(" + v + ")", mem -> mem.putShort(v), le(2).putShort(v).array()));
                }
                for (char v : new char[]{0, 'a', '\u20ac', Character.MAX_VALUE}) {
                    cases.add(new PutCase("putChar(" + (int) v + ")", mem -> mem.putChar(v), le(2).putChar(v).array()));
                }
                for (int v : new int[]{0, -1, Integer.MIN_VALUE, Integer.MIN_VALUE + 1, Integer.MAX_VALUE}) {
                    cases.add(new PutCase("putInt(" + v + ")", mem -> mem.putInt(v), le(4).putInt(v).array()));
                }
                for (float v : new float[]{0f, -0f, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.MIN_VALUE, -Float.MAX_VALUE}) {
                    cases.add(new PutCase("putFloat(" + v + ")", mem -> mem.putFloat(v), le(4).putFloat(v).array()));
                }
                for (long v : new long[]{0, -1, Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MAX_VALUE}) {
                    cases.add(new PutCase("putLong(" + v + ")", mem -> mem.putLong(v), le(8).putLong(v).array()));
                }
                for (double v : new double[]{0d, -0d, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.MIN_VALUE, Double.MAX_VALUE}) {
                    cases.add(new PutCase("putDouble(" + v + ")", mem -> mem.putDouble(v), le(8).putDouble(v).array()));
                }
                cases.add(new PutCase("putLong128", mem -> mem.putLong128(Long.MIN_VALUE, -1), le(16).putLong(Long.MIN_VALUE).putLong(-1).array()));
                cases.add(new PutCase("putLong256", mem -> mem.putLong256(1, Long.MIN_VALUE, -1, Long.MAX_VALUE), le(32).putLong(1).putLong(Long.MIN_VALUE).putLong(-1).putLong(Long.MAX_VALUE).array()));
                final String hex = "1111111111111111222222222222222233333333333333334444444444444444";
                final BigInteger hexValue = new BigInteger(hex, 16);
                cases.add(new PutCase(
                        "putLong256(hex)",
                        mem -> mem.putLong256("0x" + hex),
                        le(32).putLong(hexValue.longValue())
                                .putLong(hexValue.shiftRight(64).longValue())
                                .putLong(hexValue.shiftRight(128).longValue())
                                .putLong(hexValue.shiftRight(192).longValue())
                                .array()
                ));
                cases.add(new PutCase("putLong256(0x05)", mem -> mem.putLong256("0x05"), le(32).putLong(5).putLong(0).putLong(0).putLong(0).array()));
                // the empty string is NULL: four LONG NULLs
                cases.add(new PutCase("putLong256('')", mem -> mem.putLong256(""), le(32).putLong(Long.MIN_VALUE).putLong(Long.MIN_VALUE).putLong(Long.MIN_VALUE).putLong(Long.MIN_VALUE).array()));
                cases.add(new PutCase("putDecimal128", mem -> mem.putDecimal128(Long.MAX_VALUE, -1), le(16).putLong(Long.MAX_VALUE).putLong(-1).array()));
                cases.add(new PutCase("putDecimal256", mem -> mem.putDecimal256(-1, 2, Long.MIN_VALUE, 4), le(32).putLong(-1).putLong(2).putLong(Long.MIN_VALUE).putLong(4).array()));
                for (String v : new String[]{"", "abc", "a\u20ac\uD83D\uDE00z"}) {
                    cases.add(new PutCase("putStr(" + v + ")", mem -> mem.putStr(v), utf16(v)));
                }
                cases.add(new PutCase("putStr(null)", mem -> mem.putStr((CharSequence) null), le(4).putInt(-1).array()));
                cases.add(new PutCase("putStr('q')", mem -> mem.putStr('q'), utf16("q")));
                // the NUL char is a NULL string
                cases.add(new PutCase("putStr(0)", mem -> mem.putStr((char) 0), le(4).putInt(-1).array()));
                // ASCII, and 2-, 3- and 4-byte UTF-8 sequences
                for (String v : new String[]{"plain", "\u00e9\u00e8", "\u20ac\u20ac", "\uD83D\uDE00", "a\u00e9\u20ac\uD83D\uDE00"}) {
                    final DirectUtf8Sink utf8 = new DirectUtf8Sink(16);
                    utf8Sinks.add(utf8);
                    utf8.put(v);
                    cases.add(new PutCase("putStrUtf8(" + v + ")", mem -> mem.putStrUtf8(utf8), utf16(v)));
                }
                final String varchar = "v\u00e9\u20ac\uD83D\uDE00";
                final Utf8String utf8Varchar = new Utf8String(varchar);
                cases.add(new PutCase("putVarchar", mem -> mem.putVarchar(utf8Varchar, 0, utf8Varchar.size()), varchar.getBytes(StandardCharsets.UTF_8)));
                final TestRecord.ArrayBinarySequence binarySequence = new TestRecord.ArrayBinarySequence().of(bin);
                cases.add(new PutCase("putBin(BinarySequence)", mem -> mem.putBin(binarySequence), binValue(bin)));
                cases.add(new PutCase("putBin(empty BinarySequence)", mem -> mem.putBin(new TestRecord.ArrayBinarySequence().of(new byte[0])), le(8).putLong(0).array()));
                cases.add(new PutCase("putBin(null)", mem -> mem.putBin((BinarySequence) null), le(8).putLong(-1).array()));
                cases.add(new PutCase("putBin(address, len)", mem -> mem.putBin(binAddress, bin.length), binValue(bin)));
                cases.add(new PutCase("putBin(address, -1)", mem -> mem.putBin(binAddress, -1), le(8).putLong(-1).array()));
                cases.add(new PutCase("putBlockOfBytes", mem -> mem.putBlockOfBytes(binAddress, bin.length), bin));
                cases.add(new PutCase("putNullStr", mem -> mem.putNullStr(), le(4).putInt(-1).array()));
                cases.add(new PutCase("putNullBin", mem -> mem.putNullBin(), le(8).putLong(-1).array()));
                cases.add(new PutCase("zeroMem", mem -> mem.zeroMem(13), new byte[13]));

                for (int i = 0, n = cases.size(); i < n; i++) {
                    final PutCase putCase = cases.getQuick(i);
                    final int width = putCase.value.length;
                    // the bytes of the value left in the page; 5 splits the first char of a string after its length
                    final TreeSet<Integer> lefts = new TreeSet<>(Arrays.asList(0, 1, width - 1, 5));
                    for (int left : lefts) {
                        if (left >= width || (left == 5 && !putCase.name.startsWith("putStr"))) {
                            continue;
                        }
                        for (int commitMode : new int[]{CommitMode.NOSYNC, CommitMode.ASYNC, CommitMode.SYNC}) {
                            assertFailedPageSwitchPut(ff, pattern, putCase, left, commitMode);
                        }
                    }
                }
            } finally {
                Misc.freeObjList(utf8Sinks);
                Unsafe.free(binAddress, bin.length, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(pattern, pageSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFailedPageSwitchKeepsAppendOffset() throws Exception {
        // The page switch releases the page before it maps the next one. When the mapping fails, nothing must
        // point into the released page: a cancel that jumps back into it, and the puts after it, map it again.
        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = Files.PAGE_SIZE;
            final AllocationCountingFacade ff = new AllocationCountingFacade();
            final long longsPerPage = pageSize / Long.BYTES;
            try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
                try (MemoryPMARImpl mem = new MemoryPMARImpl(ff, path.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                    for (long i = 0; i < longsPerPage; i++) {
                        mem.putLong(i);
                    }
                    ff.failingFd.set(mem.getFd());
                    try {
                        mem.putLong(-1);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
                    }
                    Assert.assertEquals(-1, ff.failingFd.get());
                    Assert.assertEquals(pageSize, mem.getAppendOffset());
                    Assert.assertEquals(0, mem.getPageAddress(0));
                    // fails again, and still leaves the append offset
                    ff.failingFd.set(mem.getFd());
                    try {
                        mem.putLong(-1);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
                    }
                    Assert.assertEquals(-1, ff.failingFd.get());
                    Assert.assertEquals(pageSize, mem.getAppendOffset());
                    // a row cancel moves back into the released page
                    mem.jumpTo(pageSize - Long.BYTES);
                    mem.putLong(100);
                    mem.putLong(101);
                    Assert.assertEquals(pageSize + Long.BYTES, mem.getAppendOffset());
                }
                try (MemoryCMRImpl file = new MemoryCMRImpl(ff, path.$(), pageSize + Long.BYTES, MemoryTag.MMAP_DEFAULT)) {
                    Assert.assertEquals(pageSize + Long.BYTES, file.size());
                    Assert.assertEquals(longsPerPage - 2, file.getLong(pageSize - 2 * Long.BYTES));
                    Assert.assertEquals(100, file.getLong(pageSize - Long.BYTES));
                    Assert.assertEquals(101, file.getLong(pageSize));
                }
            }
        });
    }

    @Test
    public void testFailedPageSwitchRepeatedThenJumpForward() throws Exception {
        // The page switch fails twice, then a jump forward past the next pages maps the page it jumps to. The
        // file holds zeros between the end of the first page and the value.
        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = Files.PAGE_SIZE;
            final AllocationCountingFacade ff = new AllocationCountingFacade();
            final File file = temp.newFile();
            try (Path path = new Path().of(file.getAbsolutePath())) {
                try (MemoryPMARImpl mem = new MemoryPMARImpl(ff, path.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                    fillPage(mem, pageSize);
                    failPut(mem, ff);
                    failPut(mem, ff);
                    Assert.assertEquals(pageSize, mem.getAppendOffset());
                    mem.jumpTo(3 * pageSize + 5);
                    mem.putLong(99);
                    Assert.assertEquals(3 * pageSize + 13, mem.getAppendOffset());
                    mem.close(true, Vm.TRUNCATE_TO_POINTER);
                }
            }
            final ByteBuffer expected = le((int) (3 * pageSize + 13));
            for (int i = 0; i < pageSize; i++) {
                expected.put(patternByte(i));
            }
            expected.position((int) (3 * pageSize + 5));
            expected.putLong(99);
            Assert.assertArrayEquals(expected.array(), java.nio.file.Files.readAllBytes(file.toPath()));
        });
    }

    @Test
    public void testFailedPageSwitchSyncNoMsync() throws Exception {
        // With no page mapped after a failed page switch, a sync has no page to msync, and a flush syncs the file
        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = Files.PAGE_SIZE;
            final AtomicInteger msyncCount = new AtomicInteger();
            final AtomicInteger fdatasyncCount = new AtomicInteger();
            final AllocationCountingFacade ff = new AllocationCountingFacade() {
                @Override
                public void fdatasync(long fd) {
                    fdatasyncCount.incrementAndGet();
                    super.fdatasync(fd);
                }

                @Override
                public void msync(long addr, long len, boolean async) {
                    msyncCount.incrementAndGet();
                    super.msync(addr, len, async);
                }
            };
            try (
                    Path path = new Path().of(temp.newFile().getAbsolutePath());
                    MemoryPMARImpl mem = new MemoryPMARImpl(ff, path.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)
            ) {
                fillPage(mem, pageSize);
                failPut(mem, ff);
                msyncCount.set(0);
                fdatasyncCount.set(0);
                mem.sync(false);
                Assert.assertEquals(0, msyncCount.get());
                mem.flushFile(false);
                Assert.assertEquals(0, msyncCount.get());
                Assert.assertEquals(1, fdatasyncCount.get());
            }
        });
    }

    @Test
    public void testFailedPageSwitchThenClose() throws Exception {
        // The page switch fails after the put wrote the bytes that fit in the page. The close truncates the file at
        // the append offset the memory kept: the end of the page.
        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = Files.PAGE_SIZE;
            final AllocationCountingFacade ff = new AllocationCountingFacade();
            final File file = temp.newFile();
            try (Path path = new Path().of(file.getAbsolutePath())) {
                try (MemoryPMARImpl mem = new MemoryPMARImpl(ff, path.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                    fillPage(mem, pageSize - 3);
                    ff.failingFd.set(mem.getFd());
                    try {
                        mem.putLong(0x0102030405060708L);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
                    }
                    Assert.assertEquals(-1, ff.failingFd.get());
                    Assert.assertEquals(pageSize, mem.getAppendOffset());
                    mem.close(true, Vm.TRUNCATE_TO_POINTER);
                }
            }
            final ByteBuffer expected = le((int) pageSize);
            for (int i = 0; i < pageSize - 3; i++) {
                expected.put(patternByte(i));
            }
            // the low 3 bytes of the long
            expected.put((byte) 0x08).put((byte) 0x07).put((byte) 0x06);
            Assert.assertArrayEquals(expected.array(), java.nio.file.Files.readAllBytes(file.toPath()));
        });
    }

    @Test
    public void testFailedPageSwitchThenTruncate() throws Exception {
        // a truncate after a failed page switch maps the first page again and appends from offset 0
        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = Files.PAGE_SIZE;
            final AllocationCountingFacade ff = new AllocationCountingFacade();
            final File file = temp.newFile();
            try (Path path = new Path().of(file.getAbsolutePath())) {
                try (MemoryPMARImpl mem = new MemoryPMARImpl(ff, path.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                    fillPage(mem, pageSize);
                    failPut(mem, ff);
                    mem.truncate();
                    Assert.assertEquals(0, mem.getAppendOffset());
                    mem.putLong(42);
                    Assert.assertEquals(Long.BYTES, mem.getAppendOffset());
                    mem.close(true, Vm.TRUNCATE_TO_POINTER);
                }
            }
            Assert.assertArrayEquals(le(8).putLong(42).array(), java.nio.file.Files.readAllBytes(file.toPath()));
        });
    }

    @Test
    public void testJumpChangesActivePage() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long pageSize = Files.PAGE_SIZE;
            ConcurrentLinkedQueue<Throwable> allErrors = new ConcurrentLinkedQueue<>();
            ObjList<Thread> threads = new ObjList<>();
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;

            for (int thread = 0; thread < 10; thread++) {
                Thread th = new Thread(() -> {

                    try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {

                        LOG.info().$(path).$();
                        try (MemoryPARWImpl mem = new MemoryPMARImpl(ff, path.$(), pageSize, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE)) {
                            long pos;

                            mem.jumpTo(0);
                            String value = "abcdef";
                            mem.putStr(value);
                            pos = mem.getAppendOffset();
                            Assert.assertEquals('f', mem.getChar(pos - 2));

                            mem.jumpTo(2 * pageSize);
                            mem.jumpTo(0);

                            Assert.assertEquals(0, mem.getAppendOffset());
                            Assert.assertEquals(0, mem.pageIndex(mem.getAppendOffset()));

                            long addr = ((MemoryM) mem).map(0, 4);
                            long pageAddress = mem.getPageAddress(0);

                            Assert.assertEquals(pageAddress, addr);
                        }
                    } catch (Throwable e) {
                        allErrors.add(e);
                    }
                });
                th.start();
                threads.add(th);
            }

            for (int i = 0, n = threads.size(); i < n; i++) {
                threads.getQuick(i).join();
            }

            if (!allErrors.isEmpty()) {
                throw new RuntimeException(allErrors.poll());
            }
        });
    }

    @Test
    public void testPageSwitchIoCalls() throws Exception {
        // A page costs one allocate() and one mmap(), and a page switch one munmap(). A failed page switch costs
        // the failed allocate(), and the put after it the allocate() and the mmap() of the page.
        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = Files.PAGE_SIZE;
            final AtomicInteger mmapCount = new AtomicInteger();
            final AtomicInteger munmapCount = new AtomicInteger();
            final AllocationCountingFacade ff = new AllocationCountingFacade() {
                @Override
                public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                    mmapCount.incrementAndGet();
                    return super.mmap(fd, len, offset, flags, memoryTag);
                }

                @Override
                public void munmap(long address, long size, int memoryTag) {
                    munmapCount.incrementAndGet();
                    super.munmap(address, size, memoryTag);
                }
            };
            try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
                try (MemoryPMARImpl mem = new MemoryPMARImpl(ff, path.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                    for (long i = 0, n = 5 * pageSize / Long.BYTES; i < n; i++) {
                        mem.putLong(i);
                    }
                    Assert.assertEquals(5, ff.allocations.get());
                    Assert.assertEquals(5, mmapCount.get());
                    Assert.assertEquals(4, munmapCount.get());
                }
                Assert.assertEquals(5, munmapCount.get());
            }

            ff.allocations.set(0);
            mmapCount.set(0);
            munmapCount.set(0);
            try (Path path = new Path().of(temp.newFile().getAbsolutePath())) {
                try (MemoryPMARImpl mem = new MemoryPMARImpl(ff, path.$(), pageSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                    fillPage(mem, pageSize);
                    Assert.assertEquals(1, ff.allocations.get());
                    Assert.assertEquals(1, mmapCount.get());
                    failPut(mem, ff);
                    Assert.assertEquals(2, ff.allocations.get());
                    Assert.assertEquals(1, mmapCount.get());
                    Assert.assertEquals(1, munmapCount.get());
                    mem.putLong(1);
                    Assert.assertEquals(3, ff.allocations.get());
                    Assert.assertEquals(2, mmapCount.get());
                    Assert.assertEquals(1, munmapCount.get());
                }
                Assert.assertEquals(2, munmapCount.get());
            }
        });
    }

    @Test
    public void testSyncMsyncsOnlyWhenNotAsync() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final AtomicInteger msyncs = new AtomicInteger();
            final FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public void msync(long addr, long len, boolean async) {
                    msyncs.incrementAndGet();
                    super.msync(addr, len, async);
                }
            };
            try (
                    Path path = new Path().of(temp.newFile().getAbsolutePath());
                    MemoryPMARImpl mem = new MemoryPMARImpl(ff, path.$(), Files.PAGE_SIZE, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)
            ) {
                mem.putLong(42);
                mem.sync(true);
                Assert.assertEquals(0, msyncs.get());
                mem.sync(false);
                Assert.assertEquals(1, msyncs.get());
            }
        });
    }

    private static class PutCase {
        final String name;
        final Consumer<MemoryPMARImpl> put;
        final byte[] value;

        PutCase(String name, Consumer<MemoryPMARImpl> put, byte[] value) {
            this.name = name;
            this.put = put;
            this.value = value;
        }
    }
}
