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

package io.questdb.test.tools;

import io.questdb.Bootstrap;
import io.questdb.Metrics;
import io.questdb.ServerMain;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cutlass.text.CopyRequestJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogRecord;
import io.questdb.mp.WorkerPool;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.ThreadLocal;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.*;
import io.questdb.test.QuestDBTestNode;
import io.questdb.test.cairo.LogRecordSinkAdapter;
import io.questdb.test.cairo.RecordCursorPrinter;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.griffin.CustomisableRunnable;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.std.Numbers.IPv4_NULL;
import static org.junit.Assert.assertNotNull;

public final class TestUtils {

    public static final RecordCursorPrinter printer = new RecordCursorPrinter();
    private static final RecordCursorPrinter printerWithTypes = new RecordCursorPrinter().withTypes(true);
    private final static ThreadLocal<StringSink> tlSink = new ThreadLocal<>(StringSink::new);

    private TestUtils() {
    }

    public static boolean areEqual(BinarySequence a, BinarySequence b) {
        if (a == b) return true;
        if (a == null || b == null) return false;

        if (a.length() != b.length()) return false;
        for (int i = 0; i < a.length(); i++) {
            if (a.byteAt(i) != b.byteAt(i)) return false;
        }
        return true;
    }

    public static void assertConnect(int fd, long sockAddr) {
        long rc = connect(fd, sockAddr);
        if (rc != 0) {
            Assert.fail("could not connect, errno=" + Os.errno());
        }
    }

    public static void assertConnect(NetworkFacade nf, int fd, long pSockAddr) {
        long rc = nf.connect(fd, pSockAddr);
        if (rc != 0) {
            Assert.fail("could not connect, errno=" + nf.errno());
        }
    }

    public static void assertConnectAddrInfo(int fd, long sockAddrInfo) {
        long rc = connectAddrInfo(fd, sockAddrInfo);
        if (rc != 0) {
            Assert.fail("could not connect, errno=" + Os.errno());
        }
    }

    public static void assertContains(String message, CharSequence actual, CharSequence expected) {
        // Assume that "" is contained in any string.
        if (expected.length() == 0) {
            return;
        }
        if (Chars.contains(actual, expected)) {
            return;
        }
        Assert.fail((message != null ? message + ": '" : "'") + actual + "' does not contain: " + expected);
    }

    public static void assertContains(CharSequence actual, CharSequence expected) {
        assertContains(null, actual, expected);
    }

    public static void assertCursor(CharSequence expected, RecordCursor cursor, RecordMetadata metadata, boolean header, MutableCharSink sink) {
        printCursor(cursor, metadata, header, sink, printer);
        assertEquals(expected, sink);
    }

    public static void assertEquals(
            RecordCursor cursorExpected,
            RecordMetadata metadataExpected,
            RecordCursor cursorActual,
            RecordMetadata metadataActual,
            boolean symbolsAsStrings
    ) {
        StringSink sink = getTlSink();
        assertEquals(metadataExpected, metadataActual, symbolsAsStrings);
        Record r = cursorExpected.getRecord();
        Record l = cursorActual.getRecord();
        final int timestampIndex = metadataActual.getTimestampIndex();

        long timestampValue = -1;
        HashMap<String, Integer> mapL = null;
        HashMap<String, Integer> mapR = null;
        AssertionError deferred = null;

        long rowIndex = 0;
        while (cursorExpected.hasNext()) {
            if (!cursorActual.hasNext()) {
                Assert.fail("Actual cursor does not have record at " + rowIndex);
            }
            rowIndex++;
            if (timestampValue != -1) {
                // we are or were stashing record with the same timestamp
                long tsL = l.getTimestamp(timestampIndex);
                long tsR = r.getTimestamp(timestampIndex);

                if (tsL == timestampValue && tsR == timestampValue) {
                    // store both records
                    addRecordToMap(sink, l, metadataActual, mapL, symbolsAsStrings);
                    addRecordToMap(sink, r, metadataExpected, mapR, symbolsAsStrings);
                    continue;
                }

                // check if we can bail out early because current record timestamps do not match
                if (tsL != tsR) {
                    throw new AssertionError(
                            String.format(
                                    "Row %d column %s[%s] %s. Expected %s but found %s",
                                    rowIndex,
                                    metadataActual.getColumnName(timestampIndex),
                                    ColumnType.TIMESTAMP,
                                    "timestamp mismatch",
                                    Timestamps.toUSecString(tsL),
                                    Timestamps.toUSecString(tsR)
                            )
                    );
                }

                // compare accumulated records
                try {
                    Assert.assertEquals(mapL, mapR);
                } catch (AssertionError ignore) {
                    throw deferred;
                }

                // something changed, reset the store
                timestampValue = -1;

                mapL.clear();
                mapR.clear();
            }
            try {
                assertColumnValues(metadataExpected, metadataActual, l, r, rowIndex, symbolsAsStrings);
            } catch (AssertionError e) {
                // Assertion error could be to do with unstable sort order,
                // lets try to eliminate this.
                if (timestampIndex == -1) {
                    // cannot do anything with tables without timestamp
                    throw e;
                } else {
                    long tsL = l.getTimestamp(timestampIndex);
                    long tsR = r.getTimestamp(timestampIndex);
                    if (tsL != tsR) {
                        // timestamps are not matching, bail out
                        throw e;
                    }

                    // will throw this later if our reordering doesn't work
                    deferred = e;

                    // this is first time we experienced this error
                    // do we have maps?
                    timestampValue = tsL;

                    if (mapL == null) {
                        mapL = new HashMap<>();
                        mapR = new HashMap<>();
                    }

                    // store both records
                    addRecordToMap(sink, l, metadataActual, mapL, symbolsAsStrings);
                    addRecordToMap(sink, r, metadataExpected, mapR, symbolsAsStrings);
                }
            }
        }

        Assert.assertFalse("Expected cursor misses record " + rowIndex, cursorActual.hasNext());
    }

    public static void assertEquals(File a, File b) {
        try (Path path = new Path()) {
            path.of(a.getAbsolutePath()).$();
            int fda = TestFilesFacadeImpl.INSTANCE.openRO(path);
            Assert.assertNotEquals(-1, fda);

            try {
                path.of(b.getAbsolutePath()).$();
                int fdb = TestFilesFacadeImpl.INSTANCE.openRO(path);
                Assert.assertNotEquals(-1, fdb);
                try {

                    Assert.assertEquals(Files.length(fda), Files.length(fdb));

                    long bufa = Unsafe.malloc(4096, MemoryTag.NATIVE_DEFAULT);
                    long bufb = Unsafe.malloc(4096, MemoryTag.NATIVE_DEFAULT);

                    long offset = 0;
                    try {

                        while (true) {
                            long reada = Files.read(fda, bufa, 4096, offset);
                            long readb = Files.read(fdb, bufb, 4096, offset);
                            Assert.assertEquals(reada, readb);

                            if (reada == 0) {
                                break;
                            }

                            offset += reada;

                            for (int i = 0; i < reada; i++) {
                                Assert.assertEquals(Unsafe.getUnsafe().getByte(bufa + i), Unsafe.getUnsafe().getByte(bufb + i));
                            }
                        }
                    } finally {
                        Unsafe.free(bufa, 4096, MemoryTag.NATIVE_DEFAULT);
                        Unsafe.free(bufb, 4096, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    TestFilesFacadeImpl.INSTANCE.close(fdb);
                }
            } finally {
                TestFilesFacadeImpl.INSTANCE.close(fda);
            }
        }
    }

    public static void assertEquals(File a, CharSequence actual) {
        try (Path path = new Path()) {
            path.of(a.getAbsolutePath()).$();
            int fda = TestFilesFacadeImpl.INSTANCE.openRO(path);
            Assert.assertNotEquals(-1, fda);

            try {
                long bufa = Unsafe.malloc(4096, MemoryTag.NATIVE_DEFAULT);
                final long str = toMemory(actual);
                long offset = 0;
                long strp = str;
                try {

                    while (true) {
                        long reada = Files.read(fda, bufa, 4096, offset);

                        if (reada == 0) {
                            break;
                        }

                        for (int i = 0; i < reada; i++) {
                            byte b = Unsafe.getUnsafe().getByte(bufa + i);
                            if (b == 13) {
                                continue;
                            }
                            byte bb = Unsafe.getUnsafe().getByte(strp);
                            strp++;
                            if (b != bb) {
                                Assert.fail(
                                        "expected: '" + (char) (bb) + "'(" + bb + ")" +
                                                ", actual: '" + (char) (b) + "'(" + b + ")" +
                                                ", at: " + (offset + i - 1));
                            }
                        }

                        offset += reada;
                    }

                    Assert.assertEquals(strp - str, actual.length());
                } finally {
                    Unsafe.free(bufa, 4096, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(str, actual.length(), MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                TestFilesFacadeImpl.INSTANCE.close(fda);
            }
        }
    }

    public static void assertEquals(CharSequence expected, Sinkable actual) {
        StringSink sink = getTlSink();
        actual.toSink(sink);
        assertEquals(null, expected, sink);
    }

    public static void assertEquals(CharSequence expected, Utf8Sequence actual) {
        StringSink sink = getTlSink();
        Utf8s.utf8ToUtf16(actual, sink);
        assertEquals(null, expected, sink);
    }

    public static void assertEquals(byte[] expected, Utf8Sequence actual) {
        if (expected == null && actual == null) {
            return;
        }

        if (expected != null && actual == null) {
            Assert.fail("Expected: \n`" + Arrays.toString(expected) + "`but have NULL");
        }

        if (expected == null) {
            Assert.fail("Expected: NULL but have \n`" + actual + "`\n");
        }

        if (expected.length != actual.size()) {
            Assert.fail("Expected size: " + expected.length + ", but have " + actual.size());
        }

        for (int i = 0; i < expected.length; i++) {
            if (expected[i] != actual.byteAt(i)) {
                Assert.fail("Expected byte: " + expected[i] + ", but have " + actual.byteAt(i) + " at index " + i);
            }
        }
    }

    public static void assertEquals(Utf8Sequence expected, Utf8Sequence actual) {
        StringSink sink = getTlSink();
        Utf8s.utf8ToUtf16(expected, sink);
        String expectedStr = sink.toString();
        sink.clear();
        Utf8s.utf8ToUtf16(actual, sink);
        assertEquals(null, expectedStr, sink);
    }

    public static void assertEquals(CharSequence expected, CharSequence actual) {
        assertEquals(null, expected, actual);
    }

    public static void assertEquals(String message, CharSequence expected, CharSequence actual) {
        if (expected == null && actual == null) {
            return;
        }

        if (expected != null && actual == null) {
            Assert.fail("Expected: \n`" + expected + "`but have NULL");
        }

        if (expected == null) {
            Assert.fail("Expected: NULL but have \n`" + actual + "`\n");
        }

        if (expected.length() != actual.length()) {
            Assert.assertEquals(message, expected, actual);
        }

        for (int i = 0; i < expected.length(); i++) {
            if (expected.charAt(i) != actual.charAt(i)) {
                Assert.assertEquals(message, expected, actual);
            }
        }
    }

    public static void assertEquals(BinarySequence bs, BinarySequence actBs, long actualLen) {
        if (bs == null) {
            Assert.assertNull(actBs);
            Assert.assertEquals(-1, actualLen);
        } else {
            Assert.assertEquals(bs.length(), actBs.length());
            Assert.assertEquals(bs.length(), actualLen);
            for (long l = 0, z = bs.length(); l < z; l++) {
                byte b1 = bs.byteAt(l);
                byte b2 = actBs.byteAt(l);
                if (b1 != b2) {
                    Assert.fail("Failed comparison at [" + l + "], expected: " + b1 + ", actual: " + b2);
                }
                Assert.assertEquals(bs.byteAt(l), actBs.byteAt(l));
            }
        }
    }

    public static void assertEquals(LongList expected, LongList actual) {
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            if (expected.getQuick(i) != actual.getQuick(i)) {
                Assert.assertEquals("index " + i, expected.getQuick(i), actual.getQuick(i));
            }
        }
    }

    public static void assertEquals(IntList expected, IntList actual) {
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            if (expected.getQuick(i) != actual.getQuick(i)) {
                Assert.assertEquals("index " + i, expected.getQuick(i), actual.getQuick(i));
            }
        }
    }

    public static <T> void assertEquals(ObjList<T> expected, ObjList<T> actual) {
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            if (expected.getQuick(i) != actual.getQuick(i)) {
                Assert.assertEquals("index " + i, expected.getQuick(i), actual.getQuick(i));
            }
        }
    }

    public static void assertEquals(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String expectedSql,
            String actualSql
    ) throws SqlException {
        try (
                RecordCursorFactory f1 = compiler.compile(expectedSql, sqlExecutionContext).getRecordCursorFactory();
                RecordCursorFactory f2 = compiler.compile(actualSql, sqlExecutionContext).getRecordCursorFactory();
                RecordCursor c1 = f1.getCursor(sqlExecutionContext);
                RecordCursor c2 = f2.getCursor(sqlExecutionContext)
        ) {
            assertEquals(c1, f1.getMetadata(), c2, f2.getMetadata(), true);
        }
    }

    public static void assertEqualsExactOrder(
            RecordCursor cursorExpected,
            RecordMetadata metadataExpected,
            RecordCursor cursorActual,
            RecordMetadata metadataActual,
            boolean symbolsAsStrings
    ) {
        assertEquals(metadataExpected, metadataActual, symbolsAsStrings);
        Record r = cursorExpected.getRecord();
        Record l = cursorActual.getRecord();
        long rowIndex = 0;
        while (cursorExpected.hasNext()) {
            if (!cursorActual.hasNext()) {
                Assert.fail("Actual cursor does not have record at " + rowIndex);
            }
            rowIndex++;
            assertColumnValues(metadataExpected, metadataActual, l, r, rowIndex, symbolsAsStrings);
        }

        Assert.assertFalse("Expected cursor misses record " + rowIndex, cursorActual.hasNext());
    }

    public static void assertEqualsExactOrder(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String expectedSql,
            String actualSql
    ) throws SqlException {
        try (
                RecordCursorFactory f1 = compiler.compile(expectedSql, sqlExecutionContext).getRecordCursorFactory();
                RecordCursorFactory f2 = compiler.compile(actualSql, sqlExecutionContext).getRecordCursorFactory();
                RecordCursor c1 = f1.getCursor(sqlExecutionContext);
                RecordCursor c2 = f2.getCursor(sqlExecutionContext)
        ) {
            assertEqualsExactOrder(c1, f1.getMetadata(), c2, f2.getMetadata(), true);
        }
    }

    public static void assertEqualsIgnoreCase(CharSequence expected, CharSequence actual) {
        assertEqualsIgnoreCase(null, expected, actual);
    }

    public static void assertEqualsIgnoreCase(String message, CharSequence expected, CharSequence actual) {
        if (expected == null && actual == null) {
            return;
        }

        if (expected != null && actual == null) {
            Assert.fail("Expected: \n`" + expected + "`but have NULL");
        }

        if (expected == null) {
            Assert.fail("Expected: NULL but have \n`" + actual + "`\n");
        }

        if (expected.length() != actual.length()) {
            Assert.assertEquals(message, expected, actual);
        }

        for (int i = 0; i < expected.length(); i++) {
            if (Character.toLowerCase(expected.charAt(i)) != Character.toLowerCase(actual.charAt(i))) {
                Assert.assertEquals(message, expected, actual);
            }
        }
    }

    public static void assertEventually(Runnable assertion) {
        assertEventually(assertion, 30);
    }

    public static void assertEventually(Runnable assertion, int timeoutSeconds) {
        long maxSleepingTimeMillis = 1000;
        long nextSleepingTimeMillis = 10;
        long startTime = System.nanoTime();
        long deadline = startTime + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        for (; ; ) {
            try {
                assertion.run();
                return;
            } catch (AssertionError error) {
                if (System.nanoTime() >= deadline) {
                    throw error;
                }
            }
            Os.sleep(nextSleepingTimeMillis);
            nextSleepingTimeMillis = Math.min(maxSleepingTimeMillis, nextSleepingTimeMillis << 1);
        }
    }

    public static void assertFileContentsEquals(Path expected, Path actual) throws IOException {
        try (BufferedInputStream expectedStream = new BufferedInputStream(new FileInputStream(expected.toString()));
             BufferedInputStream actualStream = new BufferedInputStream(new FileInputStream(actual.toString()))) {
            int byte1, byte2;
            long length = 0;
            do {
                length++;
                byte1 = expectedStream.read();
                byte2 = actualStream.read();
            } while (byte1 == byte2 && byte1 > 0);

            if (byte1 != byte2) {
                Assert.fail("Files are different at offset " + (length - 1));
            }
        }
    }

    public static void assertIndexBlockCapacity(CairoEngine engine, String tableName, String columnName) {

        engine.releaseAllReaders();
        TableToken tt = engine.verifyTableName(tableName);
        try (TableReader rdr = engine.getReader(tt)) {
            TableReaderMetadata metadata = rdr.getMetadata();
            int symIndex = metadata.getColumnIndex(columnName);

            if (metadata.isColumnIndexed(symIndex)) {
                int expectedCapacity = metadata.getIndexValueBlockCapacity(symIndex);

                for (int partitionIndex = 0; partitionIndex < rdr.getPartitionCount(); partitionIndex++) {
                    BitmapIndexReader bitmapIndexReader = rdr.getBitmapIndexReader(0, symIndex, BitmapIndexReader.DIR_BACKWARD);
                    Assert.assertEquals(expectedCapacity, bitmapIndexReader.getValueBlockCapacity() + 1);
                }
            }
        }
    }

    public static void assertMemoryLeak(LeakProneCode runnable) throws Exception {
        Path.clearThreadLocals();
        long mem = Unsafe.getMemUsed();
        long[] memoryUsageByTag = new long[MemoryTag.SIZE];
        for (int i = MemoryTag.MMAP_DEFAULT; i < MemoryTag.SIZE; i++) {
            memoryUsageByTag[i] = Unsafe.getMemUsedByTag(i);
        }

        Assert.assertTrue("Initial file unsafe mem should be >= 0", mem >= 0);
        long fileCount = Files.getOpenFileCount();
        String fileDebugInfo = Files.getOpenFdDebugInfo();
        Assert.assertTrue("Initial file count should be >= 0", fileCount >= 0);

        int addrInfoCount = Net.getAllocatedAddrInfoCount();
        Assert.assertTrue("Initial allocated addrinfo count should be >= 0", addrInfoCount >= 0);

        int sockAddrCount = Net.getAllocatedSockAddrCount();
        Assert.assertTrue("Initial allocated sockaddr count should be >= 0", sockAddrCount >= 0);

        runnable.run();
        Path.clearThreadLocals();
        if (fileCount != Files.getOpenFileCount()) {
            Assert.assertEquals("file descriptors, expected: " + fileDebugInfo + ", actual: " + Files.getOpenFdDebugInfo(), fileCount, Files.getOpenFileCount());
        }

        // Checks that the same tag used for allocation and freeing native memory
        long memAfter = Unsafe.getMemUsed();
        long memNativeSqlCompilerDiff = 0;
        Assert.assertTrue(memAfter > -1);
        if (mem != memAfter) {
            for (int i = MemoryTag.MMAP_DEFAULT; i < MemoryTag.SIZE; i++) {
                long actualMemByTag = Unsafe.getMemUsedByTag(i);
                if (memoryUsageByTag[i] != actualMemByTag) {
                    if (i != MemoryTag.NATIVE_SQL_COMPILER) {
                        Assert.assertEquals("Memory usage by tag: " + MemoryTag.nameOf(i) + ", difference: " + (actualMemByTag - memoryUsageByTag[i]), memoryUsageByTag[i], actualMemByTag);
                        Assert.assertTrue(actualMemByTag > -1);
                    } else {
                        // SqlCompiler memory is not released immediately as compilers are pooled
                        Assert.assertTrue(actualMemByTag >= memoryUsageByTag[i]);
                        memNativeSqlCompilerDiff = actualMemByTag - memoryUsageByTag[i];
                    }
                }
            }
            Assert.assertEquals(mem + memNativeSqlCompilerDiff, memAfter);
        }

        int addrInfoCountAfter = Net.getAllocatedAddrInfoCount();
        Assert.assertTrue(addrInfoCountAfter > -1);
        if (addrInfoCount != addrInfoCountAfter) {
            Assert.fail("AddrInfo allocation count before the test: " + addrInfoCount + ", after the test: " + addrInfoCountAfter);
        }

        int sockAddrCountAfter = Net.getAllocatedSockAddrCount();
        Assert.assertTrue(sockAddrCountAfter > -1);
        if (sockAddrCount != sockAddrCountAfter) {
            Assert.fail("SockAddr allocation count before the test: " + sockAddrCount + ", after the test: " + sockAddrCountAfter);
        }
    }

    public static void assertReader(CharSequence expected, TableReader reader, MutableCharSink sink) {
        assertCursor(
                expected,
                reader.getCursor(),
                reader.getMetadata(),
                true,
                sink
        );
    }

    public static void assertSql(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            CharSequence sql,
            MutableCharSink sink,
            CharSequence expected
    ) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            assertSql(compiler, sqlExecutionContext, sql, sink, expected);
        }
    }

    public static void assertSql(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            CharSequence sql,
            MutableCharSink sink,
            CharSequence expected
    ) throws SqlException {
        printSql(
                compiler,
                sqlExecutionContext,
                sql,
                sink
        );
        assertEquals(expected, sink);
    }

    public static void assertSqlCursors(CairoEngine engine, SqlExecutionContext sqlExecutionContext, CharSequence expected, CharSequence actual, Log log) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            assertSqlCursors(compiler, sqlExecutionContext, expected, actual, log);
        }
    }

    public static void assertSqlCursors(CairoEngine engine, SqlExecutionContext sqlExecutionContext, CharSequence expected, CharSequence actual, Log log, boolean symbolsAsStrings) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            assertSqlCursors(compiler, sqlExecutionContext, expected, actual, log, symbolsAsStrings);
        }
    }

    public static void assertSqlCursors(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, CharSequence expected, CharSequence actual, Log log) throws SqlException {
        assertSqlCursors(compiler, sqlExecutionContext, expected, actual, log, false);
    }

    public static void assertSqlCursors(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, CharSequence expected, CharSequence actual, Log log, boolean symbolsAsStrings) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(expected, sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursorFactory factory2 = compiler.compile(actual, sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor1 = factory.getCursor(sqlExecutionContext)) {
                    try (RecordCursor cursor2 = factory2.getCursor(sqlExecutionContext)) {
                        assertEquals(cursor1, factory.getMetadata(), cursor2, factory2.getMetadata(), symbolsAsStrings);
                    }
                } catch (AssertionError e) {
                    log.error().$(e).$();
                    try (RecordCursor expectedCursor = factory.getCursor(sqlExecutionContext)) {
                        try (RecordCursor actualCursor = factory2.getCursor(sqlExecutionContext)) {
                            log.xDebugW().$();

                            LogRecordSinkAdapter recordSinkAdapter = new LogRecordSinkAdapter();
                            LogRecord record = log.xDebugW().$("java.lang.AssertionError: expected:<");
                            printer.printHeaderNoNl(factory.getMetadata(), recordSinkAdapter.of(record));
                            record.$();
                            printer.print(expectedCursor, factory.getMetadata(), false, log);

                            record = log.xDebugW().$("> but was:<");
                            printer.printHeaderNoNl(factory2.getMetadata(), recordSinkAdapter.of(record));
                            record.$();

                            printer.print(actualCursor, factory2.getMetadata(), false, log);
                            log.xDebugW().$(">").$();
                        }
                    }
                    throw e;
                }
            }
        }
    }

    public static void assertSqlCursors(QuestDBTestNode node, ObjList<QuestDBTestNode> nodes, String expected, String actual, Log log, boolean symbolsAsStrings) throws SqlException {
        try (
                SqlCompiler compiler = node.getEngine().getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(expected, node.getSqlExecutionContext()).getRecordCursorFactory()
        ) {
            for (int i = 0, n = nodes.size(); i < n; i++) {
                final QuestDBTestNode dbNode = nodes.get(i);
                try (
                        SqlCompiler compiler2 = dbNode.getEngine().getSqlCompiler();
                        RecordCursorFactory factory2 = compiler2.compile(actual, dbNode.getSqlExecutionContext()).getRecordCursorFactory()
                ) {
                    try (RecordCursor cursor1 = factory.getCursor(node.getSqlExecutionContext())) {
                        try (RecordCursor cursor2 = factory2.getCursor(dbNode.getSqlExecutionContext())) {
                            assertEquals(cursor1, factory.getMetadata(), cursor2, factory2.getMetadata(), symbolsAsStrings);
                        }
                    } catch (AssertionError e) {
                        log.error().$(e).$();
                        try (RecordCursor expectedCursor = factory.getCursor(node.getSqlExecutionContext())) {
                            try (RecordCursor actualCursor = factory2.getCursor(dbNode.getSqlExecutionContext())) {
                                log.xDebugW().$();

                                LogRecordSinkAdapter recordSinkAdapter = new LogRecordSinkAdapter();
                                LogRecord record = log.xDebugW().$("java.lang.AssertionError: expected:<");
                                printer.printHeaderNoNl(factory.getMetadata(), recordSinkAdapter.of(record));
                                record.$();
                                printer.print(expectedCursor, factory.getMetadata(), false, log);

                                record = log.xDebugW().$("> but was:<");
                                printer.printHeaderNoNl(factory2.getMetadata(), recordSinkAdapter.of(record));
                                record.$();

                                printer.print(actualCursor, factory2.getMetadata(), false, log);
                                log.xDebugW().$(">").$();
                            }
                        }
                        throw e;
                    }
                }
            }
        }
    }

    public static void assertSqlWithTypes(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            CharSequence sql,
            MutableCharSink sink,
            CharSequence expected
    ) throws SqlException {
        printSqlWithTypes(
                compiler,
                sqlExecutionContext,
                sql,
                sink
        );
        assertEquals(expected, sink);
    }

    public static void await(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (Throwable ignore) {
        }
    }

    public static long connect(int fd, long sockAddr) {
        Assert.assertTrue(fd > -1);
        return Net.connect(fd, sockAddr);
    }

    public static long connectAddrInfo(int fd, long sockAddrInfo) {
        Assert.assertTrue(fd > -1);
        return Net.connectAddrInfo(fd, sockAddrInfo);
    }

    public static void copyDirectory(Path src, Path dst, int dirMode) {
        if (Files.mkdirs(dst, dirMode) != 0) {
            Assert.fail("Cannot create " + dst + ". Error: " + Os.errno());
        }
        FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        Assert.assertEquals(0, ff.copyRecursive(src, dst, dirMode));
    }

    public static void copyMimeTypes(String targetDir) throws IOException {
        try (InputStream stream = Files.class.getResourceAsStream("/io/questdb/site/conf/mime.types")) {
            Assert.assertNotNull(stream);
            final File target = new File(targetDir, "conf/mime.types");
            Assert.assertTrue(target.getParentFile().mkdirs());
            try (FileOutputStream fos = new FileOutputStream(target)) {
                byte[] buffer = new byte[1024 * 1204];
                int len;
                while ((len = stream.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
            }
        }
    }

    public static TableToken create(TableModel model, CairoEngine engine) {
        int tableId = (int) engine.getTableIdGenerator().getNextId();
        TableToken tableToken = engine.lockTableName(model.getTableName(), tableId, model.isWalEnabled());
        if (tableToken == null) {
            throw new RuntimeException("table already exists: " + model.getTableName());
        }
        TableUtils.createTable(
                engine.getConfiguration(),
                model.getMem(),
                model.getPath(),
                model,
                ColumnType.VERSION,
                tableId,
                tableToken.getDirName()
        );
        engine.registerTableToken(tableToken);
        if (model.isWalEnabled()) {
            engine.getTableSequencerAPI().registerTable(tableId, model, tableToken);
        }
        return tableToken;
    }

    public static void createPopulateTable(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            TableModel tableModel,
            int totalRows,
            String startDate,
            int partitionCount
    ) throws NumericException, SqlException {
        createPopulateTable(
                tableModel.getTableName(),
                compiler,
                sqlExecutionContext,
                tableModel,
                totalRows,
                startDate,
                partitionCount
        );
    }

    public static void createPopulateTable(
            CharSequence tableName,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            TableModel tableModel,
            int totalRows,
            String startDate,
            int partitionCount
    ) throws NumericException, SqlException {
        compiler.compile(
                createPopulateTableStmt(
                        tableName,
                        tableModel,
                        totalRows,
                        startDate,
                        partitionCount
                ),
                sqlExecutionContext
        );
    }

    public static String createPopulateTableStmt(
            CharSequence tableName,
            TableModel tableModel,
            int totalRows,
            String startDate,
            int partitionCount
    ) throws NumericException {
        long fromTimestamp = IntervalUtils.parseFloorPartialTimestamp(startDate);
        long increment = partitionIncrement(tableModel.getPartitionBy(), fromTimestamp, totalRows, partitionCount);
        if (PartitionBy.isPartitioned(tableModel.getPartitionBy())) {
            final PartitionBy.PartitionAddMethod partitionAddMethod = PartitionBy.getPartitionAddMethod(tableModel.getPartitionBy());
            assert partitionAddMethod != null;
            long toTs = partitionAddMethod.calculate(fromTimestamp, partitionCount) - fromTimestamp - Timestamps.SECOND_MICROS;
            increment = totalRows > 0 ? Math.max(toTs / totalRows, 1) : 0;
        }

        StringBuilder sql = new StringBuilder();
        StringBuilder indexes = new StringBuilder();
        sql.append("create table ").append(tableName).append(" as (").append(Misc.EOL).append("select").append(Misc.EOL);
        for (int i = 0; i < tableModel.getColumnCount(); i++) {
            int colType = ColumnType.tagOf(tableModel.getColumnType(i));
            CharSequence colName = tableModel.getColumnName(i);
            switch (colType) {
                case ColumnType.INT:
                    sql.append("cast(x as int) ").append(colName);
                    break;
                case ColumnType.STRING:
                    sql.append("CAST(x as STRING) ").append(colName);
                    break;
                case ColumnType.LONG:
                    sql.append("x ").append(colName);
                    break;
                case ColumnType.DOUBLE:
                    sql.append("x / 1000.0 ").append(colName);
                    break;
                case ColumnType.TIMESTAMP:
                    sql.append("CAST(").append(fromTimestamp).append("L AS TIMESTAMP) + x * ").append(increment).append("  ").append(colName);
                    break;
                case ColumnType.SYMBOL:
                    sql.append("rnd_symbol(4,4,4,2) ").append(colName);
                    if (tableModel.isIndexed(i)) {
                        indexes.append(",index(").append(colName).append(") ");
                    }
                    break;
                case ColumnType.BOOLEAN:
                    sql.append("rnd_boolean() ").append(colName);
                    break;
                case ColumnType.FLOAT:
                    sql.append("CAST(x / 1000.0 AS FLOAT) ").append(colName);
                    break;
                case ColumnType.DATE:
                    sql.append("CAST(").append(fromTimestamp).append("L AS DATE) ").append(colName);
                    break;
                case ColumnType.LONG256:
                    sql.append("CAST(x AS LONG256) ").append(colName);
                    break;
                case ColumnType.BYTE:
                    sql.append("CAST(x AS BYTE) ").append(colName);
                    break;
                case ColumnType.CHAR:
                    sql.append("CAST(x AS CHAR) ").append(colName);
                    break;
                case ColumnType.SHORT:
                    sql.append("CAST(x AS SHORT) ").append(colName);
                    break;
                case ColumnType.IPv4:
                    sql.append("CAST(x AS IPv4) ").append(colName);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            if (i < tableModel.getColumnCount() - 1) {
                sql.append("," + Misc.EOL);
            }
        }

        sql.append(Misc.EOL + "from long_sequence(").append(totalRows).append(")");
        sql.append(")" + Misc.EOL);
        sql.append(indexes);
        if (tableModel.getTimestampIndex() != -1) {
            CharSequence timestampCol = tableModel.getColumnName(tableModel.getTimestampIndex());
            sql.append(" timestamp(").append(timestampCol).append(")");
        }
        if (PartitionBy.isPartitioned(tableModel.getPartitionBy())) {
            sql.append(" Partition By ").append(PartitionBy.toString(tableModel.getPartitionBy()));
        }

        if (tableModel.isWalEnabled()) {
            sql.append(" WAL ");
        }
        return sql.toString();
    }

    public static SqlExecutionContext createSqlExecutionCtx(CairoEngine engine) {
        return new SqlExecutionContextImpl(engine, 1).with(engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(), null);
    }

    public static SqlExecutionContext createSqlExecutionCtx(CairoEngine engine, BindVariableService bindVariableService) {
        SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1);
        ctx.with(engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(), bindVariableService);
        return ctx;
    }

    public static SqlExecutionContextImpl createSqlExecutionCtx(CairoEngine engine, int workerCount) {
        return new SqlExecutionContextImpl(engine, workerCount).with(engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(), null);
    }

    public static void createTestPath(CharSequence root) {
        try (Path path = new Path().of(root).$()) {
            if (Files.exists(path)) {
                return;
            }
            Files.mkdirs(path.of(root).slash$(), 509);
        }
    }

    public static Timestamp createTimestamp(long epochMicros) {
        // constructor requires epoch millis
        Timestamp ts = new Timestamp(epochMicros / 1000);
        ts.setNanos((int) ((epochMicros % 1_000_000) * 1000));
        return ts;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public static void drainCursor(RecordCursor cursor) {
        while (cursor.hasNext()) {
        }
    }

    public static void drainTextImportJobQueue(CairoEngine engine) throws Exception {
        try (CopyRequestJob copyRequestJob = new CopyRequestJob(engine, 1)) {
            copyRequestJob.drain(0);
        }
    }

    public static void drainWalQueue(CairoEngine engine) {
        try (final ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 1, 1)) {
            walApplyJob.drain(0);
            new CheckWalTransactionsJob(engine).run(0);
            // run once again as there might be notifications to handle now
            walApplyJob.drain(0);
        }
    }

    public static boolean equals(CharSequence expected, CharSequence actual) {
        if (expected == null && actual == null) {
            return true;
        }

        if (expected == null || actual == null) {
            return false;
        }

        if (expected.length() != actual.length()) {
            return false;
        }

        for (int i = 0; i < expected.length(); i++) {
            if (expected.charAt(i) != actual.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    public static void execute(
            @Nullable WorkerPool pool,
            CustomisableRunnable runnable,
            CairoConfiguration configuration,
            Metrics metrics,
            Log log
    ) throws Exception {
        execute(pool, null, runnable, configuration, metrics, log);
    }

    public static void execute(
            @Nullable WorkerPool pool,
            WorkerPoolCallback poolCallback,
            CustomisableRunnable runnable,
            CairoConfiguration configuration,
            Metrics metrics,
            Log log
    ) throws Exception {
        final int workerCount = pool != null ? pool.getWorkerCount() : 1;
        try (
                final CairoEngine engine = new CairoEngine(configuration, metrics);
                final SqlCompiler compiler = engine.getSqlCompiler();
                final SqlExecutionContext sqlExecutionContext = createSqlExecutionCtx(engine, workerCount)
        ) {
            try {
                if (pool != null) {
                    if (poolCallback != null) {
                        poolCallback.setupJobs(engine);
                    }
                    setupWorkerPool(pool, engine);
                    pool.start(log);
                }

                runnable.run(engine, compiler, sqlExecutionContext);
            } finally {
                if (pool != null) {
                    pool.halt();
                }
            }
            Assert.assertEquals(0, engine.getBusyWriterCount());
            Assert.assertEquals(0, engine.getBusyReaderCount());
        }
    }

    public static void execute(
            @Nullable WorkerPool pool,
            CustomisableRunnable runner,
            CairoConfiguration configuration,
            Log log
    ) throws Exception {
        execute(pool, null, runner, configuration, Metrics.disabled(), log);
    }

    public static void execute(
            @Nullable WorkerPool pool,
            WorkerPoolCallback poolCallback,
            CustomisableRunnable runner,
            CairoConfiguration configuration,
            Log log
    ) throws Exception {
        execute(pool, poolCallback, runner, configuration, Metrics.disabled(), log);
    }

    @NotNull
    public static Rnd generateRandom(Log log) {
        return generateRandom(log, System.nanoTime(), System.currentTimeMillis());
    }

    @NotNull
    public static Rnd generateRandom(Log log, long s0, long s1) {
        if (log != null) {
            log.info().$("random seeds: ").$(s0).$("L, ").$(s1).$('L').$();
        }
        System.out.printf("random seeds: %dL, %dL%n", s0, s1);
        return new Rnd(s0, s1);
    }

    public static String getCsvRoot() {
        return getTestResourcePath("/csv");
    }

    public static int getJavaVersion() {
        String version = System.getProperty("java.version");
        if (version.startsWith("1.")) {
            version = version.substring(2, 3);
        } else {
            int dot = version.indexOf(".");
            if (dot != -1) {
                version = version.substring(0, dot);
            }
        }
        return Integer.parseInt(version);
    }

    public static String getResourcePath(String resourceName) {
        URL resource = ServerMain.class.getResource(resourceName);
        assertNotNull("Someone accidentally deleted resource " + resourceName + "?", resource);
        try {
            return Paths.get(resource.toURI()).toFile().getAbsolutePath();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Could not determine resource path", e);
        }
    }

    @NotNull
    public static NetworkFacade getSendDelayNetworkFacade(int startDelayDelayAfter) {
        return new NetworkFacadeImpl() {
            final AtomicInteger totalSent = new AtomicInteger();

            @Override
            public int sendRaw(int fd, long buffer, int bufferLen) {
                if (startDelayDelayAfter == 0) {
                    return super.sendRaw(fd, buffer, bufferLen);
                }

                int sentNow = totalSent.get();
                if (bufferLen > 0) {
                    if (sentNow >= startDelayDelayAfter) {
                        totalSent.set(0);
                        return 0;
                    }

                    int result = super.sendRaw(fd, buffer, Math.min(bufferLen, startDelayDelayAfter - sentNow));
                    totalSent.addAndGet(result);
                    return result;
                }
                return 0;
            }
        };
    }

    public static String[] getServerMainArgs(CharSequence root) {
        return new String[]{
                "-d",
                Chars.toString(root),
                Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION
        };
    }

    public static String getTestResourcePath(String resourceName) {
        URL resource = TestUtils.class.getResource(resourceName);
        assertNotNull("Someone accidentally deleted test resource " + resourceName + "?", resource);
        try {
            return Paths.get(resource.toURI()).toFile().getAbsolutePath();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Could not determine resource path", e);
        }
    }

    public static TableWriter getWriter(CairoEngine engine, CharSequence tableName) {
        return getWriter(engine, engine.verifyTableName(tableName));
    }

    public static TableWriter getWriter(CairoEngine engine, TableToken tableToken) {
        return engine.getWriter(tableToken, "test");
    }

    public static String insertFromSelectPopulateTableStmt(
            TableModel tableModel,
            int totalRows,
            String startDate,
            int partitionCount
    ) throws NumericException {
        long fromTimestamp = IntervalUtils.parseFloorPartialTimestamp(startDate);
        long increment = partitionIncrement(tableModel.getPartitionBy(), fromTimestamp, totalRows, partitionCount);

        StringBuilder insertFromSelect = new StringBuilder();
        insertFromSelect.append("INSERT INTO ").append(tableModel.getTableName()).append(" SELECT").append(Misc.EOL);
        for (int i = 0; i < tableModel.getColumnCount(); i++) {
            CharSequence colName = tableModel.getColumnName(i);
            switch (ColumnType.tagOf(tableModel.getColumnType(i))) {
                case ColumnType.INT:
                    insertFromSelect.append("CAST(x as INT) ").append(colName);
                    break;
                case ColumnType.STRING:
                    insertFromSelect.append("CAST(x as STRING) ").append(colName);
                    break;
                case ColumnType.LONG:
                    insertFromSelect.append("x ").append(colName);
                    break;
                case ColumnType.DOUBLE:
                    insertFromSelect.append("x / 1000.0 ").append(colName);
                    break;
                case ColumnType.TIMESTAMP:
                    insertFromSelect.append("CAST(").append(fromTimestamp).append("L AS TIMESTAMP) + x * ").append(increment).append("  ").append(colName);
                    break;
                case ColumnType.SYMBOL:
                    insertFromSelect.append("rnd_symbol(4,4,4,2) ").append(colName);
                    break;
                case ColumnType.BOOLEAN:
                    insertFromSelect.append("rnd_boolean() ").append(colName);
                    break;
                case ColumnType.FLOAT:
                    insertFromSelect.append("CAST(x / 1000.0 AS FLOAT) ").append(colName);
                    break;
                case ColumnType.DATE:
                    insertFromSelect.append("CAST(").append(fromTimestamp).append("L AS DATE) ").append(colName);
                    break;
                case ColumnType.LONG256:
                    insertFromSelect.append("CAST(x AS LONG256) ").append(colName);
                    break;
                case ColumnType.BYTE:
                    insertFromSelect.append("CAST(x AS BYTE) ").append(colName);
                    break;
                case ColumnType.CHAR:
                    insertFromSelect.append("CAST(x AS CHAR) ").append(colName);
                    break;
                case ColumnType.SHORT:
                    insertFromSelect.append("CAST(x AS SHORT) ").append(colName);
                    break;
                case ColumnType.UUID:
                    insertFromSelect.append("rnd_uuid4() ").append(colName);
                    break;
                case ColumnType.LONG128:
                    insertFromSelect.append("to_long128(x, 0) ").append(colName);
                    break;
                case ColumnType.IPv4:
                    insertFromSelect.append("CAST(x as IPv4) ").append(colName);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            if (i < tableModel.getColumnCount() - 1) {
                insertFromSelect.append("," + Misc.EOL);
            }
        }
        insertFromSelect.append(Misc.EOL + "FROM long_sequence(").append(totalRows).append(")");
        insertFromSelect.append(")" + Misc.EOL);
        return insertFromSelect.toString();
    }

    public static String ipv4ToString(int ip) {
        StringSink sink = getTlSink();
        Numbers.intToIPv4Sink(sink, ip);
        return sink.toString();
    }

    public static String ipv4ToString2(long ipAndBroadcast) {
        StringSink sink = getTlSink();
        Numbers.intToIPv4Sink(sink, (int) (ipAndBroadcast >> 32));
        sink.put('/');
        Numbers.intToIPv4Sink(sink, (int) (ipAndBroadcast));
        return sink.toString();
    }

    public static int maxDayOfMonth(int month) {
        switch (month) {
            case 1:
            case 3:
            case 5:
            case 7:
            case 8:
            case 10:
            case 12:
                return 31;
            case 2:
                return 28;
            case 4:
            case 6:
            case 9:
            case 11:
                return 30;
            default:
                throw new IllegalArgumentException("[1..12]");
        }
    }

    public static void messTxnUnallocated(FilesFacade ff, Path path, Rnd rnd, TableToken tableToken) {
        path.concat(tableToken).concat(TableUtils.TXN_FILE_NAME);
        try (MemoryMARW txFile = Vm.getCMARWInstance(
                ff,
                path.$(),
                Files.PAGE_SIZE,
                -1,
                MemoryTag.NATIVE_MIG_MMAP,
                CairoConfiguration.O_NONE
        )) {
            long version = txFile.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
            boolean isA = (version & 1L) == 0L;
            long baseOffset = isA ? txFile.getInt(TX_BASE_OFFSET_A_32) : txFile.getInt(TX_BASE_OFFSET_B_32);
            long start = baseOffset + TX_OFFSET_SEQ_TXN_64 + 8;
            long lim = baseOffset + TX_OFFSET_MAP_WRITER_COUNT_32;
            for (long i = start; i < lim; i++) {
                txFile.putByte(i, rnd.nextByte());
            }
            txFile.close(false);
        }
    }

    public static void printColumn(Record r, RecordMetadata m, int i, CharSink sink) {
        printColumn(r, m, i, sink, false, false);
    }

    public static void printColumn(Record r, RecordMetadata m, int i, CharSink sink, boolean printTypes) {
        printColumn(r, m, i, sink, false, printTypes);
    }

    public static void printColumn(Record r, RecordMetadata m, int i, CharSink sink, boolean symbolAsString, boolean printTypes) {
        printColumn(r, m, i, sink, symbolAsString, printTypes, null);
    }

    public static void printColumn(Record r, RecordMetadata m, int i, CharSink sink, boolean symbolAsString, boolean printTypes, String nullStringValue) {
        final int columnType = m.getColumnType(i);
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DATE:
                DateFormatUtils.appendDateTime(sink, r.getDate(i));
                break;
            case ColumnType.TIMESTAMP:
                TimestampFormatUtils.appendDateTimeUSec(sink, r.getTimestamp(i));
                break;
            case ColumnType.DOUBLE:
                sink.put(r.getDouble(i), Numbers.MAX_SCALE);
                break;
            case ColumnType.FLOAT:
                sink.put(r.getFloat(i), 4);
                break;
            case ColumnType.INT:
                sink.put(r.getInt(i));
                break;
            case ColumnType.NULL:
                sink.put("null");
                break;
            case ColumnType.STRING:
                if (!symbolAsString | m.getColumnType(i) != ColumnType.SYMBOL) {
                    sink.put(r.getStr(i));
                    break;
                } // Fall down to SYMBOL
            case ColumnType.SYMBOL:
                CharSequence sym = r.getSym(i);
                sink.put(sym != null ? sym : nullStringValue);
                break;
            case ColumnType.SHORT:
                sink.put(r.getShort(i));
                break;
            case ColumnType.CHAR:
                char c = r.getChar(i);
                if (c > 0) {
                    sink.put(c);
                }
                break;
            case ColumnType.LONG:
                sink.put(r.getLong(i));
                break;
            case ColumnType.GEOBYTE:
                putGeoHash(r.getGeoByte(i), ColumnType.getGeoHashBits(columnType), sink);
                break;
            case ColumnType.GEOSHORT:
                putGeoHash(r.getGeoShort(i), ColumnType.getGeoHashBits(columnType), sink);
                break;
            case ColumnType.GEOINT:
                putGeoHash(r.getGeoInt(i), ColumnType.getGeoHashBits(columnType), sink);
                break;
            case ColumnType.GEOLONG:
                putGeoHash(r.getGeoLong(i), ColumnType.getGeoHashBits(columnType), sink);
                break;
            case ColumnType.BYTE:
                sink.put(r.getByte(i));
                break;
            case ColumnType.BOOLEAN:
                sink.put(r.getBool(i));
                break;
            case ColumnType.BINARY:
                Chars.toSink(r.getBin(i), sink);
                break;
            case ColumnType.LONG256:
                r.getLong256(i, sink);
                break;
            case ColumnType.LONG128:
                // fall through
            case ColumnType.UUID:
                long hi = r.getLong128Hi(i);
                long lo = r.getLong128Lo(i);
                if (!Uuid.isNull(lo, hi)) {
                    Uuid uuid = new Uuid(lo, hi);
                    uuid.toSink(sink);
                }
                break;
            case ColumnType.IPv4: {
                final int val = r.getIPv4(i);
                if (val != IPv4_NULL) {
                    Numbers.intToIPv4Sink(sink, val);
                }
                break;
            }
            default:
                break;
        }
        if (printTypes) {
            int printColType = symbolAsString && ColumnType.isSymbol(columnType) ? ColumnType.STRING : columnType;
            sink.put(':').put(ColumnType.nameOf(printColType));
        }
    }

    public static void printCursor(RecordCursor cursor, RecordMetadata metadata, boolean header, MutableCharSink sink, RecordCursorPrinter printer) {
        sink.clear();
        printer.print(cursor, metadata, header, sink);
    }

    public static void printSql(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            CharSequence sql,
            MutableCharSink sink
    ) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            printSql(compiler, sqlExecutionContext, sql, sink);
        }
    }

    public static void printSql(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            CharSequence sql,
            MutableCharSink sink
    ) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                printCursor(cursor, factory.getMetadata(), true, sink, printer);
            }
        }
    }

    public static void printSqlWithTypes(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            CharSequence sql,
            MutableCharSink sink
    ) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                printCursor(cursor, factory.getMetadata(), true, sink, printerWithTypes);
            }
        }
    }

    public static void putUtf8(TableWriter.Row r, String s, int columnIndex, boolean symbol) {
        byte[] bytes = s.getBytes(Files.UTF_8);
        long len = bytes.length;
        long p = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(p + i, bytes[i]);
            }
            DirectUtf8String seq = new DirectUtf8String();
            seq.of(p, p + len);
            if (symbol) {
                r.putSymUtf8(columnIndex, seq, true);
            } else {
                r.putStrUtf8(columnIndex, seq, true);
            }
        } finally {
            Unsafe.free(p, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    public static StringSink putWithLeadingZeroIfNeeded(StringSink seq, int len, int value) {
        seq.clear(len);
        if (value < 10) {
            seq.put('0');
        }
        seq.put(value);
        return seq;
    }

    public static String readStringFromFile(File file) {
        try {
            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] buffer
                        = new byte[(int) fis.getChannel().size()];
                int totalRead = 0;
                int read;
                while (totalRead < buffer.length
                        && (read = fis.read(buffer, totalRead, buffer.length - totalRead)) > 0) {
                    totalRead += read;
                }
                return new String(buffer, Files.UTF_8);
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot read from " + file.getAbsolutePath(), e);
        }
    }

    public static void removeTestPath(CharSequence root) {
        final Path path = Path.getThreadLocal(root);
        FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        path.slash$();
        Assert.assertTrue("Test dir cleanup error", !ff.exists(path) || ff.rmdir(path.slash$()));
    }

    public static void setupWorkerPool(WorkerPool workerPool, CairoEngine cairoEngine) throws SqlException {
        O3Utils.setupWorkerPool(workerPool, cairoEngine, null);
    }

    public static long toMemory(CharSequence sequence) {
        long ptr = Unsafe.malloc(sequence.length(), MemoryTag.NATIVE_DEFAULT);
        Utf8s.strCpyAscii(sequence, sequence.length(), ptr);
        return ptr;
    }

    public static void txnPartitionConditionally(Path path, int txn) {
        if (txn > -1) {
            path.put('.').put(txn);
        }
    }

    public static void unchecked(CheckedRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static void unchecked(CheckedRunnable runnable, AtomicInteger failureCounter) {
        try {
            runnable.run();
        } catch (Throwable e) {
            failureCounter.incrementAndGet();
            throw new RuntimeException(e);
        }
    }

    public static <T> T unchecked(CheckedSupplier<T> runnable) {
        try {
            return runnable.get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static int unchecked(CheckedIntFunction runnable) {
        try {
            return runnable.get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeStringToFile(File file, String s) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(s.getBytes(Files.UTF_8));
        }
    }

    private static void assertColumnValues(
            RecordMetadata metadataExpected,
            RecordMetadata metadataActual,
            Record lr,
            Record rr,
            long rowIndex,
            boolean symbolsAsStrings
    ) {
        int columnType = 0;
        for (int i = 0, n = metadataExpected.getColumnCount(); i < n; i++) {
            String columnName = metadataExpected.getColumnName(i);
            try {
                columnType = metadataExpected.getColumnType(i);
                int tagType = ColumnType.tagOf(columnType);
                switch (tagType) {
                    case ColumnType.DATE:
                        Assert.assertEquals(rr.getDate(i), lr.getDate(i));
                        break;
                    case ColumnType.TIMESTAMP:
                        if (rr.getTimestamp(i) != lr.getTimestamp(i)) {
                            Assert.assertEquals(Timestamps.toString(rr.getTimestamp(i)), Timestamps.toString(lr.getTimestamp(i)));
                        }
                        break;
                    case ColumnType.DOUBLE:
                        Assert.assertEquals(rr.getDouble(i), lr.getDouble(i), 1E-6);
                        break;
                    case ColumnType.FLOAT:
                        Assert.assertEquals(rr.getFloat(i), lr.getFloat(i), 1E-4);
                        break;
                    case ColumnType.INT:
                        Assert.assertEquals(rr.getInt(i), lr.getInt(i));
                        break;
                    case ColumnType.IPv4:
                        Assert.assertEquals(rr.getIPv4(i), lr.getIPv4(i));
                        break;
                    case ColumnType.GEOINT:
                        Assert.assertEquals(rr.getGeoInt(i), lr.getGeoInt(i));
                        break;
                    case ColumnType.STRING:
                        CharSequence actual = symbolsAsStrings && ColumnType.isSymbol(metadataActual.getColumnType(i)) ? lr.getSym(i) : lr.getStr(i);
                        CharSequence expected = rr.getStr(i);
                        TestUtils.assertEquals(expected, actual);
                        break;
                    case ColumnType.SYMBOL:
                        Assert.assertEquals(rr.getSym(i), lr.getSym(i));
                        break;
                    case ColumnType.SHORT:
                        Assert.assertEquals(rr.getShort(i), lr.getShort(i));
                        break;
                    case ColumnType.CHAR:
                        Assert.assertEquals(rr.getChar(i), lr.getChar(i));
                        break;
                    case ColumnType.GEOSHORT:
                        Assert.assertEquals(rr.getGeoShort(i), lr.getGeoShort(i));
                        break;
                    case ColumnType.LONG:
                        Assert.assertEquals(rr.getLong(i), lr.getLong(i));
                        break;
                    case ColumnType.GEOLONG:
                        Assert.assertEquals(rr.getGeoLong(i), lr.getGeoLong(i));
                        break;
                    case ColumnType.GEOBYTE:
                        Assert.assertEquals(rr.getGeoByte(i), lr.getGeoByte(i));
                        break;
                    case ColumnType.BYTE:
                        Assert.assertEquals(rr.getByte(i), lr.getByte(i));
                        break;
                    case ColumnType.BOOLEAN:
                        Assert.assertEquals(rr.getBool(i), lr.getBool(i));
                        break;
                    case ColumnType.BINARY:
                        Assert.assertTrue(areEqual(rr.getBin(i), lr.getBin(i)));
                        break;
                    case ColumnType.LONG256:
                        assertEquals(rr.getLong256A(i), lr.getLong256A(i));
                        break;
                    case ColumnType.LONG128:
                        // fall-through
                    case ColumnType.UUID:
                        Assert.assertEquals(rr.getLong128Hi(i), lr.getLong128Hi(i));
                        Assert.assertEquals(rr.getLong128Lo(i), lr.getLong128Lo(i));
                        break;
                    default:
                        // Unknown record type.
                        assert false;
                        break;
                }
            } catch (AssertionError e) {
                String expected = recordToString(rr, metadataExpected, symbolsAsStrings);
                String actual = recordToString(lr, metadataActual, symbolsAsStrings);
                Assert.assertEquals(
                        String.format(String.format("Row %d column %s[%s]", rowIndex, columnName, ColumnType.nameOf(columnType))),
                        expected,
                        actual
                );
                // If above didn't fail because of types not included or double precision not enough, throw here anyway
                throw new AssertionError(String.format("Row %d column %s[%s] %s", rowIndex, columnName, ColumnType.nameOf(columnType), e.getMessage()));
            }
        }
    }

    private static void assertEquals(RecordMetadata metadataExpected, RecordMetadata metadataActual, boolean symbolsAsStrings) {
        Assert.assertEquals("Column count must be same", metadataExpected.getColumnCount(), metadataActual.getColumnCount());
        for (int i = 0, n = metadataExpected.getColumnCount(); i < n; i++) {
            Assert.assertEquals("Column name " + i, metadataExpected.getColumnName(i), metadataActual.getColumnName(i));
            int columnType1 = metadataExpected.getColumnType(i);
            columnType1 = symbolsAsStrings && ColumnType.isSymbol(columnType1) ? ColumnType.STRING : columnType1;
            int columnType2 = metadataActual.getColumnType(i);
            columnType2 = symbolsAsStrings && ColumnType.isSymbol(columnType2) ? ColumnType.STRING : columnType2;
            Assert.assertEquals("Column type " + i, columnType1, columnType2);
        }
    }

    private static void assertEquals(Long256 expected, Long256 actual) {
        if (expected == actual) return;
        if (actual == null) {
            Assert.fail("Expected " + toHexString(expected) + ", but was: null");
        }

        if (expected.getLong0() != actual.getLong0()
                || expected.getLong1() != actual.getLong1()
                || expected.getLong2() != actual.getLong2()
                || expected.getLong3() != actual.getLong3()) {
            Assert.assertEquals(toHexString(expected), toHexString(actual));
        }
    }

    private static StringSink getTlSink() {
        StringSink ss = tlSink.get();
        ss.clear();
        return ss;
    }

/*
    private static RecordMetadata copySymAstStr(RecordMetadata src) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int i = 0, n = src.getColumnCount(); i < n; i++) {
            metadata.add(
                    new TableColumnMetadata(
                            src.getColumnName(i),
                            src.getColumnType(i) != ColumnType.SYMBOL ? src.getColumnType(i) : ColumnType.STRING
                    )
            );
        }
        metadata.setTimestampIndex(src.getTimestampIndex());
        return metadata;
    }
*/

    private static long partitionIncrement(int partitionBy, long fromTimestamp, int totalRows, int partitionCount) {
        long increment = 0;
        if (PartitionBy.isPartitioned(partitionBy)) {
            final PartitionBy.PartitionAddMethod partitionAddMethod = PartitionBy.getPartitionAddMethod(partitionBy);
            assert partitionAddMethod != null;
            long toTs = partitionAddMethod.calculate(fromTimestamp, partitionCount) - fromTimestamp - Timestamps.SECOND_MICROS;
            increment = totalRows > 0 ? Math.max(toTs / totalRows, 1) : 0;
        }
        return increment;
    }

    private static void putGeoHash(long hash, int bits, CharSink sink) {
        if (hash == GeoHashes.NULL) {
            return;
        }
        if (bits % 5 == 0) {
            GeoHashes.appendCharsUnsafe(hash, bits / 5, sink);
        } else {
            GeoHashes.appendBinaryStringUnsafe(hash, bits, sink);
        }
    }

    private static String recordToString(Record record, RecordMetadata metadata, boolean symbolsAsStrings) {
        StringSink sink = getTlSink();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            printColumn(record, metadata, i, sink, symbolsAsStrings, false);
            if (i < n - 1) {
                sink.put('\t');
            }
        }
        return sink.toString();
    }

    private static String toHexString(Long256 expected) {
        return Long.toHexString(expected.getLong0()) + " " +
                Long.toHexString(expected.getLong1()) + " " +
                Long.toHexString(expected.getLong2()) + " " +
                Long.toHexString(expected.getLong3());
    }

    static void addAllRecordsToMap(StringSink sink, RecordCursor cursor, RecordMetadata metadata, Map<String, Integer> map) {
        cursor.toTop();
        Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            addRecordToMap(sink, record, metadata, map, false);
        }
    }

    static void addRecordToMap(StringSink sink, Record record, RecordMetadata metadata, Map<String, Integer> map, boolean symbolsAsStrings) {
        sink.clear();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            printColumn(record, metadata, i, sink, symbolsAsStrings, true);
        }
        String printed = sink.toString();
        map.compute(printed, (s, i) -> {
            if (i == null) {
                return 1;
            }
            return i + 1;
        });
    }

    public interface CheckedIntFunction {
        int get() throws Throwable;
    }

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws Throwable;
    }

    public interface CheckedSupplier<T> {
        T get() throws Throwable;
    }

    @FunctionalInterface
    public interface LeakProneCode {
        void run() throws Exception;
    }

    public interface WorkerPoolCallback {
        void setupJobs(CairoEngine engine);
    }
}
