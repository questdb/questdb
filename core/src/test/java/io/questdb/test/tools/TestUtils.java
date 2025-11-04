/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.MessageBus;
import io.questdb.MessageBusImpl;
import io.questdb.ServerMain;
import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.DefaultDdlListener;
import io.questdb.cairo.DefaultLifecycleManager;
import io.questdb.cairo.LogRecordSinkAdapter;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.Response;
import io.questdb.cutlass.text.CopyImportRequestJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.functions.str.SizePrettyFunctionFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.ObjObjHashMap;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;
import io.questdb.std.ThreadLocal;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.MutableUtf16Sink;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.QuestDBTestNode;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.griffin.CustomisableRunnable;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.test.AbstractTest.CLOSEABLE;
import static org.junit.Assert.assertNotNull;

public final class TestUtils {
    private static final Log LOG = LogFactory.getLog(TestUtils.class);
    private static final ThreadLocal<StringSink> tlSink = new ThreadLocal<>(StringSink::new);

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

    public static void assertAsciiCompliance(@Nullable Utf8Sequence utf8Sequence) {
        if (utf8Sequence == null || utf8Sequence.isAscii() != Utf8s.isAscii(utf8Sequence)) {
            Utf8StringSink sink = new Utf8StringSink();
            sink.put("ascii flag set to '").put(utf8Sequence == null || utf8Sequence.isAscii())
                    .put("' for value '").put(utf8Sequence).put("'. ");
            Assert.assertEquals(
                    sink.toString(),
                    Utf8s.isAscii(utf8Sequence), utf8Sequence == null || utf8Sequence.isAscii()
            );
        }
    }

    public static void assertConnect(long fd, long sockAddr) {
        long rc = connect(fd, sockAddr);
        if (rc != 0) {
            Assert.fail("could not connect, errno=" + Os.errno());
        }
    }

    public static void assertConnect(NetworkFacade nf, long fd, long pSockAddr) {
        long rc = nf.connect(fd, pSockAddr);
        if (rc != 0) {
            Assert.fail("could not connect, errno=" + nf.errno());
        }
    }

    public static void assertConnectAddrInfo(long fd, long sockAddrInfo) {
        long rc = connectAddrInfo(fd, sockAddrInfo);
        if (rc != 0) {
            Assert.fail("could not connect, errno=" + Os.errno());
        }
    }

    public static void assertContains(String message, CharSequence sequence, CharSequence term) {
        // Assume that "" is contained in any string.
        if (term.isEmpty()) {
            return;
        }
        if (Chars.contains(sequence, term)) {
            return;
        }
        Assert.fail((message != null ? message + ": '" : "'") + sequence + "' does not contain: " + term);
    }

    public static void assertContains(CharSequence sequence, CharSequence term) {
        assertContains(null, sequence, term);
    }

    public static void assertContainsEither(CharSequence sequence, CharSequence term1, CharSequence term2) {
        // Assume that "" is contained in any string.
        if (term1.isEmpty() || term2.isEmpty()) {
            return;
        }

        if (Chars.contains(sequence, term1)) {
            return;
        }
        if (Chars.contains(sequence, term2)) {
            return;
        }
        Assert.fail("'" + sequence + "' does not contain either: " + term1 + " or " + term2);
    }

    public static void assertCursor(
            CharSequence expected,
            RecordCursor cursor,
            RecordMetadata metadata,
            boolean header,
            MutableUtf16Sink sink
    ) {
        CursorPrinter.println(cursor, metadata, sink, header, false);
        assertEquals(expected, sink);
    }

    public static void assertCursor(
            CharSequence expected,
            RecordCursor cursor,
            RecordMetadata metadata,
            boolean header,
            boolean printTypes,
            MutableUtf16Sink sink
    ) {
        CursorPrinter.println(cursor, metadata, sink, header, printTypes);
        assertEquals(expected, sink);
    }

    public static void assertEquals(
            RecordCursor cursorExpected,
            RecordMetadata metadataExpected,
            RecordCursor cursorActual,
            RecordMetadata metadataActual,
            boolean genericStringMatch
    ) {
        StringSink sink = getTlSink();
        assertEquals(metadataExpected, metadataActual, genericStringMatch);
        Record r = cursorExpected.getRecord();
        Record l = cursorActual.getRecord();
        final int timestampIndex = metadataActual.getTimestampIndex();
        final int timestampType = metadataActual.getTimestampType();
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);

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
                    addRecordToMap(sink, l, metadataActual, mapL, genericStringMatch);
                    addRecordToMap(sink, r, metadataExpected, mapR, genericStringMatch);
                    continue;
                }

                // check if we can bail out early because current record timestamps do not match
                if (tsL != tsR) {
                    throw new AssertionError(String.format(
                            "Row %d column %s[%s] %s. Expected %s but found %s",
                            rowIndex, metadataActual.getColumnName(timestampIndex), timestampType,
                            "timestamp mismatch", driver.toMSecString(tsL), driver.toUSecString(tsR)
                    ));
                }

                // compare accumulated records
                try {
                    Assert.assertEquals(mapL, mapR);
                } catch (AssertionError ignore) {
                    throw deferred;
                }

                // something changed, reset the store
                timestampValue = -1;
                deferred = null;

                mapL.clear();
                mapR.clear();
            }
            try {
                assertColumnValues(metadataExpected, metadataActual, l, r, rowIndex, genericStringMatch);
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
                    addRecordToMap(sink, l, metadataActual, mapL, genericStringMatch);
                    addRecordToMap(sink, r, metadataExpected, mapR, genericStringMatch);
                }
            }
        }

        if (deferred != null) {
            // looks like there was no more rows to do the comparison, so do it now
            try {
                Assert.assertEquals(mapL, mapR);
            } catch (AssertionError ignore) {
                throw deferred;
            }
        }

        Assert.assertFalse("Expected cursor misses record " + rowIndex, cursorActual.hasNext());
    }

    public static void assertEquals(File a, File b) {
        try (Path path = new Path()) {
            path.of(a.getAbsolutePath());
            long fda = TestFilesFacadeImpl.INSTANCE.openRO(path.$());
            Assert.assertNotEquals(-1, fda);

            try {
                path.of(b.getAbsolutePath());
                long fdb = TestFilesFacadeImpl.INSTANCE.openRO(path.$());
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
                                Assert.assertEquals(
                                        Unsafe.getUnsafe().getByte(bufa + i),
                                        Unsafe.getUnsafe().getByte(bufb + i)
                                );
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
            path.of(a.getAbsolutePath());
            long fda = TestFilesFacadeImpl.INSTANCE.openRO(path.$());
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
                                Assert.fail("expected: '" + (char) (bb) + "'(" + bb + ")" + ", actual: '" + (char) (b)
                                        + "'(" + b + ")" + ", at: " + (offset + i - 1));
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
        if (expected == null && actual == null) {
            return;
        }

        if (expected != null && actual == null) {
            Assert.fail("Expected: \n`" + expected + "`\nbut have NULL. ");
        }

        if (expected == null) {
            Assert.fail("Expected: NULL but have \n`" + actual + "`\n");
        }

        if (expected.size() != actual.size()) {
            Assert.assertEquals(expected, actual);
        }

        StringSink sink = getTlSink();
        Utf8s.utf8ToUtf16(expected, sink);
        String expectedStr = sink.toString();
        sink.clear();
        Utf8s.utf8ToUtf16(actual, sink);
        assertEquals(expectedStr, sink);
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
            SqlCompiler compiler, SqlExecutionContext sqlExecutionContext,
            String expectedSql, String actualSql
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

    public static void assertEquals(ArrayView expected, ArrayView actual) {
        if (expected == null) {
            Assert.assertNull("expected NULL array", actual);
            return;
        }
        Assert.assertNotNull("expected NON-NULL array", actual);
        // Check that the number of dimensions matches
        final int expectedDimCount = expected.getDimCount();
        Assert.assertEquals("Array dimensionality mismatch", expectedDimCount, actual.getDimCount());
        if (expectedDimCount == 0) {
            return;
        }
        // Check if each dimension has the same length
        for (int i = 0; i < expectedDimCount; i++) {
            Assert.assertEquals(expected.getDimLen(i), actual.getDimLen(i));
        }
        // Compare elements using flat indexing
        assertEqualsRecursive(expected, actual, 0, 0, 0);
    }

    public static void assertEqualsExactOrder(
            RecordCursor cursorExpected, RecordMetadata metadataExpected,
            RecordCursor cursorActual, RecordMetadata metadataActual, boolean genericStringMatch
    ) {
        assertEquals(metadataExpected, metadataActual, genericStringMatch);
        Record r = cursorExpected.getRecord();
        Record l = cursorActual.getRecord();
        long rowIndex = 0;
        while (cursorExpected.hasNext()) {
            if (!cursorActual.hasNext()) {
                Assert.fail("Actual cursor does not have record at " + rowIndex);
            }
            rowIndex++;
            assertColumnValues(metadataExpected, metadataActual, l, r, rowIndex, genericStringMatch);
        }

        Assert.assertFalse("Expected cursor misses record " + rowIndex, cursorActual.hasNext());
    }

    public static void assertEqualsExactOrder(
            SqlCompiler compiler, SqlExecutionContext sqlExecutionContext,
            String expectedSql, String actualSql
    ) throws SqlException {
        try (RecordCursorFactory f1 = compiler.compile(expectedSql, sqlExecutionContext).getRecordCursorFactory();
             RecordCursorFactory f2 = compiler.compile(actualSql, sqlExecutionContext).getRecordCursorFactory();
             RecordCursor c1 = f1.getCursor(sqlExecutionContext); RecordCursor c2 = f2.getCursor(sqlExecutionContext)
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

    public static void assertEventually(EventualCode assertion) throws Exception {
        assertEventually(assertion, 60);
    }

    public static void assertEventually(EventualCode assertion, Set<Class<?>> exceptionTypesToCatch) throws Exception {
        exceptionTypesToCatch.add(AssertionError.class);
        assertEventually(assertion, 30, exceptionTypesToCatch);
    }

    public static void assertEventually(EventualCode assertion, int timeoutSeconds, Set<Class<?>> exceptionTypesToCatch) throws Exception {
        long maxSleepingTimeMillis = 1000;
        long nextSleepingTimeMillis = 10;
        long startTime = System.nanoTime();
        long deadline = startTime + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        for (; ; ) {
            try {
                assertion.run();
                return;
            } catch (Exception error) {
                if (!exceptionTypesToCatch.contains(error.getClass())) {
                    throw error;
                }
                if (System.nanoTime() >= deadline) {
                    throw error;
                }
            }
            Os.sleep(nextSleepingTimeMillis);
            nextSleepingTimeMillis = Math.min(maxSleepingTimeMillis, nextSleepingTimeMillis << 1);
        }
    }

    public static void assertEventually(EventualCode assertion, int timeoutSeconds) throws Exception {
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
             BufferedInputStream actualStream = new BufferedInputStream(new FileInputStream(actual.toString()))
        ) {
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
                    BitmapIndexReader bitmapIndexReader =
                            rdr.getBitmapIndexReader(0, symIndex, BitmapIndexReader.DIR_BACKWARD);
                    Assert.assertEquals(expectedCapacity, bitmapIndexReader.getValueBlockCapacity() + 1);
                }
            }
        }
    }

    public static void assertMemoryLeak(LeakProneCode runnable) throws Exception {
        try (LeakCheck ignore = new LeakCheck()) {
            try {
                runnable.run();
            } catch (Throwable e) {
                ignore.skipChecks();
                throw e;
            }
        }
    }

    /**
     * Asserts that a {@code CharSequence} does NOT contain another {@code CharSequence}.
     *
     * @param sequence the {@code CharSequence} to check.
     * @param term     the {@code CharSequence} to search for (and assert its absence).
     * @see #assertNotContains(String, CharSequence, CharSequence)
     */
    public static void assertNotContains(CharSequence sequence, CharSequence term) {
        assertNotContains(null, sequence, term);
    }

    /**
     * Asserts that a {@code CharSequence} does NOT contain another {@code CharSequence}.
     * <p>
     * Fails if the {@code term} is empty (""), because the convention established by
     * {@link #assertContains(String, CharSequence, CharSequence)} considers an empty
     * term to be contained within any sequence.
     * </p>
     *
     * @param message  the identifying message for the {@link AssertionError} (<code>null</code> okay)
     * @param sequence the {@code CharSequence} to check.
     * @param term     the {@code CharSequence} to search for (and assert its absence).
     */
    public static void assertNotContains(String message, CharSequence sequence, CharSequence term) {
        if (term.isEmpty()) {
            String formatted = "";
            if (message != null) {
                formatted = message + " ";
            }
            Assert.fail(formatted + "Cannot assert that sequence does not contain an empty term; an empty term is always considered contained by definition.");
        }

        if (!Chars.contains(sequence, term)) {
            return;
        }

        String formatted = "";
        if (message != null) {
            formatted = message + " ";
        }
        Assert.fail(formatted + "Expected sequence <" + sequence + "> to NOT contain term <" + term + "> but it did.");
    }

    public static void assertReader(CharSequence expected, TableReader reader, MutableUtf16Sink sink) {
        try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
            assertCursor(expected, cursor, reader.getMetadata(), true, sink);
        }
    }

    public static void assertResponse(HttpClient.Request request, int expectedStatusCode, String expectedHttpResponse) {
        try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
            responseHeaders.await();

            assertEquals(String.valueOf(expectedStatusCode), responseHeaders.getStatusCode());

            final Utf8StringSink sink = new Utf8StringSink();

            Fragment fragment;
            final Response response = responseHeaders.getResponse();
            while ((fragment = response.recv()) != null) {
                Utf8s.strCpy(fragment.lo(), fragment.hi(), sink);
            }

            assertEquals(expectedHttpResponse, sink);
            sink.clear();
        }
    }

    /**
     * Asserts that the actual lines are the same as the expected lines in the reverse order.
     * For example, assertion with expected: "123\n456\n789\n" and actual: "789\n456\n123\n" passes.
     * This method expects for the char sequences to end up with a newline character.
     */
    public static void assertReverseLinesEqual(@Nullable String message, CharSequence expected, CharSequence actual) {
        String cleanMessage = message == null ? "" : message;
        if (expected == null && actual == null) {
            return;
        }

        if (expected != null && actual == null) {
            Assert.fail(cleanMessage + "expected:<" + expected + "> but was: NULL");
        }

        if (expected == null) {
            Assert.fail(cleanMessage + "expected: NULL but was:<" + actual + ">");
        }

        if (expected.length() != actual.length()) {
            Assert.fail(cleanMessage + "expected:<" + reverseLines(expected) + "> but was:<" + actual + ">");
        }

        if (expected.isEmpty()) {
            // If expected is empty, so is actual here (otherwise it would have failed the last condition).
            return;
        }

        if (expected.charAt(expected.length() - 1) != '\n') {
            Assert.fail(cleanMessage + "expected must end up with a newline character");
        }

        if (actual.charAt(actual.length() - 1) != '\n') {
            Assert.fail(cleanMessage + "actual must end up with a newline character");
        }

        int expLo = expected.length();
        int actHi = -1;
        final int actLen = actual.length();
        while (expLo != 0) {
            int idx = Chars.lastIndexOf(expected, 0, expLo - 1, '\n');
            final int len = expLo - idx - 1;
            expLo = idx + 1;

            final int actLo = actHi + 1;
            idx = Chars.indexOf(actual, actLo, actLen, '\n');
            if (idx == -1 || idx - actHi != len) {
                Assert.fail(cleanMessage + "expected:<" + reverseLines(expected) + "> but was:<" + actual + ">");
            }
            actHi = idx;

            for (int j = 0; j < len; j++) {
                if (expected.charAt(expLo + j) != actual.charAt(actLo + j)) {
                    Assert.fail(cleanMessage + "expected:<" + reverseLines(expected) + "> but was:<" + actual + ">");
                }
            }
        }

        if (actHi != actLen - 1) {
            Assert.fail(cleanMessage + "expected:<" + reverseLines(expected) + "> but was:<" + actual + ">");
        }
    }

    public static void assertSql(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            CharSequence sql,
            MutableUtf16Sink sink,
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
            MutableUtf16Sink sink,
            CharSequence expected
    ) throws SqlException {
        printSql(compiler, sqlExecutionContext, sql, sink);
        assertEquals(expected, sink);
    }

    public static void assertSqlCursors(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            CharSequence expected,
            CharSequence actual,
            Log log
    ) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            assertSqlCursors(compiler, sqlExecutionContext, expected, actual, log);
        }
    }

    public static void assertSqlCursors(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            CharSequence expected,
            CharSequence actual,
            Log log,
            boolean genericStringMatch
    ) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            assertSqlCursors(compiler, sqlExecutionContext, expected, actual, log, genericStringMatch);
        }
    }

    public static void assertSqlCursors(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            CharSequence expected,
            CharSequence actual,
            Log log
    ) throws SqlException {
        assertSqlCursors(compiler, sqlExecutionContext, expected, actual, log, false);
    }

    public static void assertSqlCursors(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            CharSequence expected,
            CharSequence actual,
            Log log,
            boolean genericStringMatch
    ) throws SqlException {
        try (
                RecordCursorFactory factory = compiler.compile(expected, sqlExecutionContext).getRecordCursorFactory();
                RecordCursorFactory factory2 = compiler.compile(actual, sqlExecutionContext).getRecordCursorFactory()
        ) {
            try (
                    RecordCursor cursor1 = factory.getCursor(sqlExecutionContext);
                    RecordCursor cursor2 = factory2.getCursor(sqlExecutionContext)
            ) {
                assertEquals(cursor1, factory.getMetadata(), cursor2, factory2.getMetadata(), genericStringMatch);
            } catch (AssertionError e) {
                log.error().$(e).$();
                try (
                        RecordCursor expectedCursor = factory.getCursor(sqlExecutionContext);
                        RecordCursor actualCursor = factory2.getCursor(sqlExecutionContext)
                ) {
                    log.xDebugW().$();

                    LogRecordSinkAdapter recordSinkAdapter = new LogRecordSinkAdapter();
                    LogRecord record = log.xDebugW().$("java.lang.AssertionError: expected:<");
                    CursorPrinter.printHeader(factory.getMetadata(), recordSinkAdapter.of(record));
                    record.$();
                    CursorPrinter.println(expectedCursor, factory.getMetadata(), false, log);

                    record = log.xDebugW().$("> but was:<");
                    CursorPrinter.printHeader(factory2.getMetadata(), recordSinkAdapter.of(record));
                    record.$();

                    CursorPrinter.println(actualCursor, factory2.getMetadata(), false, log);
                    log.xDebugW().$(">").$();
                }
                throw e;
            }
        }
    }

    public static void assertSqlCursors(
            QuestDBTestNode node,
            ObjList<QuestDBTestNode> nodes,
            String expected,
            String actual,
            Log log,
            boolean genericStringMatch
    ) throws SqlException {
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
                    try (
                            RecordCursor cursor1 = factory.getCursor(node.getSqlExecutionContext());
                            RecordCursor cursor2 = factory2.getCursor(dbNode.getSqlExecutionContext())
                    ) {
                        assertEquals(cursor1, factory.getMetadata(), cursor2, factory2.getMetadata(), genericStringMatch);
                    } catch (AssertionError e) {
                        log.error().$(e).$();
                        try (
                                RecordCursor expectedCursor = factory.getCursor(node.getSqlExecutionContext());
                                RecordCursor actualCursor = factory2.getCursor(dbNode.getSqlExecutionContext())
                        ) {
                            log.xDebugW().$();

                            LogRecordSinkAdapter recordSinkAdapter = new LogRecordSinkAdapter();
                            LogRecord record = log.xDebugW().$("java.lang.AssertionError: expected:<");
                            CursorPrinter.printHeader(factory.getMetadata(), recordSinkAdapter.of(record));
                            record.$();
                            CursorPrinter.println(expectedCursor, factory.getMetadata(), false, log);

                            record = log.xDebugW().$("> but was:<");
                            CursorPrinter.printHeader(factory2.getMetadata(), recordSinkAdapter.of(record));
                            record.$();

                            CursorPrinter.println(actualCursor, factory2.getMetadata(), false, log);
                            log.xDebugW().$(">").$();
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
            MutableUtf16Sink sink,
            CharSequence expected
    ) throws SqlException {
        printSqlWithTypes(compiler, sqlExecutionContext, sql, sink);
        assertEquals(expected, sink);
    }

    public static void await(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (Throwable ignore) {
        }
    }

    public static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (Throwable ignore) {
        }
    }

    // Useful for debugging
    @SuppressWarnings("unused")
    public static long beHexToLong(String hex) {
        return Long.parseLong(reverseBeHex(hex), 16);
    }

    // Useful for debugging
    @SuppressWarnings("unused")
    public static String beHexToTs(String hex, TimestampDriver driver) {
        long l = beHexToLong(hex);
        return driver.toUSecString(l);
    }

    /**
     * Generates a cartesian product from multiple sets of values.
     * <p>
     * This utility method creates all possible combinations of elements where each combination
     * takes exactly one element from each input set. It's primarily used to generate comprehensive
     * test parameters for parameterized tests.
     * <p>
     * Example usage:
     * <pre>{@code
     * Object[][] parameters = cartesianProduct(new Object[][]{
     *     JoinType.values(),     // First set: all join types
     *     {true, false},         // Second set: boolean values
     *     LimitType.values(),    // Third set: all limit types
     *     {true, false}          // Fourth set: more boolean values
     * });
     * }</pre>
     *
     * @param values A non-empty array of arrays, where each inner array represents a set of values.
     *               None of the inner arrays can be empty.
     * @return A two-dimensional array containing all possible combinations of the input values,
     * where each row is one combination.
     * @throws AssertionError If the input array is empty or any inner array is empty
     */
    public static Object[][] cartesianProduct(@NotNull Object[][] values) {
        if (values.length == 0) {
            throw new AssertionError("Expected at least one set of values");
        }
        return cartesianProduct(values, 0);
    }

    public static int connect(long fd, long sockAddr) {
        Assert.assertTrue(fd > -1);
        // clients may run out of ephemeral ports, that are still lingering
        // enable port reuse to avoid WSAEADDRINUSE(10048)
        Net.setReusePort(fd);
        Net.setReuseAddress(fd);
        return Net.connect(fd, sockAddr);
    }

    public static long connectAddrInfo(long fd, long sockAddrInfo) {
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

    public static void createPopulateTable(
            SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, TableModel tableModel,
            int totalRows, String startDate, int partitionCount
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
        CairoEngine.execute(
                compiler,
                createPopulateTableStmt(tableName, tableModel, totalRows, startDate, partitionCount),
                sqlExecutionContext,
                null
        );
    }

    public static String createPopulateTableStmt(
            CharSequence tableName,
            TableModel tableModel,
            int totalRows,
            String startDate,
            int partitionCount
    ) throws NumericException {
        int timestampType = TableUtils.getTimestampType(tableModel);
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        long fromTimestamp = driver.parseFloorLiteral(startDate);
        int partitionBy = tableModel.getPartitionBy();
        long increment = partitionIncrement(
                driver,
                partitionBy,
                fromTimestamp,
                totalRows,
                partitionCount
        );
        if (PartitionBy.isPartitioned(partitionBy)) {
            final TimestampDriver.PartitionAddMethod partitionAddMethod =
                    PartitionBy.getPartitionAddMethod(
                            timestampType,
                            partitionBy
                    );
            assert partitionAddMethod != null;
            long toTs = partitionAddMethod.calculate(fromTimestamp, partitionCount) - fromTimestamp - driver.fromSeconds(1);
            increment = totalRows > 0 ? Math.max(toTs / totalRows, 1) : 0;
        }

        StringBuilder sql = new StringBuilder();
        StringBuilder indexes = new StringBuilder();
        sql.append("create atomic table ").append(tableName).append(" as (").append(Misc.EOL).append("select").append(Misc.EOL);
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
                case ColumnType.VARCHAR:
                    sql.append("CAST(x as VARCHAR) ").append(colName);
                    break;
                case ColumnType.LONG:
                    sql.append("x ").append(colName);
                    break;
                case ColumnType.DOUBLE:
                    sql.append("x / 1000.0 ").append(colName);
                    break;
                case ColumnType.TIMESTAMP:
                    sql.append("CAST(").append(fromTimestamp).append("L AS ").append(ColumnType.nameOf(timestampType)).append(") + x * ")
                            .append(increment).append("  ").append(colName);
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
        return new SqlExecutionContextImpl(engine, 1)
                .with(
                        engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null
                );
    }

    public static SqlExecutionContext createSqlExecutionCtx(CairoEngine engine, BindVariableService bindVariableService) {
        SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1);
        ctx.with(
                engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                bindVariableService
        );
        return ctx;
    }

    public static SqlExecutionContextImpl createSqlExecutionCtx(CairoEngine engine, int workerCount) {
        return new SqlExecutionContextImpl(engine, workerCount)
                .with(engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext());
    }

    public static SqlExecutionContextImpl createSqlExecutionCtx(
            CairoEngine engine, BindVariableService bindVariableService, int workerCount
    ) {
        return new SqlExecutionContextImpl(engine, workerCount)
                .with(
                        engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        bindVariableService
                );
    }

    public static TableToken createTable(CairoEngine engine, TableStructure structure) {
        return createTable(engine, structure, engine.getNextTableId());
    }

    public static void createTable(
            TableModel model,
            CairoConfiguration configuration,
            int tableVersion,
            int tableId,
            TableToken tableToken
    ) {
        try (
                Path path = new Path();
                MemoryMARW mem = Vm.getCMARWInstance()
        ) {
            TableUtils.createTable(configuration, mem, path, model, tableVersion, tableId, tableToken.getDirName());
        }
    }

    public static TableToken createTable(CairoEngine engine, TableStructure structure, int tableId) {
        try (MemoryMARW mem = Vm.getCMARWInstance(); Path path = new Path()) {
            return TestUtils.createTable(engine, mem, path, structure, tableId, structure.getTableName());
        }
    }

    public static TableToken createTable(
            CairoEngine engine,
            MemoryMARW memory,
            Path path,
            TableStructure structure,
            int tableId,
            CharSequence tableName
    ) {
        TableToken token = engine.lockTableName(tableName, tableId, structure.isMatView(), structure.isWalEnabled());
        if (token == null) {
            throw new RuntimeException("table already exists: " + tableName);
        }
        path.of(engine.getConfiguration().getDbRoot()).concat(token);
        TableUtils.createTable(engine.getConfiguration(), memory, path, structure, ColumnType.VERSION, tableId, token.getDirName());
        engine.registerTableToken(token);
        if (structure.isWalEnabled()) {
            engine.getTableSequencerAPI().registerTable(tableId, structure, token);
        }
        return token;
    }

    public static void createTestPath(CharSequence root) {
        try (Path path = new Path()) {
            path.of(root);
            if (Files.exists(path.$())) {
                return;
            }
            Files.mkdirs(path.of(root).slash(), 509);
        }
    }

    public static Timestamp createTimestamp(long epochMicros) {
        // constructor requires epoch millis
        Timestamp ts = new Timestamp(epochMicros / 1000);
        ts.setNanos((int) ((epochMicros % 1_000_000) * 1000));
        return ts;
    }

    public static void drainCopyImportJobQueue(CairoEngine engine) throws Exception {
        try (CopyImportRequestJob copyRequestJob = new CopyImportRequestJob(engine, 1)) {
            copyRequestJob.drain(0);
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public static void drainCursor(RecordCursor cursor) {
        while (cursor.hasNext()) {
        }
    }

    public static void drainPurgeJob(CairoEngine engine) {
        drainPurgeJob(engine, engine.getConfiguration().getFilesFacade());
    }

    public static void drainPurgeJob(CairoEngine engine, FilesFacade filesFacade) {
        try (WalPurgeJob job = new WalPurgeJob(
                engine,
                filesFacade,
                engine.getConfiguration().getMicrosecondClock()
        )) {
            engine.setWalPurgeJobRunLock(job.getRunLock());
            job.drain(0);
        }
    }

    public static void drainWalQueue(CairoEngine engine) {
        try (final ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0)) {
            walApplyJob.drain(0);
            new CheckWalTransactionsJob(engine).run(0);
            // run once again as there might be notifications to handle now
            walApplyJob.drain(0);
        }
    }

    public static String dumpMetadataCache(CairoEngine engine) {
        try (MetadataCacheReader ro = engine.getMetadataCache().readLock()) {
            StringSink sink = new StringSink();
            ro.toSink(sink);
            return sink.toString();
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
            Log log
    ) throws Exception {
        final int workerCount = pool != null ? pool.getWorkerCount() : 1;
        final BindVariableServiceImpl bindVariableService = new BindVariableServiceImpl(configuration);
        try (
                final CairoEngine engine = new CairoEngine(configuration);
                final SqlCompiler compiler = engine.getSqlCompiler();
                final SqlExecutionContext sqlExecutionContext = createSqlExecutionCtx(engine, bindVariableService, workerCount)
        ) {
            try {
                if (pool != null) {
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

    public static void execute(Connection conn, String sql, String... bindVars) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < bindVars.length; i++) {
                stmt.setString(i + 1, bindVars[i]);
            }
            stmt.execute();
        }
    }

    public static void executeSQLViaPostgres(String username, String password, int pgPort, String... sqls) throws SQLException {
        try (final Connection connection = getConnectionForUser(username, password, pgPort)) {
            for (String sql : sqls) {
                execute(connection, sql);
            }
        }
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
        Rnd rnd = new Rnd(s0, s1);
        // Random impl is biased on first few calls, always return same bool,
        // so we need to make a few calls to get it going randomly
        rnd.nextBoolean();
        rnd.nextBoolean();
        return rnd;
    }

    @NotNull
    public static Rnd generateRandomForTestParams(Log log, long s0, long s1) {
        if (log != null) {
            log.info().$("random test params seeds: ").$(s0).$("L, ").$(s1).$('L').$();
        }
        System.out.printf("random test params seeds: %dL, %dL%n", s0, s1);
        Rnd rnd = new Rnd(s0, s1);
        // Random impl is biased on first few calls, always return same bool,
        // so we need to make a few calls to get it going randomly
        rnd.nextBoolean();
        rnd.nextBoolean();
        rnd.nextBoolean();
        rnd.nextBoolean();
        rnd.nextBoolean();
        return rnd;
    }

    @NotNull
    public static Rnd generateRandomForTestParams(Log log) {
        return generateRandomForTestParams(log, System.nanoTime(), System.currentTimeMillis());
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

    public static long getMetricValue(CairoEngine engine, String tag) {
        try (DirectUtf8Sink metricsSink = new DirectUtf8Sink(1024)) {
            engine.getMetrics().scrapeIntoPrometheus(metricsSink);
            String[] lines = metricsSink.toString().split("\n");
            Optional<String> metricsLine = Arrays.stream(lines)
                    .filter(line -> line.startsWith(tag + ' ')).findFirst();
            Assert.assertTrue(tag + " missing", metricsLine.isPresent());
            return Long.parseLong(metricsLine.get().substring(tag.length() + 1));
        }
    }

    public static String getPgConnectionUri(int pgPort) {
        return "jdbc:postgresql://127.0.0.1:" + pgPort + "/qdb";
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
            public int sendRaw(long fd, long buffer, int bufferLen) {
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

    public static int getSystemTablesCount(CairoEngine engine) {
        final ObjHashSet<TableToken> tableBucket = new ObjHashSet<>();
        engine.getTableTokens(tableBucket, false);
        int systemTableCount = 0;
        for (int i = 0, n = tableBucket.size(); i < n; i++) {
            final TableToken tt = tableBucket.get(i);
            if (tt.isSystem()) {
                systemTableCount++;
            }
        }
        return systemTableCount;
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

    public static TestTimestampType getTimestampType() {
        return getTimestampType(generateRandom(LOG));
    }

    public static TestTimestampType getTimestampType(Rnd rnd) {
        return rnd.nextBoolean() ? TestTimestampType.MICRO : TestTimestampType.NANO;
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
        TimestampDriver driver = ColumnType.getTimestampDriver(TableUtils.getTimestampType(tableModel));
        long fromTimestamp = driver.parseFloorLiteral(startDate);
        long increment = partitionIncrement(
                driver,
                tableModel.getPartitionBy(),
                fromTimestamp,
                totalRows,
                partitionCount
        );

        StringBuilder insertFromSelect = new StringBuilder();
        insertFromSelect.append("INSERT ATOMIC INTO ")
                .append(tableModel.getTableName()).append(" SELECT").append(Misc.EOL);
        for (int i = 0; i < tableModel.getColumnCount(); i++) {
            CharSequence colName = tableModel.getColumnName(i);
            switch (ColumnType.tagOf(tableModel.getColumnType(i))) {
                case ColumnType.INT:
                    insertFromSelect.append("CAST(x as INT) ").append(colName);
                    break;
                case ColumnType.STRING:
                    insertFromSelect.append("CAST(x as STRING) ").append(colName);
                    break;
                case ColumnType.VARCHAR:
                    insertFromSelect.append("rnd_varchar(1, 15, 1) ").append(colName);
                    break;
                case ColumnType.LONG:
                    insertFromSelect.append("x ").append(colName);
                    break;
                case ColumnType.DOUBLE:
                    insertFromSelect.append("x / 1000.0 ").append(colName);
                    break;
                case ColumnType.TIMESTAMP:
                    insertFromSelect.append("CAST(").append(fromTimestamp).append("L AS ").append(ColumnType.nameOf(tableModel.getColumnType(i))).append(") + x * ")
                            .append(increment).append("  ").append(colName);
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
        insertFromSelect.append(";" + Misc.EOL);
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

    /**
     * Helper method to bias probability of "wal" tests to 80%
     *
     * @return true when tests should run in WAL-enabled mode, false otherwise.
     */
    public static boolean isWal() {
        return isWal(generateRandom(LOG));
    }

    public static boolean isWal(Rnd rnd) {
        return rnd.nextInt(100) < 80;
    }

    public static int maxDayOfMonth(int month) {
        return switch (month) {
            case 1, 3, 5, 7, 8, 10, 12 -> 31;
            case 2 -> 28;
            case 4, 6, 9, 11 -> 30;
            default -> throw new IllegalArgumentException("[1..12]");
        };
    }

    public static void messTxnUnallocated(FilesFacade ff, Path path, Rnd rnd, TableToken tableToken) {
        path.concat(tableToken).concat(TableUtils.TXN_FILE_NAME);
        try (MemoryMARW txFile = Vm.getCMARWInstance(
                ff, path.$(), Files.PAGE_SIZE, -1,
                MemoryTag.NATIVE_MIG_MMAP, CairoConfiguration.O_NONE
        )
        ) {
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

    public static TableWriter newOffPoolWriter(CairoConfiguration configuration, TableToken tableToken, CairoEngine engine) {
        return newOffPoolWriter(configuration, tableToken, new MessageBusImpl(configuration), engine);
    }

    public static TableWriter newOffPoolWriter(
            CairoConfiguration configuration,
            TableToken tableToken,
            MessageBus messageBus,
            CairoEngine engine
    ) {
        return new TableWriter(
                configuration,
                tableToken,
                null,
                messageBus,
                true,
                DefaultLifecycleManager.INSTANCE,
                configuration.getDbRoot(),
                DefaultDdlListener.INSTANCE,
                () -> Numbers.LONG_NULL,
                engine
        );
    }

    public static void printSql(
            CairoEngine engine, SqlExecutionContext sqlExecutionContext, CharSequence sql, MutableUtf16Sink sink
    ) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            printSql(compiler, sqlExecutionContext, sql, sink);
        }
    }

    public static void printSql(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            CharSequence sql,
            MutableUtf16Sink sink
    ) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                RecordMetadata metadata = factory.getMetadata();
                sink.clear();
                CursorPrinter.println(metadata, sink);

                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    println(record, metadata, sink);
                }
            }
        }
    }

    public static String printSqlToString(
            CairoEngine engine, SqlExecutionContext sqlExecutionContext, CharSequence sql, MutableUtf16Sink sink
    ) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            printSql(compiler, sqlExecutionContext, sql, sink);
            return sink.toString();
        }
    }

    public static void printSqlWithTypes(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            CharSequence sql,
            MutableUtf16Sink sink
    ) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                RecordMetadata metadata = factory.getMetadata();
                CursorPrinter.println(cursor, metadata, sink, true, true);
            }
        }
    }

    public static void println(Record record, RecordMetadata metadata, CharSink<?> sink) {
        CursorPrinter.println(record, metadata, sink);
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (metadata.getColumnType(i) == ColumnType.VARCHAR) {
                assertAsciiCompliance(record.getVarcharA(i));
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
                r.putSymUtf8(columnIndex, seq);
            } else {
                r.putStrUtf8(columnIndex, seq);
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

    public static String randomiseCase(Rnd rnd, String columName) {
        int changeCase = rnd.nextInt(3);
        if (changeCase == 0) {
            return columName;
        }
        StringSink sink = Misc.getThreadLocalSink();
        sink.put(columName);

        for (int i = 0; i < changeCase; i++) {
            int pos = rnd.nextInt(columName.length());
            char ch = columName.charAt(pos);
            if (Character.isLowerCase(ch)) {
                sink.setCharAt(pos, Character.toUpperCase(ch));
            } else {
                sink.setCharAt(pos, Character.toLowerCase(ch));
            }
        }
        return sink.toString();
    }

    public static String readStringFromFile(File file) {
        try {
            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] buffer = new byte[(int) fis.getChannel().size()];
                int totalRead = 0;
                int read;
                while (totalRead < buffer.length && (read = fis.read(buffer, totalRead, buffer.length - totalRead)) > 0) {
                    totalRead += read;
                }
                return new String(buffer, Files.UTF_8);
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot read from " + file.getAbsolutePath(), e);
        }
    }

    public static boolean remove(LPSZ lpsz) {
        if (Files.remove(lpsz)) {
            return true;
        }

        // could not remove file, logging error
        final FilesFacade ff = FilesFacadeImpl.INSTANCE;
        LOG.error().$("Could not remove file [path=").$safe(lpsz).$(", errno=").$(ff.errno()).I$();
        return false;
    }

    public static void removeTestPath(CharSequence root) {
        try (Path path = new Path()) {
            path.of(root);
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            path.slash();
            if (ff.exists(path.$()) && !ff.rmdir(path, true)) {
                StringSink dir = new StringSink();
                dir.put(path.$());
                Assert.fail("Test dir " + dir + " cleanup error: " + ff.errno());
            }

            path.parent().concat(RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME);
            if (ff.exists(path.$()) && !ff.removeQuiet(path.$())) {
                StringSink dir = new StringSink();
                dir.put(path.$());
                Assert.fail("Checkpoint dir " + dir + " trigger cleanup error:: " + ff.errno());
            }
        }
    }

    public static String replaceSizeToMatchOS(
            String expected,
            String tableName,
            CairoConfiguration configuration,
            CairoEngine engine,
            StringSink sink
    ) {
        return replaceSizeToMatchOS(expected, new Utf8String(configuration.getDbRoot()), tableName, engine, sink);
    }

    public static String replaceSizeToMatchOS(
            String expected,
            Utf8Sequence root,
            String tableName,
            CairoEngine engine,
            StringSink sink
    ) {
        ObjObjHashMap<String, Long> sizes = findPartitionSizes(root, tableName, engine, sink);
        String[] lines = expected.split("\n");
        sink.clear();
        sink.put(lines[0]).put('\n');
        StringSink auxSink = new StringSink();
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i];
            String nameColumn = line.split("\t")[2];
            Long s = sizes.get(nameColumn);
            long size = s != null ? s : 0L;
            SizePrettyFunctionFactory.toSizePretty(auxSink, size);
            line = line.replaceAll("SIZE", String.valueOf(size));
            line = line.replaceAll("HUMAN", auxSink.toString());
            sink.put(line).put('\n');
        }
        return sink.toString();
    }

    // Useful for debugging
    @SuppressWarnings("unused")
    public static String reverseBeHex(String hex) {
        var sb = new char[hex.length()];
        for (int i = 0; i < hex.length(); i += 2) {
            sb[hex.length() - i - 1] = hex.charAt(i + 1);
            sb[hex.length() - i - 2] = hex.charAt(i);
        }
        return new String(sb);
    }

    public static void setupWorkerPool(WorkerPool workerPool, CairoEngine cairoEngine) throws SqlException {
        WorkerPoolUtils.setupQueryJobs(workerPool, cairoEngine);
        WorkerPoolUtils.setupWriterJobs(workerPool, cairoEngine);
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

    private static void assertCharEquals(
            RecordMetadata metaL, RecordMetadata metaR, Record lr, Record rr, boolean genericStringMatch, int col
    ) {
        if (genericStringMatch && metaL.getColumnType(col) != metaR.getColumnType(col)) {
            char right = readAsChar(metaR, rr, col);
            char left = readAsChar(metaL, lr, col);
            Assert.assertEquals(left, right);
            return;
        }
        Assert.assertEquals(rr.getChar(col), lr.getChar(col));
    }

    private static void assertColumnValues(
            RecordMetadata metadataExpected, RecordMetadata metadataActual, Record lr, Record rr,
            long rowIndex, boolean genericStringMatch
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
                        TimestampDriver driver = ColumnType.getTimestampDriver(columnType);
                        if (rr.getTimestamp(i) != lr.getTimestamp(i)) {
                            Assert.assertEquals(driver.toMSecString(rr.getTimestamp(i)), driver.toMSecString(lr.getTimestamp(i)));
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
                    case ColumnType.SYMBOL:
                    case ColumnType.STRING:
                    case ColumnType.VARCHAR:
                        assertStringEquals(metadataActual, metadataExpected, lr, rr, genericStringMatch, i);
                        break;
                    case ColumnType.SHORT:
                        Assert.assertEquals(rr.getShort(i), lr.getShort(i));
                        break;
                    case ColumnType.CHAR:
                        assertCharEquals(metadataActual, metadataExpected, lr, rr, genericStringMatch, i);
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
                    case ColumnType.ARRAY:
                        assertEquals(rr.getArray(i, columnType), lr.getArray(i, columnType));
                        break;
                    default:
                        // Unknown record type.
                        assert false;
                        break;
                }
            } catch (AssertionError e) {
                String expected = recordToString(rr, metadataExpected, genericStringMatch);
                String actual = recordToString(lr, metadataActual, genericStringMatch);
                Assert.assertEquals(
                        String.format(String.format(
                                "Row %d column %s[%s]",
                                rowIndex, columnName, ColumnType.nameOf(columnType)
                        )), expected, actual
                );
                // If above didn't fail because of types not included or double precision not enough, throw here anyway
                throw new AssertionError(String.format(
                        "Row %d column %s[%s] %s", rowIndex, columnName,
                        ColumnType.nameOf(columnType), e.getMessage()
                ));
            }
        }
    }

    private static void assertEquals(RecordMetadata metadataExpected, RecordMetadata metadataActual, boolean genericStringMatch) {
        Assert.assertEquals("Column count must be same", metadataExpected.getColumnCount(), metadataActual.getColumnCount());
        for (int i = 0, n = metadataExpected.getColumnCount(); i < n; i++) {
            Assert.assertEquals("Column name " + i, metadataExpected.getColumnName(i), metadataActual.getColumnName(i));
            int columnType1 = metadataExpected.getColumnType(i);
            columnType1 = genericStringMatch && (ColumnType.isSymbol(columnType1) || columnType1 == ColumnType.VARCHAR
                    || columnType1 == ColumnType.CHAR) ? ColumnType.STRING : columnType1;
            int columnType2 = metadataActual.getColumnType(i);
            columnType2 = genericStringMatch && (ColumnType.isSymbol(columnType2) || columnType2 == ColumnType.VARCHAR
                    || columnType2 == ColumnType.CHAR) ? ColumnType.STRING : columnType2;
            Assert.assertEquals("Column type " + i, columnType1, columnType2);
        }
    }

    private static void assertEquals(Long256 expected, Long256 actual) {
        if (expected == actual) return;
        if (actual == null) {
            Assert.fail("Expected " + toHexString(expected) + ", but was: null");
        }

        if (
                expected.getLong0() != actual.getLong0()
                        || expected.getLong1() != actual.getLong1()
                        || expected.getLong2() != actual.getLong2()
                        || expected.getLong3() != actual.getLong3()
        ) {
            Assert.assertEquals(toHexString(expected), toHexString(actual));
        }
    }

    private static void assertEqualsRecursive(
            ArrayView expected,
            ArrayView actual,
            int dim,
            int expectedFlatIndex,
            int actualFlatIndex
    ) {
        // last dimension
        int dimLen = actual.getDimLen(dim);
        if (dim == actual.getDimCount() - 1) {
            for (int i = 0; i < dimLen; i++) {
                Assert.assertEquals(
                        expected.getDouble(expectedFlatIndex + i * expected.getStride(dim)),
                        actual.getDouble(actualFlatIndex + i * actual.getStride(dim)),
                        Numbers.TOLERANCE
                );
            }
        } else {
            for (int i = 0; i < dimLen; i++) {
                assertEqualsRecursive(
                        expected,
                        actual,
                        dim + 1,
                        expectedFlatIndex + i * expected.getStride(dim),
                        actualFlatIndex + i * actual.getStride(dim)
                );
            }
        }
    }

    private static void assertStringEquals(
            RecordMetadata metaL, RecordMetadata metaR, Record lr, Record rr, boolean genericStringMatch, int col
    ) {
        int colTypeL = metaL.getColumnType(col);
        int colTypeR = metaR.getColumnType(col);
        if (genericStringMatch && colTypeL != colTypeR) {
            if (colTypeL != ColumnType.VARCHAR && colTypeR != ColumnType.VARCHAR) {
                CharSequence left = readAsCharSequence(colTypeL, lr, col);
                CharSequence right = readAsCharSequence(colTypeR, rr, col);
                TestUtils.assertEquals(left, right);
            } else {
                if (colTypeL == ColumnType.VARCHAR) {
                    Utf8Sequence left = lr.getVarcharA(col);
                    CharSequence right = readAsCharSequence(colTypeR, rr, col);
                    if (!Utf8s.equalsUtf16Nc(right, left)) {
                        Assert.fail("Expected " + right + ", but was: " + left);
                    }
                } else {
                    CharSequence left = readAsCharSequence(colTypeL, lr, col);
                    Utf8Sequence right = rr.getVarcharA(col);
                    if (!Utf8s.equalsUtf16Nc(left, right)) {
                        Assert.fail("Expected " + right + ", but was: " + left);
                    }
                }
            }
            return;
        }

        switch (colTypeL) {
            case ColumnType.SYMBOL:
                TestUtils.assertEquals(rr.getSymA(col), lr.getSymA(col));
                break;
            case ColumnType.STRING:
                TestUtils.assertEquals(rr.getStrA(col), lr.getStrA(col));
                break;
            case ColumnType.VARCHAR:
                TestUtils.assertEquals(rr.getVarcharA(col), lr.getVarcharA(col));
                break;
            default:
                throw new UnsupportedOperationException("Unexpected column type: " + ColumnType.nameOf(colTypeL));
        }
    }

    private static Object[][] cartesianProduct(Object[][] values, int offset) {
        Object[] currentLvlValues = values[offset];
        if (currentLvlValues.length == 0) {
            throw new AssertionError("Expected at least one value [offset=" + offset + "]");
        }

        if (values.length - offset == 1) {
            // the last level
            Object[][] result = new Object[currentLvlValues.length][1];
            for (int i = 0; i < currentLvlValues.length; i++) {
                result[i][0] = currentLvlValues[i];
            }
            return result;
        }

        Object[][] lowerLvlValues = cartesianProduct(values, offset + 1);
        Object[][] res = new Object[currentLvlValues.length * lowerLvlValues.length][lowerLvlValues[0].length + 1];
        for (int i = 0; i < currentLvlValues.length; i++) {
            for (int j = 0; j < lowerLvlValues.length; j++) {
                res[i * lowerLvlValues.length + j][0] = currentLvlValues[i];
                System.arraycopy(lowerLvlValues[j], 0, res[i * lowerLvlValues.length + j], 1, lowerLvlValues[0].length);
            }
        }
        return res;
    }

    private static ObjObjHashMap<String, Long> findPartitionSizes(
            Utf8Sequence root,
            String tableName,
            CairoEngine engine,
            StringSink sink
    ) {
        ObjObjHashMap<String, Long> sizes = new ObjObjHashMap<>();
        TableToken tableToken = engine.verifyTableName(tableName);
        try (Path path = new Path().of(root).concat(tableToken)) {
            int len = path.size();
            long pFind = Files.findFirst(path.$());
            try {
                do {
                    long namePtr = Files.findName(pFind);
                    if (Files.notDots(namePtr)) {
                        sink.clear();
                        Utf8s.utf8ToUtf16Z(namePtr, sink);
                        path.trimTo(len).concat(sink).$();
                        int n = sink.length();
                        int limit = n;
                        for (int i = 0; i < n; i++) {
                            if (sink.charAt(i) == '.' && i < n - 1) {
                                char c = sink.charAt(i + 1);
                                if (c >= '0' && c <= '9') {
                                    limit = i;
                                    break;
                                }
                            }
                        }
                        sink.clear(limit);
                        sizes.put(sink.toString(), Files.getDirSize(path));
                    }
                } while (Files.findNext(pFind) > 0);
            } finally {
                Files.findClose(pFind);
            }
        }
        return sizes;
    }

    private static StringSink getTlSink() {
        StringSink ss = tlSink.get();
        ss.clear();
        return ss;
    }

    private static long partitionIncrement(TimestampDriver driver, int partitionBy, long fromTimestamp, int totalRows, int partitionCount) {
        long increment = 0;
        if (PartitionBy.isPartitioned(partitionBy)) {
            final TimestampDriver.PartitionAddMethod partitionAddMethod = PartitionBy.getPartitionAddMethod(driver.getTimestampType(), partitionBy);
            assert partitionAddMethod != null;
            long toTs = partitionAddMethod.calculate(fromTimestamp, partitionCount) - fromTimestamp - driver.fromSeconds(1);
            increment = totalRows > 0 ? Math.max(toTs / totalRows, 1) : 0;
        }
        return increment;
    }

    private static char readAsChar(RecordMetadata metaR, Record rr, int col) {
        switch (metaR.getColumnType(col)) {
            case ColumnType.CHAR:
                return rr.getChar(col);
            case ColumnType.SYMBOL:
                CharSequence symbol = rr.getSymA(0);
                Assert.assertTrue(symbol == null || symbol.length() == 1);
                return symbol == null ? 0 : symbol.charAt(0);
            case ColumnType.STRING:
                CharSequence str = rr.getStrA(col);
                Assert.assertTrue(str == null || str.length() == 1);
                return str != null ? str.charAt(0) : 0;
            case ColumnType.VARCHAR:
                Utf8Sequence vc = rr.getVarcharA(col);
                Assert.assertTrue(vc == null || vc.size() == 1);
                return vc == null ? 0 : vc.asAsciiCharSequence().charAt(0);
            default:
                throw new UnsupportedOperationException("Unexpected column type: " + ColumnType.nameOf(metaR.getColumnType(col)));
        }
    }

    @Nullable
    private static CharSequence readAsCharSequence(int columnType, Record rr, int col) {
        return switch (columnType) {
            case ColumnType.SYMBOL -> rr.getSymA(col);
            case ColumnType.STRING -> rr.getStrA(col);
            case ColumnType.VARCHAR -> {
                Utf8Sequence vc = rr.getVarcharA(col);
                yield vc == null ? null : vc.toString();
            }
            default ->
                    throw new UnsupportedOperationException("Unexpected column type: " + ColumnType.nameOf(columnType));
        };
    }

    private static String recordToString(Record record, RecordMetadata metadata, boolean genericStringMatch) {
        StringSink sink = getTlSink();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            CursorPrinter.printColumn(record, metadata, i, sink, genericStringMatch, false);
            if (i < n - 1) {
                sink.put('\t');
            }
        }
        return sink.toString();
    }

    private static CharSequence reverseLines(CharSequence expected) {
        String[] lines = expected.toString().split("\n");
        StringSink sink = new StringSink(expected.length());
        for (int i = 0, n = lines.length; i < n; i++) {
            sink.put(lines[n - i - 1]).put('\n');
        }
        return sink;
    }

    private static String toHexString(Long256 expected) {
        return Long.toHexString(expected.getLong0())
                + " " + Long.toHexString(expected.getLong1())
                + " " + Long.toHexString(expected.getLong2())
                + " " + Long.toHexString(expected.getLong3());
    }

    static void addAllRecordsToMap(StringSink sink, RecordCursor cursor, RecordMetadata metadata, Map<String, Integer> map) {
        cursor.toTop();
        Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            addRecordToMap(sink, record, metadata, map, false);
        }
    }

    static void addRecordToMap(
            StringSink sink, Record record, RecordMetadata metadata, Map<String, Integer> map, boolean genericStringMatch
    ) {
        sink.clear();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            CursorPrinter.printColumn(record, metadata, i, sink, genericStringMatch, true, "<null>");
        }
        String printed = sink.toString();
        map.compute(
                printed, (s, i) -> {
                    if (i == null) {
                        return 1;
                    }
                    return i + 1;
                }
        );
    }

    static Connection getConnectionForUser(String username, String password, int pgPort) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        return DriverManager.getConnection(getPgConnectionUri(pgPort), properties);
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

    public interface EventualCode {
        void run() throws Exception;
    }

    @FunctionalInterface
    public interface LeakProneCode {
        void run() throws Exception;
    }

    public static class LeakCheck implements QuietCloseable {
        private final int addrInfoCount;
        private final long cachedFileCount;
        private final long fileCount;
        private final String fileDebugInfo;
        private final long mem;
        private final long[] memoryUsageByTag = new long[MemoryTag.SIZE];
        private final int sockAddrCount;
        private boolean skipChecksOnClose;

        public LeakCheck() {
            Path.clearThreadLocals();
            CLOSEABLE.forEach(Misc::free);
            mem = Unsafe.getMemUsed();
            for (int i = MemoryTag.MMAP_DEFAULT; i < MemoryTag.SIZE; i++) {
                memoryUsageByTag[i] = Unsafe.getMemUsedByTag(i);
            }

            Assert.assertTrue("Initial file unsafe mem should be >= 0", mem >= 0);
            fileCount = Files.getOpenFileCount();
            cachedFileCount = Files.getOpenCachedFileCount();
            fileDebugInfo = Files.getOpenFdDebugInfo();
            Assert.assertTrue("Initial file count should be >= 0", fileCount >= 0);

            addrInfoCount = Net.getAllocatedAddrInfoCount();
            Assert.assertTrue("Initial allocated addrinfo count should be >= 0", addrInfoCount >= 0);

            sockAddrCount = Net.getAllocatedSockAddrCount();
            Assert.assertTrue("Initial allocated sockaddr count should be >= 0", sockAddrCount >= 0);
        }

        @Override
        public void close() {
            if (skipChecksOnClose) {
                return;
            }

            Path.clearThreadLocals();
            CLOSEABLE.forEach(Misc::free);
            if (cachedFileCount != Files.getOpenCachedFileCount() || fileCount != Files.getOpenFileCount()) {
                Assert.fail(
                        "expected: cached file descriptors: " + cachedFileCount +
                                ", expected OS file descriptors: " + fileCount +
                                ", list: " + fileDebugInfo +
                                " actual: cached file descriptors: " + Files.getOpenCachedFileCount() +
                                ", OS file descriptors: " + Files.getOpenFileCount() +
                                ", list: " + Files.getOpenFdDebugInfo()
                );
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
                            Assert.assertEquals(
                                    "Memory usage by tag: " + MemoryTag.nameOf(i)
                                            + ", difference: " + (actualMemByTag - memoryUsageByTag[i]),
                                    memoryUsageByTag[i], actualMemByTag
                            );
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
                Assert.fail("AddrInfo allocation count before the test: " + addrInfoCount
                        + ", after the test: " + addrInfoCountAfter);
            }

            int sockAddrCountAfter = Net.getAllocatedSockAddrCount();
            Assert.assertTrue(sockAddrCountAfter > -1);
            if (sockAddrCount != sockAddrCountAfter) {
                Assert.fail("SockAddr allocation count before the test: " + sockAddrCount
                        + ", after the test: " + sockAddrCountAfter);
            }
        }

        public void skipChecks() {
            skipChecksOnClose = true;
        }
    }
}
