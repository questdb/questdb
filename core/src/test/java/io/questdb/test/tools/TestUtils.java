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

package io.questdb.test.tools;

import io.questdb.Metrics;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cutlass.text.TextImportRequestJob;
import io.questdb.griffin.*;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogRecord;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.MutableCharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.hamcrest.MatcherAssert;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public final class TestUtils {

    public static final RecordCursorPrinter printer = new RecordCursorPrinter();

    private static final StringSink sink = new StringSink();

    private static final RecordCursorPrinter printerWithTypes = new RecordCursorPrinter().withTypes(true);

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

    public static void assertContains(CharSequence _this, CharSequence that) {
        if (Chars.contains(_this, that)) {
            return;
        }
        Assert.fail("'" + _this.toString() + "' does not contain: " + that);
    }

    public static void assertCursor(CharSequence expected, RecordCursor cursor, RecordMetadata metadata, boolean header, MutableCharSink sink) {
        printCursor(cursor, metadata, header, sink, printer);
        assertEquals(expected, sink);
    }

    public static void assertEquals(RecordCursor cursorExpected, RecordMetadata metadataExpected, RecordCursor cursorActual, RecordMetadata metadataActual, boolean symbolsAsStrings) {
        assertEquals(metadataExpected, metadataActual, symbolsAsStrings);
        Record r = cursorExpected.getRecord();
        Record l = cursorActual.getRecord();
        long rowIndex = 0;
        while (cursorExpected.hasNext()) {
            if (!cursorActual.hasNext()) {
                Assert.fail("Actual cursor does not have record at " + rowIndex);
            }
            rowIndex++;
            for (int i = 0; i < metadataExpected.getColumnCount(); i++) {
                String columnName = metadataExpected.getColumnName(i);
                try {
                    int columnType = metadataExpected.getColumnType(i);
                    int tagType = ColumnType.tagOf(columnType);
                    switch (tagType) {
                        case ColumnType.DATE:
                            Assert.assertEquals(r.getDate(i), l.getDate(i));
                            break;
                        case ColumnType.TIMESTAMP:
                            Assert.assertEquals(r.getTimestamp(i), l.getTimestamp(i));
                            break;
                        case ColumnType.DOUBLE:
                            Assert.assertEquals(r.getDouble(i), l.getDouble(i), Numbers.MAX_SCALE);
                            break;
                        case ColumnType.FLOAT:
                            Assert.assertEquals(r.getFloat(i), l.getFloat(i), 4);
                            break;
                        case ColumnType.INT:
                            Assert.assertEquals(r.getInt(i), l.getInt(i));
                            break;
                        case ColumnType.GEOINT:
                            Assert.assertEquals(r.getGeoInt(i), l.getGeoInt(i));
                            break;
                        case ColumnType.STRING:
                            CharSequence actual = symbolsAsStrings && ColumnType.isSymbol(metadataActual.getColumnType(i)) ? l.getSym(i) : l.getStr(i);
                            CharSequence expected = r.getStr(i);
                            TestUtils.assertEquals(expected, actual);
                            break;
                        case ColumnType.SYMBOL:
                            Assert.assertEquals(r.getSym(i), l.getSym(i));
                            break;
                        case ColumnType.SHORT:
                            Assert.assertEquals(r.getShort(i), l.getShort(i));
                            break;
                        case ColumnType.CHAR:
                            Assert.assertEquals(r.getChar(i), l.getChar(i));
                            break;
                        case ColumnType.GEOSHORT:
                            Assert.assertEquals(r.getGeoShort(i), l.getGeoShort(i));
                            break;
                        case ColumnType.LONG:
                            Assert.assertEquals(r.getLong(i), l.getLong(i));
                            break;
                        case ColumnType.GEOLONG:
                            Assert.assertEquals(r.getGeoLong(i), l.getGeoLong(i));
                            break;
                        case ColumnType.GEOBYTE:
                            Assert.assertEquals(r.getGeoByte(i), l.getGeoByte(i));
                            break;
                        case ColumnType.BYTE:
                            Assert.assertEquals(r.getByte(i), l.getByte(i));
                            break;
                        case ColumnType.BOOLEAN:
                            Assert.assertEquals(r.getBool(i), l.getBool(i));
                            break;
                        case ColumnType.BINARY:
                            Assert.assertTrue(areEqual(r.getBin(i), l.getBin(i)));
                            break;
                        case ColumnType.LONG256:
                            assertEquals(r.getLong256A(i), l.getLong256A(i));
                            break;
                        default:
                            // Unknown record type.
                            assert false;
                            break;
                    }
                } catch (AssertionError e) {
                    throw new AssertionError(String.format("Row %d column %s %s", rowIndex, columnName, e.getMessage()));
                }
            }
        }

        Assert.assertFalse("Expected cursor misses record " + rowIndex, cursorActual.hasNext());
    }

    public static void assertEquals(File a, File b) {
        try (Path path = new Path()) {
            path.of(a.getAbsolutePath()).$();
            long fda = Files.openRO(path);
            Assert.assertNotEquals(-1, fda);

            try {
                path.of(b.getAbsolutePath()).$();
                long fdb = Files.openRO(path);
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
                    Files.close(fdb);
                }
            } finally {
                Files.close(fda);
            }
        }
    }

    public static void assertEquals(File a, CharSequence actual) {
        try (Path path = new Path()) {
            path.of(a.getAbsolutePath()).$();
            long fda = Files.openRO(path);
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
                Files.close(fda);
            }
        }
    }

    public static void assertEquals(CharSequence expected, Sinkable actual) {
        sink.clear();
        actual.toSink(sink);
        assertEquals(null, expected, sink);
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
        try (
                BufferedInputStream expectedStream = new BufferedInputStream(new FileInputStream(expected.toString()));
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

    public static void assertIndexBlockCapacity(SqlExecutionContext sqlExecutionContext, CairoEngine engine, String tableName, String columnName) {

        engine.releaseAllReaders();
        try (TableReader rdr = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
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
        Assert.assertTrue("Initial file count should be >= 0", fileCount >= 0);

        int addrInfoCount = Net.getAllocatedAddrInfoCount();
        Assert.assertTrue("Initial allocated addrinfo count should be >= 0", addrInfoCount >= 0);

        int sockAddrCount = Net.getAllocatedSockAddrCount();
        Assert.assertTrue("Initial allocated sockaddr count should be >= 0", sockAddrCount >= 0);

        runnable.run();
        Path.clearThreadLocals();
        if (fileCount != Files.getOpenFileCount()) {
            Assert.assertEquals("file descriptors " + Files.getOpenFdDebugInfo(), fileCount, Files.getOpenFileCount());
        }

        // Checks that the same tag used for allocation and freeing native memory
        long memAfter = Unsafe.getMemUsed();
        Assert.assertTrue(memAfter > -1);
        if (mem != memAfter) {
            for (int i = MemoryTag.MMAP_DEFAULT; i < MemoryTag.SIZE; i++) {
                long actualMemByTag = Unsafe.getMemUsedByTag(i);
                if (memoryUsageByTag[i] != actualMemByTag) {
                    Assert.assertEquals("Memory usage by tag: " + MemoryTag.nameOf(i) + ", difference: " + (actualMemByTag - memoryUsageByTag[i]), memoryUsageByTag[i], actualMemByTag);
                    Assert.assertTrue(actualMemByTag > -1);
                }
            }
            Assert.assertEquals(mem, memAfter);
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

    public static void assertSqlCursors(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String expected, String actual, Log log) throws SqlException {
        assertSqlCursors(compiler, sqlExecutionContext, expected, actual, log, false);
    }

    public static void assertSqlCursors(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String expected, String actual, Log log, boolean symbolsAsStrings) throws SqlException {
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

    public static long connect(long fd, long sockAddr) {
        Assert.assertTrue(fd > -1);
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
        FilesFacade ff = FilesFacadeImpl.INSTANCE;
        Assert.assertEquals(0, ff.copyRecursive(src, dst, dirMode));
    }

    public static void copyMimeTypes(String targetDir) throws IOException {
        try (InputStream stream = TestUtils.class.getResourceAsStream("/site/conf/mime.types")) {
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
        long increment = partitionIncrement(tableModel, fromTimestamp, totalRows, partitionCount);
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
        return sql.toString();
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

    public static void drainTextImportJobQueue(CairoEngine engine) throws Exception {
        try (TextImportRequestJob processingJob = new TextImportRequestJob(engine, 1, null)) {
            while (processingJob.run(0)) {
                Os.pause();
            }
        }
    }

    public static void execute(
            @Nullable WorkerPool pool,
            CustomisableRunnable runnable,
            CairoConfiguration configuration,
            Metrics metrics,
            Log log
    ) throws Exception {
        final int workerCount = pool != null ? pool.getWorkerCount() : 1;
        try (
                final CairoEngine engine = new CairoEngine(configuration, metrics);
                final SqlCompiler compiler = new SqlCompiler(engine);
                final SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, workerCount)
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

    public static void execute(
            @Nullable WorkerPool pool,
            CustomisableRunnable runner,
            CairoConfiguration configuration,
            Log log
    ) throws Exception {
        execute(pool, runner, configuration, Metrics.disabled(), log);
    }

    @NotNull
    public static Rnd generateRandom() {
        long s0 = System.nanoTime();
        long s1 = System.currentTimeMillis();
        return new Rnd(s0, s1);
    }

    @NotNull
    public static Rnd generateRandom(Log log) {
        long s0 = System.nanoTime();
        long s1 = System.currentTimeMillis();
        log.info().$("random seeds: ").$(s0).$("L, ").$(s1).$('L').$();
        return new Rnd(s0, s1);
    }

    public static String getCsvRoot() {
        URL rootSource = TestUtils.class.getResource("/csv/test-import.csv");
        try {
            assert rootSource != null : "huh, somebody deleted from test-import.csv?";
            return new File(rootSource.toURI()).getParent();
        } catch (URISyntaxException e) {
            throw new AssertionError("missing test-import.csv", e);
        }
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

    @NotNull
    public static NetworkFacade getSendDelayNetworkFacade(int startDelayDelayAfter) {
        return new NetworkFacadeImpl() {
            final AtomicInteger totalSent = new AtomicInteger();

            @Override
            public int send(long fd, long buffer, int bufferLen) {
                if (startDelayDelayAfter == 0) {
                    return super.send(fd, buffer, bufferLen);
                }

                int sentNow = totalSent.get();
                if (bufferLen > 0) {
                    if (sentNow >= startDelayDelayAfter) {
                        totalSent.set(0);
                        return 0;
                    }

                    int result = super.send(fd, buffer, Math.min(bufferLen, startDelayDelayAfter - sentNow));
                    totalSent.addAndGet(result);
                    return result;
                }
                return 0;
            }
        };
    }

    public static void insert(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, CharSequence insertSql) throws SqlException {
        CompiledQuery compiledQuery = compiler.compile(insertSql, sqlExecutionContext);
        Assert.assertNotNull(compiledQuery.getInsertOperation());
        final InsertOperation insertOperation = compiledQuery.getInsertOperation();
        try (InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)) {
            insertMethod.execute();
            insertMethod.commit();
        }
    }

    public static String insertFromSelectPopulateTableStmt(
            TableModel tableModel,
            int totalRows,
            String startDate,
            int partitionCount
    ) throws NumericException {
        long fromTimestamp = IntervalUtils.parseFloorPartialTimestamp(startDate);
        long increment = partitionIncrement(tableModel, fromTimestamp, totalRows, partitionCount);

        StringBuilder insertFromSelect = new StringBuilder();
        insertFromSelect.append("INSERT INTO ").append(tableModel.getTableName()).append(" SELECT").append(Misc.EOL);
        for (int i = 0; i < tableModel.getColumnCount(); i++) {
            CharSequence colName = tableModel.getColumnName(i);
            switch (ColumnType.tagOf(tableModel.getColumnType(i))) {
                case ColumnType.INT:
                    insertFromSelect.append("cast(x as int) ").append(colName);
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

    public static void printColumn(Record r, RecordMetadata m, int i, CharSink sink) {
        printColumn(r, m, i, sink, false);
    }

    public static void printColumn(Record r, RecordMetadata m, int i, CharSink sink, boolean printTypes) {
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
                r.getStr(i, sink);
                break;
            case ColumnType.SYMBOL:
                sink.put(r.getSym(i));
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
                long long128Hi = r.getLong128Hi(i);
                long long128Lo = r.getLong128Lo(i);
                if (!Long128Util.isNull(long128Hi, long128Lo)) {
                    UUID guid = new UUID(long128Hi, long128Lo);
                    sink.put(guid.toString());
                }
                break;
            default:
                break;
        }
        if (printTypes) {
            sink.put(':').put(ColumnType.nameOf(columnType));
        }
    }

    public static void printCursor(RecordCursor cursor, RecordMetadata metadata, boolean header, MutableCharSink sink, RecordCursorPrinter printer) {
        sink.clear();
        printer.print(cursor, metadata, header, sink);
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
        Path path = Path.getThreadLocal(root);
        MatcherAssert.assertThat("Test dir cleanup", Files.rmdir(path.slash$()), is(lessThanOrEqualTo(0)));
    }

    public static void runWithTextImportRequestJob(CairoEngine engine, LeakProneCode task) throws Exception {
        WorkerPoolConfiguration config = new WorkerPoolConfiguration() {
            @Override
            public int[] getWorkerAffinity() {
                return new int[1];
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public boolean haltOnError() {
                return true;
            }
        };
        WorkerPool pool = new WorkerPool(config, Metrics.disabled());
        TextImportRequestJob processingJob = new TextImportRequestJob(engine, 1, null);
        try {
            pool.assign(processingJob);
            pool.freeOnHalt(processingJob);
            pool.start(null);
            task.run();
        } finally {
            pool.halt();
        }
    }

    public static void setupWorkerPool(WorkerPool workerPool, CairoEngine cairoEngine) throws SqlException {
        workerPool.assignCleaner(Path.CLEANER);
        O3Utils.setupWorkerPool(workerPool, cairoEngine, null, null);
    }

    public static long toMemory(CharSequence sequence) {
        long ptr = Unsafe.malloc(sequence.length(), MemoryTag.NATIVE_DEFAULT);
        Chars.asciiStrCpy(sequence, sequence.length(), ptr);
        return ptr;
    }

    // used in tests
    public static void writeStringToFile(File file, String s) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(s.getBytes(Files.UTF_8));
        }
    }

    private static long partitionIncrement(TableModel tableModel, long fromTimestamp, int totalRows, int partitionCount) {
        long increment = 0;
        if (PartitionBy.isPartitioned(tableModel.getPartitionBy())) {
            final PartitionBy.PartitionAddMethod partitionAddMethod = PartitionBy.getPartitionAddMethod(tableModel.getPartitionBy());
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

    private static String toHexString(Long256 expected) {
        return Long.toHexString(expected.getLong0()) + " " +
                Long.toHexString(expected.getLong1()) + " " +
                Long.toHexString(expected.getLong2()) + " " +
                Long.toHexString(expected.getLong3());
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

    @FunctionalInterface
    public interface LeakProneCode {
        void run() throws Exception;
    }
}
