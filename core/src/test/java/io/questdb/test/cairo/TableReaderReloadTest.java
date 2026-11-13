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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.ColumnVersionWriter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.cairo.TableReaderTest.assertOpenPartitionCount;

public class TableReaderReloadTest extends AbstractCairoTest {

    @Test
    public void testReloadAcrossColumnVersionRolledBackOnWriterOpen() throws Exception {
        // Commit F published _cv and stopped before its _txn commit, as a crash or a writer that failed between the
        // two leaves it. The next writer rolls _cv back to the version _txn names when it opens, and its commit G
        // publishes F's version number again, with other column versions. A reader that read F's _cv and stalls
        // until G has committed finds G's _txn naming the version number it saw: it must end with G's column
        // versions, not F's.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE nt (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO nt VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-02T00:00:00.000000Z'),
                    (4, '2024-01-02T01:00:00.000000Z'),
                    (7, '2024-01-03T00:00:00.000000Z')""");
            execute("ALTER TABLE nt ADD COLUMN c INT");
            execute("INSERT INTO nt VALUES (8, '2024-01-03T01:00:00.000000Z', 80)");
            final TableToken token = engine.verifyTableName("nt");
            final long firstPartitionTimestamp;
            final int columnIndex;
            final long columnNameTxn;
            try (TableReader reader = getReader(token)) {
                firstPartitionTimestamp = reader.getPartitionTimestampByIndex(0);
                columnIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("c"));
                columnNameTxn = reader.getColumnVersionReader().getDefaultColumnNameTxn(columnIndex);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            final long columnVersion;
            try (
                    Path path = new Path().of(configuration.getDbRoot()).concat(token).concat(TableUtils.COLUMN_VERSION_FILE_NAME);
                    ColumnVersionWriter columnVersionWriter = new ColumnVersionWriter(configuration, path.$(), true)
            ) {
                columnVersion = columnVersionWriter.getVersion();
                // F: column c of the first partition holds data from its first row
                columnVersionWriter.upsert(firstPartitionTimestamp, columnIndex, columnNameTxn, 0);
                columnVersionWriter.commit();
            }
            Assert.assertEquals(columnVersion + 1, readColumnVersionFileVersion(token));

            final CountDownLatch readerStalled = new CountDownLatch(1);
            final CountDownLatch readerRelease = new CountDownLatch(1);
            final AtomicBoolean isStallArmed = new AtomicBoolean(true);
            final AtomicReference<Throwable> readerError = new AtomicReference<>();
            final MicrosecondClock clock = testMicrosClock;
            final Thread readerThread = new Thread(() -> {
                try {
                    // the pool keeps the reader built across the writer's rollback
                    engine.getReader(token).close();
                } catch (Throwable th) {
                    readerError.set(th);
                } finally {
                    Path.clearThreadLocals();
                }
            });
            // Stalls the reader thread on the deadline check of TableReader.reloadColumnVersion(), which it reaches
            // after it read a _cv ahead of the _txn it loaded: a reader descheduled between two polls of its reload.
            testMicrosClock = () -> {
                final long ticks = clock.getTicks();
                if (Thread.currentThread() == readerThread && isCalledFromColumnVersionReload() && isStallArmed.compareAndSet(true, false)) {
                    readerStalled.countDown();
                    awaitLatch(readerRelease);
                }
                return ticks;
            };
            readerThread.start();
            try {
                awaitLatch(readerStalled);
                // G: the writer opens, rolls _cv back, and merges a row into 2024-01-02, which predates column c
                execute("INSERT INTO nt VALUES (6, '2024-01-02T00:30:00.000000Z', 60)");
                Assert.assertEquals("G must reuse the version number of F", columnVersion + 1, readColumnVersionFileVersion(token));
            } finally {
                readerRelease.countDown();
                readerThread.join();
                testMicrosClock = clock;
            }
            if (readerError.get() != null) {
                throw new AssertionError(readerError.get());
            }

            // the pool hands out the reader built across the rollback first
            final String pooledColumnVersions;
            try (TableReader pooled = getReader(token)) {
                Assert.assertEquals(columnVersion + 1, pooled.getColumnVersionReader().getVersion());
                pooledColumnVersions = pooled.getColumnVersionReader().toString();
            }
            try (TableReader fresh = newOffPoolReader(configuration, "nt")) {
                Assert.assertEquals(fresh.getColumnVersionReader().toString(), pooledColumnVersions);
            }
            assertQuery("SELECT v, ts, c FROM nt")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            v\tts\tc
                            1\t2024-01-01T00:00:00.000000Z\tnull
                            2\t2024-01-01T01:00:00.000000Z\tnull
                            3\t2024-01-02T00:00:00.000000Z\tnull
                            6\t2024-01-02T00:30:00.000000Z\t60
                            4\t2024-01-02T01:00:00.000000Z\tnull
                            7\t2024-01-03T00:00:00.000000Z\tnull
                            8\t2024-01-03T01:00:00.000000Z\t80
                            """);
        });
    }

    @Test
    public void testReloadAfterDropBitmapIndexedColumnBeforePostingIndexedColumn() throws Exception {
        testReloadAfterDropIndexedColumnBeforeIndexedColumn("BITMAP", "POSTING");
    }

    @Test
    public void testReloadAfterDropPostingIndexedColumnBeforeBitmapIndexedColumn() throws Exception {
        testReloadAfterDropIndexedColumnBeforeIndexedColumn("POSTING", "BITMAP");
    }

    @Test
    public void testReloadTruncateByDay() {
        testReloadAfterTruncate(PartitionBy.DAY, 3000000000L, false);
    }

    @Test
    public void testReloadTruncateByMonth() {
        testReloadAfterTruncate(PartitionBy.MONTH, 50000000000L, false);
    }

    @Test
    public void testReloadTruncateByNone() {
        testReloadAfterTruncate(PartitionBy.NONE, 1000000, false);
    }

    @Test
    public void testReloadTruncateByWeek() {
        testReloadAfterTruncate(PartitionBy.WEEK, 7 * 3000000000L, false);
    }

    @Test
    public void testReloadTruncateByYear() {
        testReloadAfterTruncate(PartitionBy.YEAR, 365 * 50000000000L, false);
    }

    @Test
    public void testReloadTruncateKeepSymbolTables() {
        testReloadAfterTruncate(PartitionBy.DAY, 3000000000L, true);
    }

    @Test
    public void testTruncateInsertReloadDay() {
        testTruncateInsertReload(PartitionBy.DAY, 3000000000L, false);
    }

    @Test
    public void testTruncateInsertReloadKeepSymbolTables() {
        testTruncateInsertReload(PartitionBy.DAY, 3000000000L, true);
    }

    @Test
    public void testTruncateInsertReloadMonth() {
        testTruncateInsertReload(PartitionBy.MONTH, 50000000000L, false);
    }

    @Test
    public void testTruncateInsertReloadNone() {
        testTruncateInsertReload(PartitionBy.NONE, 1000000L, false);
    }

    @Test
    public void testTruncateInsertReloadWeek() {
        testTruncateInsertReload(PartitionBy.WEEK, 7 * 3000000000L, false);
    }

    @Test
    public void testTruncateInsertReloadYear() {
        testTruncateInsertReload(PartitionBy.YEAR, 365 * 50000000000L, false);
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("latch timeout");
            }
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    // Whether TableReader.reloadColumnVersion() reads the clock, under the clock wrappers of the test
    private static boolean isCalledFromColumnVersionReload() {
        return StackWalker.getInstance().walk(frames -> frames
                .filter(f -> !f.getClassName().startsWith(TableReaderReloadTest.class.getName())
                        && !f.getClassName().startsWith(CairoTestConfiguration.class.getName()))
                .findFirst()
                .map(f -> TableReader.class.getName().equals(f.getClassName()) && "reloadColumnVersion".equals(f.getMethodName()))
                .orElse(false));
    }

    private static long readColumnVersionFileVersion(TableToken token) {
        try (
                Path path = new Path().of(configuration.getDbRoot()).concat(token).concat(TableUtils.COLUMN_VERSION_FILE_NAME);
                ColumnVersionReader columnVersionReader = new ColumnVersionReader().ofRO(configuration.getFilesFacade(), path.$())
        ) {
            return columnVersionReader.readUnsafe();
        }
    }

    private void assertIndexScans(String column) throws Exception {
        final String expected = "ts\t" + column + "\n"
                + "2024-01-01T00:00:00.000000Z\tx\n"
                + "2024-01-02T00:00:00.000000Z\tx\n";
        assertQuery("SELECT ts, " + column + " FROM t WHERE " + column + " = 'x'")
                .noLeakCheck()
                .timestamp("ts")
                .withPlanContaining("Index forward scan on: " + column)
                .returns(expected);
        final String expectedDesc = "ts\t" + column + "\n"
                + "2024-01-02T00:00:00.000000Z\tx\n"
                + "2024-01-01T00:00:00.000000Z\tx\n";
        assertQuery("SELECT ts, " + column + " FROM t WHERE " + column + " = 'x' ORDER BY ts DESC")
                .noLeakCheck()
                .timestampDesc("ts")
                .withPlanContaining("Index backward scan on: " + column)
                .returns(expectedDesc);
    }

    private void assertTable(Rnd rnd, long buffer, RecordCursor cursor, Record record) {
        while (cursor.hasNext()) {
            Assert.assertEquals(rnd.nextInt(), record.getInt(0));
            Assert.assertEquals(rnd.nextShort(), record.getShort(1));
            Assert.assertEquals(rnd.nextByte(), record.getByte(2));
            Assert.assertEquals(rnd.nextDouble(), record.getDouble(3), 0.00001);
            Assert.assertEquals(rnd.nextFloat(), record.getFloat(4), 0.00001);
            Assert.assertEquals(rnd.nextLong(), record.getLong(5));
            TestUtils.assertEquals(rnd.nextChars(3), record.getStrA(6));
            TestUtils.assertEquals(rnd.nextChars(2), record.getSymA(7));
            Assert.assertEquals(rnd.nextBoolean(), record.getBool(8));

            rnd.nextChars(buffer, 1024 / 2);
            Assert.assertEquals(rnd.nextLong(), record.getDate(10));
        }
    }

    private void populateTable(Rnd rnd, long buffer, long timestamp, long increment, TableWriter writer) {
        for (int i = 0; i < 100; i++) {
            TableWriter.Row row = writer.newRow(timestamp);
            row.putInt(0, rnd.nextInt());
            row.putShort(1, rnd.nextShort());
            row.putByte(2, rnd.nextByte());
            row.putDouble(3, rnd.nextDouble());
            row.putFloat(4, rnd.nextFloat());
            row.putLong(5, rnd.nextLong());
            row.putStr(6, rnd.nextChars(3));
            row.putSym(7, rnd.nextChars(2));
            row.putBool(8, rnd.nextBoolean());
            rnd.nextChars(buffer, 1024 / 2);
            row.putBin(9, buffer, 1024);
            row.putDate(10, rnd.nextLong());
            row.append();
            timestamp += increment;
        }
        writer.commit();
    }

    private void testReloadAfterDropIndexedColumnBeforeIndexedColumn(String droppedIndexType, String keptIndexType) throws Exception {
        // Dropping the first column moves the second one into its slot of the pooled reader.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, a SYMBOL INDEX TYPE " + droppedIndexType
                    + ", b SYMBOL INDEX TYPE " + keptIndexType + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'x', 'x'), ('2024-01-02T00:00:00', 'x', 'x')");
            assertIndexScans("a");
            assertIndexScans("b");
            execute("ALTER TABLE t DROP COLUMN a");
            assertIndexScans("b");
        });
    }

    private void testReloadAfterTruncate(int partitionBy, long increment, boolean keepSymbolTables) {
        if (Os.isWindows()) {
            return;
        }
        final Rnd rnd = new Rnd();
        final int bufferSize = 1024;
        long buffer = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        TableModel model = CreateTableTestUtils.getAllTypesModel(configuration, partitionBy);
        model.timestamp();
        AbstractCairoTest.create(model);

        long timestamp = 0;
        try (TableWriter writer = newOffPoolWriter(configuration, "all")) {
            try (TableReader reader = newOffPoolReader(configuration, "all")) {
                Assert.assertFalse(reader.reload());
            }

            populateTable(rnd, buffer, timestamp, increment, writer);
            rnd.reset();

            try (
                    TableReader reader = newOffPoolReader(configuration, "all");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                final Record record = cursor.getRecord();
                assertTable(rnd, buffer, cursor, record);
                assertOpenPartitionCount(reader);

                if (keepSymbolTables) {
                    writer.truncateSoft();
                } else {
                    writer.truncate();
                }
                Assert.assertTrue(reader.reload());
                assertOpenPartitionCount(reader);
                cursor.toTop();
                Assert.assertFalse(cursor.hasNext());

                rnd.reset();
                populateTable(rnd, buffer, timestamp, increment, writer);
                Assert.assertTrue(reader.reload());
                assertOpenPartitionCount(reader);

                rnd.reset();
                cursor.toTop();
                assertTable(rnd, buffer, cursor, record);
                assertOpenPartitionCount(reader);
            }
        }
    }

    private void testTruncateInsertReload(int partitionBy, long increment, boolean keepSymbolTables) {
        if (Os.isWindows()) {
            return;
        }

        final Rnd rnd = new Rnd();
        final int bufferSize = 1024;
        int timestampType = TestUtils.generateRandom(LOG).nextBoolean() ? ColumnType.TIMESTAMP_MICRO : ColumnType.TIMESTAMP_NANO;
        TimestampDriver timestampDriver = ColumnType.getTimestampDriver(timestampType);
        increment = timestampDriver.fromMicros(increment);
        long buffer = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        TableModel model = CreateTableTestUtils.getAllTypesModel(configuration, partitionBy);
        model.timestamp(timestampType);
        AbstractCairoTest.create(model);

        long timestamp = 0;
        try (TableWriter writer = newOffPoolWriter(configuration, "all")) {
            try (TableReader reader = newOffPoolReader(configuration, "all")) {
                Assert.assertFalse(reader.reload());
            }

            populateTable(rnd, buffer, timestamp, increment, writer);
            rnd.reset();

            try (
                    TableReader reader = newOffPoolReader(configuration, "all");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                final Record record = cursor.getRecord();
                assertTable(rnd, buffer, cursor, record);
                assertOpenPartitionCount(reader);

                if (keepSymbolTables) {
                    writer.truncateSoft();
                } else {
                    writer.truncate();
                }

                // Write different data
                rnd.reset(123, 123);
                populateTable(rnd, buffer, timestamp, increment / 2, writer);
                Assert.assertTrue(reader.reload());
                assertOpenPartitionCount(reader);

                // Assert the data is what was written the second time
                rnd.reset(123, 123);
                cursor.toTop();
                assertTable(rnd, buffer, cursor, record);
                assertOpenPartitionCount(reader);
            }
        }
    }
}
