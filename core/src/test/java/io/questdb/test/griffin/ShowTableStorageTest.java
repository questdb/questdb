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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableFlagResolver;
import io.questdb.cairo.TableNameRegistry;
import io.questdb.cairo.TableNameRegistryRW;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class ShowTableStorageTest extends AbstractCairoTest {

    @Test
    public void testAllPartitionsStorageForMultipleTablesPartitionByHour() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades_1(timestamp TIMESTAMP, " +
                    "id SYMBOL , price INT)TIMESTAMP(timestamp) PARTITION BY HOUR;");
            execute("create table trades_2(timestamp TIMESTAMP, " +
                    "id SYMBOL , price INT)TIMESTAMP(timestamp) PARTITION BY HOUR;");
            execute(
                    """
                            INSERT INTO trades_1
                            VALUES
                                ('2021-10-05T11:31:35.878Z', 's1', 245),
                                ('2021-10-05T12:31:35.878Z', 's2', 245),
                                ('2021-10-05T13:31:35.878Z', 's3', 250),
                                ('2021-10-05T14:31:35.878Z', 's4', 250);"""
            );
            execute(
                    """
                            INSERT INTO trades_2
                            VALUES
                                ('2021-10-05T11:31:35.878Z', 's1', 245),
                                ('2021-10-05T12:31:35.878Z', 's2', 245),
                                ('2021-10-05T13:31:35.878Z', 's3', 250),
                                ('2021-10-05T14:31:35.878Z', 's4', 250);"""
            );
            drainWalQueue();
            engine.releaseAllWriters();
            final CharSequence size1 = Long.toString(getDirSize("trades_1"));
            final CharSequence size2 = Long.toString(getDirSize("trades_2"));
            assertQuery("select * from table_storage()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("tableName\twalEnabled\tpartitionBy\tpartitionCount\trowCount\tdiskSize\n" +
                            "trades_2\tfalse\tHOUR\t4\t4\t" + size1 + "\n" +
                            "trades_1\tfalse\tHOUR\t4\t4\t" + size2 + "\n");
        });
    }

    @Test
    public void testAllPartitionsStorageForMultipleTablesWithNoPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades_1(timestamp TIMESTAMP, " +
                    "id SYMBOL , price INT)TIMESTAMP(timestamp);");
            execute("create table trades_2(timestamp TIMESTAMP, " +
                    "id SYMBOL , price INT)TIMESTAMP(timestamp);");
            execute(
                    """
                            INSERT INTO trades_1
                            VALUES
                                ('2021-10-05T11:31:35.878Z', 's1', 245),
                                ('2021-10-05T12:31:35.878Z', 's2', 245),
                                ('2021-10-05T13:31:35.878Z', 's3', 250),
                                ('2021-10-05T14:31:35.878Z', 's4', 250);"""
            );
            execute(
                    """
                            INSERT INTO trades_2
                            VALUES
                                ('2021-10-05T11:31:35.878Z', 's1', 245),
                                ('2021-10-05T12:31:35.878Z', 's2', 245),
                                ('2021-10-05T13:31:35.878Z', 's3', 250),
                                ('2021-10-05T14:31:35.878Z', 's4', 250);"""
            );
            drainWalQueue();
            engine.releaseAllWriters();
            final CharSequence size1 = Long.toString(getDirSize("trades_1"));
            final CharSequence size2 = Long.toString(getDirSize("trades_2"));
            assertQuery("select * from table_storage()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("tableName\twalEnabled\tpartitionBy\tpartitionCount\trowCount\tdiskSize\n" +
                            "trades_2\tfalse\tNONE\t1\t4\t" + size1 + "\n" +
                            "trades_1\tfalse\tNONE\t1\t4\t" + size2 + "\n");
        });
    }

    @Test
    public void testAllPartitionsStorageForSingleTablePartitionByHour() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades_1(timestamp TIMESTAMP, " +
                    "id SYMBOL , price INT)TIMESTAMP(timestamp) PARTITION BY HOUR;");
            execute(
                    """
                            INSERT INTO trades_1
                            VALUES
                                ('2021-10-05T11:31:35.878Z', 's1', 245),
                                ('2021-10-05T12:31:35.878Z', 's2', 245),
                                ('2021-10-05T13:31:35.878Z', 's3', 250),
                                ('2021-10-05T14:31:35.878Z', 's4', 250);
                            """
            );
            drainWalQueue();
            engine.releaseAllWriters();
            engine.releaseAllWriters();
            final CharSequence size = Long.toString(getDirSize("trades_1"));
            assertQuery("select * from table_storage()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("tableName\twalEnabled\tpartitionBy\tpartitionCount\trowCount\tdiskSize\n" +
                            "trades_1\tfalse\tHOUR\t4\t4\t" + size + "\n");
        });
    }

    @Test
    public void testAllPartitionsStorageForSingleTableWithNoPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades_1(timestamp TIMESTAMP, " +
                    "id SYMBOL , price INT)TIMESTAMP(timestamp);");
            execute(
                    """
                            INSERT INTO trades_1
                            VALUES
                                ('2021-10-05T11:31:35.878Z', 's1', 245),
                                ('2021-10-05T12:31:35.878Z', 's2', 245),
                                ('2021-10-05T13:31:35.878Z', 's3', 250),
                                ('2021-10-05T14:31:35.878Z', 's4', 250);"""
            );
            drainWalQueue();
            engine.releaseAllWriters();
            final CharSequence size = Long.toString(getDirSize("trades_1"));
            assertQuery("select * from table_storage()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("tableName\twalEnabled\tpartitionBy\tpartitionCount\trowCount\tdiskSize\n" +
                            "trades_1\tfalse\tNONE\t1\t4\t" + size + "\n");
        });
    }

    @Test
    public void testCountExcludesSystemTables() throws Exception {
        assertMemoryLeak(() -> {
            createTable("x", false);
            createTable(configuration.getSystemTableNamePrefix() + "x", false);
            // size() of the cursor matches the rows it returns, so count() and LIMIT -N skip system tables too
            assertQuery("SELECT count() FROM table_storage()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n1\n");
            assertQuery("SELECT tableName FROM table_storage() LIMIT -1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("tableName\nx\n");
        });
    }

    @Test
    public void testFetchNonExistingColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades_1(timestamp TIMESTAMP, " +
                    "id SYMBOL , price INT)TIMESTAMP(timestamp) PARTITION BY HOUR;");
            execute(
                    """
                            INSERT INTO trades_1
                            VALUES
                                ('2021-10-05T11:31:35.878Z', 's1', 245),
                                ('2021-10-05T12:31:35.878Z', 's2', 245),
                                ('2021-10-05T13:31:35.878Z', 's3', 250),
                                ('2021-10-05T14:31:35.878Z', 's4', 250);
                            """
            );
            drainWalQueue();
            engine.releaseAllWriters();
            assertQuery("select *, size_pretty(hello) from table_storage()")
                    .fails(22, "Invalid column: hello");
        });
    }

    @Test
    public void testFilterOnTableNameWalksOnlyMatchingTable() throws Exception {
        final DirListingRecordingFilesFacade ff = new DirListingRecordingFilesFacade();
        assertMemoryLeak(ff, () -> {
            createTable("x", false);
            createTable("y", false);
            final String expected = "tableName\tdiskSize\nx\t" + getDirSize("x") + "\n";
            ff.listedDirs.clear();
            assertQuery("SELECT tableName, diskSize FROM table_storage() WHERE tableName = 'x'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);
            Assert.assertTrue(ff.listedDirs.size() > 0);
            final String yDir = Files.SEPARATOR + engine.verifyTableName("y").getDirName();
            for (int i = 0, n = ff.listedDirs.size(); i < n; i++) {
                final String dir = ff.listedDirs.getQuick(i);
                Assert.assertFalse(dir, dir.endsWith(yDir) || dir.contains(yDir + Files.SEPARATOR));
            }
        });
    }

    @Test
    public void testLiveViewListed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:00:00.000000Z')");
            drainWalQueue();
            // CREATE LIVE VIEW registers the name in a separate step after it creates the files,
            // and the listing shows the live view once that step completes
            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS
                    SELECT sym, val, ts, sum(val) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) AS cum
                    FROM base
                    """);
            assertQuery("SELECT tableName, walEnabled, partitionBy, partitionCount, rowCount FROM table_storage() WHERE tableName = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            tableName\twalEnabled\tpartitionBy\tpartitionCount\trowCount
                            lv\ttrue\tHOUR\t0\t0
                            """);
        });
    }

    @Test
    public void testProjectionWithoutDiskSizeDoesNotListDirectories() throws Exception {
        final DirListingRecordingFilesFacade ff = new DirListingRecordingFilesFacade();
        assertMemoryLeak(ff, () -> {
            createTable("x", false);
            createTable("y", true);
            ff.listedDirs.clear();
            assertQuery("SELECT tableName, walEnabled, partitionBy, partitionCount, rowCount FROM table_storage() ORDER BY tableName")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            tableName\twalEnabled\tpartitionBy\tpartitionCount\trowCount
                            x\tfalse\tDAY\t3\t3
                            y\ttrue\tDAY\t3\t3
                            """);
            Assert.assertEquals(0, ff.listedDirs.size());
            Assert.assertEquals(0, engine.getTableDiskSizeCache().getTableCount());
        });
    }

    @Test
    public void testTableDroppedBeforeMetadataRead() throws Exception {
        assertRowOfVanishingTable("DROP TABLE x", false);
    }

    @Test
    public void testTableDroppedBeforeTxnRead() throws Exception {
        assertRowOfVanishingTable("DROP TABLE x", true);
    }

    @Test
    public void testTableDroppedDuringIteration() throws Exception {
        assertMemoryLeak(() -> {
            createTable("a", false);
            createTable("b", false);
            assertRowOfVanishedTable("DROP TABLE %s");
        });
    }

    @Test
    public void testTableListedWhileItsNameResolvesToEqualToken() throws Exception {
        // the read-only registry swaps its maps on reload, so the listing and the name can hold
        // equal tokens that are distinct objects: the listing compares the tokens by value
        assertListingWhileNameResolvesTo(token -> token.renamed(token.getTableName()), true);
    }

    @Test
    public void testTableNotListedBeforeItsNameResolves() throws Exception {
        // CREATE publishes the table to the listing before its name resolves: reading the row
        // would fail on a name the engine cannot resolve yet
        assertListingWhileNameResolvesTo(token -> TableNameRegistry.LOCKED_TOKEN, false);
    }

    @Test
    public void testTableNotListedWhileItsNameIsDropped() throws Exception {
        // DROP holds the name locked while it retires it
        assertListingWhileNameResolvesTo(token -> TableNameRegistry.LOCKED_DROP_TOKEN, false);
    }

    @Test
    public void testTableNotListedWhileItsNameResolvesToAnotherTable() throws Exception {
        // the listing still holds a table that a new table with the same name replaced
        assertListingWhileNameResolvesTo(
                token -> new TableToken(token.getTableName(), token.getDirName(), null, token.getTableId() + 1, token.isWal(), false, false),
                false
        );
    }

    @Test
    public void testTableRecreatedDuringIteration() throws Exception {
        assertMemoryLeak(() -> {
            createTable("a", false);
            createTable("b", false);
            // the new table takes the directory of the dropped one, and the row of the dropped
            // table must not show the files of the new one
            assertRowOfVanishedTable(
                    "DROP TABLE %s",
                    "CREATE TABLE '%s' (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL"
            );
        });
    }

    @Test
    public void testTableRecreatedWhileQueryChecksIt() throws Exception {
        final TxnFaultFilesFacade ff = new TxnFaultFilesFacade();
        assertMemoryLeak(ff, () -> {
            createTable("x", false);
            final TableToken token = sqlExecutionContext.getTableToken("x");
            try (
                    RecordCursorFactory factory = select("SELECT * FROM table_storage() WHERE tableName = 'x'");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                final Record record = cursor.getRecord();
                Assert.assertTrue(cursor.hasNext());
                TestUtils.assertEquals("DAY", record.getStrA(2));
                // The query fails to open _txn of x while x is current. Before the query looks for
                // _txn, a DROP and a CREATE of x run, and the new x brings _txn back to the same
                // directory: the row of the old x still turns NULL instead of failing the query.
                ff.of(token.getDirName());
                ff.failNextTxnOpen();
                ff.runOnTxnExistsCheck(() -> executeOnOtherThread(
                        "DROP TABLE x",
                        "CREATE TABLE x (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL"
                ));
                Assert.assertEquals(Numbers.LONG_NULL, record.getLong(4));
                TestUtils.assertEquals("x", record.getStrA(0));
                Assert.assertNull(record.getStrA(2));
                Assert.assertEquals(Numbers.LONG_NULL, record.getLong(3));
                Assert.assertEquals(Numbers.LONG_NULL, record.getLong(5));
                Assert.assertFalse(cursor.hasNext());
            }
            final TableToken recreated = sqlExecutionContext.getTableToken("x");
            Assert.assertNotEquals(token, recreated);
            Assert.assertEquals(token.getDirName(), recreated.getDirName());
        });
    }

    @Test
    public void testTableRenamedBeforeMetadataRead() throws Exception {
        assertRowOfVanishingTable("RENAME TABLE x TO y", false);
    }

    @Test
    public void testTableRenamedBeforeTxnRead() throws Exception {
        assertRowOfVanishingTable("RENAME TABLE x TO y", true);
    }

    @Test
    public void testTableRenamedDuringIteration() throws Exception {
        assertMemoryLeak(() -> {
            createTable("a", false);
            createTable("b", false);
            assertRowOfVanishedTable("RENAME TABLE %s TO c");
        });
    }

    @Test
    public void testTableStorageIncludesMaterializedView() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (sym varchar, price double, ts timestamp) " +
                    "timestamp(ts) partition by DAY WAL");
            execute("create materialized view price_1h as " +
                    "select sym, last(price) as price, ts from base_price sample by 1h");
            execute("insert into base_price values" +
                    "('gbpusd', 1.320, '2024-09-10T12:01')" +
                    ",('gbpusd', 1.323, '2024-09-10T12:02')");
            drainWalAndMatViewQueues();
            engine.releaseAllWriters();
            // a materialized view is a WAL-backed table, so it appears as a row in table_storage()
            assertQuery("select tableName, walEnabled, rowCount from table_storage() where tableName = 'price_1h'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("tableName\twalEnabled\trowCount\n" +
                            "price_1h\ttrue\t1\n");
        });
    }

    @Test
    public void testTxnReadFailureOfLiveTableFailsQuery() throws Exception {
        // _txn of the live non-WAL table exists but does not open: the failure says nothing about
        // a DROP or RENAME, so the query fails instead of hiding the table behind NULLs
        assertTxnReadFailureFailsQuery(false);
    }

    @Test
    public void testTxnReadFailureOfLiveWalTableFailsQuery() throws Exception {
        // _txn of the live WAL table looks missing: WAL DDL retires the name before the files go,
        // so a missing _txn of a WAL table with a current name is an error, not a vanished table
        assertTxnReadFailureFailsQuery(true);
    }

    @Test
    public void testView() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", true);
            execute("CREATE VIEW v AS (SELECT ts, v FROM t WHERE v > 0)");
            drainWalQueue();
            engine.releaseAllWriters();
            // a view stores no rows, its directory holds the definition and the WAL files
            assertQuery("SELECT * FROM table_storage() WHERE tableName = 'v'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("tableName\twalEnabled\tpartitionBy\tpartitionCount\trowCount\tdiskSize\n" +
                            "v\ttrue\tN/A\t0\t0\t" + getDirSize("v") + "\n");
        });
    }

    @Test
    public void testWalTableDroppedDuringIteration() throws Exception {
        assertMemoryLeak(() -> {
            createTable("a", true);
            createTable("b", true);
            assertRowOfVanishedTable("DROP TABLE %s");
        });
    }

    @Test
    public void testWalTableRenamedDuringIteration() throws Exception {
        assertMemoryLeak(() -> {
            createTable("a", true);
            createTable("b", true);
            assertRowOfVanishedTable("RENAME TABLE %s TO c");
        });
    }

    @Test
    public void testWalTableStorage() throws Exception {
        assertMemoryLeak(() -> {
            createTable("w", true);
            // the size includes the WAL segments and the sequencer files next to the partitions
            assertQuery("SELECT * FROM table_storage()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("tableName\twalEnabled\tpartitionBy\tpartitionCount\trowCount\tdiskSize\n" +
                            "w\ttrue\tDAY\t3\t3\t" + getDirSize("w") + "\n");
        });
    }

    private static void assertListedRows(RecordCursorFactory factory, SqlExecutionContext executionContext, String expected) throws SqlException {
        final StringSink rows = new StringSink();
        try (RecordCursor cursor = factory.getCursor(executionContext)) {
            final Record record = cursor.getRecord();
            long rowCount = 0;
            while (cursor.hasNext()) {
                rows.put(record.getStrA(0)).put('\t').put(record.getLong(1)).put('\n');
                rowCount++;
            }
            // the listing drops tables up front, so that size() matches the rows
            Assert.assertEquals(rowCount, cursor.size());
        }
        TestUtils.assertEquals(expected, rows);
    }

    // Runs table_storage() on an engine whose registry answers the next lookup of the name x with
    // the token that the function derives from the token of x, and checks whether the query lists
    // x. The query that follows looks the name up as usual and lists x with its values.
    private static void assertListingWhileNameResolvesTo(Function<TableToken, TableToken> answer, boolean isListed) throws Exception {
        assertMemoryLeak(() -> {
            try (
                    CairoEngine engine = new ScriptedLookupCairoEngine(new DefaultTestCairoConfiguration(temp.newFolder().getAbsolutePath()));
                    SqlExecutionContext executionContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)
            ) {
                engine.execute("CREATE TABLE x (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", executionContext);
                engine.execute("""
                        INSERT INTO x VALUES
                            ('2024-01-01T00:00:00.000000Z', 1),
                            ('2024-01-02T00:00:00.000000Z', 2)
                        """, executionContext);
                engine.releaseAllWriters();
                final ScriptedLookupRegistry registry = (ScriptedLookupRegistry) engine.getTableNameRegistry();
                try (RecordCursorFactory factory = engine.select("SELECT tableName, rowCount FROM table_storage()", executionContext)) {
                    registry.answerNextLookup("x", answer.apply(engine.verifyTableName("x")));
                    assertListedRows(factory, executionContext, isListed ? "x\t2\n" : "");
                    Assert.assertTrue("the query did not look the name up", registry.hasAnswered());
                    assertListedRows(factory, executionContext, "x\t2\n");
                }
            }
        });
    }

    // Lists the tables a and b, applies the DDL statements to the table listed second, then reads
    // its row.
    private static void assertRowOfVanishedTable(String... ddlTemplates) throws SqlException {
        try (
                RecordCursorFactory factory = select("SELECT * FROM table_storage()");
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            Assert.assertTrue(cursor.hasNext());
            final String vanished = Chars.equals(record.getStrA(0), "a") ? "b" : "a";
            for (String ddlTemplate : ddlTemplates) {
                execute(String.format(ddlTemplate, vanished));
            }

            Assert.assertTrue(cursor.hasNext());
            TestUtils.assertEquals(vanished, record.getStrA(0));
            Assert.assertNull(record.getStrA(2));
            Assert.assertEquals(Numbers.LONG_NULL, record.getLong(3));
            Assert.assertEquals(Numbers.LONG_NULL, record.getLong(4));
            Assert.assertEquals(Numbers.LONG_NULL, record.getLong(5));
            Assert.assertFalse(cursor.hasNext());
        }
    }

    // Reads the row of the non-WAL table x while a DROP or RENAME of x is in flight: the DDL runs on
    // another thread, deletes or moves the table directory, and parks before the engine retires the
    // name x, so the token of x stays current while its files are gone. With isMetadataRead, the
    // query reads the metadata of x before the DDL starts, and the DDL runs when the query opens
    // _txn; otherwise the query lists and reads x while the DDL is parked.
    private static void assertRowOfVanishingTable(String ddl, boolean isMetadataRead) throws Exception {
        final DdlParkingFilesFacade ff = new DdlParkingFilesFacade();
        assertMemoryLeak(ff, () -> {
            createTable("x", false);
            ff.of(sqlExecutionContext.getTableToken("x").getDirName(), ddl);
            try {
                if (!isMetadataRead) {
                    ff.startDdlAndAwaitPark();
                }
                try (
                        RecordCursorFactory factory = select("SELECT * FROM table_storage() WHERE tableName = 'x'");
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    final Record record = cursor.getRecord();
                    Assert.assertTrue(cursor.hasNext());
                    if (isMetadataRead) {
                        TestUtils.assertEquals("DAY", record.getStrA(2));
                        ff.startDdlOnTxnOpen();
                    }
                    Assert.assertEquals(Numbers.LONG_NULL, record.getLong(4));
                    TestUtils.assertEquals("x", record.getStrA(0));
                    Assert.assertNull(record.getStrA(2));
                    Assert.assertEquals(Numbers.LONG_NULL, record.getLong(3));
                    Assert.assertEquals(Numbers.LONG_NULL, record.getLong(5));
                    Assert.assertFalse(cursor.hasNext());
                }
            } finally {
                ff.releaseDdl();
            }
            ff.assertDdlCompleted();
            Assert.assertNull(engine.getTableTokenIfExists("x"));
            Assert.assertEquals(0, engine.getTableDiskSizeCache().getTableCount());
        });
    }

    // Reads the row of the live table x while the files facade fails the read of its _txn: the
    // query must fail, since x was neither dropped nor renamed.
    private static void assertTxnReadFailureFailsQuery(boolean isWal) throws Exception {
        final TxnFaultFilesFacade ff = new TxnFaultFilesFacade();
        assertMemoryLeak(ff, () -> {
            createTable("x", isWal);
            try (
                    RecordCursorFactory factory = select("SELECT * FROM table_storage() WHERE tableName = 'x'");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                final Record record = cursor.getRecord();
                Assert.assertTrue(cursor.hasNext());
                TestUtils.assertEquals("DAY", record.getStrA(2));
                ff.of(sqlExecutionContext.getTableToken("x").getDirName());
                if (isWal) {
                    ff.hideTxn();
                } else {
                    ff.failNextTxnOpen();
                }
                try {
                    record.getLong(4);
                    Assert.fail("the query hid a live table it could not read");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not open");
                }
            }
            Assert.assertNotNull(engine.getTableTokenIfExists("x"));
        });
    }

    private static void createTable(String tableName, boolean isWal) throws SqlException {
        execute("CREATE TABLE '" + tableName + "' (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
        execute("""
                INSERT INTO '%s' VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-02T00:00:00.000000Z', 2),
                    ('2024-01-03T00:00:00.000000Z', 3)
                """.formatted(tableName));
        if (isWal) {
            drainWalQueue();
        }
        engine.releaseAllWriters();
    }

    // Runs the statements on another thread, as another connection would, and waits for them.
    private static void executeOnOtherThread(String... sqls) {
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                for (String sql : sqls) {
                    engine.execute(sql, executionContext);
                }
            } catch (Throwable th) {
                error.set(th);
            } finally {
                Path.clearThreadLocals();
            }
        });
        thread.start();
        try {
            thread.join(TimeUnit.MINUTES.toMillis(1));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertFalse("the statements did not complete", thread.isAlive());
        if (error.get() != null) {
            throw new AssertionError("the statements failed", error.get());
        }
    }

    private long getDirSize(@NotNull CharSequence tableName) {
        final TableToken token = sqlExecutionContext.getTableToken(tableName);
        return Files.getDirSize(
                Path.getThreadLocal(configuration.getDbRoot()).concat(token.getDirName()));
    }

    // Runs a DDL on another thread and parks it right after it deletes or moves the directory of the
    // table, before the engine retires the table name.
    private static class DdlParkingFilesFacade extends TestFilesFacadeImpl {
        private final CountDownLatch ddlParked = new CountDownLatch(1);
        private final CountDownLatch ddlReleased = new CountDownLatch(1);
        private volatile String ddlSql;
        private volatile Thread ddlThread;
        private volatile String dirName;
        private volatile Throwable error;
        private volatile boolean isDdlOnTxnOpen;
        private volatile boolean isParked;
        private volatile String txnFileSuffix;

        @Override
        public long length(LPSZ name) {
            if (isDdlOnTxnOpen && Utf8s.endsWithAscii(name, txnFileSuffix)) {
                isDdlOnTxnOpen = false;
                startDdlAndAwaitPark();
            }
            return super.length(name);
        }

        @Override
        public int rename(LPSZ from, LPSZ to) {
            final int result = super.rename(from, to);
            parkDdl(from);
            return result;
        }

        @Override
        public boolean unlinkOrRemove(Path path, Log log) {
            final boolean result = super.unlinkOrRemove(path, log);
            parkDdl(path.$());
            return result;
        }

        private static void await(CountDownLatch latch) {
            try {
                Assert.assertTrue(latch.await(1, TimeUnit.MINUTES));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private void assertDdlCompleted() {
            if (error != null) {
                throw new AssertionError("DDL failed", error);
            }
            Assert.assertTrue("DDL did not park", isParked);
            Assert.assertFalse("DDL did not complete", ddlThread.isAlive());
        }

        private void of(String dirName, String ddlSql) {
            this.ddlSql = ddlSql;
            this.dirName = dirName;
            this.txnFileSuffix = Files.SEPARATOR + dirName + Files.SEPARATOR + TableUtils.TXN_FILE_NAME;
        }

        private void parkDdl(LPSZ path) {
            if (Thread.currentThread() == ddlThread && !isParked && Utf8s.endsWithAscii(path, Files.SEPARATOR + dirName)) {
                isParked = true;
                ddlParked.countDown();
                await(ddlReleased);
            }
        }

        private void releaseDdl() throws InterruptedException {
            ddlReleased.countDown();
            final Thread ddlThread = this.ddlThread;
            if (ddlThread != null) {
                ddlThread.join(TimeUnit.MINUTES.toMillis(1));
            }
        }

        private void startDdlAndAwaitPark() {
            final Thread ddlThread = new Thread(() -> {
                try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    engine.execute(ddlSql, executionContext);
                } catch (Throwable th) {
                    error = th;
                } finally {
                    // unblocks the query when the DDL fails before it parks
                    ddlParked.countDown();
                    Path.clearThreadLocals();
                }
            });
            this.ddlThread = ddlThread;
            ddlThread.start();
            await(ddlParked);
            if (!isParked) {
                throw new AssertionError("DDL did not park", error);
            }
        }

        private void startDdlOnTxnOpen() {
            isDdlOnTxnOpen = true;
        }
    }

    private static class DirListingRecordingFilesFacade extends TestFilesFacadeImpl {
        private final ObjList<String> listedDirs = new ObjList<>();

        @Override
        public long findFirst(LPSZ path) {
            listedDirs.add(path.toString());
            return super.findFirst(path);
        }

        @Override
        public long getDirSize(Path path) {
            listedDirs.add(path.toString());
            return super.getDirSize(path);
        }
    }

    private static class ScriptedLookupCairoEngine extends CairoEngine {

        private ScriptedLookupCairoEngine(CairoConfiguration configuration) {
            super(configuration);
        }

        @Override
        protected @NotNull TableNameRegistry createTableNameRegistry(CairoConfiguration configuration, TableFlagResolver tableFlagResolver) {
            return new ScriptedLookupRegistry(this, tableFlagResolver);
        }
    }

    // Answers the next lookup of one table name with a scripted token, then resolves the name as
    // usual. A LOCKED_TOKEN answer is what a query sees when it looks the name up between the two
    // map updates of TableNameRegistryRW.registerName(), right before CREATE completes.
    private static class ScriptedLookupRegistry extends TableNameRegistryRW {
        private volatile TableToken answer;
        private volatile boolean hasAnswered;
        private volatile String scriptedName;

        private ScriptedLookupRegistry(CairoEngine engine, TableFlagResolver tableFlagResolver) {
            super(engine, tableFlagResolver);
        }

        @Override
        public TableToken getTableToken(CharSequence tableName) {
            final String scriptedName = this.scriptedName;
            if (scriptedName != null && Chars.equals(scriptedName, tableName)) {
                this.scriptedName = null;
                hasAnswered = true;
                return answer;
            }
            return super.getTableToken(tableName);
        }

        private void answerNextLookup(String tableName, TableToken answer) {
            this.answer = answer;
            hasAnswered = false;
            scriptedName = tableName;
        }

        private boolean hasAnswered() {
            return hasAnswered;
        }
    }

    // Fails the reads of the _txn file of one table on the thread that armed the facade.
    private static class TxnFaultFilesFacade extends TestFilesFacadeImpl {
        private volatile Thread faultThread;
        private volatile boolean isTxnHidden;
        private volatile boolean isTxnOpenFailing;
        private volatile Runnable txnExistsCheckHook;
        private volatile String txnFileSuffix;

        @Override
        public boolean exists(LPSZ path) {
            if (isFaulted(path)) {
                final Runnable txnExistsCheckHook = this.txnExistsCheckHook;
                if (txnExistsCheckHook != null) {
                    this.txnExistsCheckHook = null;
                    txnExistsCheckHook.run();
                }
                if (isTxnHidden) {
                    return false;
                }
            }
            return super.exists(path);
        }

        @Override
        public long length(LPSZ name) {
            return isTxnHidden && isFaulted(name) ? -1 : super.length(name);
        }

        @Override
        public long openRO(LPSZ name) {
            return isOpenFaulted(name) ? -1 : super.openRO(name);
        }

        @Override
        public long openRONoCache(LPSZ path) {
            return isOpenFaulted(path) ? -1 : super.openRONoCache(path);
        }

        private void failNextTxnOpen() {
            isTxnOpenFailing = true;
        }

        private void hideTxn() {
            isTxnHidden = true;
        }

        private boolean isFaulted(LPSZ path) {
            final String txnFileSuffix = this.txnFileSuffix;
            return txnFileSuffix != null && Thread.currentThread() == faultThread && Utf8s.endsWithAscii(path, txnFileSuffix);
        }

        private boolean isOpenFaulted(LPSZ path) {
            if (isTxnOpenFailing && isFaulted(path)) {
                isTxnOpenFailing = false;
                return true;
            }
            return false;
        }

        private void of(String dirName) {
            faultThread = Thread.currentThread();
            txnFileSuffix = Files.SEPARATOR + dirName + Files.SEPARATOR + TableUtils.TXN_FILE_NAME;
        }

        private void runOnTxnExistsCheck(Runnable hook) {
            txnExistsCheckHook = hook;
        }
    }
}
