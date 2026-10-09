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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

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
    public void testTableDroppedDuringIteration() throws Exception {
        assertMemoryLeak(() -> {
            createTable("a", false);
            createTable("b", false);
            assertRowOfVanishedTable("DROP TABLE %s");
        });
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

    // Lists the tables a and b, applies the DDL to the table listed second, then reads its row.
    private static void assertRowOfVanishedTable(String ddlTemplate) throws SqlException {
        try (
                RecordCursorFactory factory = select("SELECT * FROM table_storage()");
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            Assert.assertTrue(cursor.hasNext());
            final String vanished = Chars.equals(record.getStrA(0), "a") ? "b" : "a";
            execute(String.format(ddlTemplate, vanished));

            Assert.assertTrue(cursor.hasNext());
            TestUtils.assertEquals(vanished, record.getStrA(0));
            Assert.assertNull(record.getStrA(2));
            Assert.assertEquals(Numbers.LONG_NULL, record.getLong(3));
            Assert.assertEquals(Numbers.LONG_NULL, record.getLong(4));
            Assert.assertEquals(Numbers.LONG_NULL, record.getLong(5));
            Assert.assertFalse(cursor.hasNext());
        }
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

    private long getDirSize(@NotNull CharSequence tableName) {
        final TableToken token = sqlExecutionContext.getTableToken(tableName);
        return Files.getDirSize(
                Path.getThreadLocal(configuration.getDbRoot()).concat(token.getDirName()));
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
}
