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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.RecentWriteTracker;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;

public class TablesFunctionFactoryTest extends AbstractCairoTest {
    @Test
    public void testMemoryPressureColumn() throws Exception {
        assertMemoryLeak(() -> {
            // Create a WAL table
            execute("CREATE TABLE test_mem_pressure (ts TIMESTAMP, value INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // Insert some data to initialize the tracker
            execute("INSERT INTO test_mem_pressure VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            // Initially, table_memory_pressure_level should be 0 (no pressure)
            assertSql(
                    """
                            table_name\ttable_memory_pressure_level
                            test_mem_pressure\t0
                            """,
                    "select table_name, table_memory_pressure_level from tables() where table_name = 'test_mem_pressure'"
            );
        });
    }

    @Test
    public void testMemoryPressureColumnNonWalTable() throws Exception {
        assertMemoryLeak(() -> {
            // Create a non-WAL table
            execute("CREATE TABLE test_non_wal_mem (ts TIMESTAMP, value INT) TIMESTAMP(ts) PARTITION BY DAY");

            // Non-WAL tables should show null for table_memory_pressure_level
            assertSql(
                    """
                            table_name\ttable_memory_pressure_level
                            test_non_wal_mem\tnull
                            """,
                    "select table_name, table_memory_pressure_level from tables() where table_name = 'test_non_wal_mem'"
            );
        });
    }

    @Test
    public void testMetadataQuery() throws Exception {
        assertMemoryLeak(() -> {
            TableModel tm1 = new TableModel(configuration, "table1", PartitionBy.DAY);
            tm1.col("abc", ColumnType.STRING);
            tm1.timestamp("ts1");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
            tm1 = new TableModel(configuration, "table2", PartitionBy.NONE);
            tm1.timestamp("ts2");
            createPopulateTable(tm1, 0, "2020-01-01", 0);

            assertSql(
                    """
                            id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag
                            2\ttable2\tts2\tNONE\t1000\t300000000
                            1\ttable1\tts1\tDAY\t1000\t300000000
                            """,
                    "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables() order by id desc"
            );
        });
    }

    @Test
    public void testMetadataQueryDefaultHysteresisParams() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 83737);
            node1.setProperty(PropertyKey.CAIRO_O3_MAX_LAG, 28);

            TableModel tm1 = new TableModel(configuration, "table1", PartitionBy.DAY);
            tm1.col("abc", ColumnType.STRING);
            tm1.timestamp("ts1");
            createPopulateTable(tm1, 0, "2020-01-01", 0);

            assertSql(
                    """
                            id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag
                            1\ttable1\tts1\tDAY\t83737\t28000
                            """,
                    "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables()"
            );
        });
    }

    @Test
    public void testMetadataQueryMissingMetaFile() throws Exception {
        assertMemoryLeak(() -> {
            TableModel tm1 = new TableModel(configuration, "table1", PartitionBy.DAY);
            tm1.col("abc", ColumnType.STRING);
            tm1.timestamp("ts1");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
            tm1 = new TableModel(configuration, "table2", PartitionBy.NONE);
            tm1.timestamp("ts2");
            createPopulateTable(tm1, 0, "2020-01-01", 0);

            engine.releaseAllWriters();
            engine.releaseAllReaders();

            FilesFacade filesFacade = configuration.getFilesFacade();
            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName("table1");
                path.concat(configuration.getDbRoot()).concat(tableToken).concat(META_FILE_NAME).$();
                filesFacade.remove(path.$());
            }

            assertException(
                    "select hydrate_table_metadata('*')",
                    7,
                    "could not open, file does not exist"
            );
        });
    }

    @Test
    public void testMetadataQueryWithWhere() throws Exception {
        assertMemoryLeak(() -> {
            TableModel tm1 = new TableModel(configuration, "table1", PartitionBy.DAY);
            tm1.col("abc", ColumnType.STRING);
            tm1.timestamp("ts1");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
            tm1 = new TableModel(configuration, "table2", PartitionBy.NONE);
            tm1.timestamp("ts2");
            createPopulateTable(tm1, 0, "2020-01-01", 0);

            assertSql(
                    """
                            id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag
                            1\ttable1\tts1\tDAY\t1000\t300000000
                            """,
                    "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables() where table_name = 'table1'"
            );
        });
    }

    @Test
    public void testMetadataQueryWithWhereAndSelect() throws Exception {
        assertMemoryLeak(() -> {
            TableModel tm1 = new TableModel(configuration, "table1", PartitionBy.DAY);
            tm1.col("abc", ColumnType.STRING);
            tm1.timestamp("ts1");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
            tm1 = new TableModel(configuration, "table2", PartitionBy.NONE);
            tm1.timestamp("ts2");
            createPopulateTable(tm1, 0, "2020-01-01", 0);

            assertSql(
                    """
                            designatedTimestamp
                            ts1
                            """,
                    "select designatedTimestamp from tables where table_name = 'table1'"
            );
        });
    }

    @Test
    public void testNonWalTableTxnColumns() throws Exception {
        assertMemoryLeak(() -> {
            // Create a non-WAL table
            execute("CREATE TABLE test_non_wal (ts TIMESTAMP, value INT) TIMESTAMP(ts) PARTITION BY NONE");

            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            tracker.clear();

            // Insert rows
            execute("INSERT INTO test_non_wal VALUES ('2024-01-01T00:00:00.000000Z', 1)");

            TableToken tableToken = engine.verifyTableName("test_non_wal");
            long writerTxn = tracker.getWriterTxn(tableToken);

            // Non-WAL tables should have table_txn but no wal_txn or wal_max_timestamp
            Assert.assertTrue("table_txn should be positive for non-WAL table", writerTxn >= 0);
            Assert.assertEquals("wal_txn should be null for non-WAL table", Numbers.LONG_NULL, tracker.getSequencerTxn(tableToken));
            Assert.assertEquals("wal_max_timestamp should be null for non-WAL table", Numbers.LONG_NULL, tracker.getLastWalTimestamp(tableToken));

            // Query via tables() function
            assertSql(
                    """
                            table_name\ttable_txn\twal_txn\twal_max_timestamp
                            test_non_wal\t""" + writerTxn + """
                            \tnull\t
                            """,
                    "select table_name, table_txn, wal_txn, wal_max_timestamp from tables() where table_name = 'test_non_wal'"
            );
        });
    }

    @Test
    public void testRowCountAndLastWriteTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // Create a WAL table and write data to it
            execute("CREATE TABLE test_writes (ts TIMESTAMP, value INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            tracker.clear();

            // Before any writes, table_row_count and table_last_write_timestamp should be null
            assertSql(
                    """
                            table_name\ttable_row_count\ttable_last_write_timestamp
                            test_writes\tnull\t
                            """,
                    "select table_name, table_row_count, table_last_write_timestamp from tables() where table_name = 'test_writes'"
            );

            // Insert rows and drain WAL
            long beforeWrite = configuration.getMicrosecondClock().getTicks();
            execute("INSERT INTO test_writes VALUES (now(), 1)");
            execute("INSERT INTO test_writes VALUES (now(), 2)");
            execute("INSERT INTO test_writes VALUES (now(), 3)");
            drainWalQueue();
            long afterWrite = configuration.getMicrosecondClock().getTicks();

            // Verify row count is tracked
            TableToken tableToken = engine.verifyTableName("test_writes");
            Assert.assertEquals(3L, tracker.getRowCount(tableToken));

            // Query via tables() function
            assertSql(
                    """
                            table_name\ttable_row_count
                            test_writes\t3
                            """,
                    "select table_name, table_row_count from tables() where table_name = 'test_writes'"
            );

            // Verify table_last_write_timestamp is within expected range
            long lastWriteTimestamp = tracker.getWriteTimestamp(tableToken);
            Assert.assertTrue("Timestamp should be >= beforeWrite", lastWriteTimestamp >= beforeWrite);
            Assert.assertTrue("Timestamp should be <= afterWrite", lastWriteTimestamp <= afterWrite);
        });
    }

    @Test
    public void testSuspendedColumn() throws Exception {
        assertMemoryLeak(() -> {
            // Create a WAL table
            execute("CREATE TABLE test_suspended (ts TIMESTAMP, value INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            TableToken tableToken = engine.verifyTableName("test_suspended");
            TableSequencerAPI sequencerAPI = engine.getTableSequencerAPI();

            // Insert some data to initialize the tracker
            execute("INSERT INTO test_suspended VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            // Initially, the table should not be suspended
            assertSql(
                    """
                            table_name\ttable_suspended
                            test_suspended\tfalse
                            """,
                    "select table_name, table_suspended from tables() where table_name = 'test_suspended'"
            );

            // Suspend the table
            sequencerAPI.suspendTable(tableToken, ErrorTag.DISK_FULL, "test suspension");

            // Now the table should be suspended
            assertSql(
                    """
                            table_name\ttable_suspended
                            test_suspended\ttrue
                            """,
                    "select table_name, table_suspended from tables() where table_name = 'test_suspended'"
            );

            // Resume the table
            sequencerAPI.resumeTable(tableToken, 0);

            // Table should no longer be suspended
            assertSql(
                    """
                            table_name\ttable_suspended
                            test_suspended\tfalse
                            """,
                    "select table_name, table_suspended from tables() where table_name = 'test_suspended'"
            );
        });
    }

    @Test
    public void testSuspendedColumnNonWalTable() throws Exception {
        assertMemoryLeak(() -> {
            // Create a non-WAL table
            execute("CREATE TABLE test_non_wal_suspended (ts TIMESTAMP, value INT) TIMESTAMP(ts) PARTITION BY DAY");

            // Non-WAL tables should always show table_suspended=false
            assertSql(
                    """
                            table_name\ttable_suspended
                            test_non_wal_suspended\tfalse
                            """,
                    "select table_name, table_suspended from tables() where table_name = 'test_non_wal_suspended'"
            );
        });
    }

    @Test
    public void testTimestampColumnsWithTimestampNs() throws Exception {
        // Regression test for https://github.com/questdb/questdb/issues/6677
        // Verifies that timestamp columns are correctly converted to microseconds
        // when the table uses TIMESTAMP_NS as the designated timestamp type
        assertMemoryLeak(() -> {
            // Create a WAL table with TIMESTAMP_NS
            execute("CREATE TABLE test_ts_ns (ts TIMESTAMP_NS, value INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            tracker.clear();

            // Insert rows and drain WAL
            execute("INSERT INTO test_ts_ns VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            execute("INSERT INTO test_ts_ns VALUES ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();

            // Query via tables() function
            // The key assertion is that years should be in the 2024 range, not 57000+ (bug symptom)
            assertSql(
                    """
                            table_name\ttable_min_timestamp\ttable_max_timestamp
                            test_ts_ns\t2024-01-01T00:00:00.000000Z\t2024-01-02T00:00:00.000000Z
                            """,
                    "select table_name, table_min_timestamp, table_max_timestamp from tables() where table_name = 'test_ts_ns'"
            );

            // Also verify wal_max_timestamp is correctly converted
            TableToken tableToken = engine.verifyTableName("test_ts_ns");
            RecentWriteTracker.WriteStats stats = tracker.getWriteStats(tableToken);
            Assert.assertNotNull("WriteStats should exist", stats);
            // The raw wal timestamp is in nanoseconds, but when accessed via tables() it should be converted to micros
            long walMaxTs = stats.getLastWalTimestamp();
            Assert.assertTrue("wal_max_timestamp should be in nanoseconds (large value)", walMaxTs > 1_000_000_000_000_000L);

            // Via the tables() function, year should be 2024, not 1970 or 57000+
            assertSql("""
                            yr
                            2024
                            """,
                    "select year(wal_max_timestamp) as yr from tables() where table_name = 'test_ts_ns'");
        });
    }

    @Test
    public void testTxnAndWalTimestampColumns() throws Exception {
        assertMemoryLeak(() -> {
            // Create a WAL table
            execute("CREATE TABLE test_txn (ts TIMESTAMP, value INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            tracker.clear();

            // Before any writes, all txn columns should be null
            assertSql(
                    """
                            table_name\ttable_txn\twal_txn\twal_max_timestamp
                            test_txn\tnull\tnull\t
                            """,
                    "select table_name, table_txn, wal_txn, wal_max_timestamp from tables() where table_name = 'test_txn'"
            );

            // Insert rows and drain WAL
            execute("INSERT INTO test_txn VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            execute("INSERT INTO test_txn VALUES ('2024-01-01T00:00:01.000000Z', 2)");
            drainWalQueue();

            // Verify txn values are tracked
            TableToken tableToken = engine.verifyTableName("test_txn");
            long writerTxn = tracker.getWriterTxn(tableToken);
            long sequencerTxn = tracker.getSequencerTxn(tableToken);
            long walTimestamp = tracker.getLastWalTimestamp(tableToken);

            Assert.assertTrue("table_txn should be positive", writerTxn > 0);
            Assert.assertTrue("wal_txn should be positive", sequencerTxn > 0);
            Assert.assertTrue("wal_max_timestamp should be positive", walTimestamp > 0);

            // Query via tables() function - verify columns are present and have values
            assertSql(
                    """
                            table_name\ttable_txn\twal_txn
                            test_txn\t""" + writerTxn + "\t" + sequencerTxn + """
                            
                            """,
                    "select table_name, table_txn, wal_txn from tables() where table_name = 'test_txn'"
            );
        });
    }
}
