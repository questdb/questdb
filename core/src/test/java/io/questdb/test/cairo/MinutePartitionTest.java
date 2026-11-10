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

import io.questdb.PropertyKey;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * SQL-level coverage of sub-hour partitioning: {@code PARTITION BY 5|10|15|20|30 MINUTES}.
 */
public class MinutePartitionTest extends AbstractCairoTest {

    @Test
    public void testCompositePartition() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 256);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY 15 MINUTES WAL");
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01T00:15', 1_000_000L), x FROM long_sequence(900)");
            drainWalQueue();
            // a later partition, so 2024-01-01T0015 stops being the active partition
            execute("INSERT INTO t VALUES ('2024-01-01T00:45:00', 0)");
            drainWalQueue();
            // backdated into the middle of the partition: merge-append rewrites the piece it hits
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01T00:20:00.5', 1_000_000L), -x FROM long_sequence(20)");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            try (TableReader reader = getReader("t")) {
                Assert.assertTrue("fixture left 2024-01-01T0015 plain", reader.getTxFile().isPartitionComposite(0));
            }
            assertQuery("SELECT name, numRows FROM table_partitions('t')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            name\tnumRows
                            2024-01-01T0015\t920
                            2024-01-01T0045\t1
                            """);
            assertQuery("SELECT count(), sum(v), min(ts), max(ts) FROM t WHERE ts IN '2024-01-01T00:15;15m'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum\tmin\tmax
                            920\t405240\t2024-01-01T00:15:00.000000Z\t2024-01-01T00:29:59.000000Z
                            """);
            assertQuery("SELECT ts, v FROM t WHERE ts BETWEEN '2024-01-01T00:20:00' AND '2024-01-01T00:20:02'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-01T00:20:00.000000Z\t301
                            2024-01-01T00:20:00.500000Z\t-1
                            2024-01-01T00:20:01.000000Z\t302
                            2024-01-01T00:20:01.500000Z\t-2
                            2024-01-01T00:20:02.000000Z\t303
                            """);
        });
    }

    @Test
    public void testConvertToParquetAndBack() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG, s SYMBOL) TIMESTAMP(ts) PARTITION BY 15 MINUTES WAL");
            execute("""
                    INSERT INTO t SELECT
                        timestamp_sequence('2024-01-01T00:00:00', 60_000_000L) ts, x, rnd_symbol('a', 'b') s
                    FROM long_sequence(60)
                    """);
            drainWalQueue();
            final String query = "SELECT count(), sum(v), min(ts), max(ts) FROM t";
            final String expected = """
                    count\tsum\tmin\tmax
                    60\t1830\t2024-01-01T00:00:00.000000Z\t2024-01-01T00:59:00.000000Z
                    """;
            assertQuery(query).noLeakCheck().noRandomAccess().expectSize().returns(expected);

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01T0015', '2024-01-01T0030'");
            drainWalQueue();
            assertQuery("SELECT name, numRows, isParquet FROM table_partitions('t')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            name\tnumRows\tisParquet
                            2024-01-01T0000\t15\tfalse
                            2024-01-01T0015\t15\ttrue
                            2024-01-01T0030\t15\ttrue
                            2024-01-01T0045\t15\tfalse
                            """);
            assertQuery(query).noLeakCheck().noRandomAccess().expectSize().returns(expected);
            assertQuery("SELECT ts, v FROM t WHERE ts IN '2024-01-01T00:29'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-01T00:29:00.000000Z\t30
                            """);

            // O3 write into a parquet partition
            execute("INSERT INTO t VALUES ('2024-01-01T00:20:30', 1000, 'c')");
            drainWalQueue();
            assertQuery("SELECT count(), sum(v) FROM t WHERE ts IN '2024-01-01T00:15;15m'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            16\t1345
                            """);

            execute("ALTER TABLE t CONVERT PARTITION TO NATIVE WHERE ts >= 0");
            drainWalQueue();
            assertQuery("SELECT count(), sum(v) FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            61\t2830
                            """);
            assertQuery("SELECT count() FROM table_partitions('t') WHERE isParquet")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n0\n");
        });
    }

    @Test
    public void testCreateTableAllSizes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t5 (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY 5 MINUTES");
            execute("CREATE TABLE t10 (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY 10 MINUTE WAL");
            execute("CREATE TABLE t15 (ts TIMESTAMP) TIMESTAMP(ts) partition by 15 minutes wal");
            execute("CREATE TABLE t20 (ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY 20 Minutes BYPASS WAL");
            execute("CREATE TABLE t30 AS (SELECT timestamp_sequence(0, 60_000_000L) ts FROM long_sequence(100)) TIMESTAMP(ts) PARTITION BY 30 MINUTES WAL");
            drainWalQueue();
            assertQuery("SELECT table_name, partitionBy, walEnabled FROM tables() ORDER BY table_name")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            table_name\tpartitionBy\twalEnabled
                            t10\t10 MINUTES\ttrue
                            t15\t15 MINUTES\ttrue
                            t20\t20 MINUTES\tfalse
                            t30\t30 MINUTES\ttrue
                            t5\t5 MINUTES\tfalse
                            """);
            assertQuery("SELECT name, numRows FROM table_partitions('t30')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            name\tnumRows
                            1970-01-01T0000\t30
                            1970-01-01T0030\t30
                            1970-01-01T0100\t30
                            1970-01-01T0130\t10
                            """);
            for (String name : new String[]{"t5", "t10", "t15", "t20", "t30"}) {
                try (TableReader reader = getReader(name)) {
                    Assert.assertTrue(PartitionBy.isMinutePartitioned(reader.getPartitionedBy()));
                }
            }
        });
    }

    @Test
    public void testCreateTableInvalidSyntax() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY 7 MINUTES")
                    .fails(57, "unsupported partition size, expected 5, 10, 15, 20 or 30 MINUTES");
            assertQuery("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY 60 MINUTES")
                    .fails(57, "unsupported partition size, expected 5, 10, 15, 20 or 30 MINUTES");
            assertQuery("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY 15 HOURS")
                    .fails(60, "'MINUTE' or 'MINUTES' expected");
            assertQuery("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY 15")
                    .fails(59, "'MINUTE' or 'MINUTES' expected");
            assertQuery("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY 15m")
                    .fails(57, "'NONE', 'HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR' or '<n> MINUTES' expected");
            assertQuery("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY 99999999999999999999 MINUTES")
                    .fails(57, "unsupported partition size, expected 5, 10, 15, 20 or 30 MINUTES");
            assertQuery("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MINUTES")
                    .fails(57, "'NONE', 'HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR' or '<n> MINUTES' expected");
            assertQuery("CREATE TABLE t (x INT) PARTITION BY 15 MINUTES")
                    .fails(36, "partitioning is possible only on tables with designated timestamps");
        });
    }

    @Test
    public void testDetachAttachPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY 5 MINUTES");
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01', 60_000_000L), x FROM long_sequence(15)");
            execute("ALTER TABLE t DETACH PARTITION LIST '2024-01-01T0005'");
            assertQuery("SELECT count(), sum(v) FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\tsum\n10\t80\n");
            renameDetachedToAttachable("t", "2024-01-01T0005");
            execute("ALTER TABLE t ATTACH PARTITION LIST '2024-01-01T0005'");
            assertQuery("SELECT count(), sum(v) FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\tsum\n15\t120\n");
        });
    }

    @Test
    public void testDropPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY 10 MINUTES WAL");
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01T23:30:00', 60_000_000L), x FROM long_sequence(60)");
            drainWalQueue();
            assertPartitions("""
                    name\tnumRows
                    2024-01-01T2330\t10
                    2024-01-01T2340\t10
                    2024-01-01T2350\t10
                    2024-01-02T0000\t10
                    2024-01-02T0010\t10
                    2024-01-02T0020\t10
                    """);

            // a directory name, or any time within the partition, picks the partition
            execute("ALTER TABLE t DROP PARTITION LIST '2024-01-01T2347', '2024-01-02T00:05:30.5'");
            drainWalQueue();
            assertPartitions("""
                    name\tnumRows
                    2024-01-01T2330\t10
                    2024-01-01T2350\t10
                    2024-01-02T0010\t10
                    2024-01-02T0020\t10
                    """);

            // the condition applies to partition timestamps
            execute("ALTER TABLE t DROP PARTITION WHERE ts < '2024-01-02T00:10'");
            drainWalQueue();
            assertPartitions("""
                    name\tnumRows
                    2024-01-02T0010\t10
                    2024-01-02T0020\t10
                    """);
            assertQuery("SELECT count(), min(ts), max(ts) FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tmin\tmax
                            20\t2024-01-02T00:10:00.000000Z\t2024-01-02T00:29:00.000000Z
                            """);

            assertQuery("ALTER TABLE t DROP PARTITION LIST '2024-01-02T00'")
                    .fails(34, "'yyyy-MM-ddTHHmm' expected, found [ts=2024-01-02T00]");
        });
    }

    @Test
    public void testMatView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY 5 MINUTES WAL");
            execute("""
                    CREATE MATERIALIZED VIEW mv AS (
                        SELECT ts, sym, avg(price) avg_price FROM base SAMPLE BY 1m
                    ) PARTITION BY 15 MINUTES
                    """);
            execute("""
                    INSERT INTO base SELECT
                        timestamp_sequence('2024-01-01', 10_000_000L) ts, rnd_symbol('a', 'b') sym, x::double price
                    FROM long_sequence(240)
                    """);
            drainWalAndMatViewQueues();
            assertQuery("SELECT table_name, partitionBy FROM tables() ORDER BY table_name")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            table_name\tpartitionBy
                            base\t5 MINUTES
                            mv\t15 MINUTES
                            """);
            assertQuery("SELECT name FROM table_partitions('mv')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            name
                            2024-01-01T0000
                            2024-01-01T0015
                            2024-01-01T0030
                            """);
            assertQuery("SELECT count() FROM (SELECT DISTINCT ts FROM mv)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n40\n");
            assertQuery("SHOW CREATE MATERIALIZED VIEW mv")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE MATERIALIZED VIEW 'mv' WITH BASE 'base' REFRESH IMMEDIATE AS (
                            SELECT ts, sym, avg(price) avg_price FROM base SAMPLE BY 1m
                            ) PARTITION BY 15 MINUTES;
                            """);
        });
    }

    @Test
    public void testNanosO3AndSplits() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 10);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP_NS, v LONG) TIMESTAMP(ts) PARTITION BY 15 MINUTES BYPASS WAL");
            execute("INSERT INTO t SELECT timestamp_sequence_ns('2024-01-01T00:15:00'::timestamp_ns, 1_000_000_000L), x FROM long_sequence(600)");
            // O3 into the middle of the last partition splits it
            execute("INSERT INTO t VALUES ('2024-01-01T00:20:00.000000001', -1)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:16:00.000000001', -2)");
            assertQuery("SELECT count(), sum(v), min(ts), max(ts) FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum\tmin\tmax
                            602\t180297\t2024-01-01T00:15:00.000000000Z\t2024-01-01T00:24:59.000000000Z
                            """);
            assertQuery("SELECT ts, v FROM t WHERE ts IN '2024-01-01T00:20:00'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-01T00:20:00.000000000Z\t301
                            2024-01-01T00:20:00.000000001Z\t-1
                            """);
            assertQuery("SELECT minTimestamp, numRows FROM table_partitions('t')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            minTimestamp\tnumRows
                            2024-01-01T00:15:00.000000000Z\t602
                            """);
        });
    }

    @Test
    public void testO3AcrossPartitionsWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY 20 MINUTES WAL");
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01T00:59:00', 60_000_000L), x FROM long_sequence(10)");
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01T00:00:30', 60_000_000L), x * 100 FROM long_sequence(70)");
            drainWalQueue();
            assertPartitions("""
                    name\tnumRows
                    2024-01-01T0000\t20
                    2024-01-01T0020\t20
                    2024-01-01T0040\t21
                    2024-01-01T0100\t19
                    """);
            assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts IN '2024-01-01T00:40;20m'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tmin\tmax
                            21\t2024-01-01T00:40:30.000000Z\t2024-01-01T00:59:30.000000Z
                            """);
        });
    }

    @Test
    public void testShowCreateTableRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY 15 MINUTE TTL 2 HOURS BYPASS WAL");
            final String ddl = """
                    ddl
                    CREATE TABLE 't' (\s
                    \tts TIMESTAMP,
                    \tv LONG
                    ) timestamp(ts) PARTITION BY 15 MINUTES TTL 2 HOURS BYPASS WAL;
                    """;
            assertQuery("SHOW CREATE TABLE t").noLeakCheck().noRandomAccess().returns(ddl);
            execute("DROP TABLE t");
            execute(ddl.substring("ddl\n".length()));
            assertQuery("SHOW CREATE TABLE t").noLeakCheck().noRandomAccess().returns(ddl);
        });
    }

    @Test
    public void testTtl() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY 15 MINUTES TTL 1 HOUR WAL");
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01T00:00:00', 60_000_000L), x FROM long_sequence(60)");
            drainWalQueue();
            assertPartitions("""
                    name\tnumRows
                    2024-01-01T0000\t15
                    2024-01-01T0015\t15
                    2024-01-01T0030\t15
                    2024-01-01T0045\t15
                    """);
            // the first two partitions end more than an hour before the newest row
            execute("INSERT INTO t VALUES ('2024-01-01T01:30:00', 0)");
            drainWalQueue();
            assertPartitions("""
                    name\tnumRows
                    2024-01-01T0030\t15
                    2024-01-01T0045\t15
                    2024-01-01T0130\t1
                    """);
        });
    }

    private void assertPartitions(String expected) throws Exception {
        assertQuery("SELECT name, numRows FROM table_partitions('t')").noLeakCheck()
                    .noRandomAccess().expectSize().returns(expected);
    }

    private void renameDetachedToAttachable(String tableName, String partitionName) {
        final TableToken token = engine.verifyTableName(tableName);
        try (
                Path src = new Path();
                Path dst = new Path()
        ) {
            src.of(configuration.getDbRoot()).concat(token).concat(partitionName).put(TableUtils.DETACHED_DIR_MARKER).$();
            dst.of(configuration.getDbRoot()).concat(token).concat(partitionName).put(configuration.getAttachPartitionSuffix()).$();
            Assert.assertEquals(0, configuration.getFilesFacade().rename(src.$(), dst.$()));
        }
    }
}
