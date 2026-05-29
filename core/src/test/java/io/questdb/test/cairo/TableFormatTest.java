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
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TableFormatTest extends AbstractCairoTest {

    @Test
    public void testAlterTableSetFormatInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("ALTER TABLE tango SET FORMAT BANANA");
                fail("Invalid format value accepted");
            } catch (SqlException e) {
                assertEquals("[29] 'parquet' or 'native' expected", e.getMessage());
            }
        });
    }

    @Test
    public void testAlterTableSetFormatNativeRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            assertTableFormat("tango", TableUtils.TABLE_FORMAT_PARQUET);

            execute("ALTER TABLE tango SET FORMAT NATIVE");
            drainWalQueue();
            assertTableFormat("tango", TableUtils.TABLE_FORMAT_NATIVE);
        });
    }

    @Test
    public void testAlterTableSetFormatNativeSilentlyAcceptedOnIneligibleTables() throws Exception {
        // SqlCompilerImpl.alterTableSetFormat gates the partitioned/WAL/matview
        // checks behind `if (format == TABLE_FORMAT_PARQUET)`, and
        // TableWriter.setMetaTableFormat has the same asymmetry. Setting FORMAT
        // NATIVE on tables that could never have accepted FORMAT PARQUET in the
        // first place succeeds silently. This test pins that surprising behavior
        // so any future fix (rejecting it, or making it a no-op early-return)
        // has to come through this test.
        assertMemoryLeak(() -> {
            // Non-partitioned table.
            execute("CREATE TABLE no_partition (ts TIMESTAMP) TIMESTAMP(ts)");
            execute("ALTER TABLE no_partition SET FORMAT NATIVE");
            assertTableFormat("no_partition", TableUtils.TABLE_FORMAT_NATIVE);

            // BYPASS WAL table.
            execute("CREATE TABLE no_wal (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("ALTER TABLE no_wal SET FORMAT NATIVE");
            assertTableFormat("no_wal", TableUtils.TABLE_FORMAT_NATIVE);

            // Contrast: the same tables reject FORMAT PARQUET with a clear error.
            try {
                execute("ALTER TABLE no_partition SET FORMAT PARQUET");
                fail("FORMAT PARQUET should be rejected on non-partitioned table");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(),
                        "FORMAT PARQUET is only supported on partitioned tables");
            }
            try {
                execute("ALTER TABLE no_wal SET FORMAT PARQUET");
                fail("FORMAT PARQUET should be rejected on non-WAL table");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(),
                        "FORMAT PARQUET is only supported on WAL tables");
            }
        });
    }

    @Test
    public void testAlterTableSetFormatParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertTableFormat("tango", TableUtils.TABLE_FORMAT_NATIVE);

            execute("ALTER TABLE tango SET FORMAT PARQUET");
            drainWalQueue();
            assertTableFormat("tango", TableUtils.TABLE_FORMAT_PARQUET);
        });
    }

    @Test
    public void testAlterTableSetFormatParquetRejectedOnNonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts)");
            try {
                execute("ALTER TABLE tango SET FORMAT PARQUET");
                fail("FORMAT PARQUET should be rejected on non-partitioned table");
            } catch (SqlException e) {
                assertEquals("[29] FORMAT PARQUET is only supported on partitioned tables", e.getMessage());
            }
        });
    }

    @Test
    public void testAlterTableSetFormatParquetRejectedOnMatView() throws Exception {
        // compileAlterTable runs checkMatViewModification before dispatching to
        // alterTableSetFormat, so ALTER TABLE on a matview never reaches the
        // FORMAT-specific "not supported on materialized views" branch in
        // SqlCompilerImpl.alterTableSetFormat or in TableWriter.setMetaTableFormat.
        // The user-visible rejection comes from the generic guard. Lock that in
        // so anyone removing the generic check is forced to look at this test.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, avg(x) FROM base SAMPLE BY 1m) PARTITION BY DAY");
            drainWalQueue();
            try {
                execute("ALTER TABLE mv SET FORMAT PARQUET");
                fail("FORMAT PARQUET should be rejected on materialized views");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(),
                        "cannot modify materialized view [view=mv]");
            }
        });
    }

    @Test
    public void testAlterTableSetFormatParquetRejectedOnNonWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            try {
                execute("ALTER TABLE tango SET FORMAT PARQUET");
                fail("FORMAT PARQUET should be rejected on non-WAL table");
            } catch (SqlException e) {
                assertEquals("[29] FORMAT PARQUET is only supported on WAL tables", e.getMessage());
            }
        });
    }

    @Test
    public void testCreateTableFormatAfterBypassWalRejected() throws Exception {
        // FORMAT PARQUET still requires WAL even when placed after BYPASS WAL.
        // The position reported in the error points at the FORMAT clause, not
        // the BYPASS WAL clause.
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL FORMAT PARQUET");
                fail("FORMAT PARQUET should be rejected on non-WAL CREATE TABLE");
            } catch (SqlException e) {
                assertEquals("[83] FORMAT PARQUET is only supported on WAL tables", e.getMessage());
            }
        });
    }

    @Test
    public void testCreateTableFormatAfterDedup() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, n LONG) TIMESTAMP(ts) PARTITION BY DAY WAL " +
                    "DEDUP UPSERT KEYS(ts) FORMAT PARQUET");
            assertTableFormat("tango", TableUtils.TABLE_FORMAT_PARQUET);
        });
    }

    @Test
    public void testCreateTableFormatAfterWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL FORMAT PARQUET");
            assertTableFormat("tango", TableUtils.TABLE_FORMAT_PARQUET);
        });
    }

    @Test
    public void testCreateTableFormatBeforeDedup() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, n LONG) TIMESTAMP(ts) PARTITION BY DAY WAL " +
                    "FORMAT PARQUET DEDUP UPSERT KEYS(ts)");
            assertTableFormat("tango", TableUtils.TABLE_FORMAT_PARQUET);
        });
    }

    @Test
    public void testCreateTableFormatDuplicateAcrossDedupRejected() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE tango (ts TIMESTAMP, n LONG) TIMESTAMP(ts) PARTITION BY DAY " +
                        "FORMAT PARQUET WAL DEDUP UPSERT KEYS(ts) FORMAT NATIVE");
                fail("duplicate FORMAT clause should be rejected");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "duplicate FORMAT clause");
            }
        });
    }

    @Test
    public void testCreateTableFormatDuplicateAcrossWalRejected() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " +
                        "FORMAT NATIVE WAL FORMAT PARQUET");
                fail("duplicate FORMAT clause should be rejected");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "duplicate FORMAT clause");
            }
        });
    }

    @Test
    public void testCreateTableFormatNative() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT NATIVE WAL");
            assertTableFormat("tango", TableUtils.TABLE_FORMAT_NATIVE);
        });
    }

    @Test
    public void testCreateTableFormatParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            assertTableFormat("tango", TableUtils.TABLE_FORMAT_PARQUET);
        });
    }

    @Test
    public void testCreateTableFormatParquetRejectedOnNonWal() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET BYPASS WAL");
                fail("FORMAT PARQUET should be rejected on non-WAL CREATE TABLE");
            } catch (SqlException e) {
                assertEquals("[72] FORMAT PARQUET is only supported on WAL tables", e.getMessage());
            }
        });
    }

    @Test
    public void testCreateTableInvalidFormatValue() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT BANANA WAL");
                fail("Invalid format value accepted");
            } catch (SqlException e) {
                assertEquals("[72] 'parquet' or 'native' expected", e.getMessage());
            }
        });
    }

    @Test
    public void testCreateTableNativeIsDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertTableFormat("tango", TableUtils.TABLE_FORMAT_NATIVE);
        });
    }

    @Test
    public void testCreateTableUnpartitionedIgnoresFormat() throws Exception {
        // FORMAT lives inside the PARTITION BY clause, so a non-partitioned
        // table never sees it. Confirm the table still creates and stays NATIVE.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts)");
            assertTableFormat("tango", TableUtils.TABLE_FORMAT_NATIVE);
        });
    }

    @Test
    public void testCreateTableUnpartitionedWithFormatParquetRejected() throws Exception {
        // A non-partitioned table cannot be WAL, and FORMAT PARQUET requires
        // WAL. The validation surfaces that via the WAL message.
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) FORMAT PARQUET");
                fail("FORMAT clause on a non-partitioned table should be rejected");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "FORMAT PARQUET is only supported on WAL tables");
            }
        });
    }

    @Test
    public void testNewPartitionLandsAsParquetWithVarSizeColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, s STRING, v VARCHAR, b BINARY) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            // rnd_bin(lo, hi, nullRate=0) guarantees non-null binary values of
            // length in [lo, hi]; pinning lo == hi == 4 gives a deterministic
            // round-tripped length to assert.
            execute("INSERT INTO tango " +
                    "SELECT '2024-01-01T00:00:00.000000Z'::timestamp, 'hello', 'world'::varchar, rnd_bin(4, 4, 0) " +
                    "UNION ALL " +
                    "SELECT '2024-01-02T00:00:00.000000Z'::timestamp, 'foo', 'bar'::varchar, rnd_bin(4, 4, 0)");
            drainWalQueue();
            assertSql("name\tisParquet\n" +
                            "2024-01-01\ttrue\n" +
                            "2024-01-02\ttrue\n",
                    "SELECT name, isParquet FROM table_partitions('tango')");
            // BINARY prints as empty in the text sink, so round-trip is asserted
            // via length(): non-null bytes survive the parquet write/read path.
            assertSql("ts\ts\tv\tlen\n" +
                            "2024-01-01T00:00:00.000000Z\thello\tworld\t4\n" +
                            "2024-01-02T00:00:00.000000Z\tfoo\tbar\t4\n",
                    "SELECT ts, s, v, length(b) AS len FROM tango");
        });
    }

    @Test
    public void testNewPartitionLandsAsParquetWithSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, sym SYMBOL, n LONG) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            execute("INSERT INTO tango VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 'a', 1), " +
                    "('2024-01-02T00:00:00.000000Z', 'b', 2), " +
                    "('2024-01-03T00:00:00.000000Z', 'a', 3)");
            drainWalQueue();
            assertSql("name\tisParquet\n" +
                            "2024-01-01\ttrue\n" +
                            "2024-01-02\ttrue\n" +
                            "2024-01-03\ttrue\n",
                    "SELECT name, isParquet FROM table_partitions('tango')");
            // Symbol values must round-trip through the parquet file.
            assertSql("ts\tsym\tn\n" +
                            "2024-01-01T00:00:00.000000Z\ta\t1\n" +
                            "2024-01-02T00:00:00.000000Z\tb\t2\n" +
                            "2024-01-03T00:00:00.000000Z\ta\t3\n",
                    "tango");
        });
    }

    @Test
    public void testNewPartitionLandsAsParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            execute("INSERT INTO tango VALUES " +
                    "('2024-01-01T00:00:00.000000Z'), " +
                    "('2024-01-02T00:00:00.000000Z'), " +
                    "('2024-01-03T00:00:00.000000Z')");
            drainWalQueue();
            // Every partition must be parquet because the table is FORMAT PARQUET.
            assertSql("name\tisParquet\n" +
                            "2024-01-01\ttrue\n" +
                            "2024-01-02\ttrue\n" +
                            "2024-01-03\ttrue\n",
                    "SELECT name, isParquet FROM table_partitions('tango')");
        });
    }

    @Test
    public void testFreshParquetMultiRowGroupSingleCommit() throws Exception {
        // A single WAL commit that creates a fresh FORMAT PARQUET partition
        // larger than the configured row-group size drives writeFreshParquetFromO3
        // through write_chunk, which splits the partition into
        // ceil(rowCount / rowGroupSize) row groups. With a 100-row group size and
        // 200 rows in one partition, the file must carry exactly 2 row groups and
        // all 200 rows must round-trip. This guards against the strided timestamp
        // encoder ignoring per-row-group bounds and re-emitting the whole column
        // in every row group.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            // 200 rows at 1-second spacing stay within a single day partition and
            // land in one WAL commit.
            execute("INSERT INTO tango " +
                    "SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 1_000_000L), x " +
                    "FROM long_sequence(200)");
            drainWalQueue();

            assertSql("name\tisParquet\n" +
                            "2024-01-01\ttrue\n",
                    "SELECT name, isParquet FROM table_partitions('tango')");

            assertEquals(2, getParquetRowGroupCount("tango"));

            // Read actual row data, not metadata. count() and min/max(ts) are
            // satisfied by row-group num_rows and parquet column statistics, so
            // they pass even when the timestamp column is re-emitted in full for
            // every row group. Pairing each v with its timestamp forces a real
            // per-row read: v=101 is the first row of the second row group, so a
            // strided encoder ignoring row-group bounds would pair it with the
            // first timestamp (00:00:00) instead of 00:01:40.
            assertSql("v\tts\n" +
                            "1\t2024-01-01T00:00:00.000000Z\n" +
                            "100\t2024-01-01T00:01:39.000000Z\n" +
                            "101\t2024-01-01T00:01:40.000000Z\n" +
                            "200\t2024-01-01T00:03:19.000000Z\n",
                    "SELECT v, ts FROM tango WHERE v IN (1, 100, 101, 200) ORDER BY v");
        });
    }

    @Test
    public void testFreshParquetTimestampDeltaBinaryPackedEncoding() throws Exception {
        assertParquetTimestampRoundTrip("delta_binary_packed");
    }

    @Test
    public void testFreshParquetTimestampPlainEncoding() throws Exception {
        assertParquetTimestampRoundTrip("plain");
    }

    @Test
    public void testFreshParquetTimestampRleDictionaryEncoding() throws Exception {
        assertParquetTimestampRoundTrip("rle_dictionary");
    }

    /**
     * Exercises both `writeFreshParquetFromO3` (first insert into each
     * partition) and `copyO3ToRowGroup` (second insert into the same partition
     * at a later timestamp) for a designated-timestamp column whose PARQUET
     * encoding has been explicitly set. Catches any regression where the
     * strided merge-index layout is mishandled on the Rust side for a given
     * encoding.
     */
    private void assertParquetTimestampRoundTrip(String encoding) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (" +
                    "  ts TIMESTAMP PARQUET(" + encoding + "), " +
                    "  v LONG" +
                    ") TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");

            // First commit: three partitions, each lands via
            // writeFreshParquetFromO3.
            execute("INSERT INTO tango VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 1), " +
                    "('2024-01-02T00:00:00.000000Z', 2), " +
                    "('2024-01-03T00:00:00.000000Z', 3)");
            drainWalQueue();
            assertSql("name\tisParquet\n" +
                            "2024-01-01\ttrue\n" +
                            "2024-01-02\ttrue\n" +
                            "2024-01-03\ttrue\n",
                    "SELECT name, isParquet FROM table_partitions('tango')");

            // Second commit: each partition gets a later timestamp. With no
            // overlap to the existing row group, the merge strategy picks
            // COPY_O3 — exercising copyO3ToRowGroup.
            execute("INSERT INTO tango VALUES " +
                    "('2024-01-01T12:00:00.000000Z', 4), " +
                    "('2024-01-02T12:00:00.000000Z', 5), " +
                    "('2024-01-03T12:00:00.000000Z', 6)");
            drainWalQueue();

            assertSql("ts\tv\n" +
                            "2024-01-01T00:00:00.000000Z\t1\n" +
                            "2024-01-01T12:00:00.000000Z\t4\n" +
                            "2024-01-02T00:00:00.000000Z\t2\n" +
                            "2024-01-02T12:00:00.000000Z\t5\n" +
                            "2024-01-03T00:00:00.000000Z\t3\n" +
                            "2024-01-03T12:00:00.000000Z\t6\n",
                    "tango");

            // Min/max round-trip — sensitive to off-by-one or wrong-stride
            // reads of the merge index.
            assertSql("min\tmax\tcount\n" +
                            "2024-01-01T00:00:00.000000Z\t2024-01-03T12:00:00.000000Z\t6\n",
                    "SELECT min(ts), max(ts), count() FROM tango");
        });
    }

    /**
     * Inject a failure in writeFreshParquetFromO3 by failing the _pm file open
     * and assert (a) the WAL apply surfaces the error by suspending the table,
     * (b) the partition is not registered, (c) no malloc'd buffers are leaked
     * (assertMemoryLeak guards this).
     */
    @Test
    public void testFreshParquetWriteFailureCleansUp() throws Exception {
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, TableUtils.PARQUET_METADATA_FILE_NAME)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, n LONG) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            execute("INSERT INTO tango VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            // The drain hits the failure: writer goes distressed, malloc'd
            // buffers are freed in the finally block, and the WAL apply error
            // is surfaced by suspending the table.
            drainWalQueue();
            TableToken token = engine.verifyTableName("tango");
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
            // After failure, the partition is not registered.
            assertSql("name\tisParquet\n", "SELECT name, isParquet FROM table_partitions('tango')");
        });
    }

    /**
     * Fail the first _pm open on a FORMAT PARQUET table that has no committed
     * rows yet, then let the retry succeed. The first apply attempt should
     * leave the table suspended with no partition registered; after RESUME WAL
     * the same WAL transaction must replay and produce a parquet partition
     * with the original row. Exercises the empty-placeholder branch in
     * processWalCommit: the inbound row is forced into a full commit (no LAG),
     * the parquet write aborts before the native placeholder is replaced, and
     * the WAL retry recovers the data.
     */
    @Test
    public void testFreshParquetWriteFailureResumeRecovers() throws Exception {
        AtomicInteger attempt = new AtomicInteger();
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, TableUtils.PARQUET_METADATA_FILE_NAME)
                        && attempt.getAndIncrement() == 0) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, n LONG) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            execute("INSERT INTO tango VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            TableToken token = engine.verifyTableName("tango");
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
            assertSql("name\tisParquet\n", "SELECT name, isParquet FROM table_partitions('tango')");

            execute("ALTER TABLE tango RESUME WAL");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertSql("name\tisParquet\n2024-01-01\ttrue\n",
                    "SELECT name, isParquet FROM table_partitions('tango')");
            assertSql("ts\tn\n2024-01-01T00:00:00.000000Z\t1\n", "tango");
        });
    }

    @Test
    public void testO3InsertCreatesParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, n LONG) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            // Seed one partition first.
            execute("INSERT INTO tango VALUES ('2024-01-05T00:00:00.000000Z', 5)");
            drainWalQueue();
            // Now insert a backdated row that creates a new partition before the seed.
            execute("INSERT INTO tango VALUES ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();
            assertSql("name\tisParquet\n" +
                            "2024-01-02\ttrue\n" +
                            "2024-01-05\ttrue\n",
                    "SELECT name, isParquet FROM table_partitions('tango')");
        });
    }

    @Test
    public void testParquetUpdateAppendInOrder() throws Exception {
        // After the initial parquet write, a subsequent in-order WAL apply
        // into the same partition must rewrite the parquet file via the
        // existing processParquetPartition path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, n LONG) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            execute("INSERT INTO tango VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            execute("INSERT INTO tango VALUES ('2024-01-01T00:00:01.000000Z', 2)");
            drainWalQueue();
            assertSql("name\tisParquet\n" +
                            "2024-01-01\ttrue\n",
                    "SELECT name, isParquet FROM table_partitions('tango')");
            assertSql("ts\tn\n" +
                            "2024-01-01T00:00:00.000000Z\t1\n" +
                            "2024-01-01T00:00:01.000000Z\t2\n",
                    "tango");
        });
    }

    @Test
    public void testFlippingFormatToParquetLeavesOldPartitionsNative() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, n LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Three native partitions.
            execute("INSERT INTO tango VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 1), " +
                    "('2024-01-02T00:00:00.000000Z', 2), " +
                    "('2024-01-03T00:00:00.000000Z', 3)");
            drainWalQueue();
            // Flip default format to parquet, then add a fourth partition.
            execute("ALTER TABLE tango SET FORMAT PARQUET");
            drainWalQueue();
            execute("INSERT INTO tango VALUES ('2024-01-04T00:00:00.000000Z', 4)");
            drainWalQueue();
            assertSql("name\tisParquet\n" +
                            "2024-01-01\tfalse\n" +
                            "2024-01-02\tfalse\n" +
                            "2024-01-03\tfalse\n" +
                            "2024-01-04\ttrue\n",
                    "SELECT name, isParquet FROM table_partitions('tango')");
        });
    }

    @Test
    public void testShowCreateTableEmitsFormatParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            assertSql("""
                            ddl
                            CREATE TABLE 'tango' (\s
                            \tts TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY FORMAT PARQUET;
                            """,
                    "SHOW CREATE TABLE tango");
        });
    }

    @Test
    public void testShowCreateTableOmitsFormatNative() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertSql("""
                            ddl
                            CREATE TABLE 'tango' (\s
                            \tts TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY;
                            """,
                    "SHOW CREATE TABLE tango");
        });
    }

    @Test
    public void testTruncateThenFormatChangeProducesParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, n LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO tango VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 1), " +
                    "('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();
            assertSql("name\tisParquet\n" +
                            "2024-01-01\tfalse\n" +
                            "2024-01-02\tfalse\n",
                    "SELECT name, isParquet FROM table_partitions('tango')");

            // Wipe all rows. After this txWriter.getRowCount() == 0 and
            // partitionCount == 0, the same state as a fresh table.
            execute("TRUNCATE TABLE tango");
            drainWalQueue();
            assertSql("name\tisParquet\n", "SELECT name, isParquet FROM table_partitions('tango')");

            execute("ALTER TABLE tango SET FORMAT PARQUET");
            drainWalQueue();
            assertTableFormat("tango", TableUtils.TABLE_FORMAT_PARQUET);

            // The next insert exercises the empty-placeholder branch in
            // processWalCommit. The new partition must be written as parquet.
            execute("INSERT INTO tango VALUES ('2024-01-03T00:00:00.000000Z', 3)");
            drainWalQueue();
            assertSql("name\tisParquet\n2024-01-03\ttrue\n",
                    "SELECT name, isParquet FROM table_partitions('tango')");
            assertSql("ts\tn\n2024-01-03T00:00:00.000000Z\t3\n", "tango");
        });
    }

    private void assertTableFormat(String tableName, int expected) {
        TableToken token = engine.verifyTableName(tableName);
        try (TableMetadata metadata = engine.getTableMetadata(token)) {
            assertEquals(expected, metadata.getTableFormat());
        }
    }

    private int getParquetRowGroupCount(String tableName) {
        try (TableReader reader = getReader(tableName)) {
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                    continue;
                }
                reader.openPartition(i);
                ParquetMetaFileReader meta = reader
                        .getAndInitParquetPartitionDecoder(i)
                        .metadata();
                return meta.getRowGroupCount();
            }
        }
        return -1;
    }
}
