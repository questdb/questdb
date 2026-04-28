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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Integration tests for O3 writes into Parquet partitions, covering the three
 * rewrite triggers:
 * <ul>
 *   <li>Single row group &mdash; always rewritten to avoid dead space</li>
 *   <li>Unused-bytes ratio exceeds threshold</li>
 *   <li>Absolute unused bytes exceeds threshold</li>
 * </ul>
 */
public class ParquetWriteTest extends AbstractCairoTest {

    @Test
    public void testArrayCorruptionAfterWriterReopenBeforeParquetConversion() throws Exception {
        // Reproduces DOUBLE[][] data corruption from WalWriterFuzzTest
        // (seeds 286709679787041L, 1772793547818L).
        //
        // Matches the fuzz test's exact scenario for partition 2022-02-27:
        //  - 896 initial rows (no array column)
        //  - ADD COLUMN DOUBLE[][] → column_top = 896
        //  - INSERT 202 rows with array data
        //  - Writer closes (engine.releaseInactive)
        //  - Convert partition to parquet (column_top=896)
        //  - Then: native→append→parquet→native→append→parquet→O3 merge
        //
        // Tests both A (writer stays open) and B (writer closes) scenarios
        // to detect if close/reopen introduces corruption.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY HOUR WAL
                            """
            );

            // Insert 896 rows (matching fuzz test column_top=896)
            // Timestamps: 2020-01-01T00:00:00 through ~2020-01-01T00:14:56 (every second)
            for (int batch = 0; batch < 9; batch++) {
                int lo = batch * 100;
                int hi = Math.min(lo + 100, 896);
                StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x) VALUES\n");
                for (int i = lo; i < hi; i++) {
                    if (i > lo) {
                        sb.append(",\n");
                    }
                    sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                            .append(i).append(')');
                }
                execute(sb.toString());
            }
            // Row in next partition so 2020-01-01T00 is non-active
            execute("INSERT INTO x(ts, x) VALUES ('2020-01-01T01:00:00.000Z', -1)");
            drainWalQueue();
            engine.releaseInactive();

            // ADD COLUMN DOUBLE[][] → column_top = 896
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            // INSERT 202 rows with 3x3 null-double arrays (matching fuzz test)
            StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 896; i < 1098; i++) {
                if (i > 896) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            // Apply ADD COLUMN + INSERT together (same WAL batch)
            drainWalQueue();

            // Verify baseline before any conversions
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 896"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 897"
            );

            // *** Critical point: writer closes before convert to parquet ***
            engine.releaseInactive();

            // Convert to parquet (column_top=896, 1098 total rows)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");
            drainWalQueue();

            // Check immediately after parquet conversion
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 896"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 897"
            );

            // Convert back to native (materializes all 1098 rows)
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01T00'");
            drainWalQueue();

            // Check after native conversion
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 896"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 897"
            );

            engine.releaseInactive();

            // Append more rows, then convert to parquet again (column_top=0)
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 1098; i < 1722; i++) {
                if (i > 1098) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();
            engine.releaseInactive();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");
            drainWalQueue();
            engine.releaseInactive();

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01T00'");
            drainWalQueue();
            engine.releaseInactive();

            // Append more
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 1722; i < 2256; i++) {
                if (i > 1722) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();
            engine.releaseInactive();

            // Convert to parquet (this is the file the O3 merge will read)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");
            drainWalQueue();

            // Check the parquet file
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 896"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 897"
            );

            engine.releaseInactive();

            // O3 merge into parquet — matches fuzz test's 949-row merge
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 0; i < 949; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                // Use half-second timestamps to interleave with existing full-second data
                sb.append(String.format("('2020-01-01T00:%02d:%02d.500Z', ", i / 60, i % 60))
                        .append(20_000 + i)
                        .append(", ARRAY[[").append(i).append("]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Verify after O3 merge
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 896"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 897"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:00:00.000000Z\t0\tnull
                            """,
                    "SELECT * FROM x WHERE x = 0"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:00:00.500000Z\t20000\t[[0.0]]
                            """,
                    "SELECT * FROM x WHERE x = 20000"
            );
        });
    }

    @Test
    public void testArrayCorruptionMinimalRoundTripInSingleWriterSession() throws Exception {
        // Minimal reproducer: just CONVERT TO PARQUET → CONVERT TO NATIVE →
        // CONVERT TO PARQUET in one writer session. No inserts between conversions.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY HOUR WAL
                            """
            );

            // Insert 896 rows
            for (int batch = 0; batch < 9; batch++) {
                int lo = batch * 100;
                int hi = Math.min(lo + 100, 896);
                StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x) VALUES\n");
                for (int i = lo; i < hi; i++) {
                    if (i > lo) {
                        sb.append(",\n");
                    }
                    sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                            .append(i).append(')');
                }
                execute(sb.toString());
            }
            drainWalQueue();
            engine.releaseInactive();

            // ADD COLUMN DOUBLE[][] → column_top = 896
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 896; i < 1098; i++) {
                if (i > 896) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();
            engine.releaseInactive();

            // Queue just: CONVERT TO PARQUET → CONVERT TO NATIVE → CONVERT TO PARQUET
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01T00'");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");

            drainWalQueue();

            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 896"
            );
        });
    }

    @Test
    public void testArrayCorruptionMultipleRoundTripsInSingleWriterSession() throws Exception {
        // Reproduces DOUBLE[][] data corruption when multiple parquet↔native
        // round-trips happen in a single writer session without ejection.
        //
        // Bug: WalWriterFuzzTest#testConvertPartitionToParquet
        //      seeds 286709679787041L, 1772793547818L
        // Error: Row 1144 column new_col_8[DOUBLE[][]]
        //        expected:<...[[[null,null,null],[null,null,null],[null,null,null]]]>
        //        but was:<...[null]>
        //
        // The critical difference from testArrayCorruptionAfterWriterReopenBeforeParquetConversion:
        // no engine.releaseInactive() between operations — all conversions
        // and inserts execute in one writer session via a single drainWalQueue().
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY HOUR WAL
                            """
            );

            // Insert 896 rows (matching fuzz test column_top=896)
            for (int batch = 0; batch < 9; batch++) {
                int lo = batch * 100;
                int hi = Math.min(lo + 100, 896);
                StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x) VALUES\n");
                for (int i = lo; i < hi; i++) {
                    if (i > lo) {
                        sb.append(",\n");
                    }
                    sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                            .append(i).append(')');
                }
                execute(sb.toString());
            }
            drainWalQueue();
            engine.releaseInactive();

            // ADD COLUMN DOUBLE[][] → column_top = 896
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            // INSERT 202 rows with 3x3 null-double arrays (matching fuzz test)
            StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 896; i < 1098; i++) {
                if (i > 896) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();
            engine.releaseInactive();

            // Verify baseline
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 897"
            );

            // Queue ALL critical operations for a single writer session

            // 1. native→parquet (column_top=896→zeroed to 0)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");

            // 2. parquet→native (materializes all 1098 rows, column_top=0)
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01T00'");

            // 3. Insert 624 more rows (timestamps 00:18:18–00:28:41)
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 1098; i < 1722; i++) {
                if (i > 1098) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());

            // 4. native→parquet (1722 rows)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");

            // 5. parquet→native (1722 rows)
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01T00'");

            // 6. Insert 534 more rows (timestamps 00:28:42–00:37:35)
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 1722; i < 2256; i++) {
                if (i > 1722) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());

            // 7. native→parquet (2256 rows — corruption manifests here)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");

            // Apply ALL operations in one writer session
            drainWalQueue();

            // Verify row 897 has 3x3 matrix, not NULL
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 896"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 897"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:00:00.000000Z\t0\tnull
                            """,
                    "SELECT * FROM x WHERE x = 0"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:18:18.000000Z\t1098\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 1098"
            );
        });
    }

    @Test
    public void testCopyRowGroupPreservesBloomFilterOffset() throws Exception {
        // Small row group size to produce 3 row groups.
        // Set absolute threshold low so the second O3 triggers a rewrite.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 100);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            // Insert 12 rows → 3 row groups (RG0, RG1, RG2) of 4 rows each.
            // Values chosen so the bloom filter is the only way to skip
            // the copied row group for specific query values.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z'),
                            (9, '2020-01-01T08:00:00.000Z'),
                            (10, '2020-01-01T09:00:00.000Z'),
                            (11, '2020-01-01T10:00:00.000Z'),
                            (12, '2020-01-01T11:00:00.000Z')
                            """
            );
            // Extra row in a different partition so 2020-01-01 is not the last.
            execute("INSERT INTO x(x, ts) VALUES (1000, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // Convert with bloom filters on the 'x' column.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01' WITH (bloom_filter_columns = 'x')");
            drainWalQueue();

            // First O3: UPDATE mode. Inserts into RG0's time range.
            // This replaces RG0, appending the new RG0' at the end of the file,
            // leaving dead RG0 data at the original position near the file start.
            // RG0' values: {1, 100, 2, 101, 3, 4} → min=1, max=101
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (100, '2020-01-01T00:30:00.000Z'),
                            (101, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            // Second O3: targets RG1's time range (different from the first O3).
            // accumulated unused_bytes > 100 → REWRITE mode.
            // In the REWRITE, RG0' (appended at the end of the old file) is
            // raw-copied to the beginning of the new file via copy_row_group,
            // creating a large offset_delta. copy_row_group must adjust
            // bloom_filter_offset by the same delta as data_page_offset.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (200, '2020-01-01T04:30:00.000Z'),
                            (201, '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            // After REWRITE, the parquet partition has 3 row groups:
            //   RG0' (copied): x in {1, 100, 2, 101, 3, 4} → min=1, max=101
            //   RG1' (merged): x in {5, 200, 6, 201, 7, 8} → min=5, max=201
            //   RG2  (copied): x in {9, 10, 11, 12}         → min=9, max=12
            //
            // Query: WHERE x = 50
            //   RG0': 50 in [1,101] → can't skip by min/max → bloom filter needed
            //         50 NOT in {1,100,2,101,3,4} → bloom SHOULD skip
            //   RG1': 50 in [5,201] → can't skip by min/max → bloom filter needed
            //         50 NOT in {5,200,6,201,7,8} → bloom SHOULD skip
            //   RG2:  50 > 12 → skipped by min/max alone
            //
            // If bloom_filter_offset is stale on copied RG0', its bloom filter
            // read fails silently → RG0' is NOT skipped → fewer skips.
            // Verify data correctness.
            assertSql(
                    """
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            100\t2020-01-01T00:30:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            101\t2020-01-01T01:30:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            4\t2020-01-01T03:00:00.000000Z
                            5\t2020-01-01T04:00:00.000000Z
                            200\t2020-01-01T04:30:00.000000Z
                            6\t2020-01-01T05:00:00.000000Z
                            201\t2020-01-01T05:30:00.000000Z
                            7\t2020-01-01T06:00:00.000000Z
                            8\t2020-01-01T07:00:00.000000Z
                            9\t2020-01-01T08:00:00.000000Z
                            10\t2020-01-01T09:00:00.000000Z
                            11\t2020-01-01T10:00:00.000000Z
                            12\t2020-01-01T11:00:00.000000Z
                            1000\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );

            // Bloom filter skip check. assertSql runs a single query execution,
            // so the counter reflects exactly one scan of the 3 row groups.
            // Query: WHERE x = 50
            //   RG0' (copied): 50 in [1,101] → needs bloom filter → should skip
            //   RG1' (merged): 50 in [5,201] → needs bloom filter → should skip
            //   RG2  (copied): 50 > 12       → skipped by min/max
            // All 3 row groups should be skipped → counter = 3.
            // With stale bloom_filter_offset on copied RG0', bloom filter read
            // fails silently → RG0' NOT skipped → counter = 2.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertSql("x\n", "SELECT x FROM x WHERE x = 50");
            Assert.assertEquals(
                    "bloom filter should skip all 3 row groups (2 by bloom, 1 by min/max)",
                    3,
                    ParquetRowGroupFilter.getRowGroupsSkipped()
            );
        });
    }

    @Test
    public void testFooterCacheUsedInUpdateMode() throws Exception {
        // Small row group size to produce multiple row groups.
        // Disable rewrite: ratio=1.0 (impossible to exceed), max_bytes=Long.MAX_VALUE.
        // This forces every O3 merge to use the UPDATE path, exercising footer_cache.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 -> 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            int rowGroupCountBefore = -1;
            try (TableReader reader = getReader("x")) {
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                        continue;
                    }
                    reader.openPartition(i);
                    ParquetPartitionDecoder decoder = reader.getAndInitParquetMetaPartitionDecoder(i);
                    rowGroupCountBefore = decoder.metadata().getRowGroupCount();
                    Assert.assertEquals("initial row group count", 2, rowGroupCountBefore);
                }
            }
            Assert.assertTrue("should find parquet partition", rowGroupCountBefore > 0);

            // First O3: UPDATE mode. FooterCache parses the original footer,
            // scans row group offsets, and appends a replacement row group.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (9, '2020-01-01T00:30:00.000Z'),
                            (10, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            int rowGroupCountAfterFirst = 0;
            long unusedAfterFirst = 0;
            try (TableReader reader = getReader("x")) {
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                        continue;
                    }
                    reader.openPartition(i);
                    rowGroupCountAfterFirst = reader.getAndInitParquetMetaPartitionDecoder(i).metadata().getRowGroupCount();
                    try (ParquetFileDecoder footerDecoder = new ParquetFileDecoder()) {
                        footerDecoder.of(reader.getParquetAddr(i), reader.getParquetFileSize(i), MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                        unusedAfterFirst = footerDecoder.metadata().getUnusedBytes();
                    }
                    Assert.assertTrue(
                            "row group count should be >= 2 after first O3 update, got " + rowGroupCountAfterFirst,
                            rowGroupCountAfterFirst >= 2
                    );
                    Assert.assertTrue(
                            "unused_bytes should be > 0 after first update, got " + unusedAfterFirst,
                            unusedAfterFirst > 0
                    );
                }
            }

            // Second O3: UPDATE mode again. FooterCache re-parses the updated footer
            // (which already contains dead space from the first update) and appends
            // another replacement row group.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (11, '2020-01-01T04:30:00.000Z'),
                            (12, '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            int rowGroupCountAfterSecond;
            long unusedAfterSecond;
            try (TableReader reader = getReader("x")) {
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                        continue;
                    }
                    reader.openPartition(i);
                    rowGroupCountAfterSecond = reader.getAndInitParquetMetaPartitionDecoder(i).metadata().getRowGroupCount();
                    try (ParquetFileDecoder footerDecoder = new ParquetFileDecoder()) {
                        footerDecoder.of(reader.getParquetAddr(i), reader.getParquetFileSize(i), MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                        unusedAfterSecond = footerDecoder.metadata().getUnusedBytes();
                    }
                    Assert.assertTrue(
                            "row group count should be >= previous after second O3 update, was " + rowGroupCountAfterFirst + ", got " + rowGroupCountAfterSecond,
                            rowGroupCountAfterSecond >= rowGroupCountAfterFirst
                    );
                    Assert.assertTrue(
                            "unused_bytes should grow after second update, got " + unusedAfterSecond,
                            unusedAfterSecond > unusedAfterFirst
                    );
                }
            }

            assertSql(
                    """
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            9\t2020-01-01T00:30:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            10\t2020-01-01T01:30:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            4\t2020-01-01T03:00:00.000000Z
                            5\t2020-01-01T04:00:00.000000Z
                            11\t2020-01-01T04:30:00.000000Z
                            6\t2020-01-01T05:00:00.000000Z
                            12\t2020-01-01T05:30:00.000000Z
                            7\t2020-01-01T06:00:00.000000Z
                            8\t2020-01-01T07:00:00.000000Z
                            100\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testNativeToParquetRoundTripColumnTopEqualsRowCount() throws Exception {
        // When a column is added after all rows already exist, the native
        // partition starts with column_top == partitionRowCount. Converting to
        // parquet must materialize those nulls into the parquet chunks and
        // normalize the parquet-side top to 0. The parquet→native round-trip
        // must then rebuild full-size native null columns rather than treating
        // the all-null parquet chunks as empty data.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts, x) VALUES
                            ('2020-01-01T00:00:00.000Z', 1),
                            ('2020-01-01T01:00:00.000Z', 2),
                            ('2020-01-01T02:00:00.000Z', 3),
                            ('2020-01-01T03:00:00.000Z', 4)
                            """
            );
            drainWalQueue();

            // Add columns after all data exists — column_top == 4 == partitionRowCount.
            // Cover various types: fixed (LONG), var-size (VARCHAR), and SYMBOL.
            execute("ALTER TABLE x ADD COLUMN n LONG");
            execute("ALTER TABLE x ADD COLUMN v VARCHAR");
            execute("ALTER TABLE x ADD COLUMN s SYMBOL");
            drainWalQueue();

            final String expected = """
                    ts\tx\tn\tv\ts
                    2020-01-01T00:00:00.000000Z\t1\tnull\t\t
                    2020-01-01T01:00:00.000000Z\t2\tnull\t\t
                    2020-01-01T02:00:00.000000Z\t3\tnull\t\t
                    2020-01-01T03:00:00.000000Z\t4\tnull\t\t
                    """;

            assertSql(expected, "SELECT * FROM x");

            // native → parquet → native round-trip
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertPmAllNullChunkUsesZeroPointers("x", "n", ColumnType.LONG, "v", ColumnType.VARCHAR_SLICE);
            assertSql(expected, "SELECT * FROM x");

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertSql(expected, "SELECT * FROM x");

            // Second round-trip to verify the normalized representation remains stable.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertSql(expected, "SELECT * FROM x");
        });
    }

    @Test
    public void testNativeToParquetRoundTripMultiDimArrayNulls() throws Exception {
        // A DOUBLE[][] column whose values are arrays of null elements
        // (e.g., [[null,null],[null,null]]) must survive a native→parquet→native
        // round-trip without collapsing into plain NULL.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts, x) VALUES
                            ('2020-01-01T00:00:00.000Z', 1),
                            ('2020-01-01T01:00:00.000Z', 2),
                            ('2020-01-01T02:00:00.000Z', 3),
                            ('2020-01-01T03:00:00.000Z', 4)
                            """
            );
            drainWalQueue();

            // Add a DOUBLE[][] column with column top = 4
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            drainWalQueue();

            // Insert rows where 'a' is an array of null elements
            execute(
                    """
                            INSERT INTO x(ts, x, a) VALUES
                            ('2020-01-01T04:00:00.000Z', 5, ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double]]),
                            ('2020-01-01T05:00:00.000Z', 6, ARRAY[[1.5, null::double], [null::double, 2.5]]),
                            ('2020-01-01T06:00:00.000Z', 7, ARRAY[[null::double]])
                            """
            );
            drainWalQueue();

            final String expected = """
                    ts\tx\ta
                    2020-01-01T00:00:00.000000Z\t1\tnull
                    2020-01-01T01:00:00.000000Z\t2\tnull
                    2020-01-01T02:00:00.000000Z\t3\tnull
                    2020-01-01T03:00:00.000000Z\t4\tnull
                    2020-01-01T04:00:00.000000Z\t5\t[[null,null,null],[null,null,null]]
                    2020-01-01T05:00:00.000000Z\t6\t[[1.5,null],[null,2.5]]
                    2020-01-01T06:00:00.000000Z\t7\t[[null]]
                    """;

            assertSql(expected, "SELECT * FROM x");

            // native → parquet → native round-trip
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertSql(expected, "SELECT * FROM x");

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertSql(expected, "SELECT * FROM x");
        });
    }

    @Test
    public void testNativeToParquetRoundTripNonExistentColumn() throws Exception {
        // When a column is added on a later partition, earlier partitions have
        // no native column files for it (getColumnTop returns -1). Converting
        // such a partition to parquet must synthesize all-null chunks, and the
        // parquet→native round-trip must materialize full native null columns
        // from those chunks even though the parquet-side top is normalized to 0.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts, x) VALUES
                            ('2020-01-01T00:00:00.000Z', 1),
                            ('2020-01-01T01:00:00.000Z', 2),
                            ('2020-01-01T02:00:00.000Z', 3),
                            ('2020-01-01T03:00:00.000Z', 4)
                            """
            );
            // Insert into a second partition so that it becomes the active one
            execute("INSERT INTO x(ts, x) VALUES ('2020-01-02T00:00:00.000Z', 10)");
            drainWalQueue();

            // Add columns while 2020-01-02 is the active partition.
            // For 2020-01-01, getColumnTop returns -1 (column does not exist).
            execute("ALTER TABLE x ADD COLUMN n LONG");
            execute("ALTER TABLE x ADD COLUMN v VARCHAR");
            execute("ALTER TABLE x ADD COLUMN s SYMBOL");
            drainWalQueue();

            final String expected01 = """
                    ts\tx\tn\tv\ts
                    2020-01-01T00:00:00.000000Z\t1\tnull\t\t
                    2020-01-01T01:00:00.000000Z\t2\tnull\t\t
                    2020-01-01T02:00:00.000000Z\t3\tnull\t\t
                    2020-01-01T03:00:00.000000Z\t4\tnull\t\t
                    """;

            assertSql(expected01, "SELECT * FROM x WHERE ts < '2020-01-02'");

            // native → parquet → native round-trip for 2020-01-01
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertSql(expected01, "SELECT * FROM x WHERE ts < '2020-01-02'");

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertSql(expected01, "SELECT * FROM x WHERE ts < '2020-01-02'");

            // Second round-trip to verify the normalized representation remains stable.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertSql(expected01, "SELECT * FROM x WHERE ts < '2020-01-02'");
        });
    }

    @Test
    public void testO3AfterAddArrayColumnMultipleRowGroups() throws Exception {
        // Exercises the collect_leaf_path() code path in Rust: when a DOUBLE[]
        // column is added after a partition is already in Parquet format, the
        // O3 merge calls copyRowGroupWithNullColumns() for untouched row groups.
        // The null column chunk for an array column uses a nested LIST schema
        // (col_name / list / element), and path_in_schema must contain the full
        // root-to-leaf path, not just [col_name].
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 -> 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Add DOUBLE[] column. Parquet file has 2 columns (x, ts), table now has 3.
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[]");
            drainWalQueue();

            // O3 insert overlapping first row group (00:00-03:00).
            // RG0 is merged (null fill for a), RG1 is copied via
            // copyRowGroupWithNullColumns (null column chunk for a — nested LIST).
            execute(
                    """
                            INSERT INTO x(x, a, ts) VALUES
                            (9, ARRAY[1.5, 2.5], '2020-01-01T00:30:00.000Z'),
                            (10, ARRAY[3.5], '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            x\tts\ta
                            1\t2020-01-01T00:00:00.000000Z\tnull
                            9\t2020-01-01T00:30:00.000000Z\t[1.5,2.5]
                            2\t2020-01-01T01:00:00.000000Z\tnull
                            10\t2020-01-01T01:30:00.000000Z\t[3.5]
                            3\t2020-01-01T02:00:00.000000Z\tnull
                            4\t2020-01-01T03:00:00.000000Z\tnull
                            5\t2020-01-01T04:00:00.000000Z\tnull
                            6\t2020-01-01T05:00:00.000000Z\tnull
                            7\t2020-01-01T06:00:00.000000Z\tnull
                            8\t2020-01-01T07:00:00.000000Z\tnull
                            100\t2020-01-02T00:00:00.000000Z\tnull
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testO3AfterAddArrayColumnNestedListEncoding() throws Exception {
        // Reproduces C2: missing repetition levels in null column chunks for
        // nested LIST schema. When rawArrayEncoding is disabled, DOUBLE[] is
        // encoded as a nested LIST group (col / list / element) with
        // max_rep_level > 0. The null column chunk generated by
        // copyRowGroupWithNullColumns must include both repetition and
        // definition level sections; otherwise split_buffer_v1 misparses
        // the page and produces garbage or crashes.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_RAW_ARRAY_ENCODING_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 -> 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Add DOUBLE[] column. With rawArrayEncoding=false the parquet
            // schema uses nested LIST: a / list / element.
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[]");
            drainWalQueue();

            // O3 insert overlapping first row group (00:00-03:00).
            // RG0 is merged (null fill for a), RG1 is copied via
            // copyRowGroupWithNullColumns which must emit correct rep+def
            // levels for the nested LIST null column chunk.
            execute(
                    """
                            INSERT INTO x(x, a, ts) VALUES
                            (9, ARRAY[1.5, 2.5], '2020-01-01T00:30:00.000Z'),
                            (10, ARRAY[3.5], '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            x\tts\ta
                            1\t2020-01-01T00:00:00.000000Z\tnull
                            9\t2020-01-01T00:30:00.000000Z\t[1.5,2.5]
                            2\t2020-01-01T01:00:00.000000Z\tnull
                            10\t2020-01-01T01:30:00.000000Z\t[3.5]
                            3\t2020-01-01T02:00:00.000000Z\tnull
                            4\t2020-01-01T03:00:00.000000Z\tnull
                            5\t2020-01-01T04:00:00.000000Z\tnull
                            6\t2020-01-01T05:00:00.000000Z\tnull
                            7\t2020-01-01T06:00:00.000000Z\tnull
                            8\t2020-01-01T07:00:00.000000Z\tnull
                            100\t2020-01-02T00:00:00.000000Z\tnull
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testO3AfterAddColumnMultipleRowGroups() throws Exception {
        // Small row group size -> multiple row groups. Schema mismatch forces
        // rewrite, exercising copyRowGroupWithNullColumns() for untouched RGs.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, s SYMBOL, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (1, 'a', '2020-01-01T00:00:00.000Z'),
                            (2, 'b', '2020-01-01T01:00:00.000Z'),
                            (3, 'a', '2020-01-01T02:00:00.000Z'),
                            (4, 'c', '2020-01-01T03:00:00.000Z'),
                            (5, 'b', '2020-01-01T04:00:00.000Z'),
                            (6, 'a', '2020-01-01T05:00:00.000Z'),
                            (7, 'c', '2020-01-01T06:00:00.000Z'),
                            (8, 'b', '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, s, ts) VALUES (100, 'd', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 -> 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN y DOUBLE");
            drainWalQueue();

            // O3 insert overlapping first row group (00:00-03:00).
            // RG0 is merged (null fill for y), RG1 is copied via
            // copyRowGroupWithNullColumns (null column chunk for y).
            execute(
                    """
                            INSERT INTO x(x, s, y, ts) VALUES
                            (9, 'b', 1.5, '2020-01-01T00:30:00.000Z'),
                            (10, 'c', 2.5, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            x\ts\tts\ty
                            1\ta\t2020-01-01T00:00:00.000000Z\tnull
                            9\tb\t2020-01-01T00:30:00.000000Z\t1.5
                            2\tb\t2020-01-01T01:00:00.000000Z\tnull
                            10\tc\t2020-01-01T01:30:00.000000Z\t2.5
                            3\ta\t2020-01-01T02:00:00.000000Z\tnull
                            4\tc\t2020-01-01T03:00:00.000000Z\tnull
                            5\tb\t2020-01-01T04:00:00.000000Z\tnull
                            6\ta\t2020-01-01T05:00:00.000000Z\tnull
                            7\tc\t2020-01-01T06:00:00.000000Z\tnull
                            8\tb\t2020-01-01T07:00:00.000000Z\tnull
                            100\td\t2020-01-02T00:00:00.000000Z\tnull
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testO3AfterAddColumnRewriteMode() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, s SYMBOL, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (1, 'a', '2020-01-01T00:00:00.000Z'),
                            (2, 'b', '2020-01-01T06:00:00.000Z'),
                            (3, 'a', '2020-01-01T12:00:00.000Z'),
                            (4, 'c', '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, s, ts) VALUES (100, 'd', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Add a new column after converting to Parquet.
            // The Parquet file has 3 columns (x, s, ts), the table now has 4 (x, s, ts, y).
            execute("ALTER TABLE x ADD COLUMN y DOUBLE");
            drainWalQueue();

            // O3 insert into the parquet partition.
            // Single row group → always REWRITE. Original rows get NULL for y.
            execute(
                    """
                            INSERT INTO x(x, s, y, ts) VALUES
                            (5, 'b', 1.5, '2020-01-01T03:00:00.000Z'),
                            (6, 'a', 2.5, '2020-01-01T09:00:00.000Z'),
                            (7, 'c', 3.5, '2020-01-01T15:00:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            x\ts\tts\ty
                            1\ta\t2020-01-01T00:00:00.000000Z\tnull
                            5\tb\t2020-01-01T03:00:00.000000Z\t1.5
                            2\tb\t2020-01-01T06:00:00.000000Z\tnull
                            6\ta\t2020-01-01T09:00:00.000000Z\t2.5
                            3\ta\t2020-01-01T12:00:00.000000Z\tnull
                            7\tc\t2020-01-01T15:00:00.000000Z\t3.5
                            4\tc\t2020-01-01T18:00:00.000000Z\tnull
                            100\td\t2020-01-02T00:00:00.000000Z\tnull
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testO3AfterAddColumnSingleRowPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (1, '2020-01-01T12:00:00.000Z')");
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN y DOUBLE");
            drainWalQueue();

            // O3 insert into the single-row parquet partition.
            execute("INSERT INTO x(x, y, ts) VALUES (2, 1.5, '2020-01-01T06:00:00.000Z')");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            x\tts\ty
                            2\t2020-01-01T06:00:00.000000Z\t1.5
                            1\t2020-01-01T12:00:00.000000Z\tnull
                            100\t2020-01-02T00:00:00.000000Z\tnull
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testO3AfterAddDedupKeyColumn() throws Exception {
        // Reproduces: dedup key column added after parquet partition exists.
        // O3 merge into that partition enters the dedup code path, where
        // the new column is a dedup key but is missing from the parquet
        // file (decodeIdx == -1), causing an assertion failure or crash.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T06:00:00.000Z'),
                            (3, '2020-01-01T12:00:00.000Z'),
                            (4, '2020-01-01T18:00:00.000Z')
                            """
            );
            // Second partition keeps 2020-01-01 as a non-active partition.
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Add a new column and make it a dedup key.
            // Parquet file has (x, ts); table now has (x, ts, y).
            execute("ALTER TABLE x ADD COLUMN y INT");
            execute("ALTER TABLE x DEDUP ENABLE UPSERT KEYS(ts, y)");
            drainWalQueue();

            // O3 insert with timestamps that duplicate existing rows.
            // Triggers dedup merge, which iterates dedup key columns.
            // Column y is a dedup key but does not exist in the parquet file.
            execute(
                    """
                            INSERT INTO x(x, y, ts) VALUES
                            (5, 10, '2020-01-01T00:00:00.000Z'),
                            (6, 20, '2020-01-01T06:00:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            // Old rows have y=NULL, new rows have y=10/20.
            // Dedup keys (ts, y) differ, so all rows are kept.
            assertSql(
                    """
                            x\tts\ty
                            1\t2020-01-01T00:00:00.000000Z\tnull
                            5\t2020-01-01T00:00:00.000000Z\t10
                            2\t2020-01-01T06:00:00.000000Z\tnull
                            6\t2020-01-01T06:00:00.000000Z\t20
                            3\t2020-01-01T12:00:00.000000Z\tnull
                            4\t2020-01-01T18:00:00.000000Z\tnull
                            100\t2020-01-02T00:00:00.000000Z\tnull
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testO3AfterAddRequiredSymbolColumnMultipleRowGroups() throws Exception {
        // Reproduces: Required Symbol null chunk encoding mismatch.
        //
        // When a SYMBOL column is added after a parquet partition exists and
        // only non-null values are inserted, the symbol map's null flag stays
        // false → the target schema marks it as Required (no def levels).
        // copy_row_group_with_null_columns must generate a Required null chunk
        // (zero-filled Int32), but generate_required_zero_page did not handle
        // ColumnTypeTag::Symbol, falling through to generate_optional_null_page
        // — a page with RLE definition levels in a Required column, producing
        // a malformed parquet file.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 -> 2 row groups (RG0: rows 1-4, RG1: rows 5-8).
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Add a SYMBOL column after the parquet partition exists.
            execute("ALTER TABLE x ADD COLUMN s SYMBOL");
            drainWalQueue();

            // O3 insert with non-null symbol values into RG0's time range.
            // null flag stays false -> Required in target schema.
            // RG0: merged with new rows (s filled for all rows).
            // RG1: copied via copy_row_group_with_null_columns — needs a
            //      Required null chunk for 's'. Bug: Symbol not handled in
            //      generate_required_zero_page -> Optional page for Required column.
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (9, 'a', '2020-01-01T00:30:00.000Z'),
                            (10, 'b', '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            // Verify new O3 rows have correct symbol values.
            assertSql(
                    """
                            x\tts\ts
                            9\t2020-01-01T00:30:00.000000Z\ta
                            10\t2020-01-01T01:30:00.000000Z\tb
                            """,
                    "SELECT * FROM x WHERE x IN (9, 10) ORDER BY ts"
            );

            // Read from the copied row group (RG1) to exercise the symbol
            // null chunk. Before the fix, the null chunk used Plain encoding
            // which the symbol decoder does not support, causing a decode error.
            // Old rows have no symbol data -> NULL (empty).
            assertSql(
                    """
                            x\tts\ts
                            5\t2020-01-01T04:00:00.000000Z\t
                            6\t2020-01-01T05:00:00.000000Z\t
                            7\t2020-01-01T06:00:00.000000Z\t
                            8\t2020-01-01T07:00:00.000000Z\t
                            """,
                    "SELECT * FROM x WHERE x >= 5 AND x <= 8 ORDER BY ts"
            );
        });
    }

    @Test
    public void testO3AfterMultipleAddColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T06:00:00.000Z'),
                            (3, '2020-01-01T12:00:00.000Z'),
                            (4, '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Add two columns after conversion. The parquet file has 2 columns,
            // the table now has 4.
            execute("ALTER TABLE x ADD COLUMN a DOUBLE");
            execute("ALTER TABLE x ADD COLUMN b VARCHAR");
            drainWalQueue();

            // O3 insert with both new columns.
            execute(
                    """
                            INSERT INTO x(x, a, b, ts) VALUES
                            (5, 1.5, 'hello', '2020-01-01T03:00:00.000Z'),
                            (6, 2.5, 'world', '2020-01-01T09:00:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            x\tts\ta\tb
                            1\t2020-01-01T00:00:00.000000Z\tnull\t
                            5\t2020-01-01T03:00:00.000000Z\t1.5\thello
                            2\t2020-01-01T06:00:00.000000Z\tnull\t
                            6\t2020-01-01T09:00:00.000000Z\t2.5\tworld
                            3\t2020-01-01T12:00:00.000000Z\tnull\t
                            4\t2020-01-01T18:00:00.000000Z\tnull\t
                            100\t2020-01-02T00:00:00.000000Z\tnull\t
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testO3IntoParquetAfterColumnTopAndConversionCycles() throws Exception {
        // Reproduces the DOUBLE[][] data corruption seen in WalWriterFuzzTest
        // with seeds 286709679787041L, 1772793547818L.
        //
        // Key sequence:
        // 1. Create partition with initial rows (no array column).
        // 2. ADD COLUMN arr DOUBLE[][] → column_top = initial rows.
        // 3. Insert rows with array data.
        // 4. Convert to parquet (column_top > 0 in QDB metadata).
        // 5. Convert back to native (decoder materializes all rows).
        // 6. Insert more rows.
        // 7. Convert to parquet (column_top = 0 now).
        // 8. O3 insert → merge into parquet.
        // 9. Verify array values are preserved.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );

            // Insert initial rows into partition 2020-01-01 (timestamps every minute)
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO x(ts, x) VALUES\n");
            for (int i = 0; i < 100; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000Z', ").append(i).append(')');
            }
            execute(sb.toString());

            // Insert a row in the next partition so 2020-01-01 is non-active
            execute("INSERT INTO x(ts, x) VALUES ('2020-01-02T00:00:00.000Z', 999)");
            drainWalQueue();

            // Add DOUBLE[][] column → column_top = 100 for partition 2020-01-01
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            drainWalQueue();

            // Insert rows with actual DOUBLE[][] data (timestamps after existing max)
            sb = new StringBuilder();
            sb.append("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 100; i < 150; i++) {
                if (i > 100) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000Z', ").append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Verify baseline
            String expected100 = "ts\tx\ta\n";
            StringBuilder expectedSb = new StringBuilder(expected100);
            for (int i = 0; i < 100; i++) {
                expectedSb.append("2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000000Z\t").append(i).append("\tnull\n");
            }
            for (int i = 100; i < 150; i++) {
                expectedSb.append("2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000000Z\t").append(i)
                        .append("\t[[null,null,null],[null,null,null],[null,null,null]]\n");
            }
            assertSql(expectedSb.toString(), "SELECT * FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02' ORDER BY ts");

            // Step 4: Convert to parquet (column_top=100 for arr)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertSql(expectedSb.toString(), "SELECT * FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02' ORDER BY ts");

            // Step 5: Convert back to native
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();
            assertSql(expectedSb.toString(), "SELECT * FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02' ORDER BY ts");

            // Step 6: Insert more rows with array data
            sb = new StringBuilder();
            sb.append("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 150; i < 200; i++) {
                if (i > 150) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000Z', ").append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Update expected
            for (int i = 150; i < 200; i++) {
                expectedSb.append("2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000000Z\t").append(i)
                        .append("\t[[null,null,null],[null,null,null],[null,null,null]]\n");
            }
            assertSql(expectedSb.toString(), "SELECT * FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02' ORDER BY ts");

            // Step 7: Convert to parquet (column_top = 0 now)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertSql(expectedSb.toString(), "SELECT * FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02' ORDER BY ts");

            // Step 8: O3 insert into the parquet partition
            // Insert rows with timestamps that fall BEFORE the existing max,
            // triggering O3 merge with the parquet partition.
            execute(
                    """
                            INSERT INTO x(ts, x, a) VALUES
                            ('2020-01-01T00:00:30.000Z', 1000, ARRAY[[42.0, 43.0]]),
                            ('2020-01-01T01:30:30.000Z', 1001, ARRAY[[44.0]]),
                            ('2020-01-01T02:15:30.000Z', 1002, ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])
                            """
            );
            drainWalQueue();

            // Step 9: Verify all data
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            // Spot-check specific array values that should NOT be null
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T01:40:00.000000Z\t100\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 100"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T02:29:00.000000Z\t149\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 149"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T02:30:00.000000Z\t150\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 150"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T02:15:30.000000Z\t1002\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 1002"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:00:30.000000Z\t1000\t[[42.0,43.0]]
                            """,
                    "SELECT * FROM x WHERE x = 1000"
            );
        });
    }

    @Test
    public void testO3IntoParquetAfterColumnTopMultipleO3Merges() throws Exception {
        // Closer to the actual fuzz failure: exercises multiple O3 merges into
        // parquet after ADD COLUMN with column_top and parquet↔native cycles.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );

            // Insert rows with timestamps every second (large enough to have
            // substantial column_top when column is added)
            StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x) VALUES\n");
            for (int i = 0; i < 500; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                // Timestamps every 2 seconds to leave gaps for O3
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2) / 60, (i * 2) % 60))
                        .append(".000Z', ").append(i).append(')');
            }
            execute(sb.toString());

            // Row in next partition
            execute("INSERT INTO x(ts, x) VALUES ('2020-01-02T00:00:00.000Z', -1)");
            drainWalQueue();

            // Convert to parquet, then back to native (cycle 1)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            // Append more rows
            sb = new StringBuilder("INSERT INTO x(ts, x) VALUES\n");
            for (int i = 500; i < 700; i++) {
                if (i > 500) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2) / 60, (i * 2) % 60))
                        .append(".000Z', ").append(i).append(')');
            }
            execute(sb.toString());
            drainWalQueue();

            // ADD COLUMN with column_top = 700
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            drainWalQueue();

            // Insert rows with actual DOUBLE[][] data
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 700; i < 900; i++) {
                if (i > 700) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2) / 60, (i * 2) % 60))
                        .append(".000Z', ").append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Convert to parquet (column_top = 700)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Convert back to native
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            // Append more rows
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 900; i < 1100; i++) {
                if (i > 900) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2) / 60, (i * 2) % 60))
                        .append(".000Z', ").append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Convert to parquet again (column_top = 0)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // O3 insert #1: overlaps with existing data
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 0; i < 200; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                // Odd seconds to interleave with even seconds
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2 + 1) / 60, (i * 2 + 1) % 60))
                        .append(".000Z', ").append(10_000 + i)
                        .append(", ARRAY[[").append(i).append("]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // O3 insert #2: more interleaving
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 200; i < 500; i++) {
                if (i > 200) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2 + 1) / 60, (i * 2 + 1) % 60))
                        .append(".000Z', ").append(10_000 + i)
                        .append(", ARRAY[[").append(i).append("]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Verify
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            // Check that array values are correct for rows after column_top
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:23:20.000000Z\t700\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 700"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:29:58.000000Z\t899\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 899"
            );
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:30:00.000000Z\t900\t[[null,null,null],[null,null,null],[null,null,null]]
                            """,
                    "SELECT * FROM x WHERE x = 900"
            );
            // Rows before column_top should be null
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:00:00.000000Z\t0\tnull
                            """,
                    "SELECT * FROM x WHERE x = 0"
            );
            // O3 rows should have their values
            assertSql(
                    """
                            ts\tx\ta
                            2020-01-01T00:00:01.000000Z\t10000\t[[0.0]]
                            """,
                    "SELECT * FROM x WHERE x = 10000"
            );
        });
    }

    @Test
    public void testO3IntoParquetWithMultiDimArrayNulls() throws Exception {
        // Reproduces a bug where DOUBLE[][] values consisting of arrays of null
        // elements (e.g., [[null,null,null],...]) collapse to plain NULL after
        // an O3 merge into a parquet partition via the Rust updater.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts, x) VALUES
                            ('2020-01-01T00:00:00.000Z', 1),
                            ('2020-01-01T01:00:00.000Z', 2),
                            ('2020-01-01T02:00:00.000Z', 3),
                            ('2020-01-01T03:00:00.000Z', 4)
                            """
            );
            drainWalQueue();

            // Add a DOUBLE[][] column — existing rows get column_top = 4
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            drainWalQueue();

            // Insert rows with DOUBLE[][] values that contain null elements
            execute(
                    """
                            INSERT INTO x(ts, x, a) VALUES
                            ('2020-01-01T04:00:00.000Z', 5, ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]]),
                            ('2020-01-01T05:00:00.000Z', 6, ARRAY[[1.5, null::double], [null::double, 2.5]]),
                            ('2020-01-01T06:00:00.000Z', 7, ARRAY[[null::double]])
                            """
            );
            drainWalQueue();

            final String expected = """
                    ts\tx\ta
                    2020-01-01T00:00:00.000000Z\t1\tnull
                    2020-01-01T01:00:00.000000Z\t2\tnull
                    2020-01-01T02:00:00.000000Z\t3\tnull
                    2020-01-01T03:00:00.000000Z\t4\tnull
                    2020-01-01T04:00:00.000000Z\t5\t[[null,null,null],[null,null,null],[null,null,null]]
                    2020-01-01T05:00:00.000000Z\t6\t[[1.5,null],[null,2.5]]
                    2020-01-01T06:00:00.000000Z\t7\t[[null]]
                    """;

            assertSql(expected, "SELECT * FROM x");

            // Convert to parquet
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertSql(expected, "SELECT * FROM x");

            // O3 insert into the parquet partition → triggers Rust O3 updater
            execute(
                    """
                            INSERT INTO x(ts, x, a) VALUES
                            ('2020-01-01T02:30:00.000Z', 10, ARRAY[[null::double, null::double, null::double]])
                            """
            );
            drainWalQueue();

            final String expectedAfterO3 = """
                    ts\tx\ta
                    2020-01-01T00:00:00.000000Z\t1\tnull
                    2020-01-01T01:00:00.000000Z\t2\tnull
                    2020-01-01T02:00:00.000000Z\t3\tnull
                    2020-01-01T02:30:00.000000Z\t10\t[[null,null,null]]
                    2020-01-01T03:00:00.000000Z\t4\tnull
                    2020-01-01T04:00:00.000000Z\t5\t[[null,null,null],[null,null,null],[null,null,null]]
                    2020-01-01T05:00:00.000000Z\t6\t[[1.5,null],[null,2.5]]
                    2020-01-01T06:00:00.000000Z\t7\t[[null]]
                    """;

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertSql(expectedAfterO3, "SELECT * FROM x");
        });
    }

    @Test
    public void testO3MergeAfterDropAllNonTimestampColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (a INT, b DOUBLE, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(a, b, ts) VALUES
                            (1, 1.1, '2020-01-01T00:00:00.000Z'),
                            (2, 2.2, '2020-01-01T06:00:00.000Z'),
                            (3, 3.3, '2020-01-01T12:00:00.000Z'),
                            (4, 4.4, '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(a, b, ts) VALUES (100, 99.9, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Drop all non-timestamp columns.
            execute("ALTER TABLE x DROP COLUMN a");
            execute("ALTER TABLE x DROP COLUMN b");
            drainWalQueue();

            // O3 insert into the parquet partition — only the timestamp column remains.
            execute("INSERT INTO x(ts) VALUES ('2020-01-01T03:00:00.000Z')");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            ts
                            2020-01-01T00:00:00.000000Z
                            2020-01-01T03:00:00.000000Z
                            2020-01-01T06:00:00.000000Z
                            2020-01-01T12:00:00.000000Z
                            2020-01-01T18:00:00.000000Z
                            2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testO3MergeAfterDropColumnWrongTimestampIndex() throws Exception {
        // Two rounds of O3 merge exercise the wrong-timestamp-index bug.
        // Round 1: DROP COLUMN 'a' (before ts) + ADD COLUMN 'b' causes
        //   schema change → rewrite. The rewritten parquet file has compacted
        //   columns: x(0), ts(1), b(2). Writer metadata keeps timestampIndex=2
        //   (stable), but ts is now at parquet position 1.
        // Round 2: second O3 merge reads the rewritten file.
        //   timestampIndex=2, timestampParquetIdx=1 — they differ.
        //   Reading row group stats with timestampIndex fetches stats for
        //   parquet column 2 (column 'b', not the timestamp), producing
        //   incorrect row group bounds and a type mismatch error.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (a INT, x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(a, x, ts) VALUES
                            (1, 10, '2020-01-01T00:00:00.000Z'),
                            (2, 20, '2020-01-01T01:00:00.000Z'),
                            (3, 30, '2020-01-01T02:00:00.000Z'),
                            (4, 40, '2020-01-01T03:00:00.000Z'),
                            (5, 50, '2020-01-01T04:00:00.000Z'),
                            (6, 60, '2020-01-01T05:00:00.000Z'),
                            (7, 70, '2020-01-01T06:00:00.000Z'),
                            (8, 80, '2020-01-01T07:00:00.000Z'),
                            (9, 90, '2020-01-01T08:00:00.000Z'),
                            (10, 100, '2020-01-01T09:00:00.000Z'),
                            (11, 110, '2020-01-01T10:00:00.000Z'),
                            (12, 120, '2020-01-01T11:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(a, x, ts) VALUES (1000, 9999, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // Parquet file: a(0), x(1), ts(2). timestampIndex=2, timestampParquetIdx=2.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Drop column 'a' (timestampIndex stays 2) and add column 'b' so
            // the rewritten parquet file has ≥3 columns and timestampIndex=2
            // points at 'b' instead of going out of bounds.
            execute("ALTER TABLE x DROP COLUMN a");
            execute("ALTER TABLE x ADD COLUMN b LONG");
            drainWalQueue();

            // Round 1: O3 insert triggers merge with schema change → rewrite.
            // Rewritten parquet: x(0), ts(1), b(2). timestampParquetIdx=1.
            execute(
                    """
                            INSERT INTO x(x, b, ts) VALUES
                            (100, 1, '2020-01-01T04:30:00.000Z'),
                            (101, 2, '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(
                    "table suspended after round 1",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            // Round 2: O3 insert against the rewritten parquet file.
            // timestampIndex=2, timestampParquetIdx=1 — they now differ.
            // With the bug, row group stat lookup reads parquet column 2
            // ('b', LONG) with type TIMESTAMP → type mismatch → table suspended.
            execute(
                    """
                            INSERT INTO x(x, b, ts) VALUES
                            (200, 3, '2020-01-01T06:30:00.000Z'),
                            (201, 4, '2020-01-01T07:30:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(
                    "table suspended after round 2",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertSql(
                    """
                            x\tts\tb
                            10\t2020-01-01T00:00:00.000000Z\tnull
                            20\t2020-01-01T01:00:00.000000Z\tnull
                            30\t2020-01-01T02:00:00.000000Z\tnull
                            40\t2020-01-01T03:00:00.000000Z\tnull
                            50\t2020-01-01T04:00:00.000000Z\tnull
                            100\t2020-01-01T04:30:00.000000Z\t1
                            60\t2020-01-01T05:00:00.000000Z\tnull
                            101\t2020-01-01T05:30:00.000000Z\t2
                            70\t2020-01-01T06:00:00.000000Z\tnull
                            200\t2020-01-01T06:30:00.000000Z\t3
                            80\t2020-01-01T07:00:00.000000Z\tnull
                            201\t2020-01-01T07:30:00.000000Z\t4
                            90\t2020-01-01T08:00:00.000000Z\tnull
                            100\t2020-01-01T09:00:00.000000Z\tnull
                            110\t2020-01-01T10:00:00.000000Z\tnull
                            120\t2020-01-01T11:00:00.000000Z\tnull
                            9999\t2020-01-02T00:00:00.000000Z\tnull
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testO3MergeWithStatisticsDisabled() throws Exception {
        // Disable parquet statistics and use small row groups to produce multiple row groups.
        // O3 merge reads row group min/max timestamps from _pm; when parquet
        // statistics are absent the stats are missing from _pm and the merge
        // must still produce correct results without crashing or corrupting data.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_STATISTICS_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z'),
                            (9, '2020-01-01T08:00:00.000Z'),
                            (10, '2020-01-01T09:00:00.000Z'),
                            (11, '2020-01-01T10:00:00.000Z'),
                            (12, '2020-01-01T11:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (1000, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 12 rows with row group size 4 → 3 row groups, no statistics.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // O3 insert into the middle row group's time range.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (100, '2020-01-01T04:30:00.000Z'),
                            (101, '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            // Table must not be suspended.
            Assert.assertFalse(
                    "table should not be suspended after O3 merge with statistics disabled",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertSql(
                    """
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            4\t2020-01-01T03:00:00.000000Z
                            5\t2020-01-01T04:00:00.000000Z
                            100\t2020-01-01T04:30:00.000000Z
                            6\t2020-01-01T05:00:00.000000Z
                            101\t2020-01-01T05:30:00.000000Z
                            7\t2020-01-01T06:00:00.000000Z
                            8\t2020-01-01T07:00:00.000000Z
                            9\t2020-01-01T08:00:00.000000Z
                            10\t2020-01-01T09:00:00.000000Z
                            11\t2020-01-01T10:00:00.000000Z
                            12\t2020-01-01T11:00:00.000000Z
                            1000\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testO3MergeWithVarSizeColumnTop() throws Exception {
        // Small row group size so the column-top covers entire row groups.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            // Insert 8 rows without the STRING column.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // Add STRING column. column_top = 8 for this partition.
            execute("ALTER TABLE x ADD COLUMN s STRING");
            drainWalQueue();

            // Insert 4 more rows with s values.
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (9, 'abc', '2020-01-01T08:00:00.000Z'),
                            (10, 'def', '2020-01-01T09:00:00.000Z'),
                            (11, 'ghi', '2020-01-01T10:00:00.000Z'),
                            (12, 'jkl', '2020-01-01T11:00:00.000Z')
                            """
            );
            drainWalQueue();

            // 12 rows with row group size 4 → 3 row groups.
            // RG0 (rows 0-3): s is entirely topped (column_top=8 ≥ 4)
            // RG1 (rows 4-7): s is entirely topped (column_top=8 ≥ 8)
            // RG2 (rows 8-11): s has actual data
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // O3 insert targeting RG0 (timestamp range 00:00-03:00).
            // The merge decodes RG0 where s has column_top → null aux_ptr.
            // Exercises the null source buffer allocation for var-size columns
            // (STRING null markers need a non-zero data buffer).
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (13, 'new', '2020-01-01T00:30:00.000Z')
                            """
            );
            drainWalQueue();

            assertSql(
                    """
                            x\tts\ts
                            1\t2020-01-01T00:00:00.000000Z\t
                            13\t2020-01-01T00:30:00.000000Z\tnew
                            2\t2020-01-01T01:00:00.000000Z\t
                            3\t2020-01-01T02:00:00.000000Z\t
                            4\t2020-01-01T03:00:00.000000Z\t
                            5\t2020-01-01T04:00:00.000000Z\t
                            6\t2020-01-01T05:00:00.000000Z\t
                            7\t2020-01-01T06:00:00.000000Z\t
                            8\t2020-01-01T07:00:00.000000Z\t
                            9\t2020-01-01T08:00:00.000000Z\tabc
                            10\t2020-01-01T09:00:00.000000Z\tdef
                            11\t2020-01-01T10:00:00.000000Z\tghi
                            12\t2020-01-01T11:00:00.000000Z\tjkl
                            100\t2020-01-02T00:00:00.000000Z\t
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testParquetToNativePreservesColumnTopZeroing() throws Exception {
        // Reproduces a bug where convertPartitionParquetToNative materializes
        // ALL rows (including column_top nulls) but does not zero the
        // column_tops in ColumnVersionWriter. A subsequent native→parquet
        // conversion reads the stale column_top, causing data to shift by
        // column_top positions and corrupting array values.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );

            // Insert 4 rows into partition 2020-01-01
            execute(
                    """
                            INSERT INTO x(ts, x) VALUES
                            ('2020-01-01T00:00:00.000Z', 1),
                            ('2020-01-01T01:00:00.000Z', 2),
                            ('2020-01-01T02:00:00.000Z', 3),
                            ('2020-01-01T03:00:00.000Z', 4)
                            """
            );
            drainWalQueue();

            // Add a DOUBLE[][] column → column_top = 4 for the existing rows
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            drainWalQueue();

            // Insert 4 more rows with actual DOUBLE[][] data
            execute(
                    """
                            INSERT INTO x(ts, x, a) VALUES
                            ('2020-01-01T04:00:00.000Z', 5, ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]]),
                            ('2020-01-01T05:00:00.000Z', 6, ARRAY[[1.5, null::double], [null::double, 2.5]]),
                            ('2020-01-01T06:00:00.000Z', 7, ARRAY[[null::double]]),
                            ('2020-01-01T07:00:00.000Z', 8, ARRAY[[3.0, 4.0]])
                            """
            );
            drainWalQueue();

            final String expected = """
                    ts\tx\ta
                    2020-01-01T00:00:00.000000Z\t1\tnull
                    2020-01-01T01:00:00.000000Z\t2\tnull
                    2020-01-01T02:00:00.000000Z\t3\tnull
                    2020-01-01T03:00:00.000000Z\t4\tnull
                    2020-01-01T04:00:00.000000Z\t5\t[[null,null,null],[null,null,null],[null,null,null]]
                    2020-01-01T05:00:00.000000Z\t6\t[[1.5,null],[null,2.5]]
                    2020-01-01T06:00:00.000000Z\t7\t[[null]]
                    2020-01-01T07:00:00.000000Z\t8\t[[3.0,4.0]]
                    """;

            assertSql(expected, "SELECT * FROM x");

            // Convert to parquet (encodes with column_top=4)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertSql(expected, "SELECT * FROM x");

            // Convert back to native — decoder materializes ALL rows.
            // Without the fix, column_top stays at 4 in ColumnVersionWriter.
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();
            assertSql(expected, "SELECT * FROM x");

            // Convert to parquet again — with the stale column_top, the
            // encoder would read from offset 0 in the native file but skip 4
            // rows, shifting DOUBLE[][] data by 4 positions.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertSql(expected, "SELECT * FROM x");
        });
    }

    @Test
    public void testParquetToNativeWithColumnTops() throws Exception {
        // Tests that converting parquet→native correctly zeroes column tops.
        // The parquet decoder materializes NULLs for column-top rows, so the
        // native files contain ALL rows. Without zeroing, the reader offsets
        // into the file incorrectly, producing wrong data.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, v VARCHAR, s SYMBOL)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts, v, s) VALUES
                            ('2020-01-01T00:00:00.000Z', 'foo', 'a'),
                            ('2020-01-01T01:00:00.000Z', 'bar', 'b'),
                            ('2020-01-01T02:00:00.000Z', 'baz', 'a'),
                            ('2020-01-01T03:00:00.000Z', 'qux', 'c')
                            """
            );
            drainWalQueue();

            // Add columns AFTER data exists, creating non-zero column tops
            execute("ALTER TABLE x ADD COLUMN n INT");
            execute("ALTER TABLE x ADD COLUMN v2 VARCHAR");
            execute("ALTER TABLE x ADD COLUMN s2 SYMBOL");
            drainWalQueue();

            // Insert more rows so the new columns have some non-null data
            execute(
                    """
                            INSERT INTO x(ts, v, s, n, v2, s2) VALUES
                            ('2020-01-01T04:00:00.000Z', 'aaa', 'd', 42, 'hello', 'x'),
                            ('2020-01-01T05:00:00.000Z', 'bbb', 'e', 99, 'world', 'y')
                            """
            );
            drainWalQueue();

            final String expected = """
                    ts\tv\ts\tn\tv2\ts2
                    2020-01-01T00:00:00.000000Z\tfoo\ta\tnull\t\t
                    2020-01-01T01:00:00.000000Z\tbar\tb\tnull\t\t
                    2020-01-01T02:00:00.000000Z\tbaz\ta\tnull\t\t
                    2020-01-01T03:00:00.000000Z\tqux\tc\tnull\t\t
                    2020-01-01T04:00:00.000000Z\taaa\td\t42\thello\tx
                    2020-01-01T05:00:00.000000Z\tbbb\te\t99\tworld\ty
                    """;

            // Verify data before conversion
            assertSql(expected, "SELECT * FROM x");

            // Convert to parquet
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Verify data in parquet format
            assertSql(expected, "SELECT * FROM x");

            // Convert back to native
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            // Verify data after converting back to native
            assertSql(expected, "SELECT * FROM x");
        });
    }

    @Test
    public void testParquetToNativeWithDeletedColumns() throws Exception {
        // Tests that converting parquet→native correctly maps columns by writer ID
        // when columns have been deleted, causing gaps in the table column indices.
        assertMemoryLeak(() -> {
            // Create table with several column types including VARCHAR
            execute(
                    """
                            CREATE TABLE x (x INT, v VARCHAR, s SYMBOL, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, v, s, ts) VALUES
                            (1, 'foo', 'a', '2020-01-01T00:00:00.000Z'),
                            (2, 'bar', 'b', '2020-01-01T01:00:00.000Z'),
                            (3, 'baz', 'a', '2020-01-01T02:00:00.000Z'),
                            (4, 'qux', 'c', '2020-01-01T03:00:00.000Z')
                            """
            );
            drainWalQueue();

            // Case 1: remove a column BEFORE converting to parquet.
            // The parquet file won't contain 'x' and its sequential column indices
            // will differ from the table column indices.
            execute("ALTER TABLE x DROP COLUMN x");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Convert back to native — this is the operation that was failing
            // because the conversion used parquet sequential indices instead of
            // table column IDs for file naming.
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            v\ts\tts
                            foo\ta\t2020-01-01T00:00:00.000000Z
                            bar\tb\t2020-01-01T01:00:00.000000Z
                            baz\ta\t2020-01-01T02:00:00.000000Z
                            qux\tc\t2020-01-01T03:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );

            // Case 2: convert to parquet again, then remove a column AFTER
            // conversion, then convert back. The parquet contains the column
            // but the table metadata marks it deleted, shifting sequential indices.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x DROP COLUMN s");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            v\tts
                            foo\t2020-01-01T00:00:00.000000Z
                            bar\t2020-01-01T01:00:00.000000Z
                            baz\t2020-01-01T02:00:00.000000Z
                            qux\t2020-01-01T03:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );

            // Case 3: convert to parquet, then rename a VARCHAR column, then convert back.
            // The parquet stores the old column name but openPartition expects the new name.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x RENAME COLUMN v TO v_renamed");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertSql(
                    """
                            v_renamed\tts
                            foo\t2020-01-01T00:00:00.000000Z
                            bar\t2020-01-01T01:00:00.000000Z
                            baz\t2020-01-01T02:00:00.000000Z
                            qux\t2020-01-01T03:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testReadParquetAfterAddColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T06:00:00.000Z'),
                            (3, '2020-01-01T12:00:00.000Z'),
                            (4, '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01', '2020-01-02'");
            drainWalQueue();

            // Add column after conversion. No O3 -- just query.
            execute("ALTER TABLE x ADD COLUMN y DOUBLE");
            drainWalQueue();

            // Insert with the new column into the non-parquet partition.
            execute("INSERT INTO x(x, y, ts) VALUES (5, 1.5, '2020-01-02T06:00:00.000Z')");
            drainWalQueue();

            // Parquet partition returns NULLs for the missing column.
            assertSql(
                    """
                            x\tts\ty
                            1\t2020-01-01T00:00:00.000000Z\tnull
                            2\t2020-01-01T06:00:00.000000Z\tnull
                            3\t2020-01-01T12:00:00.000000Z\tnull
                            4\t2020-01-01T18:00:00.000000Z\tnull
                            100\t2020-01-02T00:00:00.000000Z\tnull
                            5\t2020-01-02T06:00:00.000000Z\t1.5
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testRewriteAbsoluteUnusedBytesThreshold() throws Exception {
        // Use small row group size to get multiple row groups.
        // Disable ratio check (set to 1.0 = impossible to exceed).
        // Set absolute threshold very low so the second O3 triggers a rewrite.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 100);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, s SYMBOL, v VARCHAR, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (1, 'a', 'foo', '2020-01-01T00:00:00.000Z'),
                            (2, 'b', 'bar', '2020-01-01T01:00:00.000Z'),
                            (3, 'a', 'baz', '2020-01-01T02:00:00.000Z'),
                            (4, 'c', 'qux', '2020-01-01T03:00:00.000Z'),
                            (5, 'b', 'abc', '2020-01-01T04:00:00.000Z'),
                            (6, 'a', 'def', '2020-01-01T05:00:00.000Z'),
                            (7, 'c', 'ghi', '2020-01-01T06:00:00.000Z'),
                            (8, 'b', 'jkl', '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, s, v, ts) VALUES (100, 'd', 'end', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 → 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // First O3: unused_bytes = 0, multiple row groups → UPDATE mode.
            // Replaces one row group, accumulating dead space.
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (9, 'b', 'mno', '2020-01-01T00:30:00.000Z'),
                            (10, 'c', 'pqr', '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            long partitionTs = parseFloorPartialTimestamp("2020-01-01");
            long versionAfterUpdate = getPartitionNameTxn(partitionTs);

            // Second O3: accumulated unused_bytes > 100 → REWRITE.
            // Rewrite copies unchanged row groups (exercises copy_row_group
            // with dictionary pages from SYMBOL columns).
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (11, 'a', 'stu', '2020-01-01T04:30:00.000Z'),
                            (12, 'c', 'vwx', '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            long versionAfterRewrite = getPartitionNameTxn(partitionTs);
            Assert.assertNotEquals("partition version should change on REWRITE", versionAfterUpdate, versionAfterRewrite);

            assertSql(
                    """
                            x\ts\tv\tts
                            1\ta\tfoo\t2020-01-01T00:00:00.000000Z
                            9\tb\tmno\t2020-01-01T00:30:00.000000Z
                            2\tb\tbar\t2020-01-01T01:00:00.000000Z
                            10\tc\tpqr\t2020-01-01T01:30:00.000000Z
                            3\ta\tbaz\t2020-01-01T02:00:00.000000Z
                            4\tc\tqux\t2020-01-01T03:00:00.000000Z
                            5\tb\tabc\t2020-01-01T04:00:00.000000Z
                            11\ta\tstu\t2020-01-01T04:30:00.000000Z
                            6\ta\tdef\t2020-01-01T05:00:00.000000Z
                            12\tc\tvwx\t2020-01-01T05:30:00.000000Z
                            7\tc\tghi\t2020-01-01T06:00:00.000000Z
                            8\tb\tjkl\t2020-01-01T07:00:00.000000Z
                            100\td\tend\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testRewriteCleanupOnCopyRowGroupFailure() throws Exception {
        // Test that the inner catch cleans up the new directory when copyRowGroup()
        // fails mid-rewrite, leaving the original partition intact.
        //
        // Strategy: override openRW to return a read-only fd for the new data.parquet
        // in rewrite mode. ParquetUpdater.of() succeeds (no writes in the constructor
        // for rewrite mode), but the first write attempt in copyRowGroup() fails with
        // EBADF, exercising the error recovery path.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 100);

        AtomicBoolean armed = new AtomicBoolean(false);

        FilesFacade dodgyFacade = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (armed.get() && Utf8s.endsWithAscii(name, "data.parquet")) {
                    // Create the file normally, then close it and re-open as
                    // read-only. The Rust writer gets an O_RDONLY fd: init
                    // succeeds (no writes), but copyRowGroup's first write
                    // fails with EBADF.
                    long rwFd = super.openRW(name, opts);
                    super.close(rwFd);
                    return super.openRO(name);
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(dodgyFacade, () -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 → 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // First O3: UPDATE mode. Merges into rg0, accumulating dead space.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (9, '2020-01-01T00:30:00.000Z'),
                            (10, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            TableToken tableToken = engine.verifyTableName("x");
            File tableDir = new File(root, tableToken.getDirName());

            // Arm the failure injection before the rewrite-triggering O3.
            armed.set(true);

            // Second O3: accumulated unused_bytes > 100 → REWRITE mode.
            // O3 data hits rg1, so the first merge action is
            // COPY_ROW_GROUP_SLICE for unmodified rg0 → copyRowGroup() fails.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (11, '2020-01-01T04:30:00.000Z'),
                            (12, '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            // Table should be suspended due to O3 error.
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));

            // The inner catch should have cleaned up the new directory.
            int partDirsAfterFailure = countPartitionDirs(tableDir);
            Assert.assertEquals(
                    "orphaned rewrite directory left on disk",
                    1,
                    partDirsAfterFailure
            );

            // Disarm, resume, and retry.
            armed.set(false);
            execute("ALTER TABLE x RESUME WAL");
            drainWalQueue();

            // After successful retry, data should be correct.
            assertSql(
                    """
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            9\t2020-01-01T00:30:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            10\t2020-01-01T01:30:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            4\t2020-01-01T03:00:00.000000Z
                            5\t2020-01-01T04:00:00.000000Z
                            11\t2020-01-01T04:30:00.000000Z
                            6\t2020-01-01T05:00:00.000000Z
                            12\t2020-01-01T05:30:00.000000Z
                            7\t2020-01-01T06:00:00.000000Z
                            8\t2020-01-01T07:00:00.000000Z
                            100\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testRewriteCleanupOnUpdateParquetIndexesFailure() throws Exception {
        // Regression test for orphaned directory when updateParquetIndexes() fails
        // in rewrite mode.
        AtomicBoolean armed = new AtomicBoolean(false);
        FilesFacade dodgyFacade = getFilesFacade(armed);

        assertMemoryLeak(dodgyFacade, () -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T06:00:00.000Z'),
                            (3, '2020-01-01T12:00:00.000Z'),
                            (4, '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // Single row group → rewrite guaranteed on O3.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Count 2020-01-01.* partition directories before O3.
            TableToken tableToken = engine.verifyTableName("x");
            File tableDir = new File(root, tableToken.getDirName());

            // Arm the failure injection.
            armed.set(true);

            // O3 insert triggers rewrite. The rewrite itself succeeds but
            // updateParquetIndexes fails because the 2nd openRO returns -1.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (5, '2020-01-01T03:00:00.000Z')
                            """
            );
            drainWalQueue();

            // Table should be suspended due to O3 error.
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));

            int partDirsAfterFailure = countPartitionDirs(tableDir);
            Assert.assertEquals(
                    "orphaned rewrite directory left on disk",
                    1,
                    partDirsAfterFailure
            );

            // Disarm, resume, and retry.
            armed.set(false);
            execute("ALTER TABLE x RESUME WAL");
            drainWalQueue();

            // After successful retry, data should be correct.
            assertSql(
                    """
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            5\t2020-01-01T03:00:00.000000Z
                            2\t2020-01-01T06:00:00.000000Z
                            3\t2020-01-01T12:00:00.000000Z
                            4\t2020-01-01T18:00:00.000000Z
                            100\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testRewriteResetsUnusedBytesToZero() throws Exception {
        // Use small row group size to get multiple row groups.
        // Set absolute threshold low so the second O3 triggers a rewrite.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 100);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 -> 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // First O3: UPDATE mode. Replaces one row group, accumulating dead space.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (9, '2020-01-01T00:30:00.000Z'),
                            (10, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            // Verify unused_bytes > 0 after in-place update.
            long unusedAfterUpdate;
            try (TableReader reader = getReader("x")) {
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                        continue;
                    }
                    reader.openPartition(i);
                    try (ParquetFileDecoder footerDecoder = new ParquetFileDecoder()) {
                        footerDecoder.of(reader.getParquetAddr(i), reader.getParquetFileSize(i), MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                        unusedAfterUpdate = footerDecoder.metadata().getUnusedBytes();
                    }
                    Assert.assertTrue("unused_bytes should be > 0 after update, got " + unusedAfterUpdate, unusedAfterUpdate > 0);
                }
            }

            // Second O3: accumulated unused_bytes > 100 -> REWRITE.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (11, '2020-01-01T04:30:00.000Z'),
                            (12, '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            // Verify unused_bytes == 0 after rewrite.
            try (TableReader reader = getReader("x")) {
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                        continue;
                    }
                    reader.openPartition(i);
                    try (ParquetFileDecoder footerDecoder = new ParquetFileDecoder()) {
                        footerDecoder.of(reader.getParquetAddr(i), reader.getParquetFileSize(i), MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                        long unusedAfterRewrite = footerDecoder.metadata().getUnusedBytes();
                        Assert.assertEquals("unused_bytes should be 0 after rewrite", 0, unusedAfterRewrite);
                    }
                }
            }

            assertSql(
                    """
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            9\t2020-01-01T00:30:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            10\t2020-01-01T01:30:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            4\t2020-01-01T03:00:00.000000Z
                            5\t2020-01-01T04:00:00.000000Z
                            11\t2020-01-01T04:30:00.000000Z
                            6\t2020-01-01T05:00:00.000000Z
                            12\t2020-01-01T05:30:00.000000Z
                            7\t2020-01-01T06:00:00.000000Z
                            8\t2020-01-01T07:00:00.000000Z
                            100\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testRewriteSingleRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, s SYMBOL, v VARCHAR, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (1, 'a', 'foo', '2020-01-01T00:00:00.000Z'),
                            (2, 'b', 'bar', '2020-01-01T06:00:00.000Z'),
                            (3, 'a', 'baz', '2020-01-01T12:00:00.000Z'),
                            (4, 'c', 'qux', '2020-01-01T18:00:00.000Z')
                            """
            );
            // Insert into the next day so 2020-01-01 is not the last partition.
            execute("INSERT INTO x(x, s, v, ts) VALUES (100, 'd', 'end', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 4 rows with default test row group size (1000) → single row group.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            long partitionTs = parseFloorPartialTimestamp("2020-01-01");
            long versionBeforeO3 = getPartitionNameTxn(partitionTs);

            // O3 insert into the parquet partition.
            // Single row group always triggers a full REWRITE.
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (5, 'b', 'abc', '2020-01-01T03:00:00.000Z'),
                            (6, 'a', 'def', '2020-01-01T09:00:00.000Z'),
                            (7, 'c', 'ghi', '2020-01-01T15:00:00.000Z')
                            """
            );
            drainWalQueue();

            long versionAfterRewrite = getPartitionNameTxn(partitionTs);
            Assert.assertNotEquals("partition version should change on REWRITE", versionBeforeO3, versionAfterRewrite);

            assertSql(
                    """
                            x\ts\tv\tts
                            1\ta\tfoo\t2020-01-01T00:00:00.000000Z
                            5\tb\tabc\t2020-01-01T03:00:00.000000Z
                            2\tb\tbar\t2020-01-01T06:00:00.000000Z
                            6\ta\tdef\t2020-01-01T09:00:00.000000Z
                            3\ta\tbaz\t2020-01-01T12:00:00.000000Z
                            7\tc\tghi\t2020-01-01T15:00:00.000000Z
                            4\tc\tqux\t2020-01-01T18:00:00.000000Z
                            100\td\tend\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testRewriteUnusedBytesRatioThreshold() throws Exception {
        // Use small row group size to get multiple row groups.
        // Set ratio to 10% to trigger rewrite after one update round.
        // Disable absolute bytes threshold.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "0.1");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, s SYMBOL, v VARCHAR, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (1, 'a', 'foo', '2020-01-01T00:00:00.000Z'),
                            (2, 'b', 'bar', '2020-01-01T01:00:00.000Z'),
                            (3, 'a', 'baz', '2020-01-01T02:00:00.000Z'),
                            (4, 'c', 'qux', '2020-01-01T03:00:00.000Z'),
                            (5, 'b', 'abc', '2020-01-01T04:00:00.000Z'),
                            (6, 'a', 'def', '2020-01-01T05:00:00.000Z'),
                            (7, 'c', 'ghi', '2020-01-01T06:00:00.000Z'),
                            (8, 'b', 'jkl', '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, s, v, ts) VALUES (100, 'd', 'end', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 → 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // First O3: unused_bytes = 0, multiple row groups → UPDATE mode.
            // Replaces one row group, accumulating dead space > 10% of file.
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (9, 'b', 'mno', '2020-01-01T00:30:00.000Z'),
                            (10, 'c', 'pqr', '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            long partitionTs = parseFloorPartialTimestamp("2020-01-01");
            long versionAfterUpdate = getPartitionNameTxn(partitionTs);

            // Second O3: unused_bytes / file_size > 0.1 → REWRITE.
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (11, 'a', 'stu', '2020-01-01T04:30:00.000Z'),
                            (12, 'c', 'vwx', '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            long versionAfterRewrite = getPartitionNameTxn(partitionTs);
            Assert.assertNotEquals("partition version should change on REWRITE", versionAfterUpdate, versionAfterRewrite);

            assertSql(
                    """
                            x\ts\tv\tts
                            1\ta\tfoo\t2020-01-01T00:00:00.000000Z
                            9\tb\tmno\t2020-01-01T00:30:00.000000Z
                            2\tb\tbar\t2020-01-01T01:00:00.000000Z
                            10\tc\tpqr\t2020-01-01T01:30:00.000000Z
                            3\ta\tbaz\t2020-01-01T02:00:00.000000Z
                            4\tc\tqux\t2020-01-01T03:00:00.000000Z
                            5\tb\tabc\t2020-01-01T04:00:00.000000Z
                            11\ta\tstu\t2020-01-01T04:30:00.000000Z
                            6\ta\tdef\t2020-01-01T05:00:00.000000Z
                            12\tc\tvwx\t2020-01-01T05:30:00.000000Z
                            7\tc\tghi\t2020-01-01T06:00:00.000000Z
                            8\tb\tjkl\t2020-01-01T07:00:00.000000Z
                            100\td\tend\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    @Test
    public void testRewriteWithColumnTop() throws Exception {
        // Regression test: after a rewrite-mode O3 merge on a partition with
        // column_top > 0, stale column_top values in the Parquet QDB metadata
        // caused the decoder to skip row groups that now contain actual data.
        // The fix zeroes column_top in the rewritten file so the decoder reads
        // the (null) pages instead of skipping them.
        //
        // Steps:
        // 1. Create table, insert rows, add a new column (column_top > 0).
        // 2. Insert more rows with the new column populated.
        // 3. Convert to Parquet (single row group → rewrite is guaranteed).
        // 4. O3 insert into the Parquet partition, triggering REWRITE.
        // 5. Read back all data — without the fix, the decoder would return
        //    wrong results for the new column in copied row group regions.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            // Insert 4 rows without the STRING column.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T06:00:00.000Z'),
                            (3, '2020-01-01T12:00:00.000Z'),
                            (4, '2020-01-01T18:00:00.000Z')
                            """
            );
            // Second partition so 2020-01-01 is not the last.
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // Add STRING column. column_top = 4 for the 2020-01-01 partition.
            execute("ALTER TABLE x ADD COLUMN s STRING");
            drainWalQueue();

            // Insert 2 rows with s values, still in the same partition.
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (5, 'hello', '2020-01-01T20:00:00.000Z'),
                            (6, 'world', '2020-01-01T22:00:00.000Z')
                            """
            );
            drainWalQueue();

            // 6 rows, default row group size (1000) → single row group.
            // column_top = 4: first 4 rows have s = NULL, last 2 have data.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            long partitionTs = parseFloorPartialTimestamp("2020-01-01");
            long versionBeforeO3 = getPartitionNameTxn(partitionTs);

            // O3 insert into the Parquet partition.
            // Single row group always triggers a full REWRITE.
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (7, 'mid', '2020-01-01T09:00:00.000Z')
                            """
            );
            drainWalQueue();

            long versionAfterRewrite = getPartitionNameTxn(partitionTs);
            Assert.assertNotEquals("partition version should change on REWRITE", versionBeforeO3, versionAfterRewrite);

            // Verify all data is correct. Without the column_top fix, the
            // decoder would see stale column_top and skip pages for 's',
            // returning incorrect NULL values for rows that have actual data.
            assertSql(
                    """
                            x\tts\ts
                            1\t2020-01-01T00:00:00.000000Z\t
                            2\t2020-01-01T06:00:00.000000Z\t
                            7\t2020-01-01T09:00:00.000000Z\tmid
                            3\t2020-01-01T12:00:00.000000Z\t
                            4\t2020-01-01T18:00:00.000000Z\t
                            5\t2020-01-01T20:00:00.000000Z\thello
                            6\t2020-01-01T22:00:00.000000Z\tworld
                            100\t2020-01-02T00:00:00.000000Z\t
                            """,
                    "SELECT * FROM x"
            );
        });
    }

    private static int countPartitionDirs(File tableDir) {
        String[] dirs = tableDir.list((dir, name) -> name.startsWith("2020-01-01"));
        return dirs != null ? dirs.length : 0;
    }

    private static @NotNull FilesFacade getFilesFacade(AtomicBoolean armed) {
        AtomicInteger openROCount = new AtomicInteger(0);

        return new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (armed.get() && Utf8s.endsWithAscii(name, "data.parquet")) {
                    // 1st openRO on data.parquet: reading original parquet (line 119) → succeed
                    // 2nd openRO on data.parquet: updateParquetIndexes (line 2949) → fail
                    if (openROCount.incrementAndGet() == 2) {
                        return -1;
                    }
                }
                return super.openRO(name);
            }
        };
    }

    private void assertPmAllNullChunkUsesZeroPointers(
            String tableName,
            String fixedColumnName,
            int fixedColumnType,
            String varColumnName,
            int varColumnType
    ) {
        try (TableReader reader = getReader(tableName)) {
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                    continue;
                }

                reader.openPartition(i);
                ParquetPartitionDecoder decoder = reader.getAndInitParquetMetaPartitionDecoder(i);
                try (
                        RowGroupBuffers rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                        DirectIntList parquetColumns = new DirectIntList(4, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER)
                ) {
                    final int fixedColumnIndex = decoder.metadata().getColumnIndex(fixedColumnName);
                    final int varColumnIndex = decoder.metadata().getColumnIndex(varColumnName);
                    Assert.assertTrue(fixedColumnName + " should exist in parquet metadata", fixedColumnIndex >= 0);
                    Assert.assertTrue(varColumnName + " should exist in parquet metadata", varColumnIndex >= 0);

                    parquetColumns.add(fixedColumnIndex);
                    parquetColumns.add(fixedColumnType);
                    parquetColumns.add(varColumnIndex);
                    parquetColumns.add(varColumnType);

                    final int rowGroupSize = (int) decoder.metadata().getRowGroupSize(0);
                    decoder.decodeRowGroup(rowGroupBuffers, parquetColumns, 0, 0, rowGroupSize);

                    Assert.assertEquals(0, rowGroupBuffers.getChunkDataPtr(0));
                    Assert.assertEquals(0, rowGroupBuffers.getChunkDataSize(0));
                    Assert.assertEquals(0, rowGroupBuffers.getChunkDataPtr(1));
                    Assert.assertEquals(0, rowGroupBuffers.getChunkDataSize(1));
                    Assert.assertEquals(0, rowGroupBuffers.getChunkAuxPtr(1));
                    Assert.assertEquals(0, rowGroupBuffers.getChunkAuxSize(1));
                }
                return;
            }
        }
        Assert.fail("should find parquet partition");
    }

    private long getPartitionNameTxn(long partitionTimestamp) {
        try (TableReader reader = getReader("x")) {
            return reader.getTxFile().getPartitionNameTxnByPartitionTimestamp(partitionTimestamp);
        }
    }
}
