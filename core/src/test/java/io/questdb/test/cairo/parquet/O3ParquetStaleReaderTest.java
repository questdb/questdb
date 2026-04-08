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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Regression test for stale-reader safety when `_pm` column tops change.
 *
 * <p>When a parquet partition's `_pm` header carries a non-zero column top
 * (e.g. a partition that pre-existed an `ALTER TABLE ADD COLUMN`), the next O3
 * merge needs to zero the effective top because the merged parquet output now
 * materializes the null prefix in page data. The `_pm` updater must express
 * that change append-only so that any concurrent {@link TableReader} holding
 * the earlier {@code parquetMetaFileSize} continues to resolve to a consistent
 * older snapshot.
 *
 * <p>The fix appends a new footer-scoped column-top override instead of
 * rewriting the existing `_pm` from offset 0. Older readers keep seeing the
 * header-based snapshot, while fresh readers pick up the new footer snapshot.
 */
public class O3ParquetStaleReaderTest extends AbstractCairoTest {

    @Test
    public void testStaleReaderSurvivesO3MergeWithColumnTops() throws Exception {
        // Force the in-place update path to be the default by making rewrite
        // thresholds permissive. The merge must stay append-only even though
        // effective column tops change across the snapshot boundary.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);

        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (a INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            // 12 rows over day 2020-01-01 → 3 row groups of 4 (rowGroupSize=4)
            // ensures the partition has rowGroupCount > 1, so the merge does
            // not fall through to the single-row-group rewrite branch.
            execute(
                    """
                            INSERT INTO x(a, ts) VALUES
                            (1,  '2020-01-01T00:00:00.000Z'),
                            (2,  '2020-01-01T01:00:00.000Z'),
                            (3,  '2020-01-01T02:00:00.000Z'),
                            (4,  '2020-01-01T03:00:00.000Z'),
                            (5,  '2020-01-01T04:00:00.000Z'),
                            (6,  '2020-01-01T05:00:00.000Z'),
                            (7,  '2020-01-01T06:00:00.000Z'),
                            (8,  '2020-01-01T07:00:00.000Z'),
                            (9,  '2020-01-01T08:00:00.000Z'),
                            (10, '2020-01-01T09:00:00.000Z'),
                            (11, '2020-01-01T10:00:00.000Z'),
                            (12, '2020-01-01T11:00:00.000Z')
                            """
            );
            // Insert into next day so 2020-01-01 is no longer the active
            // (last) partition, which is required for CONVERT TO PARQUET.
            execute("INSERT INTO x(a, ts) VALUES (99, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // ALTER TABLE ADD COLUMN updates the column-version writer with a
            // non-zero column top for the new column on every existing
            // partition. The next CONVERT TO PARQUET reads that column top
            // straight into the parquet partition's `_pm` header.
            execute("ALTER TABLE x ADD COLUMN newcol DOUBLE");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Sanity-check the precondition: the parquet partition's `_pm`
            // header records a non-zero column top for the new column.
            try (TableReader reader = getReader("x")) {
                int parquetIdx = findParquetPartitionIndex(reader);
                Assert.assertTrue("expected a parquet partition", parquetIdx >= 0);
                reader.openPartition(parquetIdx);
                ParquetMetaFileReader meta = reader.getAndInitParquetMetaPartitionDecoder(parquetIdx).metadata();
                boolean hasNonZeroTop = false;
                for (int c = 0, n = meta.getColumnCount(); c < n; c++) {
                    if (meta.getColumnTop(c) != 0) {
                        hasNonZeroTop = true;
                        break;
                    }
                }
                Assert.assertTrue(
                        "expected the parquet `_pm` to record a non-zero column top after ADD COLUMN + CONVERT",
                        hasNonZeroTop
                );
                Assert.assertTrue(
                        "expected the parquet partition to have at least 2 row groups",
                        meta.getRowGroupCount() >= 2
                );
            }

            // Acquire a reader and pin it across the corrupting merge. The
            // reader holds a scoreboard reference at the pre-merge txn, so
            // even if the writer queues the previous partition directory for
            // purge, the on-disk bytes survive long enough for us to re-open
            // the partition through this reader's stale snapshot.
            try (TableReader staleReader = getReader("x")) {
                int parquetIdx = findParquetPartitionIndex(staleReader);
                Assert.assertTrue("expected a parquet partition", parquetIdx >= 0);

                final long sizeBefore = staleReader.openPartition(parquetIdx);
                Assert.assertTrue("partition should have rows", sizeBefore > 0);

                // O3 insert into the parquet partition with values for the
                // newly-added column. The merge must zero the effective column
                // top for the new column in the active `_pm` snapshot while
                // leaving the old bytes intact for the stale reader.
                execute(
                        """
                                INSERT INTO x(a, newcol, ts) VALUES
                                (50, 7.5, '2020-01-01T00:30:00.000Z'),
                                (51, 8.5, '2020-01-01T01:30:00.000Z')
                                """
                );
                drainWalQueue();

                // Re-open the parquet partition through the held reader
                // without refreshing its txn snapshot. The stale reader must
                // keep reading the pre-merge `_pm` snapshot even though a fresh
                // footer has been appended for the committed state.
                staleReader.closePartitionByIndex(parquetIdx);
                final long sizeAfter = staleReader.openPartition(parquetIdx);
                Assert.assertEquals(
                        "stale snapshot should resolve to the same partition row count",
                        sizeBefore,
                        sizeAfter
                );
            }

            // A fresh reader sees the post-merge state with the new rows
            // merged in.
            assertSql(
                    "count\n14\n",
                    "SELECT count() FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02'"
            );
            assertSql(
                    """
                            a\tnewcol\tts
                            1\tnull\t2020-01-01T00:00:00.000000Z
                            50\t7.5\t2020-01-01T00:30:00.000000Z
                            2\tnull\t2020-01-01T01:00:00.000000Z
                            51\t8.5\t2020-01-01T01:30:00.000000Z
                            3\tnull\t2020-01-01T02:00:00.000000Z
                            4\tnull\t2020-01-01T03:00:00.000000Z
                            5\tnull\t2020-01-01T04:00:00.000000Z
                            6\tnull\t2020-01-01T05:00:00.000000Z
                            7\tnull\t2020-01-01T06:00:00.000000Z
                            8\tnull\t2020-01-01T07:00:00.000000Z
                            9\tnull\t2020-01-01T08:00:00.000000Z
                            10\tnull\t2020-01-01T09:00:00.000000Z
                            11\tnull\t2020-01-01T10:00:00.000000Z
                            12\tnull\t2020-01-01T11:00:00.000000Z
                            99\tnull\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT a, newcol, ts FROM x ORDER BY ts, a"
            );
        });
    }

    private static int findParquetPartitionIndex(TableReader reader) {
        for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
            if (reader.getPartitionFormat(i) == PartitionFormat.PARQUET) {
                return i;
            }
        }
        return -1;
    }
}
