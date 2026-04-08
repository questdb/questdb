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
 * Regression test for the `_pm` in-place full-rewrite stale-reader corruption.
 *
 * <p>When a parquet partition's `_pm` header carries a non-zero column top
 * (e.g. a partition that pre-existed an `ALTER TABLE ADD COLUMN`), the next
 * O3 merge used to overwrite the existing `_pm` from offset 0 inside its
 * in-place update path. Any concurrent {@link TableReader} that still held
 * the partition's earlier {@code parquetMetaFileSize} would then read the
 * new bytes through the old logical EOF and decode garbage, throwing
 * {@code invalid _pm footer offset}.
 *
 * <p>The fix escalates the merge to rewrite mode (new {@code nameTxn}
 * directory) whenever the existing `_pm` has any non-zero column top, so
 * the previous bytes are left untouched and the stale-reader contract is
 * preserved.
 */
public class O3ParquetStaleReaderTest extends AbstractCairoTest {

    @Test
    public void testStaleReaderSurvivesO3MergeWithColumnTops() throws Exception {
        // Force the in-place update path to be the default by making rewrite
        // thresholds permissive: the merge would land in the unsafe fallback
        // pre-fix unless something else (e.g. our column-top check) forces
        // rewrite mode.
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
                // newly-added column. Pre-fix this took the in-place update
                // path, hit `column_tops_changed`, and rewrote the existing
                // `_pm` from offset 0 — corrupting the bytes the staleReader
                // would resolve through its earlier `parquetMetaFileSize`.
                // Post-fix the merge escalates to rewrite mode, leaving the
                // pre-merge file intact.
                execute(
                        """
                                INSERT INTO x(a, newcol, ts) VALUES
                                (50, 7.5, '2020-01-01T00:30:00.000Z'),
                                (51, 8.5, '2020-01-01T01:30:00.000Z')
                                """
                );
                drainWalQueue();

                // Re-open the parquet partition through the held reader
                // without refreshing its txn snapshot. Pre-fix the
                // `parquetMetaReader.of(...)` call inside `openParquetMetadata`
                // would throw `invalid _pm footer offset`. Post-fix the
                // partition is read from the still-intact pre-merge directory
                // and the assertion below holds.
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
