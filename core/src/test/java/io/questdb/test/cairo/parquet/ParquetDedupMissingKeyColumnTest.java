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
import io.questdb.cairo.TableWriter;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Dedup O3 merge into a parquet partition that spans several row groups, where
 * a dedup key column is missing from the parquet file (added after CONVERT) or
 * present in it with a stale partition-level column top. The dedup comparer runs
 * on row-group-local indexes, so the column top it receives must be in row-group
 * units: the whole row group for a missing column, zero for a decoded one. Each
 * test runs the same DDL/DML against a native twin "nat" and a parquet twin "pq"
 * and compares them.
 */
public class ParquetDedupMissingKeyColumnTest extends AbstractCairoTest {
    // 2024-01-01T00:00:00.000000Z
    private static final long DAY_TS = 1_704_067_200_000_000L;
    private static final String TAIL = "SELECT id, ts, k FROM pq WHERE ts >= '2024-01-01T20:00' AND ts < '2024-01-02' ORDER BY ts, id";

    @Before
    public void setUp() {
        super.setUp();
        // 12 rows in 2024-01-01 -> 3 row groups of 4 rows
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
    }

    @Test
    public void testKeyAddedAfterConvertDuplicateOfExistingRow() throws Exception {
        // O3 row with k = NULL is an exact dedup duplicate of existing row id=12
        assertMemoryLeak(() -> {
            runAddAfterConvert(12, "INT", "2024-01-01T22:00", false, false);
            assertQuery(TAIL).noLeakCheck().timestamp("ts").returns(
                    """
                            id\tts\tk
                            11\t2024-01-01T20:00:00.000000Z\tnull
                            1000\t2024-01-01T22:00:00.000000Z\t7
                            1001\t2024-01-01T22:00:00.000000Z\tnull
                            """
            );
        });
    }

    @Test
    public void testKeyAddedAfterConvertFirstRowGroup() throws Exception {
        assertMemoryLeak(() -> runAddAfterConvert(12, "INT", "2024-01-01T00:30", false, false));
    }

    @Test
    public void testKeyAddedAfterConvertLastRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            runAddAfterConvert(12, "INT", "2024-01-01T21:30", false, false);
            assertQuery(TAIL).noLeakCheck().timestamp("ts").returns(
                    """
                            id\tts\tk
                            11\t2024-01-01T20:00:00.000000Z\tnull
                            1000\t2024-01-01T21:30:00.000000Z\t7
                            1001\t2024-01-01T21:30:00.000000Z\tnull
                            12\t2024-01-01T22:00:00.000000Z\tnull
                            """
            );
        });
    }

    @Test
    public void testKeyAddedAfterConvertNewerPartitionAddedLater() throws Exception {
        // 2024-01-01 was the last partition at ADD COLUMN time; 2024-01-02 appears afterwards
        assertMemoryLeak(() -> runAddAfterConvert(12, "INT", "2024-01-01T21:30", false, true));
    }

    @Test
    public void testKeyAddedAfterConvertNotLastPartitionAtAdd() throws Exception {
        // no explicit column version record for 2024-01-01
        assertMemoryLeak(() -> runAddAfterConvert(12, "INT", "2024-01-01T21:30", true, false));
    }

    @Test
    public void testKeyAddedAfterConvertSingleRowGroup() throws Exception {
        assertMemoryLeak(() -> runAddAfterConvert(4, "INT", "2024-01-01T03:30", false, false));
    }

    @Test
    public void testKeyAddedAfterConvertVarchar() throws Exception {
        assertMemoryLeak(() -> {
            runAddAfterConvert(12, "VARCHAR", "2024-01-01T21:30", false, false);
            assertQuery(TAIL).noLeakCheck().timestamp("ts").returns(
                    """
                            id\tts\tk
                            11\t2024-01-01T20:00:00.000000Z\t
                            1000\t2024-01-01T21:30:00.000000Z\tv
                            1001\t2024-01-01T21:30:00.000000Z\t
                            12\t2024-01-01T22:00:00.000000Z\t
                            """
            );
        });
    }

    @Test
    public void testKeyAddedBeforeConvert() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(12, false);
            addKey("INT");
            convert();
            insertO3("INT", "2024-01-01T21:30");
            assertTwinsEqual();
        });
    }

    @Test
    public void testKeyPresentWithMidPartitionTop() throws Exception {
        // k is added after 6 rows, so rows 0-5 are NULL in the file (row group 0 entirely,
        // row group 1 half). Dedup must treat those decoded rows as NULL and the rest as values.
        assertMemoryLeak(() -> {
            for (String t : new String[]{"nat", "pq"}) {
                execute("CREATE TABLE " + t + " (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
                execute("INSERT INTO " + t + " SELECT x::INT, '2024-01-01'::TIMESTAMP + (x - 1) * 7_200_000_000L FROM long_sequence(6)");
            }
            drainWalQueue();
            addKey("INT");
            for (String t : new String[]{"nat", "pq"}) {
                execute("INSERT INTO " + t + " SELECT (x + 6)::INT, '2024-01-01T12:00'::TIMESTAMP + (x - 1) * 7_200_000_000L, ((x + 6) * 10)::INT FROM long_sequence(6)");
            }
            drainWalQueue();
            convert();
            for (String t : new String[]{"nat", "pq"}) {
                // replaces id=6 (k NULL in the column top region)
                execute("INSERT INTO " + t + "(id, ts) VALUES (1000, '2024-01-01T10:00')");
                // new row: k=5 does not match the NULL of id=6
                execute("INSERT INTO " + t + "(id, ts, k) VALUES (1001, '2024-01-01T10:00', 5)");
                // replaces id=7 (k=70)
                execute("INSERT INTO " + t + "(id, ts, k) VALUES (1002, '2024-01-01T12:00', 70)");
                // replaces id=1 (k NULL, row group 0 entirely in the column top)
                execute("INSERT INTO " + t + "(id, ts) VALUES (1003, '2024-01-01T00:00')");
            }
            drainWalQueue();
            assertTwinsEqual();
            assertQuery("SELECT id, ts, k FROM pq WHERE ts IN ('2024-01-01T00:00', '2024-01-01T10:00', '2024-01-01T12:00') ORDER BY ts, id").noLeakCheck().timestamp("ts").returns(
                    """
                            id\tts\tk
                            1003\t2024-01-01T00:00:00.000000Z\tnull
                            1000\t2024-01-01T10:00:00.000000Z\tnull
                            1001\t2024-01-01T10:00:00.000000Z\t5
                            1002\t2024-01-01T12:00:00.000000Z\t70
                            """
            );
        });
    }

    @Test
    public void testKeyPresentWithStalePartitionTopAfterInPlaceMerge() throws Exception {
        // k is added before CONVERT with no data, so the file holds k while the column
        // version keeps a partition-level top of 12. The first O3 (k=7) merges in place;
        // the second (same ts, k=NULL) must not dedup against the k=7 row.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        assertMemoryLeak(() -> {
            createTwins(12, false);
            addKey("INT");
            convert();
            for (String t : new String[]{"nat", "pq"}) {
                execute("INSERT INTO " + t + "(id, ts, k) VALUES (1000, '2024-01-01T21:30', 7)");
            }
            drainWalQueue();
            for (String t : new String[]{"nat", "pq"}) {
                execute("INSERT INTO " + t + "(id, ts) VALUES (1001, '2024-01-01T21:30')");
            }
            drainWalQueue();
            assertTwinsEqual();
            assertQuery(TAIL).noLeakCheck().timestamp("ts").returns(
                    """
                            id\tts\tk
                            11\t2024-01-01T20:00:00.000000Z\tnull
                            1000\t2024-01-01T21:30:00.000000Z\t7
                            1001\t2024-01-01T21:30:00.000000Z\tnull
                            12\t2024-01-01T22:00:00.000000Z\tnull
                            """
            );
        });
    }

    @Test
    public void testKeyPresentWithStampedStaleTop() throws Exception {
        // k holds real values in the file but the partition-level column version carries a
        // stale full-partition top. The comparer must read the decoded values, not NULLs:
        // an exact duplicate of id=7 must replace it.
        assertMemoryLeak(() -> {
            for (String t : new String[]{"nat", "pq"}) {
                execute("CREATE TABLE " + t + " (id INT, ts TIMESTAMP, k INT, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k)");
                execute("INSERT INTO " + t + " SELECT x::INT, '2024-01-01'::TIMESTAMP + (x - 1) * 7_200_000_000L, (x * 10)::INT, (x * 100)::INT FROM long_sequence(12)");
            }
            drainWalQueue();
            convert();
            try (TableWriter writer = TestUtils.getWriter(engine, "pq")) {
                writer.upsertColumnVersion(DAY_TS, writer.getColumnIndex("k"), 12);
                writer.commit();
            }
            engine.releaseAllWriters();
            try (TableWriter writer = TestUtils.getWriter(engine, "pq")) {
                Assert.assertEquals(12, writer.getColumnTop(DAY_TS, writer.getColumnIndex("k"), -1));
            }
            for (String t : new String[]{"nat", "pq"}) {
                execute("INSERT INTO " + t + "(id, ts, k, v) VALUES (1000, '2024-01-01T12:00', 70, 1)");
            }
            drainWalQueue();
            assertTwinsEqual();
            assertQuery("SELECT id, ts, k, v FROM pq WHERE ts BETWEEN '2024-01-01T10:00' AND '2024-01-01T14:00'").noLeakCheck().timestamp("ts").returns(
                    """
                            id\tts\tk\tv
                            6\t2024-01-01T10:00:00.000000Z\t60\t600
                            1000\t2024-01-01T12:00:00.000000Z\t70\t1
                            8\t2024-01-01T14:00:00.000000Z\t80\t800
                            """
            );
        });
    }

    private void addKey(String kType) throws Exception {
        for (String t : new String[]{"nat", "pq"}) {
            execute("ALTER TABLE " + t + " ADD COLUMN k " + kType);
            execute("ALTER TABLE " + t + " DEDUP ENABLE UPSERT KEYS(ts, k)");
        }
        drainWalQueue();
    }

    private void assertTwinsEqual() throws Exception {
        Assert.assertFalse("nat suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("nat")));
        Assert.assertFalse("pq suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
        assertQuery("SELECT isParquet FROM table_partitions('pq') WHERE minTimestamp < '2024-01-02'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("isParquet\ntrue\n");
        assertSqlCursors("nat", "pq");
        assertSqlCursors("SELECT count(), min(ts), max(ts) FROM nat", "SELECT count(), min(ts), max(ts) FROM pq");
    }

    private void convert() throws Exception {
        execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
        drainWalQueue();
    }

    private void createTwins(int rows, boolean secondPartition) throws Exception {
        for (String t : new String[]{"nat", "pq"}) {
            execute("CREATE TABLE " + t + " (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
            execute("INSERT INTO " + t + " SELECT x::INT, '2024-01-01'::TIMESTAMP + (x - 1) * 7_200_000_000L FROM long_sequence(" + rows + ")");
            if (secondPartition) {
                execute("INSERT INTO " + t + " VALUES (500, '2024-01-02T01:00')");
            }
        }
        drainWalQueue();
    }

    private void insertO3(String kType, String o3Ts) throws Exception {
        for (String t : new String[]{"nat", "pq"}) {
            execute("INSERT INTO " + t + "(id, ts, k) VALUES (1000, '" + o3Ts + "', " + ("VARCHAR".equals(kType) ? "'v'" : "7") + ")");
            execute("INSERT INTO " + t + "(id, ts) VALUES (1001, '" + o3Ts + "')");
        }
        drainWalQueue();
    }

    private void runAddAfterConvert(
            int rows,
            String kType,
            String o3Ts,
            boolean secondPartitionBeforeAdd,
            boolean secondPartitionAfterAdd
    ) throws Exception {
        createTwins(rows, secondPartitionBeforeAdd);
        convert();
        addKey(kType);
        if (secondPartitionAfterAdd) {
            for (String t : new String[]{"nat", "pq"}) {
                execute("INSERT INTO " + t + "(id, ts) VALUES (600, '2024-01-02T01:00')");
            }
            drainWalQueue();
        }
        insertO3(kType, o3Ts);
        assertTwinsEqual();
    }
}
