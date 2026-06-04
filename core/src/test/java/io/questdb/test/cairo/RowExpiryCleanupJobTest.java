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

package io.questdb.test.cairo;

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Drives {@link RowExpiryCleanupJob} deterministically (pinned {@code now()} via
 * {@link #setCurrentMicros(long)}) and verifies PHYSICAL row reclamation independently of the
 * read-time filter. Physical removal is checked by opening a {@link TableReader} directly (which
 * bypasses SQL and the read filter entirely) and counting raw rows, plus {@code table_partitions('t')}
 * for partition counts.
 * <p>
 * 2024-01-10T00:00:00Z = 1704844800000000 micros; the policy {@code ts < now()} therefore expires
 * everything strictly before 2024-01-10.
 */
public class RowExpiryCleanupJobTest extends AbstractCairoTest {

    // 2024-01-10T00:00:00.000000Z
    private static final long NOW_MICROS = 1704844800000000L;

    @Test
    public void testJobBestEffortReadCorrectBeforeAndAfter() throws Exception {
        // Best-effort: a normal SELECT already hides expired rows BEFORE the job (read filter). AFTER
        // the job, the raw on-disk rows match what the filter showed.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN ts < now()");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // expired
            execute("insert into t values ('BBB', 2.0, '2024-01-06T00:00:00.000000Z')"); // expired
            execute("insert into t values ('CCC', 3.0, '2024-01-10T06:00:00.000000Z')"); // live (active partition)
            drainWalQueue();

            // BEFORE: filtered read hides the two expired rows; raw read still has all 3.
            assertSql("count\n1\n", "select count() from t");
            assertEquals(3, rawRowCount("t"));

            runCleanup();
            drainWalQueue();

            // AFTER: raw == filtered. The 2024-01-10 active partition row survives; the two older
            // fully-expired day-partitions are physically dropped.
            assertSql("count\n1\n", "select count() from t");
            assertEquals(1, rawRowCount("t"));
        });
    }

    @Test
    public void testJobDropsFullyExpiredPartitions() throws Exception {
        // Fast path: bare "ts < now()" => classification straight from partition min/max, no SQL count.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN ts < now()");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // expired partition
            execute("insert into t values ('BBB', 2.0, '2024-01-06T00:00:00.000000Z')"); // expired partition
            execute("insert into t values ('CCC', 3.0, '2024-01-07T00:00:00.000000Z')"); // expired partition
            execute("insert into t values ('DDD', 4.0, '2024-01-10T06:00:00.000000Z')"); // live / active
            drainWalQueue();

            assertSql("count\n4\n", "select count() from table_partitions('t')");
            assertEquals(4, rawRowCount("t"));

            runCleanup();
            drainWalQueue();

            // Three older fully-expired day-partitions physically gone; only the active partition left.
            assertSql("count\n1\n", "select count() from table_partitions('t')");
            assertEquals(1, rawRowCount("t"));
            assertSql(
                    "name\n2024-01-10\n",
                    "select name from table_partitions('t') order by name"
            );
        });
    }

    @Test
    public void testJobNonTimePredicate() throws Exception {
        // CUSTOM (non-timestamp) predicate => survivors counted via SQL per partition.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN v < 2.0");
            // 2024-01-05 partition: one expired (v=1.0), one kept (v=5.0) -> REPLACE
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')");
            execute("insert into t values ('BBB', 5.0, '2024-01-05T12:00:00.000000Z')");
            // 2024-01-06 partition: all expired (v<2) -> DROP
            execute("insert into t values ('CCC', 0.5, '2024-01-06T00:00:00.000000Z')");
            execute("insert into t values ('DDD', 1.5, '2024-01-06T12:00:00.000000Z')");
            // active partition 2024-01-10: expired row stays (active protection)
            execute("insert into t values ('EEE', 0.1, '2024-01-10T06:00:00.000000Z')");
            drainWalQueue();

            assertEquals(5, rawRowCount("t"));

            runCleanup();
            drainWalQueue();

            // Physically: 2024-01-05 keeps only v=5.0; 2024-01-06 dropped; 2024-01-10 untouched (active).
            assertEquals(2, rawRowCount("t"));
            assertEquals(1, rawRowCountWhere("t", "2024-01-05")); // only the kept v=5.0 row remains
            // The only raw row with v<2 left is in the ACTIVE partition (2024-01-10, v=0.1); all non-active
            // v<2 rows are physically gone. Read the v column directly to bypass the read filter.
            assertEquals(1, rawCountVBelow("t", 2.0));
        });
    }

    @Test
    public void testJobReplacePartialPartition() throws Exception {
        // A non-active partition with mixed expired+fresh rows: after the job its raw row count equals
        // the survivor count, and other partitions are intact.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN ts < dateadd('h', -12, now())"); // expires < 2024-01-09T12:00
            // 2024-01-09 partition straddles the cutoff: 06:00 expired, 18:00 kept -> REPLACE, 1 survivor
            execute("insert into t values ('AAA', 1.0, '2024-01-09T06:00:00.000000Z')"); // expired
            execute("insert into t values ('BBB', 2.0, '2024-01-09T18:00:00.000000Z')"); // kept
            // 2024-01-08 fully expired -> would be dropped; keep one to prove only it is affected
            execute("insert into t values ('CCC', 3.0, '2024-01-08T00:00:00.000000Z')"); // expired
            // active partition (newest) untouched
            execute("insert into t values ('DDD', 4.0, '2024-01-10T06:00:00.000000Z')");
            drainWalQueue();

            assertEquals(4, rawRowCount("t"));

            runCleanup();
            drainWalQueue();

            // 2024-01-09 compacted to its single survivor; 2024-01-08 dropped; 2024-01-10 intact.
            assertEquals(1, rawRowCountWhere("t", "2024-01-09"));
            assertEquals(0, rawRowCountWhere("t", "2024-01-08"));
            assertEquals(1, rawRowCountWhere("t", "2024-01-10"));
            assertEquals(2, rawRowCount("t"));
        });
    }

    @Test
    public void testJobSkipsActivePartition() throws Exception {
        // Expired rows in the active (newest) partition must remain PHYSICALLY present after the job
        // (they stay hidden by the read filter). All rows live in a single day-partition, which is
        // therefore the active partition; now() is set well past them so every row is expired.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN ts < now()");
            execute("insert into t values ('AAA', 1.0, '2024-01-10T01:00:00.000000Z')");
            execute("insert into t values ('BBB', 2.0, '2024-01-10T02:00:00.000000Z')");
            drainWalQueue();

            // Advance now() to 2024-01-15: both 2024-01-10 rows are now expired, but they live in the
            // ONLY partition, which is the active one => the job must skip it.
            setCurrentMicros(NOW_MICROS + 5L * 24 * 3600 * 1_000_000L);
            runCleanup();
            drainWalQueue();

            // Both rows physically remain (active-partition protection); the read filter still hides them.
            assertEquals(2, rawRowCount("t"));
            assertEquals(1, partitionCount("t"));
            assertSql("count\n0\n", "select count() from t");
        });
    }

    @Test
    public void testJobViaRunSeriallyDiscoversPoliciedTables() throws Exception {
        // Exercises the full discovery -> classify -> reclaim flow through runSerially() (metadata-cache
        // discovery + global throttle). The first runSerially() always proceeds (lastGlobalCheck unset).
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            // Policied table.
            execute("create table p (sym symbol, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN ts < now()");
            execute("insert into p values ('AAA', '2024-01-05T00:00:00.000000Z')"); // expired partition
            execute("insert into p values ('BBB', '2024-01-10T06:00:00.000000Z')"); // active partition
            // Non-policied table must be ignored by discovery.
            execute("create table np (sym symbol, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into np values ('XXX', '2024-01-05T00:00:00.000000Z')");
            execute("insert into np values ('YYY', '2024-01-06T00:00:00.000000Z')");
            drainWalQueue();

            assertEquals(2, rawRowCount("p"));
            assertEquals(2, rawRowCount("np"));

            final RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine);
            try {
                // run(0) is the public Job entrypoint; it delegates to runSerially(). First call always
                // proceeds (lastGlobalCheck unset) and discovers policied objects via the metadata cache.
                job.run(0);
            } finally {
                Misc.free(job);
            }
            drainWalQueue();

            // Policied table: expired partition dropped, active row kept.
            assertEquals(1, rawRowCount("p"));
            // Non-policied table untouched.
            assertEquals(2, rawRowCount("np"));
        });
    }

    private static int partitionCount(String tableName) {
        final TableToken token = engine.verifyTableName(tableName);
        try (TableReader reader = engine.getReader(token)) {
            return reader.getPartitionCount();
        }
    }

    private static long rawRowCount(String tableName) {
        final TableToken token = engine.verifyTableName(tableName);
        try (TableReader reader = engine.getReader(token)) {
            return reader.size();
        }
    }

    /**
     * Counts raw on-disk rows whose {@code v} (double) column is strictly below {@code threshold},
     * reading the column memory directly. Bypasses SQL and the read filter entirely.
     */
    private static long rawCountVBelow(String tableName, double threshold) {
        final TableToken token = engine.verifyTableName(tableName);
        long count = 0;
        try (TableReader reader = engine.getReader(token)) {
            final int vIndex = reader.getMetadata().getColumnIndex("v");
            final int n = reader.getPartitionCount();
            for (int i = 0; i < n; i++) {
                final long rows = reader.openPartition(i);
                if (rows <= 0) {
                    continue;
                }
                final int columnBase = reader.getColumnBase(i);
                final MemoryCR vColumn = reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, vIndex));
                for (long r = 0; r < rows; r++) {
                    if (vColumn.getDouble(r * Double.BYTES) < threshold) {
                        count++;
                    }
                }
            }
        }
        return count;
    }

    /**
     * Counts raw on-disk rows in the day-partition whose floor matches {@code dayPrefix} (e.g.
     * {@code "2024-01-09"}). Opens partitions directly, so the read filter is bypassed.
     */
    private static long rawRowCountWhere(String tableName, String dayPrefix) {
        final TableToken token = engine.verifyTableName(tableName);
        final StringSink sink = new StringSink();
        long total = 0;
        try (TableReader reader = engine.getReader(token)) {
            final int n = reader.getPartitionCount();
            for (int i = 0; i < n; i++) {
                final long floor = reader.getPartitionTimestampByIndex(i);
                sink.clear();
                PartitionBy.setSinkForPartition(
                        sink,
                        reader.getMetadata().getTimestampType(),
                        reader.getPartitionedBy(),
                        floor
                );
                if (sink.toString().startsWith(dayPrefix)) {
                    total += reader.openPartition(i);
                }
            }
        }
        return total;
    }

    private void runCleanup() {
        final TableToken token = engine.verifyTableName("t");
        final RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine);
        try {
            // cleanupIntervalMicros is irrelevant when calling cleanupTable directly (cadence is enforced
            // only in runSerially()); pass 0 so there is no implied throttle.
            job.cleanupTable(token, expiryPredicateOf(token), 0);
        } finally {
            Misc.free(job);
        }
    }

    private static String expiryPredicateOf(TableToken token) {
        try (TableMetadata metadata = engine.getTableMetadata(token)) {
            return metadata.getExpiryPredicate();
        }
    }
}
