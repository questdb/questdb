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

import io.questdb.PropertyKey;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        // Older day-partitions are entirely older than now() => zero survivors => DROP the whole day.
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
    public void testJobKeepsNullPredicateColumnRow() throws Exception {
        // Data-loss regression: a row whose predicate column is NULL has NOT expired ("v < 2.0" is UNKNOWN,
        // not TRUE, for NULL v), so the cleanup job MUST keep it. A plain NOT(v < 2.0) keep-filter would
        // (wrongly) drop it; the job uses CASE WHEN (v < 2.0) THEN false ELSE true END, which keeps NULL.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN v < 2.0");
            // 2024-01-05 (non-active): expired (v=1.0), live (v=5.0), live-because-NULL (v=null) -> REPLACE
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')");
            execute("insert into t values ('BBB', 5.0, '2024-01-05T06:00:00.000000Z')");
            execute("insert into t values ('CCC', null, '2024-01-05T12:00:00.000000Z')");
            // active partition keeps it non-active
            execute("insert into t values ('EEE', 5.0, '2024-01-10T06:00:00.000000Z')");
            drainWalQueue();

            assertEquals(3, rawRowCountWhere("t", "2024-01-05"));
            runCleanup();
            drainWalQueue();

            // Only the v=1.0 row is expired; the v=5.0 and the NULL-v rows both survive physically.
            assertEquals(2, rawRowCountWhere("t", "2024-01-05"));
            // Bypassing the read filter, the NULL-v row is still on disk (it was never expired).
            assertSql("sym\n" + "CCC\n", "select sym from t where v is null");
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

    @Test
    public void testJobParquetPartition() throws Exception {
        // Finding D regression: a parquet (non-native) partition must not crash cleanup. The job reads
        // partition totals from the tx file (not raw column memory), so parquet partitions are classified
        // and reclaimed via the SQL survivor path. The old code read the timestamp column directly, which
        // is null for parquet => NPE that aborted the whole table's cleanup.
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN v < 2.0");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // 2024-01-05: all v<2 -> DROP
            execute("insert into t values ('BBB', 1.5, '2024-01-05T06:00:00.000000Z')");
            execute("insert into t values ('CCC', 5.0, '2024-01-06T00:00:00.000000Z')"); // 2024-01-06: all v>=2 -> SKIP
            execute("insert into t values ('DDD', 6.0, '2024-01-06T06:00:00.000000Z')");
            execute("insert into t values ('EEE', 7.0, '2024-01-10T06:00:00.000000Z')"); // active partition
            drainWalQueue();

            // Convert the non-active days to parquet (the active partition is never converted).
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-10T00:00:00.000000Z'");
            drainWalQueue();

            assertEquals(5, rawRowCount("t"));

            runCleanup();
            drainWalQueue();

            // No NPE: 2024-01-05 (all expired) dropped; 2024-01-06 (all kept) untouched; active intact.
            assertEquals(3, rawRowCount("t"));
            assertEquals(0, rawRowCountWhere("t", "2024-01-05"));
            assertEquals(2, rawRowCountWhere("t", "2024-01-06"));
            assertEquals(1, rawRowCountWhere("t", "2024-01-10"));
        });
    }

    @Test
    public void testJobSplitPartition() throws Exception {
        // Bug A/C regression: an O3 split divides one logical day into several physical partitions. The
        // job must act on the whole LOGICAL day — never DROP one split while a sibling split has keepers,
        // and never build an IN literal from a split's directory name (not parseable). Processing per
        // logical day fixes both; the old per-physical-split code threw every cycle and left a latent
        // sibling-wipe DROP. Forward regression guard for the per-logical-day rework.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);
        // Keep the split on 2024-01-08 after it stops being the last partition (otherwise mid-partition
        // splits are squashed back into a single physical partition once a newer day arrives).
        setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 2);
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN v < 2.0");
            // 2024-01-08: dense keeper clusters (v=5) at 00:00 and 01:00 (1000 rows total), in order.
            execute("insert into t select 5.0, '2024-01-08T00:00:00.000000Z'::timestamp from long_sequence(800)");
            execute("insert into t select 5.0, '2024-01-08T01:00:00.000000Z'::timestamp from long_sequence(200)");
            drainWalQueue();
            // O3-insert 5 expired rows (v=1) at 00:30 (between the clusters) so the day splits at 00:30.
            execute("insert into t select 1.0, '2024-01-08T00:30:00.000000Z'::timestamp from long_sequence(5)");
            drainWalQueue();
            // Newer day makes 2024-01-08 (and its splits) non-active.
            execute("insert into t values (5.0, '2024-01-10T06:00:00.000000Z')");
            drainWalQueue();

            assertTrue("expected 2024-01-08 to be split into multiple physical partitions",
                    physicalPartitionsForDay("t", "2024-01-08") > 1);
            assertEquals(1006, rawRowCount("t")); // 1000 keepers + 5 expired + 1 active

            runCleanup();
            drainWalQueue();

            // The 5 expired rows are gone; all 1000 keepers in 2024-01-08 survive (no loss, no
            // duplication); the day is NOT dropped (it has keepers); the active day is untouched.
            assertEquals(1001, rawRowCount("t"));
            assertEquals(1000, rawRowCountWhere("t", "2024-01-08"));
            assertEquals(1, rawRowCountWhere("t", "2024-01-10"));
            assertEquals(0, rawCountVBelow("t", 2.0));
        });
    }

    private static int physicalPartitionsForDay(String tableName, String dayPrefix) {
        final TableToken token = engine.verifyTableName(tableName);
        final StringSink sink = new StringSink();
        int count = 0;
        try (TableReader reader = engine.getReader(token)) {
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                sink.clear();
                PartitionBy.setSinkForPartition(
                        sink,
                        reader.getMetadata().getTimestampType(),
                        reader.getPartitionedBy(),
                        reader.getPartitionTimestampByIndex(i)
                );
                if (sink.toString().startsWith(dayPrefix)) {
                    count++;
                }
            }
        }
        return count;
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
