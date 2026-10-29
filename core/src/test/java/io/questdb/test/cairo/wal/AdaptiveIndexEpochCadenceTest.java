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


package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.mp.SCSequence;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Under ADAPTIVE, an apply pass takes no inline save point (durable epoch) for an index, even when it
 * publishes a posting-index reseal purge. A purge waits only for a superseded {@code .pv} the last save point
 * still reads, until the next periodic save point moves the pin; every other purge completes at once. Covers
 * every index kind of a symbol column, with and without DEDUP, for in-order appends across partitions and
 * late rows into native and parquet partitions (late rows into a native partition always publish purges).
 */
@RunWith(Parameterized.class)
public class AdaptiveIndexEpochCadenceTest extends AbstractCairoTest {
    private static final long DAY = 86_400_000_000L;
    private static final long DAY1 = 1_704_067_200_000_000L; // 2024-01-01T00:00:00Z
    private static final String EPOCH_METRIC = "questdb_wal_adaptive_epoch_advances_total";
    private static final long HOUR = 3_600_000_000L;
    private static final String[] INDEX_KINDS = {
            "NONE",
            "BITMAP",
            "POSTING",
            "POSTING DELTA",
            "POSTING EF",
            "POSTING INCLUDE (v)"
    };
    private static final int PASSES = 60;
    private static final int RECLAIM_PASSES = 5;
    private static final int ROWS_PER_PASS = 10;
    private final boolean dedup;
    private final String indexKind;
    private final WritePattern pattern;

    public AdaptiveIndexEpochCadenceTest(String indexKind, boolean dedup, WritePattern pattern) {
        this.indexKind = indexKind;
        this.dedup = dedup;
        this.pattern = pattern;
    }

    @Parameterized.Parameters(name = "index={0}, dedup={1}, pattern={2}")
    public static Collection<Object[]> data() {
        final List<Object[]> params = new ArrayList<>();
        for (String indexKind : INDEX_KINDS) {
            for (boolean dedup : new boolean[]{false, true}) {
                for (WritePattern pattern : WritePattern.values()) {
                    params.add(new Object[]{indexKind, dedup, pattern});
                }
            }
        }
        return params;
    }

    @Test
    public void testNoInlineSavePoint() throws Exception {
        setUpAdaptive();
        assertMemoryLeak(() -> {
            createAndSeed();
            drainPurgeQueue();

            final StringBuilder passLog = new StringBuilder();
            int passesWithPurge = 0;
            for (int pass = 0; pass < PASSES; pass++) {
                execute(passSql(pass));
                final long epochsBefore = TestUtils.getMetricValue(engine, EPOCH_METRIC);
                drainWalQueue();
                final long epochs = TestUtils.getMetricValue(engine, EPOCH_METRIC) - epochsBefore;
                final long purges = drainPurgeQueue();
                passLog.append(" [").append(pass).append(": purges=").append(purges)
                        .append(", savePoints=").append(epochs).append(']');
                if (purges > 0) {
                    passesWithPurge++;
                }
                if (epochs != 0) {
                    Assert.fail("inline save point at pass " + pass + ':' + passLog);
                }
            }
            if (indexKind.startsWith("POSTING") && pattern == WritePattern.O3_NATIVE) {
                Assert.assertTrue("late rows queued no posting purge:" + passLog, passesWithPurge > 0);
            }
            if (!indexKind.startsWith("POSTING")) {
                Assert.assertEquals(0, passesWithPurge);
            }
            assertQuery("SELECT count() FROM t").noRandomAccess().expectSize().returns("count\n" + (ROWS_PER_PASS * (PASSES + 1) + 1) + "\n");
        });
    }

    @Test
    public void testSupersededValueFileReclaimedAfterTwoSavePoints() throws Exception {
        Assume.assumeTrue(indexKind.startsWith("POSTING"));
        setUpAdaptive();
        // The reclaim under test starts from an in-place commit resealing the index and superseding a .pv the save
        // point reads. Only the whole-partition rewrite reseals: with merge-append on (the harness default) a
        // covering index grows by a generation per commit and supersedes nothing. Pin the production default.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        // Retry a held purge on every job run, so a released one is removed on the next run.
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_LIMIT, 1);
        assertMemoryLeak(() -> {
            createAndSeed();
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("t"));
            try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                runPurgeJob(job);
                final long pin = tracker.getPinnedEpochTxn();
                Assert.assertEquals(List.of(), assertOnlyPinHolds(job, purges(job.getLogTableName()), pin, tracker.getOtherPinnedEpochTxn()));
                final long epochsBefore = TestUtils.getMetricValue(engine, EPOCH_METRIC);
                for (int pass = 0; pass < RECLAIM_PASSES; pass++) {
                    execute(passSql(pass));
                    drainWalQueue();
                    runPurgeJob(job);
                }
                Assert.assertEquals("inline save point", epochsBefore, TestUtils.getMetricValue(engine, EPOCH_METRIC));
                Assert.assertEquals(pin, tracker.getPinnedEpochTxn());
                final List<String> held = assertOnlyPinHolds(job, purges(job.getLogTableName()), pin, tracker.getOtherPinnedEpochTxn());
                // A commit that takes the O3 path and appends to the partition the save point indexed reseals a
                // covering index in place, superseding the .pv the save point reads: DEDUP sends every commit
                // there, and so does a late row into the parquet partition. Elsewhere the reseal lands in a new
                // partition version, whose predecessor goes with the partition purge.
                if (indexKind.contains("INCLUDE") && (dedup || pattern == WritePattern.O3_PARQUET)) {
                    Assert.assertFalse("no .pv of the save point superseded", held.isEmpty());
                }

                // The next periodic save point keeps the prior one pinned, as recovery falls back to it if the new one
                // is damaged; the one after it moves both pins past every held window.
                node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 1);
                execute(passSql(RECLAIM_PASSES));
                drainWalQueue();
                Assert.assertEquals("periodic save point", epochsBefore + 1, TestUtils.getMetricValue(engine, EPOCH_METRIC));
                runPurgeJob(job);
                final long nextPin = tracker.getPinnedEpochTxn();
                Assert.assertTrue(nextPin > pin);
                Assert.assertEquals(pin, tracker.getOtherPinnedEpochTxn());
                Assert.assertTrue("the older pin keeps what it held", assertOnlyPinHolds(job, purges(job.getLogTableName()), nextPin, pin).containsAll(held));
                execute(passSql(RECLAIM_PASSES + 1));
                drainWalQueue();
                Assert.assertEquals("second periodic save point", epochsBefore + 2, TestUtils.getMetricValue(engine, EPOCH_METRIC));
                runPurgeJob(job);
                Assert.assertEquals(nextPin, tracker.getOtherPinnedEpochTxn());
                // Only what the two current pins name may still be held; the pass after the first save point can
                // supersede a file that save point names.
                assertOnlyPinHolds(job, purges(job.getLogTableName()), tracker.getPinnedEpochTxn(), nextPin);
                for (String file : held) {
                    Assert.assertFalse("not removed after the second save point: " + file, exists(file));
                }
            }
            assertQuery("SELECT count() FROM t").noRandomAccess().expectSize().returns("count\n" + (ROWS_PER_PASS * (RECLAIM_PASSES + 3) + 1) + "\n");
            // The live index still answers for every value.
            assertQuery("SELECT count() FROM t WHERE s IN ('a', 'b', 'c', 'd')").noRandomAccess().expectSize()
                    .returns("count\n" + (ROWS_PER_PASS * (RECLAIM_PASSES + 3) + 1) + "\n");
        });
    }

    private static void appendRow(StringBuilder sql, long ts, long v) {
        sql.append('(').append(ts).append("::timestamp, '").append((char) ('a' + Math.abs(v % 4))).append("', ")
                .append(v).append(')');
    }

    // Asserts that exactly the purges whose window holds the pin wait, with their .pv still on disk, and
    // that every other purge is done. Returns the held .pv files.
    private static List<String> assertOnlyPinHolds(PostingSealPurgeJob job, List<Purge> purges, long pin, long otherPin) {
        final List<String> held = new ArrayList<>();
        for (int i = 0, n = purges.size(); i < n; i++) {
            final Purge purge = purges.get(i);
            if ((purge.fromTxn <= pin && pin < purge.toTxn) || (purge.fromTxn <= otherPin && otherPin < purge.toTxn)) {
                Assert.assertFalse("purge done under the pin " + pin + ": " + purge, purge.completed);
                Assert.assertTrue("removed under the pin " + pin + ": " + purge, exists(purge.file));
                held.add(purge.file);
            } else {
                Assert.assertTrue("purge waits outside the pin " + pin + ": " + purge, purge.completed);
                Assert.assertFalse("still on disk outside the pin " + pin + ": " + purge, exists(purge.file));
            }
        }
        Assert.assertEquals(held.size(), job.getOutstandingPurgeTasks());
        return held;
    }

    // Consumes the posting seal-purge tasks published so far and returns how many there were. No purge
    // job runs in this harness, so this is the only consumer.
    private static long drainPurgeQueue() {
        final SCSequence sub = engine.getMessageBus().getPostingSealPurgeSubSeq();
        long n = 0;
        while (true) {
            final long cursor = sub.next();
            if (cursor == -2) {
                continue;
            }
            if (cursor < 0) {
                return n;
            }
            sub.done(cursor);
            n++;
        }
    }

    private static boolean exists(String file) {
        return engine.getConfiguration().getFilesFacade().exists(Path.getThreadLocal(file).$());
    }

    // The purges of table t in the purge log, in publish order.
    private static List<Purge> purges(String logTable) throws Exception {
        final TableToken token = engine.verifyTableName("t");
        final List<Purge> purges = new ArrayList<>();
        try (
                RecordCursorFactory factory = select("SELECT partition_timestamp, partition_name_txn, column_name, "
                        + "posting_column_name_txn, seal_txn, partition_by, timestamp_type, from_table_txn, to_table_txn, "
                        + "completed FROM '" + logTable + "' WHERE table_name = '" + token.getDirName() + "'");
                RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                Path path = new Path()
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                path.of(engine.getConfiguration().getDbRoot()).concat(token);
                TableUtils.setPathForNativePartition(path, record.getInt(6), record.getInt(5), record.getTimestamp(0), record.getLong(1));
                PostingIndexUtils.valueFileName(path, record.getSymA(2), record.getLong(3), record.getLong(4));
                purges.add(new Purge(path.toString(), record.getLong(7), record.getLong(8), record.getTimestamp(9) != Numbers.LONG_NULL));
            }
        }
        return purges;
    }

    private static void runPurgeJob(PostingSealPurgeJob job) {
        for (int i = 0; i < 4; i++) {
            Os.sleep(1);
            job.run();
        }
    }

    private static void setUpAdaptive() {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1_000_000_000L);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, Long.MAX_VALUE);
    }

    private void createAndSeed() throws Exception {
        final String symbol = "NONE".equals(indexKind) ? "s SYMBOL" : "s SYMBOL INDEX TYPE " + indexKind;
        execute("CREATE TABLE t (ts TIMESTAMP, " + symbol + ", v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL"
                + (dedup ? " DEDUP UPSERT KEYS(ts, s)" : ""));
        final TableToken token = engine.verifyTableName("t");

        // Day 1 is the late-row target, day 2 the append start. The first apply pass always takes a save
        // point, and so does the parquet conversion; both happen before counting starts.
        final StringBuilder sql = new StringBuilder("INSERT INTO t VALUES ");
        for (int i = 0; i < ROWS_PER_PASS; i++) {
            appendRow(sql.append(i == 0 ? "" : ","), DAY1 + i * HOUR, i);
        }
        appendRow(sql.append(','), DAY1 + DAY, -1);
        execute(sql.toString());
        drainWalQueue();
        if (pattern == WritePattern.O3_PARQUET) {
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
        }
        try (TableWriter w = getWriter(token)) {
            Assert.assertEquals(CommitMode.ADAPTIVE, w.getEffectiveCommitMode());
        }
    }

    // One WAL commit of ROWS_PER_PASS rows, one hour per pass from day 2, so in-order appends cross a
    // partition boundary every 24 passes. The first row of an O3 pass is late: 30 minutes before the
    // previous pass's maximum (native), or inside day 1 once it is parquet.
    private String passSql(int pass) {
        final long start = DAY1 + DAY + (pass + 1) * HOUR;
        final long step = HOUR / ROWS_PER_PASS;
        final StringBuilder sql = new StringBuilder("INSERT INTO t VALUES ");
        for (int i = 0; i < ROWS_PER_PASS; i++) {
            long ts = start + i * step;
            if (i == 0) {
                switch (pattern) {
                    case O3_NATIVE -> ts = start - HOUR / 2 - 1;
                    case O3_PARQUET -> ts = DAY1 + (pass + 1) * 1_000_000L + 1;
                    default -> {
                    }
                }
            }
            appendRow(sql.append(i == 0 ? "" : ","), ts, (long) pass * ROWS_PER_PASS + i);
        }
        return sql.toString();
    }

    public enum WritePattern {
        IN_ORDER, O3_NATIVE, O3_PARQUET
    }

    private record Purge(String file, long fromTxn, long toTxn, boolean completed) {
    }
}
