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

package io.questdb.test.griffin;

import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Randomised end-to-end test of the row-expiry feature's two invariants, across a variety of predicate
 * shapes (value, time, symbol-set, composite OR), with NULL values and out-of-order inserts:
 * <ol>
 *     <li><b>Read filter:</b> a query over a policied table shows EXACTLY the rows that have NOT expired —
 *     i.e. the predicate is FALSE or NULL for them (three-valued: a NULL operand never expires a row).</li>
 *     <li><b>Cleanup never loses a non-expired row:</b> running the background cleanup must not change the
 *     visible set (no non-expired row is physically deleted, no expired row is resurrected), and the
 *     physical rows remaining are a subset of what was inserted and a superset of the non-expired rows.</li>
 * </ol>
 * The expected non-expired set is computed independently in Java from the same predicate, so the SQL read
 * filter is checked against a second implementation rather than against itself. {@code now()} is pinned so
 * time predicates are deterministic; the seed is logged by {@link TestUtils#generateRandom} for replay.
 */
public class RowExpiryFuzzTest extends AbstractCairoTest {

    // 2024-02-01T00:00:00.000000Z — leaves 8 days of past data below now() plus headroom above it.
    private static final long NOW_MICROS = 1706745600000000L;
    private static final long DAY_MICROS = 86_400_000_000L;
    private static final Log LOG = LogFactory.getLog(RowExpiryFuzzTest.class);

    @Test
    public void testReadFilterAndCleanupNeverLoseNonExpiredRows() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            setCurrentMicros(NOW_MICROS);
            final int iterations = 25;
            for (int iter = 0; iter < iterations; iter++) {
                runOneIteration(rnd, iter);
            }
        });
    }

    // True iff the predicate is TRUE for the row (i.e. the row has expired). FALSE/UNKNOWN (NULL operand)
    // means the row is kept. Mirrors the SQL predicate built in runOneIteration, in three-valued logic.
    private static boolean isExpiredTrue(int kind, String sym, boolean nullV, double v, long ts,
                                         double vThreshold, long timeThreshold) {
        switch (kind) {
            case 0:
                return !nullV && v < vThreshold;
            case 1:
                return !nullV && v > vThreshold;
            case 2:
                return ts < timeThreshold;
            case 3:
                return "S0".equals(sym) || "S2".equals(sym);
            default:
                // v < T OR ts < timeThreshold; with three-valued OR, TRUE iff either disjunct is TRUE.
                return ts < timeThreshold || (!nullV && v < vThreshold);
        }
    }

    private static LongList queryTs(String sql) throws Exception {
        final LongList out = new LongList();
        try (RecordCursorFactory factory = select(sql)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    out.add(record.getTimestamp(0));
                }
            }
        }
        return out;
    }

    private static void runCleanup(String table) {
        final RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine);
        try {
            job.cleanupTable(engine.verifyTableName(table), expiryPredicateOf(table), 0);
        } finally {
            Misc.free(job);
        }
    }

    private static String expiryPredicateOf(String table) {
        try (io.questdb.cairo.sql.TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName(table))) {
            return metadata.getExpiryPredicate();
        }
    }

    private static void shuffle(ObjList<String> list, Rnd rnd) {
        for (int i = list.size() - 1; i > 0; i--) {
            final int j = rnd.nextInt(i + 1);
            final String tmp = list.getQuick(i);
            list.setQuick(i, list.getQuick(j));
            list.setQuick(j, tmp);
        }
    }

    private void assertVisible(String table, LongList expectedSortedTs, String msg) throws Exception {
        final LongList actual = queryTs("select ts from \"" + table + "\" order by ts");
        Assert.assertEquals(msg + " — visible count", expectedSortedTs.size(), actual.size());
        for (int i = 0, n = expectedSortedTs.size(); i < n; i++) {
            Assert.assertEquals(msg + " — visible ts at " + i, expectedSortedTs.getQuick(i), actual.getQuick(i));
        }
    }

    private void runOneIteration(Rnd rnd, int iter) throws Exception {
        final String table = "t" + iter;
        final int kind = rnd.nextInt(5);
        final double vThreshold = 1 + rnd.nextInt(8); // 1..8
        final int days = 1 + rnd.nextInt(5);          // ts < now() - days
        final long timeThreshold = NOW_MICROS - days * DAY_MICROS;

        final String predicate;
        switch (kind) {
            case 0:
                predicate = "v < " + vThreshold;
                break;
            case 1:
                predicate = "v > " + vThreshold;
                break;
            case 2:
                predicate = "ts < dateadd('d', -" + days + ", now())";
                break;
            case 3:
                predicate = "sym in ('S0', 'S2')";
                break;
            default:
                predicate = "v < " + vThreshold + " OR ts < dateadd('d', -" + days + ", now())";
                break;
        }

        execute("create table \"" + table + "\" (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                "EXPIRE ROWS WHEN " + predicate);

        // Unique, strictly-increasing ts spread across the 8 days below now() (so a time predicate has both
        // expired and live rows, and value predicates produce partial partitions -> REPLACE). Inserted in a
        // shuffled order so the WAL commit handles them out-of-order.
        final int rowCount = 20 + rnd.nextInt(60);
        final long stepMicros = (8 * DAY_MICROS) / rowCount;
        final long base = NOW_MICROS - 8 * DAY_MICROS;

        final LongList allTs = new LongList();
        final LongList visibleTs = new LongList();
        final ObjList<String> inserts = new ObjList<>();
        for (int i = 0; i < rowCount; i++) {
            final long ts = base + i * stepMicros + rnd.nextInt(1_000_000); // jitter << stepMicros -> still unique
            final String sym = "S" + rnd.nextInt(4);
            final boolean nullV = rnd.nextInt(5) == 0; // ~20% NULL
            final double v = rnd.nextDouble() * 10.0;
            allTs.add(ts);
            if (!isExpiredTrue(kind, sym, nullV, v, ts, vThreshold, timeThreshold)) {
                visibleTs.add(ts);
            }
            inserts.add("insert into \"" + table + "\" values ('" + sym + "', " + (nullV ? "null" : Double.toString(v)) + ", " + ts + ")");
        }
        shuffle(inserts, rnd);
        for (int i = 0, n = inserts.size(); i < n; i++) {
            execute(inserts.getQuick(i));
        }
        drainWalQueue();
        visibleTs.sort();

        final String tag = "iter=" + iter + ", pred=[" + predicate + "]";

        // Invariant 1: the read filter shows EXACTLY the non-expired rows.
        assertVisible(table, visibleTs, "read filter " + tag);

        // Invariant 2: the cleanup does not change visibility (no non-expired row deleted; none resurrected).
        runCleanup(table);
        drainWalQueue();
        assertVisible(table, visibleTs, "post-cleanup " + tag);

        // Invariant 3: physically, the cleanup removed only expired rows — every non-expired row is still
        // present, and nothing outside the inserted set appeared.
        execute("alter table \"" + table + "\" drop expire");
        drainWalQueue();
        final LongList physical = queryTs("select ts from \"" + table + "\" order by ts");
        final LongHashSet physicalSet = new LongHashSet(physical.size() + 1);
        for (int i = 0, n = physical.size(); i < n; i++) {
            physicalSet.add(physical.getQuick(i));
        }
        for (int i = 0, n = visibleTs.size(); i < n; i++) {
            Assert.assertTrue("cleanup deleted a NON-expired row ts=" + visibleTs.getQuick(i) + " [" + tag + "]",
                    physicalSet.contains(visibleTs.getQuick(i)));
        }
        final LongHashSet insertedSet = new LongHashSet(allTs.size() + 1);
        for (int i = 0, n = allTs.size(); i < n; i++) {
            insertedSet.add(allTs.getQuick(i));
        }
        for (int i = 0, n = physical.size(); i < n; i++) {
            Assert.assertTrue("a row not in the inserted set survived cleanup ts=" + physical.getQuick(i) + " [" + tag + "]",
                    insertedSet.contains(physical.getQuick(i)));
        }

        execute("drop table \"" + table + "\"");
        drainWalQueue();
    }
}
