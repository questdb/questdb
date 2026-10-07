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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.mp.SCSequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Under ADAPTIVE, an apply pass takes an inline save point (durable epoch) for an index only when the
 * pass really published a posting-index reseal purge: the purge is gated behind the save-point pin, and
 * the save point moves the pin past it. A pass that published no purge takes no inline save point. The
 * periodic save point is configured out of reach, so every save point counted here is an inline one.
 * <p>
 * Covers every index kind a symbol column can carry, with and without DEDUP, for in-order appends that
 * cross partition boundaries, late rows into a native partition, and late rows into a parquet partition.
 * Late rows into a native partition reseal a posting index on every pass, so those runs also check that a
 * published purge does get its save point.
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
    public void testSavePointOnlyWhenPurgeQueued() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1_000_000_000L);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, Long.MAX_VALUE);
        assertMemoryLeak(() -> {
            final String symbol = "NONE".equals(indexKind) ? "s SYMBOL" : "s SYMBOL INDEX TYPE " + indexKind;
            execute("CREATE TABLE t (ts TIMESTAMP, " + symbol + ", v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL"
                    + (dedup ? " DEDUP UPSERT KEYS(ts, s)" : ""));
            final TableToken token = engine.verifyTableName("t");

            // Day 1 is the late-row target, day 2 the append start. The first apply pass always takes a
            // save point, and so does the parquet conversion; both happen before counting starts.
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
            drainPurgeQueue();
            try (TableWriter w = getWriter(token)) {
                Assert.assertEquals(CommitMode.ADAPTIVE, w.getEffectiveCommitMode());
            }

            final StringBuilder passLog = new StringBuilder();
            int passesWithPurge = 0;
            int savePoints = 0;
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
                savePoints += (int) epochs;
                if (purges == 0 && epochs != 0) {
                    Assert.fail("save point without a queued purge at pass " + pass + ':' + passLog);
                }
                if (purges > 0 && epochs == 0) {
                    Assert.fail("queued purge without a save point at pass " + pass + ':' + passLog);
                }
            }
            System.out.println("AdaptiveIndexEpochCadence index=" + indexKind + " dedup=" + dedup
                    + " pattern=" + pattern
                    + " passes=" + PASSES + " passesWithPurge=" + passesWithPurge + " savePoints=" + savePoints);
            if (indexKind.startsWith("POSTING") && pattern == WritePattern.O3_NATIVE) {
                Assert.assertTrue("late rows queued no posting purge:" + passLog, passesWithPurge > 0);
            }
            if (!indexKind.startsWith("POSTING")) {
                Assert.assertEquals(0, passesWithPurge);
            }
            assertQuery("SELECT count() FROM t").noRandomAccess().expectSize().returns("count\n" + (ROWS_PER_PASS * (PASSES + 1) + 1) + "\n");
        });
    }

    private static void appendRow(StringBuilder sql, long ts, long v) {
        sql.append('(').append(ts).append("::timestamp, '").append((char) ('a' + Math.abs(v % 4))).append("', ")
                .append(v).append(')');
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
}
