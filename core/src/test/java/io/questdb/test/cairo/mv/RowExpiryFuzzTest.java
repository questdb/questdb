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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Property-based fuzz test for the relative/window EXPIRE ROWS retention modes on passthrough materialized
 * views. For each (seed, mode) it inserts random rows (random key, value with NULLs, unique timestamps
 * spanning many partitions, shuffled insert order to exercise O3), creates a passthrough view with the
 * policy, and asserts two invariants against an INDEPENDENT in-Java oracle:
 * <ol>
 *     <li>the read filter shows EXACTLY the rows the oracle keeps (by id); and</li>
 *     <li>after a physical cleanup sweep the visible set is UNCHANGED — cleanup never deletes a kept row.</li>
 * </ol>
 * An integer {@code id} column is the comparison key, so the assertions avoid timestamp/double formatting.
 */
public class RowExpiryFuzzTest extends AbstractCairoTest {

    // Bridge: AbstractCairoTest.assertSql(expected, sql) was removed in favor of the QueryAssertion
    // builder (OSS #7195). Drive the builder via returnsOnce() so the suite's calls keep working.
    private void assertSql(CharSequence expected, CharSequence sql) throws Exception {
        assertQuery(sql).noLeakCheck().returnsOnce(expected);
    }

    private static final long BASE_TS = 1704067200000000L;   // 2024-01-01T00:00:00Z
    private static final int NUM_KEYS = 4;
    private static final int ROWS = 40;
    private static final long STEP = 21_600_000_000L;        // 6h -> ROWS rows span ROWS/4 days (partitions)

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testFuzzRelativeModes() throws Exception {
        assertMemoryLeak(() -> {
            final long[] seeds = {42L, 7L, 123456L};
            final Mode[] modes = Mode.values();
            int iter = 0;
            for (long seed : seeds) {
                for (Mode mode : modes) {
                    runOne("b" + iter, "v" + iter, seed, mode);
                    iter++;
                }
            }
        });
    }

    private void runOne(String base, String view, long seed, Mode mode) throws Exception {
        final Random rng = new Random(seed + mode.ordinal() * 1000L);
        final int[] id = new int[ROWS];
        final String[] k = new String[ROWS];
        final double[] v = new double[ROWS];
        final boolean[] vNull = new boolean[ROWS];
        final long[] ts = new long[ROWS];
        for (int i = 0; i < ROWS; i++) {
            id[i] = i;
            k[i] = "K" + rng.nextInt(NUM_KEYS);
            // top-N null ordering is implementation-defined; keep that mode NULL-free so the oracle is exact.
            vNull[i] = mode != Mode.TOP_N && rng.nextInt(10) == 0;
            v[i] = rng.nextInt(6); // small range -> ties at the group max/min
            ts[i] = BASE_TS + (long) i * STEP;
        }

        execute("create table " + base + " (id int, k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
        final List<Integer> order = new ArrayList<>();
        for (int i = 0; i < ROWS; i++) {
            order.add(i);
        }
        Collections.shuffle(order, rng); // shuffle insert order -> O3
        final StringBuilder ins = new StringBuilder("insert into ").append(base).append(" values ");
        for (int j = 0; j < order.size(); j++) {
            final int i = order.get(j);
            if (j > 0) {
                ins.append(',');
            }
            ins.append('(').append(id[i]).append(",'").append(k[i]).append("',")
                    .append(vNull[i] ? "null" : Double.toString(v[i]))
                    .append(",cast(").append(ts[i]).append(" as timestamp))");
        }
        execute(ins.toString());
        drainWalAndMatViewQueues();
        execute("create materialized view " + view + " as (select * from " + base + ") " + mode.clause);
        drainWalAndMatViewQueues();

        final String expected = expectedIds(mode, id, k, v, vNull, ts);
        final String msg = "seed=" + seed + " mode=" + mode;
        assertSql(expected, "select id from " + view + " order by id");

        // Physical cleanup must not change what is visible (it only removes already-expired rows).
        final TableToken token = engine.verifyTableName(view);
        final String predicate;
        try (TableMetadata m = engine.getTableMetadata(token)) {
            predicate = m.getExpiryPredicate();
        }
        try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
            job.cleanupTable(token, predicate);
        }
        drainWalAndMatViewQueues();
        try {
            assertSql(expected, "select id from " + view + " order by id");
        } catch (AssertionError e) {
            throw new AssertionError("cleanup changed visible rows [" + msg + "]: " + e.getMessage(), e);
        }
    }

    /**
     * Independent oracle: the ids the policy keeps, sorted, formatted as an {@code assertSql} block.
     */
    private static String expectedIds(Mode mode, int[] id, String[] k, double[] v, boolean[] vNull, long[] ts) {
        // INDEPENDENT formulation for the keep-max/min/window modes: compute each group's extreme in a
        // SEPARATE precomputation pass (a plain max/min reduction over the non-null values per group), then
        // keep a row iff (value IS NULL OR value == extreme). This deliberately does NOT reuse the SUT's
        // "v < max(v) over (...)" predicate shape -- a shared spec bug (e.g. NULL handling) would not be
        // hidden by the oracle mirroring the implementation. For the global mode the group is the empty key.
        final java.util.Map<String, Double> extreme;
        switch (mode) {
            case KEEP_MAX:
            case WINDOW_WHEN:
                extreme = groupExtreme(k, v, vNull, true, true);
                break;
            case KEEP_MAX_GLOBAL:
                extreme = groupExtreme(k, v, vNull, true, false);
                break;
            case KEEP_MIN:
                extreme = groupExtreme(k, v, vNull, false, true);
                break;
            default:
                extreme = null;
        }

        final List<Integer> kept = new ArrayList<>();
        for (int i = 0; i < id.length; i++) {
            final boolean keep;
            if (extreme != null) {
                // value IS NULL OR value == the independently-computed per-group extreme.
                if (vNull[i]) {
                    keep = true;
                } else {
                    final Double e = extreme.get(keyForExtreme(mode, k[i]));
                    keep = e != null && v[i] == e;
                }
            } else {
                keep = keeps(mode, i, k, v, vNull, ts);
            }
            if (keep) {
                kept.add(id[i]);
            }
        }
        Collections.sort(kept);
        final StringBuilder sb = new StringBuilder("id\n");
        for (int x : kept) {
            sb.append(x).append('\n');
        }
        return sb.toString();
    }

    // The grouping key for the extreme lookup: the row's key for partitioned modes, a single shared bucket
    // ("") for the global mode.
    private static String keyForExtreme(Mode mode, String key) {
        return mode == Mode.KEEP_MAX_GLOBAL ? "" : key;
    }

    // Independent per-group extreme: a straight max (or min) reduction over the NON-NULL values, bucketed by
    // key (or a single "" bucket when {@code partitioned} is false). No reference to the SUT predicate.
    private static java.util.Map<String, Double> groupExtreme(String[] k, double[] v, boolean[] vNull, boolean max, boolean partitioned) {
        final java.util.Map<String, Double> m = new java.util.HashMap<>();
        for (int i = 0; i < v.length; i++) {
            if (vNull[i]) {
                continue;
            }
            final String key = partitioned ? k[i] : "";
            final Double cur = m.get(key);
            if (cur == null || (max ? v[i] > cur : v[i] < cur)) {
                m.put(key, v[i]);
            }
        }
        return m;
    }

    private static boolean keeps(Mode mode, int i, String[] k, double[] v, boolean[] vNull, long[] ts) {
        switch (mode) {
            case KEEP_LATEST:
                // keep the row with the max ts per key (timestamps are unique).
                for (int j = 0; j < v.length; j++) {
                    if (k[j].equals(k[i]) && ts[j] > ts[i]) {
                        return false;
                    }
                }
                return true;
            // KEEP_MAX / WINDOW_WHEN / KEEP_MAX_GLOBAL / KEEP_MIN are handled by the INDEPENDENT
            // precomputed-extreme path in expectedIds (value IS NULL OR value == separately-computed extreme),
            // deliberately NOT by a "no greater j" predicate that would mirror the SUT's v < max(v) rule.
            case TOP_N: {
                // keep the top 2 per key by (v desc, ts desc); NULL-free in this mode.
                int better = 0;
                for (int j = 0; j < v.length; j++) {
                    if (k[j].equals(k[i]) && (v[j] > v[i] || (v[j] == v[i] && ts[j] > ts[i]))) {
                        better++;
                    }
                }
                return better < 2;
            }
            case SCALAR_WHEN:
                // scalar value predicate: a row expires when v < 3; NULL is kept (v < 3 is UNKNOWN). This is
                // the one keep-set NOT shared with the read filter (cleanup uses buildRowExpiryKeepFilter).
                return vNull[i] || v[i] >= 3;
            default:
                throw new IllegalStateException();
        }
    }

    private enum Mode {
        KEEP_LATEST("expire rows keep latest partition by k"),
        KEEP_MAX("expire rows keep highest v partition by k"),
        KEEP_MAX_GLOBAL("expire rows keep highest v"),
        KEEP_MIN("expire rows keep lowest v partition by k"),
        TOP_N("expire rows keep 2 highest v partition by k"),
        WINDOW_WHEN("expire rows when v < max(v) over (partition by k)"),
        SCALAR_WHEN("expire rows when v < 3");

        final String clause;

        Mode(String clause) {
            this.clause = clause;
        }
    }
}
