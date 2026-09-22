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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Property test: whether a materialized-view DDL is legal must not depend on anything except the
 * SQL itself.
 * <p>
 * This is the invariant that generalizes. The accept-side corpus can only cover shapes someone
 * thought to write down, but a guard that consults a per-factory property breaks this property for
 * <i>any</i> factory that forgets to override it, including ones added in the future. Two knobs
 * silently change which factory the code generator picks:
 * <ul>
 *     <li>parallelism flags - {@code cairo.mat.view.parallel.sql.enabled} defaults to
 *     {@code cpuAvailable >= 4}, so a config-sensitive guard makes the same DDL legal on a 2-vCPU
 *     node and illegal on a 4-vCPU one;</li>
 *     <li>indexes - adding an index to an unrelated table swaps a filtered scan for an
 *     index-driven row cursor.</li>
 * </ul>
 */
public class MatViewGuardConfigInvarianceTest extends AbstractCairoTest {
    private static final String[] PREDICATES = {
            "ts > (SELECT max(lim) FROM cfg)",
            "ts > (SELECT max(lim) FROM cfg WHERE k = 'a')",
            "ts > (SELECT max(lim) FROM cfg WHERE n > 0 GROUP BY k LIMIT 1)",
            "ts > (SELECT max(lim) FROM cfg GROUP BY k, n LIMIT 1)",
            "ts > (SELECT max(lim) FROM cfg GROUP BY concat(k, 'x') LIMIT 1)",
            "ts > (SELECT lim FROM cfg ORDER BY ts DESC LIMIT 1)",
            "ts > (SELECT max(c1.lim) FROM cfg c1 JOIN cfg c2 ON c1.k = c2.k)",
            "ts > (SELECT lim FROM cfg LATEST ON ts PARTITION BY k LIMIT 1)",
            "k IN (SELECT k FROM cfg)",
    };

    @Test
    public void testVerdictDoesNotDependOnIndexes() throws Exception {
        final List<String> plain = verdicts(false, false);
        final List<String> indexed = verdicts(false, true);
        for (int i = 0; i < PREDICATES.length; i++) {
            Assert.assertEquals(
                    "adding an index to an unrelated table must not change whether a materialized view "
                            + "is legal; predicate: " + PREDICATES[i],
                    plain.get(i),
                    indexed.get(i)
            );
        }
    }

    @Test
    public void testVerdictDoesNotDependOnParallelism() throws Exception {
        final List<String> sequential = verdicts(false, false);
        final List<String> parallel = verdicts(true, false);
        for (int i = 0; i < PREDICATES.length; i++) {
            Assert.assertEquals(
                    "parallelism configuration must not change whether a materialized view is legal "
                            + "(the same DDL would otherwise be accepted on a 2-vCPU node and rejected "
                            + "on a 4-vCPU one); predicate: " + PREDICATES[i],
                    sequential.get(i),
                    parallel.get(i)
            );
        }
    }

    /**
     * Compiles every predicate under one configuration and returns an ACCEPTED/REJECTED verdict per
     * predicate. Verdicts are compared across configurations rather than asserted absolutely, so the
     * property holds even for shapes that are illegal for unrelated reasons.
     */
    private List<String> verdicts(boolean parallel, boolean indexed) throws Exception {
        final List<String> result = new ArrayList<>();
        // Each invocation gets its own namespace: a single test compares two configurations, so the
        // tables and views of the first run must not collide with the second.
        final String ns = (parallel ? "p" : "s") + (indexed ? "i" : "n");
        final String base = "base_" + ns;
        final String cfg = "cfg_" + ns;
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf(parallel));
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED, String.valueOf(parallel));
        // The knob named in the class javadoc: it defaults to cpuAvailable >= 4, so it is the one that
        // actually differs between a 2-vCPU and a 4-vCPU node. Varying only the generic parallel flags
        // would leave the documented failure mode untested.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, String.valueOf(parallel));
        assertMemoryLeak(() -> {
            execute("CREATE TABLE " + base + " (ts TIMESTAMP, k SYMBOL, v DOUBLE, n LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE " + cfg + " (ts TIMESTAMP, k SYMBOL, lim TIMESTAMP, n LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO " + cfg + " VALUES ('2024-01-01T00:00:00Z', 'a', '2024-01-01T00:00:00Z', 1)");
            drainWalQueue();
            if (indexed) {
                execute("ALTER TABLE " + cfg + " ALTER COLUMN k ADD INDEX");
                drainWalQueue();
            }
            for (int i = 0; i < PREDICATES.length; i++) {
                final String predicate = PREDICATES[i].replace("cfg", cfg);
                final String sql = "CREATE MATERIALIZED VIEW mv_inv_" + ns + "_" + i
                        + " AS SELECT ts, sum(v) AS s FROM " + base + " WHERE "
                        + predicate + " SAMPLE BY 1h";
                try {
                    execute(sql);
                    result.add("ACCEPTED");
                } catch (Throwable e) {
                    final String message = String.valueOf(e.getMessage());
                    // Collapse to the guard verdict; unrelated failures compare equal across configs.
                    result.add(message.contains("non-deterministic function cannot be used in materialized view")
                            ? "REJECTED_BY_GUARD"
                            : "REJECTED_OTHER");
                }
            }
        });
        return result;
    }
}
