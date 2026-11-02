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

package io.questdb.test;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.AbstractCairoTest.parseFloorPartialTimestamp;

public class ServerMainMatViewBackfillTest extends AbstractBootstrapTest {
    private static final Log LOG = LogFactory.getLog(ServerMainMatViewBackfillTest.class);

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testConcurrentBackfillAndRefreshFuzz() throws Exception {
        // Concurrency fuzz for the frozen-zone backfill path. While the worker pool runs
        // IMMEDIATE refreshes (triggered by an advancing base frontier), the test thread
        // hammers the view with frozen-zone backfills. Every backfill sits below the
        // boundary, and the boundary only rises, so the gate and the commit-time validator
        // must accept all of them and no concurrent refresh (REPLACE_RANGE + boundary-floor
        // publish) may lose or corrupt any. This stresses the validator-floor read vs
        // refresh-floor publish race and the dual-WAL-writer + apply-ordering interaction
        // that the deterministic C1/C1' tests only cover sequentially.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.CAIRO_MAT_VIEW_ENABLED.getEnvVarName(), "true"
            )) {
                final CairoEngine engine = serverMain.getEngine();
                serverMain.execute("create table base_price (sym symbol, price double, ts timestamp) timestamp(ts) partition by DAY WAL");
                serverMain.execute("insert into base_price values('a', 1.0, '2024-09-10T12:00')");
                serverMain.awaitTxn("base_price", 1);
                serverMain.execute("create materialized view price_1h refresh immediate as " +
                        "select sym, last(price) price, ts from base_price sample by 1h");
                serverMain.execute("alter materialized view price_1h set refresh limit 1 hour");

                // Wait for SET REFRESH LIMIT to apply (ground-truth poll on the gate predicate).
                final TableToken viewToken = engine.verifyTableName("price_1h");
                long deadline = System.currentTimeMillis() + 60_000;
                while (!engine.isBackfillableMatView(viewToken) && System.currentTimeMillis() < deadline) {
                    Os.sleep(5);
                }
                Assert.assertTrue("SET REFRESH LIMIT did not apply in time", engine.isBackfillableMatView(viewToken));

                final int backfills = 300;
                final long bucketMicros = 3_600_000_000L; // 1h
                // Distinct frozen buckets, descending from just below the initial boundary
                // (base max 12:00, limit 1h -> boundary 11:00). The boundary only rises as
                // base advances, so every backfill stays frozen for the whole run.
                final long frozenTop = parseFloorPartialTimestamp("2024-09-10T10:00:00.000000Z");
                long advanceTs = parseFloorPartialTimestamp("2024-09-10T13:00:00.000000Z");

                int rejected = 0;
                for (int i = 0; i < backfills; i++) {
                    // Randomised cadence/step for the base frontier -> the boundary-movement
                    // pattern (and so the overlap with refreshes) varies run-to-run. Each
                    // advance triggers an IMMEDIATE auto-refresh on the worker pool, racing the
                    // backfills. Base stays below the real wall clock.
                    if (rnd.nextInt(8) == 0) {
                        serverMain.execute("insert into base_price values('a', 2.0, " + advanceTs + "::timestamp)");
                        advanceTs += bucketMicros * (1 + rnd.nextInt(4));
                    }
                    final long ts = frozenTop - (long) i * bucketMicros; // distinct, descending, all frozen
                    try {
                        serverMain.execute("insert into price_1h values('a', " + (i + 1) + ", " + ts + "::timestamp)");
                    } catch (Exception rejectedEx) {
                        // Count only an expected engine/SQL rejection; let Errors (AssertionError,
                        // OutOfMemoryError) propagate so a real failure fails the test fast instead
                        // of being miscounted as a rejection.
                        rejected++;
                    }
                }
                Assert.assertEquals("frozen backfills must never be rejected", 0, rejected);

                // Wait for all WAL (backfills + concurrent refreshes) to apply, then verify every
                // frozen backfill survived: rows below the boundary must equal the backfill count
                // (refresh only materialises managed-zone rows at-or-above 11:00).
                deadline = System.currentTimeMillis() + 120_000;
                boolean survived = false;
                while (System.currentTimeMillis() < deadline) {
                    try {
                        serverMain.assertSql(
                                "select count() from price_1h where ts < '2024-09-10T11:00:00.000000Z'",
                                "count\n" + backfills + "\n"
                        );
                        survived = true;
                        break;
                    } catch (AssertionError e) {
                        Os.sleep(50);
                    }
                }
                Assert.assertTrue("not all frozen backfills survived concurrent refresh", survived);
            }
        });
    }
}
