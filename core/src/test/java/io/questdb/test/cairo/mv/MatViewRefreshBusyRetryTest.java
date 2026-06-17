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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.test.AbstractCairoTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Verifies that a transient "table busy" error (e.g. base table reader pool exhausted) while
 * refreshing a materialized view does NOT invalidate the view. Instead, the refresh is deferred and
 * re-driven by {@link MatViewTimerJob} once the backoff elapses, after which the view catches up.
 */
public class MatViewRefreshBusyRetryTest extends AbstractCairoTest {

    private static final AtomicBoolean failBaseReader = new AtomicBoolean(false);
    private static volatile String baseTableName;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Inject an engine whose getReader() throws EntryUnavailableException for the base table,
        // simulating reader pool exhaustion during a materialized view refresh.
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public TableReader getReader(TableToken tableToken) {
                if (failBaseReader.get() && tableToken.getTableName().equals(baseTableName)) {
                    throw EntryUnavailableException.instance("pool size exceeded");
                }
                return super.getReader(tableToken);
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testReaderPoolExhaustionDefersRefreshInsteadOfInvalidating() throws Exception {
        // 0 backoff so the timer sweep re-drives the deferred refresh on the very next tick.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_TIMEOUT, 0);
        failBaseReader.set(false);
        baseTableName = "base_price";
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL;"
            );

            // First bucket (hour 0).
            execute("insert into base_price values('a', 1.0, '2020-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();

            execute(
                    "create materialized view price_1h as (" +
                            "  select ts, avg(price) as avg_price from base_price sample by 1h" +
                            ") partition by day"
            );
            drainWalAndMatViewQueues();

            // Baseline: view is valid and holds exactly one bucket.
            assertViewStatus("valid");
            assertViewRowCount(1);

            // Second bucket (hour 1) lands in the base table.
            execute("insert into base_price values('a', 2.0, '2020-01-01T01:00:00.000000Z')");
            drainWalQueue();

            // Now make the base table reader unavailable and try to refresh.
            failBaseReader.set(true);
            drainMatViewQueue(engine);

            // The transient failure must NOT invalidate the view (it stays pending/refreshing), and
            // the view must not have caught up yet (still one bucket).
            assertNoInvalidViews();
            assertViewRowCount(1);

            // Clear the transient failure and let the timer job re-drive the deferred refresh.
            failBaseReader.set(false);
            drainMatViewTimerQueue(new MatViewTimerJob(engine));
            drainWalAndMatViewQueues();

            // The view stayed valid throughout and has now caught up to both buckets.
            assertViewStatus("valid");
            assertViewRowCount(2);
        });
    }

    private void assertNoInvalidViews() throws Exception {
        assertQuery("select count() from materialized_views where view_status = 'invalid'")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n0\n");
    }

    private void assertViewRowCount(long expected) throws Exception {
        assertQuery("select count() from price_1h")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n" + expected + "\n");
    }

    private void assertViewStatus(String status) throws Exception {
        assertQuery("select view_name, view_status from materialized_views")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_name\tview_status\nprice_1h\t" + status + "\n");
    }
}
