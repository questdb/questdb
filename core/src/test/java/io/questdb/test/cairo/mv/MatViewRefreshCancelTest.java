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

import io.questdb.cairo.mv.MatViewRefreshSqlExecutionContext;
import io.questdb.griffin.QueryRegistry;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Verifies that an in-flight materialized view refresh is cancellable and that the refresh execution
 * context's circuit breaker honours the cancel.
 * <p>
 * The refresh job compiles the view query through {@code SqlCompiler.compile()}, which wraps the
 * SELECT factory in a {@code QueryProgress}. {@code QueryProgress.getCursor()} registers the query in
 * the {@link QueryRegistry} (so the refresh shows up in {@code query_activity()} and is reachable by
 * {@code cancel_query()}) and wires the registry entry's cancelled flag into the context's circuit
 * breaker. The refresh context exposes the cancellable simple circuit breaker, so a cancel trips the
 * running cursor on its next check and aborts the refresh.
 * <p>
 * These tests stand in for an operator running {@code cancel_query()} against the refresh, but do so
 * deterministically: a {@link QueryRegistry.Listener} fires inside {@code register()}, before the
 * cursor starts, so the very first circuit breaker check trips. This needs no product code - it
 * exercises the same registration and circuit breaker path that {@code cancel_query()} drives.
 */
public class MatViewRefreshCancelTest extends AbstractCairoTest {

    @Test
    public void testCancelInFlightRefreshTripsCircuitBreaker() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL;"
            );
            execute("insert into base_price values('a', 1.0, '2020-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();

            execute(
                    "create materialized view price_1h as (" +
                            "  select ts, avg(price) as avg_price from base_price sample by 1h" +
                            ") partition by day"
            );
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(1);

            // Cancel every mat view refresh query the moment it registers.
            final QueryRegistry queryRegistry = engine.getQueryRegistry();
            queryRegistry.setListener((query, queryId, executionContext) -> {
                if (executionContext instanceof MatViewRefreshSqlExecutionContext) {
                    queryRegistry.getEntry(queryId).cancel();
                }
            });
            try {
                execute("insert into base_price values('a', 2.0, '2020-01-01T01:00:00.000000Z')");
                drainWalAndMatViewQueues();
            } finally {
                queryRegistry.setListener(null);
            }

            // The cancelled refresh did not advance the view: it still holds a single bucket.
            assertViewRowCount(1);
            // The refresh failed because the circuit breaker tripped, leaving the view invalid with a
            // reason that names the cancellation. This proves the cancel actually reached the cursor
            // rather than being silently ignored.
            assertViewStatus("invalid");
            assertQuery(
                    "select count() from materialized_views " +
                            "where view_status = 'invalid' and invalidation_reason like '%cancelled by user%'"
            )
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
        });
    }

    @Test
    public void testRefreshResumesCleanlyAfterAnEarlierCancel() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL;"
            );
            execute("insert into base_price values('a', 1.0, '2020-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();

            execute(
                    "create materialized view price_1h as (" +
                            "  select ts, avg(price) as avg_price from base_price sample by 1h" +
                            ") partition by day"
            );
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(1);

            // Cancel a single refresh, then remove the listener. The refresh context is reused across
            // refreshes, so this also checks that a cancel does not leave a stale cancelled flag on the
            // circuit breaker that would poison the next refresh.
            final QueryRegistry queryRegistry = engine.getQueryRegistry();
            queryRegistry.setListener((query, queryId, executionContext) -> {
                if (executionContext instanceof MatViewRefreshSqlExecutionContext) {
                    queryRegistry.getEntry(queryId).cancel();
                }
            });
            try {
                execute("insert into base_price values('a', 2.0, '2020-01-01T01:00:00.000000Z')");
                drainWalAndMatViewQueues();
            } finally {
                queryRegistry.setListener(null);
            }
            assertViewStatus("invalid");

            // An explicit full refresh now succeeds and catches up the whole backlog, confirming the
            // circuit breaker reset properly after the earlier cancel.
            execute("refresh materialized view price_1h full;");
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(2);
        });
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
