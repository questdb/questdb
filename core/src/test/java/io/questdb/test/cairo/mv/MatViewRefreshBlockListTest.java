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
import org.junit.Test;

/**
 * Verifies the {@code cairo.mat.view.refresh.block.list} configuration: views named in the block
 * list are skipped by every refresh path (incremental, full, range) without being invalidated.
 * The option is an operator escape hatch for a view whose refresh keeps crashing the database -
 * blocking it lets the database start and stay up while the view itself is left untouched.
 */
public class MatViewRefreshBlockListTest extends AbstractCairoTest {

    @Test
    public void testBlockedViewIsNotRefreshedAndCatchesUpAfterUnblock() throws Exception {
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

            // Block the view's refresh. The name is matched case-insensitively, so an upper-cased
            // entry must still block the lower-cased view.
            setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BLOCK_LIST, "PRICE_1H");

            // A second bucket lands in the base table. The refresh must be skipped, not invalidated.
            execute("insert into base_price values('a', 2.0, '2020-01-01T01:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("valid"); // not invalidated
            assertViewRowCount(1);     // not refreshed, still one bucket

            // Unblock the view. A subsequent base commit drives an incremental refresh that catches
            // up the whole backlog accumulated while the view was blocked.
            setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BLOCK_LIST, "");
            execute("insert into base_price values('a', 3.0, '2020-01-01T02:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(3);
        });
    }

    @Test
    public void testBlockedViewSkipsExplicitFullRefresh() throws Exception {
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

            // Block the view, then land more data and request an explicit full rebuild. The block
            // list must win: a full refresh is exactly the dangerous rebuild we want to suppress, so
            // the view stays valid and stale.
            setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BLOCK_LIST, "price_1h");
            execute("insert into base_price values('a', 2.0, '2020-01-01T01:00:00.000000Z')");
            drainWalQueue();

            execute("refresh materialized view price_1h full;");
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(1);

            // Unblock and re-run the full refresh: now it rebuilds and picks up both buckets.
            setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BLOCK_LIST, "");
            execute("refresh materialized view price_1h full;");
            drainWalAndMatViewQueues();
            assertViewStatus("valid");
            assertViewRowCount(2);
        });
    }

    @Test
    public void testUnlistedViewRefreshesWhileAnotherIsBlocked() throws Exception {
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
            execute(
                    "create materialized view price_1d as (" +
                            "  select ts, avg(price) as avg_price from base_price sample by 1d" +
                            ") partition by day"
            );
            drainWalAndMatViewQueues();
            assertViewRowCount("price_1h", 1);
            assertViewRowCount("price_1d", 1);

            // Block only price_1h. The other view sharing the same base table keeps refreshing.
            setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BLOCK_LIST, "price_1h");
            execute("insert into base_price values('a', 2.0, '2020-01-01T01:00:00.000000Z')");
            drainWalAndMatViewQueues();

            assertViewRowCount("price_1h", 1); // blocked: still one bucket
            // Both rows fall in the same day bucket, so the row count alone cannot distinguish a
            // refreshed price_1d from a stalled one. Assert the aggregate advanced (avg(1.0, 2.0) =
            // 1.5) to prove the unlisted view actually folded in the second base row.
            assertViewRowCount("price_1d", 1);
            assertQuery("select avg_price from price_1d")
                    .noLeakCheck()
                    .expectSize()
                    .returns("avg_price\n1.5\n");
            assertQuery("select count() from materialized_views where view_status != 'valid'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n0\n");
        });
    }

    private void assertViewRowCount(String viewName, long expected) throws Exception {
        assertQuery("select count() from " + viewName)
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n" + expected + "\n");
    }

    private void assertViewRowCount(long expected) throws Exception {
        assertViewRowCount("price_1h", expected);
    }

    private void assertViewStatus(String status) throws Exception {
        assertQuery("select view_name, view_status from materialized_views")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_name\tview_status\nprice_1h\t" + status + "\n");
    }
}
