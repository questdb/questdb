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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewRefreshJob;
import org.junit.Before;
import org.junit.Test;

/**
 * A live view's output SYMBOL column inherits the cache flag of the base column
 * it projects, and deliberately does not inherit its capacity.
 * <p>
 * The cache flag is what costs heap: a cached SYMBOL keeps a {@code String} per
 * distinct value in the writer and an {@code ObjList} per reader, so a view over
 * a multi-million-cardinality NOCACHE base column that re-enables caching costs
 * more heap than its own window state. Inheriting the capacity looks like the
 * companion change and is not: it leaves heap unchanged and makes refresh several
 * times slower, because the view probes its own committed dictionary once per row
 * and that probe is slower against a pre-sized index than against one that grew.
 * {@link #testSymbolCapacityIsNotInherited} pins that decision.
 * <p>
 * The engine resolves the base column by name, which is exact rather than
 * heuristic here: a live view's factory tree must be
 * {@code WindowRecordCursorFactory -> [filter?] -> PageFrameRecordCursorFactory}
 * with no projection in between, so a plain column cannot be aliased, and no
 * window function returns SYMBOL. Every output SYMBOL column therefore carries
 * its base column's own name. {@link #testAliasedSymbolProjectionIsRejected}
 * pins that premise.
 */
public class LiveViewOutputSymbolCacheTest extends AbstractLiveViewTest {

    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testAliasedSymbolProjectionIsRejected() throws Exception {
        // Pins the premise the name-based resolution rests on. If live views ever
        // admit a projection layer, an aliased SYMBOL column stops naming its base
        // column and this test is where that shows up: the resolution then needs
        // the query model, the way CreateMatViewOperationImpl does it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, g SYMBOL CAPACITY 65536 NOCACHE, x DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertQuery("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, g AS acct, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY g ORDER BY ts ANCHOR DAILY '00:00')")
                    .failsWith("live view select must be a simple scan of a single WAL base table");
        });
    }

    @Test
    public void testCachedBaseSymbolPropagatesCacheFlag() throws Exception {
        // The flag is inherited in both directions - a CACHE base column keeps
        // caching on the view - so this is propagation, not a blanket NOCACHE.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, g SYMBOL CAPACITY 1024 CACHE, x DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, g, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY g ORDER BY ts ANCHOR DAILY '00:00')");

            assertQuery("SELECT \"column\", symbolCached, symbolCapacity FROM (SHOW COLUMNS FROM lv)")
                    .noRandomAccess()
                    .returns("""
                            column\tsymbolCached\tsymbolCapacity
                            ts\tfalse\t0
                            g\ttrue\t128
                            s\tfalse\t0
                            """);
        });
    }

    @Test
    public void testHighCardinalityNoCacheBaseSymbolPropagates() throws Exception {
        // The customer's shape: a multi-million-cardinality NOCACHE account column.
        // Before the propagation the view's column came out CACHE.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, g SYMBOL CAPACITY 2097152 NOCACHE INDEX CAPACITY 4, x DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 5s START FROM NOW AS " +
                    "SELECT ts, g, sum(x) OVER w AS s, count(g) OVER w AS c FROM base " +
                    "WINDOW w AS (PARTITION BY g ORDER BY ts ANCHOR DAILY '00:00')");

            // indexed stays false whatever the base does: the view never builds a
            // bitmap index over its own output.
            assertQuery("SELECT \"column\", indexed, symbolCached, symbolCapacity FROM (SHOW COLUMNS FROM lv)")
                    .noRandomAccess()
                    .returns("""
                            column\tindexed\tsymbolCached\tsymbolCapacity
                            ts\tfalse\tfalse\t0
                            g\tfalse\tfalse\t128
                            s\tfalse\tfalse\t0
                            c\tfalse\tfalse\t0
                            """);
        });
    }

    @Test
    public void testPropagatedSymbolResolvesValuesAcrossFlush() throws Exception {
        // The propagated cache flag is what the view's dictionary is built with, so
        // a wrong value would surface as unresolvable symbols rather than as a
        // metadata mismatch. Drive real rows through the view and read them back,
        // NULL included.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, g SYMBOL CAPACITY 4096 NOCACHE, x DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, g, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY g ORDER BY ts ANCHOR DAILY '00:00')");

            execute("""
                    INSERT INTO base VALUES
                      ('1970-01-01T00:00:01.000000Z', 'a', 1.0),
                      ('1970-01-01T00:00:02.000000Z', 'b', 2.0),
                      ('1970-01-01T00:00:03.000000Z', 'a', 3.0),
                      ('1970-01-01T00:00:04.000000Z', NULL, 4.0)
                    """);
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT ts, g, s FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tg\ts
                            1970-01-01T00:00:01.000000Z\ta\t1.0
                            1970-01-01T00:00:02.000000Z\tb\t2.0
                            1970-01-01T00:00:03.000000Z\ta\t4.0
                            1970-01-01T00:00:04.000000Z\t\t4.0
                            """);

            // A WHERE over the SYMBOL resolves through the view's own dictionary,
            // which is the path a wrong cache flag or capacity would break.
            assertQuery("SELECT count() FROM lv WHERE g = 'a'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            2
                            """);
        });
    }

    @Test
    public void testSymbolCapacityIsNotInherited() throws Exception {
        // Pins the deliberate half of the decision. A base capacity of 2M leaves
        // the view at the server default, because inheriting it measured 5-7x
        // slower on refresh for no heap saving - see LiveViewTableStructure
        // .getSymbolCapacity. Flip this expectation only with a measurement.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, g SYMBOL CAPACITY 2097152 NOCACHE, x DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, g, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY g ORDER BY ts ANCHOR DAILY '00:00')");

            assertQuery("SELECT symbolCapacity FROM (SHOW COLUMNS FROM lv) WHERE \"column\" = 'g'")
                    .noRandomAccess()
                    .returns("""
                            symbolCapacity
                            128
                            """);
        });
    }

    @Test
    public void testTwoSymbolColumnsResolveIndependently() throws Exception {
        // Proves the mapping is per column rather than positional or first-match:
        // the two base SYMBOL columns differ in cache flag, and the view projects
        // them in the opposite order to their base order.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (" +
                    "ts TIMESTAMP, " +
                    "acct SYMBOL CAPACITY 1048576 NOCACHE, " +
                    "region SYMBOL CAPACITY 512 CACHE, " +
                    "x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, region, acct, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY acct ORDER BY ts ANCHOR DAILY '00:00')");

            assertQuery("SELECT \"column\", symbolCached, symbolCapacity FROM (SHOW COLUMNS FROM lv)")
                    .noRandomAccess()
                    .returns("""
                            column\tsymbolCached\tsymbolCapacity
                            ts\tfalse\t0
                            region\ttrue\t128
                            acct\tfalse\t128
                            s\tfalse\t0
                            """);
        });
    }
}
