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
 * CACHE enables a native writer value-to-key map and lets each reader lazily
 * retain resolved values on the heap. Inheriting NOCACHE prevents the view from
 * re-enabling those costs for a high-cardinality output.
 * Inheriting the capacity looks like the companion change and is not: it makes
 * refresh several times slower, because the view probes its own committed
 * dictionary once per row and that probe is slower against a pre-sized index
 * than against one that grew.
 * {@link #testSymbolCapacityIsNotInherited} pins that decision.
 * <p>
 * The engine resolves the base column by tracing the output column back through the
 * compiled plan's nodes, not by matching its name. A live view admits an alias and a
 * scalar projection on either side of the window, so {@code g AS acct} leaves an output
 * SYMBOL column that no base column is named after; a name match answers "not found"
 * there and falls back to the server default, which is the direction that turns caching
 * back on for a base that asked for NOCACHE. {@link #testAliasedSymbolPropagatesCacheFlag}
 * and {@link #testSymbolThroughOutputProjectionPropagatesCacheFlag} pin the two shapes
 * the trace has to survive.
 */
public class LiveViewOutputSymbolCacheTest extends AbstractLiveViewTest {

    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testAliasedSymbolPropagatesCacheFlag() throws Exception {
        // The alias compiles to a mapping node between the base scan and the window, so
        // the view's column is named `acct` and no base column is. A name match finds
        // nothing and silently hands back the server default - CACHE - for a column the
        // base explicitly declared NOCACHE. The trace follows the mapping's cross index
        // instead and keeps the flag.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, g SYMBOL CAPACITY 65536 NOCACHE, x DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, g AS acct, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY g ORDER BY ts ANCHOR DAILY '00:00')");

            assertQuery("SELECT \"column\", symbolCached, symbolCapacity FROM (SHOW COLUMNS FROM lv)")
                    .noRandomAccess()
                    .returns("""
                            column\tsymbolCached\tsymbolCapacity
                            ts\tfalse\t0
                            acct\tfalse\t128
                            s\tfalse\t0
                            """);
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
    public void testSymbolThroughOutputProjectionPropagatesCacheFlag() throws Exception {
        // The other side of the window: wrapping the window function in an expression
        // puts a projection above the window, so every output column - the pass-through
        // SYMBOL included - is reached through that projection's functions rather than
        // straight off the window factory. The trace has to cross it too.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, g SYMBOL CAPACITY 65536 NOCACHE, x DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, g, x - sum(x) OVER w AS dev FROM base " +
                    "WINDOW w AS (PARTITION BY g ORDER BY ts ANCHOR DAILY '00:00')");

            assertQuery("SELECT \"column\", symbolCached, symbolCapacity FROM (SHOW COLUMNS FROM lv)")
                    .noRandomAccess()
                    .returns("""
                            column\tsymbolCached\tsymbolCapacity
                            ts\tfalse\t0
                            g\tfalse\t128
                            dev\tfalse\t0
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
