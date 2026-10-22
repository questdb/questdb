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

package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CastStrToSymbolFunctionFactoryTest extends AbstractCairoTest {
    // s::symbol compared to a timestamp interns every row through getInt(), so each cursor grows a
    // dictionary charged to the query's memory tracker. returns() opens the factory more than once,
    // and each later getCursor() takes the previous query's tracker back from the pool, where
    // PerQueryMemoryTracker asserts that the cursor owner released every charge.
    private static final String TS = "'1970-01-01T00:00:03.000000Z'::timestamp";

    @Test
    public void testCursorCloseReleasesDictionaryInAsOfJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.v, b.v FROM x a ASOF JOIN (SELECT * FROM y WHERE s::symbol = " + TS + ") b")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Filtered AsOf Join Fast")
                    .returns("""
                            v\tv1
                            1\tnull
                            2\tnull
                            3\t3
                            4\t3
                            5\t3
                            6\t3
                            7\t3
                            8\t8
                            9\t8
                            10\t8
                            """);
            assertQuery("SELECT a.v, b.v FROM x a ASOF JOIN (SELECT * FROM y WHERE s::symbol = " + TS + ") b ON (k)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Filtered AsOf Join Fast", "symbolKeyJoin: true")
                    .returns("""
                            v\tv1
                            1\tnull
                            2\tnull
                            3\t3
                            4\tnull
                            5\t3
                            6\tnull
                            7\t3
                            8\t8
                            9\t3
                            10\t8
                            """);
        });
    }

    @Test
    public void testCursorCloseReleasesDictionaryInFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT v FROM (SELECT * FROM x LIMIT 100) WHERE s::symbol = " + TS)
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: s::symbol=")
                    .returns("""
                            v
                            3
                            8
                            """);
        });
    }

    @Test
    public void testCursorCloseReleasesDictionaryInGroupByKey() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT (s::symbol = " + TS + ") b, count() c FROM x GROUP BY b ORDER BY b")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("GroupBy vectorized: false")
                    .returns("""
                            b\tc
                            false\t8
                            true\t2
                            """);
        });
    }

    @Test
    public void testCursorCloseReleasesDictionaryInHashOuterJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.v, b.v FROM x a LEFT JOIN y b ON a.v = b.v AND b.s::symbol = " + TS)
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Hash Left Outer Join Light")
                    .returns("""
                            v\tv1
                            1\tnull
                            2\tnull
                            3\t3
                            4\tnull
                            5\tnull
                            6\tnull
                            7\tnull
                            8\t8
                            9\tnull
                            10\tnull
                            """);
            assertQuery("SELECT a.v, b.v FROM x a RIGHT JOIN y b ON a.v = b.v AND a.s::symbol = " + TS + " ORDER BY b.v")
                    .noLeakCheck()
                    .withPlanContaining("Hash Right Outer Join Light")
                    .returns("""
                            v\tv1
                            null\t1
                            null\t2
                            3\t3
                            null\t4
                            null\t5
                            null\t6
                            null\t7
                            8\t8
                            null\t9
                            null\t10
                            """);
            assertQuery("SELECT a.v, b.v FROM x a FULL JOIN y b ON a.v = b.v AND a.s::symbol = " + TS + " ORDER BY a.v, b.v")
                    .noLeakCheck()
                    .withPlanContaining("Hash Full Outer Join Light")
                    .returns("""
                            v\tv1
                            null\t1
                            null\t2
                            null\t4
                            null\t5
                            null\t6
                            null\t7
                            null\t9
                            null\t10
                            1\tnull
                            2\tnull
                            3\t3
                            4\tnull
                            5\tnull
                            6\tnull
                            7\tnull
                            8\t8
                            9\tnull
                            10\tnull
                            """);
        });
    }

    @Test
    public void testCursorCloseReleasesDictionaryInHorizonJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT h.offset, first(b.s::symbol) f FROM x a HORIZON JOIN y b ON (k) LIST (0s) AS h")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Horizon Join offsets: 1", "keys: [offset]")
                    .returns("""
                            offset\tf
                            0\t1970-01-01T00:00:01.000000Z
                            """);
            assertQuery("SELECT first(b.s::symbol) f FROM x a HORIZON JOIN y b ON (k) LIST (0s) AS h")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Horizon Join offsets: 1")
                    .returns("""
                            f
                            1970-01-01T00:00:01.000000Z
                            """);
        });
    }

    @Test
    public void testCursorCloseReleasesDictionaryInIndexedFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT v FROM x WHERE k IN ('0', '1') AND s::symbol = " + TS)
                    .noLeakCheck()
                    .withPlanContaining("FilterOnValues")
                    .returns("""
                            v
                            3
                            8
                            """);
        });
    }

    @Test
    public void testCursorCloseReleasesDictionaryInNestedLoopJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.v, b.v FROM x a LEFT JOIN y b ON a.v < b.v AND b.s::symbol = " + TS)
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Nested Loop Left Join")
                    .returns("""
                            v\tv1
                            1\t3
                            1\t8
                            2\t3
                            2\t8
                            3\t8
                            4\t8
                            5\t8
                            6\t8
                            7\t8
                            8\tnull
                            9\tnull
                            10\tnull
                            """);
            assertQuery("SELECT a.v, b.v FROM x a RIGHT JOIN y b ON a.v > b.v AND a.s::symbol = " + TS)
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Nested Loop Right Join")
                    .returns("""
                            v\tv1
                            3\t1
                            8\t1
                            3\t2
                            8\t2
                            8\t3
                            8\t4
                            8\t5
                            8\t6
                            8\t7
                            null\t8
                            null\t9
                            null\t10
                            """);
            assertQuery("SELECT a.v, b.v FROM x a FULL JOIN y b ON a.v < b.v AND b.s::symbol = " + TS)
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Nested Loop Full Join")
                    .returns("""
                            v\tv1
                            1\t3
                            1\t8
                            2\t3
                            2\t8
                            3\t8
                            4\t8
                            5\t8
                            6\t8
                            7\t8
                            8\tnull
                            9\tnull
                            10\tnull
                            null\t1
                            null\t2
                            null\t4
                            null\t5
                            null\t6
                            null\t7
                            null\t9
                            null\t10
                            """);
        });
    }

    @Test
    public void testCursorCloseReleasesDictionaryInNotKeyedGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT first(s::symbol) f, count_distinct(s::symbol) d FROM x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("GroupBy vectorized: false")
                    .returns("""
                            f\td
                            1970-01-01T00:00:01.000000Z\t5
                            """);
        });
    }

    @Test
    public void testCursorCloseReleasesDictionaryInParallelFilter() throws Exception {
        assertParallel((engine, context) -> {
            assertQuery("SELECT v FROM x WHERE s::symbol = " + TS)
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .withPlanContaining("Async Filter workers: 4")
                    .returns("""
                            v
                            3
                            8
                            """);
            assertQuery("SELECT v FROM x WHERE s::symbol = " + TS + " ORDER BY v DESC LIMIT 3")
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Async Top K lo: 3 workers: 4")
                    .returns("""
                            v
                            8
                            3
                            """);
        });
    }

    @Test
    public void testCursorCloseReleasesDictionaryInParallelGroupBy() throws Exception {
        assertParallel((engine, context) -> {
            assertQuery("SELECT k, count() c FROM x WHERE s::symbol = " + TS + " GROUP BY k ORDER BY k")
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Async Group By workers: 4")
                    .returns("""
                            k\tc
                            0\t1
                            1\t1
                            """);
            assertQuery("SELECT count() c, sum(v) s FROM x WHERE s::symbol = " + TS)
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Async Group By workers: 4")
                    .returns("""
                            c\ts
                            2\t11
                            """);
        });
    }

    @Test
    public void testCursorCloseReleasesDictionaryInParallelHorizonJoin() throws Exception {
        assertParallel((engine, context) -> assertQuery("SELECT h.offset, count() c FROM x a HORIZON JOIN y b ON (k) LIST (0s) AS h WHERE a.s::symbol = " + TS)
                .withEngine(engine)
                .withContext(context)
                .noLeakCheck()
                .expectSize()
                .withPlanContaining("Async Horizon Join workers: 4")
                .returns("""
                        offset\tc
                        0\t2
                        """));
    }

    @Test
    public void testCursorCloseReleasesDictionaryInParallelWindowJoin() throws Exception {
        assertParallel((engine, context) -> {
            assertQuery("SELECT a.v, count() c FROM x a WINDOW JOIN y b ON (a.k = b.k) RANGE BETWEEN 1 SECOND PRECEDING AND CURRENT ROW WHERE a.s::symbol = " + TS)
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Async Window Fast Join workers: 4", "master filter: s::symbol=")
                    .returns("""
                            v\tc
                            3\t2
                            8\t2
                            """);
            assertQuery("SELECT a.v, count() c FROM x a WINDOW JOIN y b ON (a.v = b.v AND b.s::symbol = " + TS + ") RANGE BETWEEN 1 SECOND PRECEDING AND CURRENT ROW")
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Async Window Join workers: 4", "join filter:")
                    .returns("""
                            v\tc
                            1\t0
                            2\t0
                            3\t1
                            4\t0
                            5\t0
                            6\t0
                            7\t0
                            8\t1
                            9\t0
                            10\t0
                            """);
        });
    }

    @Test
    public void testCursorCloseReleasesDictionaryInSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, first(s::symbol) f FROM x SAMPLE BY 3s ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .withPlanContaining("Sample By")
                    .returns("""
                            ts\tf
                            1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:01.000000Z
                            1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:04.000000Z
                            1970-01-01T00:00:07.000000Z\t1970-01-01T00:00:02.000000Z
                            1970-01-01T00:00:10.000000Z\t1970-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCursorCloseReleasesDictionaryInWindowJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.v, first(b.s::symbol) f FROM x a WINDOW JOIN y b ON (a.k = b.k) RANGE BETWEEN 1 SECOND PRECEDING AND CURRENT ROW")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Window Fast Join")
                    .returns("""
                            v\tf
                            1\t1970-01-01T00:00:01.000000Z
                            2\t1970-01-01T00:00:02.000000Z
                            3\t1970-01-01T00:00:01.000000Z
                            4\t1970-01-01T00:00:02.000000Z
                            5\t1970-01-01T00:00:03.000000Z
                            6\t1970-01-01T00:00:04.000000Z
                            7\t1970-01-01T00:00:00.000000Z
                            8\t1970-01-01T00:00:01.000000Z
                            9\t1970-01-01T00:00:02.000000Z
                            10\t1970-01-01T00:00:03.000000Z
                            """);
            assertQuery("SELECT a.v, first(b.s::symbol) f FROM x a WINDOW JOIN y b RANGE BETWEEN 1 SECOND PRECEDING AND CURRENT ROW")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Window Join")
                    .returns("""
                            v\tf
                            1\t1970-01-01T00:00:01.000000Z
                            2\t1970-01-01T00:00:01.000000Z
                            3\t1970-01-01T00:00:02.000000Z
                            4\t1970-01-01T00:00:03.000000Z
                            5\t1970-01-01T00:00:04.000000Z
                            6\t1970-01-01T00:00:00.000000Z
                            7\t1970-01-01T00:00:01.000000Z
                            8\t1970-01-01T00:00:02.000000Z
                            9\t1970-01-01T00:00:03.000000Z
                            10\t1970-01-01T00:00:04.000000Z
                            """);
        });
    }


    // A lazy cast-to-symbol only needs a dictionary to answer the integer-key API
    // (getInt/valueOf). getSymbol()/getSymbolB() are pure pass-throughs: the wire
    // serializers, ORDER BY and the non-static GROUP BY key sink all read the column
    // through getSymbol on every row, so interning there is dead work that grows an
    // unnecessary cardinality-dependent dictionary for values no consumer ever looks up
    // by key. This pins the contract: a value seen only through getSymbol consumes no
    // symbol key, so the first value routed through getInt is assigned key 0.
    @Test
    public void testGetSymbolPassThroughAndDictionaryLifecycle() throws Exception {
        // The dictionary allocates native memory even without a query tracker, so wrap the body
        // in assertMemoryLeak: a missing free in releaseDictionary() must surface here too, not
        // only in the tracker-bound sibling tests.
        assertMemoryLeak(() -> {
            final FeedFunction arg = new FeedFunction();
            final CastStrToSymbolFunctionFactory.Func func = new CastStrToSymbolFunctionFactory.Func(arg);
            try {
                // getSymbol reads the A-slot and getSymbolB the B-slot; both pass their argument
                // through verbatim without touching the dictionary. Distinct A/B feed values prove
                // getSymbolB reads getStrB, not getStrA.
                arg.valueA = "a_via_getSymbol";
                arg.valueB = "b_via_getSymbolB";
                Assert.assertEquals("a_via_getSymbol", func.getSymbol(null));
                Assert.assertEquals("b_via_getSymbolB", func.getSymbolB(null));
                arg.valueA = null;
                Assert.assertNull(func.getSymbol(null));

                // Those getSymbol calls must have consumed no symbol keys, so the first value
                // routed through getInt is assigned key 0.
                arg.valueA = "seen_via_getInt";
                Assert.assertEquals(0, func.getInt(null));
                arg.valueA = "second_via_getInt";
                Assert.assertEquals(1, func.getInt(null));
                final SymbolTable symbolTableView = func.newSymbolTable();

                // Exercise directory and text-arena growth, then verify that every key still
                // resolves after several rehashes/reallocations. A separately requested symbol
                // table view must follow the live dictionary without owning a native copy.
                for (int i = 2; i < 100; i++) {
                    arg.valueA = "value_" + i;
                    Assert.assertEquals(i, func.getInt(null));
                }
                TestUtils.assertEquals("seen_via_getInt", func.valueOf(0));
                TestUtils.assertEquals("second_via_getInt", func.valueBOf(1));
                TestUtils.assertEquals("seen_via_getInt", symbolTableView.valueOf(0));
                TestUtils.assertEquals("second_via_getInt", symbolTableView.valueBOf(1));
                for (int i = 2; i < 100; i++) {
                    Assert.assertEquals("value_" + i, func.valueOf(i).toString());
                }
                arg.valueA = "seen_via_getInt";
                Assert.assertEquals(0, func.getInt(null));

                // Empty text has a header but no UTF-16 payload. Keep it after multiple rehashes
                // to pin the cached-hash/length/text offsets for this boundary case.
                arg.valueA = "";
                Assert.assertEquals(100, func.getInt(null));
                Assert.assertEquals("", func.valueOf(100).toString());
                Assert.assertEquals("", symbolTableView.valueOf(100).toString());
                Assert.assertEquals(100, func.getInt(null));

                // NULL maps to the null sentinel and resolves back to null.
                arg.valueA = null;
                Assert.assertEquals(SymbolTable.VALUE_IS_NULL, func.getInt(null));
                Assert.assertNull(func.valueOf(SymbolTable.VALUE_IS_NULL));

                // A cached factory may outlive the cursor. Closing the cursor must drop the
                // dictionary, so the next use starts from key 0 even before another init call.
                func.cursorClosed();
                arg.valueA = "after_cursor_close";
                Assert.assertEquals(0, func.getInt(null));
            } finally {
                func.close();
            }
        });
    }

    @Test
    public void testKeyDictionaryIsBoundedByQueryMemoryTracker() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 512L);
            final MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(
                    sqlExecutionContext.getSecurityContext(),
                    1L,
                    MemoryTrackerWorkload.QUERY
            );
            final FeedFunction arg = new FeedFunction();
            final CastStrToSymbolFunctionFactory.Func func = new CastStrToSymbolFunctionFactory.Func(arg);
            sqlExecutionContext.setMemoryTracker(tracker);
            try {
                func.init(null, sqlExecutionContext);
                try {
                    for (int i = 0; i < 10_000; i++) {
                        arg.valueA = "dynamic_symbol_" + i;
                        func.getInt(null);
                    }
                    Assert.fail("expected the dynamic symbol dictionary to reach the query memory limit");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                }
                Assert.assertTrue("dictionary allocation must be charged to the query", tracker.getUsed() > 0);
            } finally {
                try {
                    func.cursorClosed();
                    Assert.assertEquals("cursor close must release the dictionary charge", 0, tracker.getUsed());
                    func.close();
                } finally {
                    sqlExecutionContext.setMemoryTracker(null);
                    tracker.close();
                }
            }
        });
    }

    @Test
    public void testKeyDictionaryReleasesTrackedMemoryAcrossCursorLifecycles() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64 * 1024L);
            final MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(
                    sqlExecutionContext.getSecurityContext(),
                    1L,
                    MemoryTrackerWorkload.QUERY
            );
            final FeedFunction arg = new FeedFunction();
            final CastStrToSymbolFunctionFactory.Func func = new CastStrToSymbolFunctionFactory.Func(arg);
            sqlExecutionContext.setMemoryTracker(tracker);
            try {
                for (int cycle = 0; cycle < 10; cycle++) {
                    func.init(null, sqlExecutionContext);
                    try {
                        for (int i = 0; i < 64; i++) {
                            arg.valueA = "cycle_" + cycle + "_symbol_" + i;
                            Assert.assertEquals(i, func.getInt(null));
                        }
                        Assert.assertTrue("dictionary allocation must be tracked", tracker.getUsed() > 0);
                    } finally {
                        func.cursorClosed();
                    }
                    Assert.assertEquals("cursor close must release every dictionary charge", 0, tracker.getUsed());
                }
            } finally {
                try {
                    func.close();
                    Assert.assertEquals("function close must leave the tracker balanced", 0, tracker.getUsed());
                } finally {
                    sqlExecutionContext.setMemoryTracker(null);
                    tracker.close();
                }
            }
        });
    }

    @Test
    public void testUnusedDictionaryDoesNotAllocateAcrossCursorLifecycles() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64 * 1024L);
            final MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(
                    sqlExecutionContext.getSecurityContext(),
                    1L,
                    MemoryTrackerWorkload.QUERY
            );
            final FeedFunction arg = new FeedFunction();
            arg.valueA = "streamed_a";
            arg.valueB = "streamed_b";
            final CastStrToSymbolFunctionFactory.Func func = new CastStrToSymbolFunctionFactory.Func(arg);
            sqlExecutionContext.setMemoryTracker(tracker);
            try {
                // getSymbol/getSymbolB are pass-throughs and never build the lazy dictionary. Across
                // many init/cursorClosed cycles the dictionary must stay unallocated, so the query's
                // native footprint - measured on this Func's own tracker, which no other thread can
                // perturb - never leaves zero. The check sits after the reads but before cursorClosed,
                // so a getSymbol that regressed to interning would show a non-zero charge here rather
                // than a per-cycle malloc/free that end-of-cycle accounting would hide.
                for (int cycle = 0; cycle < 10_000; cycle++) {
                    func.init(null, sqlExecutionContext);
                    Assert.assertEquals("streamed_a".length(), func.getSymbol(null).length());
                    Assert.assertEquals("streamed_b".length(), func.getSymbolB(null).length());
                    Assert.assertEquals("a getSymbol-only dictionary must not allocate", 0, tracker.getUsed());
                    func.cursorClosed();
                }
            } finally {
                try {
                    func.close();
                    Assert.assertEquals("function close must leave the tracker balanced", 0, tracker.getUsed());
                } finally {
                    sqlExecutionContext.setMemoryTracker(null);
                    tracker.close();
                }
            }
        });
    }

    // valueOf/valueBOf hand out DirectString flyweights over the dictionary's text storage, and a
    // consumer may hold one while the cursor keeps advancing: RecordChain resolves a symbol as
    // getSymbolTable(col).valueOf(getInt(col)) and a sort comparator holds the left-hand value
    // across further reads. Text storage is therefore append-only - growing it must never move or
    // overwrite an entry already handed out. Interning here far past the first chunk forces many
    // growths; a storage that reallocated instead would leave every held value pointing at freed
    // memory, so these assertions read back garbage rather than the original text.
    @Test
    public void testValuesSurviveFurtherInterning() throws Exception {
        assertMemoryLeak(() -> {
            final FeedFunction arg = new FeedFunction();
            final CastStrToSymbolFunctionFactory.Func func = new CastStrToSymbolFunctionFactory.Func(arg);
            try {
                arg.valueA = "held_by_key_0";
                Assert.assertEquals(0, func.getInt(null));
                arg.valueA = "held_by_key_1";
                Assert.assertEquals(1, func.getInt(null));

                // Take the references BEFORE the dictionary grows, through both the function itself
                // and a separately requested view, and keep them live across every later intern.
                final CharSequence heldA = func.valueOf(0);
                final CharSequence heldB = func.valueBOf(1);
                final SymbolTable view = func.newSymbolTable();
                final CharSequence heldView = view.valueOf(0);

                // A long value forces an oversized chunk; the short ones drive the doubling schedule.
                final StringBuilder longValue = new StringBuilder();
                for (int i = 0; i < 4096; i++) {
                    longValue.append('x');
                }
                for (int i = 2; i < 3000; i++) {
                    arg.valueA = i == 1500 ? longValue.toString() : "filler_" + i;
                    Assert.assertEquals(i, func.getInt(null));
                }

                TestUtils.assertEquals("held_by_key_0", heldA);
                TestUtils.assertEquals("held_by_key_1", heldB);
                TestUtils.assertEquals("held_by_key_0", heldView);
                // The dictionary still answers correctly for old and new keys alike.
                TestUtils.assertEquals("held_by_key_0", func.valueOf(0));
                TestUtils.assertEquals("filler_2999", func.valueOf(2999));
                Assert.assertEquals(4096, view.valueOf(1500).length());
            } finally {
                func.close();
            }
        });
    }

    private static void createTables(CairoEngine engine, SqlExecutionContext context) throws SqlException {
        for (String table : new String[]{"x", "y"}) {
            engine.execute("CREATE TABLE " + table + " (ts TIMESTAMP, s STRING, k SYMBOL INDEX, v LONG) TIMESTAMP(ts) PARTITION BY HOUR", context);
            engine.execute(
                    "INSERT INTO " + table + " SELECT (x * 1_000_000L)::timestamp, ((x % 5) * 1_000_000L)::timestamp::string, (x % 2)::symbol, x FROM long_sequence(10)",
                    context
            );
        }
    }

    private void assertParallel(ParallelAssertion assertion) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 2);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 2);
        assertMemoryLeak(() -> TestUtils.execute(
                new TestWorkerPool(4),
                (engine, compiler, context) -> {
                    createTables(engine, context);
                    assertion.run(engine, context);
                },
                configuration,
                LOG
        ));
    }

    private void createTables() throws SqlException {
        createTables(engine, sqlExecutionContext);
    }

    @FunctionalInterface
    private interface ParallelAssertion {
        void run(CairoEngine engine, SqlExecutionContext context) throws Exception;
    }

    private static class FeedFunction extends StrFunction {
        CharSequence valueA;
        CharSequence valueB;

        @Override
        public CharSequence getStrA(Record rec) {
            return valueA;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return valueB;
        }
    }
}
