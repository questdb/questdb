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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.std.MemoryTracker;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Direct function tests register an execution before init(), just as QueryProgress does for SQL.
 * Ending that execution must free the dictionary while the cached function remains open.
 */
public class CastStrToSymbolFunctionFactoryTest extends AbstractCairoTest {

    // A lazy cast-to-symbol only needs a dictionary to answer the integer-key API
    // (getInt/valueOf). getSymbol()/getSymbolB() are pure pass-throughs: the wire
    // serializers, ORDER BY and the non-static GROUP BY key sink all read the column
    // through getSymbol on every row, so interning there is dead work that grows an
    // unnecessary cardinality-dependent dictionary for values no consumer ever looks up
    // by key. This pins the contract: a value seen only through getSymbol consumes no
    // symbol key, so the first value routed through getInt is assigned key 0.
    @Test
    public void testGetSymbolPassThroughAndDictionaryLifecycle() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final FeedFunction arg = new FeedFunction();
            try (CastStrToSymbolFunctionFactory.Func func = new CastStrToSymbolFunctionFactory.Func(arg)) {
                long id = registry.register("string cast lifecycle", sqlExecutionContext);
                final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                try {
                    func.init(null, sqlExecutionContext);
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
                } finally {
                    registry.unregister(id, sqlExecutionContext);
                }
                // Check execution cleanup before init() or close() could hide a retained dictionary.
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertNull(func.valueOf(0));
                id = registry.register("string cast reuse", sqlExecutionContext);
                try {
                    func.init(null, sqlExecutionContext);
                    arg.valueA = "after_execution_close";
                    Assert.assertEquals(0, func.getInt(null));
                } finally {
                    registry.unregister(id, sqlExecutionContext);
                }
            }
        });
    }

    @Test
    public void testKeyDictionaryIsBoundedByQueryMemoryTracker() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 512L);
            final QueryRegistry registry = engine.getQueryRegistry();
            final FeedFunction arg = new FeedFunction();
            try (CastStrToSymbolFunctionFactory.Func func = new CastStrToSymbolFunctionFactory.Func(arg)) {
                long id = registry.register("string cast memory limit", sqlExecutionContext);
                final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
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
                    registry.unregister(id, sqlExecutionContext);
                }
                Assert.assertEquals("execution close must release the dictionary charge", 0, tracker.getUsed());
                Assert.assertNull(func.valueOf(0));

                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64 * 1024L);
                id = registry.register("string cast after memory limit", sqlExecutionContext);
                final MemoryTracker recoveryTracker = sqlExecutionContext.getMemoryTracker();
                try {
                    func.init(null, sqlExecutionContext);
                    arg.valueA = "recovered";
                    Assert.assertEquals(0, func.getInt(null));
                    TestUtils.assertEquals("recovered", func.valueOf(0));
                } finally {
                    registry.unregister(id, sqlExecutionContext);
                }
                Assert.assertEquals(0, recoveryTracker.getUsed());
            }
        });
    }

    @Test
    public void testKeyDictionaryReleasesTrackedMemoryAcrossExecutions() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64 * 1024L);
            final QueryRegistry registry = engine.getQueryRegistry();
            final FeedFunction arg = new FeedFunction();
            try (CastStrToSymbolFunctionFactory.Func func = new CastStrToSymbolFunctionFactory.Func(arg)) {
                for (int cycle = 0; cycle < 10; cycle++) {
                    final long id = registry.register("string cast execution", sqlExecutionContext);
                    final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                    try {
                        func.init(null, sqlExecutionContext);
                        for (int i = 0; i < 64; i++) {
                            arg.valueA = "cycle_" + cycle + "_symbol_" + i;
                            Assert.assertEquals(i, func.getInt(null));
                        }
                        Assert.assertTrue("dictionary allocation must be tracked", tracker.getUsed() > 0);
                    } finally {
                        registry.unregister(id, sqlExecutionContext);
                    }
                    Assert.assertEquals("execution close must release every dictionary charge", 0, tracker.getUsed());
                    Assert.assertNull(func.valueOf(0));
                }
            }
        });
    }

    @Test
    public void testUnusedDictionaryDoesNotAllocateAcrossExecutions() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64 * 1024L);
            final QueryRegistry registry = engine.getQueryRegistry();
            final FeedFunction arg = new FeedFunction();
            arg.valueA = "streamed_a";
            arg.valueB = "streamed_b";
            try (CastStrToSymbolFunctionFactory.Func func = new CastStrToSymbolFunctionFactory.Func(arg)) {
                // Read before ending each execution so cleanup cannot hide an unwanted allocation.
                for (int cycle = 0; cycle < 10_000; cycle++) {
                    final long id = registry.register("string cast pass-through", sqlExecutionContext);
                    final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                    try {
                        func.init(null, sqlExecutionContext);
                        Assert.assertEquals("streamed_a".length(), func.getSymbol(null).length());
                        Assert.assertEquals("streamed_b".length(), func.getSymbolB(null).length());
                        Assert.assertEquals("a getSymbol-only dictionary must not allocate", 0, tracker.getUsed());
                    } finally {
                        registry.unregister(id, sqlExecutionContext);
                    }
                    Assert.assertEquals(0, tracker.getUsed());
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
            final QueryRegistry registry = engine.getQueryRegistry();
            final FeedFunction arg = new FeedFunction();
            try (CastStrToSymbolFunctionFactory.Func func = new CastStrToSymbolFunctionFactory.Func(arg)) {
                final long id = registry.register("string cast growth", sqlExecutionContext);
                final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                try {
                    func.init(null, sqlExecutionContext);
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
                    registry.unregister(id, sqlExecutionContext);
                }
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertNull(func.valueOf(0));
            }
        });
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
