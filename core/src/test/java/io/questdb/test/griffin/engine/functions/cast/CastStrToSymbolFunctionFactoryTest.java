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
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CastStrToSymbolFunctionFactoryTest extends AbstractCairoTest {

    // A lazy cast-to-symbol only needs a dictionary to answer the integer-key API
    // (getInt/valueOf). getSymbol()/getSymbolB() are pure pass-throughs: the wire
    // serializers, ORDER BY and the non-static GROUP BY key sink all read the column
    // through getSymbol on every row, so interning there is dead work that grows an
    // unnecessary cardinality-dependent dictionary for values no consumer ever looks up
    // by key. This pins the contract: a value seen only through getSymbol consumes no
    // symbol key, so the first value routed through getInt is assigned key 0.
    @Test
    public void testGetSymbolIsPassThroughAndConsumesNoSymbolKey() {
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
