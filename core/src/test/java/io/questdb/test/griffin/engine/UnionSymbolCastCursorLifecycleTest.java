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

package io.questdb.test.griffin.engine;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.columns.StrColumn;
import io.questdb.griffin.engine.union.UnionSymbolCastRecordCursorFactory;
import io.questdb.std.MemoryTracker;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class UnionSymbolCastCursorLifecycleTest extends AbstractUnionSymbolCastTest {
    private static final StaticSymbolTable SYMBOL_TABLE = new StaticSymbolTable() {
        @Override
        public boolean containsNullValue() {
            return true;
        }

        @Override
        public int getSymbolCount() {
            return 2;
        }

        @Override
        public int keyOf(CharSequence value) {
            if (value == null) {
                return VALUE_IS_NULL;
            }
            if ("alpha".contentEquals(value)) {
                return 100;
            }
            if ("beta".contentEquals(value)) {
                return 200;
            }
            return VALUE_NOT_FOUND;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return switch (key) {
                case 100 -> "alpha";
                case 200 -> "beta";
                default -> null;
            };
        }
    };

    @Test
    public void testCursorReuseWithoutCloseResolvesAgainstRebuiltDictionary() throws Exception {
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireTracker();
            final StaticSymbolCursorFactory base = new StaticSymbolCursorFactory(SYMBOL_TABLE, new String[][]{{"alpha"}});
            final TrackingSymbolFunction function = new TrackingSymbolFunction(new StrColumn(0));
            final ObjList<Function> functions = functions(function);
            try {
                try (UnionSymbolCastRecordCursorFactory factory = newSymbolProjection(base, functions)) {
                    // Prime the per-source cache: intern "alpha" and cache its source-key -> result-key
                    // translation.
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                    Assert.assertTrue(cursor.hasNext());
                    final int firstKey = cursor.getRecord().getInt(0);
                    TestUtils.assertEquals("alpha", cursor.getSymbolTable(0).valueOf(firstKey));

                    // Re-acquire the cursor WITHOUT closing it. getCursor() re-runs Function.init(),
                    // which empties the re-symbolising dictionary; the per-source key cache must be
                    // dropped in lockstep. Otherwise the stale cache serves a result key that no longer
                    // exists in the rebuilt dictionary, so valueOf() reads back null. Same cursor
                    // instance is reused, so the enclosing factory owns the single close.
                    cursor = factory.getCursor(sqlExecutionContext);
                    Assert.assertTrue(cursor.hasNext());
                    final int secondKey = cursor.getRecord().getInt(0);
                    final CharSequence resolved = cursor.getSymbolTable(0).valueOf(secondKey);
                    Assert.assertNotNull("reused cursor must rebuild the dictionary, not read a stale cached key", resolved);
                    TestUtils.assertEquals("alpha", resolved);
                }
                Assert.assertEquals("all query-tracked native state must be released", 0, tracker.getUsed());
            } finally {
                releaseTracker(tracker);
            }
        });
    }

    @Test
    public void testFactoryCloseReleasesCursorOwnedState() throws Exception {
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireTracker();
            final StaticSymbolCursorFactory base = new StaticSymbolCursorFactory(SYMBOL_TABLE, new String[][]{{"alpha"}, {"beta"}});
            final TrackingSymbolFunction function = new TrackingSymbolFunction(new StrColumn(0));
            final ObjList<Function> functions = functions(function);
            try {
                final UnionSymbolCastRecordCursorFactory factory = newSymbolProjection(base, functions);
                // Read a key so the cursor builds the per-source native key map it owns, then close
                // only the factory. A factory owns its cursor, so closing it has to release
                // everything the cursor holds - the cursor's own close() is not the only path.
                final RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, cursor.getRecord().getInt(0));
                Assert.assertTrue(tracker.getUsed() > 0);

                factory.close();
                Assert.assertEquals("closing the factory must release the cursor's native state", 0, tracker.getUsed());
                Assert.assertEquals(1, base.cursor.closeCount);
                Assert.assertEquals(1, function.cursorClosedCount);
            } finally {
                releaseTracker(tracker);
            }
        });
    }

    @Test
    public void testFunctionInitFailureClosesBaseCursorAndFunctions() throws Exception {
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireTracker();
            final StaticSymbolCursorFactory base = new StaticSymbolCursorFactory(SYMBOL_TABLE, new String[][]{{"alpha", "beta"}});
            final TrackingSymbolFunction functionA = new TrackingSymbolFunction(new StrColumn(0));
            final TrackingSymbolFunction functionB = new FailingInitSymbolFunction(new StrColumn(1));
            final ObjList<Function> functions = functions(functionA, functionB);
            try (UnionSymbolCastRecordCursorFactory factory = newSymbolProjection(base, functions)) {
                try {
                    factory.getCursor(sqlExecutionContext);
                    Assert.fail("expected injected function initialization failure");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "injected function initialization failure");
                }
                assertCursorClosed(base, tracker, functionA, functionB);
            } finally {
                releaseTracker(tracker);
            }
        });
    }

    @Test
    public void testNativeKeyCacheBreachesQueryMemoryLimit() throws Exception {
        assertMemoryLeak(() -> {
            // A tight per-query limit. The integrated native-key path charges the tracker for BOTH
            // the re-symbolising dictionary and the per-source native key cache, so interning enough
            // distinct source keys must breach the limit and surface the per-query out-of-memory
            // error rather than growing unbounded. Cursor close must then leave the tracker balanced
            // even though the failure landed mid-iteration. 512 bytes trips only after the cache has
            // opened and rehashed at least once (its first rehash is on the 4th distinct key).
            final MemoryTracker tracker = acquireTracker(512L);
            final int distinctCount = 1000;
            final StaticSymbolTable table = denseKeySymbolTable(distinctCount);
            final String[][] rows = new String[distinctCount][];
            for (int i = 0; i < distinctCount; i++) {
                rows[i] = new String[]{"v" + i};
            }
            final StaticSymbolCursorFactory base = new StaticSymbolCursorFactory(table, rows);
            final TrackingSymbolFunction function = new TrackingSymbolFunction(new StrColumn(0));
            final ObjList<Function> functions = functions(function);
            try (UnionSymbolCastRecordCursorFactory factory = newSymbolProjection(base, functions)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    try {
                        while (cursor.hasNext()) {
                            record.getInt(0);
                        }
                        Assert.fail("expected the native key path to reach the query memory limit");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                    }
                    Assert.assertTrue("the native key path must charge the tracker before the breach", tracker.getUsed() > 0);
                }
                // Close releases the dictionary and the per-source key caches even after the
                // mid-iteration breach, so the query-tracked balance returns to zero.
                assertCursorClosed(base, tracker, function);
            } finally {
                releaseTracker(tracker);
            }
        });
    }

    @Test
    public void testNativeKeyCacheRehashesUnderTrackerAndReleases() throws Exception {
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireTracker();
            // The per-source native key cache (a DirectIntIntHashMap) starts at capacity 8 / free 4,
            // so it only rehashes once a single leg feeds more than four distinct native source keys
            // through getInt. Drive well past that (40 distinct keys => four rehashes), then a repeat
            // pass, to exercise the cache's tracker-charged growth and its release at cursor close -
            // the path the smaller native-key tests (<= 2 keys/leg) never reach.
            final int distinctCount = 40;
            final StaticSymbolTable table = denseKeySymbolTable(distinctCount);
            final String[][] rows = new String[distinctCount * 2][];
            for (int i = 0; i < distinctCount; i++) {
                rows[i] = new String[]{"v" + i};               // first sight: intern + cache write (grows the map)
                rows[distinctCount + i] = new String[]{"v" + i}; // repeat: resolves from the rehashed cache
            }
            final StaticSymbolCursorFactory base = new StaticSymbolCursorFactory(table, rows);
            final TrackingSymbolFunction function = new TrackingSymbolFunction(new StrColumn(0));
            final ObjList<Function> functions = functions(function);
            try (UnionSymbolCastRecordCursorFactory factory = newSymbolProjection(base, functions)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    final SymbolTable symbolTable = cursor.getSymbolTable(0);
                    final int[] keyByRow = new int[rows.length];
                    for (int i = 0; i < rows.length; i++) {
                        Assert.assertTrue(cursor.hasNext());
                        keyByRow[i] = record.getInt(0);
                    }
                    Assert.assertFalse(cursor.hasNext());

                    for (int i = 0; i < distinctCount; i++) {
                        // The repeat row resolves to the same merged key from the grown cache...
                        Assert.assertEquals(keyByRow[i], keyByRow[distinctCount + i]);
                        // ...and every key still resolves to the right text after the rehashes. Distinct
                        // text per key also proves the keys are pairwise distinct.
                        TestUtils.assertEquals("v" + i, symbolTable.valueOf(keyByRow[i]));
                    }
                    // Each distinct source value interned exactly once despite the repeat pass.
                    Assert.assertEquals(distinctCount, function.internCount);
                    // The source dictionary resolved each distinct native key once, not once per row.
                    Assert.assertEquals(1, base.cursor.symbolTableLookupCount);
                    Assert.assertTrue(tracker.getUsed() > 0);
                }
                assertCursorClosed(base, tracker, function);
            } finally {
                releaseTracker(tracker);
            }
        });
    }

    @Test
    public void testNativeKeyCacheTranslatesEachSourceKeyOnce() throws Exception {
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireTracker();
            // A static source dictionary hands the same native key back on repeated rows: alpha
            // (source key 100) appears three times, beta (200) once. The projection caches the
            // source-key -> result-key translation per source, so it must intern each distinct
            // source value exactly once and serve the rest from the cache.
            final StaticSymbolCursorFactory base = new StaticSymbolCursorFactory(SYMBOL_TABLE, new String[][]{
                    {"alpha"},
                    {"alpha"},
                    {"beta"},
                    {"alpha"}
            });
            final TrackingSymbolFunction function = new TrackingSymbolFunction(new StrColumn(0));
            final ObjList<Function> functions = functions(function);
            try (UnionSymbolCastRecordCursorFactory factory = newSymbolProjection(base, functions)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    final SymbolTable symbolTable = cursor.getSymbolTable(0);

                    Assert.assertTrue(cursor.hasNext());
                    final int alphaKey = record.getInt(0);
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals("a repeated source key must resolve to the cached result key", alphaKey, record.getInt(0));
                    Assert.assertTrue(cursor.hasNext());
                    final int betaKey = record.getInt(0);
                    Assert.assertNotEquals(alphaKey, betaKey);
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(alphaKey, record.getInt(0));
                    Assert.assertFalse(cursor.hasNext());

                    TestUtils.assertEquals("alpha", symbolTable.valueOf(alphaKey));
                    TestUtils.assertEquals("beta", symbolTable.valueOf(betaKey));
                    // The cache interns each distinct source value once; without it every row would
                    // re-intern, so this count is what pins the cache write.
                    Assert.assertEquals(2, function.internCount);
                    // The source dictionary resolved each distinct native key once, not once per row.
                    Assert.assertEquals(1, base.cursor.symbolTableLookupCount);
                }
                assertCursorClosed(base, tracker, function);
            } finally {
                releaseTracker(tracker);
            }
        });
    }

    @Test
    public void testPartialSourceStateFailureClosesCursorAndFunctions() throws Exception {
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireTracker();
            final StaticSymbolCursorFactory base = new StaticSymbolCursorFactory(SYMBOL_TABLE, new String[][]{{"alpha", "beta"}});
            base.cursor.symbolTableFailureColumn = 1;
            final TrackingSymbolFunction functionA = new TrackingSymbolFunction(new StrColumn(0));
            final TrackingSymbolFunction functionB = new TrackingSymbolFunction(new StrColumn(1));
            final ObjList<Function> functions = functions(functionA, functionB);
            try (UnionSymbolCastRecordCursorFactory factory = newSymbolProjection(base, functions)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    try {
                        cursor.getRecord().getInt(0);
                        Assert.fail("expected injected source state construction failure");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "injected source state construction failure");
                    }
                }
                Assert.assertEquals(2, base.cursor.symbolTableLookupCount);
                assertCursorClosed(base, tracker, functionA, functionB);
            } finally {
                releaseTracker(tracker);
            }
        });
    }

    @Test
    public void testSourceStateRegistrationFailureIsCleanAndRetryable() throws Exception {
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireTracker();
            final StaticSymbolCursorFactory base = new StaticSymbolCursorFactory(SYMBOL_TABLE, new String[][]{{"alpha"}});
            final TrackingSymbolFunction function = new TrackingSymbolFunction(new StrColumn(0));
            final ObjList<Function> functions = functions(function);
            try (UnionSymbolCastRecordCursorFactory factory = newSymbolProjection(base, functions)) {
                final int[] registrationCount = {0};
                factory.setCursorTestHook(() -> {
                    if (registrationCount[0]++ == 0) {
                        throw CairoException.nonCritical().put("injected source state registration failure");
                    }
                });
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    final Record record = cursor.getRecord();
                    try {
                        record.getInt(0);
                        Assert.fail("expected injected source state registration failure");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "injected source state registration failure");
                    }

                    final int key = record.getInt(0);
                    Assert.assertEquals(0, key);
                    TestUtils.assertEquals("alpha", cursor.getSymbolTable(0).valueOf(key));
                    Assert.assertTrue(tracker.getUsed() > 0);
                }
                Assert.assertEquals(2, registrationCount[0]);
                assertCursorClosed(base, tracker, function);
            } finally {
                releaseTracker(tracker);
            }
        });
    }

    @Test
    public void testTextReadsBypassTheSymbolFunction() throws Exception {
        assertMemoryLeak(() -> {
            final StaticSymbolCursorFactory base = new StaticSymbolCursorFactory(SYMBOL_TABLE, new String[][]{{"alpha", "beta"}});
            final TrackingSymbolFunction functionA = new TrackingSymbolFunction(new StrColumn(0));
            final TrackingSymbolFunction functionB = new TrackingSymbolFunction(new StrColumn(1));
            final ObjList<Function> functions = functions(functionA, functionB);
            try (UnionSymbolCastRecordCursorFactory factory = newSymbolProjection(base, functions)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    Assert.assertTrue(cursor.hasNext());
                    TestUtils.assertEquals("alpha", record.getSymA(0));
                    TestUtils.assertEquals("beta", record.getSymB(1));
                    // getSymB must read the record's B slot, not its A slot, so that a caller can
                    // hold both flyweights at once.
                    Assert.assertNotSame(record.getSymA(1), record.getSymB(1));
                    Assert.assertFalse(cursor.hasNext());
                }
                // Every re-symbolised column projects CastStrToSymbol(StrColumn(col)), so routing a
                // text read through the function only lands back on the union record's own
                // getStrA/getStrB for the same column. The projection must read the record directly.
                Assert.assertEquals(0, functionA.symbolCallCount);
                Assert.assertEquals(0, functionB.symbolCallCount);
                // The source symbol tables answer keys, not text, so a text read must not reach them.
                Assert.assertEquals(0, base.cursor.symbolTableLookupCount);
            }
        });
    }

    @Test
    public void testUnsupportedSourceSymbolTableFallsBackToTextKeys() throws Exception {
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireTracker();
            final StaticSymbolCursorFactory base = new StaticSymbolCursorFactory(SYMBOL_TABLE, new String[][]{
                    {"alpha"},
                    {null},
                    {"beta"},
                    {"alpha"}
            });
            base.cursor.isSymbolTableUnsupported = true;
            base.cursor.isNativeKeyAccessForbidden = true;
            final TrackingSymbolFunction function = new TrackingSymbolFunction(new StrColumn(0));
            final ObjList<Function> functions = functions(function);
            try (UnionSymbolCastRecordCursorFactory factory = newSymbolProjection(base, functions)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    final SymbolTable symbolTable = cursor.getSymbolTable(0);
                    final int[] expectedKeys = {0, SymbolTable.VALUE_IS_NULL, 1, 0};
                    final String[] expectedValues = {"alpha", null, "beta", "alpha"};
                    for (int i = 0; i < expectedKeys.length; i++) {
                        Assert.assertTrue(cursor.hasNext());
                        TestUtils.assertEquals(expectedValues[i], record.getSymA(0));
                        TestUtils.assertEquals(expectedValues[i], record.getSymB(0));
                        final int key = record.getInt(0);
                        Assert.assertEquals(expectedKeys[i], key);
                        TestUtils.assertEquals(expectedValues[i], symbolTable.valueOf(key));
                        TestUtils.assertEquals(expectedValues[i], symbolTable.valueBOf(key));
                    }
                    Assert.assertFalse(cursor.hasNext());
                    Assert.assertTrue(tracker.getUsed() > 0);
                }
                Assert.assertEquals(1, base.cursor.symbolTableLookupCount);
                assertCursorClosed(base, tracker, function);
            } finally {
                releaseTracker(tracker);
            }
        });
    }

    private static void assertCursorClosed(
            StaticSymbolCursorFactory base,
            MemoryTracker tracker,
            TrackingSymbolFunction... functions
    ) {
        Assert.assertEquals(1, base.cursor.closeCount);
        for (int i = 0; i < functions.length; i++) {
            Assert.assertEquals(1, functions[i].cursorClosedCount);
        }
        Assert.assertEquals(0, tracker.getUsed());
    }

    /**
     * A static source dictionary of {@code count} distinct symbols "v0".."v(count-1)", each with a
     * distinct native key, so a single leg can feed the per-source native key cache enough distinct
     * keys to force it to grow and rehash. keyOf recovers the key from the text without a lookup
     * table, keeping an arbitrarily large distinct key space cheap to build.
     */
    private static StaticSymbolTable denseKeySymbolTable(int count) {
        return new StaticSymbolTable() {
            @Override
            public boolean containsNullValue() {
                return false;
            }

            @Override
            public int getSymbolCount() {
                return count;
            }

            @Override
            public int keyOf(CharSequence value) {
                if (value == null) {
                    return VALUE_IS_NULL;
                }
                if (value.length() < 2 || value.charAt(0) != 'v') {
                    return VALUE_NOT_FOUND;
                }
                int key = 0;
                for (int i = 1, n = value.length(); i < n; i++) {
                    final char c = value.charAt(i);
                    if (c < '0' || c > '9') {
                        return VALUE_NOT_FOUND;
                    }
                    key = key * 10 + (c - '0');
                }
                return key < count ? key : VALUE_NOT_FOUND;
            }

            @Override
            public CharSequence valueBOf(int key) {
                return valueOf(key);
            }

            @Override
            public CharSequence valueOf(int key) {
                return key >= 0 && key < count ? "v" + key : null;
            }
        };
    }

    private static class FailingInitSymbolFunction extends TrackingSymbolFunction {
        private FailingInitSymbolFunction(Function arg) {
            super(arg);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            intern("init-allocation");
            throw SqlException.$(0, "injected function initialization failure");
        }
    }

    private static class TrackingSymbolFunction extends CastStrToSymbolFunctionFactory.Func {
        private int cursorClosedCount;
        private int internCount;
        private int symbolCallCount;

        private TrackingSymbolFunction(Function arg) {
            super(arg);
        }

        @Override
        public void cursorClosed() {
            try {
                super.cursorClosed();
            } finally {
                cursorClosedCount++;
            }
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            symbolCallCount++;
            return super.getSymbol(rec);
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            symbolCallCount++;
            return super.getSymbolB(rec);
        }

        @Override
        public int intern(CharSequence value) {
            internCount++;
            return super.intern(value);
        }
    }
}
