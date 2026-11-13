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

package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.griffin.engine.functions.cast.CastDoubleToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToSymbolFunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;

/**
 * {@code RecordCursor.newSymbolTable()} hands a key-to-value view to a caller that may free it -
 * {@code Misc.freeObjListIfCloseable} over the per-worker tables is the usual shape, and
 * {@code QueryAssertion.testSymbolAPI} does exactly that. Every {@code Cast*ToSymbolFunctionFactory}
 * used to answer with a clone of itself that held, and on close released, the argument the live
 * projection was still reading, so freeing the view closed the projection's own argument.
 * <p>
 * A plain column argument hides the defect, because closing a column function releases nothing. It
 * needed an argument that owns a resource: {@code json_extract} frees its parser state and nulls
 * its JSON pointer on close, after which every read of the column came back NULL.
 */
public class CastToSymbolTableTest extends AbstractCairoTest {

    /**
     * One chain per {@code Cast*ToSymbolFunctionFactory} reachable from {@code json_extract}, which
     * is the argument that makes the ownership bug observable. Only the LONG256 cast is absent - no
     * {@code json_extract} target reaches it.
     */
    private static final String[] CAST_CHAINS = {
            "json_extract(text,'$.a')::double",
            "json_extract(text,'$.a')::double::real",
            "json_extract(text,'$.a')::double::int",
            "json_extract(text,'$.a')::double::short",
            "json_extract(text,'$.a')::long",
            "json_extract(text,'$.a')::double::string",
            "json_extract(text,'$.a')::double::varchar",
            "json_extract(text,'$.a')::date",
            "json_extract(text,'$.a')::timestamp",
            "json_extract(text,'$.b')::boolean",
            "json_extract(text,'$.b')::boolean::byte",
            "json_extract(text,'$.b')::boolean::char",
    };
    /**
     * The casts to SYMBOL that hand out a view over a dictionary, one per source type, over the
     * columns of {@link #createCastSourceTable()}: the column, the text the cast gives its values a,
     * b and c, and the rows to scan. BYTE and SHORT have no NULL, so they skip the NULL row.
     */
    private static final String[][] DICTIONARY_CASTS = {
            {"bt", "1", "2", "3", "4"},
            {"sh", "1", "2", "3", "4"},
            {"ch", "a", "b", "c", "5"},
            {"i", "1", "2", "3", "5"},
            {"f", "1.5", "2.5", "3.5", "5"},
            {"l", "1", "2", "3", "5"},
            // the LONG256 cast renders the lowest 64 bits
            {"l256", "1", "2", "3", "5"},
            {"d", "1.5", "2.5", "3.5", "5"},
            {"dt", "1704067200000", "1704153600000", "1704240000000", "5"},
            {"ts", "1704067200000000", "1704153600000000", "1704240000000000", "5"},
            {"tsn", "1704067200000000000", "1704153600000000000", "1704240000000000000", "5"},
            {"v", "ąść", "", "😀", "5"},
            {"s", "ąść", "", "😀", "5"},
    };
    // The keys the scan of a DICTIONARY_CASTS column mints, row by row.
    private static final int[] SCANNED_KEYS = {0, 1, 0, 2, SymbolTable.VALUE_IS_NULL};
    // Keys no dictionary cast mints over createCastSourceTable(): the NULL key, the other negative
    // keys, one past the last key the scan mints, and the largest key.
    private static final int[] UNMINTED_KEYS = {
            SymbolTable.VALUE_IS_NULL,
            Integer.MIN_VALUE + 1,
            SymbolTable.VALUE_NOT_FOUND,
            -1,
            3,
            Integer.MAX_VALUE
    };

    @Test
    public void testBooleanCastViewAndFunctionAgree() throws Exception {
        // BOOLEAN has no dictionary: getInt() answers 0 for false and 1 for true. Its table and the
        // function resolve those two keys, and every other key - including the NULL key a borrower
        // such as lag() mints - to null, before and after the execution ends.
        assertMemoryLeak(() -> {
            createCastSourceTable();

            try (RecordCursorFactory factory = select("SELECT bo::symbol sy FROM t")) {
                final SymbolTable function;
                final SymbolTable view;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    final int[] expectedKeys = {1, 0, 1, 0, 0};
                    int row = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(expectedKeys[row++], record.getInt(0));
                    }
                    Assert.assertEquals(5, row);
                    function = cursor.getSymbolTable(0);
                    view = cursor.newSymbolTable(0);
                    assertBooleanKeys(function, view);
                }
                assertBooleanKeys(function, view);
            }
        });
    }

    @Test
    public void testCastSymbolColumnsPassSymbolApiCheck() throws Exception {
        // QueryAssertion.testSymbolAPI takes each column's view after a full scan and reads it on one
        // thread while another reads the cast itself, with no key being minted. Every cast's view has
        // to answer like the cast under those concurrent reads.
        assertMemoryLeak(() -> {
            createCastSourceTable();

            assertQuery("""
                    SELECT bo::symbol bo, bt::symbol bt, sh::symbol sh, ch::symbol ch, i::symbol i, f::symbol f,
                        l::symbol l, l256::symbol l256, d::symbol d, dt::symbol dt, ts::symbol ts, tsn::symbol tsn,
                        v::symbol v, s::symbol s
                    FROM t
                    """)
                    .expectSize()
                    .returns("""
                            bo\tbt\tsh\tch\ti\tf\tl\tl256\td\tdt\tts\ttsn\tv\ts
                            true\t1\t1\ta\t1\t1.5\t1\t1\t1.5\t1704067200000\t1704067200000000\t1704067200000000000\tąść\tąść
                            false\t2\t2\tb\t2\t2.5\t2\t2\t2.5\t1704153600000\t1704153600000000\t1704153600000000000\t\t
                            true\t1\t1\ta\t1\t1.5\t1\t1\t1.5\t1704067200000\t1704067200000000\t1704067200000000000\tąść\tąść
                            false\t3\t3\tc\t3\t3.5\t3\t3\t3.5\t1704240000000\t1704240000000000\t1704240000000000000\t😀\t😀
                            false\t0\t0\t\t\t\t\t\t\t\t\t\t\t
                            """);
        });
    }

    @Test
    public void testFreeingTheHandedOutTableLeavesTheColumnReadable() throws Exception {
        // The regression proper. Read the column, take the view the way a worker would, free it,
        // then read the column again. Pre-fix the second read answered NULL on every row, because
        // freeing the view closed the json_extract function underneath the cast.
        assertMemoryLeak(() -> {
            createSourceTable();

            for (String chain : CAST_CHAINS) {
                final String sql = "SELECT id, (" + chain + ")::symbol sy FROM j ORDER BY id";
                try (
                        RecordCursorFactory factory = select(sql);
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    final String before = scan(cursor);

                    final SymbolTable table = cursor.newSymbolTable(1);
                    Assert.assertNotNull(sql, table);
                    Misc.freeIfCloseable(table);

                    cursor.toTop();
                    Assert.assertEquals(sql, before, scan(cursor));

                    // The structural half of the guard, asserted after the behavioural one so a
                    // regression reports as wrong data rather than as a type check: a view that owns
                    // nothing cannot take the projection's argument down with it, whoever frees it.
                    Assert.assertFalse(
                            sql + ": newSymbolTable() handed out a closeable " + table.getClass().getName(),
                            table instanceof Closeable
                    );
                }
            }
        });
    }

    @Test
    public void testGroupByOverABooleanCastKeepsEmptyGroupNull() throws Exception {
        // first(), last() and mode() store VALUE_IS_NULL for a group that saw no rows and resolve
        // it through the cast's table. BOOLEAN has no NULL, so the cast never mints that key
        // itself, and it used to decode it as 'false'.
        assertMemoryLeak(() -> {
            createSourceTable();

            assertQuery("""
                    SELECT
                        first(json_extract(text,'$.b')::boolean::symbol) f,
                        last(json_extract(text,'$.b')::boolean::symbol) l,
                        mode(json_extract(text,'$.b')::boolean::symbol) m
                    FROM j
                    WHERE id > 100
                    """)
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            f\tl\tm
                            \t\t
                            """);
        });
    }

    @Test
    public void testGroupByOverACastSymbolSurvivesTheHandOut() throws Exception {
        // mode() answered newSymbolTable() with itself, which is the same hazard one level up: its
        // values belong to the argument, so freeing what it handed out closed the cast underneath
        // it and the json_extract underneath that. Pre-fix this raised
        // "Cannot invoke SimdJsonParser.queryPointerDouble ... because this.stateA.parser is null"
        // rather than returning wrong rows.
        assertMemoryLeak(() -> {
            createSourceTable();

            assertQuery("SELECT id, mode((json_extract(text,'$.a')::double)::symbol) m FROM j ORDER BY id")
                    .expectSize()
                    .returns("""
                            id\tm
                            1\t1.5
                            2\t2.5
                            3\t1.5
                            """);
        });
    }

    @Test
    public void testHandedOutTableResolvesEveryKeyTheCursorProduces() throws Exception {
        // The view has to answer for the keys the record hands out, or a worker resolving keys off
        // it disagrees with the cursor that produced them.
        assertMemoryLeak(() -> {
            createSourceTable();

            for (String chain : CAST_CHAINS) {
                final String sql = "SELECT (" + chain + ")::symbol sy FROM j ORDER BY id";
                try (
                        RecordCursorFactory factory = select(sql);
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    // Warm the cast's table up first - a key that getInt() has never handed out is
                    // not in it, which is the documented SymbolFunction contract.
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        record.getInt(0);
                    }

                    final SymbolTable table = cursor.newSymbolTable(0);
                    cursor.toTop();
                    while (cursor.hasNext()) {
                        final int key = record.getInt(0);
                        TestUtils.assertEquals(record.getSymA(0), table.valueOf(key));
                        TestUtils.assertEquals(record.getSymA(0), table.valueBOf(key));
                    }
                }
            }
        });
    }

    @Test
    public void testNullKeyResolvesToNull() throws Exception {
        // A consumer that borrows the cast's table - lag()/lead() for a missing neighbor - mints
        // VALUE_IS_NULL on its own, even over a cast from a type that has no NULL. Both the live
        // table and the handed-out view have to resolve that key to NULL, not to a value.
        assertMemoryLeak(() -> {
            createSourceTable();

            for (String chain : CAST_CHAINS) {
                final String sql = "SELECT (" + chain + ")::symbol sy FROM j ORDER BY id";
                try (
                        RecordCursorFactory factory = select(sql);
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        record.getInt(0);
                    }

                    final SymbolTable live = cursor.getSymbolTable(0);
                    Assert.assertNull(chain, live.valueOf(SymbolTable.VALUE_IS_NULL));
                    Assert.assertNull(chain, live.valueBOf(SymbolTable.VALUE_IS_NULL));

                    final SymbolTable view = cursor.newSymbolTable(0);
                    Assert.assertNull(chain, view.valueOf(SymbolTable.VALUE_IS_NULL));
                    Assert.assertNull(chain, view.valueBOf(SymbolTable.VALUE_IS_NULL));
                }
            }
        });
    }

    @Test
    public void testReinitializedCastAndItsViewAgree() throws Exception {
        // A parent may initialize a cast again within one execution, which empties its dictionary and
        // mints keys from 0 again. A view handed out before that answers every key with the value the
        // cast itself answers now: the re-minted value, or null once nothing is minted.
        assertMemoryLeak(() -> {
            final String[] feed = new String[1];
            try (Function cast = new CastStrToSymbolFunctionFactory.Func(new StrFunction() {
                @Override
                public CharSequence getStrA(Record rec) {
                    return feed[0];
                }

                @Override
                public CharSequence getStrB(Record rec) {
                    return feed[0];
                }
            })) {
                assertViewFollowsReinitialization(cast, feed, "x", "y", "z");
            }
            try (Function cast = newCast(new CastVarcharToSymbolFunctionFactory(), new VarcharFunction() {
                @Override
                public Utf8Sequence getVarcharA(Record rec) {
                    return new Utf8String(feed[0]);
                }

                @Override
                public Utf8Sequence getVarcharB(Record rec) {
                    return new Utf8String(feed[0]);
                }
            })) {
                assertViewFollowsReinitialization(cast, feed, "x", "y", "z");
            }
            try (Function cast = newCast(new CastLongToSymbolFunctionFactory(), new LongFunction() {
                @Override
                public long getLong(Record rec) {
                    return Long.parseLong(feed[0]);
                }
            })) {
                assertViewFollowsReinitialization(cast, feed, "1", "2", "3");
            }
            try (Function cast = newCast(new CastDoubleToSymbolFunctionFactory(), new DoubleFunction() {
                @Override
                public double getDouble(Record rec) {
                    return Double.parseDouble(feed[0]);
                }
            })) {
                assertViewFollowsReinitialization(cast, feed, "1.5", "2.5", "3.5");
            }
        });
    }

    @Test
    public void testTheViewReadsNullAfterTheExecutionEnds() throws Exception {
        // The view copies nothing: it reads the cast's dictionary, which the execution releases when
        // it ends. A view held past that point - here, past the factory itself - reads null for every
        // key, as the cast does, rather than a stale copy or freed memory.
        assertMemoryLeak(() -> {
            createSourceTable();

            final SymbolTable table;
            try (RecordCursorFactory factory = select("SELECT (json_extract(text,'$.a')::double)::symbol sy FROM j ORDER BY id")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        record.getInt(0);
                    }
                    table = cursor.newSymbolTable(0);
                    TestUtils.assertEquals("1.5", table.valueOf(0));
                    TestUtils.assertEquals("2.5", table.valueBOf(1));
                }
            }
            Assert.assertNull(table.valueOf(0));
            Assert.assertNull(table.valueBOf(0));
            Assert.assertNull(table.valueOf(1));
            Assert.assertNull(table.valueBOf(1));
            Assert.assertNull(table.valueOf(SymbolTable.VALUE_IS_NULL));
        });
    }

    @Test
    public void testViewFollowsTheCachedFactoryAcrossExecutions() throws Exception {
        // A cached factory keeps its cast between executions, and each execution mints its keys anew.
        // A view taken during one execution answers what the cast answers during the next one, and
        // null once that one ends too.
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            bindVariableService.setLong(0, 0);
            try (RecordCursorFactory factory = select("SELECT (x + $1)::symbol sy FROM long_sequence(3)")) {
                final SymbolTable view;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertScanMintsValues(cursor, "1", "2", "3");
                    view = cursor.newSymbolTable(0);
                    assertViewResolves(view, "1", "2", "3");
                }

                bindVariableService.setLong(0, 10);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertScanMintsValues(cursor, "11", "12", "13");
                    assertViewResolves(cursor.getSymbolTable(0), "11", "12", "13");
                    assertViewResolves(view, "11", "12", "13");
                }
                assertViewResolves(view, null, null, null);
            }
        });
    }

    @Test
    public void testViewOverAnEmptyAndAllNullScan() throws Exception {
        // A scan that mints no key leaves the view nothing to resolve; a single row mints key 0 alone.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE e (v VARCHAR, s STRING, l LONG)");
            execute("CREATE TABLE n AS (SELECT NULL::varchar v, NULL::string s, NULL::long l FROM long_sequence(3))");
            execute("CREATE TABLE o AS (SELECT 'a'::varchar v, 'a'::string s, 7L l FROM long_sequence(1))");

            for (String column : new String[]{"v", "s", "l"}) {
                for (String table : new String[]{"e", "n"}) {
                    try (
                            RecordCursorFactory factory = select("SELECT " + column + "::symbol sy FROM " + table);
                            RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                    ) {
                        final Record record = cursor.getRecord();
                        int rows = 0;
                        while (cursor.hasNext()) {
                            Assert.assertEquals(column, SymbolTable.VALUE_IS_NULL, record.getInt(0));
                            rows++;
                        }
                        Assert.assertEquals(column, "e".equals(table) ? 0 : 3, rows);
                        final SymbolTable view = cursor.newSymbolTable(0);
                        Assert.assertNull(column, view.valueOf(SymbolTable.VALUE_IS_NULL));
                        Assert.assertNull(column, view.valueBOf(SymbolTable.VALUE_IS_NULL));
                        Assert.assertNull(column, view.valueOf(0));
                        Assert.assertNull(column, view.valueBOf(0));
                    }
                }

                final String expected = "l".equals(column) ? "7" : "a";
                try (RecordCursorFactory factory = select("SELECT " + column + "::symbol sy FROM o")) {
                    final SymbolTable view;
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final Record record = cursor.getRecord();
                        Assert.assertTrue(column, cursor.hasNext());
                        Assert.assertEquals(column, 0, record.getInt(0));
                        Assert.assertFalse(column, cursor.hasNext());
                        view = cursor.newSymbolTable(0);
                        TestUtils.assertEquals(column, expected, view.valueOf(0));
                        TestUtils.assertEquals(column, expected, view.valueBOf(0));
                        Assert.assertNull(column, view.valueOf(1));
                        Assert.assertNull(column, view.valueBOf(1));
                    }
                    Assert.assertNull(column, view.valueOf(0));
                    Assert.assertNull(column, view.valueBOf(0));
                }
            }
        });
    }

    @Test
    public void testViewReadsNullAfterMemoryLimitBreach() throws Exception {
        // A scan that breaches the query memory limit ends the execution, which releases the cast's
        // dictionary. Views taken before the scan and part-way through it then read null for every
        // key, and the cached factory runs again once the limit is lifted.
        assertMemoryLeak(() -> {
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            try (RecordCursorFactory factory = select("SELECT x::symbol sy FROM long_sequence(10_000)")) {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4096L);
                final ObjList<SymbolTable> views = new ObjList<>();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    views.add(cursor.newSymbolTable(0));
                    final Record record = cursor.getRecord();
                    int rows = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rows, record.getInt(0));
                        if (++rows == 8) {
                            final SymbolTable view = cursor.newSymbolTable(0);
                            TestUtils.assertEquals("1", view.valueOf(0));
                            TestUtils.assertEquals("8", view.valueBOf(7));
                            views.add(view);
                        }
                    }
                    Assert.fail("10,000 distinct values must exceed 4 KiB");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                }
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                Assert.assertEquals("the scan must mint 8 keys before the breach", 2, views.size());
                for (int i = 0, n = views.size(); i < n; i++) {
                    final SymbolTable view = views.getQuick(i);
                    for (int key = 0; key < 8; key++) {
                        Assert.assertNull(view.valueOf(key));
                        Assert.assertNull(view.valueBOf(key));
                    }
                }

                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    int rows = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rows, record.getInt(0));
                        rows++;
                    }
                    Assert.assertEquals(10_000, rows);
                }
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
            }
        });
    }

    @Test
    public void testViewResolvesBoundaryKeys() throws Exception {
        // The view resolves every key the way the cast's own valueOf() does: each key the scan minted
        // to its text, empty text included, and the NULL key, every other negative key and every key
        // past the last minted one to null.
        assertMemoryLeak(() -> {
            createCastSourceTable();

            for (String[] cast : DICTIONARY_CASTS) {
                final String column = cast[0];
                try (
                        RecordCursorFactory factory = select(castSql(cast));
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    assertScanMintsKeys(column, cursor, Integer.parseInt(cast[4]));
                    final SymbolTable function = cursor.getSymbolTable(0);
                    final SymbolTable view = cursor.newSymbolTable(0);
                    for (int key = 0; key < 3; key++) {
                        final String expected = cast[key + 1];
                        TestUtils.assertEquals(column, expected, function.valueOf(key));
                        TestUtils.assertEquals(column, expected, view.valueOf(key));
                        TestUtils.assertEquals(column, expected, view.valueBOf(key));
                    }
                    // the A and B results are separate flyweights, so neither overwrites the other
                    final CharSequence valueA = view.valueOf(0);
                    final CharSequence valueB = view.valueBOf(2);
                    TestUtils.assertEquals(column, cast[1], valueA);
                    TestUtils.assertEquals(column, cast[3], valueB);
                    for (int key : UNMINTED_KEYS) {
                        Assert.assertNull(column + " key " + key, function.valueOf(key));
                        Assert.assertNull(column + " key " + key, view.valueOf(key));
                        Assert.assertNull(column + " key " + key, view.valueBOf(key));
                    }
                }
            }
        });
    }

    @Test
    public void testViewTakenAfterTheScanCopiesNothing() throws Exception {
        // A view taken after the scan finds every key minted already, and still copies none of them.
        // Once the execution ends and releases the cast's dictionary, every key reads null, as the cast
        // itself answers.
        assertMemoryLeak(() -> {
            createCastSourceTable();

            for (String[] cast : DICTIONARY_CASTS) {
                final String column = cast[0];
                try (RecordCursorFactory factory = select(castSql(cast))) {
                    final SymbolTable function;
                    final SymbolTable view;
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        assertScanMintsKeys(column, cursor, Integer.parseInt(cast[4]));
                        function = cursor.getSymbolTable(0);
                        view = cursor.newSymbolTable(0);
                        for (int key = 0; key < 3; key++) {
                            TestUtils.assertEquals(column, cast[key + 1], view.valueOf(key));
                            TestUtils.assertEquals(column, cast[key + 1], view.valueBOf(key));
                        }
                    }
                    for (int key = 0; key < 3; key++) {
                        Assert.assertNull(column, function.valueOf(key));
                        Assert.assertNull(column, view.valueOf(key));
                        Assert.assertNull(column, view.valueBOf(key));
                    }
                }
            }
        });
    }

    @Test
    public void testViewTakenBeforeGrowthResolvesEveryKey() throws Exception {
        // The view is taken after the first key. The scan then mints 9,999 more, which moves the
        // descriptor block, fills many text chunks and rehashes. The view resolves every key, the text
        // it handed out before the growth stays valid, and it all reads null once the execution ends.
        assertMemoryLeak(() -> {
            for (String cast : new String[]{"x::symbol", "x::varchar::symbol", "x::string::symbol"}) {
                try (RecordCursorFactory factory = select("SELECT " + cast + " sy FROM long_sequence(10_000)")) {
                    final SymbolTable view;
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final Record record = cursor.getRecord();
                        Assert.assertTrue(cast, cursor.hasNext());
                        Assert.assertEquals(cast, 0, record.getInt(0));
                        view = cursor.newSymbolTable(0);
                        final CharSequence held = view.valueOf(0);
                        TestUtils.assertEquals(cast, "1", held);

                        int key = 1;
                        while (cursor.hasNext()) {
                            Assert.assertEquals(cast, key++, record.getInt(0));
                        }
                        Assert.assertEquals(cast, 10_000, key);
                        TestUtils.assertEquals(cast, "1", held);

                        final SymbolTable function = cursor.getSymbolTable(0);
                        for (int k = 0; k < 10_000; k++) {
                            final String expected = Integer.toString(k + 1);
                            TestUtils.assertEquals(cast, expected, function.valueOf(k));
                            TestUtils.assertEquals(cast, expected, view.valueOf(k));
                            TestUtils.assertEquals(cast, expected, view.valueBOf(k));
                        }
                    }
                    for (int k = 0; k < 10_000; k++) {
                        Assert.assertNull(cast, view.valueOf(k));
                        Assert.assertNull(cast, view.valueBOf(k));
                    }
                }
            }
        });
    }

    @Test
    public void testViewTakenBeforeTheScanFollowsTheLiveDictionary() throws Exception {
        // lag()/lead() take the view at init, before the scan mints any key, and resolve every key
        // through it. A key minted after the hand-out resolves through the cast's live native
        // dictionary; once QueryProgress ends the execution and frees that dictionary, it must read
        // as null rather than as freed memory.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT ('v' || (x % 7))::varchar v, x % 7 l, (x % 7)::int i, (x % 7) / 2.0 d FROM long_sequence(50))");
            for (String column : new String[]{"v", "l", "i", "d"}) {
                try (RecordCursorFactory factory = select("SELECT " + column + "::symbol sy FROM t")) {
                    final SymbolTable table;
                    int maxKey = -1;
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        table = cursor.newSymbolTable(0);
                        final Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            final int key = record.getInt(0);
                            maxKey = Math.max(maxKey, key);
                            TestUtils.assertEquals(column, record.getSymA(0), table.valueOf(key));
                            TestUtils.assertEquals(column, record.getSymB(0), table.valueBOf(key));
                        }
                    }
                    Assert.assertEquals(column, 6, maxKey);
                    for (int key = 0; key <= maxKey; key++) {
                        Assert.assertNull(column, table.valueOf(key));
                        Assert.assertNull(column, table.valueBOf(key));
                    }
                }
            }
        });
    }

    private static void assertBooleanKeys(SymbolTable function, SymbolTable view) {
        for (SymbolTable table : new SymbolTable[]{function, view}) {
            TestUtils.assertEquals("false", table.valueOf(0));
            TestUtils.assertEquals("false", table.valueBOf(0));
            TestUtils.assertEquals("true", table.valueOf(1));
            TestUtils.assertEquals("true", table.valueBOf(1));
            for (int key : new int[]{SymbolTable.VALUE_IS_NULL, Integer.MIN_VALUE + 1, SymbolTable.VALUE_NOT_FOUND, -1, 2, Integer.MAX_VALUE}) {
                Assert.assertNull("key " + key, table.valueOf(key));
                Assert.assertNull("key " + key, table.valueBOf(key));
            }
        }
    }

    private static void assertScanMintsKeys(String column, RecordCursor cursor, int rowCount) {
        final Record record = cursor.getRecord();
        int row = 0;
        while (cursor.hasNext()) {
            Assert.assertEquals(column, SCANNED_KEYS[row++], record.getInt(0));
        }
        Assert.assertEquals(column, rowCount, row);
    }

    // Scans a cursor whose rows mint the keys 0, 1, 2... with the given text.
    private static void assertScanMintsValues(RecordCursor cursor, String... values) {
        final Record record = cursor.getRecord();
        int row = 0;
        while (cursor.hasNext()) {
            Assert.assertEquals(row, record.getInt(0));
            TestUtils.assertEquals(values[row], record.getSymA(0));
            row++;
        }
        Assert.assertEquals(values.length, row);
    }

    private static void assertViewFollowsReinitialization(
            Function cast,
            String[] feed,
            String first,
            String second,
            String third
    ) throws SqlException {
        final SymbolFunction function = (SymbolFunction) cast;
        function.init(null, sqlExecutionContext);
        feed[0] = first;
        Assert.assertEquals(0, function.getInt(null));
        final SymbolTable view = function.newSymbolTable();
        Assert.assertNotNull(view);
        TestUtils.assertEquals(first, view.valueOf(0));

        function.init(null, sqlExecutionContext);
        feed[0] = second;
        Assert.assertEquals(0, function.getInt(null));
        feed[0] = third;
        Assert.assertEquals(1, function.getInt(null));
        TestUtils.assertEquals(second, function.valueOf(0));
        TestUtils.assertEquals(second, view.valueOf(0));
        TestUtils.assertEquals(second, view.valueBOf(0));
        TestUtils.assertEquals(third, function.valueBOf(1));
        TestUtils.assertEquals(third, view.valueOf(1));
        TestUtils.assertEquals(third, view.valueBOf(1));

        function.init(null, sqlExecutionContext);
        for (int key = 0; key < 2; key++) {
            Assert.assertNull(function.valueOf(key));
            Assert.assertNull(view.valueOf(key));
            Assert.assertNull(view.valueBOf(key));
        }
    }

    // Asserts the text that the keys 0, 1, 2... resolve to.
    private static void assertViewResolves(SymbolTable table, String... values) {
        for (int key = 0; key < values.length; key++) {
            TestUtils.assertEquals(values[key], table.valueOf(key));
            TestUtils.assertEquals(values[key], table.valueBOf(key));
        }
    }

    private static String castSql(String[] cast) {
        return "SELECT " + cast[0] + "::symbol sy FROM t WHERE id <= " + cast[4];
    }

    private static Function newCast(FunctionFactory factory, Function arg) throws SqlException {
        final ObjList<Function> args = new ObjList<>();
        args.add(arg);
        final IntList positions = new IntList();
        positions.add(0);
        return factory.newInstance(0, args, positions, configuration, sqlExecutionContext);
    }

    private static String scan(RecordCursor cursor) {
        final Record record = cursor.getRecord();
        final StringBuilder sink = new StringBuilder();
        while (cursor.hasNext()) {
            sink.append(record.getInt(0)).append('=').append(record.getSymA(1)).append('\n');
        }
        return sink.toString();
    }

    private void createCastSourceTable() throws Exception {
        // Rows 1 to 4 hold the values a, b, a and c of each type, row 5 NULL wherever the type has it.
        execute("""
                CREATE TABLE t (
                    id INT, bo BOOLEAN, bt BYTE, sh SHORT, ch CHAR, i INT, f FLOAT, l LONG, l256 LONG256,
                    d DOUBLE, dt DATE, ts TIMESTAMP, tsn TIMESTAMP_NS, v VARCHAR, s STRING
                )""");
        execute("""
                INSERT INTO t VALUES
                    (1, true, 1, 1, 'a', 1, 1.5, 1, 0x01, 1.5, '2024-01-01', '2024-01-01', '2024-01-01', 'ąść', 'ąść'),
                    (2, false, 2, 2, 'b', 2, 2.5, 2, 0x02, 2.5, '2024-01-02', '2024-01-02', '2024-01-02', '', ''),
                    (3, true, 1, 1, 'a', 1, 1.5, 1, 0x01, 1.5, '2024-01-01', '2024-01-01', '2024-01-01', 'ąść', 'ąść'),
                    (4, false, 3, 3, 'c', 3, 3.5, 3, 0x03, 3.5, '2024-01-03', '2024-01-03', '2024-01-03', '😀', '😀'),
                    (5, false, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)""");
    }

    private void createSourceTable() throws Exception {
        execute("CREATE TABLE j (id INT, text VARCHAR)");
        execute("""
                INSERT INTO j VALUES
                  (1, '{"a":1.5,"b":true}'),
                  (2, '{"a":2.5,"b":false}'),
                  (3, '{"a":1.5,"b":true}')""");
    }
}
