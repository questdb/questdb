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

package io.questdb.test.griffin.engine.functions;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Pins which symbol functions keep the integer-key egress path.
 * <p>
 * {@code QwpResultBatchBuffer.beginBatch} only reads a column by native key - shipping each
 * distinct value once - when its symbol table answers {@code supportsKeyValueAccess()}. Everything
 * else falls back to {@code getSymA()} plus a full UTF-8 re-encode of the value on every row. The
 * functions below each hold a fixed dictionary built once per cursor, so {@code getInt()} returns
 * an index and {@code valueOf()} resolves it without touching text: dropping the opt-in on any of
 * them turns an O(1)-per-row egress into an O(length)-per-row one while every result assertion in
 * the suite stays green.
 */
public class SymbolFunctionKeyValueAccessTest extends AbstractCairoTest {
    private static final List<String> FLOAT_GROUPS = Arrays.asList("1.5", "8.25");

    @Test
    public void testFixedDictionarySymbolFunctionsOptIntoKeyValueAccess() throws Exception {
        assertKeyValueAccess("rnd_symbol('a', 'b', 'c')");
        assertKeyValueAccess("rnd_symbol(4, 3, 6, 0)");
        assertKeyValueAccess("rnd_symbol_weighted('A', 2.5, 'B', 1.5, 'C', 1.0)");
        assertKeyValueAccess("rnd_symbol_zipf('A', 'B', 'C', 'D', 'E', 2.0)");
        assertKeyValueAccess("rnd_symbol_zipf(5, 1.5)");
        assertKeyValueAccess("list('RXGZ', 'HYRX', 'ABC')");
    }

    @Test
    public void testFixedDictionarySymbolFunctionsResolveEachKeyToItsOwnValue() throws Exception {
        // These build their dictionary from the call's own literal arguments, so the key-to-value
        // mapping can be read straight off the symbol table without drawing a single row. That is
        // the guard list() lacked: its valueOf resolved through TableUtils.toIndexKey, which adds
        // one for a leading null slot these lists do not carry, so every key returned the following
        // value. Only the resulting out-of-bounds read on the highest key made that visible to a
        // non-null assertion - an in-bounds shift stayed silent.
        assertMemoryLeak(() -> {
            assertResolvesLiteralDictionary("list('RXGZ', 'HYRX', 'ABC')", "RXGZ", "HYRX", "ABC");
            assertResolvesLiteralDictionary("rnd_symbol('a', 'b', 'c')", "a", "b", "c");
            assertResolvesLiteralDictionary("rnd_symbol_weighted('A', 2.5, 'B', 1.5, 'C', 1.0)", "A", "B", "C");
            assertResolvesLiteralDictionary("rnd_symbol_zipf('A', 'B', 'C', 'D', 'E', 2.0)", "A", "B", "C", "D", "E");
        });
    }

    @Test
    public void testListSymbolFunctionResolvesEachKeyToItsOwnValue() throws Exception {
        // list() is the one function above whose DRAW is deterministic - getInt() is
        // position++ % count - so its key sequence, not just its mapping, can be pinned. (The
        // mapping alone is pinned for the other literal-dictionary generators by
        // testFixedDictionarySymbolFunctionsResolveEachKeyToItsOwnValue, which needs no draw.)
        // list() is also the one that had the bug: valueOf() used to resolve through
        // TableUtils.toIndexKey, which adds one to account for a leading null slot this list does
        // not carry: every key returned the FOLLOWING value, and the highest ran off the end.
        // assertKeyValueAccess only asserts non-null, so it catches the overrun but not the
        // off-by-one underneath it - a wrong mapping that stays in bounds passes there.
        assertMemoryLeak(() -> {
            final List<String> expected = Arrays.asList("RXGZ", "HYRX", "ABC", "RXGZ", "HYRX", "ABC", "RXGZ", "HYRX");
            final List<String> resolved = new ArrayList<>();
            try (RecordCursorFactory factory = select("SELECT list('RXGZ', 'HYRX', 'ABC') s FROM long_sequence(8)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final SymbolTable symbolTable = cursor.getSymbolTable(0);
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        // One accessor per row: getInt() advances the cycle, so reading the text
                        // as well would consume a second position and desynchronise the sequence.
                        final CharSequence value = symbolTable.valueOf(record.getInt(0));
                        resolved.add(value == null ? null : value.toString());
                    }
                }
            }
            Assert.assertEquals("list() must resolve each key to its own value", expected, resolved);
        });
    }

    @Test
    public void testMemoizedSymbolFunctionsKeepKeyValueAccess() throws Exception {
        // Production wraps a SYMBOL projection in SymbolFunctionMemoizer when the function asks for
        // it or its alias is referenced more than once. ALLOW_FUNCTION_MEMOIZATION defaults to true
        // there but the test base turns it off, so without this switch the opt-ins asserted above
        // are pinned only in a configuration production never runs: the memoizer caches getInt(),
        // so it has to pass the hint through or it demotes every function it wraps.
        allowFunctionMemoization();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE memo (v FLOAT)");
            execute("INSERT INTO memo VALUES (1.5), (8.25), (null)");
            // rnd_symbol(count, lo, hi, nullRate) declares shouldMemoize(), so it is always wrapped.
            // Its draws are not reproducible across cursors, so pin only that the key path survives
            // and that every key it mints resolves.
            assertResolvesThroughKey("SELECT rnd_symbol(4, 3, 6, 0) s FROM memo", 0);
            // An alias read twice is wrapped for the same reason, and is deterministic, so it can be
            // held to the values the rows actually carry.
            assertKeyAndTextMatch(
                    "SELECT s, s ss FROM (SELECT v::symbol s FROM memo)",
                    0,
                    Arrays.asList("1.5", "8.25", null)
            );
        });
    }

    @Test
    public void testPureScalarKeyedSymbolFunctionsOptIntoKeyValueAccess() throws Exception {
        // A symbol constant resolves a key from a field, and a cast-to-symbol resolves one with a
        // single probe on the already-decoded scalar. Neither hashes the row's text, so the key
        // path costs strictly less than re-encoding the value on every row. Unlike the generators
        // above these are pure functions of the row, so a row's key must resolve to the very text
        // the row reads directly - the opt-in is only sound while it does.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (b BYTE, sh SHORT, i INT, l LONG, f FLOAT, d DOUBLE, c CHAR, bo BOOLEAN, ts TIMESTAMP, dt DATE)");
            execute("""
                    INSERT INTO t VALUES
                        (1, 2, 3, 4, 1.5, 2.5, 'x', true, '2024-01-01T00:00:00.000000Z', '2024-01-01T00:00:00.000Z'),
                        (5, 6, 7, 8, 8.25, 9.75, 'y', false, '2024-06-02T03:04:05.000006Z', '2024-06-02T03:04:05.000Z'),
                        (9, 10, null, null, null, null, null, true, null, null)
                    """);
            // One per changed class. FLOAT keys on the raw bits and CHAR on the code point, so
            // their key is an encoding rather than the value itself - the two cases where minting
            // the dictionary entry from the key rather than the value silently ships the encoding.
            assertPureKeyValueAccess("b::symbol");
            assertPureKeyValueAccess("sh::symbol");
            assertPureKeyValueAccess("i::symbol");
            assertPureKeyValueAccess("l::symbol");
            assertPureKeyValueAccess("f::symbol");
            assertPureKeyValueAccess("d::symbol");
            assertPureKeyValueAccess("c::symbol");
            assertPureKeyValueAccess("bo::symbol");
            assertPureKeyValueAccess("'US'::symbol");
            // The TIMESTAMP and DATE casts render the epoch value rather than the ISO text that
            // ::string produces, so they carry their own expectation. Their key IS the value, with
            // no encoding step, so the CHAR/FLOAT hazard cannot arise for them.
            assertPureKeyValueAccess("ts::symbol", Arrays.asList("1704067200000000", "1717297445000006", null));
            assertPureKeyValueAccess("dt::symbol", Arrays.asList("1704067200000", "1717297445000", null));
        });
    }

    @Test
    public void testSymbolAggregatesKeepKeyValueAccess() throws Exception {
        // first/last/mode read the key straight out of the aggregation map and resolve it through
        // the aggregated argument, so they never hash the row's text either. They only reach the
        // gate when the argument is a dynamic symbol expression: over a stored column the QWP
        // unwrap finds the static table first.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE agg (k INT, v FLOAT)");
            execute("INSERT INTO agg VALUES (1, 1.5), (1, 1.5), (2, 8.25)");
            // Each group holds one distinct value, so first/last/mode all yield it. Hard-coding the
            // expectation keeps the oracle independent of the symbol path under test.
            assertKeyAndTextMatch("SELECT k, first(v::symbol) s FROM agg ORDER BY k", 1, FLOAT_GROUPS);
            assertKeyAndTextMatch("SELECT k, last(v::symbol) s FROM agg ORDER BY k", 1, FLOAT_GROUPS);
            assertKeyAndTextMatch("SELECT k, mode(v::symbol) s FROM agg ORDER BY k", 1, FLOAT_GROUPS);
        });
    }

    private void assertKeyAndTextMatch(String sql, int columnIndex, List<String> expected) throws SqlException {
        // Read each accessor on its own factory, and hold both to an expectation derived from the
        // input rather than from the other accessor. For several of these functions getSymbol() IS
        // valueOf(getInt(rec)), so comparing the two against each other agrees even on a wrong value.
        Assert.assertEquals(sql + " resolved through its key", expected, read(sql, columnIndex, true));
        Assert.assertEquals(sql + " read as text", expected, read(sql, columnIndex, false));
    }


    private void assertKeyValueAccess(String expression) throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = select("SELECT " + expression + " s FROM long_sequence(8)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(
                            expression + " must keep the native-key egress path",
                            cursor.getSymbolTable(0).supportsKeyValueAccess()
                    );
                    // The opt-in is only sound if valueOf() resolves every key getInt() mints,
                    // on every row rather than just the first. Read one accessor per row and
                    // resolve through the table: these generators draw a fresh random value on
                    // each accessor call, so getInt() and getSymA() on the same row are two
                    // independent draws and must not be compared against each other.
                    final SymbolTable symbolTable = cursor.getSymbolTable(0);
                    while (cursor.hasNext()) {
                        final int key = cursor.getRecord().getInt(0);
                        Assert.assertTrue(expression + " minted a negative key", key >= 0);
                        Assert.assertNotNull(
                                expression + " must resolve key " + key + " without reading row text",
                                symbolTable.valueOf(key)
                        );
                    }
                }
            }
        });
    }

    private void assertPureKeyValueAccess(String expression) throws SqlException {
        // The cast to STRING is the oracle: a separate function with its own formatting and no
        // shared dictionary.
        assertPureKeyValueAccess(expression, read("SELECT " + expression.replace("::symbol", "::string") + " s FROM t", 0, false));
    }

    private void assertPureKeyValueAccess(String expression, List<String> expected) throws SqlException {
        Assert.assertEquals(expression + " read an unexpected number of rows", 3, expected.size());
        assertKeyAndTextMatch("SELECT " + expression + " s FROM t", 0, expected);
    }

    private void assertResolvesLiteralDictionary(String expression, String... expected) throws SqlException {
        try (RecordCursorFactory factory = select("SELECT " + expression + " s FROM long_sequence(1)")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final SymbolTable symbolTable = cursor.getSymbolTable(0);
                for (int i = 0; i < expected.length; i++) {
                    final CharSequence value = symbolTable.valueOf(i);
                    Assert.assertNotNull(expression + " must resolve key " + i, value);
                    Assert.assertEquals(expression + " resolved key " + i + " wrongly", expected[i], value.toString());
                }
            }
        }
    }

    private void assertResolvesThroughKey(String sql, int columnIndex) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final SymbolTable symbolTable = cursor.getSymbolTable(columnIndex);
                Assert.assertTrue(sql + " must keep the native-key egress path", symbolTable.supportsKeyValueAccess());
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    final int key = record.getInt(columnIndex);
                    Assert.assertTrue(sql + " minted a negative key", key >= 0);
                    Assert.assertNotNull(sql + " must resolve key " + key, symbolTable.valueOf(key));
                }
            }
        }
    }

    private List<String> read(String sql, int columnIndex, boolean isReadThroughKey) throws SqlException {
        final List<String> values = new ArrayList<>();
        try (RecordCursorFactory factory = select(sql)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                SymbolTable symbolTable = null;
                if (isReadThroughKey) {
                    symbolTable = cursor.getSymbolTable(columnIndex);
                    Assert.assertTrue(
                            sql + " must keep the native-key egress path",
                            symbolTable.supportsKeyValueAccess()
                    );
                }
                while (cursor.hasNext()) {
                    final CharSequence value = isReadThroughKey
                            ? symbolTable.valueOf(record.getInt(columnIndex))
                            : record.getStrA(columnIndex);
                    values.add(value == null ? null : value.toString());
                }
            }
        }
        return values;
    }
}
