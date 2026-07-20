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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

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

    @Test
    public void testFixedDictionarySymbolFunctionsOptIntoKeyValueAccess() throws Exception {
        assertKeyValueAccess("rnd_symbol('a', 'b', 'c')");
        assertKeyValueAccess("rnd_symbol(4, 3, 6, 0)");
        assertKeyValueAccess("rnd_symbol_weighted('A', 2.5, 'B', 1.5, 'C', 1.0)");
        assertKeyValueAccess("rnd_symbol_zipf('A', 'B', 'C', 'D', 'E', 2.0)");
        assertKeyValueAccess("rnd_symbol_zipf(5, 1.5)");
        assertKeyValueAccess("list('RXGZ', 'HYRX', 'ABC')");
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
}
