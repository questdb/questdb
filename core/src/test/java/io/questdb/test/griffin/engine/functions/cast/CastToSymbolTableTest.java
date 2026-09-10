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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.Misc;
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
    public void testTheViewOutlivesTheCursorItCameFrom() throws Exception {
        // A worker may still hold the view when the cursor closes. It carries its own copy of the
        // values, so it keeps answering; the clone it replaced would have been reading a closed
        // function's state by then.
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
                }
            }
            TestUtils.assertEquals("1.5", table.valueOf(0));
            TestUtils.assertEquals("2.5", table.valueOf(1));
            Assert.assertNull(table.valueOf(SymbolTable.VALUE_IS_NULL));
        });
    }

    private static String scan(RecordCursor cursor) {
        final Record record = cursor.getRecord();
        final StringBuilder sink = new StringBuilder();
        while (cursor.hasNext()) {
            sink.append(record.getInt(0)).append('=').append(record.getSymA(1)).append('\n');
        }
        return sink.toString();
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
