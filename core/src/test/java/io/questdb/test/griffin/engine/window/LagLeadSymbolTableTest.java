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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.join.SymbolJoinKeyMapping;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class LagLeadSymbolTableTest extends AbstractCairoTest {
    @Test
    public void testAsOfJoinCachedLight() throws Exception {
        assertAsOfJoin(true);
    }

    @Test
    public void testAsOfJoinCachedNonLight() throws Exception {
        assertAsOfJoin(false);
    }

    @Test
    public void testBorrowedAndClonedStaticTablesIncludeGeneratedNull() throws Exception {
        assertMemoryLeak(() -> {
            createSourceTable();
            for (String function : List.of("lag", "lead")) {
                for (String over : List.of("OVER ()", "OVER (PARTITION BY grp ORDER BY rn)")) {
                    for (int offset = 0; offset <= 1; offset++) {
                        String sql = "SELECT a, " + function + "(a, " + offset + ") " + over + " g FROM src";
                        try (RecordCursorFactory factory = select(sql)) {
                            Assert.assertTrue(factory.getMetadata().isSymbolTableStatic(1));
                            for (int run = 0; run < 2; run++) {
                                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                    StaticSymbolTable source = SymbolJoinKeyMapping.toStaticSymbolTable(cursor.getSymbolTable(0));
                                    StaticSymbolTable borrowed = SymbolJoinKeyMapping.toStaticSymbolTable(cursor.getSymbolTable(1));
                                    Assert.assertFalse(source.containsNullValue());
                                    assertSymbolTable(source, borrowed, offset > 0);
                                    Assert.assertSame(borrowed, SymbolJoinKeyMapping.toStaticSymbolTable(cursor.getSymbolTable(1)));
                                    SymbolTable clone = cursor.newSymbolTable(1);
                                    try {
                                        StaticSymbolTable cloned = SymbolJoinKeyMapping.toStaticSymbolTable(clone);
                                        assertSymbolTable(source, cloned, offset > 0);
                                        CharSequence a = cloned.valueOf(0);
                                        TestUtils.assertEquals("bb", source.valueOf(1));
                                        TestUtils.assertEquals("aa", a);
                                        TestUtils.assertEquals("bb", cloned.valueBOf(1));
                                        TestUtils.assertEquals("aa", a);
                                    } finally {
                                        Misc.freeIfCloseable(clone);
                                    }
                                    TestUtils.assertEquals("aa", source.valueOf(0));
                                    for (int pass = 0; pass < 2; pass++) {
                                        int count = 0;
                                        while (cursor.hasNext()) {
                                            int key = cursor.getRecord().getInt(1);
                                            TestUtils.assertEquals(borrowed.valueOf(key), cursor.getRecord().getSymA(1));
                                            count++;
                                        }
                                        Assert.assertEquals(4, count);
                                        cursor.toTop();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testDynamicArgumentsKeepTheirSymbolTables() throws Exception {
        assertMemoryLeak(() -> {
            createSourceTable();
            for (String function : List.of("lag", "lead")) {
                String query = "SELECT rn, " + function + "(a::STRING::SYMBOL) OVER (ORDER BY rn) g FROM src";
                try (RecordCursorFactory factory = select(query)) {
                    Assert.assertFalse(factory.getMetadata().isSymbolTableStatic(1));
                }
                assertQuery(query)
                        .noLeakCheck()
                        .expectSize()
                        .returns(function.equals("lag")
                                ? """
                                rn\tg
                                1\t
                                2\taa
                                3\tbb
                                4\tcc
                                """
                                : """
                                rn\tg
                                1\tbb
                                2\tcc
                                3\tdd
                                4\t
                                """);
            }
        });
    }

    @Test
    public void testWindowJoinMasterGeneratedNull() throws Exception {
        assertMemoryLeak(() -> {
            createSourceTable();
            execute("CREATE TABLE prices (ts TIMESTAMP, g SYMBOL, price LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO prices VALUES
                    ('2026-05-12T00:00:00.000000Z', NULL, 7),
                    ('2026-05-12T00:00:00.100000Z', 'cc', 17),
                    ('2026-05-12T00:00:00.200000Z', 'bb', 13),
                    ('2026-05-12T00:00:00.300000Z', 'aa', 11)
                    """);
            for (String function : List.of("lag", "lead")) {
                assertQuery("SELECT w.rn, sum(p.price) total FROM " +
                        "(SELECT ts, rn, " + function + "(a) OVER (ORDER BY rn) g FROM src ORDER BY ts) w " +
                        "WINDOW JOIN prices p ON (w.g = p.g) " +
                        "RANGE BETWEEN 1 DAY PRECEDING AND 0 FOLLOWING EXCLUDE PREVAILING")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("Window Fast Join", "CachedWindowLight")
                        .returns(function.equals("lag")
                                ? """
                                rn\ttotal
                                1\t7
                                2\t11
                                3\t13
                                4\t17
                                """
                                : """
                                rn\ttotal
                                1\t13
                                2\t17
                                3\tnull
                                4\t7
                                """);
            }
        });
    }

    private static void assertSymbolTable(StaticSymbolTable source, StaticSymbolTable table, boolean hasNull) {
        Assert.assertEquals(hasNull, table.containsNullValue());
        Assert.assertEquals(source.getSymbolCount(), table.getSymbolCount());
        Assert.assertEquals(source.getSymbolTableGeneration(), table.getSymbolTableGeneration());
        Assert.assertEquals(SymbolTable.VALUE_IS_NULL, table.keyOf(null));
        Assert.assertNull(table.valueOf(SymbolTable.VALUE_IS_NULL));
        Assert.assertNull(table.valueBOf(SymbolTable.VALUE_IS_NULL));
        Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, table.keyOf("absent"));
        for (int key = 0; key < source.getSymbolCount(); key++) {
            String value = source.valueOf(key).toString();
            TestUtils.assertEquals(value, table.valueOf(key));
            Assert.assertEquals(key, table.keyOf(value));
        }
    }

    private void assertAsOfJoin(boolean isLight) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, isLight);
        assertMemoryLeak(() -> {
            createSourceTable();
            execute("CREATE TABLE probe (ts TIMESTAMP, id INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // Different key order from src, plus a missing key and a NULL absent from src's dictionary.
            execute("""
                    INSERT INTO probe VALUES
                    ('2026-05-12T00:00:09.000000Z', 1, 'bb'),
                    ('2026-05-12T00:00:10.000000Z', 2, 'aa'),
                    ('2026-05-12T00:00:11.000000Z', 3, NULL),
                    ('2026-05-12T00:00:12.000000Z', 4, 'absent')
                    """);
            for (String function : List.of("lag", "lead")) {
                for (boolean isPartitioned : new boolean[]{false, true}) {
                    String expected = function.equals("lag")
                            ? (isPartitioned ? """
                            id\trn
                            1\t4
                            2\t3
                            3\t2
                            4\tnull
                            """ : """
                            id\trn
                            1\t3
                            2\t2
                            3\t1
                            4\tnull
                            """)
                            : (isPartitioned ? """
                            id\trn
                            1\tnull
                            2\tnull
                            3\t4
                            4\tnull
                            """ : """
                            id\trn
                            1\t1
                            2\tnull
                            3\t4
                            4\tnull
                            """);
                    for (String nullTreatment : List.of("", "IGNORE NULLS")) {
                        String window = "SELECT ts, rn, " + function + "(a) " + nullTreatment + " OVER (" +
                                (isPartitioned ? "PARTITION BY grp " : "") + "ORDER BY rn) g FROM src ORDER BY ts";
                        for (String type : List.of("SYMBOL", "STRING", "VARCHAR")) {
                            assertQuery("SELECT p.id, w.rn FROM (SELECT ts, id, g::" + type + " g FROM probe) p " +
                                    "ASOF JOIN (" + window + ") w ON (p.g = w.g)")
                                    .noLeakCheck()
                                    .noRandomAccess()
                                    .expectSize()
                                    .withPlanContaining("AsOf Join Light", isLight ? "CachedWindowLight\n" : "CachedWindow\n")
                                    .returns(expected);
                        }
                    }
                }
            }
        });
    }

    private void createSourceTable() throws Exception {
        execute("CREATE TABLE src (ts TIMESTAMP, a SYMBOL NOCACHE, grp SYMBOL, rn INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("""
                INSERT INTO src VALUES
                ('2026-05-12T00:00:01.000000Z', 'aa', 'x', 1),
                ('2026-05-12T00:00:02.000000Z', 'bb', 'y', 2),
                ('2026-05-12T00:00:03.000000Z', 'cc', 'x', 3),
                ('2026-05-12T00:00:04.000000Z', 'dd', 'y', 4)
                """);
    }
}
