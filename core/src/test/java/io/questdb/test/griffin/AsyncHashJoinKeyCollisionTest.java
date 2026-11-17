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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The INNER and LEFT joins of {@code HashJoinTest}, whose keys once collided in the serial hash
 * join's key layouts and returned wrong rows, run on the parallel hash join, the default plan:
 * composite SYMBOL and STRING or VARCHAR keys whose columns sit at colliding indexes on either
 * side, a self-join of a SYMBOL with a STRING, an empty VARCHAR whose ASCII flag is clear, DECIMAL
 * keys of every width and UUID keys next to a SYMBOL, both with NULLs, SYMBOL pairs whose
 * dictionaries order the texts differently, and a SYMBOL that the other side lacks.
 * {@code HashJoinTest} pins them to the serial plan; here each must return the rows that
 * {@code HashJoinTest} expects, and the rows of the ordinary plan, in order, with their symbol
 * tables and B views, on the default context, under frames of one row and builds in rounds, and on
 * a pool of four workers.
 */
public class AsyncHashJoinKeyCollisionTest extends AbstractCairoTest {
    private static final String[][] DECIMALS = {
            {"DECIMAL(2,1)", "1.2", "2.3", "3.4"},
            {"DECIMAL(4,2)", "12.34", "23.45", "34.56"},
            {"DECIMAL(9,4)", "12345.6789", "23456.7890", "34567.8901"},
            {"DECIMAL(18,6)", "123456789012.345678", "234567890123.456789", "345678901234.567890"},
            {"DECIMAL(38,10)", "1234567890123456789012345678.9012345678", "2345678901234567890123456789.0123456789", "3456789012345678901234567890.1234567890"},
            {
                    "DECIMAL(76,20)",
                    "12345678901234567890123456789012345678901234567890123456.78901234567890123456",
                    "23456789012345678901234567890123456789012345678901234567.89012345678901234567",
                    "34567890123456789012345678901234567890123456789012345678.90123456789012345678"
            },
    };
    private static final String ORDERS_INNER = """
            ts\tsym\tqty
            2024-01-01T00:00:01.000000Z\tAAPL\t10
            2024-01-01T00:00:02.000000Z\tMSFT\t20
            """;
    private static final String ORDERS_LEFT = """
            ts\tsym\tqty
            2024-01-01T00:00:01.000000Z\tAAPL\t10
            2024-01-01T00:00:02.000000Z\tMSFT\t20
            2024-01-01T00:00:03.000000Z\tIBM\tnull
            2024-01-01T00:00:04.000000Z\tAAPL\tnull
            """;
    private static final String SYMBOL_PAIRS = """
            sym1\tsym2\tv\tv1
            A\tX\t1\t10
            B\tY\t2\t13
            """;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @Test
    public void testDefaultContext() throws Exception {
        assertMemoryLeak(() -> assertCases(engine, sqlExecutionContext));
    }

    @Test
    public void testFourWorkersFramesOfOneRowBuildsInRounds() throws Exception {
        assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(4, WorkerPoolMode.LEGACY), (db, compiler, ignore) -> {
            try (
                    SqlExecutionContextImpl context = new SqlExecutionContextImpl(db, 4)
                            .with(AllowAllSecurityContext.INSTANCE, null, null, -1, null)
            ) {
                context.setParallelHashJoinEnabled(true);
                context.changePageFrameSizes(1, 1);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 1);
                assertCases(db, context);
            }
        }, configuration, LOG));
    }

    @Test
    public void testFramesOfOneRowBuildsInRounds() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(1, 1);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 1);
            try {
                assertCases(engine, sqlExecutionContext);
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    // Runs each query on the parallel hash join and asserts the rows that HashJoinTest expects of
    // the serial plan, null where it states none, and the ordinary plan's rows; then drops the
    // tables.
    private static void assertCase(CairoEngine db, SqlExecutionContext context, String... queriesAndRows) throws Exception {
        for (int i = 0; i < queriesAndRows.length; i += 2) {
            final String sql = queriesAndRows[i];
            final String expected = queriesAndRows[i + 1];
            assertParallelHashJoin(sql, context);
            if (expected != null) {
                Assert.assertEquals(sql, expected, rows(db, context, sql));
            }
        }
        db.execute("DROP ALL TABLES", context);
    }

    private static void assertCases(CairoEngine db, SqlExecutionContext context) throws Exception {
        final String inner = "SELECT o.ts, o.sym, f.qty FROM orders o JOIN fills f ON o.sym = f.sym AND o.venue = f.venue";
        final String left = "SELECT o.ts, o.sym, f.qty FROM orders o LEFT JOIN fills f ON o.sym = f.sym AND o.venue = f.venue";
        for (String venueType : new String[]{"STRING", "VARCHAR"}) {
            createOrdersAndFills(db, context, venueType);
            assertCase(
                    db, context,
                    inner, ORDERS_INNER,
                    left, ORDERS_LEFT,
                    left + " AND f.qty <> 20", """
                            ts\tsym\tqty
                            2024-01-01T00:00:01.000000Z\tAAPL\t10
                            2024-01-01T00:00:02.000000Z\tMSFT\tnull
                            2024-01-01T00:00:03.000000Z\tIBM\tnull
                            2024-01-01T00:00:04.000000Z\tAAPL\tnull
                            """,
                    // The mirror: the SYMBOL venue of fills meets orders' STRING or VARCHAR one.
                    "SELECT f.venue, o.sym, f.qty FROM fills f JOIN orders o ON f.sym = o.sym AND f.venue = o.venue", """
                            venue\tsym\tqty
                            NYSE\tAAPL\t10
                            NASDAQ\tMSFT\t20
                            """
            );
        }
        // fills grows past orders, so the INNER join may swap its inputs.
        createOrdersAndFills(db, context, "STRING");
        db.execute("""
                INSERT INTO fills VALUES
                    ('2024-01-01T00:00:00.000000Z', 'GOOG', 'NYSE', 50),
                    ('2024-01-01T00:00:00.000000Z', 'AMZN', 'ARCA', 60)""", context);
        assertCase(db, context, "SELECT o.venue, o.sym, f.qty FROM orders o JOIN fills f ON o.sym = f.sym AND o.venue = f.venue", """
                venue\tsym\tqty
                NYSE\tAAPL\t10
                NASDAQ\tMSFT\t20
                """);

        // A self-join of a SYMBOL with a STRING, each way.
        db.execute("CREATE TABLE book (ts TIMESTAMP, sym SYMBOL, side SYMBOL, side_str STRING, qty LONG) TIMESTAMP(ts) PARTITION BY DAY", context);
        db.execute("""
                INSERT INTO book VALUES
                    ('2024-01-01T00:00:01.000000Z', 'AAPL', 'buy', 'buy', 1),
                    ('2024-01-01T00:00:02.000000Z', 'MSFT', 'sell', 'sell', 2),
                    ('2024-01-01T00:00:03.000000Z', 'IBM', 'buy', 'sell', 3)""", context);
        assertCase(
                db, context,
                "SELECT a.ts, a.sym, b.qty FROM book a JOIN book b ON a.sym = b.sym AND a.side = b.side_str", """
                        ts\tsym\tqty
                        2024-01-01T00:00:01.000000Z\tAAPL\t1
                        2024-01-01T00:00:02.000000Z\tMSFT\t2
                        """,
                "SELECT a.ts, b.qty, b.sym FROM book a JOIN book b ON a.sym = b.sym AND a.side_str = b.side", """
                        ts\tqty\tsym
                        2024-01-01T00:00:01.000000Z\t1\tAAPL
                        2024-01-01T00:00:02.000000Z\t2\tMSFT
                        """,
                "SELECT a.ts, a.sym, b.qty FROM book a LEFT JOIN book b ON a.sym = b.sym AND a.side = b.side_str", null
        );

        // An empty VARCHAR stored with its ASCII flag clear meets one stored with it set.
        createEmptyNonAsciiVarchar(db, context);
        assertCase(
                db, context,
                "SELECT m.v, s.w FROM m JOIN s ON m.k = s.k ORDER BY m.v", """
                        v\tw
                        1\t10
                        2\t20
                        """,
                "SELECT m.v, s.w FROM m LEFT JOIN s ON m.k = s.k", null,
                "SELECT mx.v, sx.w FROM mx LEFT JOIN sx ON mx.k = sx.k AND mx.i = sx.i", null,
                "SELECT sx.w, mx.v FROM sx LEFT JOIN mx ON sx.k = mx.k", null
        );

        // DECIMAL keys of every width next to a SYMBOL, with NULLs in both.
        for (String[] decimal : DECIMALS) {
            final String a = decimal[1] + "::" + decimal[0];
            createKeyTables(db, context, decimal[0], a, decimal[2] + "::" + decimal[0], decimal[3] + "::" + decimal[0], a);
            assertCase(
                    db, context,
                    "SELECT m.v, s.v FROM m JOIN s ON m.id = s.id AND m.sym = s.sym ORDER BY m.v", """
                            v\tv1
                            1\t10
                            3\t30
                            4\t40
                            5\t50
                            """,
                    "SELECT m.v, s.v FROM m LEFT JOIN s ON m.id = s.id AND m.sym = s.sym ORDER BY m.v", """
                            v\tv1
                            1\t10
                            2\tnull
                            3\t30
                            4\t40
                            5\t50
                            """,
                    "SELECT m.*, s.* FROM m LEFT JOIN s ON m.id = s.id AND m.sym = s.sym", null
            );
        }

        // UUID keys next to a SYMBOL, with NULLs in both.
        createKeyTables(
                db, context, "UUID",
                "'11111111-1111-1111-1111-111111111111'", "'22222222-2222-2222-2222-222222222222'", "'33333333-3333-3333-3333-333333333333'",
                "'44444444-4444-4444-4444-444444444444'"
        );
        assertCase(
                db, context,
                "SELECT m.id, m.sym, m.v, s.v FROM m JOIN s ON m.id = s.id AND m.sym = s.sym ORDER BY m.v", """
                        id\tsym\tv\tv1
                        11111111-1111-1111-1111-111111111111\tA\t1\t10
                        33333333-3333-3333-3333-333333333333\tC\t3\t30
                        \tN\t4\t40
                        44444444-4444-4444-4444-444444444444\t\t5\t50
                        """,
                "SELECT m.id, m.sym, m.v, s.v FROM m LEFT JOIN s ON m.id = s.id AND m.sym = s.sym ORDER BY m.v", """
                        id\tsym\tv\tv1
                        11111111-1111-1111-1111-111111111111\tA\t1\t10
                        22222222-2222-2222-2222-222222222222\tB\t2\tnull
                        33333333-3333-3333-3333-333333333333\tC\t3\t30
                        \tN\t4\t40
                        44444444-4444-4444-4444-444444444444\t\t5\t50
                        """,
                "SELECT m.*, s.* FROM m LEFT JOIN s ON m.id = s.id AND m.sym = s.sym", null
        );

        // Two SYMBOL keys whose dictionaries list the texts in other orders, and in columns of
        // other indexes.
        for (String buildColumns : new String[]{"sym1 SYMBOL, sym2 SYMBOL", "sym2 SYMBOL, sym1 SYMBOL"}) {
            db.execute("CREATE TABLE m (sym1 SYMBOL, sym2 SYMBOL, v INT)", context);
            db.execute("CREATE TABLE s (" + buildColumns + ", v INT)", context);
            db.execute("INSERT INTO m VALUES ('A', 'X', 1), ('B', 'Y', 2)", context);
            db.execute("""
                    INSERT INTO s (sym1, sym2, v) VALUES
                        ('A', 'X', 10), ('A', 'Y', 11), ('B', 'X', 12), ('B', 'Y', 13), ('C', 'X', 14), ('C', 'Y', 15)""", context);
            assertCase(
                    db, context,
                    "SELECT m.sym1, m.sym2, m.v, s.v FROM m JOIN s ON m.sym1 = s.sym1 AND m.sym2 = s.sym2 ORDER BY m.v", SYMBOL_PAIRS,
                    "SELECT * FROM m JOIN s ON m.sym1 = s.sym1 AND m.sym2 = s.sym2", null
            );
        }

        // A SYMBOL text that the other side lacks.
        db.execute("CREATE TABLE m (sym SYMBOL, v INT)", context);
        db.execute("CREATE TABLE s (sym SYMBOL, v INT)", context);
        db.execute("INSERT INTO m VALUES ('A', 1), ('B', 2), ('C', 3)", context);
        db.execute("INSERT INTO s VALUES ('A', 10), ('B', 20), ('D', 40)", context);
        assertCase(
                db, context,
                "SELECT * FROM m JOIN s ON m.sym = s.sym ORDER BY m.v", """
                        sym\tv\tsym1\tv1
                        A\t1\tA\t10
                        B\t2\tB\t20
                        """,
                "SELECT * FROM m LEFT JOIN s ON m.sym = s.sym ORDER BY m.v", """
                        sym\tv\tsym1\tv1
                        A\t1\tA\t10
                        B\t2\tB\t20
                        C\t3\t\tnull
                        """
        );

        // A LEFT join's ON residual that compares one side's SYMBOL with either of two of the other's.
        db.execute("CREATE TABLE taba (i LONG, locale_name SYMBOL)", context);
        db.execute("CREATE TABLE tabb (i LONG, state SYMBOL, city SYMBOL)", context);
        db.execute("INSERT INTO taba VALUES (1, 'pl')", context);
        db.execute("INSERT INTO tabb VALUES (1, 'a', 'pl'), (1, 'b', 'b')", context);
        assertCase(db, context, "SELECT * FROM taba LEFT JOIN tabb ON taba.i = tabb.i AND (locale_name = state OR locale_name = city)", """
                i\tlocale_name\ti1\tstate\tcity
                1\tpl\t1\ta\tpl
                """);
    }

    // m and s hold '' with its ASCII flag clear, as HashJoinTest writes it; mx holds it written
    // through SQL, and sx with the flag clear, next to a second key column.
    private static void createEmptyNonAsciiVarchar(CairoEngine db, SqlExecutionContext context) throws Exception {
        final Utf8String emptyNonAscii = new Utf8String(new byte[0], false);
        Assert.assertFalse(emptyNonAscii.isAscii());
        db.execute("CREATE TABLE m (k VARCHAR, v INT)", context);
        db.execute("CREATE TABLE s (k VARCHAR, w INT)", context);
        for (String table : new String[]{"m", "s"}) {
            try (TableWriter writer = TestUtils.getWriter(db, table)) {
                TableWriter.Row row = writer.newRow();
                row.putVarchar(0, new Utf8String("a"));
                row.putInt(1, table.equals("m") ? 1 : 10);
                row.append();
                row = writer.newRow();
                row.putVarchar(0, emptyNonAscii);
                row.putInt(1, table.equals("m") ? 2 : 20);
                row.append();
                writer.commit();
            }
        }
        db.execute("CREATE TABLE mx (k VARCHAR, i INT, v INT)", context);
        db.execute("CREATE TABLE sx (k VARCHAR, i INT, w INT)", context);
        db.execute("INSERT INTO mx VALUES ('a', 1, 1), ('', 1, 2), (NULL, 1, 3)", context);
        try (TableWriter writer = TestUtils.getWriter(db, "sx")) {
            TableWriter.Row row = writer.newRow();
            row.putVarchar(0, new Utf8String("a"));
            row.putInt(1, 1);
            row.putInt(2, 10);
            row.append();
            row = writer.newRow();
            row.putVarchar(0, emptyNonAscii);
            row.putInt(1, 1);
            row.putInt(2, 20);
            row.append();
            writer.commit();
        }
    }

    // m and s, keyed by an id of the given type next to a SYMBOL, as HashJoinTest creates them: the
    // ids a, b and c, a NULL id, and the id e under a NULL SYMBOL; s holds b under another SYMBOL.
    private static void createKeyTables(CairoEngine db, SqlExecutionContext context, String type, String a, String b, String c, String e) throws Exception {
        db.execute("CREATE TABLE m (id " + type + ", sym SYMBOL, v INT)", context);
        db.execute("CREATE TABLE s (id " + type + ", sym SYMBOL, v INT)", context);
        db.execute("INSERT INTO m VALUES (" + a + ", 'A', 1), (" + b + ", 'B', 2), (" + c + ", 'C', 3), (NULL, 'N', 4), (" + e + ", NULL, 5)", context);
        db.execute("INSERT INTO s VALUES (" + a + ", 'A', 10), (" + b + ", 'X', 20), (" + c + ", 'C', 30), (NULL, 'N', 40), (" + e + ", NULL, 50)", context);
    }

    // orders' venue, of the given type, sits at the column index of fills' SYMBOL venue's neighbor.
    private static void createOrdersAndFills(CairoEngine db, SqlExecutionContext context, String venueType) throws Exception {
        db.execute("CREATE TABLE orders (ts TIMESTAMP, sym SYMBOL, venue " + venueType + ") TIMESTAMP(ts) PARTITION BY DAY", context);
        db.execute("CREATE TABLE fills (ts TIMESTAMP, sym SYMBOL, venue SYMBOL, qty LONG) TIMESTAMP(ts) PARTITION BY DAY", context);
        db.execute("""
                INSERT INTO orders VALUES
                    ('2024-01-01T00:00:01.000000Z', 'AAPL', 'NYSE'),
                    ('2024-01-01T00:00:02.000000Z', 'MSFT', 'NASDAQ'),
                    ('2024-01-01T00:00:03.000000Z', 'IBM', 'NYSE'),
                    ('2024-01-01T00:00:04.000000Z', 'AAPL', 'ARCA')""", context);
        db.execute("""
                INSERT INTO fills VALUES
                    ('2024-01-01T00:00:00.000000Z', 'TSLA', 'NASDAQ', 40),
                    ('2024-01-01T00:00:00.000000Z', 'AAPL', 'NYSE', 10),
                    ('2024-01-01T00:00:00.000000Z', 'MSFT', 'NASDAQ', 20),
                    ('2024-01-01T00:00:00.000000Z', 'IBM', 'ARCA', 30)""", context);
    }

    // The rows of the query, which runs the parallel hash join, as printSql() prints them.
    private static String rows(CairoEngine db, SqlExecutionContext context, String sql) throws Exception {
        final StringSink sink = new StringSink();
        try (SqlCompiler compiler = db.getSqlCompiler()) {
            try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                Assert.assertNotNull(sql, find(factory));
            }
            TestUtils.printSql(compiler, context, sql, sink);
        }
        return sink.toString();
    }
}
