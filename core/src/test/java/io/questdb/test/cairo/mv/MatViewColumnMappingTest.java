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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.mv.MatViewColumnMapping;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cutlass.text.SqlExecutionContextStub;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Refresh maps the columns of a materialized view's query to the view's columns by name, with the record copiers'
 * implicit casts. A view whose SQL relies on generated column names copies by position, as before.
 * <p>
 * No SQL statement can make a view's columns differ from its SQL's output yet, so the mapping tests over reordered,
 * missing and extra columns copy into tables whose columns differ from a query's. The integration tests change a
 * view's columns through its WAL writer instead.
 */
public class MatViewColumnMappingTest extends AbstractCairoTest {
    private static final String BASE_ROWS = """
            sym\tprice\tamount\tts
            a\t1.5\t10.0\t2024-01-01T00:10:00.000000Z
            b\t2.5\t20.0\t2024-01-01T00:20:00.000000Z
            a\t3.5\t30.0\t2024-01-01T00:40:00.000000Z
            a\t4.5\t40.0\t2024-01-01T01:10:00.000000Z
            """;

    private static final int[] COPIER_TYPES = {
            RecordToRowCopierUtils.COPIER_TYPE_SINGLE_METHOD,
            RecordToRowCopierUtils.COPIER_TYPE_CHUNKED,
            RecordToRowCopierUtils.COPIER_TYPE_LOOPING
    };

    @Test
    public void testCaseInsensitiveNames() throws Exception {
        assertMemoryLeak(() -> assertCopy(
                "CREATE TABLE v (\"TS\" TIMESTAMP, \"Price\" DOUBLE) TIMESTAMP(\"TS\") PARTITION BY DAY",
                "SELECT x * 1.5 AS price, timestamp_sequence('2024-01-01', 3_600_000_000) AS ts FROM long_sequence(2)",
                false,
                """
                        TS\tPrice
                        2024-01-01T00:00:00.000000Z\t1.5
                        2024-01-01T01:00:00.000000Z\t3.0
                        """,
                true
        ));
    }

    @Test
    public void testCastsByName() throws Exception {
        // Each query column goes to the view column of its name and is cast to the view column's type, NULLs included.
        Assert.assertTrue(RecordToRowCopierUtils.isCopierConvertible(ColumnType.INT, ColumnType.LONG));
        Assert.assertTrue(RecordToRowCopierUtils.isCopierConvertible(ColumnType.FLOAT, ColumnType.DOUBLE));
        Assert.assertTrue(RecordToRowCopierUtils.isCopierConvertible(ColumnType.STRING, ColumnType.VARCHAR));
        Assert.assertTrue(RecordToRowCopierUtils.isCopierConvertible(ColumnType.VARCHAR, ColumnType.SYMBOL));
        Assert.assertTrue(RecordToRowCopierUtils.isCopierConvertible(ColumnType.VARCHAR, ColumnType.DOUBLE));
        assertMemoryLeak(() -> assertCopy(
                "CREATE TABLE v (ts TIMESTAMP, i LONG, d DOUBLE, s VARCHAR, sym SYMBOL, n DOUBLE) TIMESTAMP(ts) PARTITION BY DAY",
                """
                        SELECT
                            CASE WHEN x = 1 THEN NULL ELSE '2.25' END::VARCHAR AS n,
                            CASE WHEN x = 1 THEN NULL ELSE 'sym' || x END::VARCHAR AS sym,
                            CASE WHEN x = 1 THEN NULL ELSE 's' || x END::STRING AS s,
                            CASE WHEN x = 1 THEN NULL ELSE x::FLOAT + 0.5f END AS d,
                            CASE WHEN x = 1 THEN NULL ELSE x::INT END AS i,
                            timestamp_sequence('2024-01-01', 3_600_000_000) AS ts
                        FROM long_sequence(2)
                        """,
                false,
                """
                        ts\ti\td\ts\tsym\tn
                        2024-01-01T00:00:00.000000Z\tnull\tnull\t\t\tnull
                        2024-01-01T01:00:00.000000Z\t2\t2.5\ts2\tsym2\t2.25
                        """,
                true
        ));
    }

    @Test
    public void testCreateRejectsBareCall() throws Exception {
        // CREATE reads a bare name as a column, so a bare pi that names no column fails and asks for the parentheses,
        // instead of being stored as a call.
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createBase();
            final String sql = "CREATE MATERIALIZED VIEW v AS (SELECT ts, sym, pi, avg(price) AS a FROM base SAMPLE BY 1h) PARTITION BY DAY";
            assertExceptionNoLeakCheck(
                    sql,
                    sql.indexOf("pi,"),
                    "pi is not a column; write pi() to call the function"
            );
        });
    }

    @Test
    public void testCreateStoresWildcardAsWritten() throws Exception {
        // CREATE stores the SQL as written, a wildcard included, also when the optimiser reads a sub-query twice, as
        // for a declared sub-query that the query reads twice.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE MATERIALIZED VIEW star_pi AS (SELECT *, pi() AS pi FROM base) PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW declared_pi AS (DECLARE @pi := (SELECT pi() FROM long_sequence(1)) "
                    + "SELECT ts, sym, avg(price) AS avg_price FROM base WHERE price < @pi OR price > @pi SAMPLE BY 1h) PARTITION BY DAY");
            assertQuery("SELECT view_name, view_sql FROM materialized_views ORDER BY view_name").noLeakCheck().returns("""
                    view_name\tview_sql
                    declared_pi\tDECLARE @pi := (SELECT pi() FROM long_sequence(1)) SELECT ts, sym, avg(price) AS avg_price FROM base WHERE price < @pi OR price > @pi SAMPLE BY 1h
                    star_pi\tSELECT *, pi() AS pi FROM base
                    """);
        });
    }

    @Test
    public void testDroppedViewColumn() throws Exception {
        // A dropped view column keeps its slot in the view's metadata; a query column of its name is ignored.
        assertMemoryLeak(() -> {
            for (int copierType : COPIER_TYPES) {
                execute("CREATE TABLE v (ts TIMESTAMP, a INT, gone INT, b INT) TIMESTAMP(ts) PARTITION BY DAY");
                execute("ALTER TABLE v DROP COLUMN gone");
                copyInto(
                        "SELECT x::INT AS a, (x * 10)::INT AS gone, (x * 100)::INT AS b, timestamp_sequence('2024-01-01', 3_600_000_000) AS ts FROM long_sequence(2)",
                        false,
                        copierType,
                        true
                );
                assertQuery("v").timestamp("ts").noLeakCheck().expectSize().returns("""
                        ts\ta\tb
                        2024-01-01T00:00:00.000000Z\t1\t100
                        2024-01-01T01:00:00.000000Z\t2\t200
                        """);
                execute("DROP TABLE v");
            }
        });
    }

    @Test
    public void testExtraQueryColumns() throws Exception {
        // Query columns the view lacks, before, between and after the mapped ones, are ignored.
        assertMemoryLeak(() -> assertCopy(
                "CREATE TABLE v (ts TIMESTAMP, a INT, b VARCHAR) TIMESTAMP(ts) PARTITION BY DAY",
                "SELECT 'x' || x AS x, x::INT AS a, x * 2 AS y, 'b' || x AS b, timestamp_sequence('2024-01-01', 3_600_000_000) AS ts, x AS z FROM long_sequence(2)",
                false,
                """
                        ts\ta\tb
                        2024-01-01T00:00:00.000000Z\t1\tb1
                        2024-01-01T01:00:00.000000Z\t2\tb2
                        """,
                true
        ));
    }

    @Test
    public void testFindFailingColumn() throws Exception {
        // A value that does not convert fails the copy. Replaying the row through the looping copier names the
        // query column and the view column, for an identity and for a reordered mapping.
        Assert.assertTrue(RecordToRowCopierUtils.isCopierConvertible(ColumnType.VARCHAR, ColumnType.INT));
        assertMemoryLeak(() -> {
            execute("CREATE TABLE identity (ts TIMESTAMP, ok INT, bad INT) TIMESTAMP(ts) PARTITION BY DAY");
            assertFailingColumn(
                    "identity",
                    "SELECT timestamp_sequence('2024-01-01', 3_600_000_000) AS ts, '7'::VARCHAR AS ok, CASE WHEN x = 1 THEN '8' ELSE 'abc' END::VARCHAR AS bad FROM long_sequence(2)",
                    false,
                    "bad",
                    "bad"
            );
            execute("CREATE TABLE reordered (ts TIMESTAMP, bad INT, ok INT, extra INT) TIMESTAMP(ts) PARTITION BY DAY");
            assertFailingColumn(
                    "reordered",
                    "SELECT 'ignored'::VARCHAR AS skipped, '7'::VARCHAR AS ok, CASE WHEN x = 1 THEN '8' ELSE 'abc' END::VARCHAR AS bad, timestamp_sequence('2024-01-01', 3_600_000_000) AS ts FROM long_sequence(2)",
                    false,
                    "bad",
                    "bad"
            );
            // A positional copy names the pair at the failing position, whatever their names.
            execute("CREATE TABLE positional (ts TIMESTAMP, first INT, second INT) TIMESTAMP(ts) PARTITION BY DAY");
            assertFailingColumn(
                    "positional",
                    "SELECT timestamp_sequence('2024-01-01', 3_600_000_000) AS ts, CASE WHEN x = 1 THEN '8' ELSE 'abc' END::VARCHAR AS a, '7'::VARCHAR AS b FROM long_sequence(2)",
                    true,
                    "a",
                    "first"
            );
        });
    }

    @Test
    public void testIdentity() throws Exception {
        assertMemoryLeak(() -> assertCopy(
                "CREATE TABLE v (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                "SELECT 's' || x AS sym, x * 1.5 AS price, timestamp_sequence('2024-01-01', 3_600_000_000) AS ts FROM long_sequence(2)",
                false,
                """
                        sym\tprice\tts
                        s1\t1.5\t2024-01-01T00:00:00.000000Z
                        s2\t3.0\t2024-01-01T01:00:00.000000Z
                        """,
                false
        ));
    }

    @Test
    public void testInconvertibleTypes() throws Exception {
        Assert.assertFalse(RecordToRowCopierUtils.isCopierConvertible(ColumnType.SYMBOL, ColumnType.LONG));
        assertMemoryLeak(() -> {
            // The error names the pair the mapping matched by name, not the columns at the same position.
            execute("CREATE TABLE v (ts TIMESTAMP, n LONG, other LONG) TIMESTAMP(ts) PARTITION BY DAY");
            assertMappingFails(
                    "SELECT 1L AS other, timestamp_sequence('2024-01-01', 3_600_000_000) AS ts, 'a'::SYMBOL AS n FROM long_sequence(1)",
                    false,
                    "inconvertible types: SYMBOL -> LONG [from=n, to=n]"
            );
            // A positional copy checks the pairs at each position.
            assertMappingFails(
                    "SELECT timestamp_sequence('2024-01-01', 3_600_000_000) AS ts, 1L AS a, 'a'::SYMBOL AS b FROM long_sequence(1)",
                    true,
                    "inconvertible types: SYMBOL -> LONG [from=b, to=other]"
            );
        });
    }

    @Test
    public void testMissingDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE v (ts TIMESTAMP, a INT) TIMESTAMP(ts) PARTITION BY DAY");
            assertMappingFails(
                    "SELECT x::INT AS a, timestamp_sequence('2024-01-01', 3_600_000_000) AS other_ts FROM long_sequence(1)",
                    false,
                    "materialized view query does not produce the designated timestamp column [column=ts]"
            );
        });
    }

    @Test
    public void testMissingQueryColumns() throws Exception {
        // View columns no query column names receive NULL, whatever their position.
        assertMemoryLeak(() -> assertCopy(
                "CREATE TABLE v (first LONG, ts TIMESTAMP, a INT, middle VARCHAR, b INT, last DOUBLE) TIMESTAMP(ts) PARTITION BY DAY",
                "SELECT x::INT AS a, (x * 10)::INT AS b, timestamp_sequence('2024-01-01', 3_600_000_000) AS ts FROM long_sequence(2)",
                false,
                """
                        first\tts\ta\tmiddle\tb\tlast
                        null\t2024-01-01T00:00:00.000000Z\t1\t\t10\tnull
                        null\t2024-01-01T01:00:00.000000Z\t2\t\t20\tnull
                        """,
                true
        ));
    }

    @Test
    public void testOlderViewWithGeneratedNamesFailsRefreshOnFewerQueryColumns() throws Exception {
        // A positional copy into a view with more columns than the query produces would leave the trailing view
        // columns NULL, or shift values into the wrong columns. The refresh fails instead, and FULL keeps the rows.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // `price * 2` is named "column": the SQL relies on a generated name.
            executeWithoutExplicitNamesCheck("CREATE MATERIALIZED VIEW price_copy AS (SELECT *, price * 2 FROM base) PARTITION BY DAY");
            execute("INSERT INTO base VALUES ('a', 1.5, '2024-01-01T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            final String rows = """
                    sym\tprice\tts\tcolumn
                    a\t1.5\t2024-01-01T00:10:00.000000Z\t3.0
                    """;
            assertQuery("price_copy").timestamp("ts").noLeakCheck().expectSize().returns(rows);
            Assert.assertTrue(getViewState("price_copy").isRecordCopyPositional());

            // Refreshed under the expression alias rules, the SQL still relies on a generated name.
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
            execute("ALTER TABLE base DROP COLUMN sym");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW price_copy FULL");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views")
                    .noRandomAccess().noLeakCheck().returns("""
                            view_status\tinvalidation_reason
                            invalid\t[-1]: materialized view query does not match view schema [view=price_copy, queryColumnCount=3, viewColumnCount=4]
                            """);
            assertQuery("price_copy").timestamp("ts").noLeakCheck().expectSize().returns(rows);
        });
    }

    @Test
    public void testOlderViewWithGeneratedNamesKeepsPositionalCopy() throws Exception {
        // CREATE under cairo.sql.column.alias.expression.enabled=false names avg(price) "avg" and max(amount) "max".
        // The refresh under the expression rules names them "avg(price)" and "max(amount)", which no view column
        // has, so a name mapping would leave both columns NULL.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
            createBase();
            executeWithoutExplicitNamesCheck("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, avg(price), max(amount) FROM base SAMPLE BY 1h) PARTITION BY DAY");
            insertBaseRows();
            drainWalAndMatViewQueues();
            final String rows = """
                    ts\tsym\tavg\tmax
                    2024-01-01T00:00:00.000000Z\ta\t2.5\t30.0
                    2024-01-01T00:00:00.000000Z\tb\t2.5\t20.0
                    2024-01-01T01:00:00.000000Z\ta\t4.5\t40.0
                    """;
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(rows);

            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(rows);
            Assert.assertTrue(getViewState("price_1h").isRecordCopyPositional());

            execute("INSERT INTO base VALUES ('b', 6.5, 60.0, '2024-01-01T01:30:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status FROM materialized_views").noRandomAccess().noLeakCheck().returns("view_status\nvalid\n");
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tavg\tmax
                    2024-01-01T00:00:00.000000Z\ta\t2.5\t30.0
                    2024-01-01T00:00:00.000000Z\tb\t2.5\t20.0
                    2024-01-01T01:00:00.000000Z\ta\t4.5\t40.0
                    2024-01-01T01:00:00.000000Z\tb\t6.5\t60.0
                    """);
        });
    }

    @Test
    public void testPositionalCopy() throws Exception {
        // A view whose SQL relies on generated names copies by position, whatever the names say.
        assertMemoryLeak(() -> assertCopy(
                "CREATE TABLE v (ts TIMESTAMP, avg DOUBLE, max DOUBLE) TIMESTAMP(ts) PARTITION BY DAY",
                "SELECT timestamp_sequence('2024-01-01', 3_600_000_000) AS ts, x * 1.5 AS \"avg(price)\", x * 2.0 AS \"max(price)\" FROM long_sequence(2)",
                true,
                """
                        ts\tavg\tmax
                        2024-01-01T00:00:00.000000Z\t1.5\t2.0
                        2024-01-01T01:00:00.000000Z\t3.0\t4.0
                        """,
                false
        ));
    }

    @Test
    public void testRefreshCastsAfterBaseColumnTypeChange() throws Exception {
        // The view's columns map by name, and FULL casts a base column whose type changed to the view column's type.
        Assert.assertTrue(RecordToRowCopierUtils.isCopierConvertible(ColumnType.FLOAT, ColumnType.DOUBLE));
        Assert.assertTrue(RecordToRowCopierUtils.isCopierConvertible(ColumnType.LONG, ColumnType.INT));
        Assert.assertTrue(RecordToRowCopierUtils.isCopierConvertible(ColumnType.VARCHAR, ColumnType.DOUBLE));
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(price) AS price, last(qty) AS qty FROM base SAMPLE BY 1h) PARTITION BY DAY");
            execute("INSERT INTO base VALUES ('a', 1.5, 7, '2024-01-01T00:10:00.000000Z'), ('b', NULL, NULL, '2024-01-01T00:20:00.000000Z'), ('a', -2.25, -3, '2024-01-01T01:10:00.000000Z')");
            drainWalAndMatViewQueues();
            final String rows = """
                    ts\tsym\tprice\tqty
                    2024-01-01T00:00:00.000000Z\ta\t1.5\t7
                    2024-01-01T00:00:00.000000Z\tb\tnull\tnull
                    2024-01-01T01:00:00.000000Z\ta\t-2.25\t-3
                    """;
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(rows);
            Assert.assertNull(getViewState("price_1h").getRecordProjectionForTesting());

            for (String alter : new String[]{
                    "ALTER TABLE base ALTER COLUMN price TYPE FLOAT",
                    "ALTER TABLE base ALTER COLUMN qty TYPE LONG",
                    "ALTER TABLE base ALTER COLUMN price TYPE VARCHAR"
            }) {
                execute(alter);
                drainWalAndMatViewQueues();
                execute("REFRESH MATERIALIZED VIEW price_1h FULL");
                drainWalAndMatViewQueues();
                assertQuery("SELECT view_status, invalidation_reason FROM materialized_views")
                        .noRandomAccess().noLeakCheck().returns("view_status\tinvalidation_reason\nvalid\t\n");
                assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(rows);
                Assert.assertFalse(getViewState("price_1h").isRecordCopyPositional());
            }

            // The copier built for the new types also serves incremental refreshes.
            execute("INSERT INTO base VALUES ('b', '0.125', 9, '2024-01-01T00:30:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tprice\tqty
                    2024-01-01T00:00:00.000000Z\ta\t1.5\t7
                    2024-01-01T00:00:00.000000Z\tb\t0.125\t9
                    2024-01-01T01:00:00.000000Z\ta\t-2.25\t-3
                    """);
        });
    }

    @Test
    public void testRefreshFailsWhenQueryStopsProducingDesignatedTimestamp() throws Exception {
        // A regular view's columns count as plain columns, so a view that reads one maps them by name. Once the
        // regular view names its timestamp differently, the query no longer produces the view's designated
        // timestamp, without which no row can be written: the refresh fails, and FULL keeps the rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, a DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 'x', 1.5)");
            drainWalQueue();
            execute("CREATE VIEW vagg AS (SELECT ts, k, avg(a) AS a FROM base SAMPLE BY 1h)");
            execute("CREATE MATERIALIZED VIEW mv WITH BASE base AS (SELECT * FROM vagg) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String rows = """
                    ts\tk\ta
                    2024-01-01T00:00:00.000000Z\tx\t1.5
                    """;
            assertQuery("mv").timestamp("ts").noLeakCheck().expectSize().returns(rows);
            Assert.assertFalse(getViewState("mv").isRecordCopyPositional());

            execute("ALTER VIEW vagg AS (SELECT ts AS ts2, k, avg(a) AS a FROM base SAMPLE BY 1h)");
            execute("INSERT INTO base VALUES ('2024-01-01T01:00:00.000000Z', 'x', 2.5)");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views")
                    .noRandomAccess().noLeakCheck().returns("""
                            view_status\tinvalidation_reason
                            invalid\t[-1]: materialized view query does not produce the designated timestamp column [column=ts]
                            """);
            assertQuery("mv").timestamp("ts").noLeakCheck().expectSize().returns(rows);
        });
    }

    @Test
    public void testRefreshIdentityMappingUsesNoProjection() throws Exception {
        // Performance property: a view whose columns match its SQL's output one to one keeps the copier positional
        // copy generates, with no projection, for an aggregating and for a passthrough view.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, avg(price) AS avg_price, sum(amount) AS total FROM base SAMPLE BY 1h) PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW base_copy AS (SELECT * FROM base) PARTITION BY DAY");
            insertBaseRows();
            drainWalAndMatViewQueues();

            for (String view : new String[]{"price_1h", "base_copy"}) {
                final MatViewState state = getViewState(view);
                Assert.assertNotNull(state.getRecordToRowCopier());
                Assert.assertNull(state.getRecordProjectionForTesting());
                Assert.assertFalse(state.isRecordCopyPositional());
            }
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tavg_price\ttotal
                    2024-01-01T00:00:00.000000Z\ta\t2.5\t40.0
                    2024-01-01T00:00:00.000000Z\tb\t2.5\t20.0
                    2024-01-01T01:00:00.000000Z\ta\t4.5\t40.0
                    """);
            assertQuery("base_copy").timestamp("ts").noLeakCheck().expectSize().returns(BASE_ROWS);
        });
    }

    @Test
    public void testRefreshKeepsPositionalCopyForLongColumnNames() throws Exception {
        // Under the default alias rules, CREATE names an unaliased reference to a column longer than 64 characters
        // after the column name cut to 64. A refresh under a larger cairo.sql.column.alias.generated.max.size, or
        // under the legacy alias rules, names the reference in full, which matches no view column. Such a view copies
        // by position under every setting, as before, for an aggregating and for a passthrough view, the designated
        // timestamp included. A reference to a column of exactly 64 characters keeps its name under every setting,
        // so its view maps by name.
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
        assertMemoryLeak(() -> {
            final String sym = "sym_" + "s".repeat(61);
            final String ts = "ts_" + "t".repeat(67);
            final String sym64 = sym.substring(0, 64);
            final String ts64 = ts.substring(0, 64);
            final String k64 = "k".repeat(64);
            final String t64 = "t".repeat(64);
            Assert.assertEquals(65, sym.length());
            Assert.assertEquals(70, ts.length());
            execute("CREATE TABLE base (" + sym + " SYMBOL, price DOUBLE, " + ts + " TIMESTAMP) TIMESTAMP(" + ts + ") PARTITION BY DAY WAL");
            execute("CREATE TABLE base64 (" + k64 + " SYMBOL, price DOUBLE, " + t64 + " TIMESTAMP) TIMESTAMP(" + t64 + ") PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW agg AS (SELECT " + ts + ", " + sym + ", avg(price) AS avg_price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW base_copy AS (SELECT " + sym + ", price, " + ts + " FROM base) PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW agg64 AS (SELECT " + t64 + ", " + k64 + ", avg(price) AS avg_price FROM base64 SAMPLE BY 1h) PARTITION BY DAY");
            final String[] rows = {
                    "('a', 1.5, '2024-01-01T00:10:00.000000Z'), ('b', 2.5, '2024-01-01T00:20:00.000000Z'), ('a', 3.5, '2024-01-01T00:40:00.000000Z')",
                    "('a', 4.5, '2024-01-01T01:10:00.000000Z')",
                    "('b', 6.5, '2024-01-01T01:30:00.000000Z')"
            };
            final String[] aggRows = {
                    """
                            2024-01-01T00:00:00.000000Z\ta\t2.5
                            2024-01-01T00:00:00.000000Z\tb\t2.5
                            """,
                    """
                            2024-01-01T01:00:00.000000Z\ta\t4.5
                            """,
                    """
                            2024-01-01T01:00:00.000000Z\tb\t6.5
                            """
            };
            final String[] copyRows = {
                    """
                            a\t1.5\t2024-01-01T00:10:00.000000Z
                            b\t2.5\t2024-01-01T00:20:00.000000Z
                            a\t3.5\t2024-01-01T00:40:00.000000Z
                            """,
                    """
                            a\t4.5\t2024-01-01T01:10:00.000000Z
                            """,
                    """
                            b\t6.5\t2024-01-01T01:30:00.000000Z
                            """
            };
            final StringBuilder expectedAgg = new StringBuilder(ts64 + "\t" + sym64 + "\tavg_price\n");
            final StringBuilder expectedAgg64 = new StringBuilder(t64 + "\t" + k64 + "\tavg_price\n");
            final StringBuilder expectedCopy = new StringBuilder(sym64 + "\tprice\t" + ts64 + "\n");
            // Default settings first, then a larger generated name size, then the legacy alias rules. Each step
            // rebuilds the views with FULL and refreshes them incrementally after a base insert.
            final Runnable[] settings = {
                    () -> {
                    },
                    () -> setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_GENERATED_MAX_SIZE, 128),
                    () -> setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false")
            };
            for (int i = 0; i < settings.length; i++) {
                settings[i].run();
                if (i > 0) {
                    for (String view : new String[]{"agg", "base_copy", "agg64"}) {
                        execute("REFRESH MATERIALIZED VIEW " + view + " FULL");
                    }
                    drainWalAndMatViewQueues();
                }
                execute("INSERT INTO base VALUES " + rows[i]);
                execute("INSERT INTO base64 VALUES " + rows[i]);
                drainWalAndMatViewQueues();

                expectedAgg.append(aggRows[i]);
                expectedAgg64.append(aggRows[i]);
                expectedCopy.append(copyRows[i]);
                assertQuery("SELECT view_name, view_status, invalidation_reason FROM materialized_views ORDER BY view_name")
                        .noLeakCheck().returns("""
                                view_name\tview_status\tinvalidation_reason
                                agg\tvalid\t
                                agg64\tvalid\t
                                base_copy\tvalid\t
                                """);
                assertQuery("SELECT * FROM agg ORDER BY 1, 2").timestamp(ts64).noLeakCheck().expectSize().returns(expectedAgg.toString());
                assertQuery("SELECT * FROM agg64 ORDER BY 1, 2").timestamp(t64).noLeakCheck().expectSize().returns(expectedAgg64.toString());
                assertQuery("base_copy").timestamp(ts64).noLeakCheck().expectSize().returns(expectedCopy.toString());
                Assert.assertTrue(getViewState("agg").isRecordCopyPositional());
                Assert.assertTrue(getViewState("base_copy").isRecordCopyPositional());
                Assert.assertFalse(getViewState("agg64").isRecordCopyPositional());
            }
        });
    }

    @Test
    public void testRefreshNamesColumnOfValueCastError() throws Exception {
        // A type pair without a copier conversion fails the refresh with an error that names both columns. A value
        // that does not convert fails it with the value's error, which names both columns the same way.
        Assert.assertFalse(RecordToRowCopierUtils.isCopierConvertible(ColumnType.BOOLEAN, ColumnType.DOUBLE));
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(qty) AS qty, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            execute("INSERT INTO base VALUES ('a', 1.5, 7, '2024-01-01T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            final String rows = """
                    ts\tsym\tqty\tprice
                    2024-01-01T00:00:00.000000Z\ta\t7\t1.5
                    """;

            execute("ALTER TABLE base ALTER COLUMN price TYPE BOOLEAN");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views")
                    .noRandomAccess().noLeakCheck().returns("""
                            view_status\tinvalidation_reason
                            invalid\t[-1]: inconvertible types: BOOLEAN -> DOUBLE [from=price, to=price]
                            """);
            // FULL checks the mapping before it truncates the view.
            assertQuery("price_1h").timestamp("ts").noLeakCheck().expectSize().returns(rows);

            execute("ALTER TABLE base ALTER COLUMN price TYPE VARCHAR");
            execute("UPDATE base SET price = 'abc'");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views")
                    .noRandomAccess().noLeakCheck().returns("""
                            view_status\tinvalidation_reason
                            invalid\tinconvertible value: `abc` [VARCHAR -> DOUBLE] [from=price, to=price]
                            """);
        });
    }

    @Test
    public void testRefreshNamesColumnsOfValueCastErrorInMappedCopy() throws Exception {
        // A query column the view lacks makes the mapping a projection. Dropping amount and adding it back moves it
        // behind extra, which the view lacks, so the failing value sits at the projection's fourth position, which is
        // the query's fifth column and the view's third. The error names the pair the projection copies.
        Assert.assertTrue(RecordToRowCopierUtils.isCopierConvertible(ColumnType.VARCHAR, ColumnType.DOUBLE));
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE MATERIALIZED VIEW base_copy AS (SELECT * FROM base) PARTITION BY DAY");
            insertBaseRows();
            drainWalAndMatViewQueues();
            execute("ALTER TABLE base ADD COLUMN extra INT");
            execute("ALTER TABLE base DROP COLUMN amount");
            execute("ALTER TABLE base ADD COLUMN amount VARCHAR");
            execute("INSERT INTO base VALUES ('c', 5.5, '2024-01-01T02:10:00.000000Z', 1, 'abc')");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW base_copy FULL");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views")
                    .noRandomAccess().noLeakCheck().returns("""
                            view_status\tinvalidation_reason
                            invalid\tinconvertible value: `abc` [VARCHAR -> DOUBLE] [from=amount, to=amount]
                            """);
        });
    }

    @Test
    public void testRefreshNamesColumnsOfValueCastErrorInPositionalCopy() throws Exception {
        // A positional copy pairs the columns at the failing position, whatever their names: under the expression
        // alias rules the query names last(price) "last(price)", while the view created under the legacy rules
        // names it "last".
        Assert.assertTrue(RecordToRowCopierUtils.isCopierConvertible(ColumnType.VARCHAR, ColumnType.DOUBLE));
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
            createBase();
            executeWithoutExplicitNamesCheck("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(price) FROM base SAMPLE BY 1h) PARTITION BY DAY");
            insertBaseRows();
            drainWalAndMatViewQueues();
            final String rows = """
                    ts\tsym\tlast
                    2024-01-01T00:00:00.000000Z\ta\t3.5
                    2024-01-01T00:00:00.000000Z\tb\t2.5
                    2024-01-01T01:00:00.000000Z\ta\t4.5
                    """;
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(rows);

            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
            execute("ALTER TABLE base ALTER COLUMN price TYPE VARCHAR");
            execute("INSERT INTO base VALUES ('c', 'abc', 50.0, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views")
                    .noRandomAccess().noLeakCheck().returns("""
                            view_status\tinvalidation_reason
                            invalid\tinconvertible value: `abc` [VARCHAR -> DOUBLE] [from=last(price), to=last]
                            """);
        });
    }

    @Test
    public void testRefreshNamesNoColumnOfCastErrorInsideRow() throws Exception {
        // A writer row converts a string to UUID itself, so the replay into a row that discards the values does not
        // fail and finds no column. The refresh fails with today's conversion error, which names no column.
        Assert.assertTrue(RecordToRowCopierUtils.isCopierConvertible(ColumnType.VARCHAR, ColumnType.UUID));
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, u UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW uuid_1h AS (SELECT ts, sym, last(u) AS u FROM base SAMPLE BY 1h) PARTITION BY DAY");
            execute("INSERT INTO base VALUES ('a', '11111111-1111-1111-1111-111111111111', '2024-01-01T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("ALTER TABLE base ALTER COLUMN u TYPE VARCHAR");
            execute("UPDATE base SET u = 'not-a-uuid'");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW uuid_1h FULL");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views")
                    .noRandomAccess().noLeakCheck().returns("""
                            view_status\tinvalidation_reason
                            invalid\tinconvertible value: `not-a-uuid` [STRING -> UUID]
                            """);
        });
    }

    @Test
    public void testRefreshPassthroughWildcardStoredAsWritten() throws Exception {
        // CREATE stores a passthrough view's wildcard as written. Refresh maps the columns it expands to by name, so
        // a column the base gains is ignored, and one the base loses reads NULL.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE MATERIALIZED VIEW base_copy AS (SELECT * FROM base) PARTITION BY DAY");
            insertBaseRows();
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_sql FROM materialized_views").noRandomAccess().noLeakCheck()
                    .returns("view_sql\nSELECT * FROM base\n");

            execute("ALTER TABLE base ADD COLUMN extra INT");
            execute("INSERT INTO base VALUES ('c', 5.5, 50.0, '2024-01-01T02:10:00.000000Z', 42)");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status FROM materialized_views").noRandomAccess().noLeakCheck().returns("view_status\nvalid\n");
            assertQuery("base_copy").timestamp("ts").noLeakCheck().expectSize().returns(BASE_ROWS + """
                    c\t5.5\t50.0\t2024-01-01T02:10:00.000000Z
                    """);
            Assert.assertNotNull(getViewState("base_copy").getRecordProjectionForTesting());

            execute("ALTER TABLE base DROP COLUMN amount");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW base_copy FULL");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status FROM materialized_views").noRandomAccess().noLeakCheck().returns("view_status\nvalid\n");
            assertQuery("base_copy").timestamp("ts").noLeakCheck().expectSize().returns("""
                    sym\tprice\tamount\tts
                    a\t1.5\tnull\t2024-01-01T00:10:00.000000Z
                    b\t2.5\tnull\t2024-01-01T00:20:00.000000Z
                    a\t3.5\tnull\t2024-01-01T00:40:00.000000Z
                    a\t4.5\tnull\t2024-01-01T01:10:00.000000Z
                    c\t5.5\tnull\t2024-01-01T02:10:00.000000Z
                    """);
        });
    }

    @Test
    public void testRefreshRebuildsCopierAfterViewColumnAddedBetweenBatchCommits() throws Exception {
        assertCopierRebuiltBetweenBatchCommits(
                true,
                """
                        sym\tprice\tts\textra
                        """,
                "\tnull"
        );
    }

    @Test
    public void testRefreshRebuildsCopierAfterViewColumnDroppedAtIntervalGap() throws Exception {
        // A refresh commits the rows of one refresh interval before it moves to the next, across a gap. A view
        // column dropped right after that commit needs a copier rebuilt for the next interval's rows.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(price) AS price, last(amount) AS amount FROM base SAMPLE BY 1h) PARTITION BY DAY");
            insertBaseRows();
            drainWalAndMatViewQueues();
            // Pin the cost model's gap threshold to 5 seconds, far below the 2-day gaps below, so the refresh keeps
            // the intervals apart whatever the machine's commit and scan speeds.
            final MatViewState state = getViewState("price_1h");
            Assert.assertTrue(state.tryLock());
            try {
                state.setRefreshMetricsForTesting(5_000_000L, 1L, 1L);
            } finally {
                state.unlock();
            }
            // Three base transactions far apart give the next refresh three intervals with gaps between them. The
            // view's WAL writer picks the dropped column up at the commit after the one the column is dropped after.
            execute("INSERT INTO base VALUES ('b', 5.5, 50.0, '2024-01-01T05:10:00.000000Z')");
            execute("INSERT INTO base VALUES ('b', 6.5, 60.0, '2024-01-03T05:10:00.000000Z')");
            execute("INSERT INTO base VALUES ('b', 7.5, 70.0, '2024-01-05T05:10:00.000000Z')");
            drainWalQueue();

            final AtomicInteger gapCommits = new AtomicInteger();
            final LogCapture logCapture = new LogCapture();
            try (MatViewRefreshJob job = createMatViewRefreshJob()) {
                job.setOnBatchCommittedForTesting(() -> {
                    if (gapCommits.getAndIncrement() == 0) {
                        dropViewColumn("price_1h", "price");
                    }
                });
                logCapture.start();
                drainMatViewQueue(job);
                logCapture.drain();
            } finally {
                logCapture.stop();
            }
            drainWalQueue();
            Assert.assertEquals(2, gapCommits.get());
            logCapture.assertOnlyOnce("view schema changed during refresh, rebuilding record copier \\[view=price_1h~\\d+");

            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views")
                    .noRandomAccess().noLeakCheck().returns("view_status\tinvalidation_reason\nvalid\t\n");
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tamount
                    2024-01-01T00:00:00.000000Z\ta\t30.0
                    2024-01-01T00:00:00.000000Z\tb\t20.0
                    2024-01-01T01:00:00.000000Z\ta\t40.0
                    2024-01-01T05:00:00.000000Z\tb\t50.0
                    2024-01-03T05:00:00.000000Z\tb\t60.0
                    2024-01-05T05:00:00.000000Z\tb\t70.0
                    """);
        });
    }

    @Test
    public void testRefreshRebuildsCopierAfterViewColumnDroppedBetweenBatchCommits() throws Exception {
        assertCopierRebuiltBetweenBatchCommits(
                false,
                """
                        sym\tts
                        """,
                null
        );
    }

    @Test
    public void testRefreshRebuildsStaleCopierAfterViewColumnDropped() throws Exception {
        // A view schema change between two refreshes makes the copier the first refresh cached stale. The next
        // refresh compiles the SQL again and maps its columns to the view's new columns.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE MATERIALIZED VIEW base_copy AS (SELECT * FROM base) PARTITION BY DAY");
            insertBaseRows();
            drainWalAndMatViewQueues();
            final RecordToRowCopier staleCopier = getViewState("base_copy").getRecordToRowCopier();
            Assert.assertNotNull(staleCopier);

            dropViewColumn("base_copy", "price");
            drainWalQueue();
            execute("INSERT INTO base VALUES ('c', 5.5, 50.0, '2024-01-01T00:30:00.000000Z')");
            drainWalAndMatViewQueues();

            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views")
                    .noRandomAccess().noLeakCheck().returns("view_status\tinvalidation_reason\nvalid\t\n");
            assertQuery("base_copy").timestamp("ts").noLeakCheck().expectSize().returns("""
                    sym\tamount\tts
                    a\t10.0\t2024-01-01T00:10:00.000000Z
                    b\t20.0\t2024-01-01T00:20:00.000000Z
                    c\t50.0\t2024-01-01T00:30:00.000000Z
                    a\t30.0\t2024-01-01T00:40:00.000000Z
                    a\t40.0\t2024-01-01T01:10:00.000000Z
                    """);
            final MatViewState state = getViewState("base_copy");
            Assert.assertNotSame(staleCopier, state.getRecordToRowCopier());
            Assert.assertNotNull(state.getRecordProjectionForTesting());
        });
    }

    @Test
    public void testReorderedColumns() throws Exception {
        assertMemoryLeak(() -> assertCopy(
                "CREATE TABLE v (ts TIMESTAMP, a INT, b DOUBLE, c VARCHAR) TIMESTAMP(ts) PARTITION BY DAY",
                "SELECT 'c' || x AS c, timestamp_sequence('2024-01-01', 3_600_000_000) AS ts, x * 0.5 AS b, x::INT AS a FROM long_sequence(2)",
                false,
                """
                        ts\ta\tb\tc
                        2024-01-01T00:00:00.000000Z\t1\t0.5\tc1
                        2024-01-01T01:00:00.000000Z\t2\t1.0\tc2
                        """,
                true
        ));
    }

    @Test
    public void testViewSqlCompileModeDefaults() throws Exception {
        assertMemoryLeak(() -> {
            // A context that does not implement the mode never enters it, and reads as positional copy.
            final SqlExecutionContextStub stub = new SqlExecutionContextStub(engine);
            stub.setViewSqlCompile(true);
            stub.setViewSqlGeneratedColumnNames(false);
            Assert.assertFalse(stub.isViewSqlCompile());
            Assert.assertTrue(stub.hasViewSqlGeneratedColumnNames());

            // The mode is off by default, so the parser reports nothing.
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1)) {
                context.with(AllowAllSecurityContext.INSTANCE);
                Assert.assertFalse(context.isViewSqlCompile());
                context.setViewSqlGeneratedColumnNames(false);
                Misc.free(select("SELECT ts, avg(price) FROM base SAMPLE BY 1h", context));
                Assert.assertFalse(context.hasViewSqlGeneratedColumnNames());

                context.setViewSqlCompile(true);
                Misc.free(select("SELECT ts, avg(price) FROM base SAMPLE BY 1h", context));
                Assert.assertTrue(context.hasViewSqlGeneratedColumnNames());
                // Only a query reports.
                context.setViewSqlGeneratedColumnNames(false);
                execute("INSERT INTO base VALUES ('a', 1.5, '2024-01-01T00:10:00.000000Z')", context);
                Assert.assertFalse(context.hasViewSqlGeneratedColumnNames());

                // Rebinding the context leaves the mode and resets the report.
                context.setViewSqlGeneratedColumnNames(false);
                context.with(AllowAllSecurityContext.INSTANCE);
                Assert.assertFalse(context.isViewSqlCompile());
                Assert.assertTrue(context.hasViewSqlGeneratedColumnNames());
            }
        });
    }

    @Test
    public void testViewSqlNamesColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, \"my price\" DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE other (sym SYMBOL, x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Regular views: one whose own SQL relies on generated names, read as plain columns by the query.
            execute("CREATE VIEW gen_view AS (SELECT ts, sym, avg(price) FROM base SAMPLE BY 1h)");
            execute("CREATE VIEW named_view AS (SELECT ts, sym, price FROM base)");
            drainWalAndViewQueues();

            final String[] namesColumns = {
                    "SELECT ts, sym, avg(price) AS avg_price FROM base SAMPLE BY 1h",
                    "SELECT ts, sym, count FROM base SAMPLE BY 1h",
                    "SELECT * FROM base",
                    "SELECT b.* FROM base b",
                    "base WHERE price > 0",
                    "SELECT b.ts, b.sym, b.price FROM base b",
                    "SELECT \"ts\", \"sym\", \"my price\" FROM base",
                    "SELECT b.\"my price\", b.ts FROM base b",
                    "SELECT TS, Sym FROM base",
                    "SELECT * FROM (SELECT ts, sym, avg(price) AS a FROM base SAMPLE BY 1h)",
                    "WITH c AS (SELECT ts, sym, avg(price) AS a FROM base SAMPLE BY 1h) SELECT * FROM c",
                    "SELECT b.ts, b.sym, o.x FROM base b JOIN other o ON b.sym = o.sym",
                    "SELECT b.ts, b.sym, o.m FROM base b JOIN (SELECT sym, max(x) AS m FROM other) o ON b.sym = o.sym",
                    "SELECT ts, sym FROM base UNION ALL SELECT ts, sym FROM base",
                    "SELECT * FROM gen_view",
                    "SELECT ts, sym, avg FROM gen_view",
                    "SELECT * FROM named_view",
            };
            for (String sql : namesColumns) {
                assertViewSqlGeneratedColumnNames(sql, false);
            }

            final String[] generatedNames = {
                    "SELECT ts, sym, avg(price) FROM base SAMPLE BY 1h",
                    "SELECT ts, sym, 1 FROM base",
                    "SELECT ts, sym, sym FROM base",
                    "SELECT b.ts, b.sym, o.sym FROM base b JOIN other o ON b.sym = o.sym",
                    "SELECT ts, price * 2 FROM base",
                    "SELECT ts, price::FLOAT FROM base",
                    "SELECT ts, row_number() OVER () FROM base",
                    "SELECT * FROM (SELECT ts, avg(price) FROM base SAMPLE BY 1h)",
                    "WITH c AS (SELECT ts, sym, avg(price) FROM base SAMPLE BY 1h) SELECT * FROM c",
                    "SELECT b.ts, b.sym, o.* FROM base b JOIN (SELECT sym, max(x) FROM other) o ON b.sym = o.sym",
                    "SELECT ts, sym FROM base UNION ALL SELECT ts, sym || '' FROM base",
                    "SELECT * FROM (SELECT ts, sym FROM base UNION ALL SELECT ts, sym || '' FROM base)",
            };
            for (String sql : generatedNames) {
                assertViewSqlGeneratedColumnNames(sql, true);
            }
        });
    }

    @Test
    public void testViewSqlNamesColumnsCutByAliasMaxSize() throws Exception {
        // The expression alias rules cut a long column reference to cairo.sql.column.alias.generated.max.size, so its
        // generated name is not the column's name.
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_GENERATED_MAX_SIZE, 4);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertViewSqlGeneratedColumnNames("SELECT ts, sym FROM base", false);
            assertViewSqlGeneratedColumnNames("SELECT ts, price FROM base", true);
            assertViewSqlGeneratedColumnNames("SELECT ts, price AS price FROM base", false);
        });
    }

    @Test
    public void testViewSqlNamesColumnsUpTo64Characters() throws Exception {
        // A reference to a column longer than 64 characters, the default cairo.sql.column.alias.generated.max.size,
        // relies on a generated name whatever the current setting and alias rules, unless it has an alias.
        final String c64 = "c".repeat(64);
        final String c65 = "c".repeat(65);
        for (String expressionEnabled : new String[]{"true", "false"}) {
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, expressionEnabled);
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_GENERATED_MAX_SIZE, 128);
            assertMemoryLeak(() -> {
                execute("CREATE TABLE base (" + c64 + " DOUBLE, " + c65 + " DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                assertViewSqlGeneratedColumnNames("SELECT ts, " + c64 + " FROM base", false);
                assertViewSqlGeneratedColumnNames("SELECT b.ts, b." + c64 + " FROM base b", false);
                assertViewSqlGeneratedColumnNames("SELECT ts, \"" + c64 + "\" FROM base", false);
                assertViewSqlGeneratedColumnNames("SELECT * FROM base", false);
                assertViewSqlGeneratedColumnNames("SELECT ts, " + c65 + " AS c FROM base", false);
                assertViewSqlGeneratedColumnNames("SELECT ts, " + c65 + " FROM base", true);
                assertViewSqlGeneratedColumnNames("SELECT b.ts, b." + c65 + " FROM base b", true);
                assertViewSqlGeneratedColumnNames("SELECT * FROM (SELECT ts, " + c65 + " FROM base)", true);
                execute("DROP TABLE base");
            });
        }
    }

    // Adds a view column through the view's WAL writer, as view ADD COLUMN will.
    private static void addViewColumn(String viewName, String columnName) {
        try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName(viewName))) {
            walWriter.addColumn(columnName, ColumnType.INT, AllowAllSecurityContext.INSTANCE);
        }
    }

    private static void assertViewSqlGeneratedColumnNames(String sql, boolean expected) throws SqlException {
        final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
        context.setViewSqlCompile(true);
        try {
            context.setViewSqlGeneratedColumnNames(!expected);
            Misc.free(select(sql, context));
            Assert.assertEquals(sql, expected, context.hasViewSqlGeneratedColumnNames());
        } finally {
            context.setViewSqlCompile(false);
        }
    }

    private static CairoConfiguration copierConfiguration(int copierType) {
        return new CairoConfigurationWrapper(configuration) {
            @Override
            public int getCopierType() {
                return copierType;
            }
        };
    }

    // Copies every row of the query into table v through the copier the mapping builds.
    private static void copyInto(String querySql, boolean isPositional, int copierType, boolean isProjected) throws SqlException {
        try (
                RecordCursorFactory factory = select(querySql);
                TableWriter writer = getWriter("v");
                SqlCompiler compiler = engine.getSqlCompiler()
        ) {
            final RecordMetadata viewMetadata = writer.getMetadata();
            final RecordToRowCopier copier = MatViewColumnMapping.newRecordToRowCopier(
                    compiler.getAsm(),
                    factory.getMetadata(),
                    viewMetadata,
                    isPositional,
                    copierConfiguration(copierType)
            );
            Assert.assertEquals(isProjected, MatViewColumnMapping.getRecordProjection(copier) != null);
            final CharSequence timestampName = viewMetadata.getColumnName(viewMetadata.getTimestampIndex());
            int timestampIndex = factory.getMetadata().getColumnIndexQuiet(timestampName);
            if (timestampIndex < 0) {
                timestampIndex = viewMetadata.getTimestampIndex();
            }
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    final TableWriter.Row row = writer.newRow(record.getTimestamp(timestampIndex));
                    copier.copy(sqlExecutionContext, record, row);
                    row.append();
                }
            }
            writer.commit();
        }
    }

    private static void createBase() throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    // Drops a view column through the view's WAL writer, as view DROP COLUMN will.
    private static void dropViewColumn(String viewName, String columnName) {
        final TableToken viewToken = engine.verifyTableName(viewName);
        try (WalWriter walWriter = engine.getWalWriter(viewToken)) {
            final AlterOperation dropColumn = new AlterOperationBuilder()
                    .ofDropColumn(0, viewToken, walWriter.getMetadata().getTableId())
                    .ofDropColumn(columnName)
                    .build();
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1)) {
                dropColumn.withContext(context.with(AllowAllSecurityContext.INSTANCE));
                walWriter.apply(dropColumn, true);
            }
        }
    }

    private static MatViewState getViewState(String viewName) {
        final MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName(viewName));
        Assert.assertNotNull(state);
        return state;
    }

    private static void insertBaseRows() throws SqlException {
        execute("""
                INSERT INTO base VALUES
                ('a', 1.5, 10.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.5, 20.0, '2024-01-01T00:20:00.000000Z'),
                ('a', 3.5, 30.0, '2024-01-01T00:40:00.000000Z'),
                ('a', 4.5, 40.0, '2024-01-01T01:10:00.000000Z')
                """);
    }

    // Changes the view's columns between the first two batch commits of a refresh: a view column added or dropped
    // lands in the refresh's WAL writer at its next batch commit, and the rows after that commit need a copier
    // built for the new columns.
    private void assertCopierRebuiltBetweenBatchCommits(boolean isAdd, String expectedHeader, String addedValue) throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_INSERT_AS_SELECT_BATCH_SIZE, 10);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 2);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym VARCHAR, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH WAL");
            execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT sym, last(price) AS price, ts FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base SELECT concat('sym', x), x, timestamp_sequence('2022-02-24', 1000000L*60*60*2) FROM long_sequence(30)");
            drainWalQueue();

            final AtomicInteger batchCommits = new AtomicInteger();
            final LogCapture logCapture = new LogCapture();
            try (MatViewRefreshJob job = createMatViewRefreshJob()) {
                job.setOnBatchCommittedForTesting(() -> {
                    if (batchCommits.getAndIncrement() == 0) {
                        if (isAdd) {
                            addViewColumn("price_1h", "extra");
                        } else {
                            dropViewColumn("price_1h", "price");
                        }
                    }
                });
                logCapture.start();
                drainMatViewQueue(job);
                logCapture.drain();
            } finally {
                logCapture.stop();
            }
            drainWalQueue();
            Assert.assertTrue(batchCommits.get() > 1);

            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views")
                    .noRandomAccess().noLeakCheck().returns("view_status\tinvalidation_reason\nvalid\t\n");
            // One base row every 2 hours from 2022-02-24, one view bucket each.
            final StringBuilder expected = new StringBuilder(expectedHeader);
            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");
            for (int i = 1; i <= 30; i++) {
                expected.append("sym").append(i);
                if (isAdd) {
                    expected.append('\t').append(i).append(".0");
                }
                expected.append('\t').append(LocalDateTime.of(2022, 2, 24, 0, 0).plusHours(2L * (i - 1)).format(formatter));
                if (addedValue != null) {
                    expected.append(addedValue);
                }
                expected.append('\n');
            }
            assertQuery("price_1h ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(expected.toString());
            Assert.assertNotNull(getViewState("price_1h").getRecordProjectionForTesting());
            // The copier was rebuilt within the refresh, not by the next one.
            logCapture.assertOnlyOnce("view schema changed during refresh, rebuilding record copier \\[view=price_1h~\\d+");

            // The rebuilt copier serves the next refresh, which commits in batches too.
            execute("INSERT INTO base SELECT concat('late', x), x, timestamp_sequence('2022-02-24T01:00', 1000000L*60*60*2) FROM long_sequence(30)");
            drainWalAndMatViewQueues();
            assertQuery("SELECT count(), count(sym) FROM price_1h").noLeakCheck().expectSize().noRandomAccess().returns("count\tcount1\n60\t60\n");
            assertQuery("SELECT sym FROM price_1h WHERE ts = '2022-02-24T01:00:00.000000Z'").noLeakCheck().returns("sym\nlate1\n");
        });
    }

    private void assertCopy(String viewDdl, String querySql, boolean isPositional, String expected, boolean isProjected) throws Exception {
        for (int copierType : COPIER_TYPES) {
            execute(viewDdl);
            copyInto(querySql, isPositional, copierType, isProjected);
            final String timestampName;
            try (RecordCursorFactory factory = select("v")) {
                timestampName = factory.getMetadata().getColumnName(factory.getMetadata().getTimestampIndex());
            }
            assertQuery("v").timestamp(timestampName).noLeakCheck().expectSize().returns(expected);
            execute("DROP TABLE v");
        }
    }

    private void assertFailingColumn(
            String tableName,
            String querySql,
            boolean isPositional,
            String expectedQueryColumn,
            String expectedViewColumn
    ) throws Exception {
        for (int copierType : COPIER_TYPES) {
            try (
                    RecordCursorFactory factory = select(querySql);
                    TableWriter writer = getWriter(tableName);
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                final RecordMetadata queryMetadata = factory.getMetadata();
                final RecordMetadata viewMetadata = writer.getMetadata();
                final RecordToRowCopier copier = MatViewColumnMapping.newRecordToRowCopier(
                        compiler.getAsm(),
                        queryMetadata,
                        viewMetadata,
                        isPositional,
                        copierConfiguration(copierType)
                );
                final int timestampIndex = queryMetadata.getColumnIndex("ts");
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    // the first row converts
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(-1, MatViewColumnMapping.findFailingColumn(sqlExecutionContext, copier, queryMetadata, viewMetadata, record, "inconvertible value: `abc` [VARCHAR -> INT]"));
                    TableWriter.Row row = writer.newRow(record.getTimestamp(timestampIndex));
                    copier.copy(sqlExecutionContext, record, row);
                    row.append();

                    Assert.assertTrue(cursor.hasNext());
                    row = writer.newRow(record.getTimestamp(timestampIndex));
                    String error = null;
                    try {
                        copier.copy(sqlExecutionContext, record, row);
                        Assert.fail("copy must fail on the malformed value");
                    } catch (ImplicitCastException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `abc` [VARCHAR -> INT]");
                        error = e.getMessage();
                    } finally {
                        row.cancel();
                    }
                    // a replay that fails with another error than the copy's names no column
                    Assert.assertEquals(-1, MatViewColumnMapping.findFailingColumn(sqlExecutionContext, copier, queryMetadata, viewMetadata, record, error + " "));
                    final int position = MatViewColumnMapping.findFailingColumn(sqlExecutionContext, copier, queryMetadata, viewMetadata, record, error);
                    Assert.assertTrue(position > -1);
                    TestUtils.assertEquals(expectedQueryColumn, queryMetadata.getColumnName(MatViewColumnMapping.getQueryColumnIndex(copier, position)));
                    TestUtils.assertEquals(expectedViewColumn, viewMetadata.getColumnName(MatViewColumnMapping.getViewColumnIndex(copier, position)));
                }
                writer.rollback();
            }
        }
    }

    private void assertMappingFails(String querySql, boolean isPositional, String expectedMessage) throws Exception {
        try (
                RecordCursorFactory factory = select(querySql);
                TableWriter writer = getWriter("v");
                SqlCompiler compiler = engine.getSqlCompiler()
        ) {
            MatViewColumnMapping.newRecordToRowCopier(compiler.getAsm(), factory.getMetadata(), writer.getMetadata(), isPositional, configuration);
            Assert.fail("the mapping must fail");
        } catch (CairoException e) {
            TestUtils.assertEquals(expectedMessage, e.getFlyweightMessage());
        }
    }
}
