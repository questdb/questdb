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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.bind.StrBindVariable;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.griffin.engine.functions.regex.ILikeSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.regex.LikeSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.regex.MatchSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.regex.SymbolKeySetProvider;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class PlanTablesTest extends AbstractCairoTest {

    @Test
    public void testCachedFactoryKeepsCompileTimeSymbolCount() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_MAX_SYMBOL_NOT_EQUALS_COUNT, 3);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '2024-01-01T00:00:00.000000Z'), ('b', '2024-01-01T00:00:01.000000Z')");
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = select(compiler, "SELECT * FROM t WHERE s != 'a'", sqlExecutionContext)
            ) {
                Assert.assertEquals(0, engine.getBusyReaderCount());
                planSink.of(factory, sqlExecutionContext);
                TestUtils.assertContains(planSink.getSink(), "FilterOnExcludedValues");
                execute("""
                        INSERT INTO t VALUES
                        ('c', '2024-01-01T00:00:02.000000Z'),
                        ('d', '2024-01-01T00:00:03.000000Z'),
                        ('e', '2024-01-01T00:00:04.000000Z')
                        """);
                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("the cached index plan must be rejected once the symbol count passes the limit");
                } catch (TableReferenceOutOfDateException ignored) {
                }
            }
            assertQuery("SELECT * FROM t WHERE s != 'a'")
                    .noLeakCheck()
                    .withPlanNotContaining("FilterOnExcludedValues")
                    .timestamp("ts")
                    .returns("""
                            s\tts
                            b\t2024-01-01T00:00:01.000000Z
                            c\t2024-01-01T00:00:02.000000Z
                            d\t2024-01-01T00:00:03.000000Z
                            e\t2024-01-01T00:00:04.000000Z
                            """);
        });
    }

    @Test
    public void testDeclarationAgreesWithInstance() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<FunctionFactory> factories = new ObjList<>();
            factories.add(new LikeSymbolFunctionFactory());
            factories.add(new ILikeSymbolFunctionFactory());
            factories.add(new MatchSymbolFunctionFactory());
            final String[] texts = {"abc", "a%", "%a", "%a%", "a_c", "a\\%", "%", "%%", "", null};
            for (int f = 0, n = factories.size(); f < n; f++) {
                final FunctionFactory factory = factories.getQuick(f);
                for (int s = 0; s < 2; s++) {
                    final boolean isStatic = s == 0;
                    for (String text : texts) {
                        assertAgreement(factory, isStatic, new ConstantExpression().ofString(text, 0), StrConstant.newInstance(text));
                        assertAgreement(factory, isStatic, new ConstantExpression().ofVarchar(text == null ? null : new Utf8String(text), 0),
                                text == null ? VarcharConstant.NULL : new VarcharConstant(text));
                    }
                    assertAgreement(factory, isStatic, new ConstantExpression().ofNull(0), NullConstant.NULL);
                    assertAgreement(factory, isStatic, new ConstantExpression().ofChar('a', 0), new CharConstant('a'));
                    assertAgreement(factory, isStatic, new ConstantExpression().ofChar('%', 0), new CharConstant('%'));
                    assertAgreement(factory, isStatic, new ConstantExpression().ofChar((char) 0, 0), new CharConstant((char) 0));
                    final StrBindVariable bindVariable = new StrBindVariable();
                    assertAgreement(factory, isStatic, new BindVariableExpression().of("$1", ColumnType.STRING,
                            BoundExpression.functionFlags(bindVariable), 0), bindVariable);
                }
            }
        });
    }

    @Test
    public void testOneReaderPerTablePerCompile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', 1, '2024-01-01T00:00:00.000000Z'), ('b', 2, '2024-01-01T00:00:01.000000Z')");
            final AtomicInteger acquisitions = new AtomicInteger();
            engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
                if (factoryType == PoolListener.SRC_READER && (event == PoolListener.EV_GET || event == PoolListener.EV_CREATE)) {
                    acquisitions.incrementAndGet();
                }
            });
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (RecordCursorFactory ignored = select(compiler, "SELECT v FROM t WHERE s = 'a'", sqlExecutionContext)) {
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                }
                Assert.assertEquals(1, acquisitions.getAndSet(0));
                try (RecordCursorFactory ignored = select(compiler, "SELECT x.v, y.v FROM t x CROSS JOIN t y WHERE x.s = 'a' AND y.s = 'b'",
                        sqlExecutionContext)) {
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                }
                Assert.assertEquals(1, acquisitions.get());
            } finally {
                engine.setPoolListener(null);
            }
            assertQuery("SELECT x.v, y.v FROM t x CROSS JOIN t y WHERE x.s = 'a' AND y.s = 'b'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            v\tv1
                            1\t2
                            """);
        });
    }

    @Test
    public void testPatternDeclarationAgreesInGeneration() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    ('ab', 1, '2024-01-01T00:00:00.000000Z'),
                    ('ba', 2, '2024-01-01T00:00:01.000000Z'),
                    (null, 3, '2024-01-01T00:00:02.000000Z')
                    """);
            assertPattern("s LIKE 'a%'", "1\n");
            assertPattern("s LIKE '%'", "1\n2\n");
            assertPattern("s LIKE '%%'", "1\n2\n");
            assertPattern("s LIKE ''", "");
            assertPattern("s ILIKE '%B'", "1\n");
            assertPattern("NOT s LIKE 'a%'", "2\n3\n");
            assertPattern("s ~ 'b'", "1\n2\n");
            assertPattern("s ~ '^b'", "2\n");
            assertPattern("s !~ '^b'", "1\n3\n");
            assertPattern("NOT s ~ '^b'", "1\n3\n");
            bindVariableService.clear();
            bindVariableService.setStr(0, "b%");
            assertPattern("s LIKE $1", "2\n");
            bindVariableService.clear();
            bindVariableService.setStr(0, "A");
            assertPattern("s ILIKE $1", "");
            bindVariableService.clear();
            bindVariableService.setStr(0, "a$");
            assertPattern("s ~ $1", "2\n");
            bindVariableService.clear();
        });
    }

    @Test
    public void testReaderReleasedAfterBindError() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            String sql = "SELECT nope FROM t";
            assertCompileFailureReleasesReaders(sql, sql.indexOf("nope"), "Invalid column: nope", false);
            sql = "SELECT x.v FROM t x JOIN nope y ON x.s = y.s";
            assertCompileFailureReleasesReaders(sql, sql.indexOf("nope"), "table does not exist [table=nope]", false);
            sql = "UPDATE t SET nope = 1";
            assertCompileFailureReleasesReaders(sql, sql.indexOf("nope"), "Invalid column: nope", false);
            sql = "EXPLAIN SELECT nope FROM t";
            assertCompileFailureReleasesReaders(sql, sql.indexOf("nope"), "Invalid column: nope", false);
        });
    }

    @Test
    public void testReaderReleasedAfterExecutionModelOnlyCompile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.generateExecutionModel("SELECT v FROM t WHERE s = 'a'", sqlExecutionContext);
                compiler.clear();
                Assert.assertEquals(0, engine.getBusyReaderCount());
                compiler.generateExecutionModel("SELECT v FROM t WHERE s = 'a'", sqlExecutionContext);
            }
            Assert.assertEquals(0, engine.getBusyReaderCount());
        });
    }

    @Test
    public void testReaderReleasedAfterExplain() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery query = compiler.compile("EXPLAIN SELECT v FROM t WHERE s = 'a'", sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.EXPLAIN, query.getType());
                Misc.free(query.getRecordCursorFactory());
                Assert.assertEquals(0, engine.getBusyReaderCount());
            }
        });
    }

    @Test
    public void testReaderReleasedAfterGenerationError() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '2024-01-01T00:00:00.000000Z')");
            final String sql = "SELECT * FROM t x SPLICE JOIN t y WHERE x.s = 'a'";
            assertCompileFailureReleasesReaders(sql, sql.indexOf("SPLICE"), "splice join doesn't support full fat mode", true);
        });
    }

    @Test
    public void testReaderReleasedAfterOptimiserError() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cities (country SYMBOL INDEX, year INT, population LONG)");
            execute("INSERT INTO cities VALUES ('NL', 2000, 1), ('US', 2010, 2)");
            execute("CREATE TABLE t1 (k INT, n INT)");
            execute("CREATE TABLE t2 (k INT)");
            final String sql = """
                    WITH p AS (cities PIVOT (SUM(population) FOR year IN (SELECT DISTINCT year FROM cities) GROUP BY country))
                    SELECT t1.k, l.c FROM t1 LEFT JOIN LATERAL (SELECT count() c FROM t2 WHERE t2.k = t1.k LIMIT t1.n) l ON true
                    CROSS JOIN p
                    """;
            assertCompileFailureReleasesReaders(sql, sql.indexOf("t1.n"), "LIMIT referencing an outer column is not supported over a scalar count", false);
        });
    }

    @Test
    public void testReaderReleasedAfterViewValidation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            String sql = "CREATE VIEW vw AS (SELECT nope FROM t)";
            assertExceptionNoLeakCheck(sql, sql.indexOf("nope"), "Invalid column: nope", sqlExecutionContext);
            Assert.assertEquals(0, engine.getBusyReaderCount());
            sql = "CREATE MATERIALIZED VIEW mv AS (SELECT ts, max(nope) FROM t SAMPLE BY 1h) PARTITION BY DAY";
            assertExceptionNoLeakCheck(sql, sql.indexOf("nope"), "Invalid column: nope", sqlExecutionContext);
            Assert.assertEquals(0, engine.getBusyReaderCount());
            execute("CREATE VIEW vw AS (SELECT v FROM t WHERE s = 'a')");
            Assert.assertEquals(0, engine.getBusyReaderCount());
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, max(v) FROM t SAMPLE BY 1h) PARTITION BY DAY");
            Assert.assertEquals(0, engine.getBusyReaderCount());
        });
    }

    private static void assertAgreement(FunctionFactory factory, boolean isStatic, BoundExpression boundPattern, Function pattern) throws Exception {
        final ObjList<BoundExpression> boundArgs = new ObjList<>();
        boundArgs.add(new ColumnExpression().of(0, ColumnType.SYMBOL, 0));
        boundArgs.add(boundPattern);
        final boolean isDeclared = factory.isSymbolKeySetProvider(boundArgs, isStatic);
        final ObjList<Function> args = new ObjList<>();
        args.add(new SymbolColumn(0, isStatic));
        args.add(pattern);
        final IntList positions = new IntList();
        positions.add(0);
        positions.add(10);
        final Function function = factory.newInstance(0, args, positions, configuration, sqlExecutionContext);
        try {
            Assert.assertEquals(factory.getSignature() + " static=" + isStatic + " pattern=" + boundPattern,
                    function instanceof SymbolKeySetProvider, isDeclared);
        } finally {
            Misc.free(function);
        }
    }

    private static void assertCompileFailureReleasesReaders(String sql, int position, String message, boolean isFullFatJoins) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setFullFatJoins(isFullFatJoins);
            try {
                compiler.compile(sql, sqlExecutionContext);
                Assert.fail("compilation must fail");
            } catch (SqlException e) {
                Assert.assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            } finally {
                compiler.setFullFatJoins(false);
            }
            Assert.assertEquals(0, engine.getBusyReaderCount());
        }
    }

    private void assertPattern(String predicate, String expectedValues) throws Exception {
        assertQuery("SELECT v FROM t WHERE " + predicate)
                .noLeakCheck()
                .returns("v\n" + expectedValues);
    }
}
