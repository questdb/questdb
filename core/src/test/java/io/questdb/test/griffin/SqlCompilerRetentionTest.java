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
import io.questdb.cairo.pool.SqlCompilerPool;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.RetainedCapacity;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.ToIntFunction;

/**
 * A statement that grows the compiler's pools past their configured capacities keeps that capacity while it comes
 * back, so compiling it again creates no pooled objects, and leaves the pools back at the configured capacities once
 * two windows of uses pass without it. Ordinary statements leave the pools untouched.
 */
public class SqlCompilerRetentionTest extends AbstractCairoTest {
    private static final int MIN_RETAINED_DEPTH = 32;

    @Test
    public void testCteChainReturnsPoolsAfterSmallStatements() throws Exception {
        assertMemoryLeak(() -> {
            final int modelCeiling = configuration.getSqlModelPoolCapacity();
            final int characterCeiling = configuration.getSqlCharacterStoreCapacity();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compileAndClose(compiler, cteChain(8));
                Assert.assertTrue(compiler.getQueryModelPoolCapacity() > modelCeiling);
                compiler.clear();
                Assert.assertTrue(compiler.getQueryModelPoolCapacity() > modelCeiling);
                Assert.assertTrue(compiler.getCharacterStoreCapacity() > characterCeiling);

                for (int i = 0, n = 2 * RetainedCapacity.WINDOW_USES; i < n; i++) {
                    compileAndClose(compiler, "SELECT 1");
                    compiler.clear();
                }
                Assert.assertEquals(modelCeiling, compiler.getQueryModelPoolCapacity());
                Assert.assertEquals(characterCeiling, compiler.getCharacterStoreCapacity());

                assertQuery("SELECT count() FROM (SELECT x FROM long_sequence(3) UNION ALL SELECT x FROM long_sequence(3))")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count
                                6
                                """);
                Assert.assertEquals(modelCeiling, compiler.getQueryModelPoolCapacity());
            }
        });
    }

    @Test
    public void testDeepBindFailureReturnsScopes() throws Exception {
        assertMemoryLeak(() -> {
            String sql = "SELECT missing FROM long_sequence(1)";
            for (int i = 0; i < MIN_RETAINED_DEPTH + 8; i++) {
                sql = "SELECT x FROM long_sequence(1) WHERE x = (" + sql + ")";
            }
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertQuery(sql)
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .fails(sql.indexOf("missing"), "Invalid column: missing");
                Assert.assertTrue(compiler.getBindScopeCount() > MIN_RETAINED_DEPTH);
                compiler.clear();
                Assert.assertTrue(compiler.getBindScopeCount() > MIN_RETAINED_DEPTH);
                clearPastRetention(compiler);
                Assert.assertEquals(MIN_RETAINED_DEPTH, compiler.getBindScopeCount());

                assertQuery("SELECT x FROM long_sequence(2) WHERE x = (SELECT max(x) FROM long_sequence(2))")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .returns("""
                                x
                                2
                                """);
                compiler.clear();
                compileAndClose(compiler, sql.replace("missing", "x"));
                Assert.assertTrue(compiler.getBindScopeCount() > MIN_RETAINED_DEPTH);
                compiler.clear();
                clearPastRetention(compiler);
                Assert.assertEquals(MIN_RETAINED_DEPTH, compiler.getBindScopeCount());
            }
        });
    }

    @Test
    public void testDeepCursorNestingReturnsGenerationFrames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s (sym SYMBOL)");
            execute("INSERT INTO s VALUES ('a'), ('b')");
            String sql = "SELECT sym FROM s";
            for (int i = 0; i < MIN_RETAINED_DEPTH + 8; i++) {
                sql = "SELECT sym FROM s WHERE sym IN (" + sql + ")";
            }
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compileAndClose(compiler, sql);
                Assert.assertTrue(compiler.getGenerationFrameCount() > MIN_RETAINED_DEPTH);
                compiler.clear();
                Assert.assertTrue(compiler.getGenerationFrameCount() > MIN_RETAINED_DEPTH);
                clearPastRetention(compiler);
                Assert.assertEquals(MIN_RETAINED_DEPTH, compiler.getGenerationFrameCount());

                assertQuery("SELECT sym FROM s WHERE sym IN (SELECT sym FROM s WHERE sym = 'b')")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .returns("""
                                sym
                                b
                                """);
                Assert.assertEquals(MIN_RETAINED_DEPTH, compiler.getGenerationFrameCount());
            }
        });
    }

    @Test
    public void testDeepGenerationFailureReturnsFrames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s (sym SYMBOL)");
            execute("INSERT INTO s VALUES ('a'), ('b')");
            String sql = "SELECT sym FROM s WHERE length(sym) > 0";
            for (int i = 0; i < MIN_RETAINED_DEPTH + 8; i++) {
                sql = "SELECT sym FROM s WHERE sym IN (" + sql + ")";
            }
            final RuntimeException failure = new IllegalStateException("generation failure");
            try (
                    RetentionCompiler compiler = new RetentionCompiler();
                    SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1) {
                        @Override
                        public boolean isParquetRowGroupPruningEnabled() {
                            if (compiler.getGenerationDepth() > MIN_RETAINED_DEPTH) {
                                throw failure;
                            }
                            return super.isParquetRowGroupPruningEnabled();
                        }
                    }
            ) {
                context.with(sqlExecutionContext.getSecurityContext());
                try (RecordCursorFactory ignored = compiler.compile(sql, context).getRecordCursorFactory()) {
                    Assert.fail("generation must fail");
                } catch (RuntimeException e) {
                    Assert.assertSame(failure, e);
                }
                Assert.assertEquals(0, compiler.getGenerationDepth());
                Assert.assertTrue(compiler.getGenerationFrameCount() > MIN_RETAINED_DEPTH);
                compiler.clear();
                Assert.assertTrue(compiler.getGenerationFrameCount() > MIN_RETAINED_DEPTH);
                clearPastRetention(compiler);
                Assert.assertEquals(MIN_RETAINED_DEPTH, compiler.getGenerationFrameCount());

                assertQuery("SELECT sym FROM s WHERE sym IN (SELECT sym FROM s WHERE sym = 'b')")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .returns("""
                                sym
                                b
                                """);
                compiler.clear();
                compileAndClose(compiler, sql);
                Assert.assertTrue(compiler.getGenerationFrameCount() > MIN_RETAINED_DEPTH);
                compiler.clear();
                clearPastRetention(compiler);
                Assert.assertEquals(MIN_RETAINED_DEPTH, compiler.getGenerationFrameCount());
            }
        });
    }

    @Test
    public void testDeepNestingReturnsBindScopes() throws Exception {
        assertMemoryLeak(() -> {
            String sql = "SELECT x FROM long_sequence(1)";
            for (int i = 0; i < MIN_RETAINED_DEPTH + 8; i++) {
                sql = "SELECT x FROM long_sequence(1) WHERE x = (" + sql + ")";
            }
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compileAndClose(compiler, sql);
                Assert.assertEquals(MIN_RETAINED_DEPTH + 9, compiler.getBindScopeCount());
                compiler.clear();
                Assert.assertEquals(MIN_RETAINED_DEPTH + 9, compiler.getBindScopeCount());
                clearPastRetention(compiler);
                Assert.assertEquals(MIN_RETAINED_DEPTH, compiler.getBindScopeCount());

                assertQuery("SELECT x FROM long_sequence(2) WHERE x = (SELECT max(x) FROM long_sequence(2))")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .returns("""
                                x
                                2
                                """);
            }
        });
    }

    @Test
    public void testFailedCteReparseLeavesNoLexerStash() throws Exception {
        assertMemoryLeak(() -> {
            final String sql = "WITH c AS (SELECT @x v FROM long_sequence(1)) SELECT * FROM c UNION ALL SELECT * FROM (DECLARE @y := 1 SELECT * FROM c)";
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (int i = 0; i < 3; i++) {
                    try {
                        compileAndClose(compiler, sql);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals(18, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column: @x");
                    }
                    Assert.assertEquals(0, compiler.getLexerStashSize());
                }
            }
        });
    }

    @Test
    public void testIdleViewLexersReturnToFloor() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG)");
            execute("INSERT INTO t VALUES (1), (2)");
            final int floor = configuration.getSqlLexerPoolCapacity();
            final StringSink view = new StringSink();
            view.put("CREATE VIEW wide AS (SELECT ");
            for (int i = 0; i < floor; i++) {
                if (i > 0) {
                    view.put(", ");
                }
                view.put("x + ").put(i).put(" c").put(i);
            }
            view.put(" FROM t)");
            execute(view);
            // the outer view holds its lexer while the inner one expands, so the wide body takes a second lexer
            execute("CREATE VIEW outer_wide AS (SELECT c0 FROM wide)");
            execute("CREATE VIEW narrow AS (SELECT x FROM t)");
            drainWalAndViewQueues();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compileAndClose(compiler, "SELECT c0 FROM outer_wide");
                compiler.clear();
                Assert.assertTrue(compiler.getViewLexerMaxPoolCapacity() > floor);
                for (int i = 0; i < RetainedCapacity.USES_TO_FLOOR; i++) {
                    compileAndClose(compiler, "SELECT x FROM narrow");
                    compiler.clear();
                }
                Assert.assertEquals(floor, compiler.getViewLexerMaxPoolCapacity());
            }
        });
    }

    @Test
    public void testLongDeclaredValueAliasesReturnStore() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
            final int valueLength = 15_000;
            final int columnCount = 1_000;
            final StringSink sql = new StringSink();
            sql.put("DECLARE @s := '").repeat("x", valueLength).put("' SELECT ");
            for (int i = 0; i < columnCount; i++) {
                if (i > 0) {
                    sql.put(',');
                }
                sql.put("@s");
            }
            sql.put(" FROM long_sequence(1)");
            final int ceiling = configuration.getSqlCharacterStoreCapacity();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compileAndClose(compiler, sql);
                Assert.assertTrue(compiler.getCharacterStoreCapacity() > ceiling);
                Assert.assertTrue(compiler.getCharacterStoreCapacity() < valueLength * 50);
                compiler.clear();
                Assert.assertTrue(compiler.getCharacterStoreCapacity() > ceiling);
                clearPastRetention(compiler);
                Assert.assertEquals(ceiling, compiler.getCharacterStoreCapacity());

                assertQuery("DECLARE @s := 'abc' SELECT @s, @s FROM long_sequence(1)")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                'abc'\t'abc'_2
                                abc\tabc
                                """);
            }
        });
    }

    @Test
    public void testLongInListPreparesNoConstants() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_EXPRESSION_POOL_CAPACITY, "64");
            final int ceiling = configuration.getSqlExpressionPoolCapacity();
            final StringSink sql = new StringSink();
            sql.put("SELECT x FROM long_sequence(3) WHERE x IN (");
            for (int i = 0, n = ceiling + 1_000; i < n; i++) {
                if (i > 0) {
                    sql.put(", ");
                }
                sql.put(i);
            }
            sql.put(')');
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertQuery(sql)
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .returns("""
                                x
                                1
                                2
                                3
                                """);
                compiler.clear();
                Assert.assertTrue("IN-list constants must not take prepared function slots", compiler.getPreparedFunctionCapacity() <= ceiling);
            }
        });
    }

    @Test
    public void testMinimalPoolCapacitiesCompile() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_EXPRESSION_POOL_CAPACITY, "1");
            setProperty(PropertyKey.CAIRO_COLUMN_POOL_CAPACITY, "1");
            setProperty(PropertyKey.CAIRO_MODEL_POOL_CAPACITY, "1");
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_COLUMN_POOL_CAPACITY, "1");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertQuery("""
                        SELECT a.x, b.x y, row_number() OVER (ORDER BY a.x) n
                        FROM long_sequence(3) a
                        JOIN long_sequence(3) b ON a.x = b.x
                        WHERE a.x > 1""")
                        .withCompiler(compiler)
                        .expectSize()
                        .returns("""
                                x\ty\tn
                                2\t2\t1
                                3\t3\t2
                                """);
            }
        });
    }

    @Test
    public void testOrdinaryStatementsKeepCapacity() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE VIEW v AS (SELECT sym, price, ts FROM t WHERE price > 0)");
            execute("INSERT INTO t VALUES ('a', 1.5, '2024-01-01T00:00:00.000000Z'), ('b', 2.5, '2024-01-01T00:00:01.000000Z')");
            drainWalAndViewQueues();
            final String sql = "SELECT sym, sum(price) s FROM v WHERE sym IN (SELECT sym FROM t) ORDER BY sym";
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                final int queryModels = compiler.getQueryModelPoolCapacity();
                final int sqlNodes = compiler.getSqlNodePoolCapacity();
                final int viewLexers = compiler.getViewLexerCapacity();
                compileAndClose(compiler, sql);
                compiler.clear();
                final int characterStore = compiler.getCharacterStorePoolCapacity();
                final int viewLexerSequences = compiler.getViewLexerMaxPoolCapacity();
                final int generationFrames = compiler.getGenerationFrameCount();
                final int planColumns = compiler.getPlanColumnPoolCapacity();
                final int preparedFunctions = compiler.getPreparedFunctionCapacity();
                for (int i = 0; i < 3; i++) {
                    assertQuery(sql)
                            .withCompiler(compiler)
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    sym\ts
                                    a\t1.5
                                    b\t2.5
                                    """);
                    compiler.clear();
                    Assert.assertEquals(queryModels, compiler.getQueryModelPoolCapacity());
                    Assert.assertEquals(sqlNodes, compiler.getSqlNodePoolCapacity());
                    Assert.assertEquals(viewLexers, compiler.getViewLexerCapacity());
                    Assert.assertEquals(viewLexerSequences, compiler.getViewLexerMaxPoolCapacity());
                    Assert.assertEquals(characterStore, compiler.getCharacterStorePoolCapacity());
                    Assert.assertEquals(generationFrames, compiler.getGenerationFrameCount());
                    Assert.assertEquals(planColumns, compiler.getPlanColumnPoolCapacity());
                    Assert.assertEquals(preparedFunctions, compiler.getPreparedFunctionCapacity());
                }
            }
        });
    }

    @Test
    public void testPooledCompilerClearsOnReturn() throws Exception {
        assertMemoryLeak(() -> {
            final int ceiling = configuration.getSqlExpressionPoolCapacity();
            final SqlCompiler compiler = engine.getSqlCompiler();
            final SqlCompilerImpl delegate = (SqlCompilerImpl) ((SqlCompilerPool.C) compiler).getDelegate();
            final int grownCapacity;
            try {
                compileAndClose(compiler, wideSelect(ceiling + 1_000, true));
                grownCapacity = delegate.getSqlNodePoolCapacity();
                Assert.assertTrue(grownCapacity > ceiling);
            } finally {
                compiler.close();
            }
            Assert.assertTrue(delegate.getSqlNodePoolCapacity() > ceiling);
            Assert.assertTrue(delegate.getSqlNodePoolCapacity() < grownCapacity);
            clearPastRetention(delegate);
            Assert.assertEquals(ceiling, delegate.getSqlNodePoolCapacity());
            Assert.assertEquals(ceiling, delegate.getPlanColumnPoolCapacity());
        });
    }

    @Test
    public void testRecurringCteChainCreatesNoPooledObjects() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_MODEL_POOL_CAPACITY, "16");
            assertRecurringStatementCreatesNoPooledObjects(cteChain(4), SqlCompilerImpl::getQueryModelPoolCapacity, 16);
        });
    }

    @Test
    public void testRecurringWideSelectCreatesNoPooledObjects() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_EXPRESSION_POOL_CAPACITY, "64");
            assertRecurringStatementCreatesNoPooledObjects(wideSelect(100, true), SqlCompilerImpl::getSqlNodePoolCapacity, 64);
        });
    }

    @Test
    public void testUnaliasedWideSelectReturnsNames() throws Exception {
        assertMemoryLeak(() -> {
            final int ceiling = configuration.getSqlExpressionPoolCapacity();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compileAndClose(compiler, wideSelect(ceiling + 1_000, false));
                Assert.assertTrue(compiler.getCharacterStorePoolCapacity() > ceiling);
                compiler.clear();
                Assert.assertTrue(compiler.getCharacterStorePoolCapacity() > ceiling);
                clearPastRetention(compiler);
                Assert.assertEquals(ceiling, compiler.getCharacterStorePoolCapacity());
            }
        });
    }

    @Test
    public void testViewExpansionsReturnLexers() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG)");
            execute("INSERT INTO t VALUES (1), (2)");
            execute("CREATE VIEW v1 AS (SELECT x FROM t)");
            execute("CREATE VIEW v2 AS (SELECT x FROM v1 UNION ALL SELECT x FROM v1)");
            // A view expanded inside another holds its lexer until the outer expansion returns,
            // so a chain of views nested deeper than the pool's capacity grows the pool.
            final int ceiling = configuration.getViewLexerPoolCapacity();
            execute("CREATE VIEW c0 AS (SELECT x FROM v2)");
            for (int i = 1; i <= ceiling; i++) {
                execute("CREATE VIEW c" + i + " AS (SELECT x FROM c" + (i - 1) + ")");
            }
            drainWalAndViewQueues();
            final String sql = "SELECT count() FROM c" + ceiling;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compileAndClose(compiler, sql);
                Assert.assertTrue(compiler.getViewLexerCapacity() > ceiling);
                compiler.clear();
                Assert.assertTrue(compiler.getViewLexerCapacity() > ceiling);
                clearPastRetention(compiler);
                Assert.assertEquals(ceiling, compiler.getViewLexerCapacity());

                assertQuery("SELECT x FROM v2 ORDER BY x")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                x
                                1
                                1
                                2
                                2
                                """);
                Assert.assertEquals(ceiling, compiler.getViewLexerCapacity());
            }
        });
    }

    @Test
    public void testViewLexersStartSmall() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG)");
            execute("INSERT INTO t VALUES (1), (2)");
            final StringSink view = new StringSink();
            view.put("CREATE VIEW wide AS (SELECT ");
            for (int i = 0; i < 100; i++) {
                if (i > 0) {
                    view.put(", ");
                }
                view.put("x + ").put(i).put(" c").put(i);
            }
            view.put(" FROM t)");
            execute(view);
            execute("CREATE VIEW narrow AS (SELECT x FROM t)");
            drainWalAndViewQueues();
            final int lexerPoolCapacity = configuration.getSqlLexerPoolCapacity();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertQuery("SELECT x FROM narrow ORDER BY x")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                x
                                1
                                2
                                """);
                final int narrowSequences = compiler.getViewLexerMaxPoolCapacity();
                Assert.assertTrue(narrowSequences < lexerPoolCapacity);

                compileAndClose(compiler, "SELECT c99 FROM wide");
                final int wideSequences = compiler.getViewLexerMaxPoolCapacity();
                Assert.assertTrue(wideSequences > narrowSequences);
                Assert.assertTrue(wideSequences <= lexerPoolCapacity);
                compiler.clear();
                compileAndClose(compiler, "SELECT c99 FROM wide");
                compiler.clear();
                Assert.assertEquals(wideSequences, compiler.getViewLexerMaxPoolCapacity());
            }
        });
    }

    @Test
    public void testWideJoinReturnsJoinKeys() throws Exception {
        assertMemoryLeak(() -> {
            final int ceiling = configuration.getSqlJoinContextPoolCapacity();
            final int columnCount = ceiling + 8;
            final StringSink ddl = new StringSink();
            for (String table : new String[]{"a", "b", "c"}) {
                ddl.clear();
                ddl.put("CREATE TABLE ").put(table).put(" (");
                for (int i = 0; i < columnCount; i++) {
                    if (i > 0) {
                        ddl.put(", ");
                    }
                    ddl.put('k').put(i).put(" LONG");
                }
                ddl.put(')');
                execute(ddl);
            }
            final StringSink sql = new StringSink();
            sql.put("SELECT a.k0 FROM a JOIN c ON a.k0 = c.k0 JOIN b ON ");
            for (int i = 0; i < columnCount; i++) {
                if (i > 0) {
                    sql.put(" AND ");
                }
                sql.put("a.k").put(i).put(" = b.k").put(i);
            }
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compileAndClose(compiler, sql);
                compiler.clear();
                final int retained = compiler.getJoinEqualityCapacity();
                Assert.assertTrue(retained > ceiling);

                compileAndClose(compiler, "SELECT a.k0 FROM a JOIN b ON a.k0 = b.k0 JOIN c ON b.k1 = c.k1");
                compiler.clear();
                Assert.assertEquals(retained, compiler.getJoinEqualityCapacity());
                clearPastRetention(compiler);
                Assert.assertEquals(ceiling, compiler.getJoinEqualityCapacity());
            }
        });
    }

    @Test
    public void testWideSelectReturnsExpressions() throws Exception {
        assertMemoryLeak(() -> {
            final int ceiling = configuration.getSqlExpressionPoolCapacity();
            final StringSink sql = new StringSink();
            sql.put("SELECT ");
            for (int i = 0, n = ceiling + 1_000; i < n; i++) {
                if (i > 0) {
                    sql.put(", ");
                }
                sql.put("x + ").put(i).put(" c").put(i);
            }
            sql.put(" FROM long_sequence(1)");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compileAndClose(compiler, sql);
                Assert.assertTrue(compiler.getSqlNodePoolCapacity() > ceiling);
                Assert.assertTrue(compiler.getPlanColumnPoolCapacity() > ceiling);
                Assert.assertTrue(compiler.getPreparedFunctionCapacity() > ceiling);
                compiler.clear();
                Assert.assertTrue(compiler.getSqlNodePoolCapacity() > ceiling);
                Assert.assertTrue(compiler.getPlanColumnPoolCapacity() > ceiling);
                Assert.assertTrue(compiler.getPreparedFunctionCapacity() > ceiling);
                clearPastRetention(compiler);
                Assert.assertEquals(ceiling, compiler.getSqlNodePoolCapacity());
                Assert.assertEquals(ceiling, compiler.getPlanColumnPoolCapacity());
                Assert.assertEquals(ceiling, compiler.getPreparedFunctionCapacity());

                assertQuery("SELECT x + 1 c0, x + 2 c1 FROM long_sequence(2)")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                c0\tc1
                                2\t3
                                3\t4
                                """);
                Assert.assertEquals(ceiling, compiler.getSqlNodePoolCapacity());
            }
        });
    }

    @Test
    public void testWideWithReturnsWithClauseCopies() throws Exception {
        assertMemoryLeak(() -> {
            final int ceiling = configuration.getWithClauseModelPoolCapacity();
            final StringSink sql = new StringSink();
            sql.put("WITH c0 AS (SELECT 0 x FROM long_sequence(1))");
            for (int i = 1, n = ceiling + 8; i < n; i++) {
                sql.put(", c").put(i).put(" AS (SELECT ").put(i).put(" x FROM long_sequence(1))");
            }
            sql.put(" SELECT x FROM c").put(ceiling + 7);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compileAndClose(compiler, sql);
                Assert.assertTrue(compiler.getWithClausesCopyPoolCapacity() > ceiling);
                compiler.clear();
                Assert.assertTrue(compiler.getWithClausesCopyPoolCapacity() > ceiling);
                clearPastRetention(compiler);
                Assert.assertEquals(ceiling, compiler.getWithClausesCopyPoolCapacity());

                assertQuery("WITH a AS (SELECT 1 x FROM long_sequence(1)), b AS (SELECT x + 1 x FROM a) SELECT x FROM b")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                x
                                2
                                """);
                Assert.assertEquals(ceiling, compiler.getWithClausesCopyPoolCapacity());
            }
        });
    }

    @Test
    public void testWindowExpressionsRetainColumnCapacity() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_COLUMN_POOL_CAPACITY, "2");
            setProperty(PropertyKey.CAIRO_COLUMN_POOL_CAPACITY, "8");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                Assert.assertEquals(2, compiler.getWindowExpressionPoolCapacity());
                compileAndClose(compiler, windowSelect(6));
                Assert.assertTrue(compiler.getWindowExpressionPoolCapacity() >= 6);
                compiler.clear();
                Assert.assertEquals(8, compiler.getWindowExpressionPoolCapacity());

                compileAndClose(compiler, windowSelect(9));
                Assert.assertTrue(compiler.getWindowExpressionPoolCapacity() >= 9);
                compiler.clear();
                final int retained = compiler.getWindowExpressionPoolCapacity();
                Assert.assertTrue(retained >= 9);

                compileAndClose(compiler, windowSelect(6));
                compiler.clear();
                Assert.assertEquals(retained, compiler.getWindowExpressionPoolCapacity());
                clearPastRetention(compiler);
                Assert.assertEquals(8, compiler.getWindowExpressionPoolCapacity());

                assertQuery("SELECT x, row_number() OVER (ORDER BY x DESC) n FROM long_sequence(3) ORDER BY x")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                x\tn
                                1\t3
                                2\t2
                                3\t1
                                """);
            }
        });
    }

    private static void assertRecurringStatementCreatesNoPooledObjects(
            CharSequence sql,
            ToIntFunction<SqlCompilerImpl> grownCapacity,
            int ceiling
    ) throws SqlException {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            compileAndClose(compiler, sql);
            compiler.clear();
            Assert.assertTrue(grownCapacity.applyAsInt(compiler) > ceiling);
            for (int i = 0, n = 3 * RetainedCapacity.WINDOW_USES; i < n; i++) {
                final String retained = capacities(compiler);
                compileAndClose(compiler, sql);
                Assert.assertEquals(retained, capacities(compiler));
                compiler.clear();
            }
            Assert.assertTrue(grownCapacity.applyAsInt(compiler) > ceiling);
        }
    }

    // the pools grow only by creating objects, so an unchanged capacity means the compile created none
    private static String capacities(SqlCompilerImpl compiler) {
        return "bindScopes=" + compiler.getBindScopeCount()
                + ", characters=" + compiler.getCharacterStoreCapacity()
                + ", characterSequences=" + compiler.getCharacterStorePoolCapacity()
                + ", generationFrames=" + compiler.getGenerationFrameCount()
                + ", joinEqualities=" + compiler.getJoinEqualityCapacity()
                + ", planColumns=" + compiler.getPlanColumnPoolCapacity()
                + ", preparedFunctions=" + compiler.getPreparedFunctionCapacity()
                + ", queryModels=" + compiler.getQueryModelPoolCapacity()
                + ", sqlNodes=" + compiler.getSqlNodePoolCapacity()
                + ", subqueries=" + compiler.getSubqueryPoolCapacity()
                + ", viewLexers=" + compiler.getViewLexerCapacity()
                + ", viewLexerSequences=" + compiler.getViewLexerMaxPoolCapacity()
                + ", windowExpressions=" + compiler.getWindowExpressionPoolCapacity()
                + ", withClauseCopies=" + compiler.getWithClausesCopyPoolCapacity();
    }

    private static void clearPastRetention(SqlCompilerImpl compiler) {
        for (int i = 0, n = 2 * RetainedCapacity.WINDOW_USES; i < n; i++) {
            compiler.clear();
        }
    }

    private static void compileAndClose(SqlCompiler compiler, CharSequence sql) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.assertNotNull(factory);
        }
    }

    private static StringSink cteChain(int levels) {
        final StringSink sql = new StringSink();
        sql.put("WITH c0 AS (SELECT x l FROM long_sequence(3))");
        for (int i = 1; i <= levels; i++) {
            sql.put(", c").put(i).put(" AS (SELECT * FROM c").put(i - 1).put(" UNION ALL SELECT * FROM c").put(i - 1).put(')');
        }
        sql.put(" SELECT count() FROM c").put(levels);
        return sql;
    }

    private static StringSink wideSelect(int columnCount, boolean isAliased) {
        final StringSink sql = new StringSink();
        sql.put("SELECT ");
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                sql.put(", ");
            }
            sql.put("x + ").put(i);
            if (isAliased) {
                sql.put(" c").put(i);
            }
        }
        sql.put(" FROM long_sequence(1)");
        return sql;
    }

    private static StringSink windowSelect(int windowCount) {
        final StringSink sql = new StringSink();
        sql.put("SELECT ").repeat("sum(x) OVER (), ", windowCount - 1).put("sum(x) OVER () FROM long_sequence(1)");
        return sql;
    }

    private static final class RetentionCompiler extends SqlCompilerImpl {
        private RetentionCompiler() {
            super(SqlCompilerRetentionTest.engine);
        }

        private int getGenerationDepth() {
            return codeGenerator.getGenerationDepth();
        }
    }
}
