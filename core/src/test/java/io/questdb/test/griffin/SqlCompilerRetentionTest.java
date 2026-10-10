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
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A statement that grows the compiler's pools past their configured capacities leaves them back at those capacities
 * once the compiler clears, and ordinary statements leave the pools untouched.
 */
public class SqlCompilerRetentionTest extends AbstractCairoTest {
    private static final int MAX_RETAINED_DEPTH = 32;

    @Test
    public void testCteChainReturnsQueryModels() throws Exception {
        assertMemoryLeak(() -> {
            final StringSink sql = new StringSink();
            sql.put("WITH c0 AS (SELECT x l FROM long_sequence(3))");
            for (int i = 1; i <= 8; i++) {
                sql.put(", c").put(i).put(" AS (SELECT * FROM c").put(i - 1).put(" UNION ALL SELECT * FROM c").put(i - 1).put(')');
            }
            sql.put(" SELECT count() FROM c8");
            final int ceiling = configuration.getSqlModelPoolCapacity();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compileAndClose(compiler, sql);
                Assert.assertTrue(compiler.getQueryModelPoolCapacity() > ceiling);
                compiler.clear();
                Assert.assertEquals(ceiling, compiler.getQueryModelPoolCapacity());

                assertQuery("SELECT count() FROM (SELECT x FROM long_sequence(3) UNION ALL SELECT x FROM long_sequence(3))")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count
                                6
                                """);
                Assert.assertEquals(ceiling, compiler.getQueryModelPoolCapacity());
            }
        });
    }

    @Test
    public void testDeepCursorNestingReturnsGenerationFrames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s (sym SYMBOL)");
            execute("INSERT INTO s VALUES ('a'), ('b')");
            String sql = "SELECT sym FROM s";
            for (int i = 0; i < MAX_RETAINED_DEPTH + 8; i++) {
                sql = "SELECT sym FROM s WHERE sym IN (" + sql + ")";
            }
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compileAndClose(compiler, sql);
                Assert.assertTrue(compiler.getGenerationFrameCount() > MAX_RETAINED_DEPTH);
                compiler.clear();
                Assert.assertEquals(MAX_RETAINED_DEPTH, compiler.getGenerationFrameCount());

                assertQuery("SELECT sym FROM s WHERE sym IN (SELECT sym FROM s WHERE sym = 'b')")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .returns("""
                                sym
                                b
                                """);
                Assert.assertEquals(MAX_RETAINED_DEPTH, compiler.getGenerationFrameCount());
            }
        });
    }

    @Test
    public void testDeepNestingReturnsBindScopes() throws Exception {
        assertMemoryLeak(() -> {
            String sql = "SELECT x FROM long_sequence(1)";
            for (int i = 0; i < MAX_RETAINED_DEPTH + 8; i++) {
                sql = "SELECT x FROM long_sequence(1) WHERE x = (" + sql + ")";
            }
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compileAndClose(compiler, sql);
                Assert.assertEquals(MAX_RETAINED_DEPTH + 9, compiler.getBindScopeCount());
                compiler.clear();
                Assert.assertEquals(MAX_RETAINED_DEPTH, compiler.getBindScopeCount());

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
                        TestUtils.assertContains(e.getFlyweightMessage(), "tried to use undeclared variable `@x`");
                    }
                    Assert.assertEquals(0, compiler.getLexerStashSize());
                }
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
            try {
                compileAndClose(compiler, wideSelect(ceiling + 1_000, true));
                Assert.assertTrue(delegate.getSqlNodePoolCapacity() > ceiling);
            } finally {
                compiler.close();
            }
            Assert.assertEquals(ceiling, delegate.getSqlNodePoolCapacity());
            Assert.assertEquals(ceiling, delegate.getPlanColumnPoolCapacity());
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
            drainWalAndViewQueues();
            final int ceiling = configuration.getViewLexerPoolCapacity();
            final StringSink sql = new StringSink();
            sql.put("SELECT count() FROM (");
            for (int i = 0; i < ceiling; i++) {
                if (i > 0) {
                    sql.put(" UNION ALL ");
                }
                sql.put("SELECT x FROM v2");
            }
            sql.put(')');
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compileAndClose(compiler, sql);
                Assert.assertTrue(compiler.getViewLexerCapacity() > ceiling);
                compiler.clear();
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
                Assert.assertEquals(ceiling, compiler.getJoinEqualityCapacity());

                compileAndClose(compiler, "SELECT a.k0 FROM a JOIN b ON a.k0 = b.k0 JOIN c ON b.k1 = c.k1");
                compiler.clear();
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

    private static void compileAndClose(SqlCompiler compiler, CharSequence sql) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.assertNotNull(factory);
        }
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
}
