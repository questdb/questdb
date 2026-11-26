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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.ToLongFunction;

/**
 * Every non-equi RIGHT/FULL OUTER join pins all models before it, so a chain of such joins gives the
 * level a quadratic number of ordering edges. reorderTables asks isJoinedAfter about the INNER joins
 * behind each CROSS join once per CROSS join of the level, and isJoinedAfter used to rescan every
 * ordering edge at every model it visited. Compile time grew as the fifth power of the table count:
 * about 4 s for 160 tables and 2 minutes for 319, and no query timeout could stop it.
 * <p>
 * The tests count the ordering edges that isJoinedAfter reads, which grow as the cube of the chain
 * length: a model at position p has O(p) in-edges, so one search reads O(V^2) edges, and a pass asks
 * about O(V) distinct pairs. Losing the answers memo repeats every search once per CROSS join, and
 * losing the edge index reads all O(V^2) edges at each visited model; either makes the count grow as
 * the fourth power. Doubling the chain multiplies the count by 8 in the first case and by 16 in the
 * second, so the tests require a factor below 12.
 * <p>
 * A time-series join after a chain of INNER joins that reorderTables keys back to the FROM table makes
 * runFromModelFirst place every table of the chain again. It used to look, at every placement, for a key
 * or a deferred edge between each table it had not placed and each table it had placed, scanning every
 * deferred edge of the level for each pair: the work grew as the cube of the chain length, times the
 * number of deferred edges. Compiling took about 6 s for a chain of 1,600 tables, and about 7 s for a
 * chain of 400 tables followed by a RIGHT JOIN and 400 INNER joins that each compare two tables of the
 * chain. The testTimeSeriesJoinAfterKeyMove tests count the links that runFromModelFirst adds and
 * follows, which grow with the number of keys and deferred edges: doubling the chain doubles them, so
 * the tests require a factor below 3.
 * <p>
 * constrainRightAndFullJoinsAfterPrefix pins the models before every RIGHT/FULL OUTER join, with join
 * keys or without, so a chain of equi outer joins has a quadratic number of ordering edges as well.
 * reorderTables orders the level once per CROSS join, and every pass used to apply all ordering edges
 * and walk them in doReorderTables. Compile time grew as the cube of the table count: about 3 s for
 * 1,280 tables with a CROSS join before every FULL join, against 0.2 s before the outer joins pinned
 * their prefixes. A pass that moves no join key would order the graph that the pass before it ordered, so
 * reorderTables skips it. The testCrossJoins tests count the ordering edges in force at each pass that
 * applyModelOnOrderingConstraints prepares, which grow as the square of the chain length when no key moves:
 * doubling the chain multiplies the count by 4, and by 8 when every pass orders the level again, so the tests
 * require a factor below 6.
 * <p>
 * Each of those pins used to take an entry in the join context parents of the outer join, in the dependencies of
 * the pinned model and in the list of recorded ordering edges, which kept arrays of quadratic size in every pooled
 * compiler: some 250 MB for a chain of 2,560 FULL joins. SqlOptimiser keeps the pins in OuterJoinOrderingEdges, a
 * bit per outer join whose whole prefix it pins and a bit per pinned model of the others, so the join contexts,
 * dependencies and lists grow linearly with the chain.
 */
public class JoinReorderCompileTimeTest extends AbstractCairoTest {

    @Test
    public void testCrossJoinsBeforeEquiFullJoins() throws Exception {
        // t0 CROSS JOIN t1 FULL JOIN t2 ON t2.k = t1.k CROSS JOIN t3 FULL JOIN t4 ON t4.k = t3.k ...
        assertOrderingEdgeApplicationsGrowAsSquare(80, 2, "FULL", (sb, a) -> sb
                .append(" CROSS JOIN t").append(a)
                .append(" FULL JOIN t").append(a + 1).append(" ON t").append(a + 1).append(".k = t").append(a).append(".k"));
    }

    @Test
    public void testCrossJoinsBeforeInnerAndEquiRightJoins() throws Exception {
        // t0 CROSS JOIN t1 JOIN t2 ON t2.k = t1.k RIGHT JOIN t3 ON t3.k = t2.k CROSS JOIN t4 ...
        assertOrderingEdgeApplicationsGrowAsSquare(60, 3, "RIGHT", (sb, a) -> sb
                .append(" CROSS JOIN t").append(a)
                .append(" JOIN t").append(a + 1).append(" ON t").append(a + 1).append(".k = t").append(a).append(".k")
                .append(" RIGHT JOIN t").append(a + 2).append(" ON t").append(a + 2).append(".k = t").append(a + 1).append(".k"));
    }

    @Test
    public void testInnerJoinKeyedToCrossJoinBehindNonEquiFullJoins() throws Exception {
        // t0 CROSS JOIN t1 JOIN t2 ON t2.k = t1.k FULL JOIN t3 ON t3.k > t2.k CROSS JOIN t4 ...
        assertEdgeReadsGrowAsCube(80, 3, "FULL", (sb, a) -> sb
                .append(" CROSS JOIN t").append(a)
                .append(" JOIN t").append(a + 1).append(" ON t").append(a + 1).append(".k = t").append(a).append(".k")
                .append(" FULL JOIN t").append(a + 2).append(" ON t").append(a + 2).append(".k > t").append(a + 1).append(".k"));
    }

    @Test
    public void testInnerJoinKeyedToTwoTablesBehindNonEquiRightJoins() throws Exception {
        // t0 CROSS JOIN t1 JOIN t2 ON t2.k = t1.k JOIN t3 ON t3.k = t1.k AND t3.j = t2.j RIGHT JOIN t4 ON t4.k > t3.k ...
        // t3 follows t1 through t2, so isJoinedAfter keeps the key t3.k = t1.k on t3 and reorderTables
        // asks again for every CROSS join of the level.
        assertEdgeReadsGrowAsCube(60, 4, "RIGHT", (sb, a) -> sb
                .append(" CROSS JOIN t").append(a)
                .append(" JOIN t").append(a + 1).append(" ON t").append(a + 1).append(".k = t").append(a).append(".k")
                .append(" JOIN t").append(a + 2).append(" ON t").append(a + 2).append(".k = t").append(a).append(".k AND t")
                .append(a + 2).append(".j = t").append(a + 1).append(".j")
                .append(" RIGHT JOIN t").append(a + 3).append(" ON t").append(a + 3).append(".k > t").append(a + 2).append(".k"));
    }

    @Test
    public void testNonEquiFullAndInnerJoinChainKeepsEdgesCompact() throws Exception {
        // t0 FULL JOIN t1 ON t1.k > t0.k JOIN t2 ON t2.k = t1.k FULL JOIN t3 ON t3.k > t2.k ...: each of the B
        // non-equi FULL joins pins its whole prefix, 1 + 3 + ... + (2B - 1) = B * B pins, and runs before every later
        // INNER join, which consumes its rows: B (B + 1) / 2 consumer edges. The join contexts hold the key of each
        // INNER join, and the dependencies those keys and the link of each FULL join to the model before it.
        assertMemoryLeak(() -> {
            createChainTables(80);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (int boundaryCount = 20; boundaryCount <= 40; boundaryCount += 20) {
                    final long pinCount = (long) boundaryCount * boundaryCount;
                    assertPinFootprint(
                            compiler,
                            nonEquiFullAndInnerChain(boundaryCount),
                            boundaryCount,
                            2L * boundaryCount,
                            pinCount,
                            0,
                            pinCount + boundaryCount * (boundaryCount + 1L) / 2
                    );
                }
            }
            assertQuery(nonEquiFullAndInnerChain(40))
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            0
                            """);
        });
    }

    @Test
    public void testRepeatedCompileReusesOrderingEdgeBuffers() throws Exception {
        // A second compile of the same chains fits in the buffers that the first one grew: the optimiser keeps
        // them and starts every level and statement from empty, without growing them again.
        assertMemoryLeak(() -> {
            createChainTables(202);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compileAndCountReads(compiler, fullChain(200), SqlCompilerImpl::getOrderingEdgeApplicationsForTesting);
                compileAndCountReads(compiler, forwardReadFullChain(200), SqlCompilerImpl::getOrderingEdgeApplicationsForTesting);
                final int capacity = compiler.getOrderingEdgeCapacityForTesting();
                for (int i = 0; i < 3; i++) {
                    compileAndCountReads(compiler, fullChain(200), SqlCompilerImpl::getOrderingEdgeApplicationsForTesting);
                    compileAndCountReads(compiler, forwardReadFullChain(200), SqlCompilerImpl::getOrderingEdgeApplicationsForTesting);
                    Assert.assertEquals(capacity, compiler.getOrderingEdgeCapacityForTesting());
                }
            }
        });
    }

    @Test
    public void testRightAndFullJoinPinsStayCompact() throws Exception {
        // Every FULL join of t0 FULL JOIN t1 ON t1.k = t0.k FULL JOIN t2 ON t2.k = t1.k ... pins its whole prefix,
        // J(J + 1) / 2 pins for J joins. In t0 JOIN t1 ON t1.k = t(J+2).k FULL JOIN t2 ON t2.k = t0.k ... FULL JOIN
        // t(J+1) ON t(J+1).k = t0.k JOIN t(J+2) ON t(J+2).k = t0.k, t1 reads a table that every FULL join precedes,
        // so each FULL join pins its prefix except t1: J(J + 1) / 2 pins again, which take a bit each, in blocks of
        // 64 joins that hold a word per model below the last join of the block. The join contexts and dependencies
        // hold the join keys and the deferred edge 0 -> 1 only, and no pin enters the list of recorded edges.
        assertMemoryLeak(() -> {
            createChainTables(202);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (int joinCount = 100; joinCount <= 200; joinCount += 100) {
                    final long pinCount = joinCount * (joinCount + 1L) / 2;
                    assertPinFootprint(compiler, fullChain(joinCount), joinCount, joinCount, pinCount, 0, pinCount);
                    // blocks of the joins 2 to 65, 66 to 101, and, for 200 joins, 66 to 129, 130 to 193 and 194 to 201
                    final int pinWordCount = joinCount == 100 ? 65 + 101 : 65 + 129 + 193 + 201;
                    assertPinFootprint(compiler, forwardReadFullChain(joinCount), joinCount + 2, joinCount + 2, pinCount, pinWordCount, pinCount + 1);
                }
            }
            assertQuery(forwardReadFullChain(200))
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            0
                            """);
        });
    }

    @Test
    public void testTimeSeriesJoinAfterKeyMoveOnLongChain() throws Exception {
        // trades t CROSS JOIN c0 JOIN c1 ON c1.p = c0.id ... JOIN cN ON cN.p = cN-1.id AND cN.sym = t.sym
        // ASOF JOIN quotes q ON (q.sym = t.sym): reorderTables moves cN.sym = t.sym onto trades, and
        // runFromModelFirst places trades and the whole chain again
        assertLinkReadsGrowLinearly(200, 0);
    }

    @Test
    public void testTimeSeriesJoinAfterKeyMoveOnLongChainWithDeferredKeys() throws Exception {
        // the same chain, then RIGHT JOIN r ON r.id = c0.id and JOIN zj ON zj.id = r.id AND ci.sym = ci-1.sym
        // for every link of the chain: the RIGHT JOIN NULL-extends the chain, so the optimiser filters on
        // ci.sym = ci-1.sym after it and keeps a deferred edge between the two tables of the chain
        assertLinkReadsGrowLinearly(100, 100);
    }

    @Test
    public void testKeyMoveInLaterPassReordersLevel() throws Exception {
        // The first pass of reorderTables skips the CROSS join t0 and moves t4.b = t3.b onto t3. The second
        // pass moves t4.a = t0.a onto t0, which turns the CROSS join of t0 into a hash join. The second
        // pass must order the level again even though the first pass ordered it: with the order of the
        // first pass, t0 is a CROSS join.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (a INT)");
            execute("CREATE TABLE t1 (k INT)");
            execute("CREATE TABLE t2 (k INT, c INT)");
            execute("CREATE TABLE t3 (b INT)");
            execute("CREATE TABLE t4 (a INT, b INT, c INT)");
            execute("INSERT INTO t0 VALUES (1), (2)");
            execute("INSERT INTO t1 VALUES (1), (2)");
            execute("INSERT INTO t2 VALUES (1, 10), (2, 20)");
            execute("INSERT INTO t3 VALUES (100), (200)");
            execute("INSERT INTO t4 VALUES (1, 100, 10), (2, 200, 20), (1, 200, 20)");
            assertQuery("""
                    SELECT t0.a, t1.k, t2.c, t3.b, t4.a a4, t4.b b4, t4.c c4
                    FROM t0
                    CROSS JOIN t1
                    JOIN t2 ON t2.k = t1.k
                    CROSS JOIN t3
                    JOIN t4 ON t4.a = t0.a AND t4.b = t3.b AND t4.c = t2.c
                    ORDER BY a4, b4, c4
                    """)
                    .noLeakCheck()
                    .withPlan("""
                            Encode sort
                              keys: [a4, b4, c4]
                                SelectedRecord
                                    Hash Join Light
                                      condition: t3.b=t4.b
                                        Hash Join Light
                                          condition: t0.a=t4.a
                                            Hash Join Light
                                              condition: t4.c=t2.c
                                                Hash Join Light
                                                  condition: t2.k=t1.k
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: t1
                                                    Hash
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: t2
                                                Hash
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: t4
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: t0
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t3
                            """)
                    .returns("""
                            a\tk\tc\tb\ta4\tb4\tc4
                            1\t1\t10\t100\t1\t100\t10
                            1\t2\t20\t200\t1\t200\t20
                            2\t2\t20\t200\t2\t200\t20
                            """);
        });
    }

    private static String chain(int blocks, int blockSize, BlockAppender appender) {
        final StringBuilder sb = new StringBuilder("SELECT count(*) FROM t0");
        for (int b = 0; b < blocks; b++) {
            appender.append(sb, b * blockSize + 1);
        }
        return sb.toString();
    }

    // t0 JOIN t1 ON t1.k = t(J+2).k FULL JOIN t2 ON t2.k = t0.k ... FULL JOIN t(J+1) ON t(J+1).k = t0.k JOIN t(J+2) ON t(J+2).k = t0.k
    private static String forwardReadFullChain(int joinCount) {
        final StringBuilder sb = new StringBuilder("SELECT count(*) FROM t0 JOIN t1 ON t1.k = t").append(joinCount + 2).append(".k");
        for (int t = 2; t <= joinCount + 1; t++) {
            sb.append(" FULL JOIN t").append(t).append(" ON t").append(t).append(".k = t0.k");
        }
        return sb.append(" JOIN t").append(joinCount + 2).append(" ON t").append(joinCount + 2).append(".k = t0.k").toString();
    }

    // t0 FULL JOIN t1 ON t1.k = t0.k FULL JOIN t2 ON t2.k = t1.k ... FULL JOIN tJ ON tJ.k = t(J-1).k
    private static String fullChain(int joinCount) {
        final StringBuilder sb = new StringBuilder("SELECT count(*) FROM t0");
        for (int t = 1; t <= joinCount; t++) {
            sb.append(" FULL JOIN t").append(t).append(" ON t").append(t).append(".k = t").append(t - 1).append(".k");
        }
        return sb.toString();
    }

    // the join level of the optimised model: the first model on the nested chain that joins several models
    private static IQueryModel joinLevel(IQueryModel model) {
        while (model.getJoinModels().size() < 2) {
            model = model.getNestedModel();
        }
        return model;
    }

    // t0 FULL JOIN t1 ON t1.k > t0.k JOIN t2 ON t2.k = t1.k ... FULL JOIN t(2B-1) ON t(2B-1).k > t(2B-2).k JOIN t2B ON t2B.k = t(2B-1).k
    private static String nonEquiFullAndInnerChain(int boundaryCount) {
        final StringBuilder sb = new StringBuilder("SELECT count(*) FROM t0");
        for (int t = 1; t <= 2 * boundaryCount; t++) {
            sb.append(t % 2 == 1 ? " FULL JOIN t" : " JOIN t").append(t).append(" ON t").append(t)
                    .append(t % 2 == 1 ? ".k > t" : ".k = t").append(t - 1).append(".k");
        }
        return sb.toString();
    }

    private static String timeSeriesJoinAfterChain(int chainLength, int deferredKeyCount) {
        final StringBuilder sb = new StringBuilder("SELECT count(*) FROM trades t CROSS JOIN c0");
        for (int i = 1; i <= chainLength; i++) {
            sb.append(" JOIN c").append(i).append(" ON c").append(i).append(".p = c").append(i - 1).append(".id");
        }
        sb.append(" AND c").append(chainLength).append(".sym = t.sym ASOF JOIN quotes q ON (q.sym = t.sym)");
        if (deferredKeyCount > 0) {
            sb.append(" RIGHT JOIN r ON r.id = c0.id");
            for (int j = 1; j <= deferredKeyCount; j++) {
                final int i = 1 + (j - 1) % chainLength;
                sb.append(" JOIN z").append(j).append(" ON z").append(j).append(".id = r.id AND c").append(i)
                        .append(".sym = c").append(i - 1).append(".sym");
            }
        }
        return sb.toString();
    }

    private void assertChainCountGrowsBelow(
            int blocks,
            int blockSize,
            String outerJoin,
            BlockAppender appender,
            ToLongFunction<SqlCompilerImpl> counter,
            String countName,
            String reachedMethod,
            String growth,
            int maxFactor
    ) throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0, n = 1 + blocks * blockSize; i < n; i++) {
                execute("CREATE TABLE t" + i + " (k INT, j INT)");
            }
            final String smallSql = chain(blocks / 2, blockSize, appender);
            final String largeSql = chain(blocks, blockSize, appender);
            Assert.assertTrue(largeSql.contains(outerJoin + " JOIN"));
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                final long smallCount = compileAndCountReads(compiler, smallSql, counter);
                final long largeCount = compileAndCountReads(compiler, largeSql, counter);
                Assert.assertTrue("the chain must reach " + reachedMethod + ", " + countName + ": " + smallCount, smallCount > 0);
                Assert.assertTrue(
                        countName + " must grow as " + growth + " of the chain length, "
                                + (1 + blocks / 2 * blockSize) + " tables: " + smallCount + ", "
                                + (1 + blocks * blockSize) + " tables: " + largeCount,
                        largeCount < maxFactor * smallCount
                );
            }
            assertQuery(largeSql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            0
                            """);
        });
    }

    private void assertEdgeReadsGrowAsCube(int blocks, int blockSize, String outerJoin, BlockAppender appender) throws Exception {
        assertChainCountGrowsBelow(
                blocks, blockSize, outerJoin, appender, SqlCompilerImpl::getKeyMoveEdgeReadsForTesting,
                "edge reads", "isJoinedAfter", "the cube", 12
        );
    }

    private void assertLinkReadsGrowLinearly(int chainLength, int deferredKeyCount) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE quotes (sym SYMBOL, bid DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE r (id INT)");
            for (int i = 0; i <= chainLength; i++) {
                execute("CREATE TABLE c" + i + " (id INT, p INT, sym SYMBOL)");
            }
            for (int j = 1; j <= deferredKeyCount; j++) {
                execute("CREATE TABLE z" + j + " (id INT)");
            }
            final String smallSql = timeSeriesJoinAfterChain(chainLength / 2, deferredKeyCount / 2);
            final String largeSql = timeSeriesJoinAfterChain(chainLength, deferredKeyCount);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                final long smallReads = compileAndCountReads(compiler, smallSql, SqlCompilerImpl::getFromModelLinkReadsForTesting);
                final long largeReads = compileAndCountReads(compiler, largeSql, SqlCompilerImpl::getFromModelLinkReadsForTesting);
                Assert.assertTrue("the chain must reach runFromModelFirst, link reads: " + smallReads, smallReads > 0);
                Assert.assertTrue(
                        "link reads must grow linearly with the chain length, "
                                + (chainLength / 2) + " joins: " + smallReads + ", "
                                + chainLength + " joins: " + largeReads,
                        largeReads < 3 * smallReads
                );
            }
            assertQuery(largeSql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            0
                            """);
        });
    }

    private void assertOrderingEdgeApplicationsGrowAsSquare(int blocks, int blockSize, String outerJoin, BlockAppender appender) throws Exception {
        assertChainCountGrowsBelow(
                blocks, blockSize, outerJoin, appender, SqlCompilerImpl::getOrderingEdgeApplicationsForTesting,
                "ordering edge applications", "applyModelOnOrderingConstraints", "the square", 6
        );
    }

    // The entries of the join context parents and the dependencies of the level, the ordering edges that
    // reorderTables indexed, the pins and their words, and the ordering edges in force at the one ordering pass
    // of the level.
    private void assertPinFootprint(
            SqlCompilerImpl compiler,
            String sql,
            long parentCount,
            long dependencyCount,
            long pinCount,
            int pinWordCount,
            long edgeCount
    ) throws Exception {
        final long edgesBefore = compiler.getOrderingEdgeApplicationsForTesting();
        final ExecutionModel executionModel = compiler.generateExecutionModel(sql, sqlExecutionContext);
        final ObjList<IQueryModel> joinModels = joinLevel((IQueryModel) executionModel).getJoinModels();
        long parents = 0;
        long dependencies = 0;
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            final JoinContext jc = joinModels.getQuick(i).getJoinContext();
            parents += jc != null ? jc.parents.size() : 0;
            dependencies += joinModels.getQuick(i).getDependencies().size();
        }
        Assert.assertEquals(sql, parentCount, parents);
        Assert.assertEquals(sql, dependencyCount, dependencies);
        Assert.assertEquals(sql, edgeCount, compiler.getOrderingEdgeApplicationsForTesting() - edgesBefore);
        Assert.assertEquals(sql, 0, compiler.getOrderingEdgeListSizeForTesting());
        Assert.assertEquals(sql, pinCount, compiler.getOuterJoinPinCountForTesting());
        Assert.assertEquals(sql, pinWordCount, compiler.getOuterJoinPinWordCountForTesting());
    }

    private long compileAndCountReads(SqlCompilerImpl compiler, String sql, ToLongFunction<SqlCompilerImpl> reads) throws Exception {
        final long readsBefore = reads.applyAsLong(compiler);
        final RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
        Misc.free(factory);
        return reads.applyAsLong(compiler) - readsBefore;
    }

    private void createChainTables(int lastTable) throws Exception {
        for (int i = 0; i <= lastTable; i++) {
            execute("CREATE TABLE t" + i + " (k INT, j INT)");
        }
    }

    @FunctionalInterface
    private interface BlockAppender {
        void append(StringBuilder sb, int firstTableIndex);
    }
}
