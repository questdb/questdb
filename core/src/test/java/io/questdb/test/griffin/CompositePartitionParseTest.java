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

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PartitionSpec;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.ops.CreateTableOperationBuilderImpl;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Parser-only tests for Task 3 of composite partitioning: the comma-separated
 * PARTITION BY dimension list, optional ORDER BY clustering and optional LAYOUT
 * naming mode, scoped to CREATE TABLE only (materialized views are out of scope).
 * <p>
 * Resolution of the raw {@link io.questdb.griffin.model.ExpressionNode} lists captured here into
 * {@link PartitionSpec}/{@code PartitionDimension} happens in a later task (see
 * {@code PartitionTransform}); these tests only prove that the parser captures the right raw shape
 * and does not regress plain single-unit PARTITION BY.
 */
public class CompositePartitionParseTest extends AbstractCairoTest {

    @Test
    public void testBareDayStillWorks() throws Exception {
        final String sql = "create table t2 (ts timestamp, s symbol) timestamp(ts) partition by day wal";

        // Non-vacuous: prove the new comma-list/order-by/layout parsing is a true no-op for the
        // plain single-unit case (empty dimension/cluster lists, default HIVE naming), not merely
        // that the statement happens to still execute.
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            CreateTableOperationBuilderImpl builder = compileCreateTableModel(compiler, sql);
            Assert.assertEquals(0, builder.getPartitionDimensionExprCount());
            Assert.assertEquals(0, builder.getClusterExprCount());
            Assert.assertEquals(PartitionSpec.MODE_HIVE, builder.getNamingMode());
        }

        execute(sql);
        assertQuery("select count() from tables() where table_name = 't2'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("""
                        count
                        1
                        """);
    }

    @Test
    public void testCompositeWithOrderByLayoutAndTtlWal() throws Exception {
        // Full tail combination: dims + ORDER BY + LAYOUT, followed by the pre-existing
        // TTL/WAL handling, to prove the lookahead token handed off to that pre-existing code is
        // exactly right (no dropped or double-read token) even with every optional clause present.
        final String sql = "create table t6 (ts timestamp, exchange symbol, price double) timestamp(ts) " +
                "partition by day, exchange order by exchange layout plain ttl 1 week wal";

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            CreateTableOperationBuilderImpl builder = compileCreateTableModel(compiler, sql);
            Assert.assertEquals(1, builder.getPartitionDimensionExprCount());
            TestUtils.assertEquals("exchange", builder.getPartitionDimensionExpr(0).token);
            Assert.assertEquals(1, builder.getClusterExprCount());
            TestUtils.assertEquals("exchange", builder.getClusterExpr(0).token);
            Assert.assertEquals(PartitionSpec.MODE_PLAIN, builder.getNamingMode());
        }

        execute(sql);
        assertQuery("select walEnabled as wal_enabled from tables() where table_name = 't6'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        wal_enabled
                        true
                        """);
    }

    @Test
    public void testLayoutHiveExplicit() throws Exception {
        final String sql = "create table t8 (ts timestamp, exchange symbol) timestamp(ts) " +
                "partition by day, exchange layout hive wal";
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            CreateTableOperationBuilderImpl builder = compileCreateTableModel(compiler, sql);
            Assert.assertEquals(1, builder.getPartitionDimensionExprCount());
            Assert.assertEquals(PartitionSpec.MODE_HIVE, builder.getNamingMode());
        }
    }

    @Test
    public void testLayoutInvalidValueThrows() throws Exception {
        final String sql = "create table t9 (ts timestamp, exchange symbol) timestamp(ts) " +
                "partition by day, exchange layout bogus wal";
        assertException(sql, sql.indexOf("bogus"), "'hive' or 'plain' expected");
    }

    @Test
    public void testLayoutPlain() throws Exception {
        final String sql = "create table t5 (ts timestamp, exchange symbol) timestamp(ts) " +
                "partition by day, exchange layout plain wal";
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            CreateTableOperationBuilderImpl builder = compileCreateTableModel(compiler, sql);
            Assert.assertEquals(1, builder.getPartitionDimensionExprCount());
            TestUtils.assertEquals("exchange", builder.getPartitionDimensionExpr(0).token);
            Assert.assertEquals(0, builder.getClusterExprCount());
            Assert.assertEquals(PartitionSpec.MODE_PLAIN, builder.getNamingMode());
        }
    }

    @Test
    public void testParseTwoDimsAndOrderBy() throws Exception {
        final String sql = "create table t (ts timestamp, exchange symbol, symbol symbol, price double) " +
                "timestamp(ts) partition by day, exchange, hash(symbol, 32) order by symbol wal";

        // Non-vacuous: assert the builder's raw parse-time lists directly (via generateExecutionModel,
        // which parses but does not execute/create anything), so this test would fail if the parser
        // silently dropped the dimensions/cluster columns instead of capturing them.
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            CreateTableOperationBuilderImpl builder = compileCreateTableModel(compiler, sql);
            Assert.assertEquals(2, builder.getPartitionDimensionExprCount());
            TestUtils.assertEquals("exchange", builder.getPartitionDimensionExpr(0).token);
            TestUtils.assertEquals("hash", builder.getPartitionDimensionExpr(1).token);
            Assert.assertEquals(1, builder.getClusterExprCount());
            TestUtils.assertEquals("symbol", builder.getClusterExpr(0).token);
            Assert.assertEquals(PartitionSpec.MODE_HIVE, builder.getNamingMode());
        }

        // Round-trip through SHOW CREATE is covered in Task 7; here assert no parse error + WAL enabled
        // (composite semantics are resolved/persisted in later tasks; parsing must not reject or corrupt
        // the DDL, and it must still produce a normal WAL table today).
        execute(sql);
        assertQuery("select walEnabled as wal_enabled from tables() where table_name = 't'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        wal_enabled
                        true
                        """);
    }

    @Test
    public void testTimeMustLead() throws Exception {
        final String sql = "create table t3 (ts timestamp, s symbol) timestamp(ts) partition by s, day wal";
        // position of the dimension-list 's' immediately following "partition by "
        assertException(
                sql,
                sql.indexOf("partition by s") + "partition by ".length(),
                "partition time unit (DAY/HOUR/WEEK/MONTH/YEAR) must come first"
        );
    }

    @Test
    public void testTimestampUnitFunctionForm() throws Exception {
        // `partition by timestamp(day)` is an alternative spelling of `partition by day` (decision 3a).
        final String sql = "create table t4 (ts timestamp, s symbol) timestamp(ts) partition by timestamp(day) wal";

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            CreateTableOperationBuilderImpl builder = compileCreateTableModel(compiler, sql);
            Assert.assertEquals(PartitionBy.DAY, builder.getPartitionByFromExpr());
            Assert.assertEquals(0, builder.getPartitionDimensionExprCount());
        }

        execute(sql);
        assertQuery("select partitionBy from tables() where table_name = 't4'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        partitionBy
                        DAY
                        """);
    }

    /**
     * Parses {@code sql} via {@link SqlCompiler#generateExecutionModel}, which parses but does not
     * execute/create anything, and returns the raw {@link CreateTableOperationBuilderImpl} so tests can
     * introspect parse-time-only state (composite dimension/cluster lists, naming mode) that the public
     * compile path does not otherwise expose. CREATE_TABLE's {@code ExecutionModel} is the builder itself.
     */
    private CreateTableOperationBuilderImpl compileCreateTableModel(SqlCompiler compiler, String sql) throws Exception {
        ExecutionModel model = compiler.generateExecutionModel(sql, sqlExecutionContext);
        Assert.assertEquals(ExecutionModel.CREATE_TABLE, model.getModelType());
        return (CreateTableOperationBuilderImpl) model;
    }
}
