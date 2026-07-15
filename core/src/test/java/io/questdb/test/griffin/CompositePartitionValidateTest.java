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
import io.questdb.cairo.PartitionDimension;
import io.questdb.cairo.PartitionSpec;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.ops.CreateTableOperationImpl;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Task 4 of composite partitioning: resolving the parse-time PARTITION BY dimension/cluster-column
 * expression lists (Task 3) into a validated {@link PartitionSpec}, attached to the
 * {@link CreateTableOperationImpl} built for a plain (non CREATE-AS-SELECT, non-LIKE) CREATE TABLE.
 */
public class CompositePartitionValidateTest extends AbstractCairoTest {

    @Test
    public void testDimMustBeSymbol() throws Exception {
        assertException(
                "create table t (ts timestamp, price double) timestamp(ts) partition by day, price wal",
                /*pos of price*/ 76, "partition dimension must be a SYMBOL column");
    }

    @Test
    public void testExpressionRequiresAlias() throws Exception {
        // Position is that of the "=" operator token: an OPERATION node reports its own
        // position at the operator, not at the start of the parenthesized expression.
        assertException(
                "create table t (ts timestamp, s symbol) timestamp(ts) partition by day, (s = 'BTC') wal",
                /*pos of '='*/ 75, "partition expression must be aliased with AS");
    }

    @Test
    public void testPartitionRequiresDesignatedTimestamp() throws Exception {
        assertException(
                "create table t (ts timestamp, s symbol) partition by day, s wal",
                53, "partitioning is possible only on tables with designated timestamps");
    }

    @Test
    public void testResolvedSpecOnOperation() throws Exception {
        // Compile (don't execute) and assert the resolved spec via a test hook returning getPartitionSpec().
        PartitionSpec s = compilePartitionSpec(
                "create table t (ts timestamp, exchange symbol, symbol symbol) " +
                        "timestamp(ts) partition by day, exchange, hash(symbol, 8) wal");
        Assert.assertEquals(PartitionBy.DAY, s.getTimeUnit());
        Assert.assertEquals(2, s.getDimensionCount());
        Assert.assertEquals(PartitionDimension.KIND_IDENTITY, s.getDimension(0).getKind());
        Assert.assertEquals(PartitionDimension.KIND_HASH, s.getDimension(1).getKind());
    }

    @Test
    public void testClusterColumnResolvesToColumnIndex() throws Exception {
        // Column declaration order: ts=0, exchange=1, price=2. ORDER BY resolves the cluster
        // column's name to its declaration index via columnNameIndexMap, independent of (and
        // after) the dimension list.
        PartitionSpec s = compilePartitionSpec(
                "create table t (ts timestamp, exchange symbol, price double) " +
                        "timestamp(ts) partition by day, exchange order by price wal");
        Assert.assertEquals(1, s.getClusterColumnCount());
        Assert.assertEquals(2, s.getClusterColumn(0));
    }

    @Test
    public void testCompositeDimensionsRequireTimePartitioning() throws Exception {
        // NONE isn't partitioned, so WAL would reject first; omit WAL so this actually reaches
        // the build()-time composite-dimensions-require-time-partitioning check. Timestamp is
        // designated so the earlier "partitioning is possible only on tables with designated
        // timestamps" parse-time check doesn't fire first either.
        assertException(
                "create table t (ts timestamp, s symbol) timestamp(ts) partition by none, s",
                /*pos of s*/ 73, "composite partitioning requires time partitioning");
    }

    @Test
    public void testNoneRejectsClusterOnlyColumns() throws Exception {
        // NONE isn't partitioned, so WAL would reject first; omit WAL so this actually reaches
        // the build()-time guard. Timestamp is designated so the earlier "partitioning is
        // possible only on tables with designated timestamps" parse-time check doesn't fire
        // first either. No dimensions here, only a cluster/ORDER BY column: PartitionSpec.isComposite()
        // is true whenever there are cluster columns too, so PARTITION BY NONE + ORDER BY must be
        // rejected the same way as PARTITION BY NONE + dimensions is.
        assertException(
                "create table t (ts timestamp, s symbol) timestamp(ts) partition by none order by s",
                /*pos of s (the cluster column)*/ 81, "composite partitioning requires time partitioning");
    }

    @Test
    public void testUnknownClusterColumnThrows() throws Exception {
        assertException(
                "create table t (ts timestamp, exchange symbol) timestamp(ts) " +
                        "partition by day, exchange order by nope wal",
                /*pos of nope*/ 97, "Invalid column: nope");
    }

    @Test
    public void testAsSelectRejectsCompositeDimensions() throws Exception {
        // CREATE TABLE AS SELECT's columns aren't known until the select executes, so composite
        // dimensions can't be resolved at build() time (no column-type/index info to drive the
        // SYMBOL resolver); the AS-SELECT branch of build() rejects them outright rather than
        // misreport columns as non-existent. This binding constraint previously had zero test
        // coverage; behavior is already correct, so this is a regression guard, not a fix.
        execute("create table src (ts timestamp, exchange symbol) timestamp(ts) partition by day wal");
        assertException(
                "create table t as (select * from src) timestamp(ts) partition by day, exchange",
                /*pos of exchange*/ 70, "composite partitioning is not yet supported with CREATE TABLE AS SELECT");
    }

    /**
     * Compiles (but does not execute) {@code sql} via {@link SqlCompiler#compile}, which parses
     * and builds the {@link CreateTableOperationImpl} (running Task 4's resolution/validation in
     * {@code CreateTableOperationBuilderImpl.build()}) without creating anything on disk, and
     * returns the resolved {@link PartitionSpec} off the built (unexecuted) operation.
     */
    private PartitionSpec compilePartitionSpec(String sql) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
            try (Operation op = cq.getOperation()) {
                return ((CreateTableOperationImpl) op).getPartitionSpec();
            }
        }
    }
}
