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

package io.questdb.test.cairo;

import io.questdb.cairo.PartitionDimension;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.CreateTableOperationBuilderImpl;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Composite partitioning, Plan 4e Task 1: SQL grammar for {@code (expr) AS alias} composite
 * dimensions (e.g. {@code partition by day, (upper(region)) AS r}), the DDL-time safe-subset /
 * string-coercibility gate on {@code CreateTableOperationBuilderImpl#resolveExpressionDimension},
 * and the clean-throw fix for the {@code TableWriter#resolveRowCellKey} AIOOBE landmine
 * ({@code getColumnIndex() == -1} for {@code KIND_EXPRESSION}, previously read unconditionally).
 * <p>
 * Per-row expression EVALUATION at ingest is Plan 4e Task 2 (not this task): an EXPRESSION-
 * dimensioned table is SQL-creatable, persists, and round-trips through SHOW CREATE TABLE here, but
 * INSERT is expected to fail with a clean, diagnosable {@code CairoException} rather than silently
 * mis-route or crash -- {@link #testInsertThrowsCleanErrorNotAioobe()} locks that in.
 */
public class CompositeExpressionDimTest extends AbstractCairoTest {

    @Test
    public void testInsertThrowsCleanErrorNotAioobe() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, region symbol, x double) timestamp(ts) " +
                    "partition by day, (upper(region)) AS r wal");
            execute("insert into c values ('2020-01-01T00:00:00.000000Z', 'us', 1.0)");
            drainWalQueue();

            Assert.assertTrue(
                    "table must be suspended by the clean EXPRESSION-not-yet-evaluated guard, not crash unnoticed",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c"))
            );
            assertQuery(
                    "select suspended, " +
                            "errorMessage like '%composite partitioning does not yet support EXPRESSION dimension evaluation%' clearMessage, " +
                            "errorMessage like '%ArrayIndexOutOfBoundsException%' isAioobe " +
                            "from wal_tables() where name = 'c'"
            )
                    .noLeakCheck().noRandomAccess()
                    .returns("suspended\tclearMessage\tisAioobe\ntrue\ttrue\tfalse\n");
        });
    }

    @Test
    public void testNonDeterministicExpressionRejected() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("create table c (ts timestamp, region symbol, x double) timestamp(ts) " +
                        "partition by day, (region || rnd_str()) AS r wal");
                Assert.fail("expected a nondeterministic partition dimension expression to be rejected");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "deterministic");
            }

            // now()/sysdate()/timestamp_sequence() etc. are equally rejected, not just the rnd_* family
            // -- the exact-name half of the deny-list, not just the rnd_ prefix check.
            try {
                execute("create table c2 (ts timestamp, region symbol, x double) timestamp(ts) " +
                        "partition by day, (now()) AS r wal");
                Assert.fail("expected now() in a partition dimension expression to be rejected");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "deterministic");
            }

            Assert.assertNull("rejected CREATE must not leave a table behind", engine.getTableTokenIfExists("c"));
            Assert.assertNull("rejected CREATE must not leave a table behind", engine.getTableTokenIfExists("c2"));
        });
    }

    @Test
    public void testNonStringExpressionWithoutCastRejected() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("create table c (ts timestamp, region symbol, x double) timestamp(ts) " +
                        "partition by day, (x) AS r wal");
                Assert.fail("expected a non-string partition dimension expression to be rejected");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "string-coercible");
            }
            Assert.assertNull("rejected CREATE must not leave a table behind", engine.getTableTokenIfExists("c"));
        });
    }

    /**
     * Parser-only introspection (mirrors {@code CompositePartitionParseTest#compileCreateTableModel}):
     * proves the grammar itself captures the expression node and its alias, isolated from the
     * resolve-time safe-subset gate and table creation.
     */
    @Test
    public void testParserCapturesAsAlias() throws Exception {
        assertMemoryLeak(() -> {
            final String sql = "create table c (ts timestamp, region symbol, x double) timestamp(ts) " +
                    "partition by day, (upper(region)) AS r wal";
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                ExecutionModel model = compiler.generateExecutionModel(sql, sqlExecutionContext);
                Assert.assertEquals(ExecutionModel.CREATE_TABLE, model.getModelType());
                CreateTableOperationBuilderImpl builder = (CreateTableOperationBuilderImpl) model;
                Assert.assertEquals(1, builder.getPartitionDimensionExprCount());
                TestUtils.assertEquals("upper", builder.getPartitionDimensionExpr(0).token);
                TestUtils.assertEquals("r", builder.getPartitionDimensionAlias(0));
            }
        });
    }

    @Test
    public void testPersistsExpressionDimensionAcrossReopen() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, region symbol, x double) timestamp(ts) " +
                    "partition by day, (upper(region)) AS r wal");
            engine.releaseInactive(); // force re-read of _meta from disk

            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("c"))) {
                Assert.assertTrue(m.getPartitionSpec().isComposite());
                Assert.assertEquals(1, m.getPartitionSpec().getDimensionCount());
                PartitionDimension dim = m.getPartitionSpec().getDimension(0);
                Assert.assertEquals(PartitionDimension.KIND_EXPRESSION, dim.getKind());
                Assert.assertEquals(-1, dim.getColumnIndex());
                Assert.assertEquals(0, dim.getParam());
                Assert.assertEquals("r", dim.getAlias());
                Assert.assertEquals("upper(region)", dim.getExprText());
            }
        });
    }

    @Test
    public void testPlainIdentityHashTruncateStillWorkUnaliased() throws Exception {
        // Regression: the AS-alias capture must be a true no-op for the pre-existing IDENTITY/HASH/
        // TRUNCATE grammar (no AS present at all).
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, sym symbol, x double) timestamp(ts) " +
                    "partition by day, exchange, hash(sym, 16), truncate(sym, 3) wal");
            engine.releaseInactive();

            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("c"))) {
                Assert.assertTrue(m.getPartitionSpec().isComposite());
                Assert.assertEquals(3, m.getPartitionSpec().getDimensionCount());
                Assert.assertEquals(PartitionDimension.KIND_IDENTITY, m.getPartitionSpec().getDimension(0).getKind());
                Assert.assertEquals(PartitionDimension.KIND_HASH, m.getPartitionSpec().getDimension(1).getKind());
                Assert.assertEquals(PartitionDimension.KIND_TRUNCATE, m.getPartitionSpec().getDimension(2).getKind());
            }
        });
    }

    @Test
    public void testShowCreateRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, region symbol, x double) timestamp(ts) " +
                    "partition by day, (upper(region)) AS r wal");

            printSql("SHOW CREATE TABLE c;");
            String ddl = sink.toString().replace("ddl\n", "");
            TestUtils.assertContains(ddl, "(upper(region)) AS r");

            execute("drop table c;");
            execute(ddl); // re-create from the emitted DDL
            printSql("SHOW CREATE TABLE c;");
            TestUtils.assertEquals(sink.toString().replace("ddl\n", ""), ddl);
        });
    }
}
