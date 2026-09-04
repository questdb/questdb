/*******************************************************************************
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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Does the "cannot drop a column a composite dimension references" invariant hold on the WRITER path,
 * or only on the SQL path?
 * <p>
 * {@code SqlCompilerImpl#refuseDroppingCompositePinnedColumn} is a PRE-CHECK in the compiler. The
 * writer-side apply it guards is shared with every non-SQL caller of {@code TableWriterAPI#apply} --
 * which is also how the composite differential fuzz issues its DDL. So "the SQL gate refuses it" says
 * nothing about whether the invariant is actually enforced where it matters.
 * <p>
 * This asks the question directly rather than assuming either answer. Written because a comment in
 * {@code CompositeFuzzRunner} had asserted, as MEASURED, that a dimension-column drop would be refused
 * for the composite twin and accepted for the plain one -- an expectation derived from the SQL gate
 * and never actually run against the writer path the fuzz uses.
 * <p>
 * ANSWER, measured 2026-08-26: <b>the invariant holds on both paths.</b> The refusal is raised by
 * {@code AlterOperation} itself, which the compiler pre-check merely front-runs, so every non-SQL
 * caller is covered too -- defence in depth, not a SQL-only gate. The two paths differ in WHEN, and
 * only in when: SQL fails the statement, the writer path fails inside the WAL-apply job and suspends
 * the table. That asymmetry is inherent to WAL DDL rather than a composite-specific bug (contrast
 * {@code CompositeAddColumnPostingGateTest}, where the statement is accepted and NOTHING refuses it
 * until a later unrelated commit).
 * <p>
 * Consequence for the fuzz: a generated dimension-column DROP suspends the composite twin while the
 * plain twin drops the column happily. Enrolling DROP/RENAME therefore needs those ops filtered out,
 * exactly as {@code dropUnsupportedAddColumnOps} filters the unsupported adds -- the divergence is a
 * harness problem, not a product one.
 */
public class CompositeDimensionColumnDropTest extends AbstractCairoTest {

    /**
     * SQL path -- the known-good half, and the control. If this ever stops refusing, the writer-path
     * result below cannot be interpreted.
     */
    @Test
    public void testSqlPathRefusesDroppingADimensionColumn() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedComposite();
            try {
                execute("ALTER TABLE c DROP COLUMN exch");
                Assert.fail("SQL must refuse dropping a column a composite dimension references");
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), "referenced by a composite partition dimension");
            }
        });
    }

    /**
     * Writer path -- the actual question, and a regression lock on the measured answer.
     * <p>
     * Asserts the SPECIFIC outcome (suspended, with that exact message) rather than a loose
     * "refused-or-suspended" disjunction. The disjunction is what this test was first written with,
     * and it would have passed just as happily on either outcome -- including on a future change that
     * turned a clean refusal into a bricked table, which is precisely the regression worth catching.
     * <p>
     * The column must still exist afterwards: a rejected DDL that had already mutated metadata would
     * be a worse failure than the drop succeeding outright.
     */
    @Test
    public void testWriterPathRefusesDroppingADimensionColumn() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createRoutedComposite();
            try (TableWriterAPI w = engine.getTableWriterAPI(token, "composite dimension drop probe")) {
                final AlterOperation op = new AlterOperationBuilder()
                        .ofDropColumn(0, token, w.getMetadata().getTableId())
                        .ofDropColumn("exch")
                        .build();
                try (SqlExecutionContextImpl ctx =
                             new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                    op.withContext(ctx);
                    w.apply(op, true);
                }
            }
            drainWalQueue();

            Assert.assertTrue(
                    "a dimension column was dropped through TableWriterAPI without being refused --"
                            + " the composite partition spec would then reference a column that no"
                            + " longer exists, and the SQL pre-check would be the ONLY thing enforcing"
                            + " this invariant",
                    engine.getTableSequencerAPI().isSuspended(token));

            final StringSink err = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext,
                    "select errorMessage from wal_tables() where name = 'c'", err);
            TestUtils.assertContains(err, "referenced by a composite partition dimension");

            // the refused DDL must not have half-applied
            try (TableMetadata meta = engine.getTableMetadata(token)) {
                Assert.assertTrue("the refused drop must not have removed the column anyway",
                        meta.getColumnIndexQuiet("exch") >= 0);
            }
        });
    }

    private TableToken createRoutedComposite() throws Exception {
        execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                + "PARTITION BY DAY, exch WAL");
        // two distinct dimension values => genuinely routed, not dormant
        execute("INSERT INTO c VALUES "
                + "('2023-01-01T01:00:00.000000Z','BTC',1.0),"
                + "('2023-01-01T02:00:00.000000Z','ETH',2.0)");
        drainWalQueue();
        return engine.verifyTableName("c");
    }
}
