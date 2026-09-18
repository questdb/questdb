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

package io.questdb.test.cairo.view;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.lv.LiveViewRefreshSqlExecutionContext;
import io.questdb.cairo.mv.MatViewRefreshSqlExecutionContext;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.view.ViewGraph;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.ViewAuditModel;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The OSS half of audited views: the parser records what an audited view was read with, and the
 * view definition remembers the flag across a restart. Enterprise turns those records into audit
 * rows and owns the {@code WITH AUDIT} syntax and the permission that guards it, so none of that
 * is asserted here.
 */
public class ViewAuditTest extends AbstractCairoTest {

    @Test
    public void testAuditRecordsOnlyAuditedParametersInNameOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL)");
            execute("INSERT INTO t VALUES ('a'), ('b'), ('z')");
            drainWalQueue();
            execute("CREATE VIEW v AS (DECLARE AUDITED @z := 'z', OVERRIDABLE AUDITED @a := 'a', " +
                    "OVERRIDABLE @plain := 'b' " +
                    "SELECT s FROM t WHERE s = @a OR s = @z OR s = @plain)");
            drainWalAndViewQueues();
            markViewAudited("v");

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final ExecutionModel model = compiler.generateExecutionModel(
                        "DECLARE @a := 'b' SELECT s FROM v", sqlExecutionContext);
                final ObjList<ViewAuditModel> audits = model.getQueryModel().getViewAudits();
                assertEquals(1, audits.size());
                final ViewAuditModel audit = audits.getQuick(0);

                // @plain is OVERRIDABLE but not AUDITED, so it is not recorded: the two markings
                // answer different questions and only AUDITED decides membership here.
                assertEquals(2, audit.getParamCount());
                // Sorted by name at capture, whatever order they were declared in.
                TestUtils.assertEquals("@a", audit.getParamName(0));
                TestUtils.assertEquals("@z", audit.getParamName(1));
                // The caller's override for @a, and the view's own default for @z.
                TestUtils.assertEquals("'b'", audit.getParamValue(0).token);
                TestUtils.assertEquals("'z'", audit.getParamValue(1).token);
            }
        });
    }

    @Test
    public void testAuditedDefinitionAddsTheExtraBlock() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            final TableToken viewToken = engine.getTableTokenIfExists("v");

            final ViewDefinition audited = new ViewDefinition();
            audited.init(viewToken, "SELECT s FROM t", 0L, true);
            writeDefinitionFile(viewToken, audited);

            final IntList types = blockTypes(viewToken);
            assertEquals(2, types.size());
            assertEquals(ViewDefinition.VIEW_DEFINITION_FORMAT_MSG_TYPE, types.getQuick(0));
            assertEquals(ViewDefinition.VIEW_DEFINITION_FORMAT_EXTRA_MSG_TYPE, types.getQuick(1));
        });
    }

    @Test
    public void testAuditedFlagSurvivesTheDefinitionFile() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            final TableToken viewToken = engine.getTableTokenIfExists("v");

            final ViewDefinition audited = new ViewDefinition();
            audited.init(viewToken, "SELECT s FROM t", 7L, true);
            writeDefinitionFile(viewToken, audited);

            final ViewDefinition readBack = readDefinitionFile(viewToken);
            assertTrue(readBack.isAudited());
            assertEquals("SELECT s FROM t", readBack.getViewSql());
            assertEquals(7L, readBack.getSeqTxn());
        });
    }

    @Test
    public void testAuditedViewReadThroughAPlainViewIsNotShadowed() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            // No parameter a caller can set, so any audited view around it would cover it.
            createAuditedView("v_a", "DECLARE AUDITED @s := 'a' SELECT s FROM t WHERE s = @s");
            execute("CREATE VIEW v_wrap AS (SELECT s FROM v_a)");
            drainWalAndViewQueues();

            // A view that is not audited records no row, so it has none to cover the read with. If
            // it shadowed, wrapping an audited view in a plain one would take it out of the trail.
            assertRecordsAuditsOf("SELECT s FROM v_wrap", "v_a");
        });
    }

    @Test
    public void testAuditedViewsReadInsideAnAuditedViewAreShadowed() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            execute("CREATE TABLE dest (s SYMBOL, l LONG)");
            // None of the three has a parameter a caller can set: v_a marks nothing AUDITED, v_b
            // only a fixed one, and v_c declares nothing.
            createAuditedView("v_a", "DECLARE OVERRIDABLE @s := 'a' SELECT s FROM t WHERE s = @s");
            createAuditedView("v_b", "DECLARE AUDITED @s := 'b' SELECT s FROM t WHERE s = @s");
            createAuditedView("v_c", "SELECT s FROM t");
            createAuditedView("v_d", "SELECT s FROM v_a UNION ALL SELECT s FROM v_b UNION ALL SELECT s FROM v_c");

            // The principal asked for v_d, and the three views are how it is built. Its record
            // says everything a caller could have changed about the read, so it is the only one.
            assertRecordsAuditsOf("SELECT s FROM v_d", "v_d");
            // The statements that copy rows out get the same set, UPDATE included: the parser
            // drops the shadowed reads before it hands the audits to the statement.
            assertRecordsAuditsOf("INSERT INTO dest SELECT s, 1 FROM v_d", "v_d");
            assertRecordsAuditsOf("UPDATE dest SET l = 1 WHERE l < (SELECT count() FROM v_d)", "v_d");
            // Shadowing belongs to the reference site, not to the view: read directly, v_b records.
            assertRecordsAuditsOf("SELECT s FROM v_b", "v_b");
        });
    }

    @Test
    public void testAuditsOfOneCompileDoNotCarryOverToTheNext() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            markViewAudited("v");
            // One compiler, so both statements go through the same parser and the same pooled query
            // models. A read of a plain table must not inherit what the statement before it recorded.
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                ExecutionModel model = compiler.generateExecutionModel("SELECT s FROM v", sqlExecutionContext);
                assertEquals(1, model.getQueryModel().getViewAudits().size());
                model = compiler.generateExecutionModel("SELECT s FROM t", sqlExecutionContext);
                assertEquals(0, model.getQueryModel().getViewAudits().size());
            }
        });
    }

    @Test
    public void testCompileFailingInsideAnAuditedViewLeavesShadowingIntact() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            createAuditedView("v_inner", "SELECT s FROM t");
            createAuditedView("v_fixed", "DECLARE AUDITED @s := 'a' SELECT s FROM v_inner WHERE s = @s");
            // One compiler, so both statements go through the same parser. The first fails while
            // the parser expands v_fixed's body, where the caller's @s meets the view's own.
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try {
                    compiler.generateExecutionModel("DECLARE @s := 'b' SELECT s FROM v_fixed", sqlExecutionContext);
                    fail("expected the caller's @s to be refused");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "variable is not overridable");
                }
                // If the parser still counted itself inside v_fixed, the next read of v_fixed would
                // not be the outermost audited view, and nothing would shadow v_inner.
                final ExecutionModel model = compiler.generateExecutionModel("SELECT s FROM v_fixed", sqlExecutionContext);
                final ObjList<ViewAuditModel> audits = model.getQueryModel().getViewAudits();
                assertEquals(1, audits.size());
                TestUtils.assertEquals("v_fixed", audits.getQuick(0).getViewName());
            }
        });
    }

    @Test
    public void testCreateTableAsSelectFromAuditedViewRecordsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            markViewAudited("v");
            assertRecordsOneAuditOf("v", "CREATE TABLE copy AS (SELECT s FROM v)");
        });
    }

    @Test
    public void testCreateViewOverAuditedViewRecordsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            markViewAudited("v");
            // Defining a second view over an audited one is a read of it: rows leave the audited
            // view either way, and the statement that copies them out is the one that has to say
            // so. The column-type probe CREATE VIEW runs over its own body is a different thing
            // and is suppressed separately - see isMetadataProbe.
            assertRecordsOneAuditOf("v", "CREATE VIEW v2 AS (SELECT s FROM v)");
        });
    }

    @Test
    public void testDefinitionBlockOrderDoesNotDecideTheFlag() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            final TableToken viewToken = engine.getTableTokenIfExists("v");

            // append() writes the definition block first, so this order is not one the current
            // writer produces. It is asserted because readFrom() walks blocks in whatever order it
            // finds them, and reading the definition block resets the flag: with the extra block
            // read first, the flag was silently dropped. A compliance marking has to survive a
            // reader that claims not to care about order, or the loop should not claim it.
            final ViewDefinition audited = new ViewDefinition();
            audited.init(viewToken, "SELECT s FROM t", 5L, true);
            try (
                    BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
                    Path path = new Path()
            ) {
                path.of(configuration.getDbRoot()).concat(viewToken.getDirName()).concat(ViewDefinition.VIEW_DEFINITION_FILE_NAME);
                writer.of(path.$());
                final AppendableBlock extra = writer.append();
                ViewDefinition.appendExtra(audited, extra);
                extra.commit(ViewDefinition.VIEW_DEFINITION_FORMAT_EXTRA_MSG_TYPE);
                final AppendableBlock block = writer.append();
                ViewDefinition.append(audited, block);
                block.commit(ViewDefinition.VIEW_DEFINITION_FORMAT_MSG_TYPE);
                writer.commit();
            }

            final ViewDefinition readBack = readDefinitionFile(viewToken);
            assertTrue("the audited flag must not depend on block order", readBack.isAudited());
            assertEquals("SELECT s FROM t", readBack.getViewSql());
            assertEquals(5L, readBack.getSeqTxn());
        });
    }

    @Test
    public void testDefinitionFileWithoutADefinitionBlockIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            final TableToken viewToken = engine.getTableTokenIfExists("v");

            // Only the extra block. The definition block carries the view SQL, so a file without
            // one has nothing to build a definition from and has to say so rather than hand back
            // an empty one.
            try (
                    BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
                    Path path = new Path()
            ) {
                path.of(configuration.getDbRoot()).concat(viewToken.getDirName()).concat(ViewDefinition.VIEW_DEFINITION_FILE_NAME);
                writer.of(path.$());
                final ViewDefinition definition = new ViewDefinition();
                definition.init(viewToken, "SELECT s FROM t", 0L, true);
                final AppendableBlock block = writer.append();
                ViewDefinition.appendExtra(definition, block);
                block.commit(ViewDefinition.VIEW_DEFINITION_FORMAT_EXTRA_MSG_TYPE);
                writer.commit();
            }

            try {
                readDefinitionFile(viewToken);
                fail("expected a rejection of a view definition file with no definition block");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot read view definition, block not found");
            }
        });
    }

    @Test
    public void testDefinitionFileWrittenBeforeAuditingReadsAsNotAudited() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            final TableToken viewToken = engine.getTableTokenIfExists("v");

            // A view created before auditing existed carries the definition block alone.
            final ViewDefinition legacy = new ViewDefinition();
            legacy.init(viewToken, "SELECT s FROM t", 3L, true);
            writeLegacyDefinitionFile(viewToken, legacy);

            final ViewDefinition readBack = readDefinitionFile(viewToken);
            assertFalse(readBack.isAudited());
            assertEquals("SELECT s FROM t", readBack.getViewSql());
            assertEquals(3L, readBack.getSeqTxn());
        });
    }

    @Test
    public void testDirectReadBesideAnAuditedViewIsNotShadowed() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            createAuditedView("v_a", "DECLARE OVERRIDABLE AUDITED @s := 'a' SELECT s FROM t WHERE s = @s");
            createAuditedView("v_cover", "DECLARE OVERRIDABLE AUDITED @s := 'a' SELECT s FROM v_a");
            createAuditedView("v_tag", "SELECT s FROM v_a");

            // v_a's own site is outside every audited view, so nothing shadows it. Its site inside
            // v_cover is shadowed.
            assertRecordsAuditsOf("SELECT c.s FROM v_cover c JOIN v_a a ON c.s = a.s", "v_a", "v_cover");
            // v_tag does not record @s, so both of v_a's sites stay. Enterprise compares them per
            // execution, and records one row when they resolve alike.
            assertRecordsAuditsOf("SELECT g.s FROM v_tag g JOIN v_a a ON g.s = a.s", "v_a", "v_a", "v_tag");
        });
    }

    @Test
    public void testInnerViewsAreCheckedAgainstTheOutermostAuditedView() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            createAuditedView("v_inner", "DECLARE OVERRIDABLE AUDITED @s := 'a' SELECT s FROM t WHERE s = @s");
            createAuditedView("v_middle", "DECLARE AUDITED @s := 'b' SELECT s FROM v_inner");
            createAuditedView("v_outer", "SELECT s FROM v_middle");

            // Read on its own, v_middle records @s, so it covers v_inner.
            assertRecordsAuditsOf("SELECT s FROM v_middle", "v_middle");
            // Inside v_outer, both are checked against v_outer, whose record is the one that is
            // certain to be kept. v_middle has no parameter a caller can set, so v_outer covers it.
            // v_outer does not record @s, so v_inner keeps its record, although v_middle fixes the
            // value. This can keep a record more than strictly needed, never one fewer.
            assertRecordsAuditsOf("SELECT s FROM v_outer", "v_inner", "v_outer");
        });
    }

    @Test
    public void testInsertSelectFromAuditedViewRecordsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            execute("CREATE TABLE dest (s SYMBOL)");
            markViewAudited("v");
            assertRecordsOneAuditOf("v", "INSERT INTO dest SELECT s FROM v");
        });
    }

    @Test
    public void testNewViewIsNotAudited() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            final TableToken viewToken = engine.getTableTokenIfExists("v");
            assertFalse(engine.getViewGraph().getViewDefinition(viewToken).isAudited());
            assertFalse(readDefinitionFile(viewToken).isAudited());
        });
    }

    @Test
    public void testNonAuditedViewWritesOnlyTheDefinitionBlock() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            final TableToken viewToken = engine.getTableTokenIfExists("v");

            // A view nobody asked to audit writes exactly what it wrote before auditing existed,
            // so rolling back to a build without this feature is a non-event for it. Only a view
            // that opted in carries the extra block, and only that view loses the flag if an
            // older build rewrites its definition.
            final IntList types = blockTypes(viewToken);
            assertEquals(1, types.size());
            assertEquals(ViewDefinition.VIEW_DEFINITION_FORMAT_MSG_TYPE, types.getQuick(0));
        });
    }

    @Test
    public void testOnlyJobContextsAreBackgroundJobs() throws Exception {
        assertMemoryLeak(() -> {
            // Enterprise records nothing for a read made under a context that says it belongs to a
            // job, so the flag has to be set on the refresh contexts and on nothing a principal
            // runs queries under. The WAL apply context is package-private; the Enterprise tests
            // cover it through the rows it does not record.
            assertFalse(sqlExecutionContext.isBackgroundJob());
            try (
                    MatViewRefreshSqlExecutionContext matViewContext = new MatViewRefreshSqlExecutionContext(engine, 1);
                    LiveViewRefreshSqlExecutionContext liveViewContext = new LiveViewRefreshSqlExecutionContext(engine, 1)
            ) {
                assertTrue(matViewContext.isBackgroundJob());
                assertTrue(liveViewContext.isBackgroundJob());
            }
        });
    }

    @Test
    public void testQueryReadingNoAuditedViewRecordsNothing() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, symbol SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE dest (ts TIMESTAMP, symbol SYMBOL, price DOUBLE)");
            // Declares an AUDITED parameter but is not audited itself: the view's flag is what turns
            // auditing on, not its declarations.
            execute("""
                    CREATE VIEW v_plain AS (
                      DECLARE OVERRIDABLE AUDITED @sym := 'AAPL'
                      SELECT ts, symbol, price FROM trades WHERE symbol = @sym)""");
            execute("CREATE VIEW v_audited AS (SELECT ts, symbol, price FROM trades)");
            execute("CREATE MATERIALIZED VIEW mv_trades AS (SELECT ts, symbol, max(price) price FROM trades SAMPLE BY 1d) PARTITION BY DAY");
            drainWalAndViewQueues();
            drainWalAndMatViewQueues();
            markViewAudited("v_audited");

            // Enterprise wraps a plan for auditing only when this list is not empty, so an empty
            // list is what leaves a query that reads no audited view with the plan it always had.
            assertRecordsNoAudit("SELECT * FROM trades");
            assertRecordsNoAudit("DECLARE @sym := 'MSFT' SELECT * FROM trades WHERE symbol = @sym");
            assertRecordsNoAudit("SELECT * FROM v_plain");
            assertRecordsNoAudit("DECLARE @sym := 'MSFT' SELECT * FROM v_plain");
            assertRecordsNoAudit("SELECT * FROM mv_trades");
            // The statements that copy rows out take the same route to the model as a SELECT.
            assertRecordsNoAudit("INSERT INTO dest SELECT ts, symbol, price FROM v_plain");
            assertRecordsNoAudit("CREATE TABLE copy AS (SELECT * FROM mv_trades)");
            assertRecordsNoAudit("UPDATE dest SET price = 1 FROM v_plain WHERE dest.symbol = v_plain.symbol");

            // The control: the same check sees the audit once an audited view is read. Beside one,
            // the sources that are not audited still add nothing.
            assertRecordsOneAuditOf("v_audited", "SELECT * FROM v_audited");
            assertRecordsOneAuditOf("v_audited", """
                    SELECT a.ts, a.symbol, p.price, m.price, t.price
                    FROM v_audited a
                    JOIN v_plain p ON a.symbol = p.symbol
                    JOIN mv_trades m ON a.symbol = m.symbol
                    JOIN trades t ON a.symbol = t.symbol""");
        });
    }

    @Test
    public void testReadOfANonAuditedViewRecordsNothing() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final ExecutionModel model = compiler.generateExecutionModel("SELECT s FROM v", sqlExecutionContext);
                assertEquals(0, model.getQueryModel().getViewAudits().size());
            }
        });
    }

    @Test
    public void testRedefiningAnAuditedViewKeepsTheFlag() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            final TableToken viewToken = engine.getTableTokenIfExists("v");
            final ViewDefinition audited = new ViewDefinition();
            audited.init(viewToken, "SELECT s FROM t", 0L, true);
            writeDefinitionFile(viewToken, audited);
            markViewAudited("v");

            // The marking has to survive a redefinition, because losing it is a compliance event
            // and neither statement asks for one. Both routes land in ViewGraph.updateView, which
            // carries the current flag rather than taking one from the statement - CREATE OR
            // REPLACE over an existing view is intercepted by compileCreate and executed as an
            // alter, so it is the same route under a different spelling. That is what makes DROP
            // the only way to remove the marking, and therefore the only place it has to be gated.
            execute("CREATE OR REPLACE VIEW v AS (SELECT s FROM t WHERE s != 'z')");
            drainWalAndViewQueues();
            assertTrue("CREATE OR REPLACE must not clear the audited flag",
                    engine.getViewGraph().getViewDefinition(engine.getTableTokenIfExists("v")).isAudited());
            assertTrue("...and it has to survive on disk too",
                    readDefinitionFile(engine.getTableTokenIfExists("v")).isAudited());

            execute("ALTER VIEW v AS (SELECT s FROM t)");
            drainWalAndViewQueues();
            assertTrue("ALTER VIEW must not clear the audited flag",
                    engine.getViewGraph().getViewDefinition(engine.getTableTokenIfExists("v")).isAudited());
            assertTrue("...and it has to survive on disk too",
                    readDefinitionFile(engine.getTableTokenIfExists("v")).isAudited());
        });
    }

    @Test
    public void testSelectFromAuditedViewRecordsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            markViewAudited("v");
            assertRecordsOneAuditOf("v", "SELECT s FROM v");
        });
    }

    @Test
    public void testShadowingLooksThroughViewsThatAreNotAudited() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            createAuditedView("v_a", "DECLARE OVERRIDABLE AUDITED @s := 'a' SELECT s FROM t WHERE s = @s");
            execute("CREATE VIEW v_wrap AS (SELECT s FROM v_a)");
            drainWalAndViewQueues();
            createAuditedView("v_tag", "SELECT s FROM v_wrap");
            createAuditedView("v_cover", "DECLARE OVERRIDABLE AUDITED @s := 'a' SELECT s FROM v_wrap");

            // v_a is checked against the audited view around it as if v_wrap were not there.
            assertRecordsAuditsOf("SELECT s FROM v_tag", "v_a", "v_tag");
            assertRecordsAuditsOf("SELECT s FROM v_cover", "v_cover");
        });
    }

    @Test
    public void testShadowingRequiresTheOuterViewToAuditTheInnerOverridableParams() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            createAuditedView("v_inner", "DECLARE OVERRIDABLE AUDITED @s := 'a' SELECT s FROM t WHERE s = @s");
            // A caller's @s reaches v_inner through each of these. v_tag does not record it, so its
            // record does not say which rows the read covered, and v_inner keeps its own.
            createAuditedView("v_tag", "DECLARE OVERRIDABLE AUDITED @tag := 'x' SELECT s FROM v_inner");
            assertRecordsAuditsOf("DECLARE @s := 'b' SELECT s FROM v_tag", "v_inner", "v_tag");

            // Recording it covers the inner read, whether the outer view lets a caller set it...
            createAuditedView("v_reexport", "DECLARE OVERRIDABLE AUDITED @s := 'a' SELECT s FROM v_inner");
            assertRecordsAuditsOf("DECLARE @s := 'b' SELECT s FROM v_reexport", "v_reexport");
            // ...or fixes it, so that no caller can.
            createAuditedView("v_fixed", "DECLARE AUDITED @s := 'b' SELECT s FROM v_inner");
            assertRecordsAuditsOf("SELECT s FROM v_fixed", "v_fixed");
            assertExceptionNoLeakCheck("DECLARE @s := 'a' SELECT s FROM v_fixed", 11, "variable is not overridable: @s");

            // Only the parameters a caller can set need covering. A fixed one could not be
            // re-declared by the outer view if it tried, and it is not on the outer record.
            createAuditedView("v_inner_mixed", "DECLARE OVERRIDABLE AUDITED @s := 'a', AUDITED @l := 'b' SELECT s FROM t WHERE s = @s OR s = @l");
            createAuditedView("v_mixed", "DECLARE OVERRIDABLE AUDITED @s := 'a' SELECT s FROM v_inner_mixed");
            assertRecordsAuditsOf("SELECT s FROM v_mixed", "v_mixed");
        });
    }

    @Test
    public void testShowCreateViewReportsTheAuditedFlag() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            final TableToken viewToken = engine.getTableTokenIfExists("v");

            // SHOW CREATE VIEW reads the _view file rather than the graph, so marking the view
            // through ViewGraph - what every other test here does - would not reach it.
            // noRandomAccess: SHOW CREATE VIEW builds its one row on the fly, so the cursor cannot
            // be re-positioned the way a table scan can.
            assertQuery("SHOW CREATE VIEW v")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("ddl\nCREATE VIEW 'v' AS ( \nSELECT s FROM t\n);\n");

            final ViewDefinition audited = new ViewDefinition();
            audited.init(viewToken, "SELECT s FROM t", 0L, true);
            writeDefinitionFile(viewToken, audited);

            // The flag's only user-visible surface in OSS. WITH AUDIT is an Enterprise clause, so
            // what this round-trips into is an Enterprise statement - which is the point: the
            // definition has to report the marking it is actually carrying.
            assertQuery("SHOW CREATE VIEW v")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("ddl\nCREATE VIEW 'v' AS ( \nSELECT s FROM t\n) WITH AUDIT;\n");
        });
    }

    @Test
    public void testUnknownTrailingBlockIsSkipped() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            final TableToken viewToken = engine.getTableTokenIfExists("v");

            // The property that makes an added block safe in both directions: a reader walks past
            // block types it does not know rather than failing on them. It is what lets an older
            // build read a file this one wrote, and what will let this one read whatever a later
            // build adds.
            final ViewDefinition definition = new ViewDefinition();
            definition.init(viewToken, "SELECT s FROM t", 11L, true);
            try (
                    BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
                    Path path = new Path()
            ) {
                path.of(configuration.getDbRoot()).concat(viewToken.getDirName()).concat(ViewDefinition.VIEW_DEFINITION_FILE_NAME);
                writer.of(path.$());
                final AppendableBlock block = writer.append();
                ViewDefinition.append(definition, block);
                block.commit(ViewDefinition.VIEW_DEFINITION_FORMAT_MSG_TYPE);
                final AppendableBlock extra = writer.append();
                ViewDefinition.appendExtra(definition, extra);
                extra.commit(ViewDefinition.VIEW_DEFINITION_FORMAT_EXTRA_MSG_TYPE);
                final AppendableBlock unknown = writer.append();
                unknown.putLong(1234L);
                unknown.commit(ViewDefinition.VIEW_DEFINITION_FORMAT_EXTRA_MSG_TYPE + 41);
                writer.commit();
            }

            final ViewDefinition readBack = readDefinitionFile(viewToken);
            assertTrue(readBack.isAudited());
            assertEquals("SELECT s FROM t", readBack.getViewSql());
            assertEquals(11L, readBack.getSeqTxn());
        });
    }

    @Test
    public void testUpdateReadingAnAuditedViewRecordsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            execute("CREATE TABLE dest (s SYMBOL, l LONG)");
            markViewAudited("v");
            assertRecordsOneAuditOf("v", "UPDATE dest SET l = 1 FROM v WHERE dest.s = v.s");
            // A sub-query is the one way a WAL table's UPDATE can read a view, so it has to carry
            // the audit as well as the FROM clause does.
            assertRecordsOneAuditOf("v", "UPDATE dest SET l = 1 WHERE l < (SELECT count() FROM v)");
        });
    }

    /**
     * Asserts which views the statement records a read of: one name per recorded read, in any
     * order.
     */
    private static void assertRecordsAuditsOf(String sql, String... viewNames) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final ExecutionModel model = compiler.generateExecutionModel(sql, sqlExecutionContext);
            final IQueryModel queryModel = readModelOf(model);
            assertNotNull("no query model for [" + sql + "]", queryModel);
            final ObjList<ViewAuditModel> audits = queryModel.getViewAudits();
            final String[] actual = new String[audits.size()];
            for (int i = 0, n = audits.size(); i < n; i++) {
                actual[i] = audits.getQuick(i).getViewName().toString();
            }
            final String[] expected = viewNames.clone();
            Arrays.sort(actual);
            Arrays.sort(expected);
            assertEquals("wrong audits for [" + sql + "]", Arrays.toString(expected), Arrays.toString(actual));
        }
    }

    private static void assertRecordsNoAudit(String sql) throws Exception {
        assertRecordsAuditsOf(sql);
    }

    private static void assertRecordsOneAuditOf(String viewName, String sql) throws Exception {
        assertRecordsAuditsOf(sql, viewName);
    }

    /**
     * The block types the view's {@code _view} file holds, in file order.
     */
    private static IntList blockTypes(TableToken viewToken) {
        final IntList types = new IntList();
        try (BlockFileReader reader = new BlockFileReader(configuration); Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(viewToken.getDirName()).concat(ViewDefinition.VIEW_DEFINITION_FILE_NAME);
            reader.of(path.$());
            final BlockFileReader.BlockCursor cursor = reader.getCursor();
            while (cursor.hasNext()) {
                types.add(cursor.next().type());
            }
        }
        return types;
    }

    private static void createAuditedView(String viewName, String body) throws Exception {
        execute("CREATE VIEW " + viewName + " AS (" + body + ")");
        drainWalAndViewQueues();
        markViewAudited(viewName);
    }

    private static void createBaseTableAndView() throws Exception {
        execute("CREATE TABLE t (s SYMBOL)");
        execute("INSERT INTO t VALUES ('a'), ('b')");
        drainWalQueue();
        execute("CREATE VIEW v AS (SELECT s FROM t)");
        drainWalAndViewQueues();
    }

    /**
     * OSS has no syntax that sets the flag - {@code WITH AUDIT} is an Enterprise clause - so a test
     * that needs an audited view swaps the graph's definition for one carrying the flag.
     */
    private static void markViewAudited(String viewName) {
        final TableToken viewToken = engine.getTableTokenIfExists(viewName);
        final ViewGraph viewGraph = engine.getViewGraph();
        final ViewDefinition current = viewGraph.getViewDefinition(viewToken);
        final ViewDefinition audited = new ViewDefinition();
        audited.init(viewToken, current.getViewSql(), current.getDependencies(), current.getSeqTxn(), true);
        viewGraph.removeView(viewToken);
        assertTrue(viewGraph.addView(audited));
    }

    private static ViewDefinition readDefinitionFile(TableToken viewToken) {
        final ViewDefinition definition = new ViewDefinition();
        try (BlockFileReader reader = new BlockFileReader(configuration); Path path = new Path()) {
            path.of(configuration.getDbRoot());
            ViewDefinition.readFrom(definition, reader, path, path.size(), viewToken);
        }
        return definition;
    }

    /**
     * The model code generation compiles into the cursor that reads the statement's rows, which is
     * the model Enterprise takes the audits from. An UPDATE compiles its nested model: the top one
     * only carries the SET expressions.
     */
    private static IQueryModel readModelOf(ExecutionModel model) {
        final IQueryModel queryModel = model.getQueryModel();
        return model.getModelType() == ExecutionModel.UPDATE ? queryModel.getNestedModel() : queryModel;
    }

    private static void writeDefinitionFile(TableToken viewToken, ViewDefinition definition) {
        try (
                BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
                Path path = new Path()
        ) {
            path.of(configuration.getDbRoot()).concat(viewToken.getDirName()).concat(ViewDefinition.VIEW_DEFINITION_FILE_NAME);
            writer.of(path.$());
            ViewDefinition.append(definition, writer);
        }
    }

    /**
     * Writes the definition block alone, the shape every {@code _view} file had before auditing.
     */
    private static void writeLegacyDefinitionFile(TableToken viewToken, ViewDefinition definition) {
        try (
                BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
                Path path = new Path()
        ) {
            path.of(configuration.getDbRoot()).concat(viewToken.getDirName()).concat(ViewDefinition.VIEW_DEFINITION_FILE_NAME);
            writer.of(path.$());
            final AppendableBlock block = writer.append();
            ViewDefinition.append(definition, block);
            block.commit(ViewDefinition.VIEW_DEFINITION_FORMAT_MSG_TYPE);
            writer.commit();
        }
    }
}
