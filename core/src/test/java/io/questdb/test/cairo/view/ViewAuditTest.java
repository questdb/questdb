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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.lv.LiveViewRefreshSqlExecutionContext;
import io.questdb.cairo.mv.MatViewRefreshSqlExecutionContext;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.view.ViewGraph;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.CreateViewOperationBuilder;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.ViewAuditModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The OSS half of audited views: the parser records what an audited view was read with, and the
 * view definition remembers the flag across a restart. Enterprise turns those records into audit
 * rows and owns the {@code WITH AUDIT} syntax and the permission that guards it, so none of that
 * is asserted here.
 */
public class ViewAuditTest extends AbstractCairoTest {
    // What each plan generated since a test armed this held on its model: the names of the views
    // it audits, sorted. Null while no test is looking.
    private static ObjList<String> generatedPlanAudits;
    // Makes the compiler refuse every plan whose model holds an audit, once the plan generated.
    private static boolean isAuditedPlanRefused;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Enterprise reads a plan's audits off the model the plan is generated from, in its
        // override of generateSelectOneShot(), and it installs that compiler through the engine.
        // So every compiler the engine pools is one, the one PIVOT borrows to run its
        // FOR ... IN (SELECT ...) sub-query included. This installs a compiler that looks at the
        // same model in the same place.
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public SqlCompilerFactory getSqlCompilerFactory() {
                return PlanAuditRecordingCompiler::new;
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testAuditRecordsOnlyAuditedParametersInNameOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL)");
            execute("INSERT INTO t VALUES ('a'), ('b'), ('z')");
            drainWalQueue();
            execute("""
                    CREATE VIEW v AS (
                      DECLARE AUDITED @z := 'z', OVERRIDABLE AUDITED @a := 'a', OVERRIDABLE @plain := 'b'
                      SELECT s FROM t WHERE s = @a OR s = @z OR s = @plain)""");
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
    public void testAuditedMarkerOutsideTheTopLevelDeclareIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            // A read records the AUDITED variables of the block that opens the view body, and no
            // others, so a marking anywhere else in the body would never reach the record. Every
            // statement that defines a body refuses it at the marker.
            final String error = "AUDITED is only allowed in the top-level DECLARE block";
            assertExceptionNoLeakCheck("CREATE VIEW v_sub AS (SELECT * FROM (DECLARE AUDITED @x := 1 SELECT @x a))", 45, error);
            assertExceptionNoLeakCheck("CREATE VIEW v_bare AS SELECT * FROM (DECLARE AUDITED @x := 1 SELECT @x a)", 45, error);
            assertExceptionNoLeakCheck("CREATE VIEW v_cte AS (WITH c AS (DECLARE OVERRIDABLE AUDITED @x := 1 SELECT @x a) SELECT * FROM c)", 53, error);
            // A caller's value reaches a set operation branch, so a read could change the rows it
            // returns without its record saying so.
            assertExceptionNoLeakCheck("CREATE VIEW v_union AS (SELECT 1 a UNION ALL DECLARE OVERRIDABLE AUDITED @x := 2 SELECT @x)", 65, error);
            assertExceptionNoLeakCheck("CREATE VIEW v_intersect AS (SELECT 1 a INTERSECT DECLARE AUDITED @x := 1 SELECT @x)", 57, error);
            // A sub-query a join reads is no top level either, lateral or not.
            assertExceptionNoLeakCheck("CREATE VIEW v_join AS (SELECT * FROM t JOIN (DECLARE AUDITED @x := 'a' SELECT @x s) j ON t.s = j.s)", 53, error);
            assertExceptionNoLeakCheck(
                    "CREATE VIEW v_lateral AS (SELECT * FROM t JOIN LATERAL (DECLARE AUDITED @x := 'a' SELECT count() c FROM t t2 WHERE t2.s = t.s OR t2.s = @x) j ON true)",
                    64,
                    error
            );
            // A nested marker that is also malformed is refused as a nested one, at its first
            // AUDITED, rather than reported as a duplicate or as lacking its variable.
            assertExceptionNoLeakCheck("CREATE VIEW v_dup AS (SELECT * FROM (DECLARE AUDITED AUDITED @x := 1 SELECT @x a))", 45, error);
            assertExceptionNoLeakCheck("CREATE VIEW v_unnamed AS (SELECT * FROM (DECLARE AUDITED := 1 SELECT 1 a))", 49, error);
            // Redefining a view parses the new body the same way.
            assertExceptionNoLeakCheck("ALTER VIEW v AS (SELECT * FROM (DECLARE AUDITED @x := 1 SELECT @x a))", 40, error);
            assertExceptionNoLeakCheck("CREATE OR REPLACE VIEW v AS (SELECT 1 a UNION ALL DECLARE AUDITED @x := 2 SELECT @x)", 58, error);

            // None of them left a view behind or changed the one they tried to redefine.
            drainWalAndViewQueues();
            assertNull(engine.getTableTokenIfExists("v_sub"));
            assertNull(engine.getTableTokenIfExists("v_bare"));
            assertNull(engine.getTableTokenIfExists("v_cte"));
            assertNull(engine.getTableTokenIfExists("v_union"));
            assertNull(engine.getTableTokenIfExists("v_intersect"));
            assertNull(engine.getTableTokenIfExists("v_join"));
            assertNull(engine.getTableTokenIfExists("v_lateral"));
            assertNull(engine.getTableTokenIfExists("v_dup"));
            assertNull(engine.getTableTokenIfExists("v_unnamed"));
            assertEquals("SELECT s FROM t", engine.getViewGraph().getViewDefinition(engine.getTableTokenIfExists("v")).getViewSql());
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
    public void testPivotSubQueryInAuditedViewBodyRecordsThatView() throws Exception {
        assertMemoryLeak(() -> {
            createPivotTablesAndAuditedView();
            // The column names of this view are rows of trades, read when a statement over the
            // view compiles. The principal asked for the view, so that read is a read of the view.
            createAuditedView(
                    "v_pivot",
                    "SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT DISTINCT symbol FROM trades))"
            );
            assertCompileTimePlansRecordAuditsOf("SELECT * FROM v_pivot", "v_pivot");
            // A view that is not audited does not hide the audited one inside it.
            execute("CREATE VIEW v_pivot_wrap AS (SELECT * FROM v_pivot)");
            drainWalAndViewQueues();
            assertCompileTimePlansRecordAuditsOf("SELECT * FROM v_pivot_wrap", "v_pivot");

            // v_audited has no parameter a caller can set, so the record of the view around it
            // covers it, in the sub-query's plan as in the statement's.
            createAuditedView(
                    "v_pivot_cover",
                    "SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT DISTINCT symbol FROM v_audited))"
            );
            assertCompileTimePlansRecordAuditsOf("SELECT * FROM v_pivot_cover", "v_pivot_cover");
            assertRecordsAuditsOf("SELECT * FROM v_pivot_cover", "v_pivot_cover");

            // v_param lets a caller choose the rows, and v_pivot_tag does not record the choice,
            // so v_param keeps its own record, in the sub-query's plan as in the statement's.
            createAuditedView(
                    "v_param",
                    "DECLARE OVERRIDABLE AUDITED @sym := 'AAPL' SELECT symbol, price FROM trades WHERE symbol = @sym"
            );
            createAuditedView(
                    "v_pivot_tag",
                    "SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT DISTINCT symbol FROM v_param))"
            );
            assertCompileTimePlansRecordAuditsOf("SELECT * FROM v_pivot_tag", "v_param, v_pivot_tag");
            assertRecordsAuditsOf("SELECT * FROM v_pivot_tag", "v_param", "v_pivot_tag");
            assertCompileTimePlansRecordAuditsOf("DECLARE @sym := 'MSFT' SELECT * FROM v_pivot_tag", "v_param, v_pivot_tag");

            // v_out reads v_param beside the PIVOT, after the sub-query, which does not read it.
            // The statement records both views, and the sub-query's plan only the view around it.
            createAuditedView(
                    "v_out",
                    """
                            SELECT p.symbol
                            FROM (SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT DISTINCT symbol FROM trades) GROUP BY symbol)) p
                            JOIN v_param q ON p.symbol = q.symbol"""
            );
            assertCompileTimePlansRecordAuditsOf("SELECT * FROM v_out", "v_out");
            assertRecordsAuditsOf("SELECT * FROM v_out", "v_out", "v_param");
        });
    }

    @Test
    public void testPivotSubQueryInNestedAuditedViewsRecordsEachViewNotShadowed() throws Exception {
        assertMemoryLeak(() -> {
            createPivotTablesAndAuditedView();
            // The sub-query sits in v_pivot_param, which lets a caller choose the rows it pivots.
            createAuditedView(
                    "v_pivot_param",
                    """
                            DECLARE OVERRIDABLE AUDITED @sym := 'AAPL'
                            SELECT * FROM (SELECT symbol, price FROM pub WHERE symbol = @sym)
                            PIVOT (sum(price) FOR symbol IN (SELECT DISTINCT symbol FROM trades))"""
            );
            assertCompileTimePlansRecordAuditsOf("SELECT * FROM v_pivot_param", "v_pivot_param");

            // v_tag does not record the caller's choice, so v_pivot_param keeps its own record
            // beside v_tag's, in the sub-query's plan as in the statement's.
            createAuditedView("v_tag", "SELECT * FROM v_pivot_param");
            assertCompileTimePlansRecordAuditsOf("SELECT * FROM v_tag", "v_pivot_param, v_tag");
            assertRecordsAuditsOf("SELECT * FROM v_tag", "v_pivot_param", "v_tag");

            // v_cover records it, so its own record is the only one, in both plans.
            createAuditedView("v_cover", "DECLARE OVERRIDABLE AUDITED @sym := 'AAPL' SELECT * FROM v_pivot_param");
            assertCompileTimePlansRecordAuditsOf("SELECT * FROM v_cover", "v_cover");
            assertRecordsAuditsOf("SELECT * FROM v_cover", "v_cover");

            // Two reads of the view are two expansions of its body, and each sub-query takes the
            // audits of the views around its own.
            assertCompileTimePlansRecordAuditsOf(
                    "SELECT * FROM v_tag UNION ALL SELECT * FROM v_cover",
                    "v_pivot_param, v_tag",
                    "v_cover"
            );
        });
    }

    @Test
    public void testPivotSubQueryInNestedPivotRecordsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createPivotTablesAndAuditedView();
            final String pivot = "SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT DISTINCT symbol FROM v_audited) GROUP BY symbol)";
            assertCompileTimePlansRecordAuditsOf("SELECT * FROM (" + pivot + ")", "v_audited");
            assertCompileTimePlansRecordAuditsOf("SELECT * FROM pub WHERE symbol IN (SELECT symbol FROM (" + pivot + "))", "v_audited");
            assertCompileTimePlansRecordAuditsOf("SELECT p.symbol FROM pub p JOIN (" + pivot + ") q ON p.symbol = q.symbol", "v_audited");
            // One plan per sub-query, each with the reads of its own sub-query.
            assertCompileTimePlansRecordAuditsOf(
                    pivot + " UNION ALL SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT DISTINCT symbol FROM pub) GROUP BY symbol)",
                    "v_audited",
                    ""
            );
            // A PIVOT inside the sub-query of another runs its own sub-query first, while the
            // outer sub-query compiles. The outer sub-query's plan returns the names of the
            // columns the inner one produced, so it carries the audit as well.
            assertCompileTimePlansRecordAuditsOf(
                    "SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT q.symbol FROM (" + pivot + ") q))",
                    "v_audited",
                    "v_audited"
            );
        });
    }

    @Test
    public void testPivotSubQueryInPlainViewBodyRecordsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createPivotTablesAndAuditedView();
            execute("CREATE VIEW v_pivot AS (SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT DISTINCT symbol FROM v_audited)))");
            drainWalAndViewQueues();
            assertCompileTimePlansRecordAuditsOf("SELECT * FROM v_pivot", "v_audited");
        });
    }

    @Test
    public void testPivotSubQueryOfFailingStatementRecordsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createPivotTablesAndAuditedView();
            generatedPlanAudits = new ObjList<>();
            try {
                // The sub-query runs, and a value it read comes back in the error. The statement's
                // own plan never generates, so the sub-query's plan is the only one there is to
                // record the read.
                final String badValues = "SELECT * FROM pub PIVOT (sum(price) FOR price IN (SELECT concat(symbol, '@', price) FROM v_audited))";
                assertExceptionNoLeakCheck(badValues, badValues.indexOf("SELECT concat"), "AAPL@100.5");
                assertEquals("[v_audited]", Arrays.toString(planAudits()));

                // A sub-query that finds no row has still read the view to find that out.
                generatedPlanAudits.clear();
                final String noValues = "SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT symbol FROM v_audited WHERE price > 1_000))";
                assertExceptionNoLeakCheck(noValues, noValues.indexOf("SELECT symbol"), "PIVOT IN subquery returned empty result set");
                assertEquals("[v_audited]", Arrays.toString(planAudits()));
            } finally {
                generatedPlanAudits = null;
            }
        });
    }

    @Test
    public void testPivotSubQueryOfOtherStatementsRecordsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createPivotTablesAndAuditedView();
            execute("CREATE TABLE dest (symbol SYMBOL, aapl DOUBLE, msft DOUBLE)");
            final String pivot = "SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT DISTINCT symbol FROM v_audited) GROUP BY symbol)";
            assertCompileTimePlansRecordAuditsOf("INSERT INTO dest " + pivot, "v_audited");
            assertCompileTimePlansRecordAuditsOf("EXPLAIN " + pivot, "v_audited");
            assertCompileTimePlansRecordAuditsOf("UPDATE dest SET aapl = 1 FROM (" + pivot + ") p WHERE dest.symbol = p.symbol", "v_audited");
            assertCompileTimePlansRecordAuditsOf("UPDATE dest SET aapl = 1 WHERE aapl < (SELECT count() FROM (" + pivot + "))", "v_audited");
            // CREATE TABLE AS SELECT optimises its query when it runs: the sub-query's plan
            // first, then the plan that feeds the new table.
            assertExecutedPlansRecordAuditsOf("CREATE TABLE copy AS (" + pivot + ")", "v_audited", "v_audited");
            // A view compiles its body when it is created, and the sub-query runs then.
            assertExecutedPlansRecordAuditsOf("CREATE VIEW v_pivot AS (" + pivot + ")", "v_audited", "v_audited");
        });
    }

    @Test
    public void testPivotSubQueryPlanRefusedByTheCompilerFailsTheStatement() throws Exception {
        assertMemoryLeak(() -> {
            createPivotTablesAndAuditedView();
            // Enterprise refuses a plan whose audit it cannot build, after the plan generated: it
            // frees the plan and throws. For the sub-query's plan that happens inside the
            // optimiser, which has to come out of it clean, with the borrowed compiler returned.
            final String sql = "SELECT * FROM long_sequence(2) PIVOT (count() FOR x IN (SELECT DISTINCT price::LONG FROM v_audited))";
            isAuditedPlanRefused = true;
            try {
                assertExceptionNoLeakCheck(sql, 0, "refused plan [audits=1]");
            } finally {
                isAuditedPlanRefused = false;
            }
            // The same statement compiles once nothing refuses its plans.
            assertCompileTimePlansRecordAuditsOf(sql, "v_audited");
            select(sql).close();
        });
    }

    @Test
    public void testPivotSubQueryReadingAuditedViewRecordsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createPivotTablesAndAuditedView();
            final String sql = "SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT concat(symbol, '@', price) FROM v_audited))";
            // PIVOT runs the sub-query while the statement compiles, for the names of the columns
            // it produces, so a statement that is prepared and never executed has read the view.
            // Enterprise records a read when the cursor of a plan opens, and the plan that reads
            // here is the sub-query's own: it has to carry the audit, or nothing records the read.
            assertCompileTimePlansRecordAuditsOf(sql, "v_audited");
            // The statement keeps the audit too: the names of the columns its plan returns are
            // rows of the view.
            assertRecordsOneAuditOf("v_audited", sql);
        });
    }

    @Test
    public void testPivotSubQueryReadingAuditedViewThroughCteRecordsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createPivotTablesAndAuditedView();
            // The first reference to a CTE reads the model its definition parsed, before the
            // sub-query began.
            assertCompileTimePlansRecordAuditsOf(
                    """
                            WITH c AS (SELECT DISTINCT symbol FROM v_audited)
                            SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT symbol FROM c))""",
                    "v_audited"
            );
            // So does a definition that reads another CTE.
            assertCompileTimePlansRecordAuditsOf(
                    """
                            WITH c AS (SELECT DISTINCT symbol FROM v_audited), d AS (SELECT symbol FROM c)
                            SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT symbol FROM d))""",
                    "v_audited"
            );
            // A later reference parses the definition again.
            assertCompileTimePlansRecordAuditsOf(
                    """
                            WITH c AS (SELECT DISTINCT symbol FROM v_audited)
                            SELECT * FROM (SELECT * FROM pub WHERE symbol IN (SELECT symbol FROM c))
                            PIVOT (sum(price) FOR symbol IN (SELECT symbol FROM c))""",
                    "v_audited"
            );
            // A CTE the sub-query defines and reads is read once.
            assertCompileTimePlansRecordAuditsOf(
                    """
                            SELECT * FROM pub PIVOT (
                              sum(price)
                              FOR symbol IN (SELECT symbol FROM (WITH c AS (SELECT DISTINCT symbol FROM v_audited) SELECT symbol FROM c)))""",
                    "v_audited"
            );
            // A CTE the sub-query does not read is not one of its reads.
            assertCompileTimePlansRecordAuditsOf(
                    """
                            WITH c AS (SELECT DISTINCT symbol FROM v_audited)
                            SELECT * FROM (SELECT * FROM pub WHERE symbol IN (SELECT symbol FROM c))
                            PIVOT (sum(price) FOR symbol IN (SELECT DISTINCT symbol FROM pub))""",
                    ""
            );
            // Nor are the reads of a definition parsed before the one the sub-query reads.
            createAuditedView("v_other", "SELECT symbol, price FROM pub");
            assertCompileTimePlansRecordAuditsOf(
                    """
                            WITH a AS (SELECT DISTINCT symbol FROM v_audited), b AS (SELECT DISTINCT symbol FROM v_other)
                            SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT symbol FROM b))""",
                    "v_other"
            );
            // Whichever reference takes the definition's model, the statement records the read
            // once for each expansion of the view, as it did.
            assertRecordsAuditsOf(
                    """
                            WITH c AS (SELECT DISTINCT symbol FROM v_audited)
                            SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT symbol FROM c))""",
                    "v_audited"
            );
            assertRecordsAuditsOf("WITH c AS (SELECT DISTINCT symbol FROM v_audited) SELECT * FROM c", "v_audited");
            assertRecordsAuditsOf("WITH c AS (SELECT DISTINCT symbol FROM v_audited) SELECT * FROM pub", "v_audited");
            // The reads an audited view shadows inside a definition stay shadowed for the
            // statement, however the optimiser folds the definition's model into its plan.
            createAuditedView("v_cover", "SELECT DISTINCT symbol FROM v_audited");
            assertRecordsAuditsOf("WITH c AS (SELECT symbol FROM v_cover) SELECT * FROM c", "v_cover");
            assertRecordsAuditsOf("WITH c AS (SELECT symbol FROM v_cover) SELECT symbol FROM c WHERE symbol = 'AAPL'", "v_cover");
            assertCompileTimePlansRecordAuditsOf(
                    """
                            WITH c AS (SELECT symbol FROM v_cover)
                            SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT symbol FROM c))""",
                    "v_cover"
            );
        });
    }

    @Test
    public void testPivotSubQueryReadingAuditedViewThroughDeclaredVariableRecordsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createPivotTablesAndAuditedView();
            // A variable declared inside the sub-query parses there, whichever read takes its model.
            assertCompileTimePlansRecordAuditsOf(
                    """
                            SELECT * FROM pub PIVOT (
                              sum(price)
                              FOR symbol IN (SELECT symbol FROM (DECLARE @q := (SELECT DISTINCT symbol FROM v_audited) SELECT * FROM @q)))""",
                    "v_audited"
            );
            assertCompileTimePlansRecordAuditsOf(
                    """
                            SELECT * FROM pub PIVOT (
                              sum(price)
                              FOR symbol IN (
                                SELECT symbol FROM (
                                  DECLARE @q := (SELECT DISTINCT symbol FROM v_audited)
                                  SELECT DISTINCT symbol FROM pub WHERE symbol IN @q)))""",
                    "v_audited"
            );

            // The variables of the statement around a PIVOT do not reach its clauses, so its
            // sub-query cannot read one of them. If that ever changes, the first read of a
            // declared sub-query takes the model the declaration parsed, before the PIVOT
            // sub-query began, as the first reference to a CTE does, and its reads have to be
            // counted the way parseWith() counts those. Until then, nothing compiles or runs.
            generatedPlanAudits = new ObjList<>();
            try {
                assertExceptionNoLeakCheck(
                        "DECLARE @q := (SELECT DISTINCT symbol FROM v_audited) SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT symbol FROM @q))",
                        124,
                        "table does not exist [table=@q]"
                );
                assertExceptionNoLeakCheck(
                        "DECLARE @q := (SELECT DISTINCT symbol FROM v_audited) SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT DISTINCT symbol FROM pub WHERE symbol IN @q))",
                        153,
                        "Invalid column: @q"
                );
                // Not a sub-query to PIVOT, which takes the brackets for a list of constants.
                assertExceptionNoLeakCheck(
                        "DECLARE @q := (SELECT DISTINCT symbol FROM v_audited) SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (@q))",
                        105,
                        "Invalid column: @q"
                );
                assertExceptionNoLeakCheck(
                        "SELECT * FROM pub PIVOT (sum(price) FOR symbol IN ((SELECT DISTINCT symbol FROM v_audited)))",
                        52,
                        "constant expected"
                );
                assertEquals("[]", Arrays.toString(planAudits()));
            } finally {
                generatedPlanAudits = null;
            }
        });
    }

    @Test
    public void testPivotSubQueryReadingAuditedViewThroughNestedSubQueryRecordsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createPivotTablesAndAuditedView();
            assertCompileTimePlansRecordAuditsOf(
                    "SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT symbol FROM (SELECT DISTINCT symbol FROM v_audited)))",
                    "v_audited"
            );
            assertCompileTimePlansRecordAuditsOf(
                    "SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT DISTINCT symbol FROM pub WHERE symbol IN (SELECT symbol FROM v_audited)))",
                    "v_audited"
            );
            assertCompileTimePlansRecordAuditsOf(
                    "SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT DISTINCT p.symbol FROM pub p JOIN v_audited a ON p.symbol = a.symbol))",
                    "v_audited"
            );
            assertCompileTimePlansRecordAuditsOf(
                    "SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT symbol FROM pub UNION SELECT symbol FROM v_audited))",
                    "v_audited"
            );
        });
    }

    @Test
    public void testPivotSubQueryReadingAuditedViewThroughViewRecordsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createPivotTablesAndAuditedView();
            // A view that is not audited records nothing, and hides nothing.
            execute("CREATE VIEW v_wrap AS (SELECT DISTINCT symbol FROM v_audited)");
            drainWalAndViewQueues();
            assertCompileTimePlansRecordAuditsOf(
                    "SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT symbol FROM v_wrap))",
                    "v_audited"
            );

            // An audited one shadows the reads its record covers, for the sub-query's plan as for
            // the statement's: v_audited has no parameter a caller can set.
            createAuditedView("v_cover", "SELECT DISTINCT symbol FROM v_audited");
            final String sql = "SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT symbol FROM v_cover))";
            assertCompileTimePlansRecordAuditsOf(sql, "v_cover");
            assertRecordsAuditsOf(sql, "v_cover");
        });
    }

    @Test
    public void testPivotSubQueryRecordsOnlyItsOwnReads() throws Exception {
        assertMemoryLeak(() -> {
            createPivotTablesAndAuditedView();
            execute("CREATE VIEW v_plain AS (SELECT symbol, price FROM pub)");
            drainWalAndViewQueues();

            // The statement reads v_audited, and its own plan records that when it runs. The
            // sub-query reads other things, so the plan that runs at compile time has nothing to
            // record: a statement that is prepared and never executed read no row of v_audited.
            final String sql = "SELECT * FROM v_audited PIVOT (sum(price) FOR symbol IN (SELECT DISTINCT symbol FROM pub))";
            assertCompileTimePlansRecordAuditsOf(sql, "");
            assertRecordsOneAuditOf("v_audited", sql);
            assertCompileTimePlansRecordAuditsOf(
                    "SELECT * FROM v_audited PIVOT (sum(price) FOR symbol IN (SELECT DISTINCT symbol FROM v_plain))",
                    ""
            );
            // One plan per sub-query, each with the reads of its own sub-query.
            assertCompileTimePlansRecordAuditsOf(
                    """
                            SELECT * FROM pub PIVOT (
                              sum(price)
                              FOR symbol IN (SELECT DISTINCT symbol FROM pub)
                                  price IN (SELECT DISTINCT price FROM v_audited))""",
                    "",
                    "v_audited"
            );
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
    public void testReadsLeaveAWindowValuedAuditedParameterAsDeclared() throws Exception {
        assertMemoryLeak(() -> {
            // An audited parameter may hold a window function, and the record keeps the
            // declaration's own value rather than a copy. Each read of the parameter names and
            // optimises a copy of the window, so the recorded one comes through as declared. The
            // reads used to share the declared window, which left it under a read's alias.
            execute("""
                    CREATE VIEW v_win AS (
                        DECLARE OVERRIDABLE AUDITED @w := row_number() OVER (PARTITION BY x % 2 ORDER BY x DESC)
                        SELECT x, @w a FROM long_sequence(4)
                    )
                    """);
            drainWalAndViewQueues();
            markViewAudited("v_win");
            assertRecordsOneAuditWithWindowParam(
                    "SELECT * FROM v_win",
                    "@w=row_number() OVER (PARTITION BY x % 2 ORDER BY x DESC)"
            );
            // The copy is the window the rows are read with.
            assertQuery("SELECT * FROM v_win")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\ta
                            1\t2
                            2\t2
                            3\t1
                            4\t1
                            """);
            // A caller's window for the parameter is recorded the same way.
            assertRecordsOneAuditWithWindowParam(
                    "DECLARE @w := rank() OVER (PARTITION BY x % 3 ORDER BY x) SELECT * FROM v_win",
                    "@w=rank() OVER (PARTITION BY x % 3 ORDER BY x)"
            );
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

            // The parsed statement prints the clause where SHOW CREATE VIEW does: after the body.
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CreateViewOperationBuilder builder = (CreateViewOperationBuilder) compiler.generateExecutionModel(
                        "CREATE VIEW v2 AS (SELECT s FROM t)", sqlExecutionContext);
                builder.setAudited(true);
                sink.clear();
                builder.toSink(sink);
                TestUtils.assertEquals("create view v2 as (select-choose s from (t)) with audit", sink);
            }
        });
    }

    @Test
    public void testStoredBodyWithANestedAuditedMarkerFailsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            // CREATE VIEW and ALTER VIEW refuse such a body, so a definition carries one only if
            // something else wrote it. Every read parses the body again, and fails rather than
            // record the read without the parameter. The error sits at the marker in the stored
            // body's text, so its position is an offset into that text, past the end of this
            // statement.
            storeAuditedDefinition("v", "SELECT * FROM (DECLARE AUDITED @x := 1 SELECT @x a)");
            assertExceptionNoLeakCheck("SELECT * FROM v", 23, "AUDITED is only allowed in the top-level DECLARE block");
        });
    }

    @Test
    public void testTopLevelAuditedDeclarationsAreRecorded() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            // The block that opens the body is recorded however the statement spells the body, and
            // a set operation after the block leaves it the top-level one.
            execute("CREATE VIEW v_bare AS DECLARE OVERRIDABLE AUDITED @x := 1 SELECT @x a");
            execute("CREATE VIEW v_brackets AS (DECLARE AUDITED @x := 1 SELECT @x a)");
            execute("CREATE VIEW v_union AS (DECLARE OVERRIDABLE AUDITED @x := 1 SELECT @x a UNION ALL SELECT 2)");
            drainWalAndViewQueues();
            markViewAudited("v_bare");
            markViewAudited("v_brackets");
            markViewAudited("v_union");
            assertRecordsOneAuditWithParams("SELECT * FROM v_bare", "v_bare", "@x=1");
            assertRecordsOneAuditWithParams("SELECT * FROM v_brackets", "v_brackets", "@x=1");
            assertRecordsOneAuditWithParams("DECLARE @x := 5 SELECT * FROM v_union", "v_union", "@x=5");
            // The caller's value is the one the rows are read with.
            assertQuery("DECLARE @x := 5 SELECT * FROM v_union")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            a
                            5
                            2
                            """);

            // A view's body is its own top level wherever the caller reads the view: in a
            // sub-query, a CTE or a set operation branch, and under a DECLARE in a sub-query.
            assertRecordsOneAuditWithParams("SELECT * FROM (SELECT a FROM v_bare)", "v_bare", "@x=1");
            assertRecordsOneAuditWithParams("WITH c AS (SELECT a FROM v_bare) SELECT * FROM c", "v_bare", "@x=1");
            assertRecordsOneAuditWithParams("SELECT 0 a UNION ALL SELECT a FROM v_bare", "v_bare", "@x=1");
            assertRecordsOneAuditWithParams("SELECT * FROM (DECLARE @x := 7 SELECT a FROM v_bare)", "v_bare", "@x=7");

            // ALTER VIEW gives the view a new body, with a top-level block of its own.
            execute("ALTER VIEW v AS (DECLARE AUDITED @s := 'a' SELECT s FROM t WHERE s = @s)");
            drainWalAndViewQueues();
            markViewAudited("v");
            assertRecordsOneAuditWithParams("SELECT * FROM v", "v", "@s='a'");
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
     * Compiles the statement as far as its execution model, which is as far as it gets before its
     * own plan is generated, and asserts the audits on the model of each plan generated on the
     * way: one entry per plan, in generation order, each the names of the views the plan audits,
     * sorted and separated by ", ". Only PIVOT generates a plan that early: it runs its
     * {@code FOR ... IN (SELECT ...)} sub-queries for the names of the columns it produces.
     */
    private static void assertCompileTimePlansRecordAuditsOf(String sql, String... expectedPlanAudits) throws Exception {
        generatedPlanAudits = new ObjList<>();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.generateExecutionModel(sql, sqlExecutionContext);
            assertEquals("wrong plan audits for [" + sql + "]", Arrays.toString(expectedPlanAudits), Arrays.toString(planAudits()));
        } finally {
            generatedPlanAudits = null;
        }
    }

    /**
     * Runs the statement and asserts the audits on the model of each plan it generated, as
     * {@link #assertCompileTimePlansRecordAuditsOf} does. The statement's own plan comes last.
     */
    private static void assertExecutedPlansRecordAuditsOf(String sql, String... expectedPlanAudits) throws Exception {
        generatedPlanAudits = new ObjList<>();
        try {
            execute(sql);
            assertEquals("wrong plan audits for [" + sql + "]", Arrays.toString(expectedPlanAudits), Arrays.toString(planAudits()));
        } finally {
            generatedPlanAudits = null;
        }
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
     * Asserts that the statement records exactly one read, of the given view, with the given
     * parameters, rendered as {@code @name=value} in name order and separated by ", ".
     */
    private static void assertRecordsOneAuditWithParams(String sql, String viewName, String expectedParams) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final ExecutionModel model = compiler.generateExecutionModel(sql, sqlExecutionContext);
            final ObjList<ViewAuditModel> audits = model.getQueryModel().getViewAudits();
            assertEquals("wrong audit count for [" + sql + "]", 1, audits.size());
            final ViewAuditModel audit = audits.getQuick(0);
            TestUtils.assertEquals(viewName, audit.getViewName());
            final StringBuilder params = new StringBuilder();
            for (int i = 0, n = audit.getParamCount(); i < n; i++) {
                if (i > 0) {
                    params.append(", ");
                }
                params.append(audit.getParamName(i)).append('=').append(audit.getParamValue(i).token);
            }
            assertEquals("wrong params for [" + sql + "]", expectedParams, params.toString());
        }
    }

    /**
     * Asserts that the statement records exactly one read, of {@code v_win}, with one parameter
     * whose value is a window function, rendered as {@code @name=function OVER (window)}, and that
     * no read named the window: a read names the copy it makes, not the declared window.
     */
    private static void assertRecordsOneAuditWithWindowParam(String sql, String expectedParam) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final ExecutionModel model = compiler.generateExecutionModel(sql, sqlExecutionContext);
            final ObjList<ViewAuditModel> audits = model.getQueryModel().getViewAudits();
            assertEquals("wrong audit count for [" + sql + "]", 1, audits.size());
            final ViewAuditModel audit = audits.getQuick(0);
            TestUtils.assertEquals("v_win", audit.getViewName());
            assertEquals("wrong param count for [" + sql + "]", 1, audit.getParamCount());
            final ExpressionNode value = audit.getParamValue(0);
            final WindowExpression window = value.windowExpression;
            assertNotNull("no window for [" + sql + "]", window);

            final StringSink param = new StringSink();
            param.put(audit.getParamName(0)).put('=');
            value.toSink(param);
            param.put(" OVER (PARTITION BY ");
            final ObjList<ExpressionNode> partitionBy = window.getPartitionBy();
            for (int i = 0, n = partitionBy.size(); i < n; i++) {
                if (i > 0) {
                    param.put(", ");
                }
                partitionBy.getQuick(i).toSink(param);
            }
            param.put(" ORDER BY ");
            final ObjList<ExpressionNode> orderBy = window.getOrderBy();
            for (int i = 0, n = orderBy.size(); i < n; i++) {
                if (i > 0) {
                    param.put(", ");
                }
                orderBy.getQuick(i).toSink(param);
                if (window.getOrderByDirection().getQuick(i) == IQueryModel.ORDER_DIRECTION_DESCENDING) {
                    param.put(" DESC");
                }
            }
            param.put(')');
            TestUtils.assertEquals(expectedParam, param);
            assertNull("the declared window took a read's alias for [" + sql + "]", window.getAlias());
        }
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

    private static void createPivotTablesAndAuditedView() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE)");
        execute("CREATE TABLE pub (symbol SYMBOL, price DOUBLE)");
        execute("INSERT INTO trades VALUES ('AAPL', 100.5), ('MSFT', 300.75)");
        execute("INSERT INTO pub VALUES ('AAPL', 1.0), ('MSFT', 2.0)");
        drainWalQueue();
        createAuditedView("v_audited", "SELECT symbol, price FROM trades");
    }

    /**
     * OSS has no syntax that sets the flag - {@code WITH AUDIT} is an Enterprise clause - so a test
     * that needs an audited view swaps the graph's definition for one carrying the flag.
     */
    private static void markViewAudited(String viewName) {
        storeAuditedDefinition(viewName, engine.getViewGraph().getViewDefinition(engine.getTableTokenIfExists(viewName)).getViewSql());
    }

    private static String[] planAudits() {
        final String[] planAudits = new String[generatedPlanAudits.size()];
        for (int i = 0, n = generatedPlanAudits.size(); i < n; i++) {
            planAudits[i] = generatedPlanAudits.getQuick(i);
        }
        return planAudits;
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

    /**
     * Swaps the graph's definition of the view for an audited one with the given body, the way
     * {@link #markViewAudited} does, without going through the statements that parse a body.
     */
    private static void storeAuditedDefinition(String viewName, String viewSql) {
        final TableToken viewToken = engine.getTableTokenIfExists(viewName);
        final ViewGraph viewGraph = engine.getViewGraph();
        final ViewDefinition current = viewGraph.getViewDefinition(viewToken);
        final ViewDefinition audited = new ViewDefinition();
        audited.init(viewToken, viewSql, current.getDependencies(), current.getSeqTxn(), true);
        viewGraph.removeView(viewToken);
        assertTrue(viewGraph.addView(audited));
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

    /**
     * Notes the audits on the model of every plan it generates, where Enterprise's compiler reads
     * them to decide whether the plan records a read when its cursor opens.
     */
    private static class PlanAuditRecordingCompiler extends SqlCompilerImpl {
        private PlanAuditRecordingCompiler(CairoEngine engine) {
            super(engine);
        }

        @Override
        protected RecordCursorFactory generateSelectOneShot(
                IQueryModel selectQueryModel,
                SqlExecutionContext executionContext,
                boolean generateProgressLogger
        ) throws SqlException {
            // Enterprise generates the plan first too, and wraps only a plan that generated.
            final RecordCursorFactory factory = super.generateSelectOneShot(selectQueryModel, executionContext, generateProgressLogger);
            final ObjList<ViewAuditModel> audits = selectQueryModel.getViewAudits();
            if (isAuditedPlanRefused && audits.size() > 0) {
                Misc.free(factory);
                throw SqlException.$(0, "refused plan [audits=").put(audits.size()).put(']');
            }
            if (generatedPlanAudits != null) {
                final String[] viewNames = new String[audits.size()];
                for (int i = 0, n = audits.size(); i < n; i++) {
                    viewNames[i] = audits.getQuick(i).getViewName().toString();
                }
                Arrays.sort(viewNames);
                generatedPlanAudits.add(String.join(", ", viewNames));
            }
            return factory;
        }
    }
}
