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
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.view.ViewGraph;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.ViewAuditModel;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

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
    public void testCreateTableAsSelectFromAuditedViewRecordsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            markViewAudited("v");
            assertRecordsOneAuditOf("v", "CREATE TABLE copy AS (SELECT s FROM v)");
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
    public void testSelectFromAuditedViewRecordsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            markViewAudited("v");
            assertRecordsOneAuditOf("v", "SELECT s FROM v");
        });
    }

    @Test
    public void testUpdateReadingAnAuditedViewRecordsTheRead() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndView();
            execute("CREATE TABLE dest (s SYMBOL, l LONG)");
            markViewAudited("v");
            assertRecordsOneAuditOf("v", "UPDATE dest SET l = 1 FROM v WHERE dest.s = v.s");
        });
    }

    private static void assertRecordsOneAuditOf(String viewName, String sql) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final ExecutionModel model = compiler.generateExecutionModel(sql, sqlExecutionContext);
            final IQueryModel queryModel = model.getQueryModel();
            assertNotNull("no query model for [" + sql + "]", queryModel);
            assertEquals("wrong audit count for [" + sql + "]", 1, queryModel.getViewAudits().size());
            final ViewAuditModel audit = queryModel.getViewAudits().getQuick(0);
            TestUtils.assertEquals(viewName, audit.getViewName());
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
