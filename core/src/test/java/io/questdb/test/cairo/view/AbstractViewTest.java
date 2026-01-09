/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.PropertyKey;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.view.ViewState;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.BeforeClass;

import static io.questdb.cairo.SqlJitMode.JIT_MODE_DISABLED;
import static org.junit.Assert.*;

class AbstractViewTest extends AbstractCairoTest {
    static final String TABLE1 = "table1";
    static final String TABLE2 = "table2";
    static final String TABLE3 = "table3";
    static final String TABLE4 = "table4";
    static final String VIEW1 = "view1";
    static final String VIEW2 = "view2";
    static final String VIEW3 = "view3";
    static final String VIEW4 = "view4";
    private final static StringSink sink = new StringSink();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // JIT does not support ARM, and we want query plans to be the same
        setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(JIT_MODE_DISABLED));
        AbstractCairoTest.setUpStatic();
    }

    private void assertReferencedViews(String query, String[] expectedReferencedViews) throws SqlException {
        if (expectedReferencedViews == null || expectedReferencedViews.length == 0) {
            return;
        }

        final LowerCaseCharSequenceHashSet expectedRefViews = new LowerCaseCharSequenceHashSet();
        for (String expectedReferencedView : expectedReferencedViews) {
            expectedRefViews.add(expectedReferencedView);
        }

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final ExecutionModel model = compiler.generateExecutionModel(query, sqlExecutionContext);
            final ObjList<ViewDefinition> referencedViews = model.getQueryModel().getReferencedViews();
            for (int i = 0; i < referencedViews.size(); i++) {
                final ViewDefinition viewDefinition = referencedViews.get(i);
                assertTrue(expectedRefViews.remove(viewDefinition.getViewToken().getTableName()) > -1);
            }
        }
        assertEquals(0, expectedRefViews.size());
    }

    static void assertViewDefinition(String name, String query, String... expectedDependencies) {
        final ViewDefinition viewDefinition = getViewDefinition(name);
        assertNotNull(viewDefinition);
        assertTrue(viewDefinition.getViewToken().isView());
        assertEquals(query, viewDefinition.getViewSql());

        if (expectedDependencies != null && expectedDependencies.length > 0) {
            final LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = viewDefinition.getDependencies();
            assertNotNull(dependencies);
            assertEquals(expectedDependencies.length, dependencies.size());
            for (String expectedDependency : expectedDependencies) {
                if (!dependencies.contains(expectedDependency)) {
                    fail(expectedDependency + " not in " + dependencies);
                }
            }
        }
    }

    static void assertViewDefinitionFile(String name, String query) {
        final TableToken viewToken = engine.getTableTokenIfExists(name);
        try (
                BlockFileReader reader = new BlockFileReader(configuration);
                Path path = new Path()
        ) {
            path.of(configuration.getDbRoot());
            final int rootLen = path.size();
            ViewDefinition viewDefinition = new ViewDefinition();
            ViewDefinition.readFrom(
                    viewDefinition,
                    reader,
                    path,
                    rootLen,
                    viewToken
            );

            assertEquals(query, viewDefinition.getViewSql());
        }
    }

    static void assertViewMetadata(String expectedMetadataJson) {
        final TableToken viewToken = engine.getTableTokenIfExists(AbstractViewTest.VIEW1);
        final StringSink sink = new StringSink();
        try (TableMetadata actualMetadata = engine.getTableMetadata(viewToken)) {
            actualMetadata.toJson(sink);
        }
        assertEquals(expectedMetadataJson, sink.toString());
    }

    static void assertViewState(String name) {
        assertViewState(name, null);
    }

    static void assertViewState(String name, String invalidationReason) {
        final TableToken viewToken = engine.getTableTokenIfExists(name);
        assertNotNull(viewToken);
        assertFalse(engine.getTableSequencerAPI().isSuspended(viewToken));

        final ViewState viewState = engine.getViewStateStore().getViewState(viewToken);
        assertNotNull(viewState);

        try {
            viewState.lockForRead();
            if (invalidationReason != null) {
                assertTrue(viewState.isInvalid());
                assertEquals(invalidationReason, viewState.getInvalidationReason(sink).toString());
            } else {
                assertFalse(viewState.isInvalid());
                assertTrue(viewState.getInvalidationReason(sink).toString().isEmpty());
            }
        } finally {
            viewState.unlockAfterRead();
        }
    }

    @NotNull
    static String getView1DefinitionSql() {
        final ViewDefinition viewDefinition = getViewDefinition(VIEW1);
        Assert.assertNotNull(viewDefinition);
        var sql = viewDefinition.getViewSql();
        Assert.assertNotNull(sql);
        return sql;
    }

    static ViewDefinition getViewDefinition(String viewName) {
        final TableToken viewToken = engine.getTableTokenIfExists(viewName);
        if (viewToken == null) {
            return null;
        }
        return engine.getViewGraph().getViewDefinition(viewToken);
    }

    void alterView(String viewQuery, String... expectedDependencies) throws SqlException {
        execute("ALTER VIEW " + VIEW1 + " AS (" + viewQuery + ")");
        drainWalAndViewQueues();
        assertViewDefinition(VIEW1, viewQuery, expectedDependencies);
        assertViewDefinitionFile(VIEW1, viewQuery);
        assertViewState(VIEW1);
    }

    void assertQueryAndPlan(String expected, String query, String expectedPlan, String... expectedReferencedViews) throws Exception {
        assertQueryAndPlan(expected, query, null, true, true, expectedPlan, expectedReferencedViews);
    }

    void assertQueryAndPlan(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize, String expectedPlan, String... expectedReferencedViews) throws Exception {
        assertQueryNoLeakCheck(expected, query, expectedTimestamp, supportsRandomAccess, expectSize);
        assertReferencedViews(query, expectedReferencedViews);
        assertQueryNoLeakCheck(expectedPlan, "explain " + query, null, false, true);
    }

    void assertView1AlterFailure(String newViewQuery) {
        String sqlBefore = getView1DefinitionSql();
        try {
            execute("ALTER VIEW " + VIEW1 + " AS (" + newViewQuery + ")");
            fail("Expected ALTER VIEW to fail");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "circular dependency detected");
        }
        assertViewDefinition(VIEW1, sqlBefore, TABLE1);
    }

    void compileView(String viewName) throws SqlException {
        execute("COMPILE VIEW " + viewName);
        drainViewQueue();
    }

    void compileView(String viewName, String expectedErrorMessage) throws Exception {
        assertExceptionNoLeakCheck("COMPILE VIEW " + viewName, "COMPILE VIEW ".length(), expectedErrorMessage);
    }

    void createMatView(String matViewName, String matViewQuery) throws SqlException {
        execute("CREATE MATERIALIZED VIEW " + matViewName + " AS (" + matViewQuery + ") PARTITION BY DAY");
        drainViewQueue();
        drainWalAndMatViewQueues();
    }

    void createOrReplaceView(String viewQuery, String... expectedDependencies) throws SqlException {
        execute("CREATE OR REPLACE VIEW " + VIEW1 + " AS (" + viewQuery + ")");
        drainWalAndViewQueues();
        assertViewDefinition(VIEW1, viewQuery, expectedDependencies);
        assertViewDefinitionFile(VIEW1, viewQuery);
        assertViewState(VIEW1);
    }

    void createTable(String tableName) throws SqlException {
        execute(
                "create table if not exists " + tableName +
                        " (ts timestamp, k symbol capacity 2048, k2 symbol capacity 512, v long)" +
                        " timestamp(ts) partition by day wal"
        );
        for (int i = 0; i < 9; i++) {
            execute("insert into " + tableName + " values (" + (i * 10000000) + ", 'k" + i + "', " + "'k2_" + i + "', " + i + ")");
        }
        drainWalQueue();
    }

    void createView(String viewName, String viewQuery, String... expectedDependencies) throws SqlException {
        execute("CREATE VIEW " + viewName + " AS (" + viewQuery + ")");
        drainWalAndViewQueues();
        assertViewDefinition(viewName, viewQuery, expectedDependencies);
        assertViewDefinitionFile(viewName, viewQuery);
        assertViewState(viewName);
    }
}
