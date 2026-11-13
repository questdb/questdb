/*+*****************************************************************************
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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.view.ViewState;
import io.questdb.cairo.view.ViewStateStoreImpl;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.std.LongList;
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
    // 2025-06-19T11:00:00.000000Z
    static final long ALTER_MICROS = 1_750_330_800_000_000L;
    // 2025-06-19T10:00:00.000000Z
    static final long CREATE_MICROS = 1_750_327_200_000_000L;
    // A PIVOT over the table t (k SYMBOL, x INT). Its compile runs the IN sub-query over the rows of
    // t for the names of the columns it produces, so the compile fails while t is empty.
    static final String PIVOT_SQL = "SELECT * FROM t PIVOT (sum(x) FOR k IN (SELECT DISTINCT k FROM t ORDER BY k))";
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

    // The view compile busy retries that the view state store keeps, one line per view in the order
    // of the view names, each as pendingCompileBusyRetry() prints it.
    static void assertPendingCompileBusyRetries(String expected) {
        final ObjList<TableToken> viewTokens = new ObjList<>();
        final LongList updateTimestamps = new LongList();
        final LongList dueTimestamps = new LongList();
        ((ViewStateStoreImpl) engine.getViewStateStore()).getPendingCompileBusyRetries(viewTokens, updateTimestamps, dueTimestamps);
        final ObjList<String> lines = new ObjList<>();
        for (int i = 0, n = viewTokens.size(); i < n; i++) {
            lines.add(pendingCompileBusyRetry(viewTokens.getQuick(i).getTableName(), updateTimestamps.getQuick(i), dueTimestamps.getQuick(i)));
        }
        lines.sort(String::compareTo);
        final StringSink sink = new StringSink();
        for (int i = 0, n = lines.size(); i < n; i++) {
            sink.put(lines.getQuick(i));
        }
        Assert.assertEquals(expected, sink.toString());
    }

    static void assertViewColumns(String viewName, String expectedColumns) {
        try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName(viewName))) {
            Assert.assertEquals(expectedColumns, columnsOf(metadata));
        }
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

    // the delay of a view compile busy retry, which cairo.view.compile.busy.retry.timeout sets
    static long busyRetryTimeoutMicros() {
        return configuration.getViewCompileBusyRetryTimeout() * 1_000;
    }

    // the columns of the metadata as name:TYPE, separated by commas
    static String columnsOf(RecordMetadata metadata) {
        final StringSink sink = new StringSink();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            sink.put(metadata.getColumnName(i)).put(':').put(ColumnType.nameOf(metadata.getColumnType(i)));
        }
        return sink.toString();
    }

    // creates the non-WAL table t (k SYMBOL, x INT) with the rows ('a', 1) and ('b', 2), which PIVOT_SQL reads
    static void createPivotTable() throws SqlException {
        execute("CREATE TABLE t (k SYMBOL, x INT)");
        execute("INSERT INTO t VALUES ('a', 1), ('b', 2)");
        Assert.assertFalse(engine.verifyTableName("t").isWal());
    }

    // creates a non-partitioned table (a INT), which is never a WAL table, with the rows 1 and 2
    static void createTableWithRows(String tableName) throws SqlException {
        execute("CREATE TABLE " + tableName + " (a INT)");
        execute("INSERT INTO " + tableName + " VALUES (1), (2)");
        Assert.assertFalse(engine.verifyTableName(tableName).isWal());
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

    // a line of assertPendingCompileBusyRetries(): the view name, the update timestamp the retry
    // compiles the view with, and the time the retry is due at
    static String pendingCompileBusyRetry(String viewName, long updateTimestamp, long dueTimestamp) {
        return viewName + '\t' + updateTimestamp + '\t' + dueTimestamp + '\n';
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
        assertQuery(query)
                .noLeakCheck()
                .timestamp(expectedTimestamp)
                .supportsRandomAccess(supportsRandomAccess)
                .expectSize(expectSize)
                .returns(expected);
        assertReferencedViews(query, expectedReferencedViews);
        assertQuery("explain " + query)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns(expectedPlan);
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

    void assertViewStatus(String viewName, String status, String reason, String updateTime) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason, view_status_update_time FROM views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize(false)
                .returns("view_status\tinvalidation_reason\tview_status_update_time\n"
                        + status + '\t' + reason + '\t' + updateTime + '\n');
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
