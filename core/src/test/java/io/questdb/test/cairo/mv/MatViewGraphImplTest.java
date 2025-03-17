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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewGraph;
import io.questdb.cairo.mv.MatViewGraphImpl;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewRefreshState;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MatViewGraphImplTest extends AbstractCairoTest {
    private final MatViewGraphImpl graph = new MatViewGraphImpl(engine);
    private final ObjList<TableToken> ordered = new ObjList<>();
    private final ObjHashSet<TableToken> tableTokens = new ObjHashSet<>();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ENABLED, "true");
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ENABLED, "true");
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        tableTokens.clear();
        ordered.clear();
        graph.clear();
    }

    @Test
    public void testAddSameViewTwice() {
        TableToken table1 = newTableToken("table1");
        TableToken view1 = newViewToken("view1");

        MatViewDefinition viewDefinition = createDefinition(view1, table1);
        try {
            graph.addView(viewDefinition);
            graph.addView(viewDefinition);
            Assert.fail("exception expected");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "materialized view state already exists");
        }
    }

    @Test
    public void testDroppedState() {
        TableToken table1 = newTableToken("table1");
        TableToken view1 = newViewToken("view1");
        MatViewDefinition viewDefinition = createDefinition(view1, table1);
        MatViewRefreshState state = graph.addView(viewDefinition);
        Assert.assertNotNull(state);
        state.markAsDropped();
        state = graph.getViewRefreshState(view1);
        Assert.assertNotNull(state);
        MatViewDefinition def = graph.getViewDefinition(view1);
        Assert.assertNull(def);
        state = graph.getViewRefreshState(view1);
        Assert.assertNull(state);
        def = graph.getViewDefinition(view1);
        Assert.assertNull(def);
    }

    @Test
    public void testGraphDependency() {
        //  table1   table2   table3
        //  /    \
        //v1      v2
        // |
        //v3
        newTableToken("table2");
        newTableToken("table3");
        TableToken view1 = newViewToken("view1");
        addDefinition(view1, newTableToken("table1"));
        addDefinition(newViewToken("view2"), newTableToken("table1"));
        addDefinition(newViewToken("view3"), view1);

        graph.orderByDependentViews(tableTokens, ordered);
        assertEquals(6, ordered.size());
        assertEquals("table2", ordered.getQuick(0).getTableName());
        assertEquals("table3", ordered.getQuick(1).getTableName());
        assertEquals("view3", ordered.getQuick(2).getTableName());
        assertEquals("view1", ordered.getQuick(3).getTableName());
        assertEquals("view2", ordered.getQuick(4).getTableName());
        assertEquals("table1", ordered.getQuick(5).getTableName());

    }

    @Test
    public void testMatViewConsistencyCheck() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "table_base";
            String viewName = "test";
            execute(
                    "create table if not exists " + tableName +
                            " (ts timestamp, k symbol, v long)" +
                            " timestamp(ts) partition by day wal"
            );

            final String query = "select ts, v+v doubleV, avg(v) from " + tableName + " sample by 30s";
            execute("create materialized view " + viewName + " as (" + query + ") partition by day");
            refresh();

            for (int i = 0; i < 9; i++) {
                execute("insert into " + tableName + " values (" + (i * 10000000) + ", 'k" + i + "', " + i + ")");
                refresh();
            }

            refresh();

            final TableToken matViewToken = engine.getTableTokenIfExists(viewName);
            final MatViewRefreshState matViewRefreshState = engine.getMatViewGraph().getViewRefreshState(matViewToken);
            assertNotNull(matViewRefreshState);


            final TableToken baseTable = engine.getTableTokenIfExists(tableName);
            long baseWalTxn = engine.getTableSequencerAPI().getTxnTracker(baseTable).getSeqTxn();
            long viewWalTxn = engine.getTableSequencerAPI().getTxnTracker(matViewToken).getSeqTxn();

            matViewRefreshState.setLastRefreshBaseTxn(baseWalTxn + 4);
            matViewRefreshState.setSeqTxn(viewWalTxn + 2);

            try (Path path = new Path()) {
                try (BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode())) {
                    writer.of(path.of(configuration.getDbRoot()).concat(matViewToken).concat(MatViewRefreshState.MAT_VIEW_STATE_FILE_NAME).$());
                    MatViewRefreshState.append(matViewRefreshState, writer);
                }

                try (BlockFileReader reader = new BlockFileReader(configuration)) {
                    reader.of(path.of(configuration.getDbRoot()).concat(matViewToken).concat(MatViewRefreshState.MAT_VIEW_STATE_FILE_NAME).$());
                    MatViewDefinition def = engine.getMatViewGraph().getViewDefinition(matViewToken);
                    assertNotNull(def);
                    MatViewRefreshState actualState = new MatViewRefreshState(
                            def,
                            false,
                            (event, tableToken, baseTableTxn, errorMessage, latencyUs) -> {
                            }
                    );
                    MatViewRefreshState.readFrom(reader, actualState);
                    assertEquals(viewWalTxn + 2, actualState.getSeqTxn());
                    assertEquals(baseWalTxn + 4, actualState.getLastRefreshBaseTxn());
                }
                MatViewGraph matViewGraph = engine.getMatViewGraph();
                matViewGraph.clear();
                engine.buildMatViewGraph(true);
                refresh();

                final MatViewRefreshState newState = engine.getMatViewGraph().getViewRefreshState(matViewToken);
                assertNotNull(newState);
                // basWalTxn recovered from wal-e
                assertEquals(baseWalTxn, newState.getLastRefreshBaseTxn());
            }
        });
    }

    @Test
    public void testNoViews() {
        newTableToken("table1");
        newTableToken("table2");
        graph.orderByDependentViews(tableTokens, ordered);
        assertEquals(2, ordered.size());
        assertEquals("table1", ordered.getQuick(0).getTableName());
        assertEquals("table2", ordered.getQuick(1).getTableName());
    }

    @Test
    public void testNoViewsNoTables() {
        graph.orderByDependentViews(tableTokens, ordered);
        assertEquals(0, ordered.size());
    }

    @Test
    public void testSingleView() {
        TableToken table1 = newTableToken("table1");
        TableToken view1 = newViewToken("view1");
        addDefinition(view1, table1);
        graph.orderByDependentViews(tableTokens, ordered);
        assertEquals(2, ordered.size());
        assertEquals("view1", ordered.getQuick(0).getTableName());
        assertEquals("table1", ordered.getQuick(1).getTableName());
    }

    private static void refresh() {
        try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine)) {
            drainWalQueue();
            while (refreshJob.run(0)) {
            }
            drainWalQueue();
        }
    }

    private void addDefinition(TableToken viewToken, TableToken baseTableToken) {
        MatViewDefinition viewDefinition = createDefinition(viewToken, baseTableToken);
        graph.addView(viewDefinition);
    }

    private MatViewDefinition createDefinition(TableToken viewToken, TableToken baseTableToken) {
        MatViewDefinition viewDefinition = new MatViewDefinition();
        viewDefinition.init(
                MatViewDefinition.INCREMENTAL_REFRESH_TYPE,
                viewToken,
                "x",
                baseTableToken.getTableName(),
                0,
                'm',
                null,
                null
        );
        return viewDefinition;
    }

    private TableToken newTableToken(String tableName) {
        TableToken t = new TableToken(tableName, tableName, 0, false, true, false, false, true);
        tableTokens.add(t);
        return t;
    }

    private TableToken newViewToken(String tableName) {
        TableToken v = new TableToken(tableName, tableName, 0, true, true, false, false, true);
        tableTokens.add(v);
        return v;
    }
}
