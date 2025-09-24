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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewGraph;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateStoreImpl;
import io.questdb.std.Numbers;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MatViewGraphAndStateStoreTest extends AbstractCairoTest {
    private final MatViewGraph graph = new MatViewGraph();
    private final ObjList<TableToken> ordered = new ObjList<>();
    private final MatViewStateStoreImpl stateStore = new MatViewStateStoreImpl(engine);
    private final ObjHashSet<TableToken> tableTokens = new ObjHashSet<>();

    @Before
    public void setUp() {
        tableTokens.clear();
        ordered.clear();
        stateStore.clear();
        graph.clear();
    }

    @Test
    public void testAddSameViewTwice() {
        TableToken table1 = newTableToken("table1");
        TableToken view1 = newViewToken("view1");

        MatViewDefinition viewDefinition = createDefinition(view1, table1);
        try {
            stateStore.addViewState(viewDefinition);
            stateStore.addViewState(viewDefinition);
            Assert.fail("store exception expected");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "materialized view state already exists");
        }

        Assert.assertTrue(graph.addView(viewDefinition));
        Assert.assertFalse(graph.addView(viewDefinition));
    }

    // loops
    @Test
    public void testDirectSelfLoop() {
        TableToken viewA = newViewToken("viewA");

        MatViewDefinition viewDefinition = createDefinition(viewA, viewA);
        try {
            graph.addView(viewDefinition);
            Assert.fail("Expected a dependency loop exception");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "circular dependency detected");
        }
    }

    @Test
    public void testDroppedState() {
        TableToken table1 = newTableToken("table1");
        TableToken view1 = newViewToken("view1");
        MatViewDefinition viewDefinition = createDefinition(view1, table1);
        graph.addView(viewDefinition);
        MatViewState state = stateStore.addViewState(viewDefinition);
        Assert.assertNotNull(state);
        state.markAsDropped();
        state = stateStore.getViewState(view1);
        Assert.assertNotNull(state);
        MatViewDefinition def = graph.getViewDefinition(view1);
        Assert.assertNotNull(def);
        state = stateStore.getViewState(view1);
        Assert.assertNull(state);
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
        Assert.assertEquals(6, ordered.size());
        Assert.assertEquals("table2", ordered.getQuick(0).getTableName());
        Assert.assertEquals("table3", ordered.getQuick(1).getTableName());
        Assert.assertEquals("view3", ordered.getQuick(2).getTableName());
        Assert.assertEquals("view1", ordered.getQuick(3).getTableName());
        Assert.assertEquals("view2", ordered.getQuick(4).getTableName());
        Assert.assertEquals("table1", ordered.getQuick(5).getTableName());

    }

    @Test
    public void testIndirectLoopViaSharedDependency() {
        TableToken viewA = newViewToken("viewA");
        TableToken viewB = newViewToken("viewB");
        TableToken viewC = newViewToken("viewC");

        addDefinition(viewA, viewB);
        addDefinition(viewC, viewB);
        MatViewDefinition viewDefinition = createDefinition(viewB, viewA);

        try {
            graph.addView(viewDefinition);
            Assert.fail("Expected a dependency loop exception");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "circular dependency detected");
        }
    }

    @Test
    public void testNoViews() {
        newTableToken("table1");
        newTableToken("table2");
        graph.orderByDependentViews(tableTokens, ordered);
        Assert.assertEquals(2, ordered.size());
        Assert.assertEquals("table1", ordered.getQuick(0).getTableName());
        Assert.assertEquals("table2", ordered.getQuick(1).getTableName());
    }

    @Test
    public void testNoViewsNoTables() {
        graph.orderByDependentViews(tableTokens, ordered);
        Assert.assertEquals(0, ordered.size());
    }

    @Test
    public void testSingleView() {
        TableToken table1 = newTableToken("table1");
        TableToken view1 = newViewToken("view1");
        addDefinition(view1, table1);
        graph.orderByDependentViews(tableTokens, ordered);
        Assert.assertEquals(2, ordered.size());
        Assert.assertEquals("view1", ordered.getQuick(0).getTableName());
        Assert.assertEquals("table1", ordered.getQuick(1).getTableName());
    }

    @Test
    public void testThreeLevelLoop() {
        TableToken viewA = newViewToken("viewA");
        TableToken viewB = newViewToken("viewB");
        TableToken viewC = newViewToken("viewC");

        addDefinition(viewA, viewB);
        addDefinition(viewB, viewC);
        MatViewDefinition viewDefinition = createDefinition(viewC, viewA);

        try {
            graph.addView(viewDefinition);
            Assert.fail("Expected a dependency loop exception");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "circular dependency detected");
        }
    }

    @Test
    public void testTwoLevelLoop() {
        TableToken viewA = newViewToken("viewA");
        TableToken viewB = newViewToken("viewB");

        addDefinition(viewA, viewB);
        MatViewDefinition viewDefinition = createDefinition(viewB, viewA);

        try {
            graph.addView(viewDefinition);
            Assert.fail("Expected a dependency loop exception");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "circular dependency detected");
        }
    }

    private void addDefinition(TableToken viewToken, TableToken baseTableToken) {
        MatViewDefinition viewDefinition = createDefinition(viewToken, baseTableToken);
        stateStore.addViewState(viewDefinition);
        graph.addView(viewDefinition);
    }

    private MatViewDefinition createDefinition(TableToken viewToken, TableToken baseTableToken) {
        MatViewDefinition viewDefinition = new MatViewDefinition();
        viewDefinition.init(
                MatViewDefinition.REFRESH_TYPE_IMMEDIATE,
                false,
                ColumnType.TIMESTAMP_MICRO,
                viewToken,
                "x",
                baseTableToken.getTableName(),
                0,
                'm',
                null,
                null,
                0,
                0,
                (char) 0,
                Numbers.LONG_NULL,
                null,
                0,
                (char) 0,
                0,
                (char) 0
        );
        return viewDefinition;
    }

    private TableToken newTableToken(String tableName) {
        TableToken t = new TableToken(tableName, tableName, null, 0, false, true, false, false, true);
        tableTokens.add(t);
        return t;
    }

    private TableToken newViewToken(String tableName) {
        TableToken v = new TableToken(tableName, tableName, null, 0, true, true, false, false, true);
        tableTokens.add(v);
        return v;
    }
}
