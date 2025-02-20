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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewGraphImpl;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MatViewGraphImplTest extends AbstractCairoTest {
    private final MatViewGraphImpl graph = new MatViewGraphImpl(engine);
    private final ObjList<TableToken> ordered = new ObjList<>();
    private final ObjHashSet<TableToken> tableTokens = new ObjHashSet<>();

    @Before
    public void setUp() {
        tableTokens.clear();
        ordered.clear();
        graph.clear();
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

    private void addDefinition(TableToken viewToken, TableToken baseTableToken) {
        MatViewDefinition def = new MatViewDefinition(viewToken, "x", baseTableToken.getTableName(), 0, 'm', null, null);
        graph.addView(def);
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
