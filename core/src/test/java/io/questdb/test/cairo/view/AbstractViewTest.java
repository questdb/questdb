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
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.view.ViewState;
import io.questdb.cairo.view.ViewStateReader;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
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

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // JIT does not support ARM, and we want query plans to be the same
        setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(JIT_MODE_DISABLED));
        setProperty(PropertyKey.CAIRO_VIEW_ENABLED, "true");
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void setUp() {
        // enable views
        setProperty(PropertyKey.CAIRO_VIEW_ENABLED, "true");
        super.setUp();
    }

    static void assertViewDefinition(String name, String query, String... expectedDependencies) {
        final ViewDefinition viewDefinition = getViewDefinition(name);
        assertNotNull(viewDefinition);
        assertTrue(viewDefinition.getViewToken().isView());
        assertEquals(query, viewDefinition.getViewSql());

        if (expectedDependencies != null && expectedDependencies.length > 0) {
            final ObjList<CharSequence> dependencies = viewDefinition.getDependencies();
            assertNotNull(dependencies);
            assertEquals(expectedDependencies.length, dependencies.size());
            for (int i = 0, n = expectedDependencies.length; i < n; i++) {
                assertTrue(dependencies.contains(expectedDependencies[i]));
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

    static void assertViewStateFile(String name) {
        assertViewStateFile(name, null);
    }

    static void assertViewStateFile(String name, String invalidationReason) {
        final TableToken viewToken = engine.getTableTokenIfExists(name);
        try (
                BlockFileReader reader = new BlockFileReader(configuration);
                Path path = new Path()
        ) {
            reader.of(path.of(configuration.getDbRoot()).concat(viewToken).concat(ViewState.VIEW_STATE_FILE_NAME).$());

            final ViewStateReader viewStateReader = new ViewStateReader();
            viewStateReader.of(reader, viewToken);

            if (invalidationReason != null) {
                assertTrue(viewStateReader.isInvalid());
                assertNotNull(viewStateReader.getInvalidationReason());
                assertEquals(invalidationReason, viewStateReader.getInvalidationReason().toString());
            } else {
                assertFalse(viewStateReader.isInvalid());
                assertNull(viewStateReader.getInvalidationReason());
            }
        }
    }

    static ViewDefinition getViewDefinition(String viewName) {
        final TableToken viewToken = engine.getTableTokenIfExists(viewName);
        if (viewToken == null) {
            return null;
        }
        return engine.getViewGraph().getViewDefinition(viewToken);
    }

    void assertQueryAndPlan(String expected, String query, String expectedPlan) throws Exception {
        assertQueryAndPlan(expected, query, null, true, true, expectedPlan);
    }

    void assertQueryAndPlan(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize, String expectedPlan) throws Exception {
        assertQueryNoLeakCheck(expected, query, expectedTimestamp, supportsRandomAccess, expectSize);
        assertQueryNoLeakCheck(expectedPlan, "explain " + query, null, false, true);
    }

    void compileView(String viewName) throws SqlException {
        execute("COMPILE VIEW " + viewName);
        drainViewQueue();
    }

    void compileView(String viewName, String expectedErrorMessage) throws Exception {
        assertExceptionNoLeakCheck("COMPILE VIEW " + viewName, 14, expectedErrorMessage);
    }

    void createMatView(String matViewName, String matViewQuery) throws SqlException {
        execute("CREATE MATERIALIZED VIEW " + matViewName + " AS (" + matViewQuery + ") PARTITION BY DAY");
        drainViewQueue();
        drainWalAndMatViewQueues();
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
        drainViewQueue();
        assertViewDefinition(viewName, viewQuery, expectedDependencies);
        assertViewDefinitionFile(viewName, viewQuery);
        assertViewStateFile(viewName);
    }
}
