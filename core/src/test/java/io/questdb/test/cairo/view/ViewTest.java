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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.view.ViewState;
import io.questdb.cairo.view.ViewStateReader;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ViewTest extends AbstractCairoTest {
    private static final String TABLE1 = "table1";
    private static final String TABLE2 = "table2";
    private static final String VIEW1 = "view1";
    private static final String VIEW2 = "view2";

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_VIEW_ENABLED, "true");
    }

    @Test
    public void testCreateView() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);
            drainWalQueue();

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ");");
            drainWalQueue();
            assertViewDefinition(VIEW1, query1);
            assertViewDefinitionFile(VIEW1, query1);
            assertViewStateFile(VIEW1);

            final String query2 = "select ts, k2, max(v) as v_max from " + TABLE2 + " where v > 6";
            execute("CREATE VIEW " + VIEW2 + " AS (" + query2 + ");");
            drainWalQueue();
            assertViewDefinition(VIEW2, query2);
            assertViewDefinitionFile(VIEW2, query2);
            assertViewStateFile(VIEW1);

            assertQueryNoLeakCheck(
                    "ts\tk\tk2\tv\n" +
                            "1970-01-01T00:00:00.000000Z\tk0\tk2_0\t0\n" +
                            "1970-01-01T00:00:10.000000Z\tk1\tk2_1\t1\n" +
                            "1970-01-01T00:00:20.000000Z\tk2\tk2_2\t2\n" +
                            "1970-01-01T00:00:30.000000Z\tk3\tk2_3\t3\n" +
                            "1970-01-01T00:00:40.000000Z\tk4\tk2_4\t4\n" +
                            "1970-01-01T00:00:50.000000Z\tk5\tk2_5\t5\n" +
                            "1970-01-01T00:01:00.000000Z\tk6\tk2_6\t6\n" +
                            "1970-01-01T00:01:10.000000Z\tk7\tk2_7\t7\n" +
                            "1970-01-01T00:01:20.000000Z\tk8\tk2_8\t8\n",
                    TABLE1,
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    "ts\tk\tk2\tv\n" +
                            "1970-01-01T00:00:00.000000Z\tk0\tk2_0\t0\n" +
                            "1970-01-01T00:00:10.000000Z\tk1\tk2_1\t1\n" +
                            "1970-01-01T00:00:20.000000Z\tk2\tk2_2\t2\n" +
                            "1970-01-01T00:00:30.000000Z\tk3\tk2_3\t3\n" +
                            "1970-01-01T00:00:40.000000Z\tk4\tk2_4\t4\n" +
                            "1970-01-01T00:00:50.000000Z\tk5\tk2_5\t5\n" +
                            "1970-01-01T00:01:00.000000Z\tk6\tk2_6\t6\n" +
                            "1970-01-01T00:01:10.000000Z\tk7\tk2_7\t7\n" +
                            "1970-01-01T00:01:20.000000Z\tk8\tk2_8\t8\n",
                    TABLE2,
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    "ts\tk\tv_max\n" +
                            "1970-01-01T00:00:50.000000Z\tk5\t5\n" +
                            "1970-01-01T00:01:00.000000Z\tk6\t6\n" +
                            "1970-01-01T00:01:10.000000Z\tk7\t7\n" +
                            "1970-01-01T00:01:20.000000Z\tk8\t8\n",
                    VIEW1
            );

            assertQueryNoLeakCheck(
                    "ts\tv_max\n" +
                            "1970-01-01T00:01:10.000000Z\t7\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n",
                    "select ts, v_max from " + VIEW2
            );

            assertQueryNoLeakCheck(
                    "ts\tv_max\n" +
                            "1970-01-01T00:00:50.000000Z\t5\n" +
                            "1970-01-01T00:01:00.000000Z\t6\n" +
                            "1970-01-01T00:01:10.000000Z\t7\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n",
                    "select v1.ts, v1.v_max from " + VIEW1 + " v1"
            );

            assertQueryNoLeakCheck(
                    "ts\tv_max\n" +
                            "1970-01-01T00:00:50.000000Z\t5\n" +
                            "1970-01-01T00:01:00.000000Z\t6\n" +
                            "1970-01-01T00:01:10.000000Z\t7\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n",
                    "select v1.ts, v_max from " + VIEW1 + " v1 join " + TABLE2 + " t2 on t2.v = v1.v_max",
                    null,
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    "ts\tv_max\n" +
                            "1970-01-01T00:01:10.000000Z\t7\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n",
                    "select t1.ts, v_max from " + TABLE1 + " t1 join (" + VIEW1 + " where v_max > 6) t2 on t1.v = t2.v_max",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    "ts\tv_max\n" +
                            "1970-01-01T00:01:10.000000Z\t7\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n",
                    "with t2 as (" + VIEW1 + " where v_max > 6) select t1.ts, v_max from " + TABLE1 + " t1 join t2 on t1.v = t2.v_max", "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    "ts\tv_max\n" +
                            "1970-01-01T00:00:50.000000Z\t5\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n",
                    "with t2 as (" + VIEW2 + " where v_max > 7 union " + VIEW1 + " where k = 'k5') select t1.ts, v_max from " + TABLE1 + " t1 join t2 on t1.v = t2.v_max",
                    "ts",
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    "ts\tk\tv_max\tts1\tk1\tv_max1\n" +
                            "1970-01-01T00:00:50.000000Z\tk5\t5\t1970-01-01T00:00:50.000000Z\tk5\t5\n" +
                            "1970-01-01T00:01:00.000000Z\tk6\t6\t1970-01-01T00:01:00.000000Z\tk6\t6\n",
                    VIEW1 + " v11 join " + VIEW1 + " v12 on v_max where v12.v_max < 7",
                    null,
                    false,
                    true
            );

            assertQueryNoLeakCheck(
                    "ts\tv_max\n" +
                            "1970-01-01T00:01:10.000000Z\t7\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n",
                    "with t2 as (" + VIEW1 + " v11 join " + VIEW1 + " v12 on v_max where v12.v_max > 6) select t1.ts, v_max from " + TABLE1 + " t1 join t2 on t1.v = t2.v_max",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testDropView() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);
            drainWalQueue();

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ");");
            drainWalQueue();
            assertViewDefinition(VIEW1, query1);
            assertViewDefinitionFile(VIEW1, query1);
            assertViewStateFile(VIEW1);

            final String query2 = "select ts, k2, max(v) as v_max from " + TABLE2 + " where v > 6";
            execute("CREATE VIEW " + VIEW2 + " AS (" + query2 + ");");
            drainWalQueue();
            assertViewDefinition(VIEW2, query2);
            assertViewDefinitionFile(VIEW2, query2);
            assertViewStateFile(VIEW1);

            assertQueryNoLeakCheck(
                    "ts\tk\tv_max\n" +
                            "1970-01-01T00:00:50.000000Z\tk5\t5\n" +
                            "1970-01-01T00:01:00.000000Z\tk6\t6\n" +
                            "1970-01-01T00:01:10.000000Z\tk7\t7\n" +
                            "1970-01-01T00:01:20.000000Z\tk8\t8\n",
                    VIEW1
            );

            assertQueryNoLeakCheck(
                    "ts\tv_max\n" +
                            "1970-01-01T00:01:10.000000Z\t7\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n",
                    "select ts, v_max from " + VIEW2
            );

            execute("DROP VIEW " + VIEW1);
            drainWalQueue();
            assertNull(getViewDefinition(VIEW1));

            execute("DROP VIEW IF EXISTS " + VIEW1);
            drainWalQueue();
            assertNull(getViewDefinition(VIEW1));

            execute("DROP VIEW IF EXISTS " + VIEW2);
            drainWalQueue();
            assertNull(getViewDefinition(VIEW2));
        });
    }

    private static void assertViewDefinition(String name, String query) {
        final ViewDefinition viewDefinition = getViewDefinition(name);
        assertNotNull(viewDefinition);
        assertTrue(viewDefinition.getViewToken().isView());
        assertEquals(query, viewDefinition.getViewSql());
    }

    private static void assertViewDefinitionFile(String name, String query) {
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

    private static void assertViewStateFile(String name) {
        final TableToken viewToken = engine.getTableTokenIfExists(name);
        try (
                BlockFileReader reader = new BlockFileReader(configuration);
                Path path = new Path()
        ) {
            reader.of(path.of(configuration.getDbRoot()).concat(viewToken).concat(ViewState.VIEW_STATE_FILE_NAME).$());

            final ViewStateReader viewStateReader = new ViewStateReader();
            viewStateReader.of(reader, viewToken);

            assertFalse(viewStateReader.isInvalid());
            assertNull(viewStateReader.getInvalidationReason());
        }
    }

    private static ViewDefinition getViewDefinition(String viewName) {
        final TableToken viewToken = engine.getTableTokenIfExists(viewName);
        if (viewToken == null) {
            return null;
        }
        return engine.getViewGraph().getViewDefinition(viewToken);
    }

    private void createTable(String tableName) throws SqlException {
        execute(
                "create table if not exists " + tableName +
                        " (ts timestamp, k symbol capacity 2048, k2 symbol capacity 512, v long)" +
                        " timestamp(ts) partition by day wal"
        );
        for (int i = 0; i < 9; i++) {
            execute("insert into " + tableName + " values (" + (i * 10000000) + ", 'k" + i + "', " + "'k2_" + i + "', " + i + ")");
        }
    }
}
