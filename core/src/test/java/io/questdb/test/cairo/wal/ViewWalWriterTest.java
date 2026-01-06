/*******************************************************************************
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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.ViewWalWriter;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class ViewWalWriterTest extends AbstractCairoTest {

    @Test
    public void testBasicViewDefinitionWrite() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_table (id int, name string)");
            execute("create view test_view as select * from base_table");

            TableToken viewToken = engine.verifyTableName("test_view");

            String viewSql = "select * from base_table where id > 10";
            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();
            LowerCaseCharSequenceHashSet columns = new LowerCaseCharSequenceHashSet();
            columns.add("id");
            columns.add("name");
            dependencies.put("base_table", columns);

            int walId;
            try (ViewWalWriter writer = engine.getViewWalWriter(viewToken)) {
                walId = writer.getWalId();
                long seqTxn = writer.replaceViewDefinition(viewSql, dependencies);
                assertTrue(seqTxn >= 0);
                writer.commit();
            }

            // Read back the view definition from WAL using WalEventReader
            try (Path path = new Path();
                 WalEventReader walEventReader = new WalEventReader(configuration)) {
                path.of(configuration.getDbRoot())
                        .concat(viewToken)
                        .concat("wal").put(walId)
                        .slash().put(0); // segment 0

                WalEventCursor eventCursor = walEventReader.of(path, 0);
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.VIEW_DEFINITION, eventCursor.getType());

                WalEventCursor.ViewDefinitionInfo viewDefInfo = eventCursor.getViewDefinitionInfo();
                assertNotNull(viewDefInfo);
                assertEquals(viewSql, viewDefInfo.getViewSql());

                LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> readDependencies = viewDefInfo.getViewDependencies();
                assertNotNull(readDependencies);
                assertEquals(1, readDependencies.size());
                assertTrue(readDependencies.contains("base_table"));

                LowerCaseCharSequenceHashSet readColumns = readDependencies.get("base_table");
                assertNotNull(readColumns);
                assertEquals(2, readColumns.size());
                assertTrue(readColumns.contains("id"));
                assertTrue(readColumns.contains("name"));
            }
        });
    }

    @Test
    public void testDoubleClose() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_table (id int, name string)");
            execute("create view test_view as select * from base_table");

            TableToken viewToken = engine.verifyTableName("test_view");

            ViewWalWriter writer = engine.getViewWalWriter(viewToken);
            writer.close();
            writer.close(); // Second close should be safe
        });
    }

    @Test
    public void testEmptyDependencyColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_table (id int, name string)");
            execute("create view test_view as select * from base_table");

            TableToken viewToken = engine.verifyTableName("test_view");

            String viewSql = "select count(*) from base_table";
            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();
            LowerCaseCharSequenceHashSet columns = new LowerCaseCharSequenceHashSet();
            // Empty columns set
            dependencies.put("base_table", columns);

            try (ViewWalWriter writer = engine.getViewWalWriter(viewToken)) {
                long seqTxn = writer.replaceViewDefinition(viewSql, dependencies);
                assertTrue(seqTxn >= 0);
                writer.commit();
            }
        });
    }

    @Test
    public void testGetTableToken() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_table (id int, name string)");
            execute("create view test_view as select * from base_table");

            TableToken viewToken = engine.verifyTableName("test_view");

            try (ViewWalWriter writer = engine.getViewWalWriter(viewToken)) {
                TableToken retrievedToken = writer.getTableToken();
                assertNotNull(retrievedToken);
                assertEquals(viewToken.getTableName(), retrievedToken.getTableName());
                assertTrue(retrievedToken.isView());
            }
        });
    }

    @Test
    public void testGetWalId() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_table (id int, name string)");
            execute("create view test_view as select * from base_table");

            TableToken viewToken = engine.verifyTableName("test_view");

            try (ViewWalWriter writer = engine.getViewWalWriter(viewToken)) {
                int walId = writer.getWalId();
                assertTrue(walId >= 0);

                String walName = writer.getWalName();
                assertNotNull(walName);
                assertTrue(walName.contains(String.valueOf(walId)));
            }
        });
    }

    @Test
    public void testMultipleCommitsAndRollbacks() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_table (id int, name string)");
            execute("create view test_view as select * from base_table");

            TableToken viewToken = engine.verifyTableName("test_view");

            String viewSql1 = "select * from base_table where id > 10";
            String viewSql2 = "select * from base_table where id < 100";
            String viewSql3 = "select * from base_table where id = 50";

            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();
            LowerCaseCharSequenceHashSet columns = new LowerCaseCharSequenceHashSet();
            columns.add("id");
            dependencies.put("base_table", columns);

            try (ViewWalWriter writer = engine.getViewWalWriter(viewToken)) {
                // First commit
                writer.replaceViewDefinition(viewSql1, dependencies);
                writer.commit();

                // Rollback second
                writer.replaceViewDefinition(viewSql2, dependencies);
                writer.rollback();

                // Third commit
                long seqTxn = writer.replaceViewDefinition(viewSql3, dependencies);
                writer.commit();

                assertTrue(seqTxn >= 0);
            }
        });
    }

    @Test
    public void testMultipleViewDefinitionUpdates() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_table (id int, name string)");
            execute("create view test_view as select * from base_table");

            TableToken viewToken = engine.verifyTableName("test_view");

            String viewSql1 = "select * from base_table where id > 10";
            String viewSql2 = "select * from base_table where id < 100";

            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();
            LowerCaseCharSequenceHashSet columns = new LowerCaseCharSequenceHashSet();
            columns.add("id");
            columns.add("name");
            dependencies.put("base_table", columns);

            try (ViewWalWriter writer = engine.getViewWalWriter(viewToken)) {
                long seqTxn1 = writer.replaceViewDefinition(viewSql1, dependencies);
                writer.commit();

                long seqTxn2 = writer.replaceViewDefinition(viewSql2, dependencies);
                writer.commit();

                assertTrue(seqTxn2 > seqTxn1);
            }
        });
    }

    @Test
    public void testOverlappingViewWalWriters() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_table (id int, name string)");
            execute("create view view1 as select * from base_table");

            TableToken viewToken = engine.verifyTableName("view1");

            String viewSql = "select * from base_table where id > 10";
            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();
            LowerCaseCharSequenceHashSet columns = new LowerCaseCharSequenceHashSet();
            columns.add("id");
            columns.add("name");
            dependencies.put("base_table", columns);

            try (ViewWalWriter writer1 = engine.getViewWalWriter(viewToken);
                 ViewWalWriter writer2 = engine.getViewWalWriter(viewToken)) {

                long seqTxn1 = writer1.replaceViewDefinition(viewSql, dependencies);
                long seqTxn2 = writer2.replaceViewDefinition(viewSql, dependencies);

                writer1.commit();
                writer2.commit();

                assertEquals(1, seqTxn1);
                assertEquals(2, seqTxn2);
            }
        });
    }

    @Test
    public void testReadViewWalAfterWrite() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_table (id int, name string)");
            execute("create view test_view as select * from base_table");

            TableToken viewToken = engine.verifyTableName("test_view");

            String viewSql = "select * from base_table where id > 10";
            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();
            LowerCaseCharSequenceHashSet columns = new LowerCaseCharSequenceHashSet();
            columns.add("id");
            columns.add("name");
            dependencies.put("base_table", columns);

            int walId;
            try (ViewWalWriter writer = engine.getViewWalWriter(viewToken)) {
                walId = writer.getWalId();
                writer.replaceViewDefinition(viewSql, dependencies);
                writer.commit();
            }

            // Read back the WAL using WalEventReader
            try (Path path = new Path();
                 WalEventReader walEventReader = new WalEventReader(configuration)) {
                path.of(configuration.getDbRoot())
                        .concat(viewToken)
                        .concat("wal").put(walId)
                        .slash().put(0); // segment 0

                WalEventCursor eventCursor = walEventReader.of(path, 0);
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.VIEW_DEFINITION, eventCursor.getType());

                WalEventCursor.ViewDefinitionInfo viewDefInfo = eventCursor.getViewDefinitionInfo();
                assertNotNull(viewDefInfo);
                assertEquals(viewSql, viewDefInfo.getViewSql());

                LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> readDependencies = viewDefInfo.getViewDependencies();
                assertNotNull(readDependencies);
                LowerCaseCharSequenceHashSet readColumns = readDependencies.get("base_table");
                assertNotNull(readColumns);
                assertTrue(readColumns.contains("id"));
                assertTrue(readColumns.contains("name"));
            }
        });
    }

    @Test
    public void testSegmentRolloverOnSizeThreshold() throws Exception {
        // small segment size to force rollover
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_SIZE, 1024);

        assertMemoryLeak(() -> {
            execute("create table base_table (id int, name string)");
            execute("create view test_view as select * from base_table");

            TableToken viewToken = engine.verifyTableName("test_view");

            // Create a large view SQL to exceed segment size
            StringBuilder largeViewSql = new StringBuilder("select * from base_table where id in (");
            for (int i = 0; i < 1000; i++) {
                if (i > 0) largeViewSql.append(", ");
                largeViewSql.append(i);
            }
            largeViewSql.append(")");

            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();
            LowerCaseCharSequenceHashSet columns = new LowerCaseCharSequenceHashSet();
            columns.add("id");
            columns.add("name");
            dependencies.put("base_table", columns);

            int walId;
            try (ViewWalWriter writer = engine.getViewWalWriter(viewToken)) {
                walId = writer.getWalId();
                // Write multiple large view definitions to trigger rollover
                for (int i = 0; i < 3; i++) {
                    writer.replaceViewDefinition(largeViewSql.toString(), dependencies);
                    writer.commit();
                }
            }

            // Verify that multiple segments were created due to rollover
            assertSegmentExistence(true, viewToken, walId, 0);
            assertSegmentExistence(true, viewToken, walId, 1);
            // At least 2 segments should exist, possibly more depending on exact sizes
            // Segment 2 might or might not exist depending on how the data was split
        });
    }

    @Test
    public void testSequentialViewDefinitionReplacements() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_table (id int, name string, value double)");
            execute("create view test_view as select * from base_table");

            TableToken viewToken = engine.verifyTableName("test_view");

            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();
            LowerCaseCharSequenceHashSet columns = new LowerCaseCharSequenceHashSet();
            columns.add("id");
            columns.add("name");
            columns.add("value");
            dependencies.put("base_table", columns);

            int walId;
            try (ViewWalWriter writer = engine.getViewWalWriter(viewToken)) {
                walId = writer.getWalId();
                for (int i = 0; i < 10; i++) {
                    String viewSql = "select * from base_table where id > " + i;
                    long seqTxn = writer.replaceViewDefinition(viewSql, dependencies);
                    writer.commit();

                    assertEquals(i + 1, seqTxn);
                }
            }

            // Read back all 10 view definitions from WAL using WalEventReader
            try (Path path = new Path();
                 WalEventReader walEventReader = new WalEventReader(configuration)) {
                path.of(configuration.getDbRoot())
                        .concat(viewToken)
                        .concat("wal").put(walId)
                        .slash().put(0); // segment 0

                // Read all 10 events from the segment
                for (int i = 0; i < 10; i++) {
                    WalEventCursor eventCursor = walEventReader.of(path, i);

                    assertEquals(i, eventCursor.getTxn());
                    assertEquals(WalTxnType.VIEW_DEFINITION, eventCursor.getType());

                    WalEventCursor.ViewDefinitionInfo viewDefInfo = eventCursor.getViewDefinitionInfo();
                    assertNotNull(viewDefInfo);

                    String expectedViewSql = "select * from base_table where id > " + i;
                    assertEquals(expectedViewSql, viewDefInfo.getViewSql());

                    LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> readDependencies = viewDefInfo.getViewDependencies();
                    assertNotNull(readDependencies);
                    assertEquals(1, readDependencies.size());
                    assertTrue(readDependencies.contains("base_table"));

                    LowerCaseCharSequenceHashSet readColumns = readDependencies.get("base_table");
                    assertNotNull(readColumns);
                    assertEquals(3, readColumns.size());
                    assertTrue(readColumns.contains("id"));
                    assertTrue(readColumns.contains("name"));
                    assertTrue(readColumns.contains("value"));
                }
            }
        });
    }

    @Test
    public void testViewDefinitionRollback() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_table (id int, name string)");
            execute("create view test_view as select * from base_table");

            TableToken viewToken = engine.verifyTableName("test_view");

            String viewSql = "select * from base_table where id > 10";
            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();
            LowerCaseCharSequenceHashSet columns = new LowerCaseCharSequenceHashSet();
            columns.add("id");
            dependencies.put("base_table", columns);

            int walId;
            try (ViewWalWriter writer = engine.getViewWalWriter(viewToken)) {
                walId = writer.getWalId();
                writer.replaceViewDefinition(viewSql, dependencies);
                writer.rollback();

                // After rollback, we should be able to write again
                long seqTxn = writer.replaceViewDefinition(viewSql, dependencies);
                assertTrue(seqTxn >= 0);
                writer.commit();
            }

            // Read back the WAL to verify only the committed data exists
            try (Path path = new Path();
                 WalEventReader walEventReader = new WalEventReader(configuration)) {

                try {
                    path.of(configuration.getDbRoot())
                            .concat(viewToken)
                            .concat("wal").put(walId)
                            .slash().put(0); // segment 0

                    // Only one event should exist - the one committed after rollback
                    walEventReader.of(path, 0);
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getMessage(), "_event.i does not have txn with id 0");
                }

                path.of(configuration.getDbRoot())
                        .concat(viewToken)
                        .concat("wal").put(walId)
                        .slash().put(1); // segment 1

                // Only one event should exist - the one committed after rollback
                WalEventCursor eventCursor = walEventReader.of(path, 0);
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.VIEW_DEFINITION, eventCursor.getType());

                WalEventCursor.ViewDefinitionInfo viewDefInfo = eventCursor.getViewDefinitionInfo();
                assertNotNull(viewDefInfo);
                assertEquals(viewSql, viewDefInfo.getViewSql());

                LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> readDependencies = viewDefInfo.getViewDependencies();
                assertNotNull(readDependencies);
                assertEquals(1, readDependencies.size());
                assertTrue(readDependencies.contains("base_table"));

                LowerCaseCharSequenceHashSet readColumns = readDependencies.get("base_table");
                assertNotNull(readColumns);
                assertEquals(1, readColumns.size());
                assertTrue(readColumns.contains("id"));

                // Verify no additional events exist (rolled back event should not be persisted)
                // Try reading transaction 1 - should fail since only txn 0 was committed
                try {
                    walEventReader.of(path, 1);
                    fail("Should not be able to read transaction 1 after rollback");
                } catch (Exception e) {
                    // Expected - transaction 1 was rolled back
                }
            }
        });
    }

    @Test
    public void testViewDefinitionWithNoDependencies() throws Exception {
        assertMemoryLeak(() -> {
            execute("create view test_view as select 1 as x");

            TableToken viewToken = engine.verifyTableName("test_view");

            String viewSql = "select 42 as answer";
            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();

            int walId;
            try (ViewWalWriter writer = engine.getViewWalWriter(viewToken)) {
                walId = writer.getWalId();
                long seqTxn = writer.replaceViewDefinition(viewSql, dependencies);
                assertTrue(seqTxn >= 0);
                writer.commit();
            }

            // Read back the WAL to verify view with no dependencies using WalEventReader
            try (Path path = new Path();
                 WalEventReader walEventReader = new WalEventReader(configuration)) {
                path.of(configuration.getDbRoot())
                        .concat(viewToken)
                        .concat("wal").put(walId)
                        .slash().put(0); // segment 0

                WalEventCursor eventCursor = walEventReader.of(path, 0);
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.VIEW_DEFINITION, eventCursor.getType());

                WalEventCursor.ViewDefinitionInfo viewDefInfo = eventCursor.getViewDefinitionInfo();
                assertNotNull(viewDefInfo);
                assertEquals(viewSql, viewDefInfo.getViewSql());

                // Verify that dependencies map is empty for views with no table dependencies
                LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> readDependencies = viewDefInfo.getViewDependencies();
                assertNotNull(readDependencies);
                assertEquals(0, readDependencies.size());
            }
        });
    }

    @Test
    public void testViewWalWritersForDifferentViews() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_table (id int, name string)");
            execute("create view view1 as select * from base_table");
            execute("create view view2 as select * from base_table");

            TableToken viewToken1 = engine.verifyTableName("view1");
            TableToken viewToken2 = engine.verifyTableName("view2");

            String viewSql = "select * from base_table where id > 10";
            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();
            LowerCaseCharSequenceHashSet columns = new LowerCaseCharSequenceHashSet();
            columns.add("id");
            columns.add("name");
            dependencies.put("base_table", columns);

            try (ViewWalWriter writer1 = engine.getViewWalWriter(viewToken1);
                 ViewWalWriter writer2 = engine.getViewWalWriter(viewToken2)) {

                long seqTxn1 = writer1.replaceViewDefinition(viewSql, dependencies);
                long seqTxn2 = writer2.replaceViewDefinition(viewSql, dependencies);

                writer1.commit();
                writer2.commit();

                assertEquals(1, seqTxn1);
                assertEquals(1, seqTxn2);
            }
        });
    }
}
