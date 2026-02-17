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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.ViewWalWriter;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.*;

public class ViewWalWriterTest extends AbstractCairoTest {

    @Test
    public void testBasicViewDefinitionUpdate() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_table (id int, name string)");
            execute("create view test_view as select * from base_table");

            TableToken viewToken = engine.verifyTableName("test_view");

            String viewSql = "select * from base_table where id > 10";
            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = getDependencies();

            int walId;
            try (ViewWalWriter writer = engine.getViewWalWriter(viewToken)) {
                walId = writer.getWalId();
                long seqTxn = writer.replaceViewDefinition(viewSql, dependencies);
                assertTrue(seqTxn >= 0);
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

            int walId;
            try (ViewWalWriter writer = engine.getViewWalWriter(viewToken)) {
                walId = writer.getWalId();
                long seqTxn = writer.replaceViewDefinition(viewSql, dependencies);
                assertTrue(seqTxn >= 0);
            }

            // Read back the WAL to verify empty column set is persisted correctly
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

                // Verify that dependency exists but with empty columns
                LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> readDependencies = viewDefInfo.getViewDependencies();
                assertNotNull(readDependencies);
                assertEquals(1, readDependencies.size());
                assertTrue(readDependencies.contains("base_table"));

                LowerCaseCharSequenceHashSet readColumns = readDependencies.get("base_table");
                assertNotNull(readColumns);
                assertEquals(0, readColumns.size());
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
    public void testMultipleViewDefinitionUpdates() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_table (id int, name string)");
            execute("create view test_view as select * from base_table");

            TableToken viewToken = engine.verifyTableName("test_view");

            String viewSql1 = "select * from base_table where id > 10";
            String viewSql2 = "select * from base_table where id < 100";

            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = getDependencies();

            int walId;
            try (ViewWalWriter writer = engine.getViewWalWriter(viewToken)) {
                walId = writer.getWalId();
                long seqTxn1 = writer.replaceViewDefinition(viewSql1, dependencies);
                long seqTxn2 = writer.replaceViewDefinition(viewSql2, dependencies);
                assertEquals(1, seqTxn1);
                assertEquals(2, seqTxn2);
            }

            // Read back both view definitions from WAL
            try (Path path = new Path();
                 WalEventReader walEventReader = new WalEventReader(configuration)) {
                path.of(configuration.getDbRoot())
                        .concat(viewToken)
                        .concat("wal").put(walId)
                        .slash().put(0); // segment 0

                // First view definition
                WalEventCursor eventCursor1 = walEventReader.of(path, 0);
                assertEquals(0, eventCursor1.getTxn());
                assertEquals(WalTxnType.VIEW_DEFINITION, eventCursor1.getType());

                WalEventCursor.ViewDefinitionInfo viewDefInfo1 = eventCursor1.getViewDefinitionInfo();
                assertNotNull(viewDefInfo1);
                assertEquals(viewSql1, viewDefInfo1.getViewSql());

                LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> readDependencies1 = viewDefInfo1.getViewDependencies();
                assertNotNull(readDependencies1);
                assertEquals(1, readDependencies1.size());
                assertTrue(readDependencies1.contains("base_table"));

                LowerCaseCharSequenceHashSet readColumns1 = readDependencies1.get("base_table");
                assertNotNull(readColumns1);
                assertEquals(2, readColumns1.size());
                assertTrue(readColumns1.contains("id"));
                assertTrue(readColumns1.contains("name"));

                // Second view definition
                WalEventCursor eventCursor2 = walEventReader.of(path, 1);
                assertEquals(1, eventCursor2.getTxn());
                assertEquals(WalTxnType.VIEW_DEFINITION, eventCursor2.getType());

                WalEventCursor.ViewDefinitionInfo viewDefInfo2 = eventCursor2.getViewDefinitionInfo();
                assertNotNull(viewDefInfo2);
                assertEquals(viewSql2, viewDefInfo2.getViewSql());

                LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> readDependencies2 = viewDefInfo2.getViewDependencies();
                assertNotNull(readDependencies2);
                assertEquals(1, readDependencies2.size());
                assertTrue(readDependencies2.contains("base_table"));

                LowerCaseCharSequenceHashSet readColumns2 = readDependencies2.get("base_table");
                assertNotNull(readColumns2);
                assertEquals(2, readColumns2.size());
                assertTrue(readColumns2.contains("id"));
                assertTrue(readColumns2.contains("name"));
            }
        });
    }

    @Test
    public void testOverlappingViewWalWriters() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_table (id int, name string)");
            execute("create view view1 as select * from base_table");

            TableToken viewToken = engine.verifyTableName("view1");

            String viewSql1 = "select * from base_table where id > 10";
            String viewSql2 = "select * from base_table where id > 20";
            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = getDependencies();

            int walId1, walId2;
            try (ViewWalWriter writer1 = engine.getViewWalWriter(viewToken);
                 ViewWalWriter writer2 = engine.getViewWalWriter(viewToken)) {

                walId1 = writer1.getWalId();
                walId2 = writer2.getWalId();

                long seqTxn11 = writer1.replaceViewDefinition(viewSql1, dependencies);
                long seqTxn21 = writer2.replaceViewDefinition(viewSql1, dependencies);
                long seqTxn12 = writer1.replaceViewDefinition(viewSql2, dependencies);
                long seqTxn22 = writer2.replaceViewDefinition(viewSql2, dependencies);

                assertEquals(1, seqTxn11);
                assertEquals(2, seqTxn21);
                assertEquals(3, seqTxn12);
                assertEquals(4, seqTxn22);
            }

            // Read back WAL events from writer1
            try (Path path = new Path();
                 WalEventReader walEventReader = new WalEventReader(configuration)) {
                path.of(configuration.getDbRoot())
                        .concat(viewToken)
                        .concat("wal").put(walId1)
                        .slash().put(0); // segment 0

                // Writer1 wrote 2 view definitions
                for (int i = 0; i < 2; i++) {
                    WalEventCursor eventCursor = walEventReader.of(path, i);
                    assertEquals(i, eventCursor.getTxn());
                    assertEquals(WalTxnType.VIEW_DEFINITION, eventCursor.getType());

                    WalEventCursor.ViewDefinitionInfo viewDefInfo = eventCursor.getViewDefinitionInfo();
                    assertNotNull(viewDefInfo);

                    String expectedViewSql = i == 0 ? viewSql1 : viewSql2;
                    assertEquals(expectedViewSql, viewDefInfo.getViewSql());

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
            }

            // Read back WAL events from writer2
            try (Path path = new Path();
                 WalEventReader walEventReader = new WalEventReader(configuration)) {
                path.of(configuration.getDbRoot())
                        .concat(viewToken)
                        .concat("wal").put(walId2)
                        .slash().put(0); // segment 0

                // Writer2 wrote 2 view definitions
                for (int i = 0; i < 2; i++) {
                    WalEventCursor eventCursor = walEventReader.of(path, i);
                    assertEquals(i, eventCursor.getTxn());
                    assertEquals(WalTxnType.VIEW_DEFINITION, eventCursor.getType());

                    WalEventCursor.ViewDefinitionInfo viewDefInfo = eventCursor.getViewDefinitionInfo();
                    assertNotNull(viewDefInfo);

                    String expectedViewSql = i == 0 ? viewSql1 : viewSql2;
                    assertEquals(expectedViewSql, viewDefInfo.getViewSql());

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

            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = getDependencies();

            int walId;
            try (ViewWalWriter writer = engine.getViewWalWriter(viewToken)) {
                walId = writer.getWalId();
                // Write multiple large view definitions to trigger rollover
                for (int i = 0; i < 3; i++) {
                    writer.replaceViewDefinition(largeViewSql.toString(), dependencies);
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
                assertEquals(1, seqTxn);
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

            String viewSql1 = "select * from base_table where id > 10";
            String viewSql2 = "select * from base_table where id > 100";
            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = getDependencies();

            int walId1, walId2;
            try (ViewWalWriter writer1 = engine.getViewWalWriter(viewToken1);
                 ViewWalWriter writer2 = engine.getViewWalWriter(viewToken2)) {

                walId1 = writer1.getWalId();
                walId2 = writer2.getWalId();

                long seqTxn1 = writer1.replaceViewDefinition(viewSql1, dependencies);
                long seqTxn2 = writer2.replaceViewDefinition(viewSql2, dependencies);

                assertEquals(1, seqTxn1);
                assertEquals(1, seqTxn2);
            }

            // Read back WAL from view1's writer
            try (Path path = new Path();
                 WalEventReader walEventReader = new WalEventReader(configuration)) {
                path.of(configuration.getDbRoot())
                        .concat(viewToken1)
                        .concat("wal").put(walId1)
                        .slash().put(0); // segment 0

                WalEventCursor eventCursor = walEventReader.of(path, 0);
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.VIEW_DEFINITION, eventCursor.getType());

                WalEventCursor.ViewDefinitionInfo viewDefInfo = eventCursor.getViewDefinitionInfo();
                assertNotNull(viewDefInfo);
                assertEquals(viewSql1, viewDefInfo.getViewSql());

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

            // Read back WAL from view2's writer
            try (Path path = new Path();
                 WalEventReader walEventReader = new WalEventReader(configuration)) {
                path.of(configuration.getDbRoot())
                        .concat(viewToken2)
                        .concat("wal").put(walId2)
                        .slash().put(0); // segment 0

                WalEventCursor eventCursor = walEventReader.of(path, 0);
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.VIEW_DEFINITION, eventCursor.getType());

                WalEventCursor.ViewDefinitionInfo viewDefInfo = eventCursor.getViewDefinitionInfo();
                assertNotNull(viewDefInfo);
                assertEquals(viewSql2, viewDefInfo.getViewSql());

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

    private static @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> getDependencies() {
        LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();
        LowerCaseCharSequenceHashSet columns = new LowerCaseCharSequenceHashSet();
        columns.add("id");
        columns.add("name");
        dependencies.put("base_table", columns);
        return dependencies;
    }
}
