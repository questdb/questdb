/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.*;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.ATTACHABLE_DIR_MARKER;
import static io.questdb.cairo.TableUtils.DETACHED_DIR_MARKER;

public class ShowPartitionsTest extends AbstractGriffinTest {

    @After
    public void tearDown() {
        super.tearDown();
        Path.clearThreadLocals();
    }

    @Test
    public void testShowPartitionsAttachablePartitionOfWrongPartitionBy() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        String tabName = testName.getMethodName();
        String tab2Name = tabName + "_fubar";
        assertMemoryLeak(() -> {
            try (
                    TableModel tab = new TableModel(configuration, tabName, PartitionBy.DAY);
                    TableModel tab2 = new TableModel(configuration, tab2Name, PartitionBy.MONTH);
                    Path dstPath = new Path();
                    Path srcPath = new Path()
            ) {
                createPopulateTable(
                        1,
                        tab.timestamp("ts")
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL),
                        10,
                        "2023-03-13",
                        2
                );
                createPopulateTable(
                        1,
                        tab2.timestamp("ts")
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL),
                        20,
                        "2023-03-13",
                        4
                );
                compile("ALTER TABLE " + tab2Name + " DETACH PARTITION LIST '2023-03'");
                engine.releaseAllWriters();
                srcPath.of(configuration.getRoot()).concat(engine.getTableToken(tab2Name)).concat("2023-03").put(DETACHED_DIR_MARKER).$();
                dstPath.of(configuration.getRoot()).concat(engine.getTableToken(tabName)).concat("2023-03").put(ATTACHABLE_DIR_MARKER).$();
                Assert.assertEquals(0, Files.softLink(srcPath, dstPath));
                assertShowPartitions(
                        "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\n" +
                                "0\tDAY\t2023-03-13\t2023-03-13T04:47:59.900000Z\t2023-03-13T23:59:59.500000Z\t5\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\n" +
                                "1\tDAY\t2023-03-14\t2023-03-14T04:47:59.400000Z\t2023-03-14T23:59:59.000000Z\t5\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\n" +
                                "NaN\tDAY\t2023-03.attachable\t\t\t-1\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\n",
                        tabName);
            }
        });
    }

    @Test
    public void testShowPartitionsAttachablePartitionOfWrongTableId() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // symlink required, and not well supported in Windows
        String tabName = testName.getMethodName();
        String tab2Name = tabName + "_fubar";
        assertMemoryLeak(() -> {
            try (
                    TableModel tab = new TableModel(configuration, tabName, PartitionBy.DAY);
                    TableModel tab2 = new TableModel(configuration, tab2Name, PartitionBy.DAY);
                    Path dstPath = new Path();
                    Path srcPath = new Path()
            ) {
                createPopulateTable(
                        1,
                        tab.timestamp("ts")
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL),
                        10,
                        "2023-03-13",
                        2
                );
                createPopulateTable(
                        99,
                        tab2.timestamp("ts")
                                .col("s1", ColumnType.SYMBOL).indexed(true, 32)
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s2", ColumnType.SYMBOL),
                        20,
                        "2023-03-13",
                        4
                );
                compile("ALTER TABLE " + tab2Name + " DETACH PARTITION LIST '2023-03-15'");
                engine.releaseAllWriters();
                srcPath.of(configuration.getRoot()).concat(engine.getTableToken(tab2Name)).concat("2023-03-15").put(DETACHED_DIR_MARKER).$();
                dstPath.of(configuration.getRoot()).concat(engine.getTableToken(tabName)).concat("2023-03-15").put(ATTACHABLE_DIR_MARKER).$();
                Assert.assertEquals(0, Files.softLink(srcPath, dstPath));
                assertShowPartitions(
                        "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\n" +
                                "0\tDAY\t2023-03-13\t2023-03-13T04:47:59.900000Z\t2023-03-13T23:59:59.500000Z\t5\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\n" +
                                "1\tDAY\t2023-03-14\t2023-03-14T04:47:59.400000Z\t2023-03-14T23:59:59.000000Z\t5\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\n" +
                                "NaN\tDAY\t2023-03-15.attachable\t\t\t-1\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\n",
                        tabName);
            }
        });
    }

    @Test
    public void testShowPartitionsDetachedPartitionPlusAttachable() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // no links in windows
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(createTable(tableName), sqlExecutionContext);
            compile("ALTER TABLE " + tableName + " DETACH PARTITION WHERE timestamp < '2023-06-01T00:00:00.000000Z'", sqlExecutionContext);

            // prepare 3 partitions for attachment
            TableToken tableToken = engine.getTableToken(tableName);
            try (
                    Path path = new Path().of(configuration.getRoot()).concat(tableToken).concat("2023-0");
                    Path link = new Path().of(configuration.getRoot()).concat(tableToken).concat("2023-0")
            ) {
                int len = path.length();
                for (int i = 2; i < 5; i++) {
                    path.trimTo(len).put(i).put(TableUtils.DETACHED_DIR_MARKER).$();
                    link.trimTo(len).put(i).put(TableUtils.ATTACHABLE_DIR_MARKER).$();
                    Assert.assertEquals(0, Files.softLink(path, link));
                }
            }

            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\n" +
                            "0\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\n" +
                            "NaN\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-04.detached\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-02.attachable\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\n" +
                            "NaN\tMONTH\t2023-03.attachable\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\n" +
                            "NaN\tMONTH\t2023-04.attachable\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\n",
                    tableName);

            compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2023-02', '2023-03'", sqlExecutionContext);

            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\n" +
                            "0\tMONTH\t2023-02\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\ttrue\tfalse\ttrue\tfalse\tfalse\n" +
                            "1\tMONTH\t2023-03\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\ttrue\tfalse\ttrue\tfalse\tfalse\n" +
                            "2\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\n" +
                            "NaN\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-04.detached\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-04.attachable\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\ttrue\n",
                    tableName);
        });
    }

    @Test
    public void testShowPartitionsOnlyDetachedPartitionMissingMeta() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(createTable(tableName), sqlExecutionContext);
            compile("ALTER TABLE " + tableName + " DETACH PARTITION WHERE timestamp < '2023-06-01T00:00:00.000000Z'", sqlExecutionContext);
            deleteFile(tableName, "2023-04" + TableUtils.DETACHED_DIR_MARKER, TableUtils.META_FILE_NAME);
            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\n" +
                            "0\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\n" +
                            "NaN\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-04.detached\t\t\t-1\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n",
                    tableName);
        });
    }

    @Test
    public void testShowPartitionsOnlyDetachedPartitionMissingTimestampColumn() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(createTable(tableName), sqlExecutionContext);
            compile("ALTER TABLE " + tableName + " DETACH PARTITION WHERE timestamp < '2023-06-01T00:00:00.000000Z'", sqlExecutionContext);
            deleteFile(tableName, "2023-04" + TableUtils.DETACHED_DIR_MARKER, "timestamp.d");
            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\n" +
                            "0\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\n" +
                            "NaN\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-04.detached\t\t\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n",
                    tableName);
        });
    }

    @Test
    public void testShowPartitionsOnlyDetachedPartitionMissingTxn() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(createTable(tableName), sqlExecutionContext);
            compile("ALTER TABLE " + tableName + " DETACH PARTITION WHERE timestamp < '2023-06-01T00:00:00.000000Z'", sqlExecutionContext);
            deleteFile(tableName, "2023-04" + TableUtils.DETACHED_DIR_MARKER, TableUtils.TXN_FILE_NAME);
            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\n" +
                            "0\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\n" +
                            "NaN\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-04.detached\t\t\t-1\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n",
                    tableName);
        });
    }

    @Test
    public void testShowPartitionsOnlyDetachedPartitions() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(createTable(tableName), sqlExecutionContext);
            compile("ALTER TABLE " + tableName + " DETACH PARTITION WHERE timestamp < '2023-06-01T00:00:00.000000Z'", sqlExecutionContext);
            compile("ALTER TABLE " + tableName + " DROP PARTITION LIST '2023-06'", sqlExecutionContext);
            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\n" +
                            "NaN\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-04.detached\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "NaN\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\tfalse\ttrue\tfalse\n",
                    tableName);
        });
    }

    @Test
    public void testShowPartitionsSelectActive() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(createTable(tableName), sqlExecutionContext);
            String finallyExpected = replaceSizeToMatchOs(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\n" +
                            "5\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\n",
                    tableName);
            assertQuery(
                    finallyExpected,
                    "SELECT * FROM table_partitions('" + tableName + "') WHERE active = true;",
                    null,
                    false,
                    false,
                    true);
        });
    }

    @Test
    public void testShowPartitionsSelectActiveMaterializing() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(createTable(tableName), sqlExecutionContext);
            compile("CREATE TABLE partitions AS (SELECT * FROM table_partitions('" + tableName + "'))", sqlExecutionContext);
            String finallyExpected = replaceSizeToMatchOs(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\n" +
                            "5\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\n",
                    tableName);
            assertQuery(
                    finallyExpected,
                    "SELECT * FROM partitions WHERE active = true;",
                    null,
                    true,
                    false,
                    true);
        });
    }

    @Test
    public void testShowPartitionsWhenThereAreNoDetachedNorAttachable() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(createTable(tableName), sqlExecutionContext);
            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\n" +
                            "0\tMONTH\t2023-01\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\n" +
                            "1\tMONTH\t2023-02\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\n" +
                            "2\tMONTH\t2023-03\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\n" +
                            "3\tMONTH\t2023-04\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\n" +
                            "4\tMONTH\t2023-05\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\n" +
                            "5\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\n",
                    tableName
            );
        });
    }

    @Test
    public void testShowPartitionsWhenThereAreNoDetachedNorAttachableMissingTimestampColumn() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(createTable(tableName), sqlExecutionContext);
            deleteFile(tableName, "2023-04", "timestamp.d");
            try {
                assertShowPartitions(
                        "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\n" +
                                "0\tMONTH\t2023-01\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\n" +
                                "1\tMONTH\t2023-02\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\n" +
                                "2\tMONTH\t2023-03\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\n" +
                                "3\tMONTH\t2023-04\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\n" +
                                "4\tMONTH\t2023-05\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\n" +
                                "5\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\n",
                        tableName
                );
                Assert.fail();
            } catch (CairoException err) {
                TestUtils.assertContains(err.getFlyweightMessage(), "no file found for designated timestamp column");
            }
        });
    }

    private static String createTable(String tableName) {
        return "CREATE TABLE " + tableName + " AS (" +
                "    SELECT" +
                "        rnd_symbol('EURO', 'USD', 'OTHER') symbol," +
                "        rnd_double() * 50.0 price," +
                "        rnd_double() * 20.0 amount," +
                "        to_timestamp('2023-01-01', 'yyyy-MM-dd') + x * 6 * 3600 * 1000000L timestamp" +
                "    FROM long_sequence(700)" +
                "), INDEX(symbol capacity 32) TIMESTAMP(timestamp) PARTITION BY MONTH;";
    }

    private static void deleteFile(String tableName, String... pathParts) {
        engine.releaseAllWriters();
        TableToken tableToken = engine.getTableToken(tableName);
        try (Path path = new Path().of(configuration.getRoot()).concat(tableToken)) {
            for (String part : pathParts) {
                path.concat(part);
            }
            path.$();
            Assert.assertTrue(Files.exists(path));
            Assert.assertTrue(Files.remove(path));
            Assert.assertFalse(Files.exists(path));
        }
    }

    private static ObjObjHashMap<String, Long> findPartitionSizes(String tableName) {
        ObjObjHashMap<String, Long> sizes = new ObjObjHashMap<>();
        TableToken tableToken = engine.getTableToken(tableName);
        try (Path path = new Path().of(configuration.getRoot()).concat(tableToken).$()) {
            int len = path.length();
            long pFind = Files.findFirst(path);
            try {
                do {
                    sink.clear();
                    Chars.utf8DecodeZ(Files.findName(pFind), sink);
                    path.trimTo(len).concat(sink).$();
                    int n = sink.length();
                    int limit = n;
                    for (int i = 0; i < n; i++) {
                        if (sink.charAt(i) == '.' && i < n - 1) {
                            char c = sink.charAt(i + 1);
                            if (c >= '0' && c <= '9') {
                                limit = i;
                                break;
                            }
                        }
                    }
                    sizes.put(Chars.toString(sink, 0, limit), Files.getDirectoryContentSize(path));
                } while (Files.findNext(pFind) > 0);
            } finally {
                Files.findClose(pFind);
            }
        }
        return sizes;
    }

    private static String replaceSizeToMatchOs(String expected, String tableName) {
        ObjObjHashMap<String, Long> sizes = findPartitionSizes(tableName);
        String[] lines = expected.split("\n");
        sink.clear();
        sink.put(lines[0]).put('\n');
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i];
            String nameColumn = line.split("\t")[2];
            long size = sizes.get(nameColumn);
            int z = Numbers.msb(size) / 10;
            String human = String.format("%.1f %sB", (float) size / (1L << z * 10), " KMGTPEZ".charAt(z));
            line = line.replaceAll("SIZE", String.valueOf(size));
            line = line.replaceAll("HUMAN", human);
            sink.put(line).put('\n');
        }
        return sink.toString();
    }

    // CHANGE assertQuery for assertCursor

    private void assertShowPartitions(String expected, String tableName) throws SqlException {
        String finallyExpected = replaceSizeToMatchOs(expected, tableName);
        assertQuery(
                finallyExpected,
                "SHOW PARTITIONS FROM " + tableName,
                null,
                false,
                false,
                true);
        assertQuery(
                finallyExpected,
                "SELECT * FROM table_partitions('" + tableName + "')",
                null,
                false,
                false,
                true);
    }
}
