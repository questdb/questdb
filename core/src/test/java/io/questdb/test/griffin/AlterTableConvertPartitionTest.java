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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

public class AlterTableConvertPartitionTest extends AbstractCairoTest {

    @Test
    public void testConvertListZeroSizeVarcharData() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                ddl("create table x as (select" +
                    " case when x % 2 = 0 then rnd_varchar(1, 40, 1) end as a_varchar," +
                    " to_timestamp('2024-07', 'yyyy-MM') as a_ts," +
                    " from long_sequence(1)) timestamp (a_ts) partition by MONTH");

                ddl("alter table x convert partition where a_ts > 0");
                assertPartitionExists("x", "2024-07");
            }
        );
    }

    @Test
    public void testConvertListPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = testName.getMethodName();
                    createTable(tableName,
                            "insert into " + tableName + " values(1, '2024-06-10T00:00:00.000000Z')",
                            "insert into " + tableName + " values(2, '2024-06-11T00:00:00.000000Z')",
                            "insert into " + tableName + " values(3, '2024-06-12T00:00:00.000000Z')",
                            "insert into " + tableName + " values(4, '2024-06-12T00:00:01.000000Z')",
                            "insert into " + tableName + " values(5, '2024-06-15T00:00:00.000000Z')",
                            "insert into " + tableName + " values(6, '2024-06-12T00:00:02.000000Z')");

                    ddl("alter table " + tableName + " convert partition list '2024-06-10', '2024-06-11', '2024-06-12', '2024-06-15'");

                    assertPartitionExists(tableName, "2024-06-10");
                    assertPartitionExists(tableName, "2024-06-11.0");
                    assertPartitionExists(tableName, "2024-06-12.1");
                    assertPartitionExists(tableName, "2024-06-15.3");
                }
        );
    }

    @Test
    public void testConvertTimestampPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = testName.getMethodName();
                    createTable(tableName,
                            "insert into " + tableName + " values(1, '2024-06-10T00:00:00.000000Z')",
                            "insert into " + tableName + " values(2, '2024-06-11T00:00:00.000000Z')",
                            "insert into " + tableName + " values(3, '2024-06-12T00:00:00.000000Z')",
                            "insert into " + tableName + " values(4, '2024-06-12T00:00:01.000000Z')",
                            "insert into " + tableName + " values(5, '2024-06-15T00:00:00.000000Z')",
                            "insert into " + tableName + " values(6, '2024-06-12T00:00:02.000000Z')");

                    assertQuery("index\tname\treadOnly\tisParquet\tparquetFileSize\n" +
                            "0\t2024-06-10\tfalse\tfalse\t-1\n" +
                            "1\t2024-06-11\tfalse\tfalse\t-1\n" +
                            "2\t2024-06-12\tfalse\tfalse\t-1\n" +
                            "3\t2024-06-15\tfalse\tfalse\t-1\n",
                            "select index, name, readOnly, isParquet, parquetFileSize from table_partitions('" + tableName + "')",
                            false, true);
                    ddl("alter table " + tableName + " convert partition where timestamp = to_timestamp('2024-06-12', 'yyyy-MM-dd')");
                    assertQuery("index\tname\treadOnly\tisParquet\tparquetFileSize\n" +
                                    "0\t2024-06-10\tfalse\tfalse\t-1\n" +
                                    "1\t2024-06-11\tfalse\tfalse\t-1\n" +
                                    "2\t2024-06-12\ttrue\ttrue\t554\n" +
                                    "3\t2024-06-15\tfalse\tfalse\t-1\n",
                            "select index, name, readOnly, isParquet, parquetFileSize from table_partitions('" + tableName + "')",
                            false, true);

                    assertPartitionDoesntExists(tableName, "2024-06-10");
                    assertPartitionDoesntExists(tableName, "2024-06-11.0");
                    assertPartitionExists(tableName, "2024-06-12.1");
                    assertPartitionDoesntExists(tableName, "2024-06-15.3");
                }
        );
    }
    @Test
    public void testConvertAllPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                final String tableName = testName.getMethodName();
                createTable(tableName,
                    "insert into " + tableName + " values(1, '2024-06-10T00:00:00.000000Z')",
                    "insert into " + tableName + " values(2, '2024-06-11T00:00:00.000000Z')",
                    "insert into " + tableName + " values(3, '2024-06-12T00:00:00.000000Z')",
                    "insert into " + tableName + " values(4, '2024-06-12T00:00:01.000000Z')",
                    "insert into " + tableName + " values(5, '2024-06-15T00:00:00.000000Z')",
                    "insert into " + tableName + " values(6, '2024-06-12T00:00:02.000000Z')");

                ddl("alter table " + tableName + " convert partition where timestamp > 0");

                assertPartitionExists(tableName, "2024-06-10");
                assertPartitionExists(tableName, "2024-06-11.0");
                assertPartitionExists(tableName, "2024-06-12.1");
                assertPartitionExists(tableName, "2024-06-15.3");
            }
        );
    }


    private void assertPartitionExists(String tableName, String partition) {
       assertPartitionExists0(tableName, false, partition);
    }

    private void assertPartitionDoesntExists(String tableName, String partition) {
        assertPartitionExists0(tableName, true, partition);
    }

    private void assertPartitionExists0(String tableName, boolean rev, String partition) {
        Path path = Path.getThreadLocal(configuration.getRoot());
        path.concat(engine.getTableTokenIfExists(tableName).getDirName());
        int tablePathLen = path.size();

        path.trimTo(tablePathLen);
        path.concat(partition).put(".parquet");
        if (rev) {
            Assert.assertFalse(ff.exists(path.$()));
        } else {
            Assert.assertTrue(ff.exists(path.$()));
        }
    }

    private void createTable(String tableName, String... inserts) throws Exception {
        TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY).col("id", ColumnType.INT).timestamp();
        AbstractCairoTest.create(model);
        for (int i = 0, n = inserts.length; i < n; i++) {
            insert(inserts[i]);
        }
    }
}
