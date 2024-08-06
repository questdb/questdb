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

import io.questdb.cairo.*;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class AlterTableConvertPartitionTest extends AbstractCairoTest {

    @Test
    public void testConvertAllPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTable(tableName, "insert into " + tableName + " values(1, '2024-06-10T00:00:00.000000Z')", "insert into " + tableName + " values(2, '2024-06-11T00:00:00.000000Z')", "insert into " + tableName + " values(3, '2024-06-12T00:00:00.000000Z')", "insert into " + tableName + " values(4, '2024-06-12T00:00:01.000000Z')", "insert into " + tableName + " values(5, '2024-06-15T00:00:00.000000Z')", "insert into " + tableName + " values(6, '2024-06-12T00:00:02.000000Z')");

            ddl("alter table " + tableName + " convert partition to parquet where timestamp > 0");

            assertPartitionExists(tableName, "2024-06-10");
            assertPartitionExists(tableName, "2024-06-11.0");
            assertPartitionExists(tableName, "2024-06-12.1");
            assertPartitionExists(tableName, "2024-06-15.3");
        });
    }

    @Test
    public void testConvertLastPartition() throws Exception {
        final long rows = 10;
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            ddl("create table x as (select" + " x id," + " rnd_boolean() a_boolean," + " rnd_byte() a_byte," + " timestamp_sequence('2024-06', 500) designated_ts" + " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month");

            ddl("alter table x convert partition to parquet list '2024-06'");
            assertPartitionExists("x", "2024-06");

            // RO partition, should be ignored
            insert("insert into x(designated_ts) values('2024-06-20')");

            insert("insert into x(designated_ts) values('1970-01')");
            ddl("alter table x convert partition to parquet list '1970-01'");
            assertPartitionExists("x", "1970-01.1");
        });
    }

    @Test
    public void testConvertListPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTable(tableName, "insert into " + tableName + " values(1, '2024-06-10T00:00:00.000000Z')", "insert into " + tableName + " values(2, '2024-06-11T00:00:00.000000Z')", "insert into " + tableName + " values(3, '2024-06-12T00:00:00.000000Z')", "insert into " + tableName + " values(4, '2024-06-12T00:00:01.000000Z')", "insert into " + tableName + " values(5, '2024-06-15T00:00:00.000000Z')", "insert into " + tableName + " values(6, '2024-06-12T00:00:02.000000Z')");

            ddl("alter table " + tableName + " convert partition to parquet list '2024-06-10', '2024-06-11', '2024-06-12'");

            assertPartitionExists(tableName, "2024-06-10");
            assertPartitionExists(tableName, "2024-06-11.0");
            assertPartitionExists(tableName, "2024-06-12.1");
            assertPartitionDoesntExists(tableName, "2024-06-15.3");
        });
    }

    @Test
    public void testConvertListZeroSizeVarcharData() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            ddl("create table x as (select" + " case when x % 2 = 0 then rnd_varchar(1, 40, 1) end as a_varchar," + " to_timestamp('2024-07', 'yyyy-MM') as a_ts," + " from long_sequence(1)) timestamp (a_ts) partition by MONTH");

            ddl("alter table x convert partition to parquet where a_ts > 0");
            assertPartitionExists("x", "2024-07");
        });
    }

    @Test
    public void testConvertPartitionAllTypes() throws Exception {
        final long rows = 1000;
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            ddl("create table x as (select" + " x id," + " rnd_boolean() a_boolean," + " rnd_byte() a_byte," + " rnd_short() a_short," + " rnd_char() a_char," + " rnd_int() an_int," + " rnd_long() a_long," + " rnd_float() a_float," + " rnd_double() a_double," + " rnd_symbol('a','b','c') a_symbol," + " rnd_geohash(4) a_geo_byte," + " rnd_geohash(8) a_geo_short," + " rnd_geohash(16) a_geo_int," + " rnd_geohash(32) a_geo_long," + " rnd_str('hello', 'world', '!') a_string," + " rnd_bin() a_bin," + " rnd_varchar('ганьба','слава','добрий','вечір') a_varchar," + " rnd_ipv4() a_ip," + " rnd_uuid4() a_uuid," + " rnd_long256() a_long256," + " to_long128(rnd_long(), rnd_long()) a_long128," + " cast(timestamp_sequence(600000000000, 700) as date) a_date," + " timestamp_sequence(500000000000, 600) a_ts," + " timestamp_sequence(400000000000, 500) designated_ts" + " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month");

            assertException("alter table x convert partition to parquet list '2024-06'", 0, "cannot convert partition to parquet, partition does not exist");

            ddl("alter table x convert partition to parquet list '1970-01'");
            assertPartitionExists("x", "1970-01");
        });
    }

    @Test
    public void testConvertPartitionBrokenSymbols() throws Exception {
        final long rows = 10;
        final String tableName = "x";
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            ddl("create table " + tableName + " as (select" + " x id," + " rnd_symbol('a','b','c') a_symbol," + " timestamp_sequence(400000000000, 500) designated_ts" + " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month");

            engine.releaseInactive();
            try (Path path = new Path().of(configuration.getRoot())) {
                TableToken tableToken = engine.getTableTokenIfExists(tableName);
                path.concat(tableToken.getDirName()).concat("a_symbol").put(".o");
                FilesFacade ff = configuration.getFilesFacade();
                Assert.assertTrue(ff.exists(path.$()));
                int fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(configuration.getFilesFacade().truncate(fd, SymbolMapWriter.HEADER_SIZE - 2));
                ff.close(fd);
            }
            try {
                ddl("alter table " + tableName + " convert partition to parquet list '1970-01'");
                Assert.fail();
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), " SymbolMap is too short");
            }
            assertPartitionDoesntExists(tableName, "1970-01");
        });
    }

    @Test
    public void testConvertPartitionSymbolMapDoesntExist() throws Exception {
        final long rows = 10;
        final String tableName = "x";
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            ddl("create table " + tableName + " as (select" + " x id," + " rnd_symbol('a','b','c') a_symbol," + " timestamp_sequence(400000000000, 500) designated_ts" + " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month");

            engine.releaseInactive();
            try (Path path = new Path().of(configuration.getRoot())) {
                TableToken tableToken = engine.getTableTokenIfExists(tableName);
                path.concat(tableToken.getDirName()).concat("a_symbol").put(".o");
                FilesFacade ff = configuration.getFilesFacade();
                ff.remove(path.$());
            }
            try {
                ddl("alter table " + tableName + " convert partition to parquet list '1970-01'");
                Assert.fail();
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), "SymbolMap does not exist");
            }
            assertPartitionDoesntExists(tableName, "1970-01");
        });
    }

    @Test
    public void testConvertPartitionsWithColTops() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTable(tableName, "insert into " + tableName + " values(1, '2024-06-10T00:00:00.000000Z')", "insert into " + tableName + " values(2, '2024-06-11T00:00:00.000000Z')", "insert into " + tableName + " values(3, '2024-06-12T00:00:00.000000Z')", "insert into " + tableName + " values(4, '2024-06-12T00:00:01.000000Z')", "insert into " + tableName + " values(5, '2024-06-15T00:00:00.000000Z')", "insert into " + tableName + " values(6, '2024-06-12T00:00:02.000000Z')");

            ddl("alter table " + tableName + " add column a int");
            insert("insert into " + tableName + " values(7, '2024-06-10T00:00:00.000000Z', 1)");

            ddl("alter table " + tableName + " convert partition to parquet where timestamp > 0 and timestamp < '2024-06-15'");

            assertPartitionExists(tableName, "2024-06-10");
            assertPartitionExists(tableName, "2024-06-11.0");
            assertPartitionExists(tableName, "2024-06-12.1");
        });
    }

    @Test
    public void testConvertTimestampPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTable(tableName, "insert into " + tableName + " values(1, '2024-06-10T00:00:00.000000Z')",
                    "insert into " + tableName + " values(2, '2024-06-11T00:00:00.000000Z')",
                    "insert into " + tableName + " values(3, '2024-06-12T00:00:00.000000Z')",
                    "insert into " + tableName + " values(4, '2024-06-12T00:00:01.000000Z')",
                    "insert into " + tableName + " values(5, '2024-06-15T00:00:00.000000Z')",
                    "insert into " + tableName + " values(6, '2024-06-12T00:00:02.000000Z')");

            assertQuery("index\tname\treadOnly\tisParquet\tparquetFileSize\n"
                    + "0\t2024-06-10\tfalse\tfalse\t-1\n"
                    + "1\t2024-06-11\tfalse\tfalse\t-1\n"
                    + "2\t2024-06-12\tfalse\tfalse\t-1\n"
                    + "3\t2024-06-15\tfalse\tfalse\t-1\n", "select index, name, readOnly, isParquet, parquetFileSize from table_partitions('" + tableName + "')", false, true
            );

            ddl("alter table " + tableName + " convert partition to parquet where timestamp = to_timestamp('2024-06-12', 'yyyy-MM-dd')");

            assertQuery("index\tname\treadOnly\tisParquet\tparquetFileSize\tminTimestamp\tmaxTimestamp\n"
                            + "0\t2024-06-10\tfalse\tfalse\t-1\t2024-06-10T00:00:00.000000Z\t2024-06-10T00:00:00.000000Z\n"
                            + "1\t2024-06-11\tfalse\tfalse\t-1\t2024-06-11T00:00:00.000000Z\t2024-06-11T00:00:00.000000Z\n"
                            + "2\t2024-06-12\ttrue\ttrue\t594\t\t\n" +
                            "3\t2024-06-15\tfalse\tfalse\t-1\t2024-06-15T00:00:00.000000Z\t2024-06-15T00:00:00.000000Z\n",
                    "select index, name, readOnly, isParquet, parquetFileSize, minTimestamp, maxTimestamp from table_partitions('" + tableName + "')", false, true);

            assertPartitionDoesntExists(tableName, "2024-06-10");
            assertPartitionDoesntExists(tableName, "2024-06-11.0");
            assertPartitionExists(tableName, "2024-06-12.1");
            assertPartitionDoesntExists(tableName, "2024-06-15.3");
        });
    }

    private void assertPartitionDoesntExists(String tableName, String partition) {
        assertPartitionExists0(tableName, true, partition);
    }

    private void assertPartitionExists(String tableName, String partition) {
        assertPartitionExists0(tableName, false, partition);
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
