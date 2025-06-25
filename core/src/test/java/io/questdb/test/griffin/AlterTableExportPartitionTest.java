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
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.PARQUET_PARTITION_NAME;

public class AlterTableExportPartitionTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        inputRoot = TestUtils.getCsvRoot();
    }

    @Test
    public void testExportAllPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTable(
                    tableName,
                    "insert into " + tableName + " values(1, '2024-06-10T00:00:00.000000Z')",
                    "insert into " + tableName + " values(2, '2024-06-11T00:00:00.000000Z')",
                    "insert into " + tableName + " values(3, '2024-06-12T00:00:00.000000Z')",
                    "insert into " + tableName + " values(4, '2024-06-12T00:00:01.000000Z')",
                    "insert into " + tableName + " values(5, '2024-06-15T00:00:00.000000Z')",
                    "insert into " + tableName + " values(6, '2024-06-12T00:00:02.000000Z')"
            );

            execute("alter table " + tableName + " export partition to parquet where timestamp > 0");

            // partitions should appear in import
            assertExportedPartitionExists(tableName, "2024-06-10.6");
            assertExportedPartitionExists(tableName, "2024-06-11.6");
            assertExportedPartitionExists(tableName, "2024-06-12.6");

            // partitions should stay in place too
            assertPartitionDoesNotExist(tableName, "2024-06-10.6");
            assertPartitionDoesNotExist(tableName, "2024-06-11.6");
            assertPartitionDoesNotExist(tableName, "2024-06-12.6");
            // last partition is not converted
        });
    }

    @Test
    public void testExportAllPartitionsToParquetAndBack() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableStr(
                    tableName,
                    "insert into " + tableName + " values(1, 'abc', '2024-06-10T00:00:00.000000Z')",
                    "insert into " + tableName + " values(2, 'edf', '2024-06-11T00:00:00.000000Z')",
                    "insert into " + tableName + " values(3, 'abc', '2024-06-12T00:00:00.000000Z')",
                    "insert into " + tableName + " values(4, 'edf', '2024-06-12T00:00:01.000000Z')",
                    "insert into " + tableName + " values(5, 'abc', '2024-06-15T00:00:00.000000Z')",
                    "insert into " + tableName + " values(6, 'edf', '2024-06-12T00:00:02.000000Z')"
            );

            drainWalQueue();

            execute("alter table " + tableName + " export partition to parquet where timestamp > 0");

            assertPartitionDoesNotExist(tableName, "2024-06-10.6");
            assertPartitionDoesNotExist(tableName, "2024-06-11.6");
            assertPartitionDoesNotExist(tableName, "2024-06-12.6");

            assertExportedPartitionExists(tableName, "2024-06-10.6");
            assertExportedPartitionExists(tableName, "2024-06-11.6");
            assertExportedPartitionExists(tableName, "2024-06-12.6");

            execute("alter table " + tableName + " drop partition where timestamp between '2024-06-10' AND '2024-06-13'");

            execute("insert into " + tableName + " select id, str, timestamp from read_parquet('testExportAllPartitionsToParquetAndBack~/2024-06-10.6/data.parquet') timestamp(timestamp)");
            execute("insert into " + tableName + " select id, str, timestamp from read_parquet('testExportAllPartitionsToParquetAndBack~/2024-06-11.6/data.parquet') timestamp(timestamp)");
            execute("insert into " + tableName + " select id, str, timestamp from read_parquet('testExportAllPartitionsToParquetAndBack~/2024-06-12.6/data.parquet') timestamp(timestamp)");

            assertSql("id\tstr\ttimestamp\n" +
                            "1\tabc\t2024-06-10T00:00:00.000000Z\n" +
                            "2\tedf\t2024-06-11T00:00:00.000000Z\n" +
                            "3\tabc\t2024-06-12T00:00:00.000000Z\n" +
                            "4\tedf\t2024-06-12T00:00:01.000000Z\n" +
                            "6\tedf\t2024-06-12T00:00:02.000000Z\n" +
                            "5\tabc\t2024-06-15T00:00:00.000000Z\n",
                    "select * from " + tableName
            );
        });
    }

    @Test
    public void testExportLastPartition() throws Exception {
        final long rows = 10;
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x as (select" +
                            " x id," +
                            " rnd_boolean() a_boolean," +
                            " rnd_byte() a_byte," +
                            " timestamp_sequence('2024-06', 500) designated_ts" +
                            " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month"
            );

            execute("alter table x export partition to parquet list '2024-06'");
            assertPartitionDoesNotExist("x", "2024-06.1");

            assertExportedPartitionDoesNotExist("x", "2024-06.1");

            execute("insert into x(designated_ts) values('1970-01')");
            execute("alter table x export partition to parquet list '1970-01'");
            assertPartitionDoesNotExist("x", "1970-01.2");

            assertExportedPartitionExists("x", "2024-06.1");
        });
    }

    @Test
    public void testExportListPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTable(
                    tableName,
                    "insert into " + tableName + " values(1, '2024-06-10T00:00:00.000000Z')",
                    "insert into " + tableName + " values(2, '2024-06-11T00:00:00.000000Z')",
                    "insert into " + tableName + " values(3, '2024-06-12T00:00:00.000000Z')",
                    "insert into " + tableName + " values(4, '2024-06-12T00:00:01.000000Z')",
                    "insert into " + tableName + " values(5, '2024-06-15T00:00:00.000000Z')",
                    "insert into " + tableName + " values(6, '2024-06-12T00:00:02.000000Z')"
            );

            execute("alter table " + tableName + " export partition to parquet list '2024-06-10', '2024-06-11', '2024-06-12'");

            assertExportedPartitionExists(tableName, "2024-06-10.6");
            assertExportedPartitionExists(tableName, "2024-06-11.6");
            assertExportedPartitionExists(tableName, "2024-06-12.6");

            assertPartitionDoesNotExist(tableName, "2024-06-10.6");
            assertPartitionDoesNotExist(tableName, "2024-06-11.6");
            assertPartitionDoesNotExist(tableName, "2024-06-12.6");

            assertPartitionDoesNotExist(tableName, "2024-06-15.3");
        });
    }

    @Test
    public void testExportListZeroSizeVarcharData() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x as (select" +
                            " case when x % 2 = 0 then rnd_varchar(1, 40, 1) end as a_varchar," +
                            " to_timestamp('2024-07', 'yyyy-MM') as a_ts," +
                            " from long_sequence(1)) timestamp (a_ts) partition by MONTH"
            );

            execute("insert into x(a_varchar, a_ts) values('', '2024-08')");
            execute("alter table x export partition to parquet where a_ts > 0");
            assertPartitionDoesNotExist("x", "2024-07.2");
            assertExportedPartitionExists("x", "2024-07.2");
        });
    }

    @Test
    public void testExportPartitionAllTypes() throws Exception {
        final long rows = 1000;
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x as (select" +
                            " x id," +
                            " rnd_boolean() a_boolean," +
                            " rnd_byte() a_byte," +
                            " rnd_short() a_short," +
                            " rnd_char() a_char," +
                            " rnd_int() an_int," +
                            " rnd_long() a_long," +
                            " rnd_float() a_float," +
                            " rnd_double() a_double," +
                            " rnd_symbol('a','b','c') a_symbol," +
                            " rnd_geohash(4) a_geo_byte," +
                            " rnd_geohash(8) a_geo_short," +
                            " rnd_geohash(16) a_geo_int," +
                            " rnd_geohash(32) a_geo_long," +
                            " rnd_str('hello', 'world', '!') a_string," +
                            " rnd_bin() a_bin," +
                            " rnd_varchar('ганьба','слава','добрий','вечір') a_varchar," +
                            " rnd_ipv4() a_ip," +
                            " rnd_uuid4() a_uuid," +
                            " rnd_long256() a_long256," +
                            " to_long128(rnd_long(), rnd_long()) a_long128," +
                            " cast(timestamp_sequence(600000000000, 700) as date) a_date," +
                            " timestamp_sequence(500000000000, 600) a_ts," +
                            " timestamp_sequence(400000000000, " + Timestamps.DAY_MICROS / 12 + ") designated_ts" +
                            " from long_sequence(" + rows + ")), index(a_symbol) timestamp(designated_ts) partition by month"
            );

            assertException("alter table x export partition to parquet list '2024-06'", 0, "cannot convert partition to parquet, partition does not exist");

            execute("alter table x export partition to parquet list '1970-01', '1970-02'");
            assertPartitionDoesNotExist("x", "1970-01.1");
            assertPartitionDoesNotExist("x", "1970-02.1");
            assertExportedPartitionExists("x", "1970-01.1");
            assertExportedPartitionExists("x", "1970-02.1");
        });
    }

    @Test
    public void testExportTimestampPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTable(
                    tableName,
                    "insert into " + tableName + " values(1, '2024-06-10T00:00:00.000000Z')",
                    "insert into " + tableName + " values(2, '2024-06-11T00:00:00.000000Z')",
                    "insert into " + tableName + " values(3, '2024-06-12T00:00:00.000000Z')",
                    "insert into " + tableName + " values(4, '2024-06-12T00:00:01.000000Z')",
                    "insert into " + tableName + " values(5, '2024-06-15T00:00:00.000000Z')",
                    "insert into " + tableName + " values(6, '2024-06-12T00:00:02.000000Z')"
            );

            assertQuery(
                    "index\tname\treadOnly\tisParquet\tparquetFileSize\n" +
                            "0\t2024-06-10\tfalse\tfalse\t-1\n" +
                            "1\t2024-06-11\tfalse\tfalse\t-1\n" +
                            "2\t2024-06-12\tfalse\tfalse\t-1\n" +
                            "3\t2024-06-15\tfalse\tfalse\t-1\n",
                    "select index, name, readOnly, isParquet, parquetFileSize from table_partitions('" + tableName + "')",
                    false,
                    true
            );

            execute("alter table " + tableName + " export partition to parquet where timestamp = to_timestamp('2024-06-12', 'yyyy-MM-dd')");

            assertQuery("index\tname\treadOnly\tisParquet\tparquetFileSize\tminTimestamp\tmaxTimestamp\n"
                            + "0\t2024-06-10\tfalse\tfalse\t-1\t2024-06-10T00:00:00.000000Z\t2024-06-10T00:00:00.000000Z\n"
                            + "1\t2024-06-11\tfalse\tfalse\t-1\t2024-06-11T00:00:00.000000Z\t2024-06-11T00:00:00.000000Z\n"
                            + "2\t2024-06-12\tfalse\ttrue\t658\t\t\n" +
                            "3\t2024-06-15\tfalse\tfalse\t-1\t2024-06-15T00:00:00.000000Z\t2024-06-15T00:00:00.000000Z\n",
                    "select index, name, readOnly, isParquet, parquetFileSize, minTimestamp, maxTimestamp from table_partitions('" + tableName + "')",
                    false,
                    true
            );

            assertPartitionExists(tableName, "2024-06-10");
            assertPartitionExists(tableName, "2024-06-11.0");
            assertPartitionDoesNotExist(tableName, "2024-06-12.6");
            assertPartitionExists(tableName, "2024-06-15.3");

            assertExportedPartitionExists(tableName, "2024-06-10");
            assertExportedPartitionExists(tableName, "2024-06-11.0");
            assertExportedPartitionExists(tableName, "2024-06-15.3");
        });
    }

    private void assertExportedPartition0(String tableName, boolean exists, String partition) {
        Path path = Path.getThreadLocal(inputRoot);
        path.concat(engine.verifyTableName(tableName));
        path.concat(partition).concat(PARQUET_PARTITION_NAME);

        if (exists) {
            Assert.assertTrue(ff.exists(path.$()));
        } else {
            Assert.assertFalse(ff.exists(path.$()));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void assertExportedPartitionDoesNotExist(String tableName, String partition) {
        assertExportedPartition0(tableName, false, partition);
    }

    private void assertExportedPartitionExists(String tableName, String partition) {
        assertExportedPartition0(tableName, true, partition);
    }

    private void assertPartitionDoesNotExist(String tableName, String partition) {
        assertPartitionOnDisk0(tableName, false, partition);
    }

    private void assertPartitionExists(String tableName, String partition) {
        assertPartitionOnDisk0(tableName, true, partition);
    }

    private void assertPartitionOnDisk0(String tableName, boolean exists, String partition) {
        Path path = Path.getThreadLocal(configuration.getDbRoot());
        path.concat(engine.verifyTableName(tableName));
        path.concat(partition).concat(PARQUET_PARTITION_NAME);

        if (exists) {
            Assert.assertTrue(ff.exists(path.$()));
        } else {
            Assert.assertFalse(ff.exists(path.$()));
        }
    }

    private void createTable(String tableName, String... inserts) throws Exception {
        TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY).col("id", ColumnType.INT).timestamp();
        AbstractCairoTest.create(model);
        for (int i = 0, n = inserts.length; i < n; i++) {
            execute(inserts[i]);
        }
    }

    private void createTableStr(String tableName, String... inserts) throws Exception {
        TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY).col("id", ColumnType.INT).col("str", ColumnType.STRING).timestamp();
        AbstractCairoTest.create(model);
        for (int i = 0, n = inserts.length; i < n; i++) {
            execute(inserts[i]);
        }
    }
}
