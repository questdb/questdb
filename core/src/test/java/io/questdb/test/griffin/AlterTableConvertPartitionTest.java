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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.TableToken;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static io.questdb.cairo.TableUtils.PARQUET_PARTITION_NAME;

@RunWith(Parameterized.class)
public class AlterTableConvertPartitionTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public AlterTableConvertPartitionTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testConvertAllPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertAllPartitions";
            createTable(
                    tableName,
                    "insert into " + tableName + " values(1, '2024-06-10T00:00:00.000000Z')",
                    "insert into " + tableName + " values(2, '2024-06-11T00:00:00.000000Z')",
                    "insert into " + tableName + " values(3, '2024-06-12T00:00:00.000000Z')",
                    "insert into " + tableName + " values(4, '2024-06-12T00:00:01.000000Z')",
                    "insert into " + tableName + " values(5, '2024-06-15T00:00:00.000000Z')",
                    "insert into " + tableName + " values(6, '2024-06-12T00:00:02.000000Z')"
            );

            execute("alter table " + tableName + " convert partition to parquet where timestamp > 0");

            assertPartitionExists(tableName, "2024-06-10.8");
            assertPartitionExists(tableName, "2024-06-11.6");
            assertPartitionExists(tableName, "2024-06-12.7");
            // last partition is not converted
        });
    }

    @Test
    public void testConvertAllPartitionsToParquetAndBack() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertAllPartitionsToParquetAndBack";
            createTableStr(
                    tableName,
                    "insert into " + tableName + " values(1, 'abc', '2024-06-10T00:00:00.000000Z')",
                    "insert into " + tableName + " values(2, 'edf', '2024-06-11T00:00:00.000000Z')",
                    "insert into " + tableName + " values(3, 'abc', '2024-06-12T00:00:00.000000Z')",
                    "insert into " + tableName + " values(4, 'edf', '2024-06-12T00:00:01.000000Z')",
                    "insert into " + tableName + " values(5, 'abc', '2024-06-15T00:00:00.000000Z')",
                    "insert into " + tableName + " values(6, 'edf', '2024-06-12T00:00:02.000000Z')"
            );

            execute("alter table " + tableName + " convert partition to parquet where timestamp > 0");

            assertPartitionExists(tableName, "2024-06-10.8");
            assertPartitionExists(tableName, "2024-06-11.6");
            assertPartitionExists(tableName, "2024-06-12.7");

            execute("alter table " + tableName + " convert partition to native where timestamp > 0");
            assertPartitionDoesNotExist(tableName, "2024-06-10.12");
            assertPartitionDoesNotExist(tableName, "2024-06-11.11");
            assertPartitionDoesNotExist(tableName, "2024-06-12.10");
            assertSql(
                    replaceTimestampSuffix("id\tstr\ttimestamp\n" +
                            "1\tabc\t2024-06-10T00:00:00.000000Z\n" +
                            "2\tedf\t2024-06-11T00:00:00.000000Z\n" +
                            "3\tabc\t2024-06-12T00:00:00.000000Z\n" +
                            "4\tedf\t2024-06-12T00:00:01.000000Z\n" +
                            "6\tedf\t2024-06-12T00:00:02.000000Z\n" +
                            "5\tabc\t2024-06-15T00:00:00.000000Z\n", timestampType.getTypeName()),
                    "select * from " + tableName
            );
        });
    }

    @Test
    public void testConvertLastPartition() throws Exception {
        final long rows = 10;
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x as (select" +
                            " x id," +
                            " rnd_boolean() a_boolean," +
                            " rnd_byte() a_byte," +
                            " timestamp_sequence('2024-06', 500)::" + timestampType.getTypeName() + " designated_ts" +
                            " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month"
            );

            execute("alter table x convert partition to parquet list '2024-06'");
            assertPartitionDoesNotExist("x", "2024-06.1");

            execute("insert into x(designated_ts) values('1970-01')");
            execute("alter table x convert partition to parquet list '1970-01'");
            assertPartitionExists("x", "1970-01.2");
        });
    }

    @Test
    public void testConvertListPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertListPartitions";
            createTable(
                    tableName,
                    "insert into " + tableName + " values(1, '2024-06-10T00:00:00.000000Z')",
                    "insert into " + tableName + " values(2, '2024-06-11T00:00:00.000000Z')",
                    "insert into " + tableName + " values(3, '2024-06-12T00:00:00.000000Z')",
                    "insert into " + tableName + " values(4, '2024-06-12T00:00:01.000000Z')",
                    "insert into " + tableName + " values(5, '2024-06-15T00:00:00.000000Z')",
                    "insert into " + tableName + " values(6, '2024-06-12T00:00:02.000000Z')"
            );

            execute("alter table " + tableName + " convert partition to parquet list '2024-06-10', '2024-06-11', '2024-06-12'");

            assertPartitionExists(tableName, "2024-06-10.6");
            assertPartitionExists(tableName, "2024-06-11.7");
            assertPartitionExists(tableName, "2024-06-12.8");
            assertPartitionDoesNotExist(tableName, "2024-06-15.3");
        });
    }

    @Test
    public void testConvertListZeroSizeArrayData() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x (" +
                            " an_array double[]," +
                            " a_ts timestamp" +
                            ") timestamp(a_ts) partition by month;"
            );

            execute("insert into x(an_array, a_ts) values(array[], '2024-07');");
            execute("insert into x(an_array, a_ts) values(array[1.0, 2.0, 3.0], '2024-08');");
            execute("alter table x convert partition to parquet where a_ts > 0;");
            assertPartitionExists("x", "2024-07.2");
        });
    }

    @Test
    public void testConvertListZeroSizeVarcharData() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x as (select" +
                            " case when x % 2 = 0 then rnd_varchar(1, 40, 1) end as a_varchar," +
                            " to_timestamp('2024-07', 'yyyy-MM')::" + timestampType.getTypeName() + " as a_ts," +
                            " from long_sequence(1)) timestamp (a_ts) partition by MONTH"
            );

            execute("insert into x(a_varchar, a_ts) values('', '2024-08')");
            execute("alter table x convert partition to parquet where a_ts > 0");
            assertPartitionExists("x", "2024-07.2");
        });
    }

    @Test
    public void testConvertPartitionAllTypes() throws Exception {
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
                            " rnd_double_array(1) an_array," +
                            " cast(timestamp_sequence(600000000000, 700) as date) a_date," +
                            " timestamp_sequence(500000000000, 600)::" + timestampType.getTypeName() + " a_ts," +
                            " timestamp_sequence(400000000000, " + Micros.DAY_MICROS / 12 + ")::" + timestampType.getTypeName() + " designated_ts" +
                            " from long_sequence(" + rows + ")), index(a_symbol) timestamp(designated_ts) partition by month"
            );

            assertException(
                    "alter table x convert partition to parquet list '2024-06'",
                    0,
                    "cannot convert partition to parquet, partition does not exist"
            );

            execute("alter table x convert partition to parquet list '1970-01', '1970-02'");
            assertPartitionExists("x", "1970-01.1");
            assertPartitionExists("x", "1970-02.2");
            assertPartitionDoesNotExist("x", "1970-03.3");
        });
    }

    @Test
    public void testConvertPartitionBrokenSymbols() throws Exception {
        final long rows = 10;
        final String tableName = "x";
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table " + tableName + " as (select" +
                            " x id," +
                            " rnd_symbol('a','b','c') a_symbol," +
                            " timestamp_sequence(400000000000, " + Micros.DAY_MICROS * 5 + ")::" + timestampType.getTypeName() + " designated_ts" +
                            " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month"
            );

            engine.releaseInactive();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                TableToken tableToken = engine.getTableTokenIfExists(tableName);
                path.concat(tableToken.getDirName()).concat("a_symbol").put(".o");
                FilesFacade ff = configuration.getFilesFacade();
                Assert.assertTrue(ff.exists(path.$()));
                long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(configuration.getFilesFacade().truncate(fd, SymbolMapWriter.HEADER_SIZE - 2));
                ff.close(fd);
            }
            try {
                execute("alter table " + tableName + " convert partition to parquet list '1970-01'");
                Assert.fail();
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), " SymbolMap is too short");
            }
            assertPartitionDoesNotExist(tableName, "1970-01.1");
        });
    }

    @Test
    public void testConvertPartitionParquetAndBackAllTypes() throws Exception {
        final long rows = 1000;
        Overrides overrides = node1.getConfigurationOverrides();
        // test multiple row groups
        overrides.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 101);
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
                            " rnd_str('abc', 'def', 'ghk') a_string," +
                            " rnd_bin() a_bin," +
                            " rnd_ipv4() a_ip," +
                            " rnd_varchar('ганьба','слава','добрий','вечір', '1111111111111111') a_varchar," +
                            " rnd_uuid4() a_uuid," +
                            " rnd_long256() a_long256," +
                            " to_long128(rnd_long(), rnd_long()) a_long128," +
                            " rnd_double_array(1) an_array," +
                            " cast(timestamp_sequence(600000000000, 700) as date) a_date," +
                            " timestamp_sequence(500000000000, 600)::" + timestampType.getTypeName() + " a_ts," +
                            " timestamp_sequence(400000000000, " + Micros.DAY_MICROS / 12 + ")::" + timestampType.getTypeName() + " designated_ts" +
                            " from long_sequence(" + rows + ")), index(a_symbol) timestamp(designated_ts) partition by month"
            );

            execute("create table y as (select * from x)", sqlExecutionContext);

            assertException(
                    "alter table x convert partition to parquet list '2024-06'",
                    0,
                    "cannot convert partition to parquet, partition does not exist"
            );

            execute("alter table x convert partition to parquet list '1970-01'");
            assertPartitionExists("x", "1970-01.1");
            execute("alter table x convert partition to native list '1970-01'");
            assertPartitionDoesNotExist("x", "1970-01.1");

            assertSqlCursors("select * from x", "select * from y");
        });
    }

    @Test
    public void testConvertPartitionSymbolMapDoesNotExist() throws Exception {
        final long rows = 10;
        final String tableName = "x";
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table " + tableName + " as (select" +
                            " x id," +
                            " rnd_symbol('a','b','c') a_symbol," +
                            " timestamp_sequence(400000000000, 500)::" + timestampType.getTypeName() + " designated_ts" +
                            " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month"
            );

            engine.releaseInactive();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                TableToken tableToken = engine.getTableTokenIfExists(tableName);
                path.concat(tableToken.getDirName()).concat("a_symbol").put(".o");
                FilesFacade ff = configuration.getFilesFacade();
                ff.remove(path.$());
            }
            try {
                execute("alter table " + tableName + " convert partition to parquet list '1970-01'");
                Assert.fail();
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), "SymbolMap does not exist");
            }
            assertPartitionDoesNotExist(tableName, "1970-01.1");
        });
    }

    @Test
    public void testConvertPartitionsWithColTops() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertPartitionsWithColTops";
            createTable(
                    tableName,
                    "insert into " + tableName + " values(1, '2024-06-10T00:00:00.000000Z')",
                    "insert into " + tableName + " values(2, '2024-06-11T00:00:00.000000Z')",
                    "insert into " + tableName + " values(3, '2024-06-12T00:00:00.000000Z')",
                    "insert into " + tableName + " values(4, '2024-06-12T00:00:01.000000Z')",
                    "insert into " + tableName + " values(5, '2024-06-15T00:00:00.000000Z')",
                    "insert into " + tableName + " values(6, '2024-06-12T00:00:02.000000Z')"
            );

            execute("alter table " + tableName + " add column a int");
            execute("insert into " + tableName + " values(7, '2024-06-10T00:00:00.000000Z', 1)");

            execute("alter table " + tableName + " convert partition to parquet where timestamp > 0 and timestamp < '2024-06-15'");

            assertPartitionExists(tableName, "2024-06-10.10");
            assertPartitionExists(tableName, "2024-06-11.8");
            assertPartitionExists(tableName, "2024-06-12.9");

            assertPartitionDoesNotExist(tableName, "2024-06-15.3");
        });
    }

    @Test
    public void testConvertPartitionsWithColTopsSelect() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertPartitionsWithColTopsSelect";
            createTable(
                    tableName,
                    "insert into " + tableName + " values(1, '2024-11-01T00:00:00.000000Z')",
                    "insert into " + tableName + " values(2, '2024-11-02T00:00:00.000000Z')",
                    "insert into " + tableName + " values(3, '2024-11-03T00:00:00.000000Z')",
                    "insert into " + tableName + " values(4, '2024-11-04T00:00:00.000000Z')",
                    "insert into " + tableName + " values(5, '2024-11-05T00:00:00.000000Z')"
            );

            execute("alter table " + tableName + " add column a int");
            execute("insert into " + tableName + " values(5, '2024-11-05T00:00:00.000000Z', 5)");
            execute("insert into " + tableName + " values(6, '2024-11-06T00:00:00.000000Z', 6)");
            execute("insert into " + tableName + " values(7, '2024-11-07T00:00:00.000000Z', 7)");

            execute("alter table " + tableName + " convert partition to parquet where timestamp >= 0");

            assertQuery(
                    replaceTimestampSuffix("id\ttimestamp\ta\n" +
                            "1\t2024-11-01T00:00:00.000000Z\tnull\n" +
                            "2\t2024-11-02T00:00:00.000000Z\tnull\n" +
                            "3\t2024-11-03T00:00:00.000000Z\tnull\n" +
                            "4\t2024-11-04T00:00:00.000000Z\tnull\n" +
                            "5\t2024-11-05T00:00:00.000000Z\tnull\n" +
                            "5\t2024-11-05T00:00:00.000000Z\t5\n" +
                            "6\t2024-11-06T00:00:00.000000Z\t6\n" +
                            "7\t2024-11-07T00:00:00.000000Z\t7\n", timestampType.getTypeName()),
                    tableName,
                    "timestamp",
                    true,
                    true
            );
        });
    }

    @Test
    public void testConvertSecondCallIgnored() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x as (select" +
                            " x a_long," +
                            " to_timestamp('2024-07', 'yyyy-MM')::" + timestampType.getTypeName() + " as a_ts," +
                            " from long_sequence(10)) timestamp (a_ts) partition by MONTH"
            );

            execute("insert into x(a_long, a_ts) values('42', '2024-08')");
            execute("alter table x convert partition to parquet where a_ts > 0");
            assertPartitionExists("x", "2024-07.2");

            // Second call should be ignored
            execute("alter table x convert partition to parquet where a_ts > 0");
            assertPartitionExists("x", "2024-07.2");
        });
    }

    @Test
    public void testConvertTimestampPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertTimestampPartitions";
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

            execute("alter table " + tableName + " convert partition to parquet where timestamp = to_timestamp('2024-06-12', 'yyyy-MM-dd')");

            assertQuery(
                    replaceTimestampSuffix(
                            "index\tname\treadOnly\tisParquet\tparquetFileSize\tminTimestamp\tmaxTimestamp\n" +
                                    "0\t2024-06-10\tfalse\tfalse\t-1\t2024-06-10T00:00:00.000000Z\t2024-06-10T00:00:00.000000Z\n" +
                                    "1\t2024-06-11\tfalse\tfalse\t-1\t2024-06-11T00:00:00.000000Z\t2024-06-11T00:00:00.000000Z\n" +
                                    "2\t2024-06-12\tfalse\ttrue\t" + (ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? 652 : 657) + "\t\t\n" +
                                    "3\t2024-06-15\tfalse\tfalse\t-1\t2024-06-15T00:00:00.000000Z\t2024-06-15T00:00:00.000000Z\n",
                            timestampType.getTypeName()
                    ),
                    "select index, name, readOnly, isParquet, parquetFileSize, minTimestamp, maxTimestamp from table_partitions('" + tableName + "')",
                    false,
                    true
            );

            assertPartitionDoesNotExist(tableName, "2024-06-10");
            assertPartitionDoesNotExist(tableName, "2024-06-11.0");
            assertPartitionExists(tableName, "2024-06-12.6");
            assertPartitionDoesNotExist(tableName, "2024-06-15.3");
        });
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
        TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                .col("id", ColumnType.INT)
                .timestamp(timestampType.getTimestampType());
        AbstractCairoTest.create(model);
        for (int i = 0, n = inserts.length; i < n; i++) {
            execute(inserts[i]);
        }
    }

    private void createTableStr(String tableName, String... inserts) throws Exception {
        TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                .col("id", ColumnType.INT)
                .col("str", ColumnType.STRING)
                .timestamp(timestampType.getTimestampType());
        AbstractCairoTest.create(model);
        for (int i = 0, n = inserts.length; i < n; i++) {
            execute(inserts[i]);
        }
    }
}
