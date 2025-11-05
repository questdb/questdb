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

package io.questdb.test.cairo.o3;

import io.questdb.Metrics;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.FilesFacade;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class O3SquashPartitionTest extends AbstractCairoTest {
    private static final TimestampDriver MICRO_DRIVER = MicrosTimestampDriver.INSTANCE;
    private final TestTimestampType timestampType;

    public O3SquashPartitionTest() {
        this.timestampType = TestUtils.getTimestampType();
    }

    @Before
    public void setUp() {
        Overrides overrides = node1.getConfigurationOverrides();
        overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 << 10);
        super.setUp();
    }

    @Test
    public void testCannotSplitPartitionAllRowsSameTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            Overrides overrides1 = node1.getConfigurationOverrides();
            overrides1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);
            long start = MICRO_DRIVER.parseFloorLiteral("2020-02-03");

            Metrics metrics = engine.getMetrics();
            int rowCount = (int) metrics.tableWriterMetrics().getPhysicallyWrittenRows();

            // create table with 800 points at 2020-02-03 sharp
            // and 200 points in at 2020-02-03T01
            executeWithRewriteTimestamp(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " rnd_double_array(1,1) arr," +
                            " cast(" + start + " + (x / 800) * 60 * 60 * 1000000L  as timestamp)::#TIMESTAMP ts" +
                            " from long_sequence(1000)" +
                            ") timestamp (ts) partition by DAY",
                    timestampType.getTypeName()
            );

            rowCount = assertRowCount(1000, rowCount);

            // Split at 2020-02-03
            execute(
                    "insert into x " +
                            "select" +
                            " cast(x as int) * 1000000 i," +
                            " -x - 1000000L as j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " rnd_double_array(1,1) arr," +
                            " cast('2020-02-03' as timestamp) ts" +
                            " from long_sequence(10)"
            );

            rowCount = assertRowCount(1010, rowCount);

            // Check that the partition is not split
            assertSql("""
                    name
                    2020-02-03
                    """, "select name from table_partitions('x')");

            // Split at 2020-02-03T01
            execute(
                    "insert into x " +
                            "select" +
                            " cast(x as int) * 1000000 i," +
                            " -x - 1000000L as j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " rnd_double_array(1,1) arr," +
                            " cast('2020-02-03T00:30' as timestamp) ts" +
                            " from long_sequence(10)"
            );

            // Check that the partition is split
            assertSql("name\tnumRows\n" +
                            "2020-02-03\t809\n" +
                            (TIMESTAMP_NS_TYPE_NAME.equals(timestampType.getTypeName()) ? "2020-02-03T000000-000000001\t211\n" : "2020-02-03T000000-000001\t211\n"),
                    "select name,numRows from table_partitions('x')");

            assertRowCount(211, rowCount);
        });
    }

    @Test
    public void testSplitLastPartition() throws Exception {
        assertMemoryLeak(() -> {
            // 4kb prefix split threshold
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 * (1 << 10));
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);
            int rowCount = (int) node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();

            execute(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " rnd_double_array(1,1) arr," +
                            " timestamp_sequence('2020-02-04T00', 60*1000000L)::" + timestampType.getTypeName() + " ts" +
                            " from long_sequence(60*(23*2-24))" +
                            ") timestamp (ts) partition by DAY",
                    sqlExecutionContext
            );

            rowCount = assertRowCount(60 * (23 * 2 - 24), rowCount);

            String sqlPrefix = "insert into x " +
                    "select" +
                    " cast(x as int) * 1000000 i," +
                    " -x - 1000000L as j," +
                    " rnd_str(5,16,2) as str," +
                    " rnd_varchar(1,40,5) as varc1," +
                    " rnd_varchar(1, 1,5) as varc2," +
                    " rnd_double_array(1,1) arr,";
            execute(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T20:01', 1000000L) ts" +
                            " from long_sequence(200)",
                    sqlExecutionContext
            );

            String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";
            assertSql(replaceTimestampSuffix1("""
                    minTimestamp\tnumRows\tname
                    2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                    2020-02-04T20:01:00.000000Z\t319\t2020-02-04T200000-000001
                    """, timestampType.getTypeName()), partitionsSql);

            rowCount = assertRowCount(319, rowCount);

            // Partition "2020-02-04" squashed the new update

            try (TableReader ignore = getReader("x")) {
                execute(sqlPrefix +
                                " timestamp_sequence('2020-02-04T18:01', 60*1000000L) ts" +
                                " from long_sequence(50)",
                        sqlExecutionContext
                );

                // Partition "2020-02-04" cannot be squashed with the new update because it's locked by the reader
                assertSql(replaceTimestampSuffix1("""
                        minTimestamp\tnumRows\tname
                        2020-02-04T00:00:00.000000Z\t1081\t2020-02-04
                        2020-02-04T18:01:00.000000Z\t170\t2020-02-04T180000-000001
                        2020-02-04T20:01:00.000000Z\t319\t2020-02-04T200000-000001
                        """, timestampType.getTypeName()), partitionsSql);

                rowCount = assertRowCount(170, rowCount);
            }

            // should squash partitions into 2 pieces
            execute(sqlPrefix +
                            " timestamp_sequence('2020-02-04T18:01', 1000000L) ts" +
                            " from long_sequence(50)",
                    sqlExecutionContext
            );

            assertSql(replaceTimestampSuffix1("""
                    minTimestamp\tnumRows\tname
                    2020-02-04T00:00:00.000000Z\t1301\t2020-02-04
                    2020-02-04T20:01:00.000000Z\t319\t2020-02-04T200000-000001
                    """, timestampType.getTypeName()), partitionsSql);

            rowCount = assertRowCount((170 + 50) * 2, rowCount);


            execute(sqlPrefix +
                            " timestamp_sequence('2020-02-04T22:01:13', 60*1000000L) ts" +
                            " from long_sequence(50)",
                    sqlExecutionContext
            );

            assertSql(replaceTimestampSuffix1("""
                    minTimestamp\tnumRows\tname
                    2020-02-04T00:00:00.000000Z\t1301\t2020-02-04
                    2020-02-04T20:01:00.000000Z\t369\t2020-02-04T200000-000001
                    """, timestampType.getTypeName()), partitionsSql);

            int delta = 50;
            rowCount = assertRowCount(delta, rowCount);

            // commit in order rolls to the next partition, should squash partition "2020-02-04" to single part
            execute(sqlPrefix +
                            " timestamp_sequence('2020-02-05T01:01:15', 10*60*1000000L) ts" +
                            " from long_sequence(50)",
                    sqlExecutionContext
            );

            assertSql(replaceTimestampSuffix1("""
                    minTimestamp\tnumRows\tname
                    2020-02-04T00:00:00.000000Z\t1670\t2020-02-04
                    2020-02-05T01:01:15.000000Z\t50\t2020-02-05
                    """, timestampType.getTypeName()), partitionsSql);

            delta = 369 + 50;
            assertRowCount(delta, rowCount);
        });
    }

    @Test
    public void testSplitLastPartitionAppend() throws Exception {
        assertMemoryLeak(() -> {
            // 4kb prefix split threshold
            Overrides overrides1 = node1.getConfigurationOverrides();
            overrides1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 * (1 << 10));
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 1);

            int rowCount = (int) node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
            execute(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " rnd_double_array(1,1) arr," +
                            " timestamp_sequence('2020-02-04T00', 60*1000000L)::" + timestampType.getTypeName() + " ts" +
                            " from long_sequence(60*(23*2-24))" +
                            ") timestamp (ts) partition by DAY"
            );

            rowCount = assertRowCount(60 * (23 * 2 - 24), rowCount);
            execute("alter table x add column k int");

            String sqlPrefix = "insert into x " +
                    "select" +
                    " cast(x as int) * 1000000 i," +
                    " -x - 1000000L as j," +
                    " rnd_str(5,16,2) as str," +
                    " rnd_varchar(1,40,5) as varc1," +
                    " rnd_varchar(1, 1,5) as varc2," +
                    " rnd_double_array(1,1) arr,";
            execute(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T20:01', 1000000L) ts," +
                            " x + 2 as k" +
                            " from long_sequence(200)"
            );

            rowCount = assertRowCount(319 * 2, rowCount);

            String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";
            assertSql(replaceTimestampSuffix1("""
                    minTimestamp\tnumRows\tname
                    2020-02-04T00:00:00.000000Z\t1520\t2020-02-04
                    """, timestampType.getTypeName()), partitionsSql);

            // Append in order to check last partition opened for writing correctly.
            execute(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T22:01', 1000000L) ts," +
                            " x + 2 as k" +
                            " from long_sequence(200)"
            );

            assertSql(replaceTimestampSuffix1("""
                    minTimestamp\tnumRows\tname
                    2020-02-04T00:00:00.000000Z\t1720\t2020-02-04
                    """, timestampType.getTypeName()), partitionsSql);

            assertRowCount(200, rowCount);
        });
    }

    @Test
    public void testSplitLastPartitionAtExistingTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // create table with 2 points every hour for 1 day of 2020-02-03

            Overrides overrides1 = node1.getConfigurationOverrides();
            overrides1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);
            long start = MICRO_DRIVER.parseFloorLiteral("2020-02-03");
            executeWithRewriteTimestamp(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " rnd_double_array(1,1) arr," +
                            " cast(" + start + " + (x / 2) * 60 * 60 * 1000000L  as timestamp)::#TIMESTAMP ts" +
                            " from long_sequence(2*24)" +
                            ") timestamp (ts) partition by DAY", timestampType.getTypeName()
            );

            try (
                    RecordCursorFactory cursorFactory = select("select * from x where ts between '2020-02-03T17' and '2020-02-03T18'");
                    // Open reader
                    RecordCursor cursor = cursorFactory.getCursor(sqlExecutionContext)
            ) {
                // Check that the originally open reader does not see these changes
                println(cursorFactory, cursor);
                String expected = replaceTimestampSuffix1("""
                        i\tj\tstr\tvarc1\tvarc2\tarr\tts
                        34\t-34\tZTCQXJOQQYU\tw\tM\t[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]\t2020-02-03T17:00:00.000000Z
                        35\t-35\tTYONWEC\t\uDBB3\uDC03몍Ө*\uDADD\uDD4C2\uD95A\uDC74\t\uDA63\uDF1C\t[null,null]\t2020-02-03T17:00:00.000000Z
                        36\t-36\t\tȾ䶲L_oW4ˣ!۱ݥ0;\uE373춑J͗Eת\tB\t[null,null,null]\t2020-02-03T18:00:00.000000Z
                        37\t-37\tEYDNMIOCCVV\tqhG+Z-%,mY*U\t|\t[null,null,null,null,null,null]\t2020-02-03T18:00:00.000000Z
                        """, timestampType.getTypeName());
                TestUtils.assertEquals(expected, sink);

                // Split at 17:30
                execute(
                        "insert into x " +
                                "select" +
                                " cast(x as int) * 1000000 i," +
                                " -x - 1000000L as j," +
                                " rnd_str(5,16,2) as str," +
                                " rnd_varchar(1,40,5) as varc1," +
                                " rnd_varchar(1, 1,5) as varc2," +
                                " rnd_double_array(1,1) arr," +
                                " timestamp_sequence('2020-02-03T17', 60*1000000L) ts" +
                                " from long_sequence(1)"
                );

                // Check that the originally open reader does not see these changes
                cursor.toTop();
                println(cursorFactory, cursor);
                TestUtils.assertEquals(expected, sink);

                // add data at 17:15
                execute(
                        "insert into x " +
                                "select" +
                                " cast(x as int) * 1000000 i," +
                                " -x - 1000000L as j," +
                                " rnd_str(5,16,2) as str," +
                                " rnd_varchar(1,40,5) as varc1," +
                                " rnd_varchar(1, 1,5) as varc2," +
                                " rnd_double_array(1,1) arr," +
                                " timestamp_sequence('2020-02-03T17', 60*1000000L) ts" +
                                " from long_sequence(1)"
                );

                // Check that the originally open reader does not see these changes
                cursor.toTop();
                println(cursorFactory, cursor);
                TestUtils.assertEquals(expected, sink);
            }
            assertSql(replaceTimestampSuffix1("""
                            i\tj\tstr\tvarc1\tvarc2\tarr\tts
                            34\t-34\tZTCQXJOQQYU\tw\tM\t[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]\t2020-02-03T17:00:00.000000Z
                            35\t-35\tTYONWEC\t\uDBB3\uDC03몍Ө*\uDADD\uDD4C2\uD95A\uDC74\t\uDA63\uDF1C\t[null,null]\t2020-02-03T17:00:00.000000Z
                            1000000\t-1000001\tPTDPZFOM\tkZh{J_c@Lk_"al_v}7GLR2w}5i2aXS\t\uD9B6\uDCED\t[null,null,null,null,null,null,null,null,null,null]\t2020-02-03T17:00:00.000000Z
                            1000000\t-1000001\tXNZKT\t\uD9B7\uDDFFR˦ӣH\uDA4A\uDCC2\uDA4E\uDC39tȑ\uD9A5\uDEBC蓡3#Ӯ\t#\t[null,null,null,null]\t2020-02-03T17:00:00.000000Z
                            36\t-36\t\tȾ䶲L_oW4ˣ!۱ݥ0;\uE373춑J͗Eת\tB\t[null,null,null]\t2020-02-03T18:00:00.000000Z
                            37\t-37\tEYDNMIOCCVV\tqhG+Z-%,mY*U\t|\t[null,null,null,null,null,null]\t2020-02-03T18:00:00.000000Z
                            """, timestampType.getTypeName()),
                    "select * from x where ts between '2020-02-03T17' and '2020-02-03T18'");
        });
    }

    @Test
    public void testSplitLastPartitionLockedAndCannotBeAppended() throws Exception {
        assertMemoryLeak(() -> {
            // create table with 2 points every hour for 1 day of 2020-02-03

            Overrides overrides1 = node1.getConfigurationOverrides();
            overrides1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);
            long start = MICRO_DRIVER.parseFloorLiteral("2020-02-03");
            executeWithRewriteTimestamp(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " rnd_double_array(1,1) arr," +
                            " cast(" + start + " + (x / 2) * 60 * 60 * 1000000L as timestamp)::#TIMESTAMP ts" +
                            " from long_sequence(2*24)" +
                            ") timestamp (ts) partition by DAY",
                    timestampType.getTypeName()
            );

            try (
                    RecordCursorFactory cursorFactory = select("select * from x where ts between '2020-02-03T17' and '2020-02-03T18'");
                    // Open reader
                    RecordCursor cursor = cursorFactory.getCursor(sqlExecutionContext)
            ) {
                // Check that the originally open reader does not see these changes
                sink.clear();
                println(cursorFactory, cursor);
                String expected = replaceTimestampSuffix1("""
                        i\tj\tstr\tvarc1\tvarc2\tarr\tts
                        34\t-34\tZTCQXJOQQYU\tw\tM\t[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]\t2020-02-03T17:00:00.000000Z
                        35\t-35\tTYONWEC\t\uDBB3\uDC03몍Ө*\uDADD\uDD4C2\uD95A\uDC74\t\uDA63\uDF1C\t[null,null]\t2020-02-03T17:00:00.000000Z
                        36\t-36\t\tȾ䶲L_oW4ˣ!۱ݥ0;\uE373춑J͗Eת\tB\t[null,null,null]\t2020-02-03T18:00:00.000000Z
                        37\t-37\tEYDNMIOCCVV\tqhG+Z-%,mY*U\t|\t[null,null,null,null,null,null]\t2020-02-03T18:00:00.000000Z
                        """, timestampType.getTypeName());
                TestUtils.assertEquals(expected, sink);

                // Split at 17:30
                execute(
                        "insert into x " +
                                "select" +
                                " cast(x as int) * 1000000 i," +
                                " -x - 1000000L as j," +
                                " rnd_str(5,16,2) as str," +
                                " rnd_varchar(1,40,5) as varc1," +
                                " rnd_varchar(1, 1,5) as varc2," +
                                " rnd_double_array(1,1) arr," +
                                " timestamp_sequence('2020-02-03T17:30', 60*1000000L) ts" +
                                " from long_sequence(1)"
                );

                // Check that the originally open reader does not see these changes
                cursor.toTop();
                println(cursorFactory, cursor);
                TestUtils.assertEquals(expected, sink);

                // add data at 17:15
                execute(
                        "insert into x " +
                                "select" +
                                " cast(x as int) * 1000000 i," +
                                " -x - 1000000L as j," +
                                " rnd_str(5,16,2) as str," +
                                " rnd_varchar(1,40,5) as varc1," +
                                " rnd_varchar(1, 1,5) as varc2," +
                                " rnd_double_array(1,1) arr," +
                                " timestamp_sequence('2020-02-03T17:15', 60*1000000L) ts" +
                                " from long_sequence(1)"
                );

                // Check that the originally open reader does not see these changes
                cursor.toTop();
                println(cursorFactory, cursor);
                TestUtils.assertEquals(expected, sink);
            }
        });
    }

    @Test
    public void testSplitMidPartitionCheckIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_symbol(null,'5','16','2') as sym," +
                            " rnd_double_array(1,1) arr," +
                            " timestamp_sequence('2020-02-03T13', 60*1000000L)::" + timestampType.getTypeName() + " ts" +
                            " from long_sequence(60*24*2)" +
                            "), index(sym) timestamp (ts) partition by DAY",
                    sqlExecutionContext
            );

            execute(
                    "create table z as (" +
                            "select" +
                            " cast(x as int) * 1000000 i," +
                            " -x - 1000000L as j," +
                            " rnd_symbol(null,'5','16','2') as sym," +
                            " rnd_double_array(1,1) arr," +
                            " timestamp_sequence('2020-02-04T23:01', 60*1000000L)::" + timestampType.getTypeName() + " ts" +
                            " from long_sequence(50))",
                    sqlExecutionContext
            );

            executeWithRewriteTimestamp(
                    "create table y (" +
                            "i int," +
                            "j long," +
                            "sym symbol," +
                            "arr double[]," +
                            "ts #TIMESTAMP)",
                    timestampType.getTypeName()
            );
            execute("insert into y select * from x", sqlExecutionContext);
            execute("insert into y select * from z", sqlExecutionContext);

            execute("insert into x select * from z", sqlExecutionContext);
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "y order by ts",
                    "x",
                    LOG,
                    true
            );
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "y where sym = '5' order by ts", "x where sym = '5'", LOG);
            TestUtils.assertIndexBlockCapacity(engine, "x", "sym");
        });
    }

    @Test
    public void testSplitMidPartitionFailedToSquash() throws Exception {
        Assume.assumeTrue(engine.getConfiguration().isWriterMixedIOEnabled());

        AtomicLong failToCopyLen = new AtomicLong();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long copyData(long srcFd, long destFd, long offsetSrc, long destOffset, long length) {
                long result = super.copyData(srcFd, destFd, offsetSrc, destOffset, length);
                if (length == failToCopyLen.get()) {
                    return failToCopyLen.get() - 1;
                }
                return result;
            }
        };

        assertMemoryLeak(ff, () -> {
            // 4kb prefix split threshold
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 * (1 << 10));
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);
            engine.resetFrameFactory();

            execute(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " rnd_double_array(1,1) arr," +
                            " timestamp_sequence('2020-02-04T00', 60*1000000L)::" + timestampType.getTypeName() + " ts" +
                            " from long_sequence(60*36)" +
                            ") timestamp (ts) partition by DAY"
            );

            execute("alter table x add column k int");

            String sqlPrefix = "insert into x " +
                    "select" +
                    " cast(x as int) * 1000000 i," +
                    " -x - 1000000L as j," +
                    " rnd_str(5,16,2) as str," +
                    " rnd_varchar(1,40,5) as varc1," +
                    " rnd_varchar(1, 1,5) as varc2," +
                    " rnd_double_array(1,1) arr,";

            try {
                // fail squashing fix len column.
                failToCopyLen.set(1756);
                execute(
                        sqlPrefix +
                                " timestamp_sequence('2020-02-04T20:01', 1000000L) ts," +
                                " x + 2 as k" +
                                " from long_sequence(200)"
                );
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "Cannot copy data");
            }

            String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";
            assertSql(replaceTimestampSuffix1("""
                    minTimestamp\tnumRows\tname
                    2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                    2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001
                    2020-02-05T00:00:00.000000Z\t720\t2020-02-05
                    """, timestampType.getTypeName()), partitionsSql);

            try {
                // Append another time and fail squashing var len column.
                failToCopyLen.set(2556);
                execute(
                        sqlPrefix +
                                " timestamp_sequence('2020-02-04T22:01', 1000000L) ts," +
                                " x + 2 as k" +
                                " from long_sequence(200)"
                );
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "Cannot copy data");
            }

            assertSql(replaceTimestampSuffix1("""
                    minTimestamp\tnumRows\tname
                    2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                    2020-02-04T20:01:00.000000Z\t639\t2020-02-04T200000-000001
                    2020-02-05T00:00:00.000000Z\t720\t2020-02-05
                    """, timestampType.getTypeName()), partitionsSql);

            // success
            failToCopyLen.set(0);
            execute(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T22:01', 1000000L) ts," +
                            " x + 2 as k" +
                            " from long_sequence(200)"
            );

            assertSql(replaceTimestampSuffix1("""
                    minTimestamp\tnumRows\tname
                    2020-02-04T00:00:00.000000Z\t2040\t2020-02-04
                    2020-02-05T00:00:00.000000Z\t720\t2020-02-05
                    """, timestampType.getTypeName()), partitionsSql);

        });
    }

    @Test
    public void testSplitMidPartitionOpenReader() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_symbol(null,'5','16','2') as sym," +
                            " rnd_double_array(1,1) arr," +
                            " timestamp_sequence('2020-02-03T13', 60*1000000L)::" + timestampType.getTypeName() + " ts" +
                            " from long_sequence(60*24*2)" +
                            "), index(sym) timestamp (ts) partition by DAY",
                    sqlExecutionContext
            );

            execute(
                    "create table z as (" +
                            "select" +
                            " cast(x as int) * 1000000 i," +
                            " -x - 1000000L as j," +
                            " rnd_symbol(null,'5','16','2') as sym," +
                            " rnd_double_array(1,1) arr," +
                            " timestamp_sequence('2020-02-04T23:01', 60*1000000L)::" + timestampType.getTypeName() + " ts" +
                            " from long_sequence(50))",
                    sqlExecutionContext
            );

            executeWithRewriteTimestamp(
                    "create table y (" +
                            "i int," +
                            "j long," +
                            "sym symbol," +
                            "arr double[]," +
                            "ts #TIMESTAMP)",
                    timestampType.getTypeName()
            );
            execute("insert into y select * from x", sqlExecutionContext);
            execute("insert into y select * from z", sqlExecutionContext);

            try (TableReader ignore = getReader("x")) {
                execute("insert into x select * from z", sqlExecutionContext);

                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "y order by ts",
                        "x",
                        LOG,
                        true
                );
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, "y where sym = '5' order by ts", "x where sym = '5'", LOG);
                assertSql(replaceTimestampSuffix1("""
                        name\tminTimestamp
                        2020-02-03\t2020-02-03T13:00:00.000000Z
                        2020-02-04\t2020-02-04T00:00:00.000000Z
                        2020-02-04T230000-000001\t2020-02-04T23:01:00.000000Z
                        2020-02-05\t2020-02-05T00:00:00.000000Z
                        """, timestampType.getTypeName()), "select name, minTimestamp from table_partitions('x')"
                );
            }

            // Another reader, should allow to squash partitions
            try (TableReader ignore = getReader("x")) {
                execute("insert into x(ts) values('2020-02-06')");
                assertSql(replaceTimestampSuffix1("""
                        name\tminTimestamp
                        2020-02-03\t2020-02-03T13:00:00.000000Z
                        2020-02-04\t2020-02-04T00:00:00.000000Z
                        2020-02-05\t2020-02-05T00:00:00.000000Z
                        2020-02-06\t2020-02-06T00:00:00.000000Z
                        """, timestampType.getTypeName()), "select name, minTimestamp from table_partitions('x')");
            }

            TestUtils.assertIndexBlockCapacity(engine, "x", "sym");
        });
    }

    @Test
    public void testSplitPartitionChangesColTop() throws Exception {
        assertMemoryLeak(() -> {
            // 4kb prefix split threshold
            Overrides overrides1 = node1.getConfigurationOverrides();
            overrides1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 * (1 << 10));
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 1);

            execute(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " rnd_double_array(1,1) arr," +
                            " timestamp_sequence('2020-02-04T00', 60*1000000L)::" + timestampType.getTypeName() + " ts" +
                            " from long_sequence(60*(23*2-24))" +
                            ") timestamp (ts) partition by DAY"
            );

            String sqlPrefix = "insert into x " +
                    "select" +
                    " cast(x as int) * 1000000 i," +
                    " -x - 1000000L as j," +
                    " rnd_str(5,16,2) as str," +
                    " rnd_varchar(1,40,5) as varc1," +
                    " rnd_varchar(1, 1,5) as varc2," +
                    " rnd_double_array(1,1) arr,";
            String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";

            // Prevent squashing
            try (TableReader ignore = getReader("x")) {
                execute(
                        sqlPrefix +
                                " timestamp_sequence('2020-02-04T20:01', 1000000L) ts," +
                                " x + 2 as k" +
                                " from long_sequence(200)"
                );

                assertSql(replaceTimestampSuffix1("""
                        minTimestamp\tnumRows\tname
                        2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                        2020-02-04T20:01:00.000000Z\t319\t2020-02-04T200000-000001
                        """, timestampType.getTypeName()), partitionsSql);
            }

            execute("alter table x add column k int");

            // Append in order to check last partition opened for writing correctly.
            execute(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T22:01', 1000000L) ts," +
                            " x + 2 as k" +
                            " from long_sequence(200)"
            );

            assertSql(replaceTimestampSuffix1("""
                    minTimestamp\tnumRows\tname
                    2020-02-04T00:00:00.000000Z\t1720\t2020-02-04
                    """, timestampType.getTypeName()), partitionsSql);

        });
    }

    @Test
    public void testSquashPartitionRollingCommits() throws Exception {
        assertMemoryLeak(() -> {
            // 4kb prefix split threshold
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 * (1 << 10));
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 5);

            execute(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " timestamp_sequence('2020-02-04T00', 60*1000000L)::" + timestampType.getTypeName() + " ts" +
                            " from long_sequence(1)" +
                            ") timestamp (ts) partition by HOUR ",
                    sqlExecutionContext
            );
            drainWalQueue();

            // Run loop to create splits, commit 100 row batches shifted 30 seconds apart
            String sqlPrefix = "insert into x " +
                    "select" +
                    " cast(x as int) * 1000000 i," +
                    " -x - 1000000L as j," +
                    " rnd_str(5,16,2) as str," +
                    " rnd_varchar(1,40,5) as varc1," +
                    " rnd_varchar(1, 1,5) as varc2,";

            long startTs = MICRO_DRIVER.parseFloorLiteral("2020-02-04T20:01");
            for (int i = 0; i < 1000; i++) {
                execute(
                        sqlPrefix +
                                " timestamp_sequence(" + startTs + "L, 1000000L) ts" +
                                " from long_sequence(100)",
                        sqlExecutionContext
                );
                startTs += 30_000_000L; // 30 seconds in microseconds
            }

            assertSql(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname
                            2020-02-04T00:00:00.000000Z\t1\t2020-02-04T00
                            2020-02-04T20:01:00.000000Z\t11680\t2020-02-04T20
                            2020-02-04T21:00:00.000000Z\t12000\t2020-02-04T21
                            2020-02-04T22:00:00.000000Z\t12000\t2020-02-04T22
                            2020-02-04T23:00:00.000000Z\t12000\t2020-02-04T23
                            2020-02-05T00:00:00.000000Z\t12000\t2020-02-05T00
                            2020-02-05T01:00:00.000000Z\t12000\t2020-02-05T01
                            2020-02-05T02:00:00.000000Z\t12000\t2020-02-05T02
                            2020-02-05T03:00:00.000000Z\t12000\t2020-02-05T03
                            2020-02-05T04:00:00.000000Z\t2500\t2020-02-05T04
                            2020-02-05T04:12:30.000000Z\t500\t2020-02-05T041229-000001
                            2020-02-05T04:15:00.000000Z\t500\t2020-02-05T041459-000001
                            2020-02-05T04:17:30.000000Z\t500\t2020-02-05T041729-000001
                            2020-02-05T04:20:00.000000Z\t320\t2020-02-05T041959-000001
                            """, timestampType.getTypeName()),
                    "select minTimestamp, numRows, name from table_partitions('x')"
            );
        });
    }

    @Test
    public void testSquashPartitionsNoLogicalPartition() throws Exception {
        assertMemoryLeak(() -> {
            // 4kb prefix split threshold
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 * (1 << 10));
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);

            execute(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " timestamp_sequence('2020-02-04T00', 60*1000000L)::" + timestampType.getTypeName() + " ts" +
                            " from long_sequence(60*(23*2))" +
                            ") timestamp (ts) partition by DAY ",
                    sqlExecutionContext
            );
            drainWalQueue();

            try (TableReader ignore = getReader("x")) {
                String sqlPrefix = "insert into x " +
                        "select" +
                        " cast(x as int) * 1000000 i," +
                        " -x - 1000000L as j," +
                        " rnd_str(5,16,2) as str," +
                        " rnd_varchar(1,40,5) as varc1," +
                        " rnd_varchar(1, 1,5) as varc2,";
                execute(
                        sqlPrefix +
                                " timestamp_sequence('2020-02-04T20:01', 1000000L) ts" +
                                " from long_sequence(200)",
                        sqlExecutionContext
                );
                drainWalQueue();

                String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";
                assertSql("minTimestamp\tnumRows\tname\n" +
                        replaceTimestampSuffix1("""
                                2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                                2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001
                                2020-02-05T00:00:00.000000Z\t1320\t2020-02-05
                                """, timestampType.getTypeName()), partitionsSql);

                execute("alter table x force drop partition list '2020-02-04'",
                        sqlExecutionContext
                );
                drainWalQueue();

                // Partition "2020-02-04" cannot be squashed with the new update because it's locked by the reader
                assertSql("minTimestamp\tnumRows\tname\n" +
                        replaceTimestampSuffix1("""
                                2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001
                                2020-02-05T00:00:00.000000Z\t1320\t2020-02-05
                                """, timestampType.getTypeName()), partitionsSql);

                // should squash partitions
                execute("alter table x squash partitions");

                drainWalQueue();
                assertSql("minTimestamp\tnumRows\tname\n" +
                        replaceTimestampSuffix1("""
                                2020-02-04T20:01:00.000000Z\t439\t2020-02-04
                                2020-02-05T00:00:00.000000Z\t1320\t2020-02-05
                                """, timestampType.getTypeName()), partitionsSql);
            }
        });
    }

    @Test
    public void testSquashPartitionsOnEmptyTable() throws Exception {
        testSquashPartitionsOnEmptyTable("");
    }

    @Test
    public void testSquashPartitionsOnEmptyTableWal() throws Exception {
        testSquashPartitionsOnEmptyTable("WAL");
    }

    @Test
    public void testSquashPartitionsOnNonEmptyTable() throws Exception {
        testSquashPartitionsOnNonEmptyTable("");
    }

    @Test
    public void testSquashPartitionsOnNonEmptyTableWal() throws Exception {
        testSquashPartitionsOnNonEmptyTable("WAL");
    }

    private int assertRowCount(int delta, int rowCount) {
        Assert.assertEquals(delta, getPhysicalRowsSinceLastCommit());
        rowCount += delta;
        Assert.assertEquals(rowCount, node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows());
        return rowCount;
    }

    private long getPhysicalRowsSinceLastCommit() {
        try (TableWriter tw = getWriter("x")) {
            return tw.getPhysicallyWrittenRowsSinceLastCommit();
        }
    }

    private void testSquashPartitionsOnEmptyTable(String wal) throws Exception {
        assertMemoryLeak(() -> {
            // 4kb prefix split threshold
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 * (1 << 10));
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);

            executeWithRewriteTimestamp(
                    "create table x (" +
                            " i int," +
                            " j long," +
                            " str string," +
                            " varc1 varchar," +
                            " varc2 varchar," +
                            " arr double[]," +
                            " ts #TIMESTAMP" +
                            ") timestamp (ts) partition by DAY " + wal,
                    timestampType.getTypeName()
            );
            drainWalQueue();

            // should squash partitions on empty table
            execute("alter table x squash partitions");
            drainWalQueue();

            String sqlPrefix = "insert into x " +
                    "select" +
                    " cast(x as int) * 1000000 i," +
                    " -x - 1000000L as j," +
                    " rnd_str(5,16,2) as str," +
                    " rnd_varchar(1,40,5) as varc1," +
                    " rnd_varchar(1, 1,5) as varc2," +
                    " rnd_double_array(1,1) arr,";
            execute(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T20:01', 1000000L) ts" +
                            " from long_sequence(200)",
                    sqlExecutionContext
            );
            drainWalQueue();

            execute(sqlPrefix +
                            " timestamp_sequence('2020-02-05T18:01', 60*1000000L) ts" +
                            " from long_sequence(200)",
                    sqlExecutionContext
            );
            drainWalQueue();

            // should squash partitions this time
            execute("alter table x squash partitions");
            // this one should be no-op
            execute("alter table x squash partitions");
            drainWalQueue();

            String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";
            assertSql(replaceTimestampSuffix1("""
                    minTimestamp\tnumRows\tname
                    2020-02-04T20:01:00.000000Z\t200\t2020-02-04
                    2020-02-05T18:01:00.000000Z\t200\t2020-02-05
                    """, timestampType.getTypeName()), partitionsSql);

            assertSql("""
                    count
                    400
                    """, "select count() from x;");
        });
    }

    private void testSquashPartitionsOnNonEmptyTable(String wal) throws Exception {
        assertMemoryLeak(() -> {
            // 4kb prefix split threshold
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 * (1 << 10));
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);

            execute(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " timestamp_sequence('2020-02-04T00', 60*1000000L)::" + timestampType.getTypeName() + " ts" +
                            " from long_sequence(60*(23*2))" +
                            ") timestamp (ts) partition by DAY " + wal,
                    sqlExecutionContext
            );
            drainWalQueue();

            try (TableReader ignore = getReader("x")) {
                String sqlPrefix = "insert into x " +
                        "select" +
                        " cast(x as int) * 1000000 i," +
                        " -x - 1000000L as j," +
                        " rnd_str(5,16,2) as str," +
                        " rnd_varchar(1,40,5) as varc1," +
                        " rnd_varchar(1, 1,5) as varc2,";
                execute(
                        sqlPrefix +
                                " timestamp_sequence('2020-02-04T20:01', 1000000L) ts" +
                                " from long_sequence(200)",
                        sqlExecutionContext
                );
                drainWalQueue();

                String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";
                assertSql("minTimestamp\tnumRows\tname\n" +
                        replaceTimestampSuffix1("""
                                2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                                2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001
                                2020-02-05T00:00:00.000000Z\t1320\t2020-02-05
                                """, timestampType.getTypeName()), partitionsSql);

                execute(sqlPrefix +
                                " timestamp_sequence('2020-02-05T18:01', 60*1000000L) ts" +
                                " from long_sequence(50)",
                        sqlExecutionContext
                );
                drainWalQueue();

                // Partition "2020-02-04" cannot be squashed with the new update because it's locked by the reader
                assertSql("minTimestamp\tnumRows\tname\n" +
                        replaceTimestampSuffix1("""
                                2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                                2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001
                                2020-02-05T00:00:00.000000Z\t1081\t2020-02-05
                                2020-02-05T18:01:00.000000Z\t289\t2020-02-05T180000-000001
                                """, timestampType.getTypeName()), partitionsSql);

                // should squash partitions
                execute("alter table x squash partitions");

                drainWalQueue();
                assertSql("minTimestamp\tnumRows\tname\n" +
                        replaceTimestampSuffix1("""
                                2020-02-04T00:00:00.000000Z\t1640\t2020-02-04
                                2020-02-05T00:00:00.000000Z\t1370\t2020-02-05
                                """, timestampType.getTypeName()), partitionsSql);

                // Insert a few more rows and verify that they're all inserted.
                sqlPrefix = "insert into x " +
                        "select" +
                        " cast(x as int) * 1000000 i," +
                        " -x - 1000000L as j," +
                        " rnd_str(5,16,2) as str," +
                        " rnd_varchar(1,40,5) as varc1," +
                        " rnd_varchar(1, 1,5) as varc2,";
                execute(
                        sqlPrefix +
                                " timestamp_sequence('2023-02-04T20:01', 1000000L) ts" +
                                " from long_sequence(200)",
                        sqlExecutionContext
                );
                drainWalQueue();

                assertSql("count\n" +
                        (60 * (23 * 2) + 450) + "\n", "select count() from x;");
            }
        });
    }
}
