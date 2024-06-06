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

import io.questdb.Metrics;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class O3SquashPartitionTest extends AbstractCairoTest {

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
            long start = TimestampFormatUtils.parseTimestamp("2020-02-03");

            Metrics metrics = engine.getMetrics();
            int rowCount = (int) metrics.tableWriter().getPhysicallyWrittenRows();

            // create table with 800 points at 2020-02-03 sharp
            // and 200 points in at 2020-02-03T01
            ddl(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " cast(" + start + " + (x / 800) * 60 * 60 * 1000000L  as timestamp) ts" +
                            " from long_sequence(1000)" +
                            ") timestamp (ts) partition by DAY"
            );

            rowCount = assertRowCount(1000, rowCount);

            // Split at 2020-02-03
            insert(
                    "insert into x " +
                            "select" +
                            " cast(x as int) * 1000000 i," +
                            " -x - 1000000L as j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " cast('2020-02-03' as timestamp) ts" +
                            " from long_sequence(10)"
            );

            rowCount = assertRowCount(1010, rowCount);

            // Check that the partition is not split
            assertSql("name\n" +
                    "2020-02-03\n", "select name from table_partitions('x')");

            // Split at 2020-02-03T01
            insert(
                    "insert into x " +
                            "select" +
                            " cast(x as int) * 1000000 i," +
                            " -x - 1000000L as j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " cast('2020-02-03T00:30' as timestamp) ts" +
                            " from long_sequence(10)"
            );

            // Check that the partition is split
            assertSql("name\tnumRows\n" +
                    "2020-02-03\t809\n" +
                    "2020-02-03T000000-000001\t211\n", "select name,numRows from table_partitions('x')");

            assertRowCount(211, rowCount);
        });
    }

    @Test
    public void testSplitLastPartition() throws Exception {
        assertMemoryLeak(() -> {
            // 4kb prefix split threshold
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 * (1 << 10));
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);
            int rowCount = (int) metrics.tableWriter().getPhysicallyWrittenRows();

            ddl(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " timestamp_sequence('2020-02-04T00', 60*1000000L) ts" +
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
                    " rnd_varchar(1, 1,5) as varc2,";
            ddl(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T20:01', 1000000L) ts" +
                            " from long_sequence(200)",
                    sqlExecutionContext
            );

            String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";
            assertSql("minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1201\t2020-02-04\n" +
                    "2020-02-04T20:01:00.000000Z\t319\t2020-02-04T200000-000001\n", partitionsSql);

            rowCount = assertRowCount(319, rowCount);

            // Partition "2020-02-04" squashed the new update

            try (TableReader ignore = getReader("x")) {
                ddl(sqlPrefix +
                                " timestamp_sequence('2020-02-04T18:01', 60*1000000L) ts" +
                                " from long_sequence(50)",
                        sqlExecutionContext
                );

                // Partition "2020-02-04" cannot be squashed with the new update because it's locked by the reader
                assertSql("minTimestamp\tnumRows\tname\n" +
                        "2020-02-04T00:00:00.000000Z\t1081\t2020-02-04\n" +
                        "2020-02-04T18:01:00.000000Z\t170\t2020-02-04T180000-000001\n" +
                        "2020-02-04T20:01:00.000000Z\t319\t2020-02-04T200000-000001\n", partitionsSql);

                rowCount = assertRowCount(170, rowCount);
            }

            // should squash partitions into 2 pieces
            ddl(sqlPrefix +
                            " timestamp_sequence('2020-02-04T18:01', 1000000L) ts" +
                            " from long_sequence(50)",
                    sqlExecutionContext
            );

            assertSql("minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1301\t2020-02-04\n" +
                    "2020-02-04T20:01:00.000000Z\t319\t2020-02-04T200000-000001\n", partitionsSql);

            rowCount = assertRowCount((170 + 50) * 2, rowCount);


            ddl(sqlPrefix +
                            " timestamp_sequence('2020-02-04T22:01:13', 60*1000000L) ts" +
                            " from long_sequence(50)",
                    sqlExecutionContext
            );

            assertSql("minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1301\t2020-02-04\n" +
                    "2020-02-04T20:01:00.000000Z\t369\t2020-02-04T200000-000001\n", partitionsSql);

            int delta = 50;
            rowCount = assertRowCount(delta, rowCount);

            // commit in order rolls to the next partition, should squash partition "2020-02-04" to single part
            ddl(sqlPrefix +
                            " timestamp_sequence('2020-02-05T01:01:15', 10*60*1000000L) ts" +
                            " from long_sequence(50)",
                    sqlExecutionContext
            );

            assertSql("minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1670\t2020-02-04\n" +
                    "2020-02-05T01:01:15.000000Z\t50\t2020-02-05\n", partitionsSql);

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

            int rowCount = (int) metrics.tableWriter().getPhysicallyWrittenRows();
            compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " timestamp_sequence('2020-02-04T00', 60*1000000L) ts" +
                            " from long_sequence(60*(23*2-24))" +
                            ") timestamp (ts) partition by DAY"
            );

            rowCount = assertRowCount(60 * (23 * 2 - 24), rowCount);
            compile("alter table x add column k int");

            String sqlPrefix = "insert into x " +
                    "select" +
                    " cast(x as int) * 1000000 i," +
                    " -x - 1000000L as j," +
                    " rnd_str(5,16,2) as str," +
                    " rnd_varchar(1,40,5) as varc1," +
                    " rnd_varchar(1, 1,5) as varc2,";
            compile(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T20:01', 1000000L) ts," +
                            " x + 2 as k" +
                            " from long_sequence(200)"
            );

            rowCount = assertRowCount(319 * 2, rowCount);

            String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";
            assertSql("minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1520\t2020-02-04\n", partitionsSql);

            // Append in order to check last partition opened for writing correctly.
            compile(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T22:01', 1000000L) ts," +
                            " x + 2 as k" +
                            " from long_sequence(200)"
            );

            assertSql("minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1720\t2020-02-04\n", partitionsSql);

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
            long start = TimestampFormatUtils.parseTimestamp("2020-02-03");
            ddl(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " cast(" + start + " + (x / 2) * 60 * 60 * 1000000L  as timestamp) ts" +
                            " from long_sequence(2*24)" +
                            ") timestamp (ts) partition by DAY"
            );

            try (
                    RecordCursorFactory cursorFactory = select("select * from x where ts between '2020-02-03T17' and '2020-02-03T18'");
                    // Open reader
                    RecordCursor cursor = cursorFactory.getCursor(sqlExecutionContext)
            ) {
                // Check that the originally open reader does not see these changes
                println(cursorFactory, cursor);
                String expected = "i\tj\tstr\tvarc1\tvarc2\tts\n" +
                        "34\t-34\tDREQTBTWILHTEI\t_oW4ˣ!۱ݥ0;\uE373춑J͗Eתᅕ\t鏡\t2020-02-03T17:00:00.000000Z\n" +
                        "35\t-35\tZEYDNMIOCCVVWMT\tZ㝣ƣ獍\uDAA7\uDDBCκ+\uDB97\uDFEB\uE607媑⻞ Vi慎ش۩\uEBCC\uD9F3\uDE4C\uDB0F\uDFC8EÖԓ髍\uDBAC\uDCF9슣ẾŰӤ0\uD98D\uDEAB?\uDBCB\uDE78\t诐\t2020-02-03T17:00:00.000000Z\n" +
                        "36\t-36\tSRNFKFZJKOJRB\tgMo!y3R6yL1Z\t\uDB53\uDE63\t2020-02-03T18:00:00.000000Z\n" +
                        "37\t-37\tZMFYLBV\tlpfUUkA \"K}GN&!vxk5[>=lDUFVJ\t~\t2020-02-03T18:00:00.000000Z\n";
                TestUtils.assertEquals(expected, sink);

                // Split at 17:30
                insert(
                        "insert into x " +
                                "select" +
                                " cast(x as int) * 1000000 i," +
                                " -x - 1000000L as j," +
                                " rnd_str(5,16,2) as str," +
                                " rnd_varchar(1,40,5) as varc1," +
                                " rnd_varchar(1, 1,5) as varc2," +
                                " timestamp_sequence('2020-02-03T17', 60*1000000L) ts" +
                                " from long_sequence(1)"
                );

                // Check that the originally open reader does not see these changes
                cursor.toTop();
                println(cursorFactory, cursor);
                TestUtils.assertEquals(expected, sink);

                // add data at 17:15
                insert(
                        "insert into x " +
                                "select" +
                                " cast(x as int) * 1000000 i," +
                                " -x - 1000000L as j," +
                                " rnd_str(5,16,2) as str," +
                                " rnd_varchar(1,40,5) as varc1," +
                                " rnd_varchar(1, 1,5) as varc2," +
                                " timestamp_sequence('2020-02-03T17', 60*1000000L) ts" +
                                " from long_sequence(1)"
                );

                // Check that the originally open reader does not see these changes
                cursor.toTop();
                println(cursorFactory, cursor);
                TestUtils.assertEquals(expected, sink);
            }
            assertSql("i\tj\tstr\tvarc1\tvarc2\tts\n" +
                            "34\t-34\tDREQTBTWILHTEI\t_oW4ˣ!۱ݥ0;\uE373춑J͗Eתᅕ\t鏡\t2020-02-03T17:00:00.000000Z\n" +
                            "35\t-35\tZEYDNMIOCCVVWMT\tZ㝣ƣ獍\uDAA7\uDDBCκ+\uDB97\uDFEB\uE607媑⻞ Vi慎ش۩\uEBCC\uD9F3\uDE4C\uDB0F\uDFC8EÖԓ髍\uDBAC\uDCF9슣ẾŰӤ0\uD98D\uDEAB?\uDBCB\uDE78\t诐\t2020-02-03T17:00:00.000000Z\n" +
                            "1000000\t-1000001\tDNWOSNHLFUNJ\t\uD908\uDF4Dͦ\uF82B\uD955\uDDC8>7\uDA70\uDD45\uECF9J9漫\uDBDB\uDDDB1fÄ}o輖N\t0\t2020-02-03T17:00:00.000000Z\n" +
                            "1000000\t-1000001\t\t\uD9CA\uDD37Ϫ\uDA9B\uDDF3둪#\uEEE6\uDBA3\uDF27MZ#J遦҇Cn>\t(\t2020-02-03T17:00:00.000000Z\n" +
                            "36\t-36\tSRNFKFZJKOJRB\tgMo!y3R6yL1Z\t\uDB53\uDE63\t2020-02-03T18:00:00.000000Z\n" +
                            "37\t-37\tZMFYLBV\tlpfUUkA \"K}GN&!vxk5[>=lDUFVJ\t~\t2020-02-03T18:00:00.000000Z\n",
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
            long start = TimestampFormatUtils.parseTimestamp("2020-02-03");
            ddl(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " cast(" + start + " + (x / 2) * 60 * 60 * 1000000L  as timestamp) ts" +
                            " from long_sequence(2*24)" +
                            ") timestamp (ts) partition by DAY"
            );

            try (
                    RecordCursorFactory cursorFactory = select("select * from x where ts between '2020-02-03T17' and '2020-02-03T18'");
                    // Open reader
                    RecordCursor cursor = cursorFactory.getCursor(sqlExecutionContext)
            ) {
                // Check that the originally open reader does not see these changes
                sink.clear();
                println(cursorFactory, cursor);
                String expected = "i\tj\tstr\tvarc1\tvarc2\tts\n" +
                        "34\t-34\tDREQTBTWILHTEI\t_oW4ˣ!۱ݥ0;\uE373춑J͗Eתᅕ\t鏡\t2020-02-03T17:00:00.000000Z\n" +
                        "35\t-35\tZEYDNMIOCCVVWMT\tZ㝣ƣ獍\uDAA7\uDDBCκ+\uDB97\uDFEB\uE607媑⻞ Vi慎ش۩\uEBCC\uD9F3\uDE4C\uDB0F\uDFC8EÖԓ髍\uDBAC\uDCF9슣ẾŰӤ0\uD98D\uDEAB?\uDBCB\uDE78\t诐\t2020-02-03T17:00:00.000000Z\n" +
                        "36\t-36\tSRNFKFZJKOJRB\tgMo!y3R6yL1Z\t\uDB53\uDE63\t2020-02-03T18:00:00.000000Z\n" +
                        "37\t-37\tZMFYLBV\tlpfUUkA \"K}GN&!vxk5[>=lDUFVJ\t~\t2020-02-03T18:00:00.000000Z\n";
                TestUtils.assertEquals(expected, sink);

                // Split at 17:30
                insert(
                        "insert into x " +
                                "select" +
                                " cast(x as int) * 1000000 i," +
                                " -x - 1000000L as j," +
                                " rnd_str(5,16,2) as str," +
                                " rnd_varchar(1,40,5) as varc1," +
                                " rnd_varchar(1, 1,5) as varc2," +
                                " timestamp_sequence('2020-02-03T17:30', 60*1000000L) ts" +
                                " from long_sequence(1)"
                );

                // Check that the originally open reader does not see these changes
                cursor.toTop();
                println(cursorFactory, cursor);
                TestUtils.assertEquals(expected, sink);

                // add data at 17:15
                insert(
                        "insert into x " +
                                "select" +
                                " cast(x as int) * 1000000 i," +
                                " -x - 1000000L as j," +
                                " rnd_str(5,16,2) as str," +
                                " rnd_varchar(1,40,5) as varc1," +
                                " rnd_varchar(1, 1,5) as varc2," +
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
            ddl(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_symbol(null,'5','16','2') as sym," +
                            " timestamp_sequence('2020-02-03T13', 60*1000000L) ts" +
                            " from long_sequence(60*24*2)" +
                            "), index(sym) timestamp (ts) partition by DAY",
                    sqlExecutionContext
            );

            ddl(
                    "create table z as (" +
                            "select" +
                            " cast(x as int) * 1000000 i," +
                            " -x - 1000000L as j," +
                            " rnd_symbol(null,'5','16','2') as sym," +
                            " timestamp_sequence('2020-02-04T23:01', 60*1000000L) ts" +
                            " from long_sequence(50))",
                    sqlExecutionContext
            );

            ddl(
                    "create table y (" +
                            "i int," +
                            "j long," +
                            "sym symbol," +
                            "ts timestamp)",
                    sqlExecutionContext
            );
            ddl("insert into y select * from x", sqlExecutionContext);
            ddl("insert into y select * from z", sqlExecutionContext);

            ddl("insert into x select * from z", sqlExecutionContext);
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
            public long copyData(int srcFd, int destFd, long offsetSrc, long destOffset, long length) {
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

            compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " timestamp_sequence('2020-02-04T00', 60*1000000L) ts" +
                            " from long_sequence(60*36)" +
                            ") timestamp (ts) partition by DAY"
            );

            compile("alter table x add column k int");

            String sqlPrefix = "insert into x " +
                    "select" +
                    " cast(x as int) * 1000000 i," +
                    " -x - 1000000L as j," +
                    " rnd_str(5,16,2) as str," +
                    " rnd_varchar(1,40,5) as varc1," +
                    " rnd_varchar(1, 1,5) as varc2,";

            try {
                // fail squashing fix len column.
                failToCopyLen.set(1756);
                compile(
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
            assertSql("minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1201\t2020-02-04\n" +
                    "2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001\n" +
                    "2020-02-05T00:00:00.000000Z\t720\t2020-02-05\n", partitionsSql);

            try {
                // Append another time and fail squashing var len column.
                failToCopyLen.set(2556);
                compile(
                        sqlPrefix +
                                " timestamp_sequence('2020-02-04T22:01', 1000000L) ts," +
                                " x + 2 as k" +
                                " from long_sequence(200)"
                );
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "Cannot copy data");
            }

            assertSql("minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1201\t2020-02-04\n" +
                    "2020-02-04T20:01:00.000000Z\t639\t2020-02-04T200000-000001\n" +
                    "2020-02-05T00:00:00.000000Z\t720\t2020-02-05\n", partitionsSql);

            // success
            failToCopyLen.set(0);
            compile(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T22:01', 1000000L) ts," +
                            " x + 2 as k" +
                            " from long_sequence(200)"
            );

            assertSql("minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t2040\t2020-02-04\n" +
                    "2020-02-05T00:00:00.000000Z\t720\t2020-02-05\n", partitionsSql);

        });
    }

    @Test
    public void testSplitMidPartitionOpenReader() throws Exception {
        assertMemoryLeak(() -> {
            ddl(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_symbol(null,'5','16','2') as sym," +
                            " timestamp_sequence('2020-02-03T13', 60*1000000L) ts" +
                            " from long_sequence(60*24*2)" +
                            "), index(sym) timestamp (ts) partition by DAY",
                    sqlExecutionContext
            );

            ddl(
                    "create table z as (" +
                            "select" +
                            " cast(x as int) * 1000000 i," +
                            " -x - 1000000L as j," +
                            " rnd_symbol(null,'5','16','2') as sym," +
                            " timestamp_sequence('2020-02-04T23:01', 60*1000000L) ts" +
                            " from long_sequence(50))",
                    sqlExecutionContext
            );

            ddl(
                    "create table y (" +
                            "i int," +
                            "j long," +
                            "sym symbol," +
                            "ts timestamp)",
                    sqlExecutionContext
            );
            ddl("insert into y select * from x", sqlExecutionContext);
            ddl("insert into y select * from z", sqlExecutionContext);

            try (TableReader ignore = getReader("x")) {
                ddl("insert into x select * from z", sqlExecutionContext);

                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "y order by ts",
                        "x",
                        LOG,
                        true
                );
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, "y where sym = '5' order by ts", "x where sym = '5'", LOG);
                assertSql("name\tminTimestamp\n" +
                        "2020-02-03\t2020-02-03T13:00:00.000000Z\n" +
                        "2020-02-04\t2020-02-04T00:00:00.000000Z\n" +
                        "2020-02-04T230000-000001\t2020-02-04T23:01:00.000000Z\n" +
                        "2020-02-05\t2020-02-05T00:00:00.000000Z\n", "select name, minTimestamp from table_partitions('x')"
                );
            }

            // Another reader, should allow to squash partitions
            try (TableReader ignore = getReader("x")) {
                insert("insert into x(ts) values('2020-02-06')");
                assertSql("name\tminTimestamp\n" +
                        "2020-02-03\t2020-02-03T13:00:00.000000Z\n" +
                        "2020-02-04\t2020-02-04T00:00:00.000000Z\n" +
                        "2020-02-05\t2020-02-05T00:00:00.000000Z\n" +
                        "2020-02-06\t2020-02-06T00:00:00.000000Z\n", "select name, minTimestamp from table_partitions('x')");
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

            compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " timestamp_sequence('2020-02-04T00', 60*1000000L) ts" +
                            " from long_sequence(60*(23*2-24))" +
                            ") timestamp (ts) partition by DAY"
            );

            String sqlPrefix = "insert into x " +
                    "select" +
                    " cast(x as int) * 1000000 i," +
                    " -x - 1000000L as j," +
                    " rnd_str(5,16,2) as str," +
                    " rnd_varchar(1,40,5) as varc1," +
                    " rnd_varchar(1, 1,5) as varc2,";
            String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";

            // Prevent squashing
            try (TableReader ignore = getReader("x")) {
                compile(
                        sqlPrefix +
                                " timestamp_sequence('2020-02-04T20:01', 1000000L) ts," +
                                " x + 2 as k" +
                                " from long_sequence(200)"
                );

                assertSql("minTimestamp\tnumRows\tname\n" +
                        "2020-02-04T00:00:00.000000Z\t1201\t2020-02-04\n" +
                        "2020-02-04T20:01:00.000000Z\t319\t2020-02-04T200000-000001\n", partitionsSql);
            }

            compile("alter table x add column k int");

            // Append in order to check last partition opened for writing correctly.
            compile(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T22:01', 1000000L) ts," +
                            " x + 2 as k" +
                            " from long_sequence(200)"
            );

            assertSql("minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1720\t2020-02-04\n", partitionsSql);

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
        Assert.assertEquals(rowCount, metrics.tableWriter().getPhysicallyWrittenRows());
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

            ddl(
                    "create table x (" +
                            " i int," +
                            " j long," +
                            " str string," +
                            " varc1 varchar," +
                            " varc2 varchar," +
                            " ts timestamp" +
                            ") timestamp (ts) partition by DAY " + wal,
                    sqlExecutionContext
            );
            drainWalQueue();

            // should squash partitions on empty table
            compile("alter table x squash partitions");
            drainWalQueue();

            String sqlPrefix = "insert into x " +
                    "select" +
                    " cast(x as int) * 1000000 i," +
                    " -x - 1000000L as j," +
                    " rnd_str(5,16,2) as str," +
                    " rnd_varchar(1,40,5) as varc1," +
                    " rnd_varchar(1, 1,5) as varc2,";
            ddl(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T20:01', 1000000L) ts" +
                            " from long_sequence(200)",
                    sqlExecutionContext
            );
            drainWalQueue();

            ddl(sqlPrefix +
                            " timestamp_sequence('2020-02-05T18:01', 60*1000000L) ts" +
                            " from long_sequence(200)",
                    sqlExecutionContext
            );
            drainWalQueue();

            // should squash partitions this time
            compile("alter table x squash partitions");
            // this one should be no-op
            compile("alter table x squash partitions");
            drainWalQueue();

            String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";
            assertSql("minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T20:01:00.000000Z\t200\t2020-02-04\n" +
                    "2020-02-05T18:01:00.000000Z\t200\t2020-02-05\n", partitionsSql);

            assertSql("count\n" +
                    "400\n", "select count() from x;");
        });
    }

    private void testSquashPartitionsOnNonEmptyTable(String wal) throws Exception {
        assertMemoryLeak(() -> {
            // 4kb prefix split threshold
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 * (1 << 10));
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);

            ddl(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " timestamp_sequence('2020-02-04T00', 60*1000000L) ts" +
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
                ddl(
                        sqlPrefix +
                                " timestamp_sequence('2020-02-04T20:01', 1000000L) ts" +
                                " from long_sequence(200)",
                        sqlExecutionContext
                );
                drainWalQueue();

                String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";
                assertSql("minTimestamp\tnumRows\tname\n" +
                        "2020-02-04T00:00:00.000000Z\t1201\t2020-02-04\n" +
                        "2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001\n" +
                        "2020-02-05T00:00:00.000000Z\t1320\t2020-02-05\n", partitionsSql);

                ddl(sqlPrefix +
                                " timestamp_sequence('2020-02-05T18:01', 60*1000000L) ts" +
                                " from long_sequence(50)",
                        sqlExecutionContext
                );
                drainWalQueue();

                // Partition "2020-02-04" cannot be squashed with the new update because it's locked by the reader
                assertSql("minTimestamp\tnumRows\tname\n" +
                        "2020-02-04T00:00:00.000000Z\t1201\t2020-02-04\n" +
                        "2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001\n" +
                        "2020-02-05T00:00:00.000000Z\t1081\t2020-02-05\n" +
                        "2020-02-05T18:01:00.000000Z\t289\t2020-02-05T180000-000001\n", partitionsSql);

                // should squash partitions
                compile("alter table x squash partitions");

                drainWalQueue();
                assertSql("minTimestamp\tnumRows\tname\n" +
                        "2020-02-04T00:00:00.000000Z\t1640\t2020-02-04\n" +
                        "2020-02-05T00:00:00.000000Z\t1370\t2020-02-05\n", partitionsSql);

                // Insert a few more rows and verify that they're all inserted.
                sqlPrefix = "insert into x " +
                        "select" +
                        " cast(x as int) * 1000000 i," +
                        " -x - 1000000L as j," +
                        " rnd_str(5,16,2) as str," +
                        " rnd_varchar(1,40,5) as varc1," +
                        " rnd_varchar(1, 1,5) as varc2,";
                ddl(
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
