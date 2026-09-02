/*+*****************************************************************************
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

package io.questdb.test.cairo.o3;

import io.questdb.Metrics;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SymbolCountProvider;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongHashSet;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
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
            assertQuery("select name from table_partitions('x')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name
                            2020-02-03
                            """);

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
            assertQuery("select name,numRows from table_partitions('x')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("name\tnumRows\n" +
                            "2020-02-03\t809\n" +
                            (TIMESTAMP_NS_TYPE_NAME.equals(timestampType.getTypeName()) ? "2020-02-03T000000-000000001\t211\n" : "2020-02-03T000000-000001\t211\n"));

            assertRowCount(211, rowCount);
        });
    }

    @Test
    public void testPartitionSquashCounterOverflow() throws Exception {
        // This asserts the SQUASH bookkeeping - a .squash_ts file left behind once the counter overflows -
        // which merge-append does not maintain: a composite partition is folded by its own path and never
        // reaches the counter this test drives. Squash over composite partitions is a known gap; until it
        // closes, pin the production default so the test keeps covering what it was written for.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            final String tableName = "backup_squash_test";
            long start = MicrosTimestampDriver.floor("2020-02-03");

            execute(
                    "create table " + tableName + " as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " rnd_double_array(1,1) arr," +
                            " cast(" + start + " + x * 60 * 1000000L  as timestamp) ts" +
                            " from long_sequence(1000)" +
                            ") timestamp (ts) partition by DAY WAL dedup upsert keys (ts)"
            );

            drainWalQueue();

            execute(
                    "insert into " + tableName +
                            " select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " rnd_double_array(1,1) arr," +
                            " cast(" + start + " + (x + 800) * 60 * 1000000L as timestamp) ts" +
                            " from long_sequence(200)"
            );

            drainWalQueue();

            execute("alter table " + tableName + " squash partitions");

            drainWalQueue();

            var tableToken = engine.verifyTableName(tableName);

            // Manually set the squash counter to max (0xFFFF) to trigger overflow on next squash
            FilesFacade ff = configuration.getFilesFacade();
            try (TxWriter txWriter = new TxWriter(ff, configuration); Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME);
                txWriter.ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);

                // Increment the squash counter until overflow
                while (txWriter.incrementPartitionSquashCounter(0)) {
                    // Keep incrementing
                }

                // Update partition size and commit to persist the counter
                txWriter.updatePartitionSizeByTimestamp(
                        txWriter.getPartitionTimestampByIndex(0),
                        txWriter.getPartitionSize(0)
                );

                ObjList<SymbolCountProvider> symbolCountSnapshot = new ObjList<>();
                for (int i = 0, n = txWriter.getSymbolColumnCount(); i < n; i++) {
                    int symbolCount = txWriter.getSymbolValueCount(i);
                    symbolCountSnapshot.add(() -> symbolCount);
                }
                txWriter.commit(symbolCountSnapshot);
            }

            // Force reload updated _txn file
            engine.releaseInactive();

            execute(
                    "insert into " + tableName +
                            " select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rnd_varchar(1,40,5) as varc1," +
                            " rnd_varchar(1, 1,5) as varc2," +
                            " rnd_double_array(1,1) arr," +
                            " cast(" + start + " + (x + 800) * 60 * 1000000L as timestamp) ts" +
                            " from long_sequence(200)"
            );

            drainWalQueue();

            execute("alter table " + tableName + " squash partitions");

            drainWalQueue();

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat("2020-02-03");
                int plen = path.size();

                path.concat(TableUtils.PARTITION_LAST_SQUASH_TIMESTAMP_FILE).$();
                long squashFileFd = configuration.getFilesFacade().openRO(path.$());
                Assert.assertTrue("Expected .squash_ts file to exist after squash counter overflow", squashFileFd != -1);

                long squashTimestamp = configuration.getFilesFacade().readNonNegativeLong(squashFileFd, 0);
                Assert.assertTrue("Expected valid squash timestamp, got: " + squashTimestamp, squashTimestamp > 0);
                configuration.getFilesFacade().close(squashFileFd);

                path.trimTo(plen);
            }
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
            assertQuery(partitionsSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname
                            2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                            2020-02-04T20:01:00.000000Z\t319\t2020-02-04T200000-000001
                            """, timestampType.getTypeName()));

            rowCount = assertRowCount(319, rowCount);

            // Partition "2020-02-04" squashed the new update

            try (TableReader ignore = getReader("x")) {
                execute(sqlPrefix +
                                " timestamp_sequence('2020-02-04T18:01', 60*1000000L) ts" +
                                " from long_sequence(50)",
                        sqlExecutionContext
                );

                // Partition "2020-02-04" cannot be squashed with the new update because it's locked by the reader
                assertQuery(partitionsSql)
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns(replaceTimestampSuffix1("""
                                minTimestamp\tnumRows\tname
                                2020-02-04T00:00:00.000000Z\t1081\t2020-02-04
                                2020-02-04T18:01:00.000000Z\t170\t2020-02-04T180000-000001
                                2020-02-04T20:01:00.000000Z\t319\t2020-02-04T200000-000001
                                """, timestampType.getTypeName()));

                rowCount = assertRowCount(170, rowCount);
            }

            // should squash partitions into 2 pieces
            execute(sqlPrefix +
                            " timestamp_sequence('2020-02-04T18:01', 1000000L) ts" +
                            " from long_sequence(50)",
                    sqlExecutionContext
            );

            assertQuery(partitionsSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname
                            2020-02-04T00:00:00.000000Z\t1301\t2020-02-04
                            2020-02-04T20:01:00.000000Z\t319\t2020-02-04T200000-000001
                            """, timestampType.getTypeName()));

            rowCount = assertRowCount((170 + 50) * 2, rowCount);


            execute(sqlPrefix +
                            " timestamp_sequence('2020-02-04T22:01:13', 60*1000000L) ts" +
                            " from long_sequence(50)",
                    sqlExecutionContext
            );

            assertQuery(partitionsSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname
                            2020-02-04T00:00:00.000000Z\t1301\t2020-02-04
                            2020-02-04T20:01:00.000000Z\t369\t2020-02-04T200000-000001
                            """, timestampType.getTypeName()));

            int delta = 50;
            rowCount = assertRowCount(delta, rowCount);

            // commit in order rolls to the next partition, should squash partition "2020-02-04" to single part
            execute(sqlPrefix +
                            " timestamp_sequence('2020-02-05T01:01:15', 10*60*1000000L) ts" +
                            " from long_sequence(50)",
                    sqlExecutionContext
            );

            assertQuery(partitionsSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname
                            2020-02-04T00:00:00.000000Z\t1670\t2020-02-04
                            2020-02-05T01:01:15.000000Z\t50\t2020-02-05
                            """, timestampType.getTypeName()));

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
            assertQuery(partitionsSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname
                            2020-02-04T00:00:00.000000Z\t1520\t2020-02-04
                            """, timestampType.getTypeName()));

            // Append in order to check last partition opened for writing correctly.
            execute(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T22:01', 1000000L) ts," +
                            " x + 2 as k" +
                            " from long_sequence(200)"
            );

            assertQuery(partitionsSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname
                            2020-02-04T00:00:00.000000Z\t1720\t2020-02-04
                            """, timestampType.getTypeName()));

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
            assertQuery("select * from x where ts between '2020-02-03T17' and '2020-02-03T18'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix1("""
                            i\tj\tstr\tvarc1\tvarc2\tarr\tts
                            34\t-34\tZTCQXJOQQYU\tw\tM\t[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]\t2020-02-03T17:00:00.000000Z
                            35\t-35\tTYONWEC\t\uDBB3\uDC03몍Ө*\uDADD\uDD4C2\uD95A\uDC74\t\uDA63\uDF1C\t[null,null]\t2020-02-03T17:00:00.000000Z
                            1000000\t-1000001\tPTDPZFOM\tkZh{J_c@Lk_"al_v}7GLR2w}5i2aXS\t\uD9B6\uDCED\t[null,null,null,null,null,null,null,null,null,null]\t2020-02-03T17:00:00.000000Z
                            1000000\t-1000001\tXNZKT\t\uD9B7\uDDFFR˦ӣH\uDA4A\uDCC2\uDA4E\uDC39tȑ\uD9A5\uDEBC蓡3#Ӯ\t#\t[null,null,null,null]\t2020-02-03T17:00:00.000000Z
                            36\t-36\t\tȾ䶲L_oW4ˣ!۱ݥ0;\uE373춑J͗Eת\tB\t[null,null,null]\t2020-02-03T18:00:00.000000Z
                            37\t-37\tEYDNMIOCCVV\tqhG+Z-%,mY*U\t|\t[null,null,null,null,null,null]\t2020-02-03T18:00:00.000000Z
                            """, timestampType.getTypeName()));
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
            assertQuery(partitionsSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname
                            2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                            2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001
                            2020-02-05T00:00:00.000000Z\t720\t2020-02-05
                            """, timestampType.getTypeName()));

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

            assertQuery(partitionsSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname
                            2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                            2020-02-04T20:01:00.000000Z\t639\t2020-02-04T200000-000001
                            2020-02-05T00:00:00.000000Z\t720\t2020-02-05
                            """, timestampType.getTypeName()));

            // success
            failToCopyLen.set(0);
            execute(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T22:01', 1000000L) ts," +
                            " x + 2 as k" +
                            " from long_sequence(200)"
            );

            Assert.assertEquals(1, getSquashCount());

            assertQuery(partitionsSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname
                            2020-02-04T00:00:00.000000Z\t2040\t2020-02-04
                            2020-02-05T00:00:00.000000Z\t720\t2020-02-05
                            """, timestampType.getTypeName()));

            // Append to partition 2020-02-04 and check that squash count is persisted
            execute(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T23:59', 1000000L) ts," +
                            " x + 2 as k" +
                            " from long_sequence(1)"
            );
            Assert.assertEquals(1, getSquashCount());

            // Replace partition 2020-02-04 with a new verion and check that squash count is reset
            execute(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04', 1000000L) ts," +
                            " x + 2 as k" +
                            " from long_sequence(1)"
            );
            Assert.assertEquals(0, getSquashCount());
        });
    }

    @Test
    public void testSplitMidPartitionMaxSplitsConfigured() throws Exception {
        // cairo.o3.mid.partition.max.splits controls how many splits a non-last
        // (mid) logical partition is allowed to keep after commit. Raising the
        // limit above the number of splits must leave them in place instead of
        // squashing them back into a single partition.
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            overrides.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 3);

            // 60*36 minute ticks span 2020-02-04 (1440 rows) and 2020-02-05 (720 rows),
            // so 2020-02-04 is a mid partition (another logical partition follows it).
            executeWithRewriteTimestamp(
                    "CREATE TABLE x AS (" +
                            "SELECT" +
                            " cast(x AS int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) AS str," +
                            " timestamp_sequence('2020-02-04T00', 60 * 1000000L)::#TIMESTAMP ts" +
                            " FROM long_sequence(60 * 36)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY",
                    timestampType.getTypeName()
            );

            // O3 insert at 2020-02-04T20:01 creates a split inside the mid partition.
            // With max.splits=3 (> 2 actual splits) the split must survive.
            execute(
                    "INSERT INTO x " +
                            "SELECT" +
                            " cast(x AS int) * 1000000 i," +
                            " -x - 1000000L AS j," +
                            " rnd_str(5,16,2) AS str," +
                            " timestamp_sequence('2020-02-04T20:01', 1000000L)::" + timestampType.getTypeName() + " ts" +
                            " FROM long_sequence(200)"
            );

            String partitionsSql = "SELECT minTimestamp, numRows, name FROM table_partitions('x') ORDER BY minTimestamp";
            assertQuery(partitionsSql)
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("minTimestamp")
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname
                            2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                            2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001
                            2020-02-05T00:00:00.000000Z\t720\t2020-02-05
                            """, timestampType.getTypeName()));
        });
    }

    @Test
    public void testSplitMidPartitionMaxSplitsDefault() throws Exception {
        // Default cairo.o3.mid.partition.max.splits=1 must squash O3 splits in a
        // non-last (mid) logical partition back into a single partition on commit.
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);

            // 60*36 minute ticks span 2020-02-04 (1440 rows) and 2020-02-05 (720 rows),
            // so 2020-02-04 is a mid partition (another logical partition follows it).
            executeWithRewriteTimestamp(
                    "CREATE TABLE x AS (" +
                            "SELECT" +
                            " cast(x AS int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) AS str," +
                            " timestamp_sequence('2020-02-04T00', 60 * 1000000L)::#TIMESTAMP ts" +
                            " FROM long_sequence(60 * 36)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY",
                    timestampType.getTypeName()
            );

            // O3 insert at 2020-02-04T20:01 would split the mid partition, but the
            // commit squashes it back because mid.partition.max.splits defaults to 1.
            execute(
                    "INSERT INTO x " +
                            "SELECT" +
                            " cast(x AS int) * 1000000 i," +
                            " -x - 1000000L AS j," +
                            " rnd_str(5,16,2) AS str," +
                            " timestamp_sequence('2020-02-04T20:01', 1000000L)::" + timestampType.getTypeName() + " ts" +
                            " FROM long_sequence(200)"
            );

            String partitionsSql = "SELECT minTimestamp, numRows, name FROM table_partitions('x') ORDER BY minTimestamp";
            assertQuery(partitionsSql)
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("minTimestamp")
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname
                            2020-02-04T00:00:00.000000Z\t1640\t2020-02-04
                            2020-02-05T00:00:00.000000Z\t720\t2020-02-05
                            """, timestampType.getTypeName()));
        });
    }

    @Test
    public void testSquashIntoOpenPartitionBehindParquetLastPartition() throws Exception {
        // A FORMAT PARQUET table makes every brand-new partition parquet from inception, so once
        // 2020-02-05 exists the writer's openLastPartition() no-ops (openLastPartitionAndSetAppendPosition
        // returns early on a parquet last partition) and the writer keeps the earlier NATIVE
        // 2020-02-04 open: lastOpenPartitionTs lags the real last partition.
        //
        // A later O3 insert into 2020-02-04 splits it and the same commit squashes the split back
        // in. squashSplitPartitions then appends into the very partition the writer holds open,
        // through the frame's own file descriptors, so the writer's column append memories describe
        // a SHORTER file than what is on disk. The next truncating close (doClose -> freeColumns ->
        // closeAppendMemoryTruncate) trims every .d back to ceilPageSize(stale append offset),
        // physically discarding the bytes the squash wrote. The 200-char strings make the discarded
        // region clear a 64K page, so the loss is observable on every platform.
        //
        // A split of 2020-02-04 lives on disk only until the same commit squashes it away, and
        // table_partitions below can only show the aftermath -- a plain whole-partition rewrite
        // leaves an identical view. So the split itself has to be witnessed while it exists,
        // through the directory the O3 job names after the split point: one tick past the last
        // pre-existing row before the 2020-02-04T20:01 insert further down, which the partition
        // name formats as 2020-02-04T200000-...
        final AtomicInteger splitDirOpens = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, "2020-02-04T2000")) {
                    splitDirOpens.incrementAndGet();
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);

            executeWithRewriteTimestamp(
                    "CREATE TABLE x AS (" +
                            "SELECT" +
                            " cast(x AS int) i," +
                            " rpad(x::string, 200, 'q') s," +
                            " timestamp_sequence('2020-02-04T00', 60 * 1000000L)::#TIMESTAMP ts" +
                            " FROM long_sequence(1440)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL",
                    timestampType.getTypeName()
            );
            drainWalQueue();

            execute("ALTER TABLE x SET FORMAT PARQUET");
            drainWalQueue();

            // 2020-02-05 is born parquet, so the writer stays on native 2020-02-04.
            executeWithRewriteTimestamp(
                    "INSERT INTO x SELECT" +
                            " cast(x AS int) + 1440 i," +
                            " rpad((x + 1440)::string, 200, 'q') s," +
                            " timestamp_sequence('2020-02-05T00', 60 * 1000000L)::#TIMESTAMP ts" +
                            " FROM long_sequence(720)",
                    timestampType.getTypeName()
            );
            drainWalQueue();

            // O3 into the still-open 2020-02-04: split, then squash into the open partition.
            executeWithRewriteTimestamp(
                    "INSERT INTO x SELECT" +
                            " cast(x AS int) + 10000 i," +
                            " rpad((x + 10000)::string, 200, 'q') s," +
                            " timestamp_sequence('2020-02-04T20:01', 1000000L)::#TIMESTAMP ts" +
                            " FROM long_sequence(200)",
                    timestampType.getTypeName()
            );
            drainWalQueue();

            Assert.assertTrue(
                    "test setup gap: the O3 insert must SPLIT 2020-02-04 -- no partition directory"
                            + " named after the 2020-02-04T20:01 insert's split point was ever opened,"
                            + " so this test"
                            + " exercises a plain whole-partition rewrite, not a squash into the open"
                            + " partition",
                    splitDirOpens.get() > 0
            );

            // The precondition chain in one check: 2020-02-05 is born parquet -- which is why
            // openLastPartition() no-ops and the writer keeps NATIVE 2020-02-04 open -- and the
            // O3 insert's split of 2020-02-04 was squashed back in by the same commit, leaving a
            // single native 2020-02-04 with all 1440 + 200 rows and no split partition. Without
            // it the test degrades to "insert data, read it back" if the split or the squash
            // stops happening.
            assertQuery("SELECT minTimestamp, numRows, name, isParquet FROM table_partitions('x') ORDER BY minTimestamp")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("minTimestamp")
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname\tisParquet
                            2020-02-04T00:00:00.000000Z\t1640\t2020-02-04\tfalse
                            2020-02-05T00:00:00.000000Z\t720\t2020-02-05\ttrue
                            """, timestampType.getTypeName()));

            // The truncating close: doClose(true) -> freeColumns(true) -> closeAppendMemoryTruncate.
            engine.releaseInactive();

            assertQuery("SELECT count(), sum(length(s)) FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tsum\n2360\t472000\n");

            // The tail of the squashed partition's s.d, verbatim: a trim to the stale append
            // offset drops these bytes and a later append memory re-extends the file zero-filled.
            assertQuery("SELECT i, s FROM x WHERE ts < '2020-02-05' ORDER BY ts DESC LIMIT 1")
                    .noLeakCheck()
                    .returns("i\ts\n1440\t1440" + "q".repeat(196) + "\n");
        });
    }

    @Test
    public void testSquashIntoOpenPartitionBehindParquetLastPartitionPostingIndexed() throws Exception {
        // Same shape as testSquashIntoOpenPartitionBehindParquetLastPartition, with a POSTING
        // index on the squashed partition. The squash's reseal restores the table's indexers to
        // lastOpenPartitionTs, and a second O3 insert then re-enters the same branch, so the
        // writer's indexer list and column memories must still be consistent afterwards.
        //
        // Both O3 inserts land in the 2020-02-04T2x:xx range (20:01, then 21:01), so each split
        // shows up as a partition directory named after its split point -- one tick past the last
        // row that precedes the inserted range, so 2020-02-04T200000-... then 2020-02-04T210000-...
        // -- before the same commit squashes it away. table_partitions below cannot see that: a
        // whole-partition rewrite leaves an identical view. Count the directories instead.
        final AtomicInteger splitDirOpens = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, "2020-02-04T2")) {
                    splitDirOpens.incrementAndGet();
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);

            executeWithRewriteTimestamp(
                    "CREATE TABLE y (i INT, sym SYMBOL INDEX TYPE POSTING, s STRING, ts #TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY WAL",
                    timestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "INSERT INTO y SELECT" +
                            " cast(x AS int)," +
                            " 'k' || (x % 4)," +
                            " rpad(x::string, 200, 'q')," +
                            " timestamp_sequence('2020-02-04T00', 60 * 1000000L)::#TIMESTAMP" +
                            " FROM long_sequence(1440)",
                    timestampType.getTypeName()
            );
            drainWalQueue();

            execute("ALTER TABLE y SET FORMAT PARQUET");
            drainWalQueue();

            executeWithRewriteTimestamp(
                    "INSERT INTO y SELECT" +
                            " cast(x AS int) + 1440," +
                            " 'k' || (x % 4)," +
                            " rpad((x + 1440)::string, 200, 'q')," +
                            " timestamp_sequence('2020-02-05T00', 60 * 1000000L)::#TIMESTAMP" +
                            " FROM long_sequence(720)",
                    timestampType.getTypeName()
            );
            drainWalQueue();

            executeWithRewriteTimestamp(
                    "INSERT INTO y SELECT" +
                            " cast(x AS int) + 10000," +
                            " 'z1'," +
                            " rpad((x + 10000)::string, 200, 'q')," +
                            " timestamp_sequence('2020-02-04T20:01', 1000000L)::#TIMESTAMP" +
                            " FROM long_sequence(200)",
                    timestampType.getTypeName()
            );
            drainWalQueue();

            final int splitDirOpensAfterFirstO3 = splitDirOpens.get();
            Assert.assertTrue(
                    "test setup gap: the first O3 insert must SPLIT 2020-02-04 -- no partition"
                            + " directory named after the 2020-02-04T20:01 insert's split point was"
                            + " ever opened",
                    splitDirOpensAfterFirstO3 > 0
            );

            // A second O3 insert re-enters the same branch on a writer that already went
            // through it once.
            executeWithRewriteTimestamp(
                    "INSERT INTO y SELECT" +
                            " cast(x AS int) + 20000," +
                            " 'z2'," +
                            " rpad((x + 20000)::string, 200, 'q')," +
                            " timestamp_sequence('2020-02-04T21:01', 1000000L)::#TIMESTAMP" +
                            " FROM long_sequence(200)",
                    timestampType.getTypeName()
            );
            drainWalQueue();

            Assert.assertTrue(
                    "test setup gap: the second O3 insert must SPLIT 2020-02-04 again -- no partition"
                            + " directory named after the 2020-02-04T21:01 insert's split point was"
                            + " ever opened",
                    splitDirOpens.get() > splitDirOpensAfterFirstO3
            );

            // The precondition chain in one check: 2020-02-05 is born parquet -- which is why
            // openLastPartition() no-ops and the writer keeps NATIVE 2020-02-04 open -- and both
            // O3 inserts split 2020-02-04 and were squashed back in by their own commits, leaving
            // a single native 2020-02-04 with all 1440 + 200 + 200 rows and no split partition.
            assertQuery("SELECT minTimestamp, numRows, name, isParquet FROM table_partitions('y') ORDER BY minTimestamp")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("minTimestamp")
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname\tisParquet
                            2020-02-04T00:00:00.000000Z\t1840\t2020-02-04\tfalse
                            2020-02-05T00:00:00.000000Z\t720\t2020-02-05\ttrue
                            """, timestampType.getTypeName()));

            engine.releaseInactive();

            assertQuery("SELECT count(), sum(length(s)) FROM y")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tsum\n2560\t512000\n");

            assertQuery("SELECT sym, count() FROM y WHERE sym IN ('z1', 'z2') ORDER BY sym")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            sym\tcount
                            z1\t200
                            z2\t200
                            """);
        });
    }

    @Test
    public void testSquashIntoOpenPartitionReopensSquashTarget() throws Exception {
        // squashSplitPartitions appends into the partition the writer holds open through the
        // frame's own file descriptors, then drops the writer's now-stale append memories. It
        // must re-open that partition afterwards: the posting-index reseal that runs immediately
        // below it, and every later commit, expect live column memories and a dense indexer list
        // that matches indexCount.
        //
        // openLastPartition() cannot do that on this branch. The squash target is never the last
        // partition (the selection loop stops one short of it, and a partition survives after the
        // target whenever lastPartitionSquashed is false), and the last partition here is parquet
        // -- which is exactly why the writer holds an earlier partition open -- so
        // openLastPartitionAndSetAppendPosition returns without opening anything.
        //
        // The contract shows up in the file descriptors: after the squashing commit the writer
        // must still hold 2020-02-04's column files open, and it must hold them through a NEW
        // openRW. Merely still holding the fds the previous commit opened is what the writer does
        // when the reopen is missing entirely, so openedSinceMark -- the fds opened by the
        // squashing commit and still open when it returns -- is what discriminates. It counts
        // opens rather than comparing fd numbers: the OS is free to hand the same number back
        // after a close.
        final String targetDataFile = "2020-02-04" + Files.SEPARATOR + "i.d";
        final LongHashSet openTargetFds = new LongHashSet();
        final LongHashSet openedSinceMark = new LongHashSet();
        final AtomicInteger splitDirOpens = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean close(long fd) {
                synchronized (openTargetFds) {
                    openTargetFds.remove(fd);
                    openedSinceMark.remove(fd);
                }
                return super.close(fd);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                if (fd > -1 && Utf8s.endsWithAscii(name, targetDataFile)) {
                    synchronized (openTargetFds) {
                        openTargetFds.add(fd);
                        openedSinceMark.add(fd);
                    }
                }
                // The split of 2020-02-04 exists on disk only until the same commit squashes it
                // away, and table_partitions can only show the aftermath, so witness the directory
                // the O3 job names after the split point -- one tick past the last pre-existing row
                // before the 2020-02-04T20:01 insert, formatted as 2020-02-04T200000-...
                if (fd > -1 && Utf8s.containsAscii(name, "2020-02-04T2000")) {
                    splitDirOpens.incrementAndGet();
                }
                return fd;
            }
        };

        assertMemoryLeak(ff, () -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);

            executeWithRewriteTimestamp(
                    "CREATE TABLE x AS (" +
                            "SELECT" +
                            " cast(x AS int) i," +
                            " rpad(x::string, 200, 'q') s," +
                            " timestamp_sequence('2020-02-04T00', 60 * 1000000L)::#TIMESTAMP ts" +
                            " FROM long_sequence(1440)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL",
                    timestampType.getTypeName()
            );
            drainWalQueue();

            execute("ALTER TABLE x SET FORMAT PARQUET");
            drainWalQueue();

            // 2020-02-05 is born parquet, so the writer stays on native 2020-02-04.
            executeWithRewriteTimestamp(
                    "INSERT INTO x SELECT" +
                            " cast(x AS int) + 1440 i," +
                            " rpad((x + 1440)::string, 200, 'q') s," +
                            " timestamp_sequence('2020-02-05T00', 60 * 1000000L)::#TIMESTAMP ts" +
                            " FROM long_sequence(720)",
                    timestampType.getTypeName()
            );
            drainWalQueue();

            synchronized (openTargetFds) {
                openedSinceMark.clear();
            }

            // O3 into the still-open 2020-02-04: split, then squash into the open partition.
            executeWithRewriteTimestamp(
                    "INSERT INTO x SELECT" +
                            " cast(x AS int) + 10000 i," +
                            " rpad((x + 10000)::string, 200, 'q') s," +
                            " timestamp_sequence('2020-02-04T20:01', 1000000L)::#TIMESTAMP ts" +
                            " FROM long_sequence(200)",
                    timestampType.getTypeName()
            );
            drainWalQueue();

            synchronized (openTargetFds) {
                Assert.assertTrue(
                        "the writer must hold the squash target's column files open after squashing into it",
                        openTargetFds.size() > 0
                );
                Assert.assertTrue(
                        "the squashing commit must RE-open the squash target: every column file the"
                                + " writer holds open for 2020-02-04 was already open before the commit,"
                                + " so nothing closed and re-opened the partition",
                        openedSinceMark.size() > 0
                );
            }

            Assert.assertTrue(
                    "test setup gap: the O3 insert must SPLIT 2020-02-04 -- no partition directory"
                            + " named after the 2020-02-04T20:01 insert's split point was ever opened,"
                            + " so this test"
                            + " exercises a plain whole-partition rewrite, not a squash into the open"
                            + " partition",
                    splitDirOpens.get() > 0
            );

            // The precondition chain in one check: 2020-02-05 is born parquet -- which is why
            // openLastPartition() no-ops and the writer keeps NATIVE 2020-02-04 open -- and the
            // O3 insert's split of 2020-02-04 was squashed back in by the same commit, leaving a
            // single native 2020-02-04 with all 1440 + 200 rows and no split partition.
            assertQuery("SELECT minTimestamp, numRows, name, isParquet FROM table_partitions('x') ORDER BY minTimestamp")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("minTimestamp")
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname\tisParquet
                            2020-02-04T00:00:00.000000Z\t1640\t2020-02-04\tfalse
                            2020-02-05T00:00:00.000000Z\t720\t2020-02-05\ttrue
                            """, timestampType.getTypeName()));

            engine.releaseInactive();

            synchronized (openTargetFds) {
                Assert.assertEquals(
                        "releasing the writer must close every column file it held open",
                        0,
                        openTargetFds.size()
                );
            }
        });
    }

    @Test
    public void testSquashPartitionClearsRemoteAndStampsTarget() throws Exception {
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);

            executeWithRewriteTimestamp(
                    "CREATE TABLE x AS (" +
                            "SELECT" +
                            " cast(x AS int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) AS str," +
                            " timestamp_sequence('2020-02-03T00', 60 * 1000000L)::#TIMESTAMP ts" +
                            " FROM long_sequence(1000)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL",
                    timestampType.getTypeName()
            );
            drainWalQueue();

            executeWithRewriteTimestamp(
                    "INSERT INTO x " +
                            "SELECT" +
                            " cast(x AS int) * 1000000 i," +
                            " -x - 1000000L AS j," +
                            " rnd_str(5,16,2) AS str," +
                            " timestamp_sequence('2020-02-03T13:20', 1000000L)::#TIMESTAMP ts" +
                            " FROM long_sequence(200)",
                    timestampType.getTypeName()
            );
            drainWalQueue();

            final long lastWriteSeqTxn;
            try (TableWriter writer = getWriter("x")) {
                TxWriter tx = writer.getTxWriter();
                Assert.assertTrue("test setup must create a split partition", tx.getPartitionCount() > 1);
                Assert.assertFalse(tx.isPartitionParquet(0));
                lastWriteSeqTxn = tx.getSeqTxn();
                tx.setPartitionRemote(0, true);
                tx.setPartitionParquetGenerated(0, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }

            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();

            try (TableReader reader = getReader("x")) {
                Assert.assertEquals(1, reader.getTxFile().getPartitionCount());
                Assert.assertFalse(reader.getTxFile().isPartitionParquet(0));
                Assert.assertFalse("squash appends bytes into the target, so REMOTE must be cleared",
                        reader.getTxFile().isPartitionRemote(0));
                Assert.assertFalse("squash invalidates any staged parquet for the old bytes",
                        reader.getTxFile().isPartitionParquetGenerated(0));
                Assert.assertEquals("squash stamps max(merged sources) -- the last write's seqTxn, not the squash commit's",
                        lastWriteSeqTxn, reader.getTxFile().getNativePartitionSeqTxn(0));
                Assert.assertTrue("the squash commit advanced the table seqTxn past the stamp",
                        reader.getTxFile().getSeqTxn() > lastWriteSeqTxn);
            }

            assertQuery("SELECT count(), sum(j) FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tsum\n1200\t-200520600\n");
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
                assertQuery("select name, minTimestamp from table_partitions('x')")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns(replaceTimestampSuffix1("""
                                name\tminTimestamp
                                2020-02-03\t2020-02-03T13:00:00.000000Z
                                2020-02-04\t2020-02-04T00:00:00.000000Z
                                2020-02-04T230000-000001\t2020-02-04T23:01:00.000000Z
                                2020-02-05\t2020-02-05T00:00:00.000000Z
                                """, timestampType.getTypeName()));
            }

            // Another reader, should allow to squash partitions
            try (TableReader ignore = getReader("x")) {
                execute("insert into x(ts) values('2020-02-06')");
                assertQuery("select name, minTimestamp from table_partitions('x')")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns(replaceTimestampSuffix1("""
                                name\tminTimestamp
                                2020-02-03\t2020-02-03T13:00:00.000000Z
                                2020-02-04\t2020-02-04T00:00:00.000000Z
                                2020-02-05\t2020-02-05T00:00:00.000000Z
                                2020-02-06\t2020-02-06T00:00:00.000000Z
                                """, timestampType.getTypeName()));
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

                assertQuery(partitionsSql)
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns(replaceTimestampSuffix1("""
                                minTimestamp\tnumRows\tname
                                2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                                2020-02-04T20:01:00.000000Z\t319\t2020-02-04T200000-000001
                                """, timestampType.getTypeName()));
            }

            execute("alter table x add column k int");

            // Append in order to check last partition opened for writing correctly.
            execute(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T22:01', 1000000L) ts," +
                            " x + 2 as k" +
                            " from long_sequence(200)"
            );

            assertQuery(partitionsSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname
                            2020-02-04T00:00:00.000000Z\t1720\t2020-02-04
                            """, timestampType.getTypeName()));

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

            assertQuery("select minTimestamp, numRows, name from table_partitions('x')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(replaceTimestampSuffix1("""
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
                            """, timestampType.getTypeName()));
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
                assertQuery(partitionsSql)
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("minTimestamp\tnumRows\tname\n" +
                                replaceTimestampSuffix1("""
                                        2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                                        2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001
                                        2020-02-05T00:00:00.000000Z\t1320\t2020-02-05
                                        """, timestampType.getTypeName()));

                execute("alter table x force drop partition list '2020-02-04'",
                        sqlExecutionContext
                );
                drainWalQueue();

                // Partition "2020-02-04" cannot be squashed with the new update because it's locked by the reader
                assertQuery(partitionsSql)
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("minTimestamp\tnumRows\tname\n" +
                                replaceTimestampSuffix1("""
                                        2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001
                                        2020-02-05T00:00:00.000000Z\t1320\t2020-02-05
                                        """, timestampType.getTypeName()));

                // should squash partitions
                execute("alter table x squash partitions");

                drainWalQueue();
                assertQuery(partitionsSql)
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("minTimestamp\tnumRows\tname\n" +
                                replaceTimestampSuffix1("""
                                        2020-02-04T20:01:00.000000Z\t439\t2020-02-04
                                        2020-02-05T00:00:00.000000Z\t1320\t2020-02-05
                                        """, timestampType.getTypeName()));
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

    private int getSquashCount() {
        try (TableReader reader = getReader("x")) {
            long timestamp = MicrosTimestampDriver.floor("2020-02-04");
            int partitionIndex = reader.getPartitionIndexByTimestamp(timestamp);
            if (partitionIndex >= 0) {
                return reader.getTxFile().getPartitionSquashCount(partitionIndex);
            }
            return 0;
        } catch (NumericException e) {
            throw new RuntimeException(e);
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
            assertQuery(partitionsSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname
                            2020-02-04T20:01:00.000000Z\t200\t2020-02-04
                            2020-02-05T18:01:00.000000Z\t200\t2020-02-05
                            """, timestampType.getTypeName()));

            assertQuery("select count() from x;")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            400
                            """);
        });
    }

    private void testSquashPartitionsOnNonEmptyTable(String wal) throws Exception {
        assertMemoryLeak(() -> {
            // This test drives the pre-merge-append split-then-squash mechanism directly (prefix
            // split on size, then an explicit ALTER TABLE SQUASH PARTITIONS): with merge-append on,
            // the very same O3 write lands as a composite piece inside the existing partition
            // directory instead of a separate split directory, so the split/squash shape this test
            // asserts never forms. Same reasoning as testPartitionSquashCounterOverflow above.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
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
                assertQuery(partitionsSql)
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("minTimestamp\tnumRows\tname\n" +
                                replaceTimestampSuffix1("""
                                        2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                                        2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001
                                        2020-02-05T00:00:00.000000Z\t1320\t2020-02-05
                                        """, timestampType.getTypeName()));

                execute(sqlPrefix +
                                " timestamp_sequence('2020-02-05T18:01', 60*1000000L) ts" +
                                " from long_sequence(50)",
                        sqlExecutionContext
                );
                drainWalQueue();

                // Partition "2020-02-04" cannot be squashed with the new update because it's locked by the reader
                assertQuery(partitionsSql)
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("minTimestamp\tnumRows\tname\n" +
                                replaceTimestampSuffix1("""
                                        2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                                        2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001
                                        2020-02-05T00:00:00.000000Z\t1081\t2020-02-05
                                        2020-02-05T18:01:00.000000Z\t289\t2020-02-05T180000-000001
                                        """, timestampType.getTypeName()));

                // should squash partitions
                execute("alter table x squash partitions");

                drainWalQueue();
                assertQuery(partitionsSql)
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("minTimestamp\tnumRows\tname\n" +
                                replaceTimestampSuffix1("""
                                        2020-02-04T00:00:00.000000Z\t1640\t2020-02-04
                                        2020-02-05T00:00:00.000000Z\t1370\t2020-02-05
                                        """, timestampType.getTypeName()));

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

                assertQuery("select count() from x;")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("count\n" +
                                (60 * (23 * 2) + 450) + "\n");
            }
        });
    }
}
