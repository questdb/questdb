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
import io.questdb.std.FilesFacade;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
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
    public void testSplitMergeIntoHardlinkSuffixChildVarchar() throws Exception {
        // Reproduces WalWriterFuzzTest#testWalAddRemoveCommitFuzzO3 (fuzzer seeds
        // 1127317073217125L, 1783074480428L, POSTING symbol index): an O3 merge into a
        // hardlinked suffix child over a VARCHAR (var-size) column reads the child's DATA
        // prefix through an aux vector based at the child's logical row 0, which is a
        // non-zero file row of the shared donor file. VarcharTypeDriver/ArrayTypeDriver
        // getDataVectorSize(aux, 0, hi) assumed row 0's data offset is 0 and returned the
        // absolute end offset instead of the true byte span, so O3CopyJob read past the
        // mapped source region and failed with EFAULT ("cannot copy var data column prefix"),
        // suspending the table. The bug bites only var-size columns whose donor prefix carries
        // real (non-inlined) data before the child, so the VARCHAR values below are all > 9
        // bytes to keep them out of the inlined aux slot.
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            overrides.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 3);
            overrides.setProperty(PropertyKey.CAIRO_PARTITION_TOP_WAL_ENABLED, "true");

            String tsType = timestampType.getTypeName();
            // 60*36 minute ticks span 2020-02-04 (1440 rows) and 2020-02-05 (720 rows),
            // so 2020-02-04 is a mid partition. All data is deterministic so the oracle
            // can rebuild it at query time.
            String baseSelect = "SELECT" +
                    " cast(x AS int) i," +
                    " ('varchar_base_value_' || x)::varchar vc," +
                    " ('2020-02-04'::timestamp + (x - 1) * 60 * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(60 * 36)";
            // O3 insert at 2020-02-04T20:01 creates a zero-copy 3-way split inside the mid
            // partition: prefix donor + merged middle + hardlinked suffix child at
            // 2020-02-04T20:05 with partitionTop > 0.
            String o3Select1 = "SELECT" +
                    " cast(x AS int) * 1000000 i," +
                    " ('varchar_o3_first_value_' || x)::varchar vc," +
                    " ('2020-02-04T20:01'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(200)";
            // O3 insert strictly inside the suffix child's range (20:05..23:59): merges into
            // the hardlinked child. Its min timestamp is after the child's first rows, so
            // those child rows form a DATA prefix (rowLo == 0) - the failing copy.
            String o3Select2 = "SELECT" +
                    " cast(x AS int) * 2000000 i," +
                    " ('varchar_o3_second_value_' || x)::varchar vc," +
                    " ('2020-02-04T21:00:30'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(10)";

            execute("CREATE TABLE x AS (" + baseSelect + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("INSERT INTO x " + o3Select1);
            drainWalQueue();

            assertQuery("SELECT minTimestamp, numRows, name FROM table_partitions('x') ORDER BY minTimestamp")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("minTimestamp")
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname
                            2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                            2020-02-04T20:01:00.000000Z\t204\t2020-02-04T200000-000001
                            2020-02-04T20:05:00.000000Z\t235\t2020-02-04T200500
                            2020-02-05T00:00:00.000000Z\t720\t2020-02-05
                            """, timestampType.getTypeName()));

            execute("INSERT INTO x " + o3Select2);
            drainWalQueue();

            Assert.assertFalse(
                    "table suspended: the suffix-child VARCHAR merge failed",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(" + baseSelect + " UNION ALL " + o3Select1 + " UNION ALL " + o3Select2 + ") ORDER BY ts",
                    "x",
                    LOG,
                    true
            );
        });
    }

    @Test
    public void testSplitMergeIntoHardlinkSuffixChild() throws Exception {
        // Reproduces WalWriterFuzzTest#testWalMetadataAddDeleteColumnHeavy fuzz failure
        // (seeds 1526190694382147614L, 2684230139848143701L): an O3 merge into a hardlinked
        // suffix child must read source column data at file_row = logical_row + partitionTop.
        // Without the +P offset the merge copies the donor's first rows instead of the
        // child's rows.
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            overrides.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 3);

            String tsType = timestampType.getTypeName();
            // 60*36 minute ticks span 2020-02-04 (1440 rows) and 2020-02-05 (720 rows),
            // so 2020-02-04 is a mid partition (another logical partition follows it).
            // All data is deterministic so the oracle can rebuild it at query time.
            String baseSelect = "SELECT" +
                    " cast(x AS int) i," +
                    " -x j," +
                    " 's' || x AS str," +
                    " ('2020-02-04'::timestamp + (x - 1) * 60 * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(60 * 36)";
            // O3 insert at 2020-02-04T20:01 creates a zero-copy 3-way split inside the mid partition:
            // prefix donor + merged middle + hardlinked suffix child at 2020-02-04T20:05 with
            // 235 logical rows and partitionTop = 1440 - 235 = 1205.
            String o3Select1 = "SELECT" +
                    " cast(x AS int) * 1000000 i," +
                    " -x - 1000000L AS j," +
                    " 'a' || x AS str," +
                    " ('2020-02-04T20:01'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(200)";
            // O3 insert strictly inside the suffix child's range (20:05..23:59): merges into
            // the hardlinked child, exercising the +partitionTop source reads.
            String o3Select2 = "SELECT" +
                    " cast(x AS int) * 2000000 i," +
                    " -x - 2000000L AS j," +
                    " 'b' || x AS str," +
                    " ('2020-02-04T21:00:30'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(10)";

            execute("CREATE TABLE x AS (" + baseSelect + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x " + o3Select1);

            assertQuery("SELECT minTimestamp, numRows, name FROM table_partitions('x') ORDER BY minTimestamp")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("minTimestamp")
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname
                            2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                            2020-02-04T20:01:00.000000Z\t204\t2020-02-04T200000-000001
                            2020-02-04T20:05:00.000000Z\t235\t2020-02-04T200500
                            2020-02-05T00:00:00.000000Z\t720\t2020-02-05
                            """, timestampType.getTypeName()));

            execute("INSERT INTO x " + o3Select2);

            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(" + baseSelect + " UNION ALL " + o3Select1 + " UNION ALL " + o3Select2 + ") ORDER BY ts",
                    "x",
                    LOG,
                    true
            );
        });
    }

    @Test
    public void testSplitAppendIntoHardlinkSuffixChildAsLast() throws Exception {
        // A 3-way hardlink split of the LAST partition leaves the suffix child as the new last
        // (transient) partition. Subsequent commits append into it through the writer-inline
        // appendLastPartition path (writer-mapped column memory), which must keep the
        // file_row = logical + partitionTop addressing: an in-place tail append lands at the
        // true tail of the shared donor files.
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            overrides.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 3);

            String tsType = timestampType.getTypeName();
            // Single DAY partition 2020-02-04 (1440 minute rows) - the LAST partition.
            String baseSelect = "SELECT" +
                    " cast(x AS int) i," +
                    " -x j," +
                    " 's' || x AS str," +
                    " ('2020-02-04'::timestamp + (x - 1) * 60 * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(60 * 24)";
            execute("CREATE TABLE x AS (" + baseSelect + ") TIMESTAMP(ts) PARTITION BY DAY");

            // O3 insert at 20:01 splits the last partition 3-way: prefix donor + merged middle +
            // hardlinked suffix child, which becomes the new last partition.
            String o3Select1 = "SELECT" +
                    " cast(x AS int) * 1000000 i," +
                    " -x - 1000000L AS j," +
                    " 'a' || x AS str," +
                    " ('2020-02-04T20:01'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(200)";
            execute("INSERT INTO x " + o3Select1);

            // O3 append into the child-as-last: rows strictly after the child's max timestamp
            // (23:59), committed as one O3 batch (append=true path).
            // NOTE: use timestamp + long, not timestamp - long: subtraction degrades to LONG
            // and the ::tsType cast then misinterprets the epoch scale under TIMESTAMP_NS.
            String o3Select2 = "SELECT" +
                    " cast(x AS int) * 2000000 i," +
                    " -x - 2000000L AS j," +
                    " 'b' || x AS str," +
                    " ('2020-02-04T23:59:30'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(20)";
            execute("INSERT INTO x " + o3Select2);

            String oracle = "(" + baseSelect + " UNION ALL " + o3Select1 + " UNION ALL " + o3Select2 + ") ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracle, "x", LOG, true);

            // Filtered scan takes the page-frame + JIT path with per-column frame addressing.
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT * FROM (" + oracle + ") WHERE j < -2000000",
                    "SELECT * FROM x WHERE j < -2000000",
                    LOG,
                    true
            );

            // Plain in-order row append to the child-as-last (non-O3 writer append path).
            String appendSelect = "SELECT" +
                    " cast(x AS int) * 3000000 i," +
                    " -x - 3000000L AS j," +
                    " 'c' || x AS str," +
                    " ('2020-02-04T23:59:51'::timestamp + (x - 1) * 100000L)::" + tsType + " ts" +
                    " FROM long_sequence(5)";
            execute("INSERT INTO x " + appendSelect);

            String oracle2 = "(" + baseSelect
                    + " UNION ALL " + o3Select1
                    + " UNION ALL " + o3Select2
                    + " UNION ALL " + appendSelect
                    + ") ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracle2, "x", LOG, true);
        });
    }

    @Test
    public void testSplitHardlinkSuffixChildIndexedSymbolScan() throws Exception {
        // After a 3-way hardlink split, index-backed scans over the split family (donor prefix,
        // merged middle, suffix child) must agree with the column data: the child's rebuilt
        // index holds PHYSICAL row ids [P, P + rows) and the reader shifts by the per-column
        // partition top. Covers the null key, a value key, forward and backward scans, a
        // column rename, and an append into the child-as-last.
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            overrides.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 3);
            overrides.setProperty(PropertyKey.CAIRO_PARTITION_TOP_WAL_ENABLED, "true");

            String tsType = timestampType.getTypeName();
            String baseSelect = "SELECT" +
                    " cast(x AS int) i," +
                    " CASE WHEN x % 3 = 0 THEN NULL ELSE 'sy' || (x % 5) END::SYMBOL sym," +
                    " ('2020-02-04'::timestamp + (x - 1) * 60 * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(60 * 24)";
            for (String indexType : new String[]{"BITMAP", "POSTING"}) {
                execute("DROP TABLE IF EXISTS x");
                drainWalQueue();
                execute("CREATE TABLE x AS (" + baseSelect + "), INDEX(sym TYPE " + indexType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
                drainWalQueue();

                // O3 insert at 20:01 splits the last partition 3-way; the hardlinked suffix child
                // becomes the new last partition and gets fresh independent indexes.
                String o3Select1 = "SELECT" +
                        " cast(x AS int) * 1000000 i," +
                        " CASE WHEN x % 4 = 0 THEN NULL ELSE 'sy' || (x % 5) END::SYMBOL sym," +
                        " ('2020-02-04T20:01'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts" +
                        " FROM long_sequence(200)";
                execute("INSERT INTO x " + o3Select1);
                drainWalQueue();

                execute("ALTER TABLE x RENAME COLUMN sym TO sym_r");
                drainWalQueue();
                String baseSelectRenamed = baseSelect.replace(" sym,", " sym_r,");
                String o3Select1Renamed = o3Select1.replace(" sym,", " sym_r,");

                // O3 append into the child-as-last, updating the child's index in place. The row
                // count must be large enough that the index append allocates NEW value blocks
                // beyond the build-time .v size (a block holds 256 row ids per key).
                // NOTE: use timestamp + long, not timestamp - long: subtraction degrades to LONG
                // and the ::tsType cast then misinterprets the epoch scale under TIMESTAMP_NS.
                String o3Select2 = "SELECT" +
                        " cast(x AS int) * 2000000 i," +
                        " CASE WHEN x % 2 = 0 THEN NULL ELSE 'sy' || (x % 5) END::SYMBOL sym_r," +
                        " ('2020-02-04T23:59:30'::timestamp + (x - 1) * 10000L)::" + tsType + " ts" +
                        " FROM long_sequence(2_000)";
                execute("INSERT INTO x " + o3Select2);
                drainWalQueue();

                // Close the writer: the lazily-kept split-child reindexer must not truncate the
                // child's .k/.v to its stale build-time size, chopping the appended index blocks.
                engine.releaseInactive();

                String oracle = "(" + baseSelectRenamed + " UNION ALL " + o3Select1Renamed + " UNION ALL " + o3Select2 + ") ORDER BY ts";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracle, "x", LOG, true);

                for (String filter : new String[]{"sym_r = null", "sym_r = 'sy1'"}) {
                    TestUtils.assertSqlCursors(
                            engine,
                            sqlExecutionContext,
                            "SELECT * FROM (" + oracle + ") WHERE " + filter + " ORDER BY ts",
                            "SELECT * FROM x WHERE " + filter + " ORDER BY ts",
                            LOG,
                            true
                    );
                    TestUtils.assertSqlCursors(
                            engine,
                            sqlExecutionContext,
                            "SELECT * FROM (" + oracle + ") WHERE " + filter + " ORDER BY ts DESC",
                            "SELECT * FROM x WHERE " + filter + " ORDER BY ts DESC",
                            LOG,
                            true
                    );
                }
            }
        });
    }

    @Test
    public void testSplitHardlinkSuffixChildColumnTypeConversion() throws Exception {
        // Reproduces WalWriterFuzzTest#testWalMetadataChangeHeavy fuzz failure: a zero-copy
        // split suffix child hardlinks the donor's column files, so a shared column's rows
        // live at file_row = logical + partitionTop - columnTop. ALTER COLUMN TYPE rewrites
        // the column into new child-local files via ConvertOperatorImpl, which reads the
        // source at file row 0 (donor rows, not the child's) and writes the destination at
        // file row 0. The new column also inherits the old column's top-partition timestamp,
        // so reader and writer keep applying the +partitionTop shift to the 0-based file and
        // read zeros where the converted values should be.
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            overrides.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 3);
            overrides.setProperty(PropertyKey.CAIRO_PARTITION_TOP_WAL_ENABLED, "true");

            String tsType = timestampType.getTypeName();
            String baseSelect = "SELECT" +
                    " cast(x AS int) i," +
                    " 's' || x AS str," +
                    " ('2020-02-04'::timestamp + (x - 1) * 60 * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(60 * 24)";
            execute("CREATE TABLE x AS (" + baseSelect + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // O3 insert at 20:01 splits the last partition 3-way: prefix donor + merged middle +
            // hardlinked suffix child, which becomes the new last partition.
            String o3Select = "SELECT" +
                    " cast(x AS int) * 1000000 i," +
                    " 'a' || x AS str," +
                    " ('2020-02-04T20:01'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(200)";
            execute("INSERT INTO x " + o3Select);
            drainWalQueue();

            // Convert the donor-shared column in place; the child partition's rows must survive.
            execute("ALTER TABLE x ALTER COLUMN i TYPE DOUBLE");
            drainWalQueue();

            String oracle = "SELECT i::double i, str, ts FROM (" + baseSelect + " UNION ALL " + o3Select + ") ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracle, "x", LOG, true);

            // Var-size conversion over the same child (STRING -> VARCHAR).
            execute("ALTER TABLE x ALTER COLUMN str TYPE VARCHAR");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracle, "x", LOG, true);

            // Append after the conversion: the writer's append position must agree with the
            // converted file's layout.
            String appendSelect = "SELECT" +
                    " cast(x AS int) * 2000000 i," +
                    " 'c' || x AS str," +
                    " ('2020-02-04T23:59:30'::timestamp + (x - 1) * 10000L)::" + tsType + " ts" +
                    " FROM long_sequence(100)";
            execute("INSERT INTO x " + appendSelect);
            drainWalQueue();

            String oracle2 = "SELECT i::double i, str, ts FROM (" + baseSelect
                    + " UNION ALL " + o3Select
                    + " UNION ALL " + appendSelect
                    + ") ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracle2, "x", LOG, true);

            // Var-to-symbol conversion after the append.
            execute("ALTER TABLE x ALTER COLUMN str TYPE SYMBOL");
            drainWalQueue();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracle2, "x", LOG, true);

            // A fresh reader must see the same data.
            engine.releaseInactive();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracle2, "x", LOG, true);
        });
    }

    @Test
    public void testSplitHardlinkSuffixChildDropPartition() throws Exception {
        // Reproduces WalWriterFuzzTest#testWalAddRemoveCommitFuzzO3 fuzz failure: DROP PARTITION
        // recomputes the table min/max timestamps by reading the head/tail of a neighbouring
        // partition's timestamp file. On a zero-copy split suffix child logical row 0 lives at
        // file row partitionTop of the shared donor file, so a read at file row 0 returns DONOR
        // timestamps: dropping the active partition then fails the partition-name sanity check
        // ("invalid timestamp data in detached partition") and suspends the WAL table, and
        // dropping the first partition records a stale donor min timestamp in _txn.
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            overrides.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 3);
            // keep the 3-piece split when the day stops being the last partition - otherwise the
            // rollover squash folds the suffix child back into the merged middle
            overrides.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 3);
            overrides.setProperty(PropertyKey.CAIRO_PARTITION_TOP_WAL_ENABLED, "true");

            String tsType = timestampType.getTypeName();
            String baseSelect = "SELECT" +
                    " cast(x AS int) i," +
                    " -x j," +
                    " 's' || x AS str," +
                    " ('2020-02-04'::timestamp + (x - 1) * 60 * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(60 * 24)";
            execute("CREATE TABLE x AS (" + baseSelect + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // O3 insert at 20:01 splits the last partition 3-way: prefix donor + merged middle +
            // hardlinked suffix child (floor 20:05, partitionTop 1205).
            String o3Select = "SELECT" +
                    " cast(x AS int) * 1000000 i," +
                    " -x - 1000000L AS j," +
                    " 'a' || x AS str," +
                    " ('2020-02-04T20:01'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(200)";
            execute("INSERT INTO x " + o3Select);
            drainWalQueue();

            // In-order rows the next day so the suffix child is no longer the last partition.
            String nextDaySelect = "SELECT" +
                    " cast(x AS int) * 2000000 i," +
                    " -x - 2000000L AS j," +
                    " 'b' || x AS str," +
                    " ('2020-02-05T01:00'::timestamp + (x - 1) * 60 * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(100)";
            execute("INSERT INTO x " + nextDaySelect);
            drainWalQueue();

            assertQuery("SELECT minTimestamp, numRows FROM table_partitions('x') ORDER BY minTimestamp")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("minTimestamp")
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows
                            2020-02-04T00:00:00.000000Z\t1201
                            2020-02-04T20:01:00.000000Z\t204
                            2020-02-04T20:05:00.000000Z\t235
                            2020-02-05T01:00:00.000000Z\t100
                            """, tsType));

            // Dropping the active partition recomputes the new max timestamp from the tail of
            // the preceding partition - the suffix child - through the shared-donor-file shift.
            execute("ALTER TABLE x DROP PARTITION LIST '2020-02-05'");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            String oracle = "(" + baseSelect + " UNION ALL " + o3Select + ") ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracle, "x", LOG, true);

            // New rows two days later so the split day is not the last logical partition.
            String lastDaySelect = "SELECT" +
                    " cast(x AS int) * 3000000 i," +
                    " -x - 3000000L AS j," +
                    " 'c' || x AS str," +
                    " ('2020-02-06T01:00'::timestamp + (x - 1) * 60 * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(100)";
            execute("INSERT INTO x " + lastDaySelect);
            drainWalQueue();

            // Dropping the split day removes its pieces one at a time; each drop of the first
            // remaining piece recomputes the table min from the head of the next piece, and for
            // the suffix child that read goes through the shared-donor-file shift too. The drop
            // also purges the donor directory while the child hardlinks its files.
            execute("ALTER TABLE x DROP PARTITION LIST '2020-02-04'");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "(" + lastDaySelect + ") ORDER BY ts", "x", LOG, true);
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                Assert.assertEquals(
                        timestampType.getDriver().parseFloorLiteral("2020-02-06T01:00"),
                        reader.getMinTimestamp()
                );
            }
        });
    }

    @Test
    public void testSplitHardlinkSuffixChildDropPartitionLateColumn() throws Exception {
        // Reproduces WalWriterFuzzTest#testWalAddRemoveCommitFuzzO3: a column added after the
        // table was created is absent in an older DAY partition. That day is split 3-way, so the
        // hardlinked suffix child stores the column's top in the shared donor file frame
        // (logicalTop + partitionTop). DROP PARTITION of the following day makes the child the new
        // last partition; dropPartitionByExactTimestamp then repositions the append cursor onto it
        // and setColumnAppendPosition computes file_row = size + partitionTop - columnTop. The
        // column's initial-partition record was reclassified onto the child by
        // replaceInitialPartitionRecords (its add partition was dropped), which drops the +P shift
        // on read/append, so the file-frame top sticking out past the child size yields a negative
        // append offset and an assertion in MemoryPARWImpl.jumpTo that suspends the WAL table.
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            overrides.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 3);
            // keep the 3-piece split once the day stops being the last partition
            overrides.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 3);
            overrides.setProperty(PropertyKey.CAIRO_PARTITION_TOP_WAL_ENABLED, "true");

            String tsType = timestampType.getTypeName();
            // day1 2020-02-04 (1440 rows) + day2 2020-02-05 (100 rows), created in one statement.
            // Oracle column order matches x after the ALTERs: i, j, str, ts, late_long, late_var, late_sym.
            String baseSelect = "SELECT" +
                    " cast(x AS int) i," +
                    " -x j," +
                    " 's' || x AS str," +
                    " ('2020-02-04'::timestamp + (x - 1) * 60 * 1000000L)::" + tsType + " ts," +
                    " cast(NULL AS long) late_long," +
                    " cast(NULL AS varchar) late_var," +
                    " cast(NULL AS symbol) late_sym" +
                    " FROM long_sequence(60 * 24 + 100)";
            execute("CREATE TABLE x AS (" +
                    " SELECT cast(x AS int) i, -x j, 's' || x AS str," +
                    " ('2020-02-04'::timestamp + (x - 1) * 60 * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(60 * 24 + 100)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // Late-added columns: absent in every existing (day1 + day2) row; their initial
            // partition is day2, the last partition at add time.
            execute("ALTER TABLE x ADD COLUMN late_long long");
            execute("ALTER TABLE x ADD COLUMN late_var varchar");
            execute("ALTER TABLE x ADD COLUMN late_sym symbol index");
            drainWalQueue();

            // O3 insert at 20:01 splits day1 3-way: prefix donor + merged middle + hardlink suffix
            // child (floor 20:05, partitionTop 1205). The O3 rows carry the late column values, so
            // the split writes explicit column-top records for the child.
            String o3Select = "SELECT" +
                    " cast(x AS int) * 1000000 i," +
                    " -x - 1000000L AS j," +
                    " 'a' || x AS str," +
                    " ('2020-02-04T20:01'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts," +
                    " x AS late_long," +
                    " ('v' || x)::varchar AS late_var," +
                    " ('y' || x)::symbol AS late_sym" +
                    " FROM long_sequence(200)";
            execute("INSERT INTO x " + o3Select);
            drainWalQueue();

            assertQuery("SELECT minTimestamp, numRows FROM table_partitions('x') ORDER BY minTimestamp")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("minTimestamp")
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows
                            2020-02-04T00:00:00.000000Z\t1201
                            2020-02-04T20:01:00.000000Z\t204
                            2020-02-04T20:05:00.000000Z\t235
                            2020-02-05T00:00:00.000000Z\t100
                            """, tsType));

            // Drop day2 -> the suffix child becomes the new last partition. Before the fix this
            // suspended the table with an AssertionError in setColumnAppendPosition.
            execute("ALTER TABLE x DROP PARTITION LIST '2020-02-05'");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            // Only day1 (base rows with null late columns) plus the O3 rows remain.
            String day1Inner = "SELECT * FROM (" + baseSelect + ") WHERE ts < '2020-02-05' UNION ALL " + o3Select;
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "(" + day1Inner + ") ORDER BY ts", "x", LOG, true);

            // A follow-up append into the child (still the last partition) must land at the right
            // file offset for the reclassified late columns.
            execute("INSERT INTO x(i,j,str,late_long,late_var,late_sym,ts)" +
                    " SELECT 7, -7, 'z', 77, 'w'::varchar, 'q'::symbol, '2020-02-04T23:59:30'::" + tsType);
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            String appendOracle = "(" + day1Inner +
                    " UNION ALL SELECT 7 i, cast(-7 AS long) j, 'z' str," +
                    " '2020-02-04T23:59:30'::" + tsType + " ts, cast(77 AS long) late_long," +
                    " 'w'::varchar late_var, 'q'::symbol late_sym FROM long_sequence(1)) ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, appendOracle, "x", LOG, true);

            // Drop the split day too: its pieces are removed one at a time and the prefix becomes
            // last during the sequence.
            execute("ALTER TABLE x DROP PARTITION LIST '2020-02-04'");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertQuery("SELECT count() FROM x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n0\n");
        });
    }

    @Test
    public void testSplitHardlinkSuffixChildUpdate() throws Exception {
        // UPDATE rewrites the affected column into new child-local files, like ALTER COLUMN
        // TYPE does. On a zero-copy split suffix child the source column is donor-shared
        // (rows at file_row = logical + partitionTop - columnTop), so the row copy must read
        // through the shift and the rewritten file's column top must follow the shared-file
        // convention.
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            overrides.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 3);
            overrides.setProperty(PropertyKey.CAIRO_PARTITION_TOP_WAL_ENABLED, "true");

            String tsType = timestampType.getTypeName();
            String baseSelect = "SELECT" +
                    " cast(x AS int) i," +
                    " 's' || x AS str," +
                    " CASE WHEN x % 3 = 0 THEN NULL ELSE 'sy' || (x % 5) END::SYMBOL sym," +
                    " ('2020-02-04'::timestamp + (x - 1) * 60 * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(60 * 24)";
            execute("CREATE TABLE x AS (" + baseSelect + "), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // O3 insert at 20:01 splits the last partition 3-way: prefix donor + merged middle +
            // hardlinked suffix child, which becomes the new last partition.
            String o3Select = "SELECT" +
                    " cast(x AS int) * 1000000 i," +
                    " 'a' || x AS str," +
                    " CASE WHEN x % 4 = 0 THEN NULL ELSE 'sy' || (x % 5) END::SYMBOL sym," +
                    " ('2020-02-04T20:01'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(200)";
            execute("INSERT INTO x " + o3Select);
            drainWalQueue();

            // The range spans the merged middle and the suffix child. Update a fixed-size,
            // a var-size and an indexed symbol column in one statement.
            String updateRange = "ts BETWEEN '2020-02-04T19:00' AND '2020-02-04T22:00'";
            execute("UPDATE x SET i = i + 10_000_000, str = 'u' || str, sym = 'up' WHERE " + updateRange);
            drainWalQueue();

            String oracle = "SELECT" +
                    " CASE WHEN " + updateRange + " THEN i + 10_000_000 ELSE i END i," +
                    " CASE WHEN " + updateRange + " THEN 'u' || str ELSE str END str," +
                    " CASE WHEN " + updateRange + " THEN 'up' ELSE sym END sym," +
                    " ts FROM ("
                    + baseSelect + " UNION ALL " + o3Select + ") ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracle, "x", LOG, true);

            // Index-backed scans over the rebuilt child index must agree with the column data.
            for (String filter : new String[]{"sym = 'up'", "sym = 'sy1'", "sym = null"}) {
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "SELECT * FROM (" + oracle + ") WHERE " + filter + " ORDER BY ts",
                        "SELECT * FROM x WHERE " + filter + " ORDER BY ts",
                        LOG,
                        true
                );
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "SELECT * FROM (" + oracle + ") WHERE " + filter + " ORDER BY ts DESC",
                        "SELECT * FROM x WHERE " + filter + " ORDER BY ts DESC",
                        LOG,
                        true
                );
            }

            // A fresh reader must see the same data.
            engine.releaseInactive();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracle, "x", LOG, true);
        });
    }

    @Test
    public void testSplitHardlinkSuffixChildWalLagSortMerge() throws Exception {
        // Reproduces WalWriterFuzzTest#testWalMetadataAddDeleteColumnHeavy fuzz failure: an
        // in-order WAL txn stashed to LAG is physically appended to the last partition at
        // file_row = logical + partitionTop (a zero-copy suffix child shares the donor file).
        // Applying the next O3 txn merge-sorts the LAG back out of the partition files; a read
        // at the logical offset returns DONOR rows instead of the LAG rows, scattering phantom
        // timestamps into earlier partitions and desyncing the tracked max timestamp from the
        // last partition's real tail (which later fires the suffixLo <= suffixHi assert in
        // O3PartitionJob branch 2).
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            overrides.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 3);
            overrides.setProperty(PropertyKey.CAIRO_PARTITION_TOP_WAL_ENABLED, "true");
            overrides.setProperty(PropertyKey.CAIRO_WAL_SQUASH_UNCOMMITTED_ROWS_MULTIPLIER, 1);

            String tsType = timestampType.getTypeName();
            String baseSelect = "SELECT" +
                    " cast(x AS int) i," +
                    " -x j," +
                    " 's' || x AS str," +
                    " ('2020-02-04'::timestamp + (x - 1) * 60 * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(60 * 24)";
            execute("CREATE TABLE x AS (" + baseSelect + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            // walMaxLagRows = multiplier(1) * maxUncommittedRows(25) = 25: the 20-row in-order
            // txn fits the LAG alone, but together with the 10-row O3 txn exceeds it, so the
            // block calculator applies the in-order txn on its own with commitToTimestamp
            // limited to the O3 txn's min - all 20 rows stash to LAG in the child's files.
            execute("ALTER TABLE x SET PARAM maxUncommittedRows = 25");
            drainWalQueue();

            // O3 insert at 20:01 splits the last partition 3-way: prefix donor + merged middle +
            // hardlinked suffix child, which becomes the new last partition.
            String o3Select1 = "SELECT" +
                    " cast(x AS int) * 1000000 i," +
                    " -x - 1000000L AS j," +
                    " 'a' || x AS str," +
                    " ('2020-02-04T20:01'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(200)";
            execute("INSERT INTO x " + o3Select1);
            drainWalQueue();

            assertQuery("SELECT minTimestamp, numRows FROM table_partitions('x') ORDER BY minTimestamp")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("minTimestamp")
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows
                            2020-02-04T00:00:00.000000Z\t1201
                            2020-02-04T20:01:00.000000Z\t204
                            2020-02-04T20:05:00.000000Z\t235
                            """, timestampType.getTypeName()));

            // In-order txn: rows after the table max (23:59:00), stashed as LAG into the
            // child-as-last partition's shared donor files.
            String lagSelect = "SELECT" +
                    " cast(x AS int) * 3000000 i," +
                    " -x - 3000000L AS j," +
                    " 'c' || x AS str," +
                    " ('2020-02-04T23:59:30'::timestamp + (x - 1) * 100000L)::" + tsType + " ts" +
                    " FROM long_sequence(20)";
            execute("INSERT INTO x " + lagSelect);
            // O3 txn: rows inside the child's range; applying it merge-sorts the LAG back out of
            // the shared donor file, which must read at file_row = logical + partitionTop.
            String o3Select2 = "SELECT" +
                    " cast(x AS int) * 4000000 i," +
                    " -x - 4000000L AS j," +
                    " 'd' || x AS str," +
                    " ('2020-02-04T21:00:30.500000'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(10)";
            execute("INSERT INTO x " + o3Select2);
            // single drain: the in-order txn stashes to LAG, the O3 txn merges with it
            drainWalQueue();

            String oracle = "(" + baseSelect
                    + " UNION ALL " + o3Select1
                    + " UNION ALL " + lagSelect
                    + " UNION ALL " + o3Select2
                    + ") ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracle, "x", LOG, true);
        });
    }

    @Test
    public void testSplitHardlinkSuffixChildWalLagSortMergeDedup() throws Exception {
        // Same LAG-over-hardlink-child flow as testSplitHardlinkSuffixChildWalLagSortMerge, but
        // on a DEDUP table: deduplicateSortedIndex maps the LAG rows of every dedup key column
        // (fixed and var-size) straight from the last partition's files and must read them at
        // file_row = logical + partitionTop of the shared donor file.
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            overrides.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 3);
            overrides.setProperty(PropertyKey.CAIRO_PARTITION_TOP_WAL_ENABLED, "true");
            overrides.setProperty(PropertyKey.CAIRO_WAL_SQUASH_UNCOMMITTED_ROWS_MULTIPLIER, 1);

            String tsType = timestampType.getTypeName();
            String baseSelect = "SELECT" +
                    " cast(x AS int) i," +
                    " -x j," +
                    " ('s' || x)::varchar AS str," +
                    " ('2020-02-04'::timestamp + (x - 1) * 60 * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(60 * 24)";
            execute("CREATE TABLE x AS (" + baseSelect + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("ALTER TABLE x SET PARAM maxUncommittedRows = 25");
            // dedup keys beyond the timestamp force the LAG key-column mapping; all row
            // timestamps are unique, so deduplication must not drop anything.
            execute("ALTER TABLE x DEDUP ENABLE UPSERT KEYS(ts, i, str)");
            drainWalQueue();

            // +500ms offset keeps the O3 timestamps distinct from the minute-aligned base rows
            String o3Select1 = "SELECT" +
                    " cast(x AS int) * 1000000 i," +
                    " -x - 1000000L AS j," +
                    " ('a' || x)::varchar AS str," +
                    " ('2020-02-04T20:01:00.500000'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(200)";
            execute("INSERT INTO x " + o3Select1);
            drainWalQueue();

            String lagSelect = "SELECT" +
                    " cast(x AS int) * 3000000 i," +
                    " -x - 3000000L AS j," +
                    " ('c' || x)::varchar AS str," +
                    " ('2020-02-04T23:59:30'::timestamp + (x - 1) * 100000L)::" + tsType + " ts" +
                    " FROM long_sequence(20)";
            execute("INSERT INTO x " + lagSelect);
            String o3Select2 = "SELECT" +
                    " cast(x AS int) * 4000000 i," +
                    " -x - 4000000L AS j," +
                    " ('d' || x)::varchar AS str," +
                    " ('2020-02-04T21:00:30.250000'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(10)";
            execute("INSERT INTO x " + o3Select2);
            drainWalQueue();

            String oracle = "(" + baseSelect
                    + " UNION ALL " + o3Select1
                    + " UNION ALL " + lagSelect
                    + " UNION ALL " + o3Select2
                    + ") ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracle, "x", LOG, true);
        });
    }

    @Test
    public void testSplitHardlinkSuffixChildWalLagPartialApply() throws Exception {
        // LAG over a hardlink suffix child, partially fast-applied: the next txn's min timestamp
        // falls INSIDE the LAG range, so applyFromWalLagToLastPartition binary-searches the LAG
        // timestamps in the shared donor file (file_row = logical + partitionTop) and commits a
        // prefix of the LAG rows, re-positioning the indexed symbol column at the file tail.
        // The LAG remainder then goes through the merge-sort read of the same file.
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            overrides.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 3);
            overrides.setProperty(PropertyKey.CAIRO_PARTITION_TOP_WAL_ENABLED, "true");
            overrides.setProperty(PropertyKey.CAIRO_WAL_SQUASH_UNCOMMITTED_ROWS_MULTIPLIER, 1);

            String tsType = timestampType.getTypeName();
            String baseSelect = "SELECT" +
                    " cast(x AS int) i," +
                    " CASE WHEN x % 3 = 0 THEN NULL ELSE 'sy' || (x % 5) END::SYMBOL sym," +
                    " ('2020-02-04'::timestamp + (x - 1) * 60 * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(60 * 24)";
            execute("CREATE TABLE x AS (" + baseSelect + "), INDEX(sym TYPE BITMAP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("ALTER TABLE x SET PARAM maxUncommittedRows = 25");
            drainWalQueue();

            String o3Select1 = "SELECT" +
                    " cast(x AS int) * 1000000 i," +
                    " CASE WHEN x % 4 = 0 THEN NULL ELSE 'sy' || (x % 5) END::SYMBOL sym," +
                    " ('2020-02-04T20:01'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(200)";
            execute("INSERT INTO x " + o3Select1);
            drainWalQueue();

            // LAG rows 23:59:30.0 .. 23:59:31.9 at 100ms steps
            String lagSelect = "SELECT" +
                    " cast(x AS int) * 3000000 i," +
                    " 'sy' || (x % 5) AS sym," +
                    " ('2020-02-04T23:59:30'::timestamp + (x - 1) * 100000L)::" + tsType + " ts" +
                    " FROM long_sequence(20)";
            execute("INSERT INTO x " + lagSelect);
            // min timestamp 23:59:31.05 is INSIDE the LAG range: the partial fast-apply commits
            // the 11 LAG rows at or before it, the remaining 9 merge-sort with these rows
            String inOrderSelect = "SELECT" +
                    " cast(x AS int) * 4000000 i," +
                    " 'sy' || (x % 5) AS sym," +
                    " ('2020-02-04T23:59:31.050000'::timestamp + (x - 1) * 10000L)::" + tsType + " ts" +
                    " FROM long_sequence(10)";
            execute("INSERT INTO x " + inOrderSelect);
            drainWalQueue();

            String oracle = "(" + baseSelect
                    + " UNION ALL " + o3Select1
                    + " UNION ALL " + lagSelect
                    + " UNION ALL " + inOrderSelect
                    + ") ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracle, "x", LOG, true);

            // index-backed scans must agree with the re-positioned symbol column
            for (String filter : new String[]{"sym = null", "sym = 'sy1'"}) {
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "SELECT * FROM (" + oracle + ") WHERE " + filter + " ORDER BY ts",
                        "SELECT * FROM x WHERE " + filter + " ORDER BY ts",
                        LOG,
                        true
                );
            }
        });
    }

    @Test
    public void testSplitMergeIntoHardlinkSuffixChildLateAddedColumns() throws Exception {
        // Reproduces WalWriterFuzzTest#testWalAddRemoveCommitFuzzO3 fuzz failure (seeds
        // 825449712927242895L, 4258917543179056722L): the first O3 write into a hardlinked
        // suffix child must treat columns added after the partition's data (absent in the
        // child) as local - their file offsets start at 0 (no +partitionTop shift) and the
        // posting index is created rather than opened. Without the per-column gate the
        // append read a zeroed varchar aux entry (VarcharTypeDriver assert) and the index
        // update tried to open a .pk file that was never created.
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            overrides.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 3);

            String tsType = timestampType.getTypeName();
            String baseSelect = "SELECT" +
                    " cast(x AS int) i," +
                    " -x j," +
                    " 's' || x AS str," +
                    " ('2020-02-04'::timestamp + (x - 1) * 60 * 1000000L)::" + tsType + " ts" +
                    " FROM long_sequence(60 * 36)";
            execute("CREATE TABLE x AS (" + baseSelect + ") TIMESTAMP(ts) PARTITION BY DAY");
            // Both columns are added after the initial load, so they are absent in the
            // 2020-02-04 partition and in the suffix child hardlinked from it.
            execute("ALTER TABLE x ADD COLUMN vch VARCHAR");
            execute("ALTER TABLE x ADD COLUMN sym SYMBOL INDEX TYPE POSTING");
            String baseSelectAllCols = "SELECT" +
                    " cast(x AS int) i," +
                    " -x j," +
                    " 's' || x AS str," +
                    " ('2020-02-04'::timestamp + (x - 1) * 60 * 1000000L)::" + tsType + " ts," +
                    " null::VARCHAR vch," +
                    " null::SYMBOL sym" +
                    " FROM long_sequence(60 * 36)";

            // O3 insert at 2020-02-04T20:01 creates a zero-copy 3-way split: prefix donor +
            // merged middle + hardlinked suffix child at 2020-02-04T20:05 (235 rows,
            // partitionTop = 1205), with vch and sym absent in the child.
            String o3Select1 = "SELECT" +
                    " cast(x AS int) * 1000000 i," +
                    " -x - 1000000L AS j," +
                    " 'a' || x AS str," +
                    " ('2020-02-04T20:01'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts," +
                    " ('va' || x)::VARCHAR vch," +
                    " 'syA' || (x % 3) AS sym" +
                    " FROM long_sequence(200)";
            execute("INSERT INTO x " + o3Select1);

            // O3 append strictly after the child's max timestamp (23:59) but within the day:
            // per-column append into the child. Shared columns append at the true tail of the
            // donor files (+partitionTop); vch and sym are created as local files at offset 0
            // with a local column top, and the posting index is initialized.
            String o3Select2 = "SELECT" +
                    " cast(x AS int) * 2000000 i," +
                    " -x - 2000000L AS j," +
                    " 'b' || x AS str," +
                    " ('2020-02-04T23:59:10'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts," +
                    " ('vb' || x)::VARCHAR vch," +
                    " 'syB' || (x % 3) AS sym" +
                    " FROM long_sequence(20)";
            execute("INSERT INTO x " + o3Select2);

            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(" + baseSelectAllCols + " UNION ALL " + o3Select1 + " UNION ALL " + o3Select2 + ") ORDER BY ts",
                    "x",
                    LOG,
                    true
            );

            // O3 insert strictly inside the child's range (21:00:30): merge into the child,
            // rewriting it into a private directory with local column tops.
            String o3Select3 = "SELECT" +
                    " cast(x AS int) * 3000000 i," +
                    " -x - 3000000L AS j," +
                    " 'c' || x AS str," +
                    " ('2020-02-04T21:00:30'::timestamp + (x - 1) * 1000000L)::" + tsType + " ts," +
                    " ('vc' || x)::VARCHAR vch," +
                    " 'syC' || (x % 3) AS sym" +
                    " FROM long_sequence(10)";
            execute("INSERT INTO x " + o3Select3);

            String oracle = "(" + baseSelectAllCols
                    + " UNION ALL " + o3Select1
                    + " UNION ALL " + o3Select2
                    + " UNION ALL " + o3Select3
                    + ") ORDER BY ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, oracle, "x", LOG, true);

            // Exercise the child's symbol index (posting) via an index-backed filter.
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT ts, sym FROM (" + oracle + ") WHERE sym = 'syB1'",
                    "SELECT ts, sym FROM x WHERE sym = 'syB1'",
                    LOG,
                    true
            );
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

            // O3 insert at 2020-02-04T20:01 creates a zero-copy 3-way split inside the mid partition:
            // prefix donor + merged middle + hardlinked suffix child. With max.splits=3 the resulting
            // 3 pieces (2 splits) stay below the cap and must survive the commit un-squashed.
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
                            2020-02-04T20:01:00.000000Z\t204\t2020-02-04T200000-000001
                            2020-02-04T20:05:00.000000Z\t235\t2020-02-04T200500
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
