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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongHashSet;
import io.questdb.std.Misc;
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

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

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
    public void testMergeBelowColumnTopLastPartition() throws Exception {
        assertMergeBelowColumnTop(true, true);
    }

    @Test
    public void testMergeBelowColumnTopLastPartitionNoColumnData() throws Exception {
        assertMergeBelowColumnTop(true, false);
    }

    @Test
    public void testMergeBelowColumnTopMidPartition() throws Exception {
        assertMergeBelowColumnTop(false, true);
    }

    @Test
    public void testMergeBelowColumnTopMidPartitionSyncCommit() throws Exception {
        // the copy job trims the column top scratch before it syncs the merged columns
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        assertMergeBelowColumnTop(false, true);
    }

    @Test
    public void testMergeBelowColumnTopMidPartitionNoColumnData() throws Exception {
        assertMergeBelowColumnTop(false, false);
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

                // Partition "2020-02-04" cannot be squashed with the new update because it's locked by the reader.
                // The reader never saw the new split, so the older split squashes into it.
                assertQuery(partitionsSql)
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns(replaceTimestampSuffix1("""
                                minTimestamp\tnumRows\tname
                                2020-02-04T00:00:00.000000Z\t1081\t2020-02-04
                                2020-02-04T18:01:00.000000Z\t489\t2020-02-04T180000-000001
                                """, timestampType.getTypeName()));

                rowCount = assertRowCount(170 + 319, rowCount);
            }

            // two pieces already match the max split count, the new rows merge into the split
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
                            2020-02-04T00:00:00.000000Z\t1081\t2020-02-04
                            2020-02-04T18:01:00.000000Z\t539\t2020-02-04T180000-000001
                            """, timestampType.getTypeName()));

            // the merge rewrites the split
            rowCount = assertRowCount(489 + 50, rowCount);


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
                            2020-02-04T00:00:00.000000Z\t1081\t2020-02-04
                            2020-02-04T18:01:00.000000Z\t589\t2020-02-04T180000-000001
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

            delta = 589 + 50;
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
    public void testSplitParentAppendAfterNeighborRewriteLateReader() throws Exception {
        // The reader opened after the split and never saw the moved rows, but the neighbor's rewrite moved
        // its name txn past the reader's txn, so the pin check counts the reader and copies the parent.
        assertSplitParentAppendAfterNeighborRewrite(false);
    }

    @Test
    public void testSplitParentAppendAfterNeighborRewrite() throws Exception {
        // The reader opened before the split and still reads the parent's old tail. All parent rows share
        // the max timestamp, so the writer cannot resplit and copies the parent.
        assertSplitParentAppendAfterNeighborRewrite(true);
    }

    @Test
    public void testPinnedSplitParentAppendResplitTooSmall() throws Exception {
        // the split threshold rejects the re-split, so the writer copies the parent instead
        assertPinnedSplitParentAppendAtMaxTimestamp(false);
    }

    @Test
    public void testPinnedSplitParentAppendAtMaxTimestamp() throws Exception {
        // A reader pinned before a split still reads the parent's old tail. An O3 row at the
        // parent's max timestamp goes into a new split together with the parent's rows at that timestamp.
        assertPinnedSplitParentAppendAtMaxTimestamp(true);
    }

    @Test
    public void testPinnedSplitParentDedupAppend() throws Exception {
        // The dedup step can turn a merge back into an in-place append when every parent row shares
        // the O3 rows' timestamp. The parent's tail is pinned, so the writer must keep the merge.
        // merge-append never opens a split directory; this test covers the plain split path
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            executeWithRewriteTimestamp(
                    "CREATE TABLE x (k INT, s STRING, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k)",
                    timestampType.getTypeName()
            );
            execute(
                    """
                            INSERT INTO x SELECT x::INT, 'str-' || x, '2020-02-03T10' FROM long_sequence(100)
                            UNION ALL
                            SELECT (100 + x)::INT, 'str-' || (100 + x), '2020-02-03T12'::TIMESTAMP + (x - 1) * 60_000_000L FROM long_sequence(5)
                            """
            );
            drainWalQueue();

            try (
                    RecordCursorFactory cursorFactory = select("SELECT * FROM x");
                    RecordCursor cursor = cursorFactory.getCursor(sqlExecutionContext)
            ) {
                sink.clear();
                println(cursorFactory, cursor);
                final String expected = sink.toString();

                // split at 11:00: the parent keeps the 100 rows that all sit at 10:00
                execute("INSERT INTO x VALUES (1_000, 'split', '2020-02-03T11')");
                drainWalQueue();
                try (TableReader reader = getReader("x")) {
                    Assert.assertEquals(2, reader.getPartitionCount());
                    Assert.assertEquals(100, reader.getPartitionRowCountFromMetadata(0));
                }

                // new keys at the parent's only timestamp: dedup finds no duplicates
                execute("INSERT INTO x SELECT (2_000 + x)::INT, 'new-' || x, '2020-02-03T10' FROM long_sequence(3)");
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

                cursor.toTop();
                sink.clear();
                println(cursorFactory, cursor);
                TestUtils.assertEquals(expected, sink);
            }

            assertQuery("SELECT count() FROM x WHERE ts = '2020-02-03T10'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n103\n");
            assertQuery("SELECT k, s FROM x WHERE ts >= '2020-02-03T11'")
                    .noLeakCheck()
                    .returns(
                            """
                                    k\ts
                                    1000\tsplit
                                    101\tstr-101
                                    102\tstr-102
                                    103\tstr-103
                                    104\tstr-104
                                    105\tstr-105
                                    """
                    );
        });
    }

    @Test
    public void testPinnedSplitParentGapAboveMax() throws Exception {
        // A range-replace commit trims the parent and leaves a gap below its split neighbor. A late row in
        // the gap sits above the parent's max; while the parent counts as pinned, the writer copies it.
        // merge-append never opens a split directory; this test covers the plain split path
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            // keeps the trimmed parent and its split apart: no reader can pin a partition version it never saw
            node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 2);
            executeWithRewriteTimestamp("CREATE TABLE x (i INT, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL", timestampType.getTypeName());
            // one row per minute from 00:00 to 01:39
            execute(
                    """
                            INSERT INTO x
                            SELECT x::INT, '2020-02-03'::TIMESTAMP + (x - 1) * 60_000_000L FROM long_sequence(100)
                            UNION ALL
                            SELECT 1_000, '2020-02-04'::TIMESTAMP FROM long_sequence(1)
                            """
            );
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("x");

            try (
                    RecordCursorFactory factoryBeforeSplit = select("SELECT * FROM x");
                    RecordCursor cursorBeforeSplit = factoryBeforeSplit.getCursor(sqlExecutionContext)
            ) {
                sink.clear();
                println(factoryBeforeSplit, cursorBeforeSplit);
                final String expectedBeforeSplit = sink.toString();

                // split at 01:20: the parent keeps the rows up to 01:20
                execute("INSERT INTO x VALUES (2_000, '2020-02-03T01:20:30')");
                drainWalQueue();
                final long splitTimestamp;
                final long rangeLo;
                try (TableReader reader = getReader("x")) {
                    Assert.assertEquals(3, reader.getPartitionCount());
                    Assert.assertEquals(81, reader.getPartitionRowCountFromMetadata(0));
                    splitTimestamp = reader.getTxFile().getPartitionTimestampByIndex(1);
                    rangeLo = ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType()).parseFloorLiteral("2020-02-03T01:10");
                }

                // delete from 01:10 up to the split: the parent keeps the rows up to 01:09
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walWriter.commitWithParams(rangeLo, splitTimestamp, WAL_DEDUP_MODE_REPLACE_RANGE);
                }
                drainWalQueue();
                final long trimmedNameTxn;
                try (TableReader reader = getReader("x")) {
                    Assert.assertEquals(3, reader.getPartitionCount());
                    Assert.assertEquals(70, reader.getPartitionRowCountFromMetadata(0));
                    Assert.assertEquals(splitTimestamp, reader.getTxFile().getPartitionTimestampByIndex(1));
                    trimmedNameTxn = reader.getTxFile().getPartitionNameTxn(0);
                }

                try (
                        RecordCursorFactory factoryAfterReplace = select("SELECT * FROM x");
                        // sees the trimmed parent; once the split is rewritten past this txn, the parent counts as pinned
                        RecordCursor cursorAfterReplace = factoryAfterReplace.getCursor(sqlExecutionContext)
                ) {
                    sink.clear();
                    println(factoryAfterReplace, cursorAfterReplace);
                    final String expectedAfterReplace = sink.toString();

                    // rewrite the split: its name txn moves past the txn the open cursor holds
                    execute("INSERT INTO x VALUES (2_001, '2020-02-03T01:30:30')");
                    drainWalQueue();
                    try (TableReader reader = getReader("x")) {
                        Assert.assertEquals(3, reader.getPartitionCount());
                        Assert.assertEquals(trimmedNameTxn, reader.getTxFile().getPartitionNameTxn(0));
                        Assert.assertEquals(21, reader.getPartitionRowCountFromMetadata(1));
                    }

                    // a late row in the gap between the parent's max and the split
                    execute("INSERT INTO x VALUES (2_002, '2020-02-03T01:15')");
                    drainWalQueue();
                    Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
                    try (TableReader reader = getReader("x")) {
                        // the pinned parent got a new version with the late row; the split stays apart
                        Assert.assertEquals(3, reader.getPartitionCount());
                        Assert.assertEquals(71, reader.getPartitionRowCountFromMetadata(0));
                        Assert.assertTrue(reader.getTxFile().getPartitionNameTxn(0) > trimmedNameTxn);
                    }

                    cursorAfterReplace.toTop();
                    sink.clear();
                    println(factoryAfterReplace, cursorAfterReplace);
                    TestUtils.assertEquals(expectedAfterReplace, sink);
                }

                cursorBeforeSplit.toTop();
                sink.clear();
                println(factoryBeforeSplit, cursorBeforeSplit);
                TestUtils.assertEquals(expectedBeforeSplit, sink);
            }

            assertQuery("SELECT i, ts FROM x WHERE ts BETWEEN '2020-02-03T01:08' AND '2020-02-03T01:21'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix1(
                            """
                                    i\tts
                                    69\t2020-02-03T01:08:00.000000Z
                                    70\t2020-02-03T01:09:00.000000Z
                                    2002\t2020-02-03T01:15:00.000000Z
                                    2000\t2020-02-03T01:20:30.000000Z
                                    82\t2020-02-03T01:21:00.000000Z
                                    """,
                            timestampType.getTypeName()
                    ));
            assertQuery("SELECT count() FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n93\n");
        });
    }

    @Test
    public void testPinnedSplitParentLargeAppend() throws Exception {
        // the merge rows outweigh the prefix, so the writer copies the parent
        assertPinnedSplitParentLargeAppend(false);
    }

    @Test
    public void testPinnedSplitParentLargeAppendSpanningSplit() throws Exception {
        assertPinnedSplitParentLargeAppend(true);
    }

    @Test
    public void testPinnedSplitParentMergeBelowColumnTop() throws Exception {
        // the merge materializes the column top nulls; the pinned reader keeps its rows
        assertPinnedSplitParentColumnTopMerge(false);
    }

    @Test
    public void testSplitParentO3AfterSquashOfNewerSplit() throws Exception {
        assertO3IntoSplitParentAfterSquashOfNewerSplit(false);
    }

    @Test
    public void testSplitParentO3AfterSquashOfNewerSplitWal() throws Exception {
        assertO3IntoSplitParentAfterSquashOfNewerSplit(true);
    }

    @Test
    public void testPinnedSplitParentResplitCyclesBelowColumnTop() throws Exception {
        // A reader pinned before the split keeps the parent's moved rows readable. Each resplit materializes the
        // column top nulls in scratch space of the new split's files, so the parent's files keep their size.
        assertMemoryLeak(() -> {
            node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            executeWithRewriteTimestamp("CREATE TABLE x (i INT, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", timestampType.getTypeName());
            execute("INSERT INTO x SELECT x::INT, '2020-02-03'::TIMESTAMP + (x - 1) * 60_000_000L FROM long_sequence(1000)");
            execute("ALTER TABLE x ADD COLUMN l LONG");
            execute("ALTER TABLE x ADD COLUMN s STRING");
            execute("ALTER TABLE x ADD COLUMN v VARCHAR");
            execute(
                    """
                            INSERT INTO x (i, ts, l, s, v)
                            SELECT (1000 + x)::INT, '2020-02-03T20'::TIMESTAMP + (x - 1) * 1_000_000L, x,
                                'string-value-' || x, 'varchar-value-' || x
                            FROM long_sequence(100)
                            """
            );
            execute("INSERT INTO x (i, ts, l, s, v) VALUES (2_000_000, '2020-02-04', 1, 'a', 'b')");

            final ObjList<String> files = new ObjList<>("l", "s", "s.i", "v", "v.i");
            final long[] sizesBeforeSplit = new long[files.size()];
            final long[][] sizes = new long[7][files.size()];
            columnFileSizes("x", "2020-02-03", files, sizesBeforeSplit);
            final String expected;
            try (
                    RecordCursorFactory cursorFactory = select("SELECT * FROM x WHERE ts >= '2020-02-03T20'");
                    RecordCursor cursor = cursorFactory.getCursor(sqlExecutionContext)
            ) {
                sink.clear();
                println(cursorFactory, cursor);
                expected = sink.toString();

                execute("INSERT INTO x (i, ts) VALUES (2_999_999, '2020-02-03'::TIMESTAMP + 995 * 60_000_000L + 30_000_000L)");
                try (TableReader reader = getReader("x")) {
                    Assert.assertEquals(3, reader.getPartitionCount());
                    Assert.assertEquals(996, reader.getPartitionRowCountFromMetadata(0));
                }
                columnFileSizes("x", "2020-02-03", files, sizes[0]);

                for (int cycle = 1; cycle < sizes.length; cycle++) {
                    // a late row just below the column top: the pinned parent resplits
                    execute("INSERT INTO x (i, ts) VALUES (" + (3_000_000 + cycle) + ", '2020-02-03'::TIMESTAMP + " + (991 - cycle) + " * 60_000_000L + 30_000_000L)");
                    columnFileSizes("x", "2020-02-03", files, sizes[cycle]);

                    cursor.toTop();
                    sink.clear();
                    println(cursorFactory, cursor);
                    TestUtils.assertEquals(expected, sink);
                }
                try (TableReader reader = getReader("x")) {
                    // the last cycle splits after row 991 - (cycles - 1)
                    Assert.assertEquals(993 - sizes.length, reader.getPartitionRowCountFromMetadata(0));
                }
            }

            for (int f = 0, n = files.size(); f < n; f++) {
                final String file = files.getQuick(f);
                for (int cycle = 0; cycle < sizes.length; cycle++) {
                    Assert.assertEquals(file + " of the pinned parent changed size, split " + cycle, sizesBeforeSplit[f], sizes[cycle][f]);
                }
            }

            // the parent's moved rows survive every resplit and the squash after the reader releases
            assertQuery("SELECT * FROM x WHERE ts >= '2020-02-03T20'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("SELECT count() FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n" + (1101 + sizes.length) + "\n");
            assertQuery("SELECT count() FROM x WHERE l IS NULL AND s IS NULL AND v IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n" + (1000 + sizes.length) + "\n");
        });
    }

    @Test
    public void testPinnedSplitParentResplitBelowColumnTop() throws Exception {
        assertPinnedSplitParentColumnTopMerge(true);
    }

    @Test
    public void testSplitParentResplitCyclesBelowColumnTop() throws Exception {
        // Without an open reader, each resplit below the column top is squashed back into the parent. The merge
        // materializes the column top in the split's files, so the parent's column file grows only by the squashed rows.
        assertMemoryLeak(() -> {
            node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            executeWithRewriteTimestamp("CREATE TABLE x (i INT, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", timestampType.getTypeName());
            execute("INSERT INTO x SELECT x::INT, '2020-02-03'::TIMESTAMP + (x - 1) * 60_000_000L FROM long_sequence(1000)");
            execute("ALTER TABLE x ADD COLUMN l LONG");
            execute(
                    """
                            INSERT INTO x (i, ts, l)
                            SELECT (1000 + x)::INT, '2020-02-03T20'::TIMESTAMP + (x - 1) * 1_000_000L, x
                            FROM long_sequence(100)
                            """
            );
            execute("INSERT INTO x (i, ts, l) VALUES (2_000_000, '2020-02-04', 1)");

            final long[] columnFileSizes = new long[5];
            for (int cycle = 0; cycle < columnFileSizes.length; cycle++) {
                // a late row just below the column top, close enough to the end that the partition resplits
                execute("INSERT INTO x (i, ts) VALUES (" + (3_000_000 + cycle) + ", '2020-02-03'::TIMESTAMP + " + (990 - cycle) + " * 60_000_000L + 30_000_000L)");
                columnFileSizes[cycle] = columnFileSize("x", "2020-02-03", "l");
            }
            // allocation granularity differs between platforms, so the check bounds the growth instead of exact sizes
            final long growth = columnFileSizes[columnFileSizes.length - 1] - columnFileSizes[0];
            Assert.assertTrue("column file grows with every cycle: " + Arrays.toString(columnFileSizes), growth < columnFileSizes[0]);

            assertQuery("SELECT count() FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1106\n");
            assertQuery("SELECT count() FROM x WHERE l IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n101\n");
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

        // Drives squashSplitPartitions over SPLIT sub-partitions. Merge-append folds a backdated
        // write into the partition's own composite geometry instead of opening a split directory, so
        // the split this test's setup asserts never appears. Squash over composite partitions is a
        // known gap; until it closes, pin the production default so the test keeps covering what it
        // was written for.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(ff, () -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            // A WAL table's splits are left to compaction up to the cap, and only cold ones are squashed.
            // Fold the split on the commit that makes it, so the commit squashes into the open partition.
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 1);
            overrides.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);

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

        // Drives squashSplitPartitions over SPLIT sub-partitions. Merge-append folds a backdated
        // write into the partition's own composite geometry instead of opening a split directory, so
        // the split this test's setup asserts never appears. Squash over composite partitions is a
        // known gap; until it closes, pin the production default so the test keeps covering what it
        // was written for.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(ff, () -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            // A WAL table's splits are left to compaction up to the cap, and only cold ones are squashed.
            // Fold the split on the commit that makes it, so the commit squashes into the open partition.
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 1);
            overrides.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);

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
    public void testSquashIntoOpenPartitionLeavesNoStaleMapping() throws Exception {
        // squashSplitPartitions appends into the squash target through the frame's own file
        // descriptors. If the writer still mapped the target from before the squash, those
        // append memories would describe a SHORTER file than what is on disk, and the next
        // truncating close would trim the squash's bytes back off ("binary is outside of file
        // boundary").
        //
        // The writer maps native 2020-02-04 until 2020-02-05 is born parquet; the commit that
        // makes the last partition append-blocked closes that mapping, so by the time the squash
        // runs the writer maps nothing and has nothing to re-sync. Whatever the writer maps, the
        // contract is the same, and it shows up in the file
        // descriptors: no 2020-02-04 column file the writer opened BEFORE the squashing commit may
        // still be open after it. openedSinceMark -- the fds opened by the squashing commit and
        // still open when it returns -- must therefore account for every open target fd. It counts
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

        // Drives squashSplitPartitions over SPLIT sub-partitions. Merge-append folds a backdated
        // write into the partition's own composite geometry instead of opening a split directory, so
        // the split this test's setup asserts never appears. Squash over composite partitions is a
        // known gap; until it closes, pin the production default so the test keeps covering what it
        // was written for.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(ff, () -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            // A WAL table's splits are left to compaction up to the cap, and only cold ones are squashed.
            // Fold the split on the commit that makes it, so the commit squashes into the open partition.
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 1);
            overrides.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);

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
                Assert.assertEquals(
                        "the writer still holds a 2020-02-04 column file it opened before squashing into"
                                + " the partition, so its append memory describes a shorter file than the"
                                + " squash left on disk",
                        openedSinceMark.size(),
                        openTargetFds.size()
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
    public void testSquashAllocatesNothingForColumnUnderItsTop() throws Exception {
        testSquashAllocatesNothingForColumnUnderItsTop(false);
    }

    @Test
    public void testSquashAllocatesNothingForColumnUnderItsTopCopiedTarget() throws Exception {
        testSquashAllocatesNothingForColumnUnderItsTop(true);
    }

    @Test
    public void testSquashPartitionClearsRemoteAndStampsTarget() throws Exception {
        // Drives squashSplitPartitions over SPLIT sub-partitions. Merge-append folds a backdated
        // write into the partition's own composite geometry instead of opening a split directory, so
        // the split this test's setup asserts never appears. Squash over composite partitions is a
        // known gap; until it closes, pin the production default so the test keeps covering what it
        // was written for.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
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
    public void testSquashPartitionsFailsToAllocateTarget() throws Exception {
        // The squash grows the target's column files through the frame columns' ranged allocate. A facade that
        // injects ENOSPC through allocate(fd, size) alone must still see that call.
        final AtomicBoolean armed = new AtomicBoolean();
        final Set<Long> targetFds = ConcurrentHashMap.newKeySet();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean allocate(long fd, long size) {
                if (armed.get() && targetFds.contains(fd)) {
                    return false;
                }
                return super.allocate(fd, size);
            }

            @Override
            public boolean close(long fd) {
                targetFds.remove(fd);
                return super.close(fd);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                if (armed.get() && Utf8s.containsAscii(name, Files.SEPARATOR + "2020-02-04" + Files.SEPARATOR)) {
                    targetFds.add(fd);
                }
                return fd;
            }
        };
        testSquashPartitionsFails(ff, armed, "No space left");
    }

    @Test
    public void testSquashPartitionsFailsToOpenSource() throws Exception {
        final AtomicBoolean armed = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (armed.get() && Utf8s.containsAscii(name, "2020-02-04T200000")
                        && Utf8s.endsWithAscii(name, Files.SEPARATOR + "str.i")) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        testSquashPartitionsFails(ff, armed, "could not open");
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
    public void testSquashOrphanSplitWithReaderAtLatestTxn() throws Exception {
        // A split without a parent gets a fresh parent named with the in-flight txn. A reader at the latest
        // txn counted as pinning it, so the squash copied into a same-named directory and then purged it.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 * (1 << 10));
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);

            execute(
                    "CREATE TABLE x AS (" +
                            "SELECT" +
                            " x::INT i," +
                            " -x j," +
                            " timestamp_sequence('2020-02-04T00', 60*1000000L)::" + timestampType.getTypeName() + " ts" +
                            " FROM long_sequence(60*(23*2))" +
                            ") TIMESTAMP (ts) PARTITION BY DAY",
                    sqlExecutionContext
            );

            String partitionsSql = "SELECT minTimestamp, numRows, name FROM table_partitions('x')";
            // the first reader keeps the split apart from its parent
            try (TableReader ignore = getReader("x")) {
                execute(
                        "INSERT INTO x SELECT x::INT * 1000000 i, -x - 1000000L j," +
                                " timestamp_sequence('2020-02-04T20:01', 1000000L) ts FROM long_sequence(200)",
                        sqlExecutionContext
                );
                execute("ALTER TABLE x FORCE DROP PARTITION LIST '2020-02-04'", sqlExecutionContext);
                assertQuery(partitionsSql)
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("minTimestamp\tnumRows\tname\n" +
                                replaceTimestampSuffix1("""
                                        2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001
                                        2020-02-05T00:00:00.000000Z\t1320\t2020-02-05
                                        """, timestampType.getTypeName()));

                // the second reader holds the latest committed txn while the squash runs
                try (TableReader ignore2 = getReader("x")) {
                    execute("ALTER TABLE x SQUASH PARTITIONS", sqlExecutionContext);
                }

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

            // 439 squashed rows and 1320 in the next day; an interval scan would miss the squashed partition
            // because the force drop left the table's min timestamp at the next day
            assertQuery("SELECT count() FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1759\n");
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

    // mid partitions also check file sizes; the writer may resize the last partition's files
    private void assertMergeBelowColumnTop(boolean isLastPartition, boolean hasColumnData) throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE x (i INT, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", timestampType.getTypeName());
            execute("INSERT INTO x SELECT x::INT, '2020-02-03'::TIMESTAMP + (x - 1) * 60_000_000L FROM long_sequence(1000)");
            execute("ALTER TABLE x ADD COLUMN l LONG");
            execute("ALTER TABLE x ADD COLUMN s STRING");
            execute("ALTER TABLE x ADD COLUMN v VARCHAR");
            execute("ALTER TABLE x ADD COLUMN b BINARY");
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[]");
            if (hasColumnData) {
                execute(
                        """
                                INSERT INTO x (i, ts, l, s, v, b, a)
                                SELECT (1_000 + x)::INT, '2020-02-03T20'::TIMESTAMP + (x - 1) * 1_000_000L, x,
                                    'string-value-' || x, 'varchar-value-' || x, rnd_bin(16, 16, 0), ARRAY[x::DOUBLE, -x::DOUBLE]
                                FROM long_sequence(100)
                                """
                );
            }
            if (!isLastPartition) {
                execute("INSERT INTO x (i, ts) VALUES (2_000_000, '2020-02-04')");
            }
            final int rowCount = hasColumnData ? 1100 : 1000;
            final ObjList<String> files = new ObjList<>("l", "s", "s.i", "v", "v.i", "b", "b.i", "a", "a.i");
            final long[] sourceSizes = new long[files.size()];
            final long partitionTimestamp;
            final long sourceNameTxn;
            try (TableReader reader = getReader("x")) {
                partitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(0);
                sourceNameTxn = reader.getTxFile().getPartitionNameTxn(0);
                for (int f = 0, n = files.size(); f < n; f++) {
                    sourceSizes[f] = columnFileSize(reader, partitionTimestamp, sourceNameTxn, files.getQuick(f));
                }
            }

            final String expected;
            try (
                    RecordCursorFactory cursorFactory = select("SELECT * FROM x");
                    // keeps the source version of the partition on disk
                    RecordCursor cursor = cursorFactory.getCursor(sqlExecutionContext)
            ) {
                sink.clear();
                println(cursorFactory, cursor);
                expected = sink.toString();

                // a late row below the column top, with too few rows before it to split: the writer merges the
                // whole partition and materializes the column top nulls of the added columns
                execute("INSERT INTO x (i, ts, l, s, v, a) VALUES (3_000_000, '2020-02-03T00:30:30', 7, 'late', 'late', ARRAY[7.0])");
                try (TableReader reader = getReader("x")) {
                    Assert.assertEquals(rowCount + 1, reader.getPartitionRowCountFromMetadata(0));
                    final long nameTxn = reader.getTxFile().getPartitionNameTxn(0);
                    Assert.assertTrue(nameTxn > sourceNameTxn);
                    for (int f = 0, n = files.size(); f < n; f++) {
                        final String file = files.getQuick(f);
                        final long size = columnFileSize(reader, partitionTimestamp, sourceNameTxn, file);
                        if (sourceSizes[f] < 0) {
                            // the merge may create the missing source file, but never writes to it
                            Assert.assertTrue(file + " of the source partition: " + size, size <= 0);
                        } else if (!isLastPartition) {
                            // the writer owns the last partition's files and may resize them
                            Assert.assertEquals(file + " of the source partition", sourceSizes[f], size);
                        }
                    }
                    if (!isLastPartition) {
                        // the merged partition has no column top, and its files end at their logical size
                        final long mergedRows = rowCount + 1;
                        assertMergedColumnFileSize(reader, partitionTimestamp, nameTxn, "l", mergedRows * Long.BYTES);
                        assertMergedColumnFileSize(reader, partitionTimestamp, nameTxn, "s.i", (mergedRows + 1) * Long.BYTES);
                        assertMergedColumnFileSize(reader, partitionTimestamp, nameTxn, "v.i", mergedRows * 2 * Long.BYTES);
                        assertMergedColumnFileSize(reader, partitionTimestamp, nameTxn, "b.i", (mergedRows + 1) * Long.BYTES);
                        assertMergedColumnFileSize(reader, partitionTimestamp, nameTxn, "a.i", mergedRows * 2 * Long.BYTES);
                    }
                }

                cursor.toTop();
                sink.clear();
                println(cursorFactory, cursor);
                TestUtils.assertEquals(expected, sink);
            }

            // every row that existed before the merge reads the same, including BINARY and array values
            assertQuery("SELECT * FROM x WHERE i != 3_000_000")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            final int nullCount = 1000 + (isLastPartition ? 0 : 1);
            assertQuery("SELECT count() FROM x WHERE l IS NULL AND s IS NULL AND v IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n" + nullCount + "\n");
            assertQuery("SELECT count() FROM x WHERE b IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n" + (nullCount + 1) + "\n");
            // a column top row, the late row and the first and last rows with column data
            assertQuery("SELECT i, l, s, v, a FROM x WHERE ts IN '2020-02-03T00:30:30' OR i IN (1, 1_001, 1_100)")
                    .noLeakCheck()
                    .returns(
                            "i\tl\ts\tv\ta\n"
                                    + "1\tnull\t\t\tnull\n"
                                    + "3000000\t7\tlate\tlate\t[7.0]\n"
                                    + (hasColumnData ? "1001\t1\tstring-value-1\tvarchar-value-1\t[1.0,-1.0]\n1100\t100\tstring-value-100\tvarchar-value-100\t[100.0,-100.0]\n" : "")
                    );
        });
    }

    private void assertMergedColumnFileSize(TableReader reader, long partitionTimestamp, long partitionNameTxn, String file, long logicalSize) {
        final long size = columnFileSize(reader, partitionTimestamp, partitionNameTxn, file);
        Assert.assertTrue(file + " of the merged partition is shorter than its data: " + size, size >= logicalSize);
        Assert.assertTrue(file + " of the merged partition keeps column top scratch: " + size, size <= Files.ceilPageSize(logicalSize));
    }

    private void assertO3IntoSplitParentAfterSquashOfNewerSplit(boolean isWal) throws Exception {
        // merge-append never opens a split directory; this test covers the plain split path
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 2);
            // a WAL table squashes only cold splits above the cap; count every split as cold at once
            node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
            executeWithRewriteTimestamp(
                    "CREATE TABLE x (i INT, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : " BYPASS WAL"),
                    timestampType.getTypeName()
            );
            // one row per minute from 00:00 to 16:39
            execute("INSERT INTO x SELECT x::INT, '2020-02-03'::TIMESTAMP + (x - 1) * 60_000_000L FROM long_sequence(1000)");
            drainWalQueue();

            // Splits at 15:00 then 12:00 exceed the cap of two. Non-WAL folds the 12:00 split into the parent,
            // WAL folds the two splits together; either way the parent's neighbor gets a newer name txn.
            execute("INSERT INTO x VALUES (1_001, '2020-02-03T15:00:30')");
            drainWalQueue();
            execute("INSERT INTO x VALUES (1_002, '2020-02-03T12:00:30')");
            drainWalQueue();
            try (TableReader reader = getReader("x")) {
                Assert.assertEquals(2, reader.getPartitionCount());
                Assert.assertEquals(isWal ? 721 : 902, reader.getPartitionRowCountFromMetadata(0));
            }

            // the next late row lands in the parent
            execute("INSERT INTO x VALUES (1_003, '2020-02-03T11:00:30')");
            drainWalQueue();
            if (isWal) {
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            }

            assertQuery("SELECT i, ts FROM x WHERE i > 1_000")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix1(
                            """
                                    i\tts
                                    1003\t2020-02-03T11:00:30.000000Z
                                    1002\t2020-02-03T12:00:30.000000Z
                                    1001\t2020-02-03T15:00:30.000000Z
                                    """,
                            timestampType.getTypeName()
                    ));
            assertQuery("SELECT count() FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1003\n");
        });
    }

    private void assertPinnedSplitParentLargeAppend(boolean isSpanningIntoSplit) throws Exception {
        assertMemoryLeak(() -> {
            node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            // a single O3 row re-splits this layout, see testPinnedSplitParentAppendAtMaxTimestamp
            executeWithRewriteTimestamp("CREATE TABLE x (i INT, str STRING, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", timestampType.getTypeName());
            execute("INSERT INTO x SELECT x::INT, 'str-' || x, '2020-02-03T16' FROM long_sequence(20)");
            execute("INSERT INTO x SELECT (20 + x)::INT, 'str-' || (20 + x), '2020-02-03T17' FROM long_sequence(2)");
            execute("INSERT INTO x SELECT (22 + x)::INT, 'str-' || (22 + x), '2020-02-03T18' FROM long_sequence(2)");

            final String expectedPinned = replaceTimestampSuffix1(
                    """
                            i\tstr\tts
                            21\tstr-21\t2020-02-03T17:00:00.000000Z
                            22\tstr-22\t2020-02-03T17:00:00.000000Z
                            23\tstr-23\t2020-02-03T18:00:00.000000Z
                            24\tstr-24\t2020-02-03T18:00:00.000000Z
                            """,
                    timestampType.getTypeName()
            );
            final int o3RowsInSplit = isSpanningIntoSplit ? 5 : 0;

            try (
                    RecordCursorFactory cursorFactory = select("SELECT * FROM x WHERE ts BETWEEN '2020-02-03T17' AND '2020-02-03T18'");
                    RecordCursor cursor = cursorFactory.getCursor(sqlExecutionContext)
            ) {
                sink.clear();
                println(cursorFactory, cursor);
                TestUtils.assertEquals(expectedPinned, sink);

                // split at 17:30: the parent keeps the rows up to 17:00
                execute("INSERT INTO x VALUES (1_000_000, 'split', '2020-02-03T17:30')");
                final long parentNameTxn;
                try (TableReader reader = getReader("x")) {
                    Assert.assertEquals(2, reader.getPartitionCount());
                    Assert.assertEquals(22, reader.getPartitionRowCountFromMetadata(0));
                    Assert.assertEquals(3, reader.getPartitionRowCountFromMetadata(1));
                    parentNameTxn = reader.getTxFile().getPartitionNameTxn(0);
                }

                // 22 merge rows outweigh the 20-row prefix, so the writer copies the parent instead of re-splitting
                execute(
                        "INSERT INTO x SELECT (100 + x)::INT, 'o3-' || x, "
                                + "CASE WHEN x <= 20 THEN '2020-02-03T17' ELSE '2020-02-03T17:45' END "
                                + "FROM long_sequence(" + (20 + o3RowsInSplit) + ")"
                );
                try (TableReader reader = getReader("x")) {
                    Assert.assertEquals(2, reader.getPartitionCount());
                    Assert.assertEquals(42, reader.getPartitionRowCountFromMetadata(0));
                    Assert.assertEquals(3 + o3RowsInSplit, reader.getPartitionRowCountFromMetadata(1));
                    Assert.assertTrue(reader.getTxFile().getPartitionNameTxn(0) > parentNameTxn);
                }

                cursor.toTop();
                sink.clear();
                println(cursorFactory, cursor);
                TestUtils.assertEquals(expectedPinned, sink);
            }

            final StringBuilder expectedAt17 = new StringBuilder("i\tstr\n21\tstr-21\n22\tstr-22\n");
            for (int i = 1; i <= 20; i++) {
                expectedAt17.append(100 + i).append("\to3-").append(i).append('\n');
            }
            assertQuery("SELECT i, str FROM x WHERE ts = '2020-02-03T17'")
                    .noLeakCheck()
                    .returns(expectedAt17.toString());

            final String o3InSplit = isSpanningIntoSplit
                    ? """
                    121\to3-21\t2020-02-03T17:45:00.000000Z
                    122\to3-22\t2020-02-03T17:45:00.000000Z
                    123\to3-23\t2020-02-03T17:45:00.000000Z
                    124\to3-24\t2020-02-03T17:45:00.000000Z
                    125\to3-25\t2020-02-03T17:45:00.000000Z
                    """
                    : "";
            assertQuery("SELECT * FROM x WHERE ts > '2020-02-03T17'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix1(
                            "i\tstr\tts\n"
                                    + "1000000\tsplit\t2020-02-03T17:30:00.000000Z\n"
                                    + o3InSplit
                                    + "23\tstr-23\t2020-02-03T18:00:00.000000Z\n"
                                    + "24\tstr-24\t2020-02-03T18:00:00.000000Z\n",
                            timestampType.getTypeName()
                    ));
        });
    }

    private void assertPinnedSplitParentColumnTopMerge(boolean isResplit) throws Exception {
        assertMemoryLeak(() -> {
            node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            executeWithRewriteTimestamp("CREATE TABLE x (i INT, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", timestampType.getTypeName());
            execute("INSERT INTO x SELECT x::INT, '2020-02-03T10'::TIMESTAMP + (x - 1) * 60_000_000L FROM long_sequence(100)");
            execute("ALTER TABLE x ADD COLUMN l LONG");
            execute("ALTER TABLE x ADD COLUMN s STRING");
            execute("ALTER TABLE x ADD COLUMN v VARCHAR");
            execute("ALTER TABLE x ADD COLUMN sym SYMBOL");
            execute(
                    """
                            INSERT INTO x (i, ts, l, s, v, sym)
                            SELECT (100 + x)::INT, '2020-02-03T12'::TIMESTAMP + (x - 1) * 60_000_000L, x * 1_000,
                                'string-value-' || x, 'varchar-value-' || x, 'sym-' || (x % 3)
                            FROM long_sequence(48)
                            """
            );

            final String expected;
            try (
                    // the fixed-size columns get their own cursor, so a fault there does not hide behind a string read error
                    RecordCursorFactory fixFactory = select("SELECT i, l, sym, ts FROM x WHERE ts >= '2020-02-03T12'");
                    RecordCursorFactory cursorFactory = select("SELECT i, l, s, v, sym, ts FROM x WHERE ts >= '2020-02-03T12'");
                    // each cursor pins a reader before the split
                    RecordCursor fixCursor = fixFactory.getCursor(sqlExecutionContext);
                    RecordCursor cursor = cursorFactory.getCursor(sqlExecutionContext)
            ) {
                sink.clear();
                println(fixFactory, fixCursor);
                final String expectedFix = sink.toString();
                sink.clear();
                println(cursorFactory, cursor);
                expected = sink.toString();

                // split right after the first row with values: the parent keeps 101 rows, 1 past the column top
                execute("INSERT INTO x (i, ts) VALUES (1_000_000, '2020-02-03T12:00:30')");
                try (TableReader reader = getReader("x")) {
                    Assert.assertEquals(2, reader.getPartitionCount());
                    Assert.assertEquals(101, reader.getPartitionRowCountFromMetadata(0));
                }

                // The O3 row lands below the column top, so the merge materializes the column top nulls.
                // A resplit keeps the parent in place, a full merge rewrites it.
                execute("INSERT INTO x (i, ts) VALUES (1_000_001, '" + (isResplit ? "2020-02-03T11:35:30" : "2020-02-03T10:30:30") + "')");
                try (TableReader reader = getReader("x")) {
                    if (isResplit) {
                        Assert.assertEquals(3, reader.getPartitionCount());
                        Assert.assertEquals(96, reader.getPartitionRowCountFromMetadata(0));
                    } else {
                        Assert.assertEquals(2, reader.getPartitionCount());
                        Assert.assertEquals(102, reader.getPartitionRowCountFromMetadata(0));
                    }
                }

                fixCursor.toTop();
                sink.clear();
                println(fixFactory, fixCursor);
                TestUtils.assertEquals(expectedFix, sink);

                cursor.toTop();
                sink.clear();
                println(cursorFactory, cursor);
                TestUtils.assertEquals(expected, sink);
            }

            // the 100 rows below the column top, plus the two inserted rows, read the materialized nulls
            assertQuery("SELECT count() FROM x WHERE l IS NULL AND s IS NULL AND v IS NULL AND sym IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n102\n");
            assertQuery("SELECT i, l, s, v, sym, ts FROM x WHERE ts >= '2020-02-03T12' AND i < 1_000_000")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("SELECT count() FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n150\n");
        });
    }

    private int assertRowCount(int delta, int rowCount) {
        Assert.assertEquals(delta, getPhysicalRowsSinceLastCommit());
        rowCount += delta;
        Assert.assertEquals(rowCount, node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows());
        return rowCount;
    }

    private void assertSplitParentAppendAfterNeighborRewrite(boolean isReaderBeforeSplit) throws Exception {
        assertMemoryLeak(() -> {
            node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            executeWithRewriteTimestamp("CREATE TABLE x (i INT, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", timestampType.getTypeName());
            // 1000 rows at the parent's max timestamp and a tail to split off
            execute("INSERT INTO x SELECT x::INT, '2020-02-03T10' FROM long_sequence(1000)");
            execute("INSERT INTO x SELECT (1000 + x)::INT, '2020-02-03T12' FROM long_sequence(10)");

            RecordCursorFactory cursorFactory = null;
            RecordCursor cursor = null;
            try {
                if (isReaderBeforeSplit) {
                    cursorFactory = select("SELECT count() FROM x");
                    cursor = cursorFactory.getCursor(sqlExecutionContext);
                }

                // split off the tail: the parent keeps the 1000 rows at 10:00
                execute("INSERT INTO x VALUES (1_000_000, '2020-02-03T11')");
                final long parentNameTxn;
                final long splitNameTxn;
                try (TableReader reader = getReader("x")) {
                    Assert.assertEquals(2, reader.getPartitionCount());
                    Assert.assertEquals(1000, reader.getPartitionRowCountFromMetadata(0));
                    parentNameTxn = reader.getTxFile().getPartitionNameTxn(0);
                    splitNameTxn = reader.getTxFile().getPartitionNameTxn(1);
                }

                if (!isReaderBeforeSplit) {
                    cursorFactory = select("SELECT count() FROM x");
                    cursor = cursorFactory.getCursor(sqlExecutionContext);
                }
                final int expectedCount = isReaderBeforeSplit ? 1010 : 1011;

                // rewrite the neighboring split: the O3 row lands inside it and the split is too small to resplit
                execute("INSERT INTO x VALUES (1_000_001, '2020-02-03T11:30')");
                try (TableReader reader = getReader("x")) {
                    Assert.assertEquals(2, reader.getPartitionCount());
                    Assert.assertEquals(parentNameTxn, reader.getTxFile().getPartitionNameTxn(0));
                    Assert.assertTrue(reader.getTxFile().getPartitionNameTxn(1) > splitNameTxn);
                }

                long writtenBefore = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
                // one row at the parent's max timestamp: the writer copies the parent
                execute("INSERT INTO x VALUES (1_000_002, '2020-02-03T10')");
                Assert.assertEquals(1001, node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - writtenBefore);
                try (TableReader reader = getReader("x")) {
                    Assert.assertEquals(2, reader.getPartitionCount());
                    Assert.assertEquals(1001, reader.getPartitionRowCountFromMetadata(0));
                    Assert.assertTrue(reader.getTxFile().getPartitionNameTxn(0) > parentNameTxn);
                }

                cursor.toTop();
                sink.clear();
                println(cursorFactory, cursor);
                TestUtils.assertEquals("count\n" + expectedCount + "\n", sink);
            } finally {
                Misc.free(cursor);
                Misc.free(cursorFactory);
            }

            // The copy has a fresh name txn, newer than the neighbor's, so without the reader the next
            // row at the parent's max timestamp appends in place.
            final long copyNameTxn;
            try (TableReader reader = getReader("x")) {
                copyNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }
            final long writtenBefore = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
            execute("INSERT INTO x VALUES (1_000_003, '2020-02-03T10')");
            Assert.assertEquals(1, node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - writtenBefore);
            try (TableReader reader = getReader("x")) {
                Assert.assertEquals(2, reader.getPartitionCount());
                Assert.assertEquals(1002, reader.getPartitionRowCountFromMetadata(0));
                Assert.assertEquals(copyNameTxn, reader.getTxFile().getPartitionNameTxn(0));
            }

            final StringBuilder expectedAt10 = new StringBuilder("i\n");
            for (int i = 1; i <= 1000; i++) {
                expectedAt10.append(i).append('\n');
            }
            expectedAt10.append("1000002\n1000003\n");
            assertQuery("SELECT i FROM x WHERE ts = '2020-02-03T10'")
                    .noLeakCheck()
                    .returns(expectedAt10.toString());
            assertQuery("SELECT count() FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1014\n");
        });
    }

    private void assertPinnedSplitParentAppendAtMaxTimestamp(boolean canResplit) throws Exception {
        assertMemoryLeak(() -> {
            node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            // rows at 16:00, 17:00 and 18:00; the re-split needs the rows before the parent's max
            // timestamp to outnumber the rows at it by more than 2x
            final int at16 = canResplit ? 20 : 2;
            final int at17 = canResplit ? 2 : 20;
            final int at18 = 2;
            executeWithRewriteTimestamp("create table x (i int, str string, ts #TIMESTAMP) timestamp(ts) partition by DAY", timestampType.getTypeName());
            execute("insert into x select cast(x as int), 'str-' || x, '2020-02-03T16' from long_sequence(" + at16 + ")");
            execute("insert into x select cast(" + at16 + " + x as int), 'str-' || (" + at16 + " + x), '2020-02-03T17' from long_sequence(" + at17 + ")");
            execute("insert into x select cast(" + (at16 + at17) + " + x as int), 'str-' || (" + (at16 + at17) + " + x), '2020-02-03T18' from long_sequence(" + at18 + ")");

            final StringBuilder expected = new StringBuilder("i\tstr\tts\n");
            for (int i = at16 + 1; i <= at16 + at17 + at18; i++) {
                expected.append(i).append("\tstr-").append(i).append(i <= at16 + at17 ? "\t2020-02-03T17:00:00.000000Z\n" : "\t2020-02-03T18:00:00.000000Z\n");
            }
            final String expectedStr = replaceTimestampSuffix1(expected.toString(), timestampType.getTypeName());

            final long parentNameTxn;
            try (
                    RecordCursorFactory cursorFactory = select("select * from x where ts between '2020-02-03T17' and '2020-02-03T18'");
                    RecordCursor cursor = cursorFactory.getCursor(sqlExecutionContext)
            ) {
                sink.clear();
                println(cursorFactory, cursor);
                TestUtils.assertEquals(expectedStr, sink);

                // split at 17:30: the parent keeps the rows up to 17:00
                execute("insert into x values (1000000, 'split', '2020-02-03T17:30')");
                try (TableReader reader = getReader("x")) {
                    Assert.assertEquals(2, reader.getPartitionCount());
                    Assert.assertEquals(at16 + at17, reader.getPartitionRowCountFromMetadata(0));
                    parentNameTxn = reader.getTxFile().getPartitionNameTxn(0);
                }

                // O3 row at exactly the parent's max timestamp; appending it in place would
                // overwrite the row at 18:00 the pinned reader still reads from the parent
                execute("insert into x values (1000001, 'X', '2020-02-03T17')");
                try (TableReader reader = getReader("x")) {
                    if (canResplit) {
                        Assert.assertEquals(3, reader.getPartitionCount());
                        Assert.assertEquals(at16, reader.getPartitionRowCountFromMetadata(0));
                        Assert.assertEquals(at17 + 1, reader.getPartitionRowCountFromMetadata(1));
                        Assert.assertEquals(parentNameTxn, reader.getTxFile().getPartitionNameTxn(0));
                    } else {
                        Assert.assertEquals(2, reader.getPartitionCount());
                        Assert.assertEquals(at16 + at17 + 1, reader.getPartitionRowCountFromMetadata(0));
                        Assert.assertTrue(reader.getTxFile().getPartitionNameTxn(0) > parentNameTxn);
                    }
                }

                cursor.toTop();
                sink.clear();
                println(cursorFactory, cursor);
                TestUtils.assertEquals(expectedStr, sink);
            }

            // no reader pins the parent's tail anymore: the next row at the parent's max
            // timestamp is appended in place
            final int partitionCount;
            final long parentRowCount;
            final long parentNameTxn2;
            try (TableReader reader = getReader("x")) {
                partitionCount = reader.getPartitionCount();
                parentRowCount = reader.getPartitionRowCountFromMetadata(0);
                parentNameTxn2 = reader.getTxFile().getPartitionNameTxn(0);
            }
            execute("insert into x values (1000002, 'Y', '2020-02-03T" + (canResplit ? "16" : "17") + "')");
            try (TableReader reader = getReader("x")) {
                Assert.assertEquals(partitionCount, reader.getPartitionCount());
                Assert.assertEquals(parentRowCount + 1, reader.getPartitionRowCountFromMetadata(0));
                Assert.assertEquals(parentNameTxn2, reader.getTxFile().getPartitionNameTxn(0));
            }

            // existing rows at 17:00 stay ahead of the O3 rows inserted at the same timestamp
            final StringBuilder expectedAt17 = new StringBuilder("i\tstr\n");
            for (int i = at16 + 1; i <= at16 + at17; i++) {
                expectedAt17.append(i).append("\tstr-").append(i).append('\n');
            }
            expectedAt17.append("1000001\tX\n");
            if (!canResplit) {
                expectedAt17.append("1000002\tY\n");
            }
            assertQuery("select i, str from x where ts = '2020-02-03T17'")
                    .noLeakCheck()
                    .returns(expectedAt17.toString());
            assertQuery("select count() from x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n" + (at16 + at17 + at18 + 3) + "\n");
        });
    }

    private long columnFileSize(String tableName, String partition, String columnName) throws NumericException {
        try (TableReader reader = getReader(tableName)) {
            return columnFileSize(reader, partition, columnName);
        }
    }

    private long columnFileSize(TableReader reader, String partition, String columnName) throws NumericException {
        final long partitionTimestamp = ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType()).parseFloorLiteral(partition);
        final int partitionIndex = reader.getTxFile().getPartitionIndex(partitionTimestamp);
        return columnFileSize(reader, partitionTimestamp, reader.getTxFile().getPartitionNameTxn(partitionIndex), columnName);
    }

    // columnName with a ".i" suffix selects the aux file of a var-size column; returns -1 when the file does not exist
    private long columnFileSize(TableReader reader, long partitionTimestamp, long partitionNameTxn, String columnName) {
        final boolean isAuxFile = columnName.endsWith(".i");
        if (isAuxFile) {
            columnName = columnName.substring(0, columnName.length() - 2);
        }
        final int columnIndex = reader.getMetadata().getColumnIndex(columnName);
        final long columnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(partitionTimestamp, reader.getMetadata().getWriterIndex(columnIndex));
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(reader.getTableToken());
            TableUtils.setPathForNativePartition(path, reader.getMetadata().getTimestampType(), PartitionBy.DAY, partitionTimestamp, partitionNameTxn);
            final LPSZ file = isAuxFile ? TableUtils.iFile(path, columnName, columnNameTxn) : TableUtils.dFile(path, columnName, columnNameTxn);
            return Files.exists(file) ? Files.length(file) : -1;
        }
    }

    private void columnFileSizes(String tableName, String partition, ObjList<String> columnNames, long[] sizes) throws NumericException {
        try (TableReader reader = getReader(tableName)) {
            for (int i = 0, n = columnNames.size(); i < n; i++) {
                sizes[i] = columnFileSize(reader, partition, columnNames.getQuick(i));
            }
        }
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

    private void testSquashAllocatesNothingForColumnUnderItsTop(boolean copyTarget) throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 3);
            execute(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " timestamp_sequence('2020-02-04T00', 1000000L)::" + timestampType.getTypeName() + " ts" +
                            " from long_sequence(86400 + 43200)" +
                            ") timestamp (ts) partition by DAY"
            );
            // A reader from before the split makes the squash copy its target rather than append to it in place.
            try (TableReader ignore = copyTarget ? getReader("x") : null) {
                execute(
                        "insert into x select cast(x as int) i," +
                                " timestamp_sequence('2020-02-04T20:01:00.5', 1000000L)::" + timestampType.getTypeName() + " ts" +
                                " from long_sequence(200)"
                );
                assertQuery("select count() c from table_partitions('x')")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("c\n3\n");

                // Added after every row: every folder holds them entirely under their column tops.
                execute("alter table x add column k long");
                execute("alter table x add column s varchar");
                execute("alter table x add column t string");
                execute("alter table x squash partitions");
            }
            engine.releaseAllWriters();

            assertQuery("select count() c from table_partitions('x')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("c\n2\n");
            assertQuery("select count() c, count(k) ck, count(s) cs, count(t) ct from x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("c\tck\tcs\tct\n129800\t0\t0\t0\n");

            // The squash wrote no value of these columns, so it must not have allocated their files either.
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(engine.verifyTableName("x").getDirName());
                final int tableLen = path.size();
                final java.io.File tableDir = new java.io.File(path.toString());
                final String[] partitionDirs = tableDir.list();
                Assert.assertNotNull(partitionDirs);
                int checkedFiles = 0;
                for (String partitionDir : partitionDirs) {
                    if (!partitionDir.startsWith("2020-02-04")) {
                        continue;
                    }
                    final String[] files = new java.io.File(tableDir, partitionDir).list();
                    Assert.assertNotNull(files);
                    for (String file : files) {
                        if (file.startsWith("k.") || file.startsWith("s.") || file.startsWith("t.")) {
                            path.trimTo(tableLen).concat(partitionDir).concat(file);
                            Assert.assertEquals(partitionDir + '/' + file, 0, ff.length(path.$()));
                            checkedFiles++;
                        }
                    }
                }
                Assert.assertTrue("the squash target must have the late columns' files", checkedFiles > 0);
            }
        });
    }

    private void testSquashPartitionsFails(FilesFacade ff, AtomicBoolean armed, String expectedError) throws Exception {
        assertMemoryLeak(ff, () -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 * (1 << 10));
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);
            engine.resetFrameFactory();

            // The squash grows the target by the 200 inserted rows only, past the page-rounded length its files were
            // truncated to on close. The wide pad column - 404 bytes a row - makes that growth outrun any page size,
            // 64KiB on Windows included, so the squash has to allocate.
            execute(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " rpad(x::string, 200, 'q') pad," +
                            " timestamp_sequence('2020-02-04T00', 60*1000000L)::" + timestampType.getTypeName() + " ts" +
                            " from long_sequence(60*36)" +
                            ") timestamp (ts) partition by DAY"
            );
            // A reader on the partition keeps the commit from squashing the split straight back.
            try (TableReader ignore = getReader("x")) {
                execute(
                        "insert into x select" +
                                " cast(x as int) * 1000000 i," +
                                " -x - 1000000L as j," +
                                " rnd_str(5,16,2) as str," +
                                " rpad(x::string, 200, 'q') pad," +
                                " timestamp_sequence('2020-02-04T20:01', 1000000L) ts" +
                                " from long_sequence(200)"
                );
            }

            final String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";
            final String splitPartitions = replaceTimestampSuffix1("""
                    minTimestamp\tnumRows\tname
                    2020-02-04T00:00:00.000000Z\t1201\t2020-02-04
                    2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001
                    2020-02-05T00:00:00.000000Z\t720\t2020-02-05
                    """, timestampType.getTypeName());
            assertQuery(partitionsSql).noLeakCheck().expectSize().noRandomAccess().returns(splitPartitions);
            execute("create table before_squash as (select * from x)");

            armed.set(true);
            try {
                execute("alter table x squash partitions");
                Assert.fail("squash should have failed");
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), expectedError);
            } finally {
                armed.set(false);
            }

            assertQuery(partitionsSql).noLeakCheck().expectSize().noRandomAccess().returns(splitPartitions);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "before_squash", "x", LOG);

            execute("alter table x squash partitions");
            assertQuery(partitionsSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(replaceTimestampSuffix1("""
                            minTimestamp\tnumRows\tname
                            2020-02-04T00:00:00.000000Z\t1640\t2020-02-04
                            2020-02-05T00:00:00.000000Z\t720\t2020-02-05
                            """, timestampType.getTypeName()));
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "before_squash", "x", LOG);
        });
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
