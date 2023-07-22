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

package io.questdb.test.griffin;

import io.questdb.Metrics;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class O3SquashPartitionTest extends AbstractGriffinTest {

    @Before
    public void setUp() {
        node1.getConfigurationOverrides().setPartitionO3SplitThreshold(4 << 10);
        super.setUp();
    }

    @Test
    public void testCannotSplitPartitionAllRowsSameTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            node1.getConfigurationOverrides().setPartitionO3SplitThreshold(1);
            node1.getConfigurationOverrides().setO3PartitionSplitMaxCount(2);
            long start = TimestampFormatUtils.parseTimestamp("2020-02-03");

            Metrics metrics = engine.getMetrics();
            int rowCount = (int) metrics.tableWriter().getPhysicallyWrittenRows();

            // create table with 800 points at 2020-02-03 sharp
            // and 200 points in at 2020-02-03T01
            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " cast(" + start + " + (x / 800) * 60 * 60 * 1000000L  as timestamp) ts" +
                            " from long_sequence(1000)" +
                            ") timestamp (ts) partition by DAY",
                    sqlExecutionContext
            );

            rowCount = assertRowCount(1000, rowCount);

            // Split at 2020-02-03
            compiler.compile(
                    "insert into x " +
                            "select" +
                            " cast(x as int) * 1000000 i," +
                            " -x - 1000000L as j," +
                            " rnd_str(5,16,2) as str," +
                            " cast('2020-02-03' as timestamp) ts" +
                            " from long_sequence(10)",
                    sqlExecutionContext
            );

            rowCount = assertRowCount(1010, rowCount);

            // Check that the partition is not split
            assertSql("select name from table_partitions('x')", "name\n" +
                    "2020-02-03\n");

            // Split at 2020-02-03T01
            compiler.compile(
                    "insert into x " +
                            "select" +
                            " cast(x as int) * 1000000 i," +
                            " -x - 1000000L as j," +
                            " rnd_str(5,16,2) as str," +
                            " cast('2020-02-03T00:30' as timestamp) ts" +
                            " from long_sequence(10)",
                    sqlExecutionContext
            );

            // Check that the partition is split
            assertSql("select name,numRows from table_partitions('x')", "name\tnumRows\n" +
                    "2020-02-03\t809\n" +
                    "2020-02-03T000000-000001\t211\n");

            assertRowCount(211, rowCount);
        });
    }

    @Test
    public void testSplitLastPartition() throws Exception {
        assertMemoryLeak(() -> {
            // 4kb prefix split threshold
            node1.getConfigurationOverrides().setPartitionO3SplitThreshold(4 * (1 << 10));
            node1.getConfigurationOverrides().setO3PartitionSplitMaxCount(2);
            int rowCount = (int) metrics.tableWriter().getPhysicallyWrittenRows();

            compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
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
                    " rnd_str(5,16,2) as str,";
            compile(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T20:01', 1000000L) ts" +
                            " from long_sequence(200)",
                    sqlExecutionContext
            );

            String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";
            assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1201\t2020-02-04\n" +
                    "2020-02-04T20:01:00.000000Z\t319\t2020-02-04T200000-000001\n");

            rowCount = assertRowCount(319, rowCount);

            // Partition "2020-02-04" squashed the new update

            try (TableReader ignore = getReader("x")) {
                compile(sqlPrefix +
                                " timestamp_sequence('2020-02-04T18:01', 60*1000000L) ts" +
                                " from long_sequence(50)",
                        sqlExecutionContext
                );

                // Partition "2020-02-04" cannot be squashed with the new update because it's locked by the reader
                assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                        "2020-02-04T00:00:00.000000Z\t1081\t2020-02-04\n" +
                        "2020-02-04T18:01:00.000000Z\t170\t2020-02-04T180000-000001\n" +
                        "2020-02-04T20:01:00.000000Z\t319\t2020-02-04T200000-000001\n");

                rowCount = assertRowCount(170, rowCount);
            }

            // should squash partitions into 2 pieces
            compile(sqlPrefix +
                            " timestamp_sequence('2020-02-04T18:01', 1000000L) ts" +
                            " from long_sequence(50)",
                    sqlExecutionContext
            );

            assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1301\t2020-02-04\n" +
                    "2020-02-04T20:01:00.000000Z\t319\t2020-02-04T200000-000001\n");

            rowCount = assertRowCount((170 + 50) * 2, rowCount);


            compile(sqlPrefix +
                            " timestamp_sequence('2020-02-04T22:01:13', 60*1000000L) ts" +
                            " from long_sequence(50)",
                    sqlExecutionContext
            );

            assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1301\t2020-02-04\n" +
                    "2020-02-04T20:01:00.000000Z\t369\t2020-02-04T200000-000001\n");

            int delta = 50;
            rowCount = assertRowCount(delta, rowCount);

            // commit in order rolls to the next partition, should squash partition "2020-02-04" to single part
            compile(sqlPrefix +
                            " timestamp_sequence('2020-02-05T01:01:15', 10*60*1000000L) ts" +
                            " from long_sequence(50)",
                    sqlExecutionContext
            );

            assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1670\t2020-02-04\n" +
                    "2020-02-05T01:01:15.000000Z\t50\t2020-02-05\n");

            delta = 369 + 50;
            assertRowCount(delta, rowCount);
        });
    }

    @Test
    public void testSplitLastPartitionAppend() throws Exception {
        assertMemoryLeak(() -> {
            // 4kb prefix split threshold
            node1.getConfigurationOverrides().setPartitionO3SplitThreshold(4 * (1 << 10));
            node1.getConfigurationOverrides().setO3PartitionSplitMaxCount(1);

            int rowCount = (int) metrics.tableWriter().getPhysicallyWrittenRows();
            compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
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
                    " rnd_str(5,16,2) as str,";
            compile(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T20:01', 1000000L) ts," +
                            " x + 2 as k" +
                            " from long_sequence(200)"
            );

            rowCount = assertRowCount(319 * 2, rowCount);

            String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";
            assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1520\t2020-02-04\n");

            // Append in order to check last partition opened for writing correctly.
            compile(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T22:01', 1000000L) ts," +
                            " x + 2 as k" +
                            " from long_sequence(200)"
            );

            assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1720\t2020-02-04\n");

            assertRowCount(200, rowCount);
        });
    }

    @Test
    public void testSplitLastPartitionAtExistingTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // create table with 2 points every hour for 1 day of 2020-02-03

            node1.getConfigurationOverrides().setPartitionO3SplitThreshold(1);
            node1.getConfigurationOverrides().setO3PartitionSplitMaxCount(2);
            long start = TimestampFormatUtils.parseTimestamp("2020-02-03");
            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " cast(" + start + " + (x / 2) * 60 * 60 * 1000000L  as timestamp) ts" +
                            " from long_sequence(2*24)" +
                            ") timestamp (ts) partition by DAY",
                    sqlExecutionContext
            );

            CompiledQuery cc = compiler.compile("select * from x where ts between '2020-02-03T17' and '2020-02-03T18'", sqlExecutionContext);
            try (RecordCursorFactory cursorFactory = cc.getRecordCursorFactory();
                 // Open reader
                 RecordCursor cursor = cursorFactory.getCursor(sqlExecutionContext)
            ) {
                // Check that the originally open reader does not see these changes
                sink.clear();
                TestUtils.printCursor(cursor, cursorFactory.getMetadata(), true, sink, TestUtils.printer);
                String expected = "i\tj\tstr\tts\n" +
                        "34\t-34\tOPHNIMY\t2020-02-03T17:00:00.000000Z\n" +
                        "35\t-35\tDTNPHFLPBNHGZWW\t2020-02-03T17:00:00.000000Z\n" +
                        "36\t-36\tNGTNLE\t2020-02-03T18:00:00.000000Z\n" +
                        "37\t-37\t\t2020-02-03T18:00:00.000000Z\n";
                TestUtils.assertEquals(expected, sink);

                // Split at 17:30
                compiler.compile(
                        "insert into x " +
                                "select" +
                                " cast(x as int) * 1000000 i," +
                                " -x - 1000000L as j," +
                                " rnd_str(5,16,2) as str," +
                                " timestamp_sequence('2020-02-03T17', 60*1000000L) ts" +
                                " from long_sequence(1)",
                        sqlExecutionContext
                );

                // Check that the originally open reader does not see these changes
                sink.clear();
                cursor.toTop();
                TestUtils.printCursor(cursor, cursorFactory.getMetadata(), true, sink, TestUtils.printer);
                TestUtils.assertEquals(expected, sink);

                // add data at 17:15
                compiler.compile(
                        "insert into x " +
                                "select" +
                                " cast(x as int) * 1000000 i," +
                                " -x - 1000000L as j," +
                                " rnd_str(5,16,2) as str," +
                                " timestamp_sequence('2020-02-03T17', 60*1000000L) ts" +
                                " from long_sequence(1)",
                        sqlExecutionContext
                );

                // Check that the originally open reader does not see these changes
                sink.clear();
                cursor.toTop();
                TestUtils.printCursor(cursor, cursorFactory.getMetadata(), true, sink, TestUtils.printer);
                TestUtils.assertEquals(expected, sink);
            }

            assertSql("select * from x where ts between '2020-02-03T17' and '2020-02-03T18'", "i\tj\tstr\tts\n" +
                    "34\t-34\tOPHNIMY\t2020-02-03T17:00:00.000000Z\n" +
                    "35\t-35\tDTNPHFLPBNHGZWW\t2020-02-03T17:00:00.000000Z\n" +
                    "1000000\t-1000001\tXEJCTIZKYFLUHZQS\t2020-02-03T17:00:00.000000Z\n" +
                    "1000000\t-1000001\tXMKJSM\t2020-02-03T17:00:00.000000Z\n" +
                    "36\t-36\tNGTNLE\t2020-02-03T18:00:00.000000Z\n" +
                    "37\t-37\t\t2020-02-03T18:00:00.000000Z\n");
        });
    }

    @Test
    public void testSplitLastPartitionLockedAndCannotBeAppended() throws Exception {
        assertMemoryLeak(() -> {
            // create table with 2 points every hour for 1 day of 2020-02-03

            node1.getConfigurationOverrides().setPartitionO3SplitThreshold(1);
            node1.getConfigurationOverrides().setO3PartitionSplitMaxCount(2);
            long start = TimestampFormatUtils.parseTimestamp("2020-02-03");
            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " cast(" + start + " + (x / 2) * 60 * 60 * 1000000L  as timestamp) ts" +
                            " from long_sequence(2*24)" +
                            ") timestamp (ts) partition by DAY",
                    sqlExecutionContext
            );

            CompiledQuery cc = compiler.compile("select * from x where ts between '2020-02-03T17' and '2020-02-03T18'", sqlExecutionContext);
            try (RecordCursorFactory cursorFactory = cc.getRecordCursorFactory();
                 // Open reader
                 RecordCursor cursor = cursorFactory.getCursor(sqlExecutionContext)
            ) {
                // Check that the originally open reader does not see these changes
                sink.clear();
                TestUtils.printCursor(cursor, cursorFactory.getMetadata(), true, sink, TestUtils.printer);
                String expected = "i\tj\tstr\tts\n" +
                        "34\t-34\tOPHNIMY\t2020-02-03T17:00:00.000000Z\n" +
                        "35\t-35\tDTNPHFLPBNHGZWW\t2020-02-03T17:00:00.000000Z\n" +
                        "36\t-36\tNGTNLE\t2020-02-03T18:00:00.000000Z\n" +
                        "37\t-37\t\t2020-02-03T18:00:00.000000Z\n";
                TestUtils.assertEquals(expected, sink);

                // Split at 17:30
                compiler.compile(
                        "insert into x " +
                                "select" +
                                " cast(x as int) * 1000000 i," +
                                " -x - 1000000L as j," +
                                " rnd_str(5,16,2) as str," +
                                " timestamp_sequence('2020-02-03T17:30', 60*1000000L) ts" +
                                " from long_sequence(1)",
                        sqlExecutionContext
                );

                // Check that the originally open reader does not see these changes
                sink.clear();
                cursor.toTop();
                TestUtils.printCursor(cursor, cursorFactory.getMetadata(), true, sink, TestUtils.printer);
                TestUtils.assertEquals(expected, sink);

                // add data at 17:15
                compiler.compile(
                        "insert into x " +
                                "select" +
                                " cast(x as int) * 1000000 i," +
                                " -x - 1000000L as j," +
                                " rnd_str(5,16,2) as str," +
                                " timestamp_sequence('2020-02-03T17:15', 60*1000000L) ts" +
                                " from long_sequence(1)",
                        sqlExecutionContext
                );

                // Check that the originally open reader does not see these changes
                sink.clear();
                cursor.toTop();
                TestUtils.printCursor(cursor, cursorFactory.getMetadata(), true, sink, TestUtils.printer);
                TestUtils.assertEquals(expected, sink);
            }
        });
    }

    @Test
    public void testSplitMidPartitionCheckIndex() throws Exception {
        assertMemoryLeak(() -> {
            compile(
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

            compile(
                    "create table z as (" +
                            "select" +
                            " cast(x as int) * 1000000 i," +
                            " -x - 1000000L as j," +
                            " rnd_symbol(null,'5','16','2') as sym," +
                            " timestamp_sequence('2020-02-04T23:01', 60*1000000L) ts" +
                            " from long_sequence(50))",
                    sqlExecutionContext
            );

            compile(
                    "create table y (" +
                            "i int," +
                            "j long," +
                            "sym symbol," +
                            "ts timestamp)",
                    sqlExecutionContext
            );
            compile("insert into y select * from x", sqlExecutionContext);
            compile("insert into y select * from z", sqlExecutionContext);

            compile("insert into x select * from z", sqlExecutionContext);
            TestUtils.assertSqlCursors(
                    compiler,
                    sqlExecutionContext,
                    "y order by ts",
                    "x",
                    LOG,
                    true
            );
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, "y where sym = '5' order by ts", "x where sym = '5'", LOG);
            TestUtils.assertIndexBlockCapacity(engine, "x", "sym");
        });
    }

    @Test
    public void testSplitMidPartitionFailedToSquash() throws Exception {
        // Windows uses mmap-based writes instead of copyData to squash split partitions.
        Assume.assumeFalse(Os.isWindows());

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
            node1.getConfigurationOverrides().setPartitionO3SplitThreshold(4 * (1 << 10));
            node1.getConfigurationOverrides().setO3PartitionSplitMaxCount(2);

            compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " timestamp_sequence('2020-02-04T00', 60*1000000L) ts" +
                            " from long_sequence(60*36)" +
                            ") timestamp (ts) partition by DAY"
            );

            compile("alter table x add column k int");

            String sqlPrefix = "insert into x " +
                    "select" +
                    " cast(x as int) * 1000000 i," +
                    " -x - 1000000L as j," +
                    " rnd_str(5,16,2) as str,";

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
            assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1201\t2020-02-04\n" +
                    "2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001\n" +
                    "2020-02-05T00:00:00.000000Z\t720\t2020-02-05\n");

            try {
                // Append another time and fail squashing var len column.
                failToCopyLen.set(13376);
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

            assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1201\t2020-02-04\n" +
                    "2020-02-04T20:01:00.000000Z\t639\t2020-02-04T200000-000001\n" +
                    "2020-02-05T00:00:00.000000Z\t720\t2020-02-05\n");

            // success
            failToCopyLen.set(0);
            compile(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T22:01', 1000000L) ts," +
                            " x + 2 as k" +
                            " from long_sequence(200)"
            );

            assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t2040\t2020-02-04\n" +
                    "2020-02-05T00:00:00.000000Z\t720\t2020-02-05\n");

        });
    }

    @Test
    public void testSplitMidPartitionOpenReader() throws Exception {
        assertMemoryLeak(() -> {
            compile(
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

            compile(
                    "create table z as (" +
                            "select" +
                            " cast(x as int) * 1000000 i," +
                            " -x - 1000000L as j," +
                            " rnd_symbol(null,'5','16','2') as sym," +
                            " timestamp_sequence('2020-02-04T23:01', 60*1000000L) ts" +
                            " from long_sequence(50))",
                    sqlExecutionContext
            );

            compile(
                    "create table y (" +
                            "i int," +
                            "j long," +
                            "sym symbol," +
                            "ts timestamp)",
                    sqlExecutionContext
            );
            compile("insert into y select * from x", sqlExecutionContext);
            compile("insert into y select * from z", sqlExecutionContext);

            try (TableReader ignore = getReader("x")) {
                compile("insert into x select * from z", sqlExecutionContext);

                TestUtils.assertSqlCursors(
                        compiler,
                        sqlExecutionContext,
                        "y order by ts",
                        "x",
                        LOG,
                        true
                );
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, "y where sym = '5' order by ts", "x where sym = '5'", LOG);
                assertSql("select name, minTimestamp from table_partitions('x')",
                        "name\tminTimestamp\n" +
                                "2020-02-03\t2020-02-03T13:00:00.000000Z\n" +
                                "2020-02-04\t2020-02-04T00:00:00.000000Z\n" +
                                "2020-02-04T230000-000001\t2020-02-04T23:01:00.000000Z\n" +
                                "2020-02-05\t2020-02-05T00:00:00.000000Z\n");
            }

            // Another reader, should allow to squash partitions
            try (TableReader ignore = getReader("x")) {
                compile("insert into x(ts) values('2020-02-06')", sqlExecutionContext);
                assertSql("select name, minTimestamp from table_partitions('x')", "name\tminTimestamp\n" +
                        "2020-02-03\t2020-02-03T13:00:00.000000Z\n" +
                        "2020-02-04\t2020-02-04T00:00:00.000000Z\n" +
                        "2020-02-05\t2020-02-05T00:00:00.000000Z\n" +
                        "2020-02-06\t2020-02-06T00:00:00.000000Z\n");
            }

            TestUtils.assertIndexBlockCapacity(engine, "x", "sym");
        });
    }

    @Test
    public void testSplitPartitionChangesColTop() throws Exception {
        assertMemoryLeak(() -> {
            // 4kb prefix split threshold
            node1.getConfigurationOverrides().setPartitionO3SplitThreshold(4 * (1 << 10));
            node1.getConfigurationOverrides().setO3PartitionSplitMaxCount(1);

            compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " timestamp_sequence('2020-02-04T00', 60*1000000L) ts" +
                            " from long_sequence(60*(23*2-24))" +
                            ") timestamp (ts) partition by DAY"
            );

            String sqlPrefix = "insert into x " +
                    "select" +
                    " cast(x as int) * 1000000 i," +
                    " -x - 1000000L as j," +
                    " rnd_str(5,16,2) as str,";
            String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";

            // Prevent squashing
            try (TableReader ignore = getReader("x")) {
                compile(
                        sqlPrefix +
                                " timestamp_sequence('2020-02-04T20:01', 1000000L) ts," +
                                " x + 2 as k" +
                                " from long_sequence(200)"
                );

                assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                        "2020-02-04T00:00:00.000000Z\t1201\t2020-02-04\n" +
                        "2020-02-04T20:01:00.000000Z\t319\t2020-02-04T200000-000001\n");
            }

            compile("alter table x add column k int");

            // Append in order to check last partition opened for writing correctly.
            compile(
                    sqlPrefix +
                            " timestamp_sequence('2020-02-04T22:01', 1000000L) ts," +
                            " x + 2 as k" +
                            " from long_sequence(200)"
            );

            assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1720\t2020-02-04\n");

        });
    }

    @Test
    public void testSquashPartitions() throws Exception {
        testSquashPartitions("");
    }

    @Test
    public void testSquashPartitionsWal() throws Exception {
        testSquashPartitions("WAL");
    }

    private int assertRowCount(int delta, int rowCount) {
        Assert.assertEquals(delta, getPhysicalRowsSinceLastCommit("x"));
        rowCount += delta;
        Assert.assertEquals(rowCount, metrics.tableWriter().getPhysicallyWrittenRows());
        return rowCount;
    }

    private long getPhysicalRowsSinceLastCommit(String table) {
        try (TableWriter tw = getWriter(table)) {
            return tw.getPhysicallyWrittenRowsSinceLastCommit();
        }
    }

    private void testSquashPartitions(String wal) throws Exception {
        assertMemoryLeak(() -> {
            // 4kb prefix split threshold
            node1.getConfigurationOverrides().setPartitionO3SplitThreshold(4 * (1 << 10));
            node1.getConfigurationOverrides().setO3PartitionSplitMaxCount(2);

            compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
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
                        " rnd_str(5,16,2) as str,";
                compile(
                        sqlPrefix +
                                " timestamp_sequence('2020-02-04T20:01', 1000000L) ts" +
                                " from long_sequence(200)",
                        sqlExecutionContext
                );
                drainWalQueue();

                String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";
                assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                        "2020-02-04T00:00:00.000000Z\t1201\t2020-02-04\n" +
                        "2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001\n" +
                        "2020-02-05T00:00:00.000000Z\t1320\t2020-02-05\n");

                compile(sqlPrefix +
                                " timestamp_sequence('2020-02-05T18:01', 60*1000000L) ts" +
                                " from long_sequence(50)",
                        sqlExecutionContext
                );
                drainWalQueue();

                // Partition "2020-02-04" cannot be squashed with the new update because it's locked by the reader
                assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                        "2020-02-04T00:00:00.000000Z\t1201\t2020-02-04\n" +
                        "2020-02-04T20:01:00.000000Z\t439\t2020-02-04T200000-000001\n" +
                        "2020-02-05T00:00:00.000000Z\t1081\t2020-02-05\n" +
                        "2020-02-05T18:01:00.000000Z\t289\t2020-02-05T180000-000001\n");

                // should squash partitions
                compile("alter table x squash partitions");

                drainWalQueue();
                assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                        "2020-02-04T00:00:00.000000Z\t1640\t2020-02-04\n" +
                        "2020-02-05T00:00:00.000000Z\t1370\t2020-02-05\n");
            }
        });
    }
}
