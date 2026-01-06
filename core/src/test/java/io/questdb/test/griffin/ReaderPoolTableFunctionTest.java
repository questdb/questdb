/*******************************************************************************
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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.table.ReaderPoolFunctionFactory;
import io.questdb.griffin.engine.table.ReaderPoolRecordCursorFactory;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;

import static org.junit.Assert.assertTrue;

public class ReaderPoolTableFunctionTest extends AbstractCairoTest {
    @Test
    public void testCursorDoesHaveUpfrontSize() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    RecordCursorFactory factory = select("select * from reader_pool()");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertEquals(-1, cursor.size());
            }
        });
    }

    @Test
    public void testCursorNotRuntimeConstant() throws Exception {
        assertMemoryLeak(() -> {
            try (Function cursorFunction = new ReaderPoolFunctionFactory().newInstance(0, new ObjList<>(), new IntList(), configuration, sqlExecutionContext)) {
                Assert.assertFalse(cursorFunction.isRuntimeConstant());
            }
        });
    }

    @Test
    public void testEmptyPool() throws Exception {
        assertQuery("table_name\towner_thread_id\tlast_access_timestamp\tcurrent_txn\n", "select * from reader_pool()", null);
    }

    @Test
    public void testFactoryDoesNotSupportRandomAccess() throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = select("select * from reader_pool()")) {
                Assert.assertFalse(factory.recordCursorSupportsRandomAccess());
            }
        });
    }

    @Test
    public void testMetadata() throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = select("select * from reader_pool()")) {
                RecordMetadata metadata = factory.getMetadata();
                Assert.assertEquals(4, metadata.getColumnCount());
                Assert.assertEquals("table_name", metadata.getColumnName(0));
                Assert.assertEquals("owner_thread_id", metadata.getColumnName(1));
                Assert.assertEquals("last_access_timestamp", metadata.getColumnName(2));
                Assert.assertEquals("current_txn", metadata.getColumnName(3));
                Assert.assertEquals(ColumnType.STRING, metadata.getColumnType(0));
                Assert.assertEquals(ColumnType.LONG, metadata.getColumnType(1));
                Assert.assertEquals(ColumnType.TIMESTAMP, metadata.getColumnType(2));
                Assert.assertEquals(ColumnType.LONG, metadata.getColumnType(3));
            }
        });
    }

    @Test
    public void testMultipleTables() throws Exception {
        assertMemoryLeak(() -> {
            // create a table
            TableModel tm = new TableModel(configuration, "tab1", PartitionBy.NONE);
            tm.timestamp("ts").col("ID", ColumnType.INT);
            createPopulateTable(tm, 20, "2020-01-01", 1);
            tm = new TableModel(configuration, "tab2", PartitionBy.NONE);
            tm.timestamp("ts").col("ID", ColumnType.INT);
            createPopulateTable(tm, 20, "2020-01-01", 1);

            int readerAcquisitionCount = configuration.getPoolSegmentSize() * 2;
            long startTime = MicrosecondClockImpl.INSTANCE.getTicks();
            long threadId = Thread.currentThread().getId();

            long allReadersAcquiredTime = acquireReaderAndRun(
                    "tab1",
                    readerAcquisitionCount, () -> acquireReaderAndRun("tab2", readerAcquisitionCount,
                            () -> {
                                assertReaderPool(readerAcquisitionCount * 2, eitherOf(
                                        recordValidator(startTime, "tab1", threadId, 1),
                                        recordValidator(startTime, "tab2", threadId, 1))
                                );
                                return MicrosecondClockImpl.INSTANCE.getTicks();
                            }));

            // all readers should be released. there should have a timestamp set >= timestamp when all readers were acquired
            assertReaderPool(readerAcquisitionCount * 2, eitherOf(
                    recordValidator(allReadersAcquiredTime, "tab1", Numbers.LONG_NULL, 1),
                    recordValidator(allReadersAcquiredTime, "tab2", Numbers.LONG_NULL, 1))
            );
        });
    }

    @Test
    public void testRandomAccessUnsupported() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    RecordCursorFactory readerPoolFactory = new ReaderPoolRecordCursorFactory(sqlExecutionContext.getCairoEngine());
                    RecordCursor readerPoolCursor = readerPoolFactory.getCursor(sqlExecutionContext)
            ) {
                Record record = readerPoolCursor.getRecord();
                readerPoolCursor.recordAt(record, 0);
                Assert.fail("Random access is not expected to be implemented");
            } catch (UnsupportedOperationException ignored) {
            }
        });
    }

    @Test
    public void testRecordBNotImplemented() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    RecordCursorFactory readerPoolFactory = new ReaderPoolRecordCursorFactory(sqlExecutionContext.getCairoEngine());
                    RecordCursor readerPoolCursor = readerPoolFactory.getCursor(sqlExecutionContext)
            ) {
                readerPoolCursor.getRecordB();
                Assert.fail("RecordB is not expected to be implemented");
            } catch (UnsupportedOperationException ignored) {
            }
        });
    }

    @Test
    public void testRecursiveAcquireAndRelease() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "tab1";
            // create a table
            TableModel tm = new TableModel(configuration, tableName, PartitionBy.NONE);
            tm.timestamp("ts").col("ID", ColumnType.INT);
            createPopulateTable(tm, 20, "2020-01-01", 1);

            // add 3 more transactions
            for (int i = 0; i < 3; i++) {
                executeTx(tableName);
            }

            int readerAcquisitionCount = configuration.getPoolSegmentSize() * 2;
            long startTime = MicrosecondClockImpl.INSTANCE.getTicks();
            long threadId = Thread.currentThread().getId();
            long allReadersAcquiredTime = acquireReaderAndRun(tableName, readerAcquisitionCount, () -> {
                assertReaderPool(readerAcquisitionCount, recordValidator(startTime, "tab1", threadId, 4));
                return MicrosecondClockImpl.INSTANCE.getTicks();
            });

            // all readers should be released. they should have a timestamp set >= timestamp when all readers were acquired
            assertReaderPool(readerAcquisitionCount, recordValidator(allReadersAcquiredTime, tableName, Numbers.LONG_NULL, 4));
        });
    }

    @Test
    public void testReleaseAcquisitionTimeRecorded() throws Exception {
        assertMemoryLeak(() -> {
            // create a table
            TableModel tm = new TableModel(configuration, "tab1", PartitionBy.NONE);
            tm.timestamp("ts").col("ID", ColumnType.INT);
            createPopulateTable(tm, 20, "2020-01-01", 1);

            long startTime = MicrosecondClockImpl.INSTANCE.getTicks();
            long threadId = Thread.currentThread().getId();
            // first check reader acquisition set a timestamp
            // the timestamp has to be greater or equals to clock before a reader was acquired
            long allReadersAcquiredTime = acquireReaderAndRun("tab1", 1, () -> {
                assertReaderPool(1, recordValidator(startTime, "tab1", threadId, 1));
                return waitUntilClockProgress();
            });

            // check table reader timestamp was updated when it was returned to the pool
            assertTrue(allReadersAcquiredTime > startTime);
            assertReaderPool(1, recordValidator(allReadersAcquiredTime, "tab1", Numbers.LONG_NULL, 1));

            // acquire again and check timestamp made progress
            // this is to make sure time is updated on re-acquisition too
            long before2ndAcquireTime = waitUntilClockProgress();
            acquireReaderAndRun("tab1", 1, () -> {
                assertReaderPool(1, recordValidator(before2ndAcquireTime, "tab1", threadId, 1));
                return waitUntilClockProgress();
            });
        });
    }

    @Test
    public void testSmoke() throws Exception {
        assertMemoryLeak(() -> {
            TableModel tm = new TableModel(configuration, "tab1", PartitionBy.NONE);
            tm.timestamp("ts").col("ID", ColumnType.INT);
            createPopulateTable(tm, 2, "2020-01-01", 1);

            assertSql("ts\tID\n" +
                    "2020-01-01T00:00:00.000000Z\t1\n" +
                    "2020-01-01T00:00:00.000000Z\t2\n", "select * from tab1");

            assertQueryNoLeakCheck(
                    "table_name\towner_thread_id\tcurrent_txn\n" +
                            "tab1\tnull\t1\n",
                    "select table_name, owner_thread_id, current_txn from reader_pool",
                    null
            );
        });
    }

    @Test
    public void testToTop() throws Exception {
        assertMemoryLeak(() -> {
            TableModel tm = new TableModel(configuration, "tab1", PartitionBy.NONE);
            tm.timestamp("ts").col("ID", ColumnType.INT);
            createPopulateTable(tm, 20, "2020-01-01", 1);

            try (TableReader ignored = getReader("tab1");
                 RecordCursorFactory readerPoolFactory = new ReaderPoolRecordCursorFactory(sqlExecutionContext.getCairoEngine());
                 RecordCursor readerPoolCursor = readerPoolFactory.getCursor(sqlExecutionContext)) {
                // scroll cursor ignoring its contents
                //noinspection StatementWithEmptyBody
                while (readerPoolCursor.hasNext()) {
                }
                readerPoolCursor.toTop();
                assertTrue(readerPoolCursor.hasNext());
            }
        });
    }

    private static void assertReaderPool(int expectedRowCount, ReaderPoolRowValidator validator) throws Exception {
        try (
                RecordCursorFactory factory = select("select * from reader_pool() order by table_name");
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            RecordMetadata metadata = factory.getMetadata();
            int i = 0;
            Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                CharSequence table = record.getStrA(metadata.getColumnIndex("table_name"));
                long owner = record.getLong(metadata.getColumnIndex("owner_thread_id"));
                long txn = record.getLong(metadata.getColumnIndex("current_txn"));
                long timestamp = record.getTimestamp(metadata.getColumnIndex("last_access_timestamp"));
                if (validator.validate(table, owner, txn, timestamp)) {
                    i++;
                }
            }
            Assert.assertEquals(expectedRowCount, i);
        }
    }

    private static void assertTimestampBetween(long timestamp, long lowerBoundInc, long upperBoundInc) {
        assertTrue("timestamp < lower bound. [timestamp=" + timestamp + ", lower-bound=" + lowerBoundInc + "]", timestamp >= lowerBoundInc);
        assertTrue("timestamp > upper bound. [timestamp=" + timestamp + ", upper-bound=" + upperBoundInc + "]", timestamp <= upperBoundInc);
    }

    /**
     * Combine two validator. Returns true when either validator returns true. It's short-circuiting. This means:
     * 1. When the first validation returns true then the 2nd validator won't be called at all
     * 2. When the first validation throws then the exception is propagated to a caller and the 2nd validator won't be called at all
     *
     * @param firstValidator  first validator
     * @param secondValidator second validator
     * @return Validator instance
     */
    private static ReaderPoolRowValidator eitherOf(ReaderPoolRowValidator firstValidator, ReaderPoolRowValidator secondValidator) {
        return (table, owner, txn, timestamp) -> firstValidator.validate(table, owner, txn, timestamp) || secondValidator.validate(table, owner, txn, timestamp);
    }

    private static void executeTx(CharSequence tableName) throws SqlException {
        execute("insert into " + tableName + " values (now(), 42)");
    }

    private static ReaderPoolRowValidator recordValidator(long startTime, CharSequence applicableTableName, long expectedOwner, long expectedTxn) {
        return (table, owner, txn, timestamp) -> {
            if (!Chars.equals(table, applicableTableName)) {
                // this record belongs to another table, skip it
                return false;
            }
            TestUtils.assertEquals(applicableTableName, table);
            Assert.assertEquals(expectedOwner, owner);
            Assert.assertEquals(expectedTxn, txn);
            assertTimestampBetween(timestamp, startTime, MicrosecondClockImpl.INSTANCE.getTicks());
            return true;
        };
    }

    private static long waitUntilClockProgress() {
        long start = MicrosecondClockImpl.INSTANCE.getTicks();
        long now;
        do {
            now = MicrosecondClockImpl.INSTANCE.getTicks();
        } while (now <= start);
        return now;
    }

    /**
     * Acquire n TableReaders for a given table and then call a task
     *
     * @param tableName name of the table to acquire
     * @param depth     how many readers to acquire before running a task
     * @param callable  a task to run after a desired number of readers were acquired
     * @return timestamp when all readers were acquired
     * @throws Exception on error and then the task throws an exception
     */
    private long acquireReaderAndRun(String tableName, int depth, Callable<Long> callable) throws Exception {
        if (depth == 0) {
            return callable.call();
        }
        try (TableReader ignored = getReader(tableName)) {
            return acquireReaderAndRun(tableName, depth - 1, callable);
        }
    }

    /**
     * Validate a given reader pool entry
     * When validator is not applicable for given entry then returns false.
     * When validator is applicable and a record passes validation then returns true.
     * When validator is applicable and a record does not pass validation then throw AssertionError
     */
    private interface ReaderPoolRowValidator {
        boolean validate(CharSequence table, long owner, long txn, long timestamp);
    }

}
