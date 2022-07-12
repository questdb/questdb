/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableModel;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;

public class ReaderPoolTableFunctionTest extends AbstractGriffinTest {
    @Test
    public void testSmoke() throws SqlException, NumericException {
        try (TableModel tm = new TableModel(configuration, "tab1", PartitionBy.NONE)) {
            tm.timestamp("ts").col("ID", ColumnType.INT);
            createPopulateTable(tm, 2, "2020-01-01", 1);
        }

        assertSql("select * from tab1", "ts\tID\n" +
                "2020-01-01T00:00:00.000000Z\t1\n" +
                "2020-01-01T00:00:00.000000Z\t2\n");

        assertSql("select table, owner, txn from reader_pool", "table\towner\ttxn\n" +
                "tab1\t-1\t1\n");
    }

    @Test
    public void testRecursiveAcquireAndRelease() throws Exception {
        String tableName = "tab1";
        // create a table
        try (TableModel tm = new TableModel(configuration, tableName, PartitionBy.NONE)) {
            tm.timestamp("ts").col("ID", ColumnType.INT);
            createPopulateTable(tm, 20, "2020-01-01", 1);
        }

        // add 3 more transactions
        for (int i = 0; i < 3; i++) {
            executeTx(tableName);
        }

        int readerAcquisitionCount = 10;
        long startTime = MicrosecondClockImpl.INSTANCE.getTicks();
        long threadId = Thread.currentThread().getId();
        long allReadersAcquiredTime = acquireReaderAndRun(tableName, readerAcquisitionCount, () -> {
            assertReaderPool(readerAcquisitionCount, recordValidator(startTime, "tab1", threadId, 4));
            return MicrosecondClockImpl.INSTANCE.getTicks();
        });

        // all readers should be released. there should have a timestamp set >= timestamp when all readers were acquired
        assertReaderPool(readerAcquisitionCount, recordValidator(allReadersAcquiredTime, tableName, -1, 4));
    }

    private static ReaderPoolRowValidator recordValidator(long startTime, CharSequence expectedTableName, long expectedOwner, long expectedTxn) {
        return (table, owner, txn, timestamp) -> {
            // todo: should this be weaker? what is there is a maintenance job reading an internal table?
            // perhaps we should skip all records not matching the expected table name?
            TestUtils.assertEquals(expectedTableName, table);
            Assert.assertEquals(expectedOwner, owner);
            Assert.assertEquals(expectedTxn, txn);
            assertTimestampBetween(timestamp, startTime, MicrosecondClockImpl.INSTANCE.getTicks());
        };
    }

    @Nullable
    private static void assertReaderPool(int expectedRowCount, ReaderPoolRowValidator validator) throws Exception {
        try (RecordCursorFactory factory = compiler.compile("select * from reader_pool order by table", sqlExecutionContext).getRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            RecordMetadata metadata = factory.getMetadata();
            int i = 0;
            Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                i++;
                CharSequence table = record.getStr(metadata.getColumnIndex("table"));
                long owner = record.getLong(metadata.getColumnIndex("owner"));
                long txn = record.getLong(metadata.getColumnIndex("txn"));
                long timestamp = record.getTimestamp(metadata.getColumnIndex("timestamp"));
                validator.validate(table, owner, txn, timestamp);
            }
            // todo: should this be a weaker comparison? what is a reader gets evicted?
            Assert.assertEquals(expectedRowCount, i);
        }
    }

    private interface ReaderPoolRowValidator {
        void validate(CharSequence table, long owner, long txn, long timestamp);
    }

    private static void assertTimestampBetween(long timestamp, long lowerBoundInc, long upperBoundInc) {
        Assert.assertTrue("timestamp < lower bound. [timestamp=" + timestamp + ", lower-bound="+lowerBoundInc + "]", timestamp >= lowerBoundInc);
        Assert.assertTrue("timestamp > upper bound. [timestamp=" + timestamp + ", upper-bound="+upperBoundInc + "]", timestamp <= upperBoundInc);
    }

    private static void executeTx(CharSequence tableName) throws SqlException {
        compiler.compile("insert into " + tableName + " values (now(), 42)", sqlExecutionContext).execute(null).await();
    }

    /**
     * Acquire n TableReaders for a given table and then call a task
     *
     * @param tableName name of the table to acquire
     * @param depth how many readers to acquire before running a task
     * @param callable a task to run after a desired number of readers were acquired
     * @return timestamp when all readers were acquired
     * @param <T>
     * @throws Exception
     */
    private <T> T acquireReaderAndRun(String tableName, int depth, Callable<T> callable) throws Exception {
        if (depth == 0) {
            return callable.call();
        }
        try (TableReader ignored = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
            return acquireReaderAndRun(tableName, depth - 1, callable);
        }
    }
}
