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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.NumericException;
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
    public void testSmoke2() throws SqlException, NumericException {
        long thread = Thread.currentThread().getId();
        try (TableModel tm = new TableModel(configuration, "tab1", PartitionBy.NONE)) {
            tm.timestamp("ts").col("ID", ColumnType.INT);
            createPopulateTable(tm, 20, "2020-01-01", 1);
        }

        for (int i = 0; i < 3; i++) {
            executeTx();
        }

        try (RecordCursorFactory factory = compiler.compile("select * from tab1", sqlExecutionContext).getRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            cursor.hasNext();
            assertSql("select table, owner, txn from reader_pool", "table\towner\ttxn\n" +
                    "tab1\t" + thread + "\t4\n");
        }

        assertSql("select table, owner, txn from reader_pool", "table\towner\ttxn\n" +
                "tab1\t-1\t4\n");
    }

    @Test
    public void testSmoke3() throws Exception {
        long thread = Thread.currentThread().getId();

        try (TableModel tm = new TableModel(configuration, "tab1", PartitionBy.NONE)) {
            tm.timestamp("ts").col("ID", ColumnType.INT);
            createPopulateTable(tm, 20, "2020-01-01", 1);
        }

        for (int i = 0; i < 3; i++) {
            executeTx();
        }

        int acquisitionCount = 100;
        acquireReaderAndRun("tab1", acquisitionCount, () -> {
            StringBuilder sb = new StringBuilder("table\towner\ttxn\n");
            for (int i = 0; i < acquisitionCount; i++) {
                sb.append("tab1\t" + thread + "\t4\n");
            }
            assertSql("select table, owner, txn from reader_pool", sb.toString());
            return null;
        });

        StringBuilder sb = new StringBuilder("table\towner\ttxn\n");
        for (int i = 0; i < acquisitionCount; i++) {
            sb.append("tab1\t-1\t4\n");
        }
        assertSql("select table, owner, txn from reader_pool", sb.toString());
    }

    private static void executeTx() throws SqlException {
        compiler.compile("insert into tab1 values (now(), 42)", sqlExecutionContext).execute(null).await();
    }

    private <T> T acquireReaderAndRun(String tableName, int depth, Callable<T> runnable) throws Exception {
        if (depth == 0) {
            return runnable.call();
        }
        try (TableReader ignored = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
            return acquireReaderAndRun(tableName, depth - 1, runnable);
        }
    }
}
