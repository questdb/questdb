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

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.mp.TestWorkerPool;
import io.questdb.mp.WorkerPool;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import static io.questdb.griffin.CompiledQuery.ALTER;

public class AlterTableDropActivePartitionTest extends AbstractGriffinTest {

    private static final String TableHeader = "id\ttimestamp\n";
    private static final String MinMaxCountHeader = "min\tmax\tcount\n";
    private static final String LastPartitionTs = "2024-10-15";
    private static final String EmptyTableMinMaxCount = MinMaxCountHeader + "\t\t0\n";

    @Test
    public void testDropOnlyPartitionNoReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = testName.getMethodName();
                    createTableX(tableName,
                            TableHeader +
                                    "5\t2024-10-15T00:00:00.000000Z\n",
                            "insert into " + tableName + " values(5, '2024-10-15T00:00:00.000000Z')");
                    dropPartition(tableName, LastPartitionTs);
                    assertTableX(tableName, TableHeader, EmptyTableMinMaxCount);
                }
        );
    }

    @Test
    public void testDropOnlyPartitionWithReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {

                    final String tableName = testName.getMethodName();
                    createTableX(tableName,
                            TableHeader +
                                    "5\t2024-10-15T00:00:00.000000Z\n" +
                                    "111\t2024-10-15T11:11:11.111111Z\n",
                            "insert into " + tableName + " values(5, '2024-10-15T00:00:00.000000Z')",
                            "insert into " + tableName + " values(111, '2024-10-15T11:11:11.111111Z')");

                    final String inTransactionReaderExpected = "id\ttimestamp\n" +
                            "777\t2024-10-13T00:10:00.000000Z\n" +
                            "888\t2024-10-15T00:00:00.000000Z\n" +
                            "5\t2024-10-15T00:00:00.000000Z\n" +
                            "111\t2024-10-15T11:11:11.111111Z\n";

                    final String afterDropActivePartitionExpected = TableHeader + "777\t2024-10-13T00:10:00.000000Z\n";

                    try (TableReader reader0 = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                        insert("insert into " + tableName + " values(888, '2024-10-15T00:00:00.000000Z');");
                        try (TableReader reader1 = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                            insert("insert into " + tableName + " values(777, '2024-10-13T00:10:00.000000Z');"); // o3

                            Assert.assertEquals(2, reader0.size());
                            Assert.assertEquals(3, reader1.size());
                            assertSql(tableName, inTransactionReaderExpected);

                            dropPartition(tableName, LastPartitionTs);

                            Assert.assertEquals(2, reader0.size());
                            Assert.assertEquals(3, reader1.size());
                            reader0.reload();
                            reader1.reload();
                            Assert.assertEquals(1, reader0.size());
                            Assert.assertEquals(1, reader1.size());
                            try (RecordCursorFactory factory = compiler.compile(tableName, sqlExecutionContext).getRecordCursorFactory()) {
                                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                    assertCursor(afterDropActivePartitionExpected, cursor, factory.getMetadata(), true);
                                }
                            }
                            assertFactoryMemoryUsage();
                        }
                    }
                    assertTableX(tableName, afterDropActivePartitionExpected, MinMaxCountHeader +
                            "2024-10-13T00:10:00.000000Z\t2024-10-13T00:10:00.000000Z\t1\n");
                }
        );
    }

    @Test
    public void testDropPartitionNoReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = testName.getMethodName();
                    createTableX(tableName,
                            TableHeader +
                                    "1\t2024-10-10T00:00:00.000000Z\n" +
                                    "2\t2024-10-11T00:00:00.000000Z\n" +
                                    "3\t2024-10-12T00:00:00.000000Z\n" +
                                    "4\t2024-10-12T00:00:01.000000Z\n" +
                                    "6\t2024-10-12T00:00:02.000000Z\n" +
                                    "5\t2024-10-15T00:00:00.000000Z\n",
                            "insert into " + tableName + " values(1, '2024-10-10T00:00:00.000000Z')",
                            "insert into " + tableName + " values(2, '2024-10-11T00:00:00.000000Z')",
                            "insert into " + tableName + " values(3, '2024-10-12T00:00:00.000000Z')",
                            "insert into " + tableName + " values(4, '2024-10-12T00:00:01.000000Z')",
                            "insert into " + tableName + " values(5, '2024-10-15T00:00:00.000000Z')",
                            "insert into " + tableName + " values(6, '2024-10-12T00:00:02.000000Z')");
                    dropPartition(tableName, LastPartitionTs);
                    assertTableX(tableName, TableHeader +
                                    "1\t2024-10-10T00:00:00.000000Z\n" +
                                    "2\t2024-10-11T00:00:00.000000Z\n" +
                                    "3\t2024-10-12T00:00:00.000000Z\n" +
                                    "4\t2024-10-12T00:00:01.000000Z\n" +
                                    "6\t2024-10-12T00:00:02.000000Z\n",
                            MinMaxCountHeader +
                                    "2024-10-10T00:00:00.000000Z\t2024-10-12T00:00:02.000000Z\t5\n");
                }
        );
    }

    @Test
    public void testDropPartitionWithReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = testName.getMethodName();

                    final String expectedTable = TableHeader +
                            "1\t2024-10-10T00:00:00.000000Z\n" +
                            "2\t2024-10-11T00:00:00.000000Z\n" +
                            "3\t2024-10-12T00:00:00.000000Z\n" +
                            "4\t2024-10-12T00:00:01.000000Z\n" +
                            "6\t2024-10-12T00:00:02.000000Z\n" +
                            "5\t2024-10-15T00:00:00.000000Z\n";

                    final String expectedTableInTransaction = TableHeader +
                            "1\t2024-10-10T00:00:00.000000Z\n" +
                            "2\t2024-10-11T00:00:00.000000Z\n" +
                            "3\t2024-10-12T00:00:00.000000Z\n" +
                            "4\t2024-10-12T00:00:01.000000Z\n" +
                            "6\t2024-10-12T00:00:02.000000Z\n" +
                            "8\t2024-10-12T00:00:05.000001Z\n" +
                            "7\t2024-10-15T00:00:01.000000Z\n";

                    final String expectedTableAfterFirstDrop = TableHeader +
                            "1\t2024-10-10T00:00:00.000000Z\n" +
                            "2\t2024-10-11T00:00:00.000000Z\n" +
                            "3\t2024-10-12T00:00:00.000000Z\n" +
                            "4\t2024-10-12T00:00:01.000000Z\n" +
                            "6\t2024-10-12T00:00:02.000000Z\n";

                    final String expectedTableAfterSecondDrop = TableHeader +
                            "1\t2024-10-10T00:00:00.000000Z\n" +
                            "2\t2024-10-11T00:00:00.000000Z\n" +
                            "3\t2024-10-12T00:00:00.000000Z\n" +
                            "4\t2024-10-12T00:00:01.000000Z\n" +
                            "6\t2024-10-12T00:00:02.000000Z\n" +
                            "8\t2024-10-12T00:00:05.000001Z\n";

                    createTableX(tableName,
                            expectedTable,
                            "insert into " + tableName + " values(1, '2024-10-10T00:00:00.000000Z')",
                            "insert into " + tableName + " values(2, '2024-10-11T00:00:00.000000Z')",
                            "insert into " + tableName + " values(3, '2024-10-12T00:00:00.000000Z')",
                            "insert into " + tableName + " values(4, '2024-10-12T00:00:01.000000Z')",
                            "insert into " + tableName + " values(5, '2024-10-15T00:00:00.000000Z')",
                            "insert into " + tableName + " values(6, '2024-10-12T00:00:02.000000Z')");
                    try (
                            TableReader reader0 = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName);
                            TableReader reader1 = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)
                    ) {
                        assertSql(tableName, expectedTable);
                        Assert.assertEquals(6, reader0.size());
                        Assert.assertEquals(6, reader1.size());

                        dropPartition(tableName, LastPartitionTs);
                        reader0.reload();
                        reader1.reload();
                        Assert.assertEquals(5, reader0.size());
                        Assert.assertEquals(5, reader1.size());
                        assertSql(tableName, expectedTableAfterFirstDrop);

                        insert("insert into " + tableName + " values(8, '2024-10-12T00:00:05.000001Z')");
                        insert("insert into " + tableName + " values(7, '2024-10-15T00:00:01.000000Z')");
                        assertSql(tableName, expectedTableInTransaction);
                        reader0.reload();
                        reader1.reload();
                        Assert.assertEquals(7, reader0.size());
                        Assert.assertEquals(7, reader1.size());
                        dropPartition(tableName, LastPartitionTs);
                    }
                    assertTableX(tableName, expectedTableAfterSecondDrop, MinMaxCountHeader +
                            "2024-10-10T00:00:00.000000Z\t2024-10-12T00:00:05.000001Z\t6\n");
                }
        );
    }


    @Test
    public void testCannotDropWhenThereIsAWriter() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = testName.getMethodName();
                    createTableX(tableName,
                            TableHeader +
                                    "1\t2024-10-10T00:00:00.000000Z\n" +
                                    "2\t2024-10-11T00:00:00.000000Z\n" +
                                    "3\t2024-10-12T00:00:00.000000Z\n" +
                                    "4\t2024-10-12T00:00:01.000000Z\n" +
                                    "6\t2024-10-12T00:00:02.000000Z\n" +
                                    "5\t2024-10-15T00:00:00.000000Z\n",
                            "insert into " + tableName + " values(1, '2024-10-10T00:00:00.000000Z')",
                            "insert into " + tableName + " values(2, '2024-10-11T00:00:00.000000Z')",
                            "insert into " + tableName + " values(3, '2024-10-12T00:00:00.000000Z')",
                            "insert into " + tableName + " values(4, '2024-10-12T00:00:01.000000Z')",
                            "insert into " + tableName + " values(5, '2024-10-15T00:00:00.000000Z')",
                            "insert into " + tableName + " values(6, '2024-10-12T00:00:02.000000Z')");
                    try (TableWriter ignore = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "testing")) {
                        dropPartition(tableName, LastPartitionTs);
                        Assert.fail();
                    } catch (EntryUnavailableException ex) {
                        TestUtils.assertContains("[-1] table busy [reason=testing]", ex.getFlyweightMessage());
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropOnlyPartitionWithUncommittedRowsNoReaders0() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = testName.getMethodName();
                    createTableX(tableName, TableHeader); // empty table
                    try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "testing")) {
                        long lastTs = TimestampFormatUtils.parseTimestamp(LastPartitionTs + "T00:00:00.000000Z");
                        long o3Ts = TimestampFormatUtils.parseTimestamp("2024-10-14T23:59:59.999999Z");

                        TableWriter.Row row = writer.newRow(lastTs); // will not survive
                        row.putInt(0, 100);
                        row.append();

                        row = writer.newRow(o3Ts); // will not survive
                        row.putInt(0, 300);
                        row.append();

                        writer.removePartition(lastTs);
                    }
                    assertTableX(tableName, TableHeader, EmptyTableMinMaxCount);
                }
        );
    }

    @Test
    public void testDropOnlyPartitionWithUncommittedRowsNoReaders1() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = testName.getMethodName();
                    createTableX(tableName, TableHeader); // empty table
                    try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "testing")) {
                        long lastTs = TimestampFormatUtils.parseTimestamp(LastPartitionTs + "T00:00:00.000000Z");
                        long o3Ts = TimestampFormatUtils.parseTimestamp("2024-10-14T23:59:59.999999Z");

                        TableWriter.Row row = writer.newRow(o3Ts); // expected to survive
                        row.putInt(0, 300);
                        row.append();

                        row = writer.newRow(lastTs); // will not survive
                        row.putInt(0, 100);
                        row.append();

                        writer.removePartition(lastTs);
                    }
                    assertTableX(tableName, TableHeader +
                                    "300\t2024-10-14T23:59:59.999999Z\n",
                            MinMaxCountHeader +
                            "2024-10-14T23:59:59.999999Z\t2024-10-14T23:59:59.999999Z\t1\n");
                }
        );
    }

    @Test
    public void testDropPartitionWithUncommittedRowsNoReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = testName.getMethodName();
                    createTableX(tableName,
                            TableHeader +
                                    "1\t2024-10-10T00:00:00.000000Z\n" +
                                    "2\t2024-10-11T00:00:00.000000Z\n" +
                                    "3\t2024-10-12T00:00:00.000000Z\n" +
                                    "4\t2024-10-12T00:00:01.000000Z\n" +
                                    "6\t2024-10-12T00:00:02.000000Z\n" +
                                    "5\t2024-10-15T00:00:00.000000Z\n",
                            "insert into " + tableName + " values(1, '2024-10-10T00:00:00.000000Z')",
                            "insert into " + tableName + " values(2, '2024-10-11T00:00:00.000000Z')",
                            "insert into " + tableName + " values(3, '2024-10-12T00:00:00.000000Z')",
                            "insert into " + tableName + " values(4, '2024-10-12T00:00:01.000000Z')",
                            "insert into " + tableName + " values(5, '2024-10-15T00:00:00.000000Z')",
                            "insert into " + tableName + " values(6, '2024-10-12T00:00:02.000000Z')");
                    try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "testing")) {
                        long lastTs = TimestampFormatUtils.parseTimestamp(LastPartitionTs + "T00:00:00.000000Z");

                        TableWriter.Row row = writer.newRow(lastTs); // expected to be lost
                        row.putInt(0, 100);
                        row.append();

                        row = writer.newRow(TimestampFormatUtils.parseTimestamp("2024-10-10T00:00:07.000000Z")); // expected to survive
                        row.putInt(0, 50);
                        row.append();

                        row = writer.newRow(TimestampFormatUtils.parseTimestamp("2024-10-12T10:00:03.000000Z")); // expected to be lost
                        row.putInt(0, 75);
                        row.append();

                        writer.removePartition(lastTs);
                    }
                    assertTableX(tableName, TableHeader +
                                    "1\t2024-10-10T00:00:00.000000Z\n" +
                                    "50\t2024-10-10T00:00:07.000000Z\n" +
                                    "2\t2024-10-11T00:00:00.000000Z\n" +
                                    "3\t2024-10-12T00:00:00.000000Z\n" +
                                    "4\t2024-10-12T00:00:01.000000Z\n" +
                                    "6\t2024-10-12T00:00:02.000000Z\n" +
                                    "75\t2024-10-12T10:00:03.000000Z\n",
                            MinMaxCountHeader +
                            "2024-10-10T00:00:00.000000Z\t2024-10-12T10:00:03.000000Z\t7\n");
                }
        );
    }

    @Test
    public void testDropWithUncommittedRowsWithReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = testName.getMethodName();
                    createTableX(tableName,
                            TableHeader +
                                    "1\t2024-10-10T00:00:00.000000Z\n" +
                                    "2\t2024-10-11T00:00:00.000000Z\n" +
                                    "3\t2024-10-12T00:00:00.000000Z\n" +
                                    "4\t2024-10-12T00:00:01.000000Z\n" +
                                    "6\t2024-10-12T00:00:02.000000Z\n" +
                                    "5\t2024-10-15T00:00:00.000000Z\n",
                            "insert into " + tableName + " values(1, '2024-10-10T00:00:00.000000Z')",
                            "insert into " + tableName + " values(2, '2024-10-11T00:00:00.000000Z')",
                            "insert into " + tableName + " values(3, '2024-10-12T00:00:00.000000Z')",
                            "insert into " + tableName + " values(4, '2024-10-12T00:00:01.000000Z')",
                            "insert into " + tableName + " values(5, '2024-10-15T00:00:00.000000Z')",
                            "insert into " + tableName + " values(6, '2024-10-12T00:00:02.000000Z')");
                    try (
                            TableReader ignore0 = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName);
                            TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName);
                            TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "testing")
                    ) {
                        long lastTs = TimestampFormatUtils.parseTimestamp(LastPartitionTs + "T00:00:00.000000Z");

                        TableWriter.Row row = writer.newRow(lastTs);
                        row.putInt(0, 100);
                        row.append();

                        row = writer.newRow(TimestampFormatUtils.parseTimestamp("2024-10-12T00:00:03.000000Z"));
                        row.putInt(0, 50);
                        row.append();

                        reader.openPartition(3); // last partition
                        writer.removePartition(lastTs);
                    }
                    assertTableX(tableName, TableHeader +
                                    "1\t2024-10-10T00:00:00.000000Z\n" +
                                    "2\t2024-10-11T00:00:00.000000Z\n" +
                                    "3\t2024-10-12T00:00:00.000000Z\n" +
                                    "4\t2024-10-12T00:00:01.000000Z\n" +
                                    "6\t2024-10-12T00:00:02.000000Z\n" +
                                    "50\t2024-10-12T00:00:03.000000Z\n",
                            MinMaxCountHeader +
                                    "2024-10-10T00:00:00.000000Z\t2024-10-12T00:00:03.000000Z\t6\n");
                }
        );
    }

    @Test
    public void testDropActiveInsertActiveDropAgain0() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = testName.getMethodName();
                    createTableX(tableName,
                            TableHeader +
                                    "5\t2024-10-15T00:00:00.000000Z\n",
                            "insert into " + tableName + " values(5, '2024-10-15T00:00:00.000000Z')");
                    dropPartition(tableName, LastPartitionTs);
                    assertTableX(tableName, TableHeader, EmptyTableMinMaxCount);
                    insert("insert into " + tableName + " values(5, '2024-10-15T00:00:00.000000Z')");
                    dropPartition(tableName, LastPartitionTs);
                    assertTableX(tableName, TableHeader, EmptyTableMinMaxCount);
                    insert("insert into " + tableName + " values(5, '2024-10-15T00:00:00.000000Z')");
                    insert("insert into " + tableName + " values(1, '2024-10-16T00:00:00.000000Z')");
                    assertSql(tableName, TableHeader +
                            "5\t2024-10-15T00:00:00.000000Z\n" +
                            "1\t2024-10-16T00:00:00.000000Z\n");
                    dropPartition(tableName, LastPartitionTs);
                    assertTableX(tableName, TableHeader +
                                    "1\t2024-10-16T00:00:00.000000Z\n",
                            MinMaxCountHeader +
                            "2024-10-16T00:00:00.000000Z\t2024-10-16T00:00:00.000000Z\t1\n");
                }
        );
    }

    @Test
    public void testCannotDropActivePartitionWhenO3HasARowFromTheFuture() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = testName.getMethodName();

                    createTableX(tableName,
                            TableHeader +
                                    "1\t2024-10-10T00:00:00.000000Z\n" +
                                    "2\t2024-10-11T00:00:00.000000Z\n" +
                                    "3\t2024-10-12T00:00:00.000000Z\n" +
                                    "4\t2024-10-12T00:00:01.000000Z\n" +
                                    "6\t2024-10-12T00:00:02.000000Z\n" +
                                    "5\t2024-10-15T00:00:00.000000Z\n",
                            "insert into " + tableName + " values(1, '2024-10-10T00:00:00.000000Z')",
                            "insert into " + tableName + " values(2, '2024-10-11T00:00:00.000000Z')",
                            "insert into " + tableName + " values(3, '2024-10-12T00:00:00.000000Z')",
                            "insert into " + tableName + " values(4, '2024-10-12T00:00:01.000000Z')",
                            "insert into " + tableName + " values(5, '2024-10-15T00:00:00.000000Z')",
                            "insert into " + tableName + " values(6, '2024-10-12T00:00:02.000000Z')");

                    dropPartition(tableName, LastPartitionTs);
                    insert("insert into " + tableName + " values(5, '2024-10-15T00:00:00.000000Z')");
                    dropPartition(tableName, "2024-10-12");
                    dropPartition(tableName, LastPartitionTs);
                    assertSql(tableName, TableHeader +
                                    "1\t2024-10-10T00:00:00.000000Z\n" +
                                    "2\t2024-10-11T00:00:00.000000Z\n");
                    insert("insert into " + tableName + " values(5, '2024-10-12T00:00:00.000000Z')");
                    insert("insert into " + tableName + " values(1, '2024-10-16T00:00:00.000000Z')");

                    try {
                        dropPartition(tableName, LastPartitionTs);
                    } catch (SqlException ex) {
                        TestUtils.assertContains("[26] could not remove partition '2024-10-15'", ex.getFlyweightMessage());
                    }
                    assertTableX(tableName, TableHeader +
                                    "1\t2024-10-10T00:00:00.000000Z\n" +
                                    "2\t2024-10-11T00:00:00.000000Z\n" +
                                    "5\t2024-10-12T00:00:00.000000Z\n" +
                                    "1\t2024-10-16T00:00:00.000000Z\n",
                            MinMaxCountHeader +
                            "2024-10-10T00:00:00.000000Z\t2024-10-16T00:00:00.000000Z\t4\n");
                }
        );
    }

    @Test
    public void testDropActivePartitionDetach() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = testName.getMethodName();

                    createTableX(tableName,
                            TableHeader +
                                    "1\t2024-10-10T00:00:00.000000Z\n" +
                                    "2\t2024-10-11T00:00:00.000000Z\n" +
                                    "3\t2024-10-12T00:00:00.000000Z\n" +
                                    "4\t2024-10-12T00:00:01.000000Z\n" +
                                    "6\t2024-10-12T00:00:02.000000Z\n" +
                                    "5\t2024-10-15T00:00:00.000000Z\n",
                            "insert into " + tableName + " values(1, '2024-10-10T00:00:00.000000Z')",
                            "insert into " + tableName + " values(2, '2024-10-11T00:00:00.000000Z')",
                            "insert into " + tableName + " values(3, '2024-10-12T00:00:00.000000Z')",
                            "insert into " + tableName + " values(4, '2024-10-12T00:00:01.000000Z')",
                            "insert into " + tableName + " values(5, '2024-10-15T00:00:00.000000Z')",
                            "insert into " + tableName + " values(6, '2024-10-12T00:00:02.000000Z')");

                    dropPartition(tableName, LastPartitionTs);
                    insert("insert into " + tableName + " values(5, '2024-10-15T00:00:00.000000Z')");
                    dropPartition(tableName, "2024-10-12");
                    dropPartition(tableName, LastPartitionTs);
                    assertSql(tableName, TableHeader +
                                    "1\t2024-10-10T00:00:00.000000Z\n" +
                                    "2\t2024-10-11T00:00:00.000000Z\n");
                    insert("insert into " + tableName + " values(5, '2024-10-12T00:00:00.000000Z')");
                    insert("insert into " + tableName + " values(1, '2024-10-16T00:00:00.000000Z')");

                    try {
                        dropPartition(tableName, LastPartitionTs);
                    } catch (SqlException ex) {
                        TestUtils.assertContains("[26] could not remove partition '2024-10-15'", ex.getFlyweightMessage());
                    }
                    detachPartition(tableName, "2024-10-12");
                    dropPartition(tableName, "2024-10-16");
                    assertTableX(tableName, TableHeader +
                                    "1\t2024-10-10T00:00:00.000000Z\n" +
                                    "2\t2024-10-11T00:00:00.000000Z\n",
                            MinMaxCountHeader +
                            "2024-10-10T00:00:00.000000Z\t2024-10-11T00:00:00.000000Z\t2\n");
                }
        );
    }

    private WorkerPool workerPool;
    private O3PartitionPurgeJob partitionPurgeJob;
    private int txn;

    private void createTableX(String tableName, String expected, String... insertStmt) throws SqlException {
        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY).col("id", ColumnType.INT).timestamp()) {
            CairoTestUtils.create(model);
        }
        txn = 0;
        for (int i = 0, n = insertStmt.length; i < n; i++) {
            insert(insertStmt[i]);
        }
        assertSql(tableName, expected);

        workerPool = new TestWorkerPool(1);
        partitionPurgeJob = new O3PartitionPurgeJob(engine.getMessageBus(), 1);
        workerPool.assign(partitionPurgeJob);
        workerPool.freeOnExit(partitionPurgeJob);
        workerPool.start(); // closed by assertTableX
    }

    private void insert(String stmt) throws SqlException {
        compiler.compile(stmt, sqlExecutionContext).execute(null).await();
        txn++;
    }

    private void assertTableX(String tableName, String expectedRows, String expectedMinMaxCount) throws SqlException {
        engine.releaseAllReaders();
        assertSql(tableName, expectedRows);
        engine.releaseAllWriters();
        try (Path path = new Path().of(root).concat(tableName).concat(LastPartitionTs)) {
            TableUtils.txnPartitionConditionally(path, txn);
            path.$();
            Assert.assertFalse(Files.exists(path));
        } finally {
            Misc.free(workerPool);
        }
        assertSql("select min(timestamp), max(timestamp), count() from " + tableName, expectedMinMaxCount);
    }


    private void dropPartition(String tableName, String partitionName) throws SqlException {
        Assert.assertEquals(ALTER,
                compile("alter table " + tableName + " drop partition list '" + partitionName + "'", sqlExecutionContext).getType());
    }

    private void detachPartition(String tableName, String partitionName) throws SqlException {
        Assert.assertEquals(ALTER,
                compile("alter table " + tableName + " detach partition list '" + partitionName + "'", sqlExecutionContext).getType());
    }
}
