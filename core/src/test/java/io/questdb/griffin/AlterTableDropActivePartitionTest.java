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
import io.questdb.mp.TestWorkerPool;
import io.questdb.mp.WorkerPool;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import java.util.UUID;

import static io.questdb.griffin.CompiledQuery.ALTER;

public class AlterTableDropActivePartitionTest extends AbstractGriffinTest {

    private static final String LastPartitionTs = "2024-10-15";
    private static final String TableHeader = "id\ttimestamp\n";

    @Test
    public void testDropOnlyPartitionNoReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    String tableName = randomTableName();
                    createTableX(tableName,
                            TableHeader +
                                    "5\t2024-10-15T00:00:00.000000Z\n",
                            "insert into " + tableName + " values(5, '2024-10-15T00:00:00.000000Z')");
                    dropActivePartition(tableName);
                    assertTableX(tableName, TableHeader);
                }
        );
    }

    @Test
    public void testDropOnlyPartitionWithReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    String tableName = randomTableName();
                    createTableX(tableName,
                            TableHeader +
                                    "5\t2024-10-15T00:00:00.000000Z\n",
                            "insert into " + tableName + " values(5, '2024-10-15T00:00:00.000000Z')");
                    insert("insert into " + tableName + " values(111, '2024-10-15T11:11:11.111111Z');");
                    try (TableReader reader0 = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                        insert("insert into " + tableName + " values(888, '2024-10-15T00:00:00.000000Z');");
                        try (TableReader reader1 = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                            Assert.assertEquals(2, reader0.size());
                            dropActivePartition(tableName);
                            Assert.assertEquals(3, reader1.size());
                        }
                    }
                    assertTableX(tableName, TableHeader);
                }
        );
    }

    @Test
    public void testDropPartitionNoReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    String tableName = randomTableName();
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
                    dropActivePartition(tableName);
                    assertTableX(tableName, TableHeader +
                            "1\t2024-10-10T00:00:00.000000Z\n" +
                            "2\t2024-10-11T00:00:00.000000Z\n" +
                            "3\t2024-10-12T00:00:00.000000Z\n" +
                            "4\t2024-10-12T00:00:01.000000Z\n" +
                            "6\t2024-10-12T00:00:02.000000Z\n");
                }
        );
    }

    @Test
    public void testDropPartitionWithReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    String tableName = randomTableName();
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
                            TableReader ignore1 = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)
                    ) {
                        dropActivePartition(tableName);
                    }
                    assertTableX(tableName, TableHeader +
                            "1\t2024-10-10T00:00:00.000000Z\n" +
                            "2\t2024-10-11T00:00:00.000000Z\n" +
                            "3\t2024-10-12T00:00:00.000000Z\n" +
                            "4\t2024-10-12T00:00:01.000000Z\n" +
                            "6\t2024-10-12T00:00:02.000000Z\n");
                }
        );
    }


    @Test
    public void testCannotDropWhenThereIsAWriter() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    String tableName = randomTableName();
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
                        dropActivePartition(tableName);
                        Assert.fail();
                    } catch (EntryUnavailableException ex) {
                        TestUtils.assertContains("[-1] table busy [reason=testing]", ex.getFlyweightMessage());
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropWithUncommittedRowsNoReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    String tableName = randomTableName();
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
                            TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "testing")
                    ) {
                        long lastTs = TimestampFormatUtils.parseTimestamp(LastPartitionTs + "T00:00:00.000000Z");

                        TableWriter.Row row = writer.newRow(lastTs);
                        row.putInt(0, 100);
                        row.append();

                        row = writer.newRow(TimestampFormatUtils.parseTimestamp("2024-10-12T00:00:03.000000Z"));
                        row.putInt(0, 50);
                        row.append();

                        writer.removePartition(lastTs);
                    }
                    assertTableX(tableName, TableHeader +
                            "1\t2024-10-10T00:00:00.000000Z\n" +
                            "2\t2024-10-11T00:00:00.000000Z\n" +
                            "3\t2024-10-12T00:00:00.000000Z\n" +
                            "4\t2024-10-12T00:00:01.000000Z\n" +
                            "6\t2024-10-12T00:00:02.000000Z\n");
                }
        );
    }

    @Test
    public void testDropWithUncommittedRowsWithReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    String tableName = randomTableName();
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
                            "6\t2024-10-12T00:00:02.000000Z\n");
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

    private void assertTableX(String tableName, String expected) throws SqlException {
        engine.releaseAllReaders();
        assertSql(tableName, expected);
        engine.releaseAllWriters();
        try (Path path = new Path().of(root).concat(tableName).concat(LastPartitionTs)) {
            TableUtils.txnPartitionConditionally(path, txn);
            path.$();
            Assert.assertFalse(Files.exists(path));
        } finally {
            Misc.free(workerPool);
        }
    }

    private void dropActivePartition(String tableName) throws SqlException {
        Assert.assertEquals(ALTER,
                compile("alter table " + tableName + " drop partition list '" + LastPartitionTs + "'", sqlExecutionContext).getType());
    }

    private static String randomTableName() {
        return UUID.randomUUID().toString().split("[-]")[0];
    }
}
