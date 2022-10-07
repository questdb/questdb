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
import io.questdb.std.Files;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import static io.questdb.griffin.CompiledQuery.ALTER;

public class AlterTableDropActivePartitionTest extends AbstractGriffinTest {

    private static final String LastPartitionTs = "2024-10-15";


    private WorkerPool workerPool;
    private PartitionPurgeJob partitionPurgeJob;

    @Before
    public void setUp() {
        super.setUp();
        workerPool = new TestWorkerPool(1);
        partitionPurgeJob = new PartitionPurgeJob(engine);
        workerPool.assign(partitionPurgeJob);
        workerPool.start();
    }

    @After
    public void tearDown() {
        super.tearDown();
        try {
            compile("drop table if exists x", sqlExecutionContext);
        } catch (SqlException e) {
            Assert.fail();
        }
        workerPool.close();
        partitionPurgeJob.close();
    }

    @Test
    public void testDropOnlyPartitionNoReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    createTableXSinglePartition();
                    Assert.assertEquals(ALTER,
                            compile("alter table x drop partition list '" + LastPartitionTs + "'", sqlExecutionContext).getType());
                    assertTableXSinglePartition();
                }
        );
    }

    @Test
    @Ignore
    // TODO: FIX this, the table is not being removed
    public void testDropOnlyPartitionWithReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    createTableXSinglePartition();
                    try (
                            TableReader ignore0 = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x");
                            TableReader ignore1 = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x")
                    ) {
                        Assert.assertEquals(ALTER,
                                compile("alter table x drop partition list '" + LastPartitionTs + "'", sqlExecutionContext).getType());
                    }
                    Os.sleep(500L);
                    assertTableXSinglePartition();
                }
        );
    }

    @Test
    public void test() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    compile(
                            "create table x (" +
                                    "    id int," +
                                    "    ts timestamp" +
                                    ") timestamp(ts) partition by DAY;",
                            sqlExecutionContext
                    );
                }
        );
    }


    @Test
    public void testDropPartitionNoReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    createTableXMultiplePartitions();
                    Assert.assertEquals(ALTER,
                            compile("alter table x drop partition list '" + LastPartitionTs + "'", sqlExecutionContext).getType());

                    assertTableXMultiplePartitions();
                }
        );
    }

    @Test
    public void testDropPartitionWithReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    createTableXMultiplePartitions();
                    try (
                            TableReader ignore0 = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x");
                            TableReader ignore1 = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x")
                    ) {
                        Assert.assertEquals(ALTER,
                                compile("alter table x drop partition list '" + LastPartitionTs + "'", sqlExecutionContext).getType());

                        assertTableXMultiplePartitions();
                    }
                }
        );
    }


    @Test
    public void testCannotDropWhenThereIsAWriter() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    createTableXMultiplePartitions();
                    try (TableWriter ignore = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "x", "testing")) {
                        compile("alter table x drop partition list '" + LastPartitionTs + "'", sqlExecutionContext);
                        Assert.fail();
                    } catch (EntryUnavailableException ex) {
                        TestUtils.assertContains("[-1] table busy [reason=testing]", ex.getFlyweightMessage());
                    }
                }
        );
    }

    @Test
    public void testDropWithUncommittedRowsNoReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    createTableXMultiplePartitions();
                    try (
                            TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "x", "testing")
                    ) {
                        long lastTs = TimestampFormatUtils.parseTimestamp(LastPartitionTs + "T00:00:00.000000Z");

                        TableWriter.Row row = writer.newRow(lastTs);
                        row.putInt(0, 100);
                        row.append();

                        row = writer.newRow(TimestampFormatUtils.parseTimestamp("2024-10-12T00:00:03.000000Z"));
                        row.putInt(0, 50);
                        row.append();

                        writer.removePartition(lastTs);

                        assertTableXMultiplePartitions();
                    }
                }
        );
    }

    @Test
    public void testDropWithUncommittedRowsWithReaders() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
                    createTableXMultiplePartitions();
                    try (
                            TableReader ignore0 = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x");
                            TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x");
                            TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "x", "testing")
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

                        assertTableXMultiplePartitions();
                    }
                }
        );
    }

    private void createTableXMultiplePartitions() throws SqlException {
        compile(
                "create table x (" +
                        "    id int," +
                        "    ts timestamp" +
                        ") timestamp(ts) partition by DAY;",
                sqlExecutionContext
        );

        insert("insert into x values(1, '2024-10-10T00:00:00.000000Z')");
        insert("insert into x values(2, '2024-10-11T00:00:00.000000Z');");
        insert("insert into x values(3, '2024-10-12T00:00:00.000000Z');");
        insert("insert into x values(4, '2024-10-12T00:00:01.000000Z');");
        insert("insert into x values(5, '2024-10-15T00:00:00.000000Z');");
        insert("insert into x values(6, '2024-10-12T00:00:02.000000Z');");
        assertSql("x",
                "id\tts\n" +
                        "1\t2024-10-10T00:00:00.000000Z\n" +
                        "2\t2024-10-11T00:00:00.000000Z\n" +
                        "3\t2024-10-12T00:00:00.000000Z\n" +
                        "4\t2024-10-12T00:00:01.000000Z\n" +
                        "6\t2024-10-12T00:00:02.000000Z\n" +
                        "5\t2024-10-15T00:00:00.000000Z\n"
        );
    }

    private void assertTableXMultiplePartitions() throws SqlException {
        assertSql("select count() from x where ts in '2024'", "count\n5\n");
        assertSql("x", "id\tts\n" +
                "1\t2024-10-10T00:00:00.000000Z\n" +
                "2\t2024-10-11T00:00:00.000000Z\n" +
                "3\t2024-10-12T00:00:00.000000Z\n" +
                "4\t2024-10-12T00:00:01.000000Z\n" +
                "6\t2024-10-12T00:00:02.000000Z\n");
        assertFolderDoesNotExist("x", LastPartitionTs);
    }

    private void createTableXSinglePartition() throws SqlException {
        compile(
                "create table x (" +
                        "    id int," +
                        "    ts timestamp" +
                        ") timestamp(ts) partition by DAY;",
                sqlExecutionContext
        );

        insert("insert into x values(5, '2024-10-15T00:00:00.000000Z');");
        assertSql("x",
                "id\tts\n" +
                        "5\t2024-10-15T00:00:00.000000Z\n"
        );
    }

    private void assertTableXSinglePartition() throws SqlException {
        assertSql("select count() from x where ts in '2024'", "count\n0\n");
        assertSql("x", "id\tts\n");
        assertFolderDoesNotExist("x", LastPartitionTs);
    }

    private void insert(String stmt) throws SqlException {
        compiler.compile(stmt, sqlExecutionContext).execute(null).await();
    }

    private void assertFolderDoesNotExist(String tableName, String partitionName) {
        try (Path path = new Path().of(root).concat(tableName).concat(partitionName).slash$()) {
            Assert.assertFalse(Files.exists(path));
        }
    }
}
