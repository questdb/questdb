/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class AlterTableDropActivePartitionTest extends AbstractCairoTest {
    private static final String LastPartitionTs = "2023-10-15";
    private static final String MinMaxCountHeader = "min\tmax\tcount\n";
    private static final String EmptyTableMinMaxCount = MinMaxCountHeader + "\t\t0\n";
    private static final String TableHeader = "id\ttimestamp\n";

    private final TestTimestampType timestampType;
    private int txn;
    private WorkerPool workerPool;

    public AlterTableDropActivePartitionTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testCannotDropActivePartitionWhenO3HasARowFromTheFuture() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";
                    try {
                        createTableX(tableName,
                                TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "2\t2023-10-11T00:00:00.000000Z\n" +
                                        "3\t2023-10-12T00:00:00.000000Z\n" +
                                        "4\t2023-10-12T00:00:01.000000Z\n" +
                                        "6\t2023-10-12T00:00:02.000000Z\n" +
                                        "5\t2023-10-15T00:00:00.000000Z\n",
                                "insert into " + tableName + " values(1, '2023-10-10T00:00:00.000000Z')",
                                "insert into " + tableName + " values(2, '2023-10-11T00:00:00.000000Z')",
                                "insert into " + tableName + " values(3, '2023-10-12T00:00:00.000000Z')",
                                "insert into " + tableName + " values(4, '2023-10-12T00:00:01.000000Z')",
                                "insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')",
                                "insert into " + tableName + " values(6, '2023-10-12T00:00:02.000000Z')");

                        dropPartition(tableName, LastPartitionTs);
                        insert("insert into " + tableName + " values(5, '2023-10-15T00:00:02.000000Z')");
                        dropPartition(tableName, "2023-10-12");
                        dropPartition(tableName, LastPartitionTs);
                        assertSql(replaceTimestampSuffix(TableHeader +
                                "1\t2023-10-10T00:00:00.000000Z\n" +
                                "2\t2023-10-11T00:00:00.000000Z\n"), tableName);
                        insert("insert into " + tableName + " values(5, '2023-10-12T00:00:00.000000Z')");
                        insert("insert into " + tableName + " values(1, '2023-10-16T00:00:00.000000Z')");

                        try {
                            dropPartition(tableName, LastPartitionTs); // because it does not exist
                        } catch (CairoException ex) {
                            TestUtils.assertContains(ex.getFlyweightMessage(), "could not remove partition [table=tab, partitionTimestamp=2023-10-15");
                        }

                        assertTableX(tableName, TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "2\t2023-10-11T00:00:00.000000Z\n" +
                                        "5\t2023-10-12T00:00:00.000000Z\n" +
                                        "1\t2023-10-16T00:00:00.000000Z\n",
                                MinMaxCountHeader +
                                        "2023-10-10T00:00:00.000000Z\t2023-10-16T00:00:00.000000Z\t4\n");

                        dropPartition(tableName, "2023-10-16"); // remove active partition
                        assertTableX(tableName, TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "2\t2023-10-11T00:00:00.000000Z\n" +
                                        "5\t2023-10-12T00:00:00.000000Z\n",
                                MinMaxCountHeader +
                                        "2023-10-10T00:00:00.000000Z\t2023-10-12T00:00:00.000000Z\t3\n");
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testCannotDropWhenThereIsAWriter() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";
                    try {
                        createTableX(
                                tableName,
                                TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "2\t2023-10-11T00:00:00.000000Z\n" +
                                        "3\t2023-10-12T00:00:00.000000Z\n" +
                                        "4\t2023-10-12T00:00:01.000000Z\n" +
                                        "6\t2023-10-12T00:00:02.000000Z\n" +
                                        "5\t2023-10-15T00:00:00.000000Z\n",
                                "insert into " + tableName + " values(1, '2023-10-10T00:00:00.000000Z')",
                                "insert into " + tableName + " values(2, '2023-10-11T00:00:00.000000Z')",
                                "insert into " + tableName + " values(3, '2023-10-12T00:00:00.000000Z')",
                                "insert into " + tableName + " values(4, '2023-10-12T00:00:01.000000Z')",
                                "insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')",
                                "insert into " + tableName + " values(6, '2023-10-12T00:00:02.000000Z')"
                        );
                        try (TableWriter ignore = getWriter(tableName)) {
                            dropPartition(tableName, LastPartitionTs);
                            Assert.fail();
                        } catch (EntryUnavailableException ex) {
                            TestUtils.assertContains(ex.getFlyweightMessage(), "table busy [reason=test");
                        }
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDetachPartitionsLongerPartitionName() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";
                    try {
                        createTableX(tableName,
                                TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "2\t2023-10-11T00:00:00.000000Z\n" +
                                        "3\t2023-10-12T00:00:00.000000Z\n" +
                                        "4\t2023-10-12T00:00:01.000000Z\n" +
                                        "6\t2023-10-12T00:00:02.000000Z\n" +
                                        "5\t2023-10-15T00:00:00.000000Z\n",
                                "insert into " + tableName + " values(1, '2023-10-10T00:00:00.000000Z')",
                                "insert into " + tableName + " values(2, '2023-10-11T00:00:00.000000Z')",
                                "insert into " + tableName + " values(3, '2023-10-12T00:00:00.000000Z')",
                                "insert into " + tableName + " values(4, '2023-10-12T00:00:01.000000Z')",
                                "insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')",
                                "insert into " + tableName + " values(6, '2023-10-12T00:00:02.000000Z')");

                        detachPartition(tableName, "2023-10-12T23:59:59.999999Z");
                        detachPartition(tableName, "2023-10-11T23:59:59.999999Z");
                        detachPartition(tableName, "2023-10-10T23:59:59.999999Z");

                        assertTableX(tableName, TableHeader +
                                        "5\t2023-10-15T00:00:00.000000Z\n",
                                MinMaxCountHeader +
                                        "2023-10-15T00:00:00.000000Z\t2023-10-15T00:00:00.000000Z\t1\n");
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropActivePartitionCreateItAgainAndDoItAgain() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";
                    try {
                        createTableX(tableName,
                                TableHeader +
                                        "5\t2023-10-15T00:00:00.000000Z\n",
                                "insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')");
                        dropPartition(tableName, LastPartitionTs);
                        assertTableX(tableName, TableHeader, EmptyTableMinMaxCount);
                        insert("insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')");
                        dropPartition(tableName, LastPartitionTs);
                        assertTableX(tableName, TableHeader, EmptyTableMinMaxCount);
                        insert("insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')");
                        insert("insert into " + tableName + " values(1, '2023-10-16T00:00:00.000000Z')"); // spurious row from the future
                        assertSql(replaceTimestampSuffix(TableHeader +
                                "5\t2023-10-15T00:00:00.000000Z\n" +
                                "1\t2023-10-16T00:00:00.000000Z\n"), tableName); // new active partition
                        dropPartition(tableName, "2023-10-16");
                        dropPartition(tableName, LastPartitionTs);
                        assertTableX(tableName, TableHeader, EmptyTableMinMaxCount);
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropActivePartitionDetach() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";
                    try {
                        createTableX(tableName,
                                TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "2\t2023-10-11T00:00:00.000000Z\n" +
                                        "3\t2023-10-12T00:00:00.000000Z\n" +
                                        "4\t2023-10-12T00:00:01.000000Z\n" +
                                        "6\t2023-10-12T00:00:02.000000Z\n" +
                                        "5\t2023-10-15T00:00:00.000000Z\n",
                                "insert into " + tableName + " values(1, '2023-10-10T00:00:00.000000Z')",
                                "insert into " + tableName + " values(2, '2023-10-11T00:00:00.000000Z')",
                                "insert into " + tableName + " values(3, '2023-10-12T00:00:00.000000Z')",
                                "insert into " + tableName + " values(4, '2023-10-12T00:00:01.000000Z')",
                                "insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')",
                                "insert into " + tableName + " values(6, '2023-10-12T00:00:02.000000Z')");

                        dropPartition(tableName, LastPartitionTs); // drop active partition
                        insert("insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')"); // recreate it
                        dropPartition(tableName, LastPartitionTs); // drop active partition

                        dropPartition(tableName, "2023-10-12"); // drop new active partition
                        assertSql(replaceTimestampSuffix(TableHeader +
                                "1\t2023-10-10T00:00:00.000000Z\n" +
                                "2\t2023-10-11T00:00:00.000000Z\n"), tableName);

                        insert("insert into " + tableName + " values(5, '2023-10-12T00:00:17.000000Z')");
                        insert("insert into " + tableName + " values(1, '2023-10-16T00:00:00.000000Z')");
                        detachPartition(tableName, "2023-10-11"); // detach prev partition
                        dropPartition(tableName, "2023-10-16"); // drop active partition

                        assertTableX(tableName, TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "5\t2023-10-12T00:00:17.000000Z\n",
                                MinMaxCountHeader +
                                        "2023-10-10T00:00:00.000000Z\t2023-10-12T00:00:17.000000Z\t2\n");
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropActivePartitionDetachHigherResolutionTimestamp() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";
                    try {
                        createTableX(tableName,
                                TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "2\t2023-10-11T00:00:00.000000Z\n" +
                                        "3\t2023-10-12T00:00:00.000000Z\n" +
                                        "4\t2023-10-12T00:00:01.000000Z\n" +
                                        "6\t2023-10-12T00:00:02.000000Z\n" +
                                        "5\t2023-10-15T00:00:00.000000Z\n",
                                "insert into " + tableName + " values(1, '2023-10-10T00:00:00.000000Z')",
                                "insert into " + tableName + " values(2, '2023-10-11T00:00:00.000000Z')",
                                "insert into " + tableName + " values(3, '2023-10-12T00:00:00.000000Z')",
                                "insert into " + tableName + " values(4, '2023-10-12T00:00:01.000000Z')",
                                "insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')",
                                "insert into " + tableName + " values(6, '2023-10-12T00:00:02.000000Z')");

                        String activePartitionTs = "2023-10-15T00:00:00.000000Z";

                        dropPartition(tableName, activePartitionTs); // drop active partition
                        insert("insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')"); // recreate it
                        dropPartition(tableName, activePartitionTs); // drop active partition

                        assertTableX(tableName, TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "2\t2023-10-11T00:00:00.000000Z\n" +
                                        "3\t2023-10-12T00:00:00.000000Z\n" +
                                        "4\t2023-10-12T00:00:01.000000Z\n" +
                                        "6\t2023-10-12T00:00:02.000000Z\n",
                                MinMaxCountHeader +
                                        "2023-10-10T00:00:00.000000Z\t2023-10-12T00:00:02.000000Z\t5\n");
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropActivePartitionDetachLowerResolutionTimestamp() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";
                    try {
                        createTableX(tableName,
                                TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "2\t2023-10-11T00:00:00.000000Z\n" +
                                        "3\t2023-10-12T00:00:00.000000Z\n" +
                                        "4\t2023-10-12T00:00:01.000000Z\n" +
                                        "6\t2023-10-12T00:00:02.000000Z\n" +
                                        "5\t2023-10-15T00:00:00.000000Z\n",
                                "insert into " + tableName + " values(1, '2023-10-10T00:00:00.000000Z')",
                                "insert into " + tableName + " values(2, '2023-10-11T00:00:00.000000Z')",
                                "insert into " + tableName + " values(3, '2023-10-12T00:00:00.000000Z')",
                                "insert into " + tableName + " values(4, '2023-10-12T00:00:01.000000Z')",
                                "insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')",
                                "insert into " + tableName + " values(6, '2023-10-12T00:00:02.000000Z')");

                        String activePartitionTs = "2023-10";
                        try {
                            dropPartition(tableName, activePartitionTs); // drop active partition
                        } catch (SqlException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "'yyyy-MM-dd' expected, found [ts=2023-10]");
                        }
                        assertTableX(tableName, TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "2\t2023-10-11T00:00:00.000000Z\n" +
                                        "3\t2023-10-12T00:00:00.000000Z\n" +
                                        "4\t2023-10-12T00:00:01.000000Z\n" +
                                        "6\t2023-10-12T00:00:02.000000Z\n" +
                                        "5\t2023-10-15T00:00:00.000000Z\n",
                                MinMaxCountHeader +
                                        "2023-10-10T00:00:00.000000Z\t2023-10-15T00:00:00.000000Z\t6\n");
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropActivePartitionFailsBecausePrevMaxPartitionIsIncorrect() throws Exception {
        FilesFacade myFf = new TestFilesFacadeImpl() {
            @Override
            public long readNonNegativeLong(long fd, long offset) {
                return 17;
            }
        };

        assertMemoryLeak(myFf, () -> {
                    final String tableName = "tab";
                    try {
                        createTableX(tableName,
                                TableHeader +
                                        "3\t2023-10-12T00:00:01.000000Z\n" +
                                        "5\t2023-10-12T00:00:02.000000Z\n" +
                                        "8\t2023-10-15T00:00:00.000000Z\n",
                                "insert into " + tableName + " values(3, '2023-10-12T00:00:01.000000Z')",
                                "insert into " + tableName + " values(5, '2023-10-12T00:00:02.000000Z')",
                                "insert into " + tableName + " values(8, '2023-10-15T00:00:00.000000Z')");

                        try {
                            dropPartition(tableName, LastPartitionTs);
                        } catch (CairoException |
                                 SqlException ex) { // the latter is due to an assertion in SqlException.position
                            TestUtils.assertContains(ex.getFlyweightMessage(), "invalid timestamp data in detached partition");
                            TestUtils.assertContains(ex.getFlyweightMessage(), replaceTimestampSuffix("minTimestamp=1970-01-01T00:00:00.000017Z, maxTimestamp=1970-01-01T00:00:00.000017Z]", timestampType.getTypeName()));
                        }
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropActivePartitionFailsBecauseWeCannotReadPrevMaxPartition() throws Exception {
        FilesFacade myFf = new TestFilesFacadeImpl() {
            @Override
            public long readNonNegativeLong(long fd, long offset) {
                return -1;
            }
        };

        assertMemoryLeak(myFf, () -> {
                    final String tableName = "tab";
                    try {
                        createTableX(tableName,
                                TableHeader +
                                        "3\t2023-10-12T00:00:01.000000Z\n" +
                                        "5\t2023-10-12T00:00:02.000000Z\n" +
                                        "8\t2023-10-15T00:00:00.000000Z\n",
                                "insert into " + tableName + " values(3, '2023-10-12T00:00:01.000000Z')",
                                "insert into " + tableName + " values(5, '2023-10-12T00:00:02.000000Z')",
                                "insert into " + tableName + " values(8, '2023-10-15T00:00:00.000000Z')");

                        try {
                            dropPartition(tableName, LastPartitionTs);
                        } catch (CairoException |
                                 SqlException ex) { // the latter is due to an assertion in SqlException.position
                            TestUtils.assertContains(ex.getFlyweightMessage(), "cannot read min, max timestamp from the");
                        }
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropActivePartitionNoReaders() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";
                    try {
                        createTableX(tableName,
                                TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "2\t2023-10-11T00:00:00.000000Z\n" +
                                        "3\t2023-10-12T00:00:00.000000Z\n" +
                                        "4\t2023-10-12T00:00:01.000000Z\n" +
                                        "6\t2023-10-12T00:00:02.000000Z\n" +
                                        "5\t2023-10-15T00:00:00.000000Z\n",
                                "insert into " + tableName + " values(1, '2023-10-10T00:00:00.000000Z')",
                                "insert into " + tableName + " values(2, '2023-10-11T00:00:00.000000Z')",
                                "insert into " + tableName + " values(3, '2023-10-12T00:00:00.000000Z')",
                                "insert into " + tableName + " values(4, '2023-10-12T00:00:01.000000Z')",
                                "insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')",
                                "insert into " + tableName + " values(6, '2023-10-12T00:00:02.000000Z')");
                        dropPartition(tableName, LastPartitionTs);
                        assertTableX(tableName, TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "2\t2023-10-11T00:00:00.000000Z\n" +
                                        "3\t2023-10-12T00:00:00.000000Z\n" +
                                        "4\t2023-10-12T00:00:01.000000Z\n" +
                                        "6\t2023-10-12T00:00:02.000000Z\n",
                                MinMaxCountHeader +
                                        "2023-10-10T00:00:00.000000Z\t2023-10-12T00:00:02.000000Z\t5\n");
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropActivePartitionWithReaders() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";

                    final String expectedTable = TableHeader +
                            "1\t2023-10-10T00:00:00.000000Z\n" +
                            "2\t2023-10-11T00:00:00.000000Z\n" +
                            "3\t2023-10-12T00:00:00.000000Z\n" +
                            "4\t2023-10-12T00:00:01.000000Z\n" +
                            "6\t2023-10-12T00:00:02.000000Z\n" +
                            "5\t2023-10-15T00:00:00.000000Z\n";

                    final String expectedTableInTransaction = TableHeader +
                            "1\t2023-10-10T00:00:00.000000Z\n" +
                            "2\t2023-10-11T00:00:00.000000Z\n" +
                            "3\t2023-10-12T00:00:00.000000Z\n" +
                            "4\t2023-10-12T00:00:01.000000Z\n" +
                            "6\t2023-10-12T00:00:02.000000Z\n" +
                            "8\t2023-10-12T00:00:05.000001Z\n" +
                            "7\t2023-10-15T00:00:01.000000Z\n";

                    final String expectedTableAfterFirstDrop = TableHeader +
                            "1\t2023-10-10T00:00:00.000000Z\n" +
                            "2\t2023-10-11T00:00:00.000000Z\n" +
                            "3\t2023-10-12T00:00:00.000000Z\n" +
                            "4\t2023-10-12T00:00:01.000000Z\n" +
                            "6\t2023-10-12T00:00:02.000000Z\n";

                    final String expectedTableAfterSecondDrop = TableHeader +
                            "1\t2023-10-10T00:00:00.000000Z\n" +
                            "2\t2023-10-11T00:00:00.000000Z\n" +
                            "3\t2023-10-12T00:00:00.000000Z\n" +
                            "4\t2023-10-12T00:00:01.000000Z\n" +
                            "6\t2023-10-12T00:00:02.000000Z\n" +
                            "8\t2023-10-12T00:00:05.000001Z\n";

                    try {
                        createTableX(tableName,
                                expectedTable,
                                "insert into " + tableName + " values(1, '2023-10-10T00:00:00.000000Z')",
                                "insert into " + tableName + " values(2, '2023-10-11T00:00:00.000000Z')",
                                "insert into " + tableName + " values(3, '2023-10-12T00:00:00.000000Z')",
                                "insert into " + tableName + " values(4, '2023-10-12T00:00:01.000000Z')",
                                "insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')",
                                "insert into " + tableName + " values(6, '2023-10-12T00:00:02.000000Z')");
                        try (
                                TableReader reader0 = getReader(tableName);
                                TableReader reader1 = getReader(tableName)
                        ) {
                            assertSql(replaceTimestampSuffix(expectedTable), tableName);
                            Assert.assertEquals(6, reader0.size());
                            Assert.assertEquals(6, reader1.size());

                            dropPartition(tableName, LastPartitionTs);
                            reader0.reload();
                            reader1.reload();
                            Assert.assertEquals(5, reader0.size());
                            Assert.assertEquals(5, reader1.size());
                            assertSql(replaceTimestampSuffix(expectedTableAfterFirstDrop), tableName);

                            insert("insert into " + tableName + " values(8, '2023-10-12T00:00:05.000001Z')");
                            insert("insert into " + tableName + " values(7, '2023-10-15T00:00:01.000000Z')");
                            assertSql(replaceTimestampSuffix(expectedTableInTransaction), tableName);
                            reader0.reload();
                            reader1.reload();
                            Assert.assertEquals(7, reader0.size());
                            Assert.assertEquals(7, reader1.size());
                            dropPartition(tableName, LastPartitionTs);
                        }
                        assertTableX(tableName, expectedTableAfterSecondDrop, MinMaxCountHeader +
                                "2023-10-10T00:00:00.000000Z\t2023-10-12T00:00:05.000001Z\t6\n");
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropActivePartitionWithUncommittedO3RowsWithReaders() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";

                    final String expectedTable = TableHeader +
                            "1\t2023-10-10T00:00:00.000000Z\n" +
                            "2\t2023-10-11T00:00:00.000000Z\n" +
                            "3\t2023-10-12T00:00:00.000000Z\n" +
                            "4\t2023-10-12T00:00:01.000000Z\n" +
                            "6\t2023-10-12T00:00:02.000000Z\n" +
                            "5\t2023-10-15T00:00:00.000000Z\n";

                    createTableX(tableName,
                            expectedTable,
                            "insert into " + tableName + " values(1, '2023-10-10T00:00:00.000000Z')",
                            "insert into " + tableName + " values(2, '2023-10-11T00:00:00.000000Z')",
                            "insert into " + tableName + " values(3, '2023-10-12T00:00:00.000000Z')",
                            "insert into " + tableName + " values(4, '2023-10-12T00:00:01.000000Z')",
                            "insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')",
                            "insert into " + tableName + " values(6, '2023-10-12T00:00:02.000000Z')");

                    final String expectedTableAfterDrop = TableHeader +
                            "1\t2023-10-10T00:00:00.000000Z\n" +
                            "2\t2023-10-11T00:00:00.000000Z\n" +
                            "3\t2023-10-12T00:00:00.000000Z\n" +
                            "4\t2023-10-12T00:00:01.000000Z\n" +
                            "6\t2023-10-12T00:00:02.000000Z\n" +
                            "50\t2023-10-12T00:00:03.000000Z\n";

                    TimestampDriver driver = timestampType.getDriver();
                    try (
                            TableReader reader0 = getReader(tableName);
                            TableReader reader1 = getReader(tableName);
                            TableWriter writer = getWriter(tableName)
                    ) {
                        long lastTs = driver.parseFloorLiteral(LastPartitionTs + "T00:00:00.000000Z");

                        TableWriter.Row row = writer.newRow(lastTs);
                        row.putInt(0, 100); // will be removed
                        row.append();

                        row = writer.newRow(driver.parseFloorLiteral("2023-10-12T00:00:03.000000Z")); // earlier timestamp
                        row.putInt(0, 50);
                        row.append();

                        Assert.assertEquals(6, reader0.size());
                        Assert.assertEquals(6, reader1.size());

                        assertSql(replaceTimestampSuffix(expectedTable), tableName);

                        writer.removePartition(lastTs);

                        Assert.assertEquals(6, reader0.size());
                        Assert.assertEquals(6, reader1.size());
                        assertSql(replaceTimestampSuffix(expectedTableAfterDrop), tableName);
                        reader0.reload();
                        reader1.reload();
                        Assert.assertEquals(6, reader0.size());
                        Assert.assertEquals(6, reader1.size());
                    }
                    assertTableX(tableName, TableHeader +
                                    "1\t2023-10-10T00:00:00.000000Z\n" +
                                    "2\t2023-10-11T00:00:00.000000Z\n" +
                                    "3\t2023-10-12T00:00:00.000000Z\n" +
                                    "4\t2023-10-12T00:00:01.000000Z\n" +
                                    "6\t2023-10-12T00:00:02.000000Z\n" +
                                    "50\t2023-10-12T00:00:03.000000Z\n",
                            MinMaxCountHeader +
                                    "2023-10-10T00:00:00.000000Z\t2023-10-12T00:00:03.000000Z\t6\n");
                }
        );
    }

    @Test
    public void testDropActivePartitionWithUncommittedRowsNoReaders() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";
                    try {
                        createTableX(tableName,
                                TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "2\t2023-10-11T00:00:00.000000Z\n" +
                                        "3\t2023-10-12T00:00:00.000000Z\n" +
                                        "4\t2023-10-12T00:00:01.000000Z\n" +
                                        "6\t2023-10-12T00:00:02.000000Z\n" +
                                        "5\t2023-10-15T00:00:00.000000Z\n",
                                "insert into " + tableName + " values(1, '2023-10-10T00:00:00.000000Z')",
                                "insert into " + tableName + " values(2, '2023-10-11T00:00:00.000000Z')",
                                "insert into " + tableName + " values(3, '2023-10-12T00:00:00.000000Z')",
                                "insert into " + tableName + " values(4, '2023-10-12T00:00:01.000000Z')",
                                "insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')",
                                "insert into " + tableName + " values(6, '2023-10-12T00:00:02.000000Z')");
                        TimestampDriver driver = timestampType.getDriver();
                        try (TableWriter writer = getWriter(tableName)) {
                            long lastTs = driver.parseFloorLiteral(LastPartitionTs + "T00:00:00.000000Z");

                            TableWriter.Row row = writer.newRow(lastTs); // expected to be lost
                            row.putInt(0, 100);
                            row.append();

                            row = writer.newRow(driver.parseFloorLiteral("2023-10-10T00:00:07.000000Z")); // expected to survive
                            row.putInt(0, 50);
                            row.append();

                            row = writer.newRow(driver.parseFloorLiteral("2023-10-12T10:00:03.000000Z")); // expected to be lost
                            row.putInt(0, 75);
                            row.append();

                            writer.removePartition(lastTs);
                        }
                        assertTableX(tableName, TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "50\t2023-10-10T00:00:07.000000Z\n" +
                                        "2\t2023-10-11T00:00:00.000000Z\n" +
                                        "3\t2023-10-12T00:00:00.000000Z\n" +
                                        "4\t2023-10-12T00:00:01.000000Z\n" +
                                        "6\t2023-10-12T00:00:02.000000Z\n" +
                                        "75\t2023-10-12T10:00:03.000000Z\n",
                                MinMaxCountHeader +
                                        "2023-10-10T00:00:00.000000Z\t2023-10-12T10:00:03.000000Z\t7\n");
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropActivePartitionWithUncommittedRowsWithReaders() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";

                    final String expectedTable = TableHeader +
                            "1\t2023-10-10T00:00:00.000000Z\n" +
                            "2\t2023-10-11T00:00:00.000000Z\n" +
                            "3\t2023-10-12T00:00:00.000000Z\n" +
                            "4\t2023-10-12T00:00:01.000000Z\n" +
                            "6\t2023-10-12T00:00:02.000000Z\n" +
                            "5\t2023-10-15T00:00:00.000000Z\n";

                    try {
                        createTableX(tableName,
                                expectedTable,
                                "insert into " + tableName + " values(1, '2023-10-10T00:00:00.000000Z')",
                                "insert into " + tableName + " values(2, '2023-10-11T00:00:00.000000Z')",
                                "insert into " + tableName + " values(3, '2023-10-12T00:00:00.000000Z')",
                                "insert into " + tableName + " values(4, '2023-10-12T00:00:01.000000Z')",
                                "insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')",
                                "insert into " + tableName + " values(6, '2023-10-12T00:00:02.000000Z')");

                        final String expectedTableAfterDrop = TableHeader +
                                "1\t2023-10-10T00:00:00.000000Z\n" +
                                "2\t2023-10-11T00:00:00.000000Z\n" +
                                "3\t2023-10-12T00:00:00.000000Z\n" +
                                "4\t2023-10-12T00:00:01.000000Z\n" +
                                "6\t2023-10-12T00:00:02.000000Z\n" +
                                "50\t2023-10-12T00:00:03.000000Z\n";

                        TimestampDriver driver = timestampType.getDriver();
                        try (
                                TableReader reader0 = getReader(tableName);
                                TableReader reader1 = getReader(tableName);
                                TableWriter writer = getWriter(tableName)
                        ) {
                            long lastTs = driver.parseFloorLiteral(LastPartitionTs + "T00:00:00.000000Z");

                            TableWriter.Row row = writer.newRow(driver.parseFloorLiteral("2023-10-12T00:00:03.000000Z")); // earlier timestamp
                            row.putInt(0, 50);
                            row.append();

                            row = writer.newRow(lastTs);
                            row.putInt(0, 100); // will be removed
                            row.append();

                            Assert.assertEquals(6, reader0.size());
                            Assert.assertEquals(6, reader1.size());

                            assertSql(replaceTimestampSuffix(expectedTable), tableName);

                            writer.removePartition(lastTs);

                            Assert.assertEquals(6, reader0.size());
                            Assert.assertEquals(6, reader1.size());
                            assertSql(replaceTimestampSuffix(expectedTableAfterDrop), tableName);
                            reader0.reload();
                            reader1.reload();
                            Assert.assertEquals(6, reader0.size());
                            Assert.assertEquals(6, reader1.size());
                        }
                        assertTableX(tableName, TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "2\t2023-10-11T00:00:00.000000Z\n" +
                                        "3\t2023-10-12T00:00:00.000000Z\n" +
                                        "4\t2023-10-12T00:00:01.000000Z\n" +
                                        "6\t2023-10-12T00:00:02.000000Z\n" +
                                        "50\t2023-10-12T00:00:03.000000Z\n",
                                MinMaxCountHeader +
                                        "2023-10-10T00:00:00.000000Z\t2023-10-12T00:00:03.000000Z\t6\n");
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropAllPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";
                    try {
                        createTableX(tableName,
                                TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "2\t2023-10-11T00:00:00.000000Z\n" +
                                        "3\t2023-10-12T00:00:00.000000Z\n" +
                                        "4\t2023-10-12T00:00:01.000000Z\n" +
                                        "6\t2023-10-12T00:00:02.000000Z\n" +
                                        "5\t2023-10-15T00:00:00.000000Z\n",
                                "insert into " + tableName + " values(1, '2023-10-10T00:00:00.000000Z')",
                                "insert into " + tableName + " values(2, '2023-10-11T00:00:00.000000Z')",
                                "insert into " + tableName + " values(3, '2023-10-12T00:00:00.000000Z')",
                                "insert into " + tableName + " values(4, '2023-10-12T00:00:01.000000Z')",
                                "insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')",
                                "insert into " + tableName + " values(6, '2023-10-12T00:00:02.000000Z')");


                        execute("alter table " + tableName + " drop partition where timestamp > 0");
                        assertTableX(tableName, TableHeader, EmptyTableMinMaxCount); // empty table
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropAllPartitionsButThereAreNoPartitions() throws Exception {
        assertMemoryLeak(() -> {
                    final String tableName = "tab";
                    try {
                        createTableX(tableName, TableHeader);
                        try {
                            execute("alter table " + tableName + " drop partition where timestamp > 0", sqlExecutionContext);
                            Assert.fail();
                        } catch (CairoException e) {
                            Assert.assertEquals(("alter table " + tableName + " drop partition where ").length(), e.getPosition());
                            TestUtils.assertContains(e.getFlyweightMessage(), "no partitions matched WHERE clause");
                        }
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropLastPartitionNoReaders() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";
                    try {
                        createTableX(tableName,
                                TableHeader +
                                        "5\t2023-10-15T00:00:00.000000Z\n",
                                "insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')");
                        dropPartition(tableName, LastPartitionTs);
                        assertTableX(tableName, TableHeader, EmptyTableMinMaxCount);
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropLastPartitionWithReaders() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";
                    try {
                        createTableX(
                                tableName,
                                TableHeader +
                                        "5\t2023-10-15T00:00:00.000000Z\n" +
                                        "111\t2023-10-15T11:11:11.111111Z\n",
                                "insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')",
                                "insert into " + tableName + " values(111, '2023-10-15T11:11:11.111111Z')"
                        );

                        final String expectedTableInTransaction = TableHeader +
                                "777\t2023-10-13T00:10:00.000000Z\n" +
                                "5\t2023-10-15T00:00:00.000000Z\n" +
                                "888\t2023-10-15T00:00:00.000000Z\n" +
                                "111\t2023-10-15T11:11:11.111111Z\n";

                        final String expectedTableAfterDrop = TableHeader + "777\t2023-10-13T00:10:00.000000Z\n";

                        try (TableReader reader0 = getReader(tableName)) {
                            insert("insert into " + tableName + " values(888, '2023-10-15T00:00:00.000000Z');");
                            try (TableReader reader1 = getReader(tableName)) {
                                insert("insert into " + tableName + " values(777, '2023-10-13T00:10:00.000000Z');"); // o3

                                Assert.assertEquals(2, reader0.size());
                                Assert.assertEquals(3, reader1.size());
                                assertSql(replaceTimestampSuffix(expectedTableInTransaction), tableName);

                                dropPartition(tableName, LastPartitionTs);

                                Assert.assertEquals(2, reader0.size());
                                Assert.assertEquals(3, reader1.size());
                                reader0.reload();
                                reader1.reload();
                                Assert.assertEquals(1, reader0.size());
                                Assert.assertEquals(1, reader1.size());
                                try (
                                        RecordCursorFactory factory = select(tableName);
                                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                                ) {
                                    assertCursor(replaceTimestampSuffix(expectedTableAfterDrop), cursor, factory.getMetadata(), true);
                                }
                                assertFactoryMemoryUsage();
                            }
                        }
                        assertTableX(tableName, expectedTableAfterDrop, MinMaxCountHeader +
                                "2023-10-13T00:10:00.000000Z\t2023-10-13T00:10:00.000000Z\t1\n");
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropLastPartitionWithUncommittedO3RowsNoReaders() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";
                    try {
                        createTableX(tableName, TableHeader); // empty table
                        TimestampDriver driver = timestampType.getDriver();
                        try (TableWriter writer = getWriter(tableName)) {
                            long lastTs = driver.parseFloorLiteral(LastPartitionTs + "T00:00:00.000000Z");
                            long o3Ts = driver.parseFloorLiteral("2023-10-14T23:59:59.999999Z"); // o3 previous day

                            TableWriter.Row row = writer.newRow(lastTs); // will not survive, as it belongs in the active partition
                            row.putInt(0, 100);
                            row.append();

                            row = writer.newRow(o3Ts); // will survive
                            row.putInt(0, 300);
                            row.append();

                            writer.removePartition(lastTs);
                        }
                        assertTableX(
                                tableName,
                                TableHeader +
                                        "300\t2023-10-14T23:59:59.999999Z\n",
                                MinMaxCountHeader +
                                        "2023-10-14T23:59:59.999999Z\t2023-10-14T23:59:59.999999Z\t1\n"
                        );
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropLastPartitionWithUncommittedRowsNoReaders() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";
                    try {
                        createTableX(tableName, TableHeader); // empty table
                        TimestampDriver driver = timestampType.getDriver();
                        try (TableWriter writer = getWriter(tableName)) {
                            long prevTs = driver.parseFloorLiteral("2023-10-14T23:59:59.999999Z"); // previous day
                            long lastTs = driver.parseFloorLiteral(LastPartitionTs + "T00:00:00.000000Z");

                            TableWriter.Row row = writer.newRow(prevTs); // expected to survive
                            row.putInt(0, 300);
                            row.append();

                            row = writer.newRow(lastTs); // will not survive, as it belongs in the active partition
                            row.putInt(0, 100);
                            row.append();

                            writer.removePartition(lastTs);
                        }
                        assertTableX(
                                tableName,
                                TableHeader +
                                        "300\t2023-10-14T23:59:59.999999Z\n",
                                MinMaxCountHeader +
                                        "2023-10-14T23:59:59.999999Z\t2023-10-14T23:59:59.999999Z\t1\n"
                        );
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    @Test
    public void testDropPartitionsLongerPartitionName() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
                    final String tableName = "tab";
                    try {
                        createTableX(tableName,
                                TableHeader +
                                        "1\t2023-10-10T00:00:00.000000Z\n" +
                                        "2\t2023-10-11T00:00:00.000000Z\n" +
                                        "3\t2023-10-12T00:00:00.000000Z\n" +
                                        "4\t2023-10-12T00:00:01.000000Z\n" +
                                        "6\t2023-10-12T00:00:02.000000Z\n" +
                                        "5\t2023-10-15T00:00:00.000000Z\n",
                                "insert into " + tableName + " values(1, '2023-10-10T00:00:00.000000Z')",
                                "insert into " + tableName + " values(2, '2023-10-11T00:00:00.000000Z')",
                                "insert into " + tableName + " values(3, '2023-10-12T00:00:00.000000Z')",
                                "insert into " + tableName + " values(4, '2023-10-12T00:00:01.000000Z')",
                                "insert into " + tableName + " values(5, '2023-10-15T00:00:00.000000Z')",
                                "insert into " + tableName + " values(6, '2023-10-12T00:00:02.000000Z')");

                        dropPartition(tableName, "2023-10-12T23:59:59.999999Z");
                        dropPartition(tableName, "2023-10-11T23:59:59.999999Z");
                        dropPartition(tableName, "2023-10-10T23:59:59.999999Z");

                        assertTableX(tableName, TableHeader +
                                        "5\t2023-10-15T00:00:00.000000Z\n",
                                MinMaxCountHeader +
                                        "2023-10-15T00:00:00.000000Z\t2023-10-15T00:00:00.000000Z\t1\n");
                    } finally {
                        Misc.free(workerPool);
                    }
                }
        );
    }

    private void assertTableX(String tableName, String expectedRows, String expectedMinMaxCount) throws SqlException {
        engine.releaseAllReaders();
        assertSql(replaceTimestampSuffix(expectedRows), tableName);
        engine.releaseAllWriters();
        try (Path path = new Path().of(root).concat(tableName).concat(LastPartitionTs)) {
            TestUtils.txnPartitionConditionally(path, txn);
            path.$();
            Assert.assertFalse(Files.exists(path.$()));
        } finally {
            Misc.free(workerPool);
        }
        assertSql(replaceTimestampSuffix(expectedMinMaxCount), "select min(timestamp), max(timestamp), count() from " + tableName);
    }

    private void createTableX(String tableName, String expected, String... insertStmt) throws SqlException {
        TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                .col("id", ColumnType.INT)
                .timestamp(timestampType.getTimestampType());
        AbstractCairoTest.create(model);
        txn = 0;
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0, n = insertStmt.length; i < n; i++) {
            insert(insertStmt[i]);
        }
        assertSql(replaceTimestampSuffix(expected), tableName);

        workerPool = new TestWorkerPool(1);
        O3PartitionPurgeJob partitionPurgeJob = new O3PartitionPurgeJob(engine, 1);
        workerPool.assign(partitionPurgeJob);
        workerPool.freeOnExit(partitionPurgeJob);
        workerPool.start(); // closed by assertTableX
    }

    @SuppressWarnings("SameParameterValue")
    private void detachPartition(String tableName, String partitionName) throws SqlException {
        execute("alter table " + tableName + " detach partition list '" + partitionName + "'");
    }

    private void dropPartition(String tableName, String partitionName) throws SqlException {
        execute("alter table " + tableName + " drop partition list '" + partitionName + "'");
    }

    private void insert(String stmt) throws SqlException {
        AbstractCairoTest.execute(stmt);
        txn++;
    }

    private String replaceTimestampSuffix(String expected) {
        return ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? expected.replaceAll("Z\t", "000Z\t").replaceAll("Z\n", "000Z\n")
                : expected;
    }
}
