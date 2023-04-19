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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class DropIndexTest extends AbstractGriffinTest {

    private static final String columnName = "sensor_id";
    private static final String expected = "sensor_id\ttemperature\tdegrees\tts\n" +
            "ALPHA\tHOT\t1548800833\t1970-01-01T00:00:00.000000Z\n" +
            "THETA\tCOLD\t-948263339\t1970-01-01T06:00:00.000000Z\n" +
            "THETA\tCOLD\t1868723706\t1970-01-01T12:00:00.000000Z\n" +
            "OMEGA\tHOT\t-2041844972\t1970-01-01T18:00:00.000000Z\n" +
            "OMEGA\tCOLD\t806715481\t1970-01-02T00:00:00.000000Z\n";
    private static final int indexBlockValueSize = 32;
    private static final String tableName = "sensors";
    private static final String CREATE_TABLE_STMT = "CREATE TABLE " + tableName + " AS (" +
            "    SELECT" +
            "        rnd_symbol('ALPHA', 'OMEGA', 'THETA') " + columnName + "," +
            "        rnd_symbol('HOT', 'COLD') temperature," +
            "        rnd_int() degrees," +
            "        timestamp_sequence(0, 21600000000) ts" + // 6h
            "    FROM long_sequence(5)" +
            "), INDEX(" + columnName + " CAPACITY " + indexBlockValueSize + ")" +
            ", INDEX(temperature CAPACITY 4) " +
            "TIMESTAMP(ts)"; // 5 partitions by hour, 2 partitions by day
    protected static SqlCompiler compiler2;
    protected static SqlExecutionContext sqlExecutionContext2;
    private static Path path;
    private static int tablePathLen;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractGriffinTest.setUpStatic();
        compiler2 = new SqlCompiler(engine, null, snapshotAgent);
        sqlExecutionContext2 = TestUtils.createSqlExecutionCtx(engine);
        CharSequence dirName = tableName + TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
        path = new Path().put(configuration.getRoot()).concat(dirName);
        tablePathLen = path.length();
    }

    @AfterClass
    public static void tearDownStatic() throws Exception {
        compiler2 = Misc.free(compiler2);
        path = Misc.free(path);
        AbstractGriffinTest.tearDownStatic();
    }

    @Test
    public void dropIndexColumnTop() throws SqlException, NumericException {
        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)) {
            model.col("a", ColumnType.INT);
            model.timestamp("ts");
            createPopulateTable(model, 5, "2022-02-24", 2);
        }
        compile("alter table " + tableName + " add column sym symbol index");
        compile("insert into " + tableName +
                " select x, timestamp_sequence('2022-02-24T01:30', 1000000000), rnd_symbol('A', 'B', 'C') from long_sequence(5)");

        assertSql(tableName, "a\tts\tsym\n" +
                "1\t2022-02-24T00:23:59.800000Z\t\n" +
                "2\t2022-02-24T00:47:59.600000Z\t\n" +
                "3\t2022-02-24T01:11:59.400000Z\t\n" +
                "1\t2022-02-24T01:30:00.000000Z\tA\n" +
                "4\t2022-02-24T01:35:59.200000Z\t\n" +
                "2\t2022-02-24T01:46:40.000000Z\tA\n" +
                "5\t2022-02-24T01:59:59.000000Z\t\n" +
                "3\t2022-02-24T02:03:20.000000Z\tB\n" +
                "4\t2022-02-24T02:20:00.000000Z\tC\n" +
                "5\t2022-02-24T02:36:40.000000Z\tC\n");


        assertSql("select * from " + tableName + " where sym is null", "a\tts\tsym\n" +
                "1\t2022-02-24T00:23:59.800000Z\t\n" +
                "2\t2022-02-24T00:47:59.600000Z\t\n" +
                "3\t2022-02-24T01:11:59.400000Z\t\n" +
                "4\t2022-02-24T01:35:59.200000Z\t\n" +
                "5\t2022-02-24T01:59:59.000000Z\t\n");

        compile("alter table " + tableName + " alter column sym drop index");

        assertSql(tableName, "a\tts\tsym\n" +
                "1\t2022-02-24T00:23:59.800000Z\t\n" +
                "2\t2022-02-24T00:47:59.600000Z\t\n" +
                "3\t2022-02-24T01:11:59.400000Z\t\n" +
                "1\t2022-02-24T01:30:00.000000Z\tA\n" +
                "4\t2022-02-24T01:35:59.200000Z\t\n" +
                "2\t2022-02-24T01:46:40.000000Z\tA\n" +
                "5\t2022-02-24T01:59:59.000000Z\t\n" +
                "3\t2022-02-24T02:03:20.000000Z\tB\n" +
                "4\t2022-02-24T02:20:00.000000Z\tC\n" +
                "5\t2022-02-24T02:36:40.000000Z\tC\n");

        assertSql("select * from " + tableName + " where sym is null", "a\tts\tsym\n" +
                "1\t2022-02-24T00:23:59.800000Z\t\n" +
                "2\t2022-02-24T00:47:59.600000Z\t\n" +
                "3\t2022-02-24T01:11:59.400000Z\t\n" +
                "4\t2022-02-24T01:35:59.200000Z\t\n" +
                "5\t2022-02-24T01:59:59.000000Z\t\n");

        assertSql("select * from " + tableName + " where sym = 'A'", "a\tts\tsym\n" +
                "1\t2022-02-24T01:30:00.000000Z\tA\n" +
                "2\t2022-02-24T01:46:40.000000Z\tA\n");
    }

    @Test
    public void dropIndexColumnTopLastPartition() throws SqlException, NumericException {
        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)) {
            model.col("a", ColumnType.INT);
            model.timestamp("ts");
            createPopulateTable(model, 5, "2022-02-24", 2);
        }
        compile("alter table " + tableName + " add column sym symbol index");

        assertSql(tableName, "a\tts\tsym\n" +
                "1\t2022-02-24T00:23:59.800000Z\t\n" +
                "2\t2022-02-24T00:47:59.600000Z\t\n" +
                "3\t2022-02-24T01:11:59.400000Z\t\n" +
                "4\t2022-02-24T01:35:59.200000Z\t\n" +
                "5\t2022-02-24T01:59:59.000000Z\t\n");

        compile("alter table " + tableName + " alter column sym drop index");

        assertSql(tableName, "a\tts\tsym\n" +
                "1\t2022-02-24T00:23:59.800000Z\t\n" +
                "2\t2022-02-24T00:47:59.600000Z\t\n" +
                "3\t2022-02-24T01:11:59.400000Z\t\n" +
                "4\t2022-02-24T01:35:59.200000Z\t\n" +
                "5\t2022-02-24T01:59:59.000000Z\t\n");

        assertSql("select * from " + tableName + " where sym is null", "a\tts\tsym\n" +
                "1\t2022-02-24T00:23:59.800000Z\t\n" +
                "2\t2022-02-24T00:47:59.600000Z\t\n" +
                "3\t2022-02-24T01:11:59.400000Z\t\n" +
                "4\t2022-02-24T01:35:59.200000Z\t\n" +
                "5\t2022-02-24T01:59:59.000000Z\t\n");

        assertSql("select * from " + tableName + " where sym = 'A'", "a\tts\tsym\n");
    }

    @Test
    public void testDropIndexFailsWhenHardLinkFails() throws Exception {
        final FilesFacade noHardLinksFF = new TestFilesFacadeImpl() {
            int numberOfCalls = 0;

            @Override
            public int errno() {
                return numberOfCalls < 5 ? super.errno() : -1;
            }

            @Override
            public int hardLink(LPSZ src, LPSZ hardLink) {
                ++numberOfCalls;
                if (numberOfCalls < 5) {
                    return super.hardLink(src, hardLink);
                }
                return -1;
            }
        };

        assertMemoryLeak(noHardLinksFF, () -> {
            compile(CREATE_TABLE_STMT + " PARTITION BY HOUR", sqlExecutionContext);
            checkMetadataAndTxn(
                    PartitionBy.HOUR,
                    1,
                    0,
                    0,
                    true,
                    indexBlockValueSize
            );
            try {
                compile(dropIndexStatement(), sqlExecutionContext);
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Cannot DROP INDEX for [txn=1, table=sensors, column=sensor_id]");
                TestUtils.assertContains(e.getFlyweightMessage(), "[-1] cannot hardLink ");
                path.trimTo(tablePathLen);
                checkMetadataAndTxn(
                        PartitionBy.HOUR,
                        1,
                        0,
                        0,
                        true,
                        indexBlockValueSize
                );

                // check the original files exist
                Assert.assertEquals(5, countDFiles(0L));
                Assert.assertEquals(10, countIndexFiles(0L));

                // check there are no leftover link files
                Assert.assertEquals(0, countDFiles(1L));
            }
        });
    }

    @Test
    public void testDropIndexNonPartitionedTable() throws Exception {
        dropIndexOfIndexedColumn(PartitionBy.NONE);
    }

    @Test
    public void testDropIndexOfNonIndexedColumnShouldFail() throws Exception {
        assertMemoryLeak(() -> {
            compile(
                    "CREATE TABLE підрахунок AS (" +
                            "  select " +
                            "    rnd_symbol('K1', 'K2') колонка " +
                            "  from long_sequence(317)" +
                            ")",
                    sqlExecutionContext
            );
            engine.releaseAllWriters();
            assertFailure(
                    "ALTER TABLE підрахунок ALTER COLUMN колонка DROP INDEX",
                    null,
                    36,
                    "Column is not indexed [name=колонка]"
            );
        });
    }

    @Test
    public void testDropIndexPartitionedByDayTable() throws Exception {
        dropIndexOfIndexedColumn(PartitionBy.DAY);
    }

    @Test
    public void testDropIndexPartitionedByHourTable() throws Exception {
        dropIndexOfIndexedColumn(PartitionBy.HOUR);
    }

    @Test
    public void testDropIndexPreservesIndexFilesWhenThereIsATransactionReadingThem() throws Exception {
        assertMemoryLeak(configuration.getFilesFacade(), () -> {
            compile(CREATE_TABLE_STMT + " PARTITION BY HOUR", sqlExecutionContext);
            checkMetadataAndTxn(
                    PartitionBy.HOUR,
                    1,
                    0,
                    0,
                    true,
                    indexBlockValueSize
            );

            final int defaultIndexValueBlockSize = configuration.getIndexValueBlockSize();
            final String select = "SELECT ts, sensor_id FROM sensors WHERE sensor_id = 'OMEGA' and ts > '1970-01-01T01:59:06.000000Z'";
            TableToken tableToken = engine.verifyTableName(tableName);
            try (Path path2 = new Path().put(configuration.getRoot()).concat(tableToken)) {
                for (int i = 0; i < 5; i++) {
                    try (RecordCursorFactory factory = compiler2.compile(select, sqlExecutionContext2).getRecordCursorFactory()) {
                        try (RecordCursor ignored = factory.getCursor(sqlExecutionContext2)) {
                            // 1st reader sees the index as DROP INDEX has not happened yet
                            // the readers that follow do not see the index, because it has been dropped
                            boolean isIndexed = i == 0;
                            path2.trimTo(tablePathLen);
                            checkMetadataAndTxn(
                                    path2,
                                    "sensor_id",
                                    PartitionBy.HOUR,
                                    isIndexed ? 1L : 2L,
                                    isIndexed ? 0L : 1L,
                                    isIndexed ? 0L : 1L,
                                    isIndexed,
                                    isIndexed ? 32 : defaultIndexValueBlockSize
                            );
                            Assert.assertEquals(5, countDFiles(isIndexed ? 0L : 1L));
                            Assert.assertEquals(isIndexed ? 10 : 0, countIndexFiles(isIndexed ? 0L : 1L));
                            if (isIndexed) {
                                compile(dropIndexStatement(), sqlExecutionContext);
                            }
                        }
                    }
                    Thread.sleep(100L);
                }
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            // no more readers from this point
            path.trimTo(tablePathLen);
            checkMetadataAndTxn(
                    PartitionBy.HOUR,
                    2L,
                    1L,
                    1L,
                    false,
                    defaultIndexValueBlockSize
            );
            Assert.assertEquals(5, countDFiles(0L));
            Assert.assertEquals(10, countIndexFiles(0L));
            Assert.assertEquals(5, countDFiles(1L));
            Assert.assertEquals(0, countIndexFiles(1L));

            // clean after
            compile("VACUUM TABLE sensors", sqlExecutionContext);
            path.trimTo(tablePathLen);
            checkMetadataAndTxn(
                    PartitionBy.HOUR,
                    2,
                    1,
                    1,
                    false,
                    defaultIndexValueBlockSize
            );
            assertSql(tableName, expected); // content is not gone
            Assert.assertEquals(0, countDFiles(0L));
            Assert.assertEquals(0, countIndexFiles(0L));
        });
    }

    @Test
    public void testDropIndexSimultaneously() throws Exception {
        assertMemoryLeak(configuration.getFilesFacade(), () -> {
            compile(CREATE_TABLE_STMT + " PARTITION BY HOUR", sqlExecutionContext);
            checkMetadataAndTxn(
                    PartitionBy.HOUR,
                    1,
                    0,
                    0,
                    true,
                    indexBlockValueSize
            );

            final CyclicBarrier startBarrier = new CyclicBarrier(2);
            final SOCountDownLatch endLatch = new SOCountDownLatch(1);
            final int defaultIndexValueBlockSize = configuration.getIndexValueBlockSize();
            final AtomicReference<Throwable> concurrentDropIndexFailure = new AtomicReference<>();

            // drop index thread
            new Thread(() -> {
                try {
                    CompiledQuery cc = compiler2.compile(dropIndexStatement(), sqlExecutionContext2);
                    startBarrier.await();
                    try (OperationFuture future = cc.execute(null)) {
                        future.await();
                    }
                } catch (Throwable e) {
                    concurrentDropIndexFailure.set(e);
                } finally {
                    engine.releaseAllWriters();
                    Path.clearThreadLocals();
                    endLatch.countDown();
                }
            }).start();

            // drop the index concurrently
            startBarrier.await();
            try {
                compile(dropIndexStatement(), sqlExecutionContext);
                endLatch.await();
                // we didnt fail, check they did
                Throwable fail = concurrentDropIndexFailure.get();
                Assert.assertNotNull(fail);
                if (fail instanceof EntryUnavailableException) {
                    // reason can be Alter table execute or Engine cleanup (unknown)
                    TestUtils.assertContains(fail.getMessage(), "table busy [reason=");
                } else if (fail instanceof SqlException) {
                    TestUtils.assertContains(fail.getMessage(), "Column is not indexed");
                }
            } catch (EntryUnavailableException e) {
                // reason can be Alter table execute or Engine cleanup (unknown)
                TestUtils.assertContains(e.getFlyweightMessage(), "table busy [reason=");
                // we failed, check they didnt
                Assert.assertNull(concurrentDropIndexFailure.get());
                endLatch.await();
            } catch (SqlException | CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "Column is not indexed");
                // we failed, check they didnt
                Assert.assertNull(concurrentDropIndexFailure.get());
                endLatch.await();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            path.trimTo(tablePathLen);
            checkMetadataAndTxn(
                    PartitionBy.HOUR,
                    2,
                    1,
                    1,
                    false,
                    defaultIndexValueBlockSize
            );
            assertSql(tableName, expected); // content is not gone
            Assert.assertEquals(5, countDFiles(1L));
            Assert.assertEquals(0, countIndexFiles(1L));
        });
    }

    @Test
    public void testDropIndexStructureOfTableAndColumnIncrease() throws Exception {
        assertMemoryLeak(configuration.getFilesFacade(), () -> {
            compile(CREATE_TABLE_STMT + " PARTITION BY DAY", sqlExecutionContext);
            checkMetadataAndTxn(
                    PartitionBy.DAY,
                    1,
                    0,
                    0,
                    true,
                    indexBlockValueSize
            );
            assertSql(tableName, expected);
            executeOperation(
                    dropIndexStatement(),
                    CompiledQuery.ALTER
            );
            path.trimTo(tablePathLen);
            checkMetadataAndTxn(
                    PartitionBy.DAY,
                    2,
                    1,
                    1,
                    false,
                    configuration.getIndexValueBlockSize()
            );
            assertSql(tableName, expected);
            Assert.assertEquals(2, countDFiles(1L));
            Assert.assertEquals(0, countIndexFiles(1L));
        });
    }

    @Test
    public void testDropIndexSyntaxErrors0() throws Exception {
        assertFailure(
                "ALTER TABLE sensors ALTER COLUMN sensor_id dope INDEX",
                CREATE_TABLE_STMT,
                43,
                "'add', 'drop', 'cache' or 'nocache' expected found 'dope'"
        );
    }

    @Test
    public void testDropIndexSyntaxErrors1() throws Exception {
        assertFailure(
                "ALTER TABLE sensors ALTER COLUMN sensor_id DROP",
                CREATE_TABLE_STMT,
                47,
                "'index' expected"
        );
    }

    @Test
    public void testDropIndexSyntaxErrors2() throws Exception {
        assertFailure(
                "ALTER TABLE sensors ALTER COLUMN sensor_id DROP INDEX,",
                CREATE_TABLE_STMT,
                53,
                "unexpected token [,] while trying to drop index"
        );
    }

    private static void checkMetadataAndTxn(
            int partitionedBy,
            long expectedReaderVersion,
            long expectedStructureVersion,
            long expectedColumnVersion,
            boolean isColumnIndexed,
            int indexValueBlockSize
    ) {
        checkMetadataAndTxn(
                path,
                columnName,
                partitionedBy,
                expectedReaderVersion,
                expectedStructureVersion,
                expectedColumnVersion,
                isColumnIndexed,
                indexValueBlockSize
        );
    }

    private static void checkMetadataAndTxn(
            Path path,
            String columnName,
            int partitionedBy,
            long expectedReaderVersion,
            long expectedStructureVersion,
            long expectedColumnVersion,
            boolean isColumnIndexed,
            int indexValueBlockSize
    ) {
        try (TxReader txReader = new TxReader(ff)) {
            int pathLen = path.length();
            txReader.ofRO(path.concat(TXN_FILE_NAME).$(), partitionedBy);
            path.trimTo(pathLen);
            txReader.unsafeLoadAll();
            Assert.assertEquals(expectedStructureVersion, txReader.getStructureVersion());
            Assert.assertEquals(expectedReaderVersion, txReader.getTxn());
            Assert.assertEquals(expectedReaderVersion, txReader.getVersion());
            Assert.assertEquals(expectedColumnVersion, txReader.getColumnVersion());
            try (TableReader reader = getReader(tableName)) {
                TableReaderMetadata metadata = reader.getMetadata();
                Assert.assertEquals(partitionedBy, metadata.getPartitionBy());
                Assert.assertEquals(expectedStructureVersion, metadata.getStructureVersion());
                int columnIndex = metadata.getColumnIndex(columnName);
                Assert.assertEquals(isColumnIndexed, metadata.isColumnIndexed(columnIndex));
                Assert.assertEquals(indexValueBlockSize, metadata.getIndexValueBlockCapacity(columnIndex));
            }
        }
    }

    private static long countFiles(String columnName, long txn, FileChecker fileChecker) throws IOException {
        TableToken tableToken = engine.verifyTableName(tableName);
        final java.nio.file.Path tablePath = FileSystems.getDefault().getPath(
                (String) configuration.getRoot(),
                tableToken.getDirName()
        );
        try (Stream<?> stream = Files.find(
                tablePath,
                Integer.MAX_VALUE,
                (filePath, _attrs) -> fileChecker.accepts(tablePath, filePath, columnName, txn)
        )) {
            return stream.count();
        }
    }

    private static String dropIndexStatement() {
        sink.clear();
        return sink.put("ALTER TABLE ").put(tableName)
                .put(" ALTER COLUMN ").put(columnName)
                .put(" DROP INDEX")
                .toString();
    }

    private static boolean isDataFile(
            java.nio.file.Path tablePath,
            java.nio.file.Path filePath,
            String columnName,
            long txn
    ) {
        final String fn = filePath.getFileName().toString();
        boolean isDFile = !filePath.getParent().equals(tablePath);
        if (!isDFile) {
            return false;
        }
        return fn.endsWith(columnName + (txn < 1 ? ".d" : ".d." + txn));
    }

    private static boolean isIndexFile(
            java.nio.file.Path tablePath,
            java.nio.file.Path filePath,
            String columnName,
            long txn
    ) {
        final String fn = filePath.getFileName().toString();
        boolean isIndexFile = !filePath.getParent().equals(tablePath);
        if (!isIndexFile) {
            return false;
        }
        String K = columnName + ".k";
        String V = columnName + ".v";
        if (txn > 0) {
            K = K + "." + txn;
            V = V + "." + txn;
        }
        return fn.endsWith(K) || fn.endsWith(V);
    }

    private long countDFiles(long txn) throws IOException {
        return countFiles(columnName, txn, DropIndexTest::isDataFile);
    }

    private long countIndexFiles(long txn) throws IOException {
        return countFiles(columnName, txn, DropIndexTest::isIndexFile);
    }

    private void dropIndexOfIndexedColumn(int partitionedBy) throws Exception {
        assertMemoryLeak(configuration.getFilesFacade(), () -> {
            String createStatement = CREATE_TABLE_STMT;
            int expectedDFiles = -1;
            switch (partitionedBy) {
                case PartitionBy.NONE:
                    expectedDFiles = 1;
                    break;
                case PartitionBy.HOUR:
                    createStatement = CREATE_TABLE_STMT + " PARTITION BY HOUR";
                    expectedDFiles = 5;
                    break;
                case PartitionBy.DAY:
                    createStatement = CREATE_TABLE_STMT + " PARTITION BY DAY";
                    expectedDFiles = 2;
                    break;
                default:
                    Assert.fail("unsupported partitionBy type");
            }
            compile(createStatement, sqlExecutionContext);
            checkMetadataAndTxn(
                    partitionedBy,
                    1,
                    0,
                    0,
                    true,
                    indexBlockValueSize
            );

            compile(dropIndexStatement(), sqlExecutionContext);
            path.trimTo(tablePathLen);
            checkMetadataAndTxn(
                    partitionedBy,
                    2,
                    1,
                    1,
                    false,
                    configuration.getIndexValueBlockSize()
            );
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            // check links have been created
            Assert.assertEquals(expectedDFiles, countDFiles(1L));
            // check index files have been dropped
            Assert.assertEquals(0, countIndexFiles(1L));

            checkMetadataAndTxn(
                    path,
                    "temperature",
                    partitionedBy,
                    2,
                    1,
                    1,
                    true,
                    4
            );
            // other indexed column remains intact
            Assert.assertEquals(expectedDFiles,
                    countFiles("temperature", 0L, DropIndexTest::isDataFile)
            );
            // check index files exist
            Assert.assertEquals(expectedDFiles * 2,
                    countFiles("temperature", 0L, DropIndexTest::isIndexFile)
            );
        });
    }

    @FunctionalInterface
    public interface FileChecker {
        boolean accepts(java.nio.file.Path tablePath, java.nio.file.Path filePath, String columnName, long txn);
    }
}
