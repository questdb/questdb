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
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Misc;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

public class DropIndexTest extends AbstractGriffinTest {

    private static final String tableName = "sensors";
    private static final String columnName = "sensor_id";
    private static final int indexBlockValueSize = 32;
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


    private static final String expected = "sensor_id\ttemperature\tdegrees\tts\n" +
            "ALPHA\tHOT\t1548800833\t1970-01-01T00:00:00.000000Z\n" +
            "THETA\tCOLD\t-948263339\t1970-01-01T06:00:00.000000Z\n" +
            "THETA\tCOLD\t1868723706\t1970-01-01T12:00:00.000000Z\n" +
            "OMEGA\tHOT\t-2041844972\t1970-01-01T18:00:00.000000Z\n" +
            "OMEGA\tCOLD\t806715481\t1970-01-02T00:00:00.000000Z\n";


    protected static SqlExecutionContext sqlExecutionContext2;
    protected static SqlCompiler compiler2;

    private Path path;
    private int tablePathLen;

    @BeforeClass
    public static void setUpStatic() {
        AbstractGriffinTest.setUpStatic();
        compiler2 = new SqlCompiler(engine, null, snapshotAgent);
        sqlExecutionContext2 = new SqlExecutionContextImpl(engine, 1)
                .with(
                        AllowAllCairoSecurityContext.INSTANCE,
                        null,
                        null,
                        -1,
                        null);
    }

    @AfterClass
    public static void tearDownStatic() {
        AbstractGriffinTest.tearDownStatic();
        compiler2.close();
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        path = new Path().put(configuration.getRoot()).concat(tableName);
        tablePathLen = path.length();
    }

    @After
    @Override
    public void tearDown() {
        super.tearDown();
        path = Misc.free(path);
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
                    dropIndexStatement("підрахунок", "колонка"),
                    null,
                    12,
                    "Column is not indexed [name=колонка][errno=-100]"
            );
        });
    }

    @Test
    public void testDropIndexFailsWhenHardLinkFails() throws Exception {
        final FilesFacade noHardLinksFF = new FilesFacadeImpl() {
            int numberOfCalls = 0;

            @Override
            public int hardLink(LPSZ src, LPSZ hardLink) {
                ++numberOfCalls;
                if (numberOfCalls < 5) {
                    return super.hardLink(src, hardLink);
                }
                return -1;
            }

            @Override
            public int errno() {
                return numberOfCalls < 5 ? super.errno() : -1;
            }
        };

        assertMemoryLeak(noHardLinksFF, () -> {
            compile(CREATE_TABLE_STMT + " PARTITION BY HOUR", sqlExecutionContext);
            checkMetadataAndTxn(
                    path,
                    tableName,
                    columnName,
                    PartitionBy.HOUR,
                    1,
                    0,
                    0,
                    true,
                    indexBlockValueSize
            );
            try {
                compile(dropIndexStatement(tableName, columnName), sqlExecutionContext);
                Assert.fail();
            } catch (SqlException expected) {
                TestUtils.assertContains(expected.getFlyweightMessage(), "Cannot DROP INDEX for [txn=1, table=sensors, column=sensor_id]");
                TestUtils.assertContains(expected.getFlyweightMessage(), "[-1] cannot hardLink ");
                path.trimTo(tablePathLen);
                checkMetadataAndTxn(
                        path,
                        tableName,
                        columnName,
                        PartitionBy.HOUR,
                        1,
                        0,
                        0,
                        true,
                        indexBlockValueSize
                );

                // check the original files exist
                Assert.assertEquals(5, countDFiles(tableName, columnName, 0L));
                Assert.assertEquals(10, countIndexFiles(tableName, columnName, 0L));

                // check there are no leftover link files
                Assert.assertEquals(0, countDFiles(tableName, columnName, 1L));
            }
        });
    }

    @Test
    public void testDropIndexNonPartitionedTable() throws Exception {
        dropIndexOfIndexedColumn(
                PartitionBy.NONE,
                true,
                indexBlockValueSize,
                2 // default partition with two files (*.k, *.v)
        );
    }

    @Test
    public void testDropIndexPartitionedByDayTable() throws Exception {
        dropIndexOfIndexedColumn(
                PartitionBy.DAY,
                true,
                indexBlockValueSize,
                4 // 2 partitions times two files (*.k, *.v)
        );
    }

    @Test
    public void testDropIndexPartitionedByHourTable() throws Exception {
        dropIndexOfIndexedColumn(
                PartitionBy.HOUR,
                true,
                indexBlockValueSize,
                10 // 5 partitions times two files (*.k, *.v)
        );
    }

    @Test
    public void testDropIndexStructureOfTableAndColumnIncrease() throws Exception {
        assertMemoryLeak(configuration.getFilesFacade(), () -> {
            compile(CREATE_TABLE_STMT + " PARTITION BY DAY", sqlExecutionContext);
            checkMetadataAndTxn(
                    path,
                    tableName,
                    columnName,
                    PartitionBy.DAY,
                    1,
                    0,
                    0,
                    true,
                    indexBlockValueSize
            );
            assertSql(tableName, expected);
            executeOperation(
                    dropIndexStatement(tableName, columnName),
                    CompiledQuery.ALTER,
                    CompiledQuery::getAlterOperation
            );
            path.trimTo(tablePathLen);
            checkMetadataAndTxn(
                    path,
                    tableName,
                    columnName,
                    PartitionBy.DAY,
                    2,
                    1,
                    1,
                    false,
                    configuration.getIndexValueBlockSize()
            );
            assertSql(tableName, expected);
            Assert.assertEquals(2, countDFiles(tableName, columnName, 1L));
            Assert.assertEquals(0, countIndexFiles(tableName, columnName, 1L));
        });
    }

    @Test
    public void testDropIndexPreservesIndexFilesWhenThereIsATransactionReadingThem() throws Exception {
        assertMemoryLeak(configuration.getFilesFacade(), () -> {
            compile(CREATE_TABLE_STMT + " PARTITION BY HOUR", sqlExecutionContext);
            checkMetadataAndTxn(
                    path,
                    "sensors",
                    "sensor_id",
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
            final AtomicReference<Throwable> readerFailure = new AtomicReference<>();

            // reader thread
            new Thread(() -> {

                final String select = "SELECT ts, sensor_id FROM sensors WHERE sensor_id = 'OMEGA' and ts > '1970-01-01T01:59:06.000000Z'";
                Path path2 = new Path().put(configuration.getRoot()).concat(tableName);
                try {
                    for (int i = 0; i < 5; i++) {
                        try (RecordCursorFactory factory = compiler2.compile(select, sqlExecutionContext2).getRecordCursorFactory()) {
                            try (RecordCursor ignored = factory.getCursor(sqlExecutionContext2)) {
                                // 1st reader sees the index as DROP INDEX has not happened yet
                                // the readers that follow do not see the index, because it has been dropped
                                boolean isIndexed = i == 0;
                                path2.trimTo(tablePathLen);
                                checkMetadataAndTxn(
                                        path2,
                                        "sensors",
                                        "sensor_id",
                                        PartitionBy.HOUR,
                                        isIndexed ? 1L : 2L,
                                        isIndexed ? 0L : 1L,
                                        isIndexed ? 0L : 1L,
                                        isIndexed,
                                        isIndexed ? 32 : defaultIndexValueBlockSize
                                );
                                Assert.assertEquals(5, countDFiles(tableName, columnName, isIndexed ? 0L : 1L));
                                Assert.assertEquals(isIndexed ? 10 : 0, countIndexFiles(tableName, columnName, isIndexed ? 0L : 1L));
                                if (isIndexed) {
                                    startBarrier.await(); // release writer
                                }
                            }
                        }
                        Thread.sleep(100L);
                    }
                } catch (Throwable e) {
                    readerFailure.set(e);
                } finally {
                    Misc.free(path2);
                    engine.releaseAllReaders();
                    endLatch.countDown();
                }
            }).start();

            // drop the index, there will be a reader seeing the index
            startBarrier.await();
            compile(dropIndexStatement("sensors", "sensor_id"), sqlExecutionContext);
            endLatch.await();

            Throwable fail = readerFailure.get();
            if (fail != null) {
                Assert.fail(fail.getMessage());
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            // no more readers from this point
            path.trimTo(tablePathLen);
            checkMetadataAndTxn(
                    path,
                    "sensors",
                    "sensor_id",
                    PartitionBy.HOUR,
                    2L,
                    1L,
                    1L,
                    false,
                    defaultIndexValueBlockSize
            );
            Assert.assertEquals(5, countDFiles(tableName, columnName, 0L));
            Assert.assertEquals(10, countIndexFiles(tableName, columnName, 0L));
            Assert.assertEquals(5, countDFiles(tableName, columnName, 1L));
            Assert.assertEquals(0, countIndexFiles(tableName, columnName, 1L));

            // clean after
            compile("VACUUM TABLE sensors", sqlExecutionContext);
            path.trimTo(tablePathLen);
            checkMetadataAndTxn(
                    path,
                    "sensors",
                    "sensor_id",
                    PartitionBy.HOUR,
                    2,
                    1,
                    1,
                    false,
                    defaultIndexValueBlockSize
            );
            assertSql(tableName, expected); // content is not gone
            Assert.assertEquals(0, countDFiles(tableName, columnName, 0L));
            Assert.assertEquals(0, countIndexFiles(tableName, columnName, 0L));
        });
    }

    @Test
    public void testDropIndexSimultaneously() throws Exception {
        assertMemoryLeak(configuration.getFilesFacade(), () -> {
            compile(CREATE_TABLE_STMT + " PARTITION BY HOUR", sqlExecutionContext);
            checkMetadataAndTxn(
                    path,
                    "sensors",
                    "sensor_id",
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
                Path path2 = new Path().put(configuration.getRoot()).concat(tableName);
                try {
                    CompiledQuery cc = compiler2.compile(dropIndexStatement("sensors", "sensor_id"), sqlExecutionContext2);
                    startBarrier.await();
                    try (OperationFuture future = cc.execute(null)) {
                        future.await();
                    }
                } catch (Throwable e) {
                    concurrentDropIndexFailure.set(e);
                } finally {
                    Misc.free(path2);
                    engine.releaseAllWriters();
                    endLatch.countDown();
                }
            }).start();

            // drop the index concurrently
            startBarrier.await();
            try {
                compile(dropIndexStatement("sensors", "sensor_id"), sqlExecutionContext);
                endLatch.await();
                // we didnt fail, check they did
                Throwable fail = concurrentDropIndexFailure.get();
                Assert.assertNotNull(fail);
                TestUtils.assertContains(fail.getMessage(), "Column is not indexed [name=sensor_id][errno=-100]");
            } catch (EntryUnavailableException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table busy [reason=Alter table execute]");
                // we failed, check they didnt
                Assert.assertNull(concurrentDropIndexFailure.get());
                endLatch.await();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            path.trimTo(tablePathLen);
            checkMetadataAndTxn(
                    path,
                    "sensors",
                    "sensor_id",
                    PartitionBy.HOUR,
                    2,
                    1,
                    1,
                    false,
                    defaultIndexValueBlockSize
            );
            assertSql(tableName, expected); // content is not gone
            Assert.assertEquals(5, countDFiles(tableName, columnName, 1L));
            Assert.assertEquals(0, countIndexFiles(tableName, columnName, 1L));
        });
    }

    private void dropIndexOfIndexedColumn(int partitionedBy, boolean isIndexed, int indexValueBockSize, int numIndexFiles) throws Exception {
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
                    path,
                    tableName,
                    columnName,
                    partitionedBy,
                    1,
                    0,
                    0,
                    isIndexed,
                    indexValueBockSize
            );

            compile(dropIndexStatement(tableName, columnName), sqlExecutionContext);
            path.trimTo(tablePathLen);
            checkMetadataAndTxn(
                    path,
                    tableName,
                    columnName,
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
            Assert.assertEquals(expectedDFiles, countDFiles(tableName, columnName, 1L));
            // check index files have been dropped
            Assert.assertEquals(0, countIndexFiles(tableName, columnName, 1L));

            checkMetadataAndTxn(
                    path,
                    tableName,
                    "temperature",
                    partitionedBy,
                    2,
                    1,
                    1,
                    true,
                    4
            );
            // other indexed column remains intact
            Assert.assertEquals(expectedDFiles, countDFiles(tableName, "temperature", 0L));
            // check index files have been dropped
            Assert.assertEquals(expectedDFiles * 2, countIndexFiles(tableName, "temperature", 0L));
        });
    }

    private static String dropIndexStatement(String tableName, String columnName) {
        return "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " DROP INDEX";
    }

    private static void checkMetadataAndTxn(
            Path path,
            String tableName,
            String columnName,
            int partitionedBy,
            long expectedReaderVersion,
            long expectedStructureVersion,
            long expectedColumnVersion,
            boolean isColumnIndexed,
            int indexValueBlockSize
    ) {
        try (TxReader txReader = new TxReader(ff)) {
            txReader.ofRO(path, partitionedBy);
            txReader.unsafeLoadAll();
            Assert.assertEquals(expectedStructureVersion, txReader.getStructureVersion());
            Assert.assertEquals(expectedReaderVersion, txReader.getTxn());
            Assert.assertEquals(expectedReaderVersion, txReader.getVersion());
            Assert.assertEquals(expectedColumnVersion, txReader.getColumnVersion());
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                TableReaderMetadata metadata = reader.getMetadata();
                Assert.assertEquals(partitionedBy, metadata.getPartitionBy());
                Assert.assertEquals(expectedStructureVersion, metadata.getStructureVersion());
                int columnIndex = metadata.getColumnIndex(columnName);
                Assert.assertEquals(isColumnIndexed, metadata.isColumnIndexed(columnIndex));
                Assert.assertEquals(indexValueBlockSize, metadata.getIndexValueBlockCapacity(columnIndex));
            }
        }
    }

    private long countIndexFiles(String tableName, String columnName, long txn) throws IOException {
        return countFiles((String) configuration.getRoot(), tableName, columnName, txn, DropIndexTest::isIndexFile);
    }

    private long countDFiles(String tableName, String columnName, long txn) throws IOException {
        return countFiles((String) configuration.getRoot(), tableName, columnName, txn, DropIndexTest::isDFile);
    }

    @FunctionalInterface
    public interface FileChecker {
        boolean isTargetFile(java.nio.file.Path tablePath, java.nio.file.Path filePath, String columnName, long txn);
    }

    private static long countFiles(String rootPath, String tableName, String columnName, long txn, FileChecker fileChecker) throws IOException {
        final java.nio.file.Path tablePath = FileSystems.getDefault().getPath(rootPath, tableName);
        return Files.find(
                tablePath,
                Integer.MAX_VALUE,
                (filePath, _attrs) -> fileChecker.isTargetFile(tablePath, filePath, columnName, txn)
        ).count();
    }

    private static boolean isIndexFile(java.nio.file.Path tablePath, java.nio.file.Path filePath, String columnName, long txn) {
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

    private static boolean isDFile(java.nio.file.Path tablePath, java.nio.file.Path filePath, String columnName, long txn) {
        final String fn = filePath.getFileName().toString();
        boolean isDFile = !filePath.getParent().equals(tablePath);
        if (!isDFile) {
            return false;
        }
        return fn.endsWith(columnName + (txn < 1 ? ".d" : ".d." + txn));
    }
}
