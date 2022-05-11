package io.questdb.cutlass.text;

import io.questdb.Metrics;
import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.PartitionBy;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.griffin.SqlException;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.LongList;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.*;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public class FileSplitterTest extends AbstractGriffinTest {

    //test csv with timestamp over buffer boundaries 
    //test csv with timestamp over buffer boundaries that's too long 
    //test csv with bad timestamp value 
    //test csv with quoted field that is too long and doesn't end before newline (should make a mess also with TextLexer/TextLoader)

    @Before
    public void before() throws IOException {
        inputRoot = new File(".").getAbsolutePath();
        if (inputWorkRoot == null) {
            inputWorkRoot = temp.newFolder("imports" + System.currentTimeMillis()).getAbsolutePath();
        }
    }

    @Test
    public void testFindChunkBoundariesForEmptyFile() throws Exception {
        executeWithPool(3, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) ->
                assertChunkBoundariesFor("test-quotes-empty.csv", null, sqlExecutionContext)
        );
    }

    @Test
    public void testFindChunkBoundariesForFileWithNoQuotes() throws Exception {
        executeWithPool(3, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) ->
                assertChunkBoundariesFor("test-import.csv", list(0, 4565, 9087, 13612), sqlExecutionContext)
        );
    }

    @Test
    public void testFindChunkBoundariesForFileWithLongLines() throws Exception {
        executeWithPool(3, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) ->
                assertChunkBoundariesFor("test-quotes-small.csv", list(0, 170, 241), sqlExecutionContext)
        );
    }

    @Test
    public void testFindChunkBoundariesForFileWithOneLongLine() throws Exception {
        executeWithPool(3, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) ->
                assertChunkBoundariesFor("test-quotes-oneline.csv", list(0, 234), sqlExecutionContext)
        );
    }

    @Test
    public void testFindChunkBoundariesWithPool() throws Exception {
        executeWithPool(4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) ->
                assertChunkBoundariesFor("test-quotes-oneline.csv", list(0, 234), sqlExecutionContext)
        );
    }

    @Test//60 seconds for boundary check + indexing of 56GB file  
    public void testProcessLargeCsvWithPool() throws Exception {
        executeWithPool(8, 16, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            //String inputDir = new File(".").getAbsolutePath();
            inputRoot = new File("E:/dev/tmp").getAbsolutePath();

            //try (Path path = new Path().of(inputDir).slash().concat("src/test/resources/csv/test-import.csv").$();
            try (Path path = new Path().of(inputRoot).slash().concat("trips300mil.csv").$();
                 FileIndexer indexer = new FileIndexer(sqlExecutionContext)) {

                DateFormat dateFormat = new TimestampFormatCompiler().compile("yyyy-MM-ddTHH:mm:ss.SSSUUUZ");
                indexer.of("trips300mil.csv", PartitionBy.YEAR, (byte) ',', 2, dateFormat, true);
                indexer.process();
            }
        });
    }

    private LongList list(long... values) {
        LongList result = new LongList();
        for (long l : values) {
            result.add(l);
        }
        return result;
    }

    private void assertChunkBoundariesFor(String fileName, LongList expectedBoundaries, SqlExecutionContext sqlExecutionContext) throws SqlException, IOException {
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        inputRoot = new File("./src/test/resources/csv/").getAbsolutePath();

        try (Path path = new Path().of(inputRoot).slash().concat(fileName).$();
             FileIndexer indexer = new FileIndexer(sqlExecutionContext)) {
            indexer.setMinChunkSize(1);
            indexer.of(fileName, PartitionBy.DAY, (byte) ',', -1, null, false);

            long fd = ff.openRO(path);
            Assert.assertTrue(fd > -1);

            try {
                LongList actualBoundaries = indexer.findChunkBoundaries(fd);
                Assert.assertEquals(expectedBoundaries, actualBoundaries);
            } finally {
                ff.close(fd);
            }
        }
    }

    @Ignore
    @Test//47s with on thread and old implementation
    public void testSimpleCsv() throws Exception {
        executeWithPool(4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            //String inputDir = new File(".").getAbsolutePath();
            inputRoot = new File("E:/dev/tmp").getAbsolutePath();

            //try (Path path = new Path().of(inputDir).slash().concat("src/test/resources/csv/test-import.csv").$();
            try (FileIndexer indexer = new FileIndexer(sqlExecutionContext)) {
                DateFormat dateFormat = new TimestampFormatCompiler().compile("yyyy-MM-ddTHH:mm:ss.SSSUUUZ");
                indexer.of("trips300mil.csv", PartitionBy.MONTH, (byte) ',', 2, dateFormat, true);
                indexer.process();
            }
        });
    }

    @Test
    public void testSimpleCsvWithPool() throws Exception {
        executeWithPool(4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            //Thread.sleep(180000);
        });
    }

    protected void executeWithPool(
            int workerCount,
            int queueCapacity,
            TextImportRunnable runnable
    ) throws Exception {
        executeVanilla(() -> {
            if (workerCount > 0) {

                int[] affinity = new int[workerCount];
                for (int i = 0; i < workerCount; i++) {
                    affinity[i] = -1;
                }

                WorkerPool pool = new WorkerPool(
                        new WorkerPoolAwareConfiguration() {
                            @Override
                            public int[] getWorkerAffinity() {
                                return affinity;
                            }

                            @Override
                            public int getWorkerCount() {
                                return workerCount;
                            }

                            @Override
                            public boolean haltOnError() {
                                return false;
                            }

                            @Override
                            public boolean isEnabled() {
                                return true;
                            }
                        },
                        Metrics.disabled()
                );

                final CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return FilesFacadeImpl.INSTANCE;
                    }

                    @Override
                    public CharSequence getInputWorkRoot() {
                        return FileSplitterTest.inputWorkRoot;
                    }

                    @Override
                    public CharSequence getInputRoot() {
                        return FileSplitterTest.inputRoot;
                    }
                };

                execute(pool, runnable, configuration);
            } else {
                // we need to create entire engine
                final CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return FilesFacadeImpl.INSTANCE;
                    }

                    @Override
                    public int getLatestByQueueCapacity() {
                        return queueCapacity;
                    }
                };
                execute(null, runnable, configuration);
            }
        });
    }

    protected static void execute(
            @Nullable WorkerPool pool,
            TextImportRunnable runnable,
            CairoConfiguration configuration
    ) throws Exception {
        final int workerCount = pool == null ? 1 : pool.getWorkerCount();
        try (
                final CairoEngine engine = new CairoEngine(configuration);
                final SqlCompiler compiler = new SqlCompiler(engine)
        ) {
            try (final SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, workerCount)
            ) {
                try {
                    if (pool != null) {
                        pool.assignCleaner(Path.CLEANER);
                        pool.assign(new TextImportJob(engine.getMessageBus())); //todo: copy-pasted, refactor this
                        pool.start(LOG);
                    }

                    runnable.run(engine, compiler, sqlExecutionContext);
                    Assert.assertEquals(0, engine.getBusyWriterCount());
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                } finally {
                    if (pool != null) {
                        pool.halt();
                    }
                }
            }
        }
    }

    static void executeVanilla(TestUtils.LeakProneCode code) throws Exception {
        TestUtils.assertMemoryLeak(code);
    }

    @FunctionalInterface
    interface TextImportRunnable {
        void run(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws Exception;
    }
}
