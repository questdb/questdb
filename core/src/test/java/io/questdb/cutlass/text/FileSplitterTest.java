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
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public class FileSplitterTest extends AbstractGriffinTest {

    //test csv with large string field 
    //test csv with timestamp over buffer boundaries 
    //test csv with timestamp over buffer boundaries that's too long 
    //test csv with bad timestamp value 
    //test csv with quoted field that is too long and doesn't end before newline (should make a mess also with TextLexer/TextLoader)

    @Before
    public void before() throws IOException {
        inputRoot = new File(".").getAbsolutePath();
        inputWorkRoot = temp.newFolder("imports").getAbsolutePath();
    }

    @Test
    public void testSimpleCsv() throws Exception {
        assertMemoryLeak(() -> {

            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            //String inputDir = new File(".").getAbsolutePath();
            String inputDir = new File("E:/dev/tmp").getAbsolutePath();

            //try (Path path = new Path().of(inputDir).slash().concat("src/test/resources/csv/test-import.csv").$();
            try (Path path = new Path().of(inputDir).slash().concat("trips300mil.csv").$();
                 FileSplitter splitter = new FileSplitter(sqlExecutionContext)) {

                DateFormat format = new TimestampFormatCompiler().compile("yyyy-MM-ddTHH:mm:ss.SSSUUUZ");

                long fd = ff.openRO(path);
                Assert.assertTrue(fd > -1);

                try {
                    //splitter.split("test-import-csv", fd, PartitionBy.MONTH, (byte) ',', 4, format, true);
                    splitter.split("test-import-csv", fd, PartitionBy.MONTH, (byte) ',', 2, format, true);
                } finally {
                    ff.close(fd);
                }
            }

            //Thread.sleep(180000);
        });
    }

    @Test
    public void testSimpleCsvWithPool() throws Exception {
        executeWithPool(4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            //String inputDir = new File(".").getAbsolutePath();
            String inputDir = new File("E:/dev/tmp").getAbsolutePath();

            //try (Path path = new Path().of(inputDir).slash().concat("src/test/resources/csv/test-import.csv").$();
            try (Path path = new Path().of(inputDir).slash().concat("trips300mil.csv").$();
                 FileSplitter splitter = new FileSplitter(sqlExecutionContext)) {

                DateFormat format = new TimestampFormatCompiler().compile("yyyy-MM-ddTHH:mm:ss.SSSUUUZ");

                long fd = ff.openRO(path);
                Assert.assertTrue(fd > -1);

                try {
                    //splitter.split("test-import-csv", fd, PartitionBy.MONTH, (byte) ',', 4, format, true);
                    splitter.split("test-import-csv", fd, PartitionBy.MONTH, (byte) ',', 2, format, true);
                } finally {
                    ff.close(fd);
                }
            }

            //Thread.sleep(180000);
        });
    }

    protected static void executeWithPool(
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
