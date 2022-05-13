package io.questdb.cutlass.text;

import io.questdb.Metrics;
import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.cairo.vm.MemoryPMARImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.griffin.*;
import io.questdb.mp.WorkerPool;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public class FileSplitterTest extends AbstractGriffinTest {
    private static final Rnd rnd = new Rnd();

    //test csv with timestamp over buffer boundaries
    //test csv with timestamp over buffer boundaries that's too long 
    //test csv with bad timestamp value 
    //test csv with quoted field that is too long and doesn't end before newline (should make a mess also with TextLexer/TextLoader)

    @Before
    public void before() throws IOException {
        rnd.reset();
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

    //random order file , 300 mil records , 100GB of data, 8 workers
    //scan + index
    //partition by year  - 1 min 56 s
    //partition by day   - 2 min 1s   (29k output files)
    //partition by hour  - 8 min 56s  
    //TODO: add max chunk size to config - e.g. 100 MB
    //start checking how to create copies of target table 
    @Test//60 seconds for boundary check + indexing of 56GB file  
    public void testProcessLargeCsvWithPool() throws Exception {
        executeWithPool(8, 16, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            //String inputDir = new File(".").getAbsolutePath();
            inputRoot = new File("E:/dev/tmp").getAbsolutePath();

            //try (Path path = new Path().of(inputDir).slash().concat("src/test/resources/csv/test-import.csv").$();
            try (FileIndexer indexer = new FileIndexer(sqlExecutionContext)) {

                DateFormat dateFormat = new TimestampFormatCompiler().compile("yyyy-MM-ddTHH:mm:ss.SSSUUUZ");
                //trips300mil.csv
                indexer.of("unordered_trips_300mil.csv", PartitionBy.YEAR, (byte) ',', 2, dateFormat, true);
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

    //1m 27s for boundaries + index 
    //1m 36s for boundaries + index + sort  
    @Test//47s with on thread and old implementation
    public void testSimpleCsv() throws Exception {
        executeWithPool(4, 8, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            //String inputDir = new File(".").getAbsolutePath();
            inputRoot = new File("E:/dev/tmp").getAbsolutePath();

            //try (Path path = new Path().of(inputDir).slash().concat("src/test/resources/csv/test-import.csv").$();
            try (FileIndexer indexer = new FileIndexer(sqlExecutionContext)) {
                DateFormat dateFormat = new TimestampFormatCompiler().compile("yyyy-MM-ddTHH:mm:ss.SSSUUUZ");
                //unordered_trips_300mil.csv
                indexer.of("trips300mil.csv", PartitionBy.YEAR, (byte) ',', 2, dateFormat, true);
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

    @Test
    public void testIndexMerge() throws Exception {
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path().of(inputRoot).slash().concat("chunks")) {
            int plen = path.length();
            path.concat("part").slash$();

            if (!ff.exists(path)) {
                int result = ff.mkdirs(path, engine.getConfiguration().getMkDirMode());
                if (result != 0) {
                    LOG.info().$("Couldn't create partition dir=").$(path).$();//TODO: maybe we can ignore it
                }
            }
            int chunks = 10;
            createAndSortChunkFiles(path, chunks, 10_000, 1_000_000);

            try (FileIndexer indexer = new FileIndexer(sqlExecutionContext)) {
                path.trimTo(plen);
                indexer.merge(path.$(), chunks);
            }
            path.trimTo(plen);
            ff.rmdir(path.$()); // clean all
        }
    }

    @Test
    public void testIndexSort() throws Exception {
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path().of(inputRoot).slash().concat("chunk").$();
             FileIndexer indexer = new FileIndexer(sqlExecutionContext)) {

            MemoryPMARImpl chunk = new MemoryPMARImpl(ff, path, ff.getPageSize(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
            try {
                long lo = 0;
                long hi = Long.MAX_VALUE;
                long range = hi - lo + 1;
                for (int i = 0; i < 10; i++) {
                    final long z = lo + rnd.nextPositiveLong() % range;
//                    final long z = 10 - i;
                    chunk.putLong(z);
                    chunk.putLong(i);
                }
            } finally {
                chunk.close(true, Vm.TRUNCATE_TO_POINTER);
            }

            //indexer.sort(path)//TODO: TEST !
            long len = ff.length(path.$());
            try (MemoryCMRImpl sorted = new MemoryCMRImpl(ff, path.$(), len, MemoryTag.MMAP_DEFAULT)) {
                long offset = 0;
                while (offset < len) {
                    long ts = sorted.getLong(offset);
                    long id = sorted.getLong(offset + 8);
                    System.err.println(ts);
                    System.err.println(id);
                    offset += 16;
                }
            }
            ff.remove(path);
//            System.err.println("---------------");
//            try(MemoryCMRImpl sorted = new MemoryCMRImpl(ff, path.chop$().put(".s").$(), len, MemoryTag.MMAP_DEFAULT)) {
//                long offset = 0;
//                while (offset < len) {
//                    long ts = sorted.getLong(offset);
//                    long id = sorted.getLong(offset + 8);
//                    System.err.println(ts);
//                    System.err.println(id);
//                    offset += 16;
//                }
//            }
        }
    }

    private void createAndSortChunkFiles(Path path, int nChunks, int chunkSizeMin, int chunkSizeMax) throws IOException {
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        int plen = path.length();
        for (int i = 0; i < nChunks; i++) {
            path.trimTo(plen);
            path.slash().concat("chunk").put(i).$();
            try (FileIndexer indexer = new FileIndexer(sqlExecutionContext)) {
                long rng = chunkSizeMax - chunkSizeMin + 1;
                final long count = chunkSizeMin + rnd.nextPositiveLong() % rng;

                MemoryPMARImpl chunk = new MemoryPMARImpl(ff, path, ff.getPageSize(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                try {
                    long lo = 0;
                    long hi = Long.MAX_VALUE;
                    long range = hi - lo + 1;
                    for (int v = 0; v < count; v++) {
                        final long z = lo + rnd.nextPositiveLong() % range;
                        chunk.putLong(z);
                        chunk.putLong(v);
                    }
                } finally {
                    chunk.close(true, Vm.TRUNCATE_TO_POINTER);
                }
                //indexer.sort(path);//TODO: fix 
            }
        }
        path.trimTo(plen);
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
