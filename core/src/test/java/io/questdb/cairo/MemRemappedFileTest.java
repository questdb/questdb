package io.questdb.cairo;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Path;

public class MemRemappedFileTest {
    private static final int NPAGES = 1000;
    private static final int NCYCLES = 4;
    private static final Log LOG = LogFactory.getLog(MemRemappedFileTest.class);
    private static final FilesFacade ff = FilesFacadeImpl.INSTANCE;
    private static final long MAPPING_PAGE_SIZE = ff.getPageSize();
    private static CharSequence root;
    private static int nFile = 0;

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    private Path path = new Path(1_000_000);
    private long expectedTotal;

    @Test
    public void testReadOnlyMemory() throws Exception {
        double micros = test(() -> {
            return new ReadOnlyMemory(ff, path, MAPPING_PAGE_SIZE, 0);
        });
        LOG.info().$("ReadOnlyMemory took ").$(micros).$("ms").$();
    }

    @Test
    public void testOnePageMemory() throws Exception {
        double micros = test(() -> {
            return new OnePageMemory(ff, path, MAPPING_PAGE_SIZE);
        });
        LOG.info().$("OnePageMemory took ").$(micros).$("ms").$();
    }

    private double test(Callable<ReadOnlyColumn> memoryBuilder) throws Exception {
        long nanos = 0;
        for (int cycle = 0; cycle < NCYCLES; cycle++) {
            path.trimTo(0).concat(root).put(Files.SEPARATOR).concat("file" + nFile).$();
            nFile++;
            Random rand = new Random(0);
            expectedTotal = 0;
            long size = NPAGES * MAPPING_PAGE_SIZE;
            try (AppendMemory mem = new AppendMemory(ff, path, size);) {
                while (size-- > 0) {
                    byte b = (byte) rand.nextInt();
                    mem.putByte(b);
                    expectedTotal += b;
                }
            }

            nanos = System.nanoTime();
            long actualTotal = 0;
            long offset = 0;
            try (ReadOnlyColumn mem = memoryBuilder.call()) {
                for (int nPage = 0; nPage < 1000; nPage++) {
                    mem.grow(MAPPING_PAGE_SIZE * (nPage + 1));
                    for (int i = 0; i < MAPPING_PAGE_SIZE; i++) {
                        actualTotal += mem.getByte(offset);
                        offset++;
                    }
                }
            }
            nanos = System.nanoTime() - nanos;
            Assert.assertEquals(expectedTotal, actualTotal);

            ff.remove(path);
        }
        return nanos / 1000000.0;
    }

    @BeforeClass
    public static void before() throws IOException {
        root = temp.newFolder("root").getAbsolutePath();
    }
}
