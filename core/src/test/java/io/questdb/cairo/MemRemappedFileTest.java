package io.questdb.cairo;

import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Random;

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
    private final Path path = new Path(1_000_000);

    @Test
    public void testReadOnlyMemory() {
        LOG.info().$("ReadOnlyMemory starting").$();
        double micros = test(new MemoryCMRImpl());
        LOG.info().$("ReadOnlyMemory took ").$(micros).$("ms").$();
    }

    @Test
    public void testExtendableOnePageMemory() {
        LOG.info().$("ExtendableOnePageMemory starting").$();
        double micros = test(new MemoryCMRImpl());
        LOG.info().$("ExtendableOnePageMemory took ").$(micros).$("ms").$();
    }

    private double test(MemoryMR readMem) {
        long nanos = 0;
        try (MemoryMA appMem = Vm.getMAInstance()) {
            for (int cycle = 0; cycle < NCYCLES; cycle++) {
                path.trimTo(0).concat(root).concat("file" + nFile).$();
                nFile++;
                Random rand = new Random(0);
                long expectedTotal = 0;

                nanos = System.nanoTime();
                long actualTotal = 0;
                long offset = 0;
                for (int nPage = 0; nPage < NPAGES; nPage++) {
                    long newSize = MAPPING_PAGE_SIZE * (nPage + 1);
                    appMem.of(ff, path, newSize, MemoryTag.MMAP_DEFAULT);
                    appMem.jumpTo(newSize - MAPPING_PAGE_SIZE);
                    for (int i = 0; i < MAPPING_PAGE_SIZE; i++) {
                        byte b = (byte) rand.nextInt();
                        appMem.putByte(b);
                        expectedTotal += b;
                    }
                    if (nPage == 0) {
                        readMem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
                    } else {
                        readMem.extend(newSize);
                    }
                    for (int i = 0; i < MAPPING_PAGE_SIZE; i++) {
                        actualTotal += readMem.getByte(offset);
                        offset++;
                    }
                }

                nanos = System.nanoTime() - nanos;
                Assert.assertEquals(expectedTotal, actualTotal);

                ff.remove(path);
            }
            readMem.close();
            return nanos / 1000000.0;
        }
    }

    @BeforeClass
    public static void beforeClass() throws IOException {
        LOG.info().$("Starting").$();
        root = temp.newFolder("root").getAbsolutePath();
    }

    @AfterClass
    public static void afterClass() {
        LOG.info().$("Finished").$();
    }
}
