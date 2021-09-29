package io.questdb.cairo;

import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExtendedOnePageMemoryTest {
    private static final int FILE_SIZE = 1024;
    private static final Path path = new Path(4096);
    private static final AtomicBoolean FILE_MAP_FAIL = new AtomicBoolean(false);
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    private static FilesFacade ff;

    @AfterClass
    public static void afterClass() {
        path.close();
    }

    @BeforeClass
    public static void beforeClass() {
        ff = new FilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (FILE_MAP_FAIL.compareAndSet(true, false)) {
                    return FilesFacade.MAP_FAILED;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
                if (FILE_MAP_FAIL.compareAndSet(true, false)) {
                    return FilesFacade.MAP_FAILED;
                }
                return super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
            }
        };
    }

    @Test
    public void testFailOnGrow() throws IOException {
        createFile();
        try (MemoryMR mem = new MemoryCMRImpl()) {
            int sz = FILE_SIZE / 2;
            mem.of(ff, path, sz, sz, MemoryTag.MMAP_DEFAULT);
            FILE_MAP_FAIL.set(true);
            sz *= 2;
            try {
                mem.extend(sz);
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not remap");
            }
        }
    }

    @Test
    public void testFailOnInitialMap() throws IOException {
        createFile();
        try (MemoryMR mem = new MemoryCMRImpl()) {
            FILE_MAP_FAIL.set(true);
            try {
                mem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not mmap");
            }
        }
    }

    private void createFile() throws IOException {
        File f = temp.newFile();
        try (FileOutputStream fos = new FileOutputStream(f)) {
            for (int i = 0; i < ExtendedOnePageMemoryTest.FILE_SIZE; i++) {
                fos.write(0);
            }
        }
        path.of(f.getCanonicalPath()).$();
    }
}
