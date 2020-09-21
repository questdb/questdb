package io.questdb.cairo;

import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Path;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExtendedOnePageMemoryTest {
    private static final int FILE_SIZE = 1024;
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    private static final Path path = new Path(4096);

    private static FilesFacade ff;

    private static final AtomicBoolean FILE_MAP_FAIL = new AtomicBoolean(false);

    @Test
    public void testFailOnInitialMap() throws IOException {
        createFile(FILE_SIZE);
        try (ExtendableOnePageMemory mem = new ExtendableOnePageMemory()) {
            FILE_MAP_FAIL.set(true);
            try {
                mem.of(ff, path, FILE_SIZE, FILE_SIZE);
                Assert.fail();
            } catch (CairoException ex) {
                Assert.assertTrue(ex.getMessage().contains("Could not mmap"));
            }
        }
    }

    @Test
    public void testFailOnGrow() throws IOException {
        createFile(FILE_SIZE);
        try (ExtendableOnePageMemory mem = new ExtendableOnePageMemory()) {
            int sz = FILE_SIZE / 2;
            mem.of(ff, path, sz, sz);
            FILE_MAP_FAIL.set(true);
            sz *= 2;
            try {
                mem.grow(sz);
                Assert.fail();
            } catch (CairoException ex) {
                Assert.assertTrue(ex.getMessage().contains("Could not remap"));
            }
        }
    }

    private void createFile(int size) throws IOException {
        File f = temp.newFile();
        try (FileOutputStream fos = new FileOutputStream(f)) {
            for (int i = 0; i < size; i++) {
                fos.write(0);
            }
        }
        path.of(f.getCanonicalPath()).$();
    }

    @SuppressWarnings("resource")
    @BeforeClass
    public static void beforeClass() {
        ff = new FilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int mode) {
                if (FILE_MAP_FAIL.compareAndSet(true, false)) {
                    return FilesFacade.MAP_FAILED;
                }
                return super.mmap(fd, len, offset, mode);
            }

            @Override
            public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode) {
                if (FILE_MAP_FAIL.compareAndSet(true, false)) {
                    return FilesFacade.MAP_FAILED;
                }
                return super.mremap(fd, addr, previousSize, newSize, offset, mode);
            }
        };
    }

    @AfterClass
    public static void afterClass() {
        path.close();
    }
}
