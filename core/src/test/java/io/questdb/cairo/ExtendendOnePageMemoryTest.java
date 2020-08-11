package io.questdb.cairo;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Path;

public class ExtendendOnePageMemoryTest {
    private static final int FILE_SIZE = 1024;
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    public static Path path;

    private static FilesFacade ff;

    private static final AtomicBoolean FILE_MAP_FAIL = new AtomicBoolean(false);

    @Test
    public void testFailOnInitialMap() {
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
    public void testFailOnGrow() {
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

    @SuppressWarnings("resource")
    @BeforeClass
    public static void beforeClass() throws IOException {
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

        String fileName = "aFile";
        File f = temp.newFile(fileName);
        path = new Path().of(f.getCanonicalPath());
        long fd = ff.openRW(path);
        ff.truncate(fd, FILE_SIZE);
        ff.close(fd);
    }

    @AfterClass
    public static void afterClass() throws IOException {
        path.close();
    }
}
