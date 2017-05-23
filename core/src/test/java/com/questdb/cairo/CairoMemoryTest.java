package com.questdb.cairo;

import com.questdb.misc.Unsafe;
import com.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CairoMemoryTest {
    private static final int N = 1000000;
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testAppendAndReadWithReadOnlyMem() throws Exception {
        long used = Unsafe.getMemUsed();
        try (Path path = new Path(temp.newFile().getAbsolutePath())) {
            long size;
            try (AppendMemory mem = new AppendMemory(path, 2 * 4096, 0)) {
                for (int i = 0; i < N; i++) {
                    mem.putLong(i);
                }
                Assert.assertEquals(8L * N, size = mem.size());
            }
            try (ReadOnlyMemory mem = new ReadOnlyMemory(path, 4096, size)) {
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8));
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testAppendMemoryReuse() throws Exception {
        long used = Unsafe.getMemUsed();
        try (AppendMemory mem = new AppendMemory()) {
            for (int j = 0; j < 10; j++) {
                try (Path path = new Path(temp.newFile().getAbsolutePath())) {
                    long size;
                    mem.of(path, 2 * 4096, 0);
                    for (int i = 0; i < N; i++) {
                        mem.putLong(i);
                    }
                    Assert.assertEquals(8L * N, size = mem.size());

                    try (ReadOnlyMemory ro = new ReadOnlyMemory(path, 4096, size)) {
                        for (int i = 0; i < N; i++) {
                            Assert.assertEquals(i, ro.getLong(i * 8));
                        }
                    }
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testWriteAndRead() throws Exception {
        long used = Unsafe.getMemUsed();
        try (Path path = new Path(temp.newFile().getAbsolutePath())) {
            long size;
            try (ReadWriteMemory mem = new ReadWriteMemory(path, 2 * 4096, 0, 4096)) {
                for (int i = 0; i < N; i++) {
                    mem.putLong(i);
                }
                // read in place
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8));
                }

                Assert.assertEquals(8L * N, size = mem.size());
            }
            try (ReadWriteMemory mem = new ReadWriteMemory(path, 4096 * 4096, size, 4096)) {
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8));
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testWriteAndReadWithReadOnlyMem() throws Exception {
        long used = Unsafe.getMemUsed();
        try (Path path = new Path(temp.newFile().getAbsolutePath())) {
            long size;
            try (ReadWriteMemory mem = new ReadWriteMemory(path, 2 * 4096, 0, 4096)) {
                for (int i = 0; i < N; i++) {
                    mem.putLong(i);
                }
                Assert.assertEquals(8L * N, size = mem.size());
            }
            try (ReadOnlyMemory mem = new ReadOnlyMemory(path, 4096, size)) {
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8));
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }
}