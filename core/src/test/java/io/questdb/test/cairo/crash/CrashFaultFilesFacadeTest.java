package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class CrashFaultFilesFacadeTest extends AbstractTest {

    @Test
    public void testCrashDropsNewFileWithoutParentFsync() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("namespace-new-unsynced").getAbsolutePath();
        ff.markDurableBaseline(dir);
        try (Path file = new Path().of(dir).concat("new.d")) {
            writeAndSync(ff, file, (byte) 1);
            ff.crash(dir);
            Assert.assertFalse(java.nio.file.Files.exists(java.nio.file.Paths.get(file.toString())));
        }
    }

    @Test
    public void testCrashKeepsNewFileAfterParentFsync() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("namespace-new-synced").getAbsolutePath();
        ff.markDurableBaseline(dir);
        try (Path file = new Path().of(dir).concat("new.d")) {
            writeAndSync(ff, file, (byte) 2);
            syncDirectory(ff, dir);
            ff.crash(dir);
            assertFileBytes(file, (byte) 2);
        }
    }

    @Test
    public void testCrashKeepsNewFileAfterNoCacheParentFsyncAndClose() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("namespace-new-nocache-synced").getAbsolutePath();
        ff.markDurableBaseline(dir);
        try (Path file = new Path().of(dir).concat("new.d"); Path directory = new Path().of(dir)) {
            writeAndSync(ff, file, (byte) 8);
            final long dirFd = ff.openRONoCache(directory.$());
            Assert.assertTrue(dirFd > -1);
            ff.fsyncAndClose(dirFd);
            ff.crash(dir);
            assertFileBytes(file, (byte) 8);
        }
    }

    @Test
    public void testCrashRestoresUnlinkedFileWithoutParentFsync() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("namespace-unlink-unsynced").getAbsolutePath();
        try (Path file = new Path().of(dir).concat("old.d")) {
            writeAndSync(ff, file, (byte) 3);
            syncDirectory(ff, dir);
            ff.markDurableBaseline(dir);
            ff.remove(file.$());
            Assert.assertFalse(java.nio.file.Files.exists(java.nio.file.Paths.get(file.toString())));
            ff.crash(dir);
            assertFileBytes(file, (byte) 3);
        }
    }

    @Test
    public void testCrashKeepsSyncedUnlinkAbsent() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("namespace-unlink-synced").getAbsolutePath();
        try (Path file = new Path().of(dir).concat("old.d")) {
            writeAndSync(ff, file, (byte) 4);
            syncDirectory(ff, dir);
            ff.markDurableBaseline(dir);
            ff.remove(file.$());
            Assert.assertFalse(java.nio.file.Files.exists(java.nio.file.Paths.get(file.toString())));
            syncDirectory(ff, dir);
            ff.crash(dir);
            Assert.assertFalse(java.nio.file.Files.exists(java.nio.file.Paths.get(file.toString())));
        }
    }

    @Test
    public void testCrashRestoresSameParentRenameWithoutParentFsync() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("namespace-rename-unsynced").getAbsolutePath();
        try (Path oldFile = new Path().of(dir).concat("old.d"); Path newFile = new Path().of(dir).concat("new.d")) {
            writeAndSync(ff, oldFile, (byte) 5);
            syncDirectory(ff, dir);
            ff.markDurableBaseline(dir);
            Assert.assertEquals(0, ff.rename(oldFile.$(), newFile.$()));
            ff.crash(dir);
            assertFileBytes(oldFile, (byte) 5);
            Assert.assertFalse(java.nio.file.Files.exists(java.nio.file.Paths.get(newFile.toString())));
        }
    }

    @Test
    public void testCrashPreservesDurableHardLinkContent() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("namespace-hard-link").getAbsolutePath();
        try (Path source = new Path().of(dir).concat("source.d"); Path target = new Path().of(dir).concat("target.d")) {
            writeAndSync(ff, source, (byte) 7);
            syncDirectory(ff, dir);
            ff.markDurableBaseline(dir);
            Assert.assertEquals(io.questdb.std.Files.FILES_RENAME_OK, ff.hardLink(source.$(), target.$()));
            Assert.assertArrayEquals(ff.durableContentOf(source.toString()), ff.durableContentOf(target.toString()));
            syncDirectory(ff, dir);
            ff.crash(dir);
            assertFileBytes(source, (byte) 7);
            assertFileBytes(target, (byte) 7);
        }
    }

    @Test
    public void testCrashRestoresBothFilesAfterUnsyncedRenameOverwrite() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("namespace-rename-overwrite").getAbsolutePath();
        try (Path oldFile = new Path().of(dir).concat("old.d"); Path targetFile = new Path().of(dir).concat("target.d")) {
            writeAndSync(ff, oldFile, (byte) 1);
            writeAndSync(ff, targetFile, (byte) 2);
            syncDirectory(ff, dir);
            ff.markDurableBaseline(dir);
            Assert.assertEquals(0, ff.rename(oldFile.$(), targetFile.$()));
            ff.crash(dir);
            assertFileBytes(oldFile, (byte) 1);
            assertFileBytes(targetFile, (byte) 2);
        }
    }

    @Test
    public void testCrashKeepsSameParentRenameAfterParentFsync() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("namespace-rename-synced").getAbsolutePath();
        try (Path oldFile = new Path().of(dir).concat("old.d"); Path newFile = new Path().of(dir).concat("new.d")) {
            writeAndSync(ff, oldFile, (byte) 6);
            syncDirectory(ff, dir);
            ff.markDurableBaseline(dir);
            Assert.assertEquals(0, ff.rename(oldFile.$(), newFile.$()));
            syncDirectory(ff, dir);
            ff.crash(dir);
            Assert.assertFalse(java.nio.file.Files.exists(java.nio.file.Paths.get(oldFile.toString())));
            assertFileBytes(newFile, (byte) 6);
        }
    }

    @Test
    public void testSyncfsPersistsNamespaceChanges() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("namespace-syncfs").getAbsolutePath();
        ff.markDurableBaseline(dir);
        try (Path file = new Path().of(dir).concat("new.d")) {
            final long fd = openAndWrite(ff, file, (byte) 7);
            try {
                ff.syncfs(fd);
            } finally {
                ff.close(fd);
            }
            ff.crash(dir);
            assertFileBytes(file, (byte) 7);
        }
    }

    @Test
    public void testCrashTruncatesToLastFsyncedSize() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("crashroot").getAbsolutePath();
        try (Path path = new Path().of(dir).concat("a.d")) {
            long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(buf, 32, (byte) 1);
                Assert.assertEquals(16, ff.write(fd, buf, 16, 0));
                ff.fsync(fd);
                Assert.assertEquals(16, ff.write(fd, buf, 16, 16));
                Assert.assertEquals(32, ff.length(fd));
            } finally {
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
                ff.close(fd);
            }
            ff.crash(dir);
            Assert.assertEquals(16, ff.length(path.$()));
        }
    }

    @Test
    public void testArmCrashThrowsAfterNthDurabilityOp() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("crashroot3").getAbsolutePath();
        try (Path path = new Path().of(dir).concat("c.d")) {
            long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
            ff.armCrashAt(2); // crash on the 2nd durability op
            try {
                ff.fsync(fd); // op 1
                try {
                    ff.fsync(fd); // op 2 -> throws
                    Assert.fail("expected CrashSimulationError");
                } catch (CrashSimulationError expected) {
                    Assert.assertEquals(2, ff.durabilityOpCount());
                }
            } finally {
                ff.close(fd);
            }
        }
    }

    @Test
    public void testDroppedTreeEvictsDurabilitySnapshots() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("tracked-tree").getAbsolutePath();
        try (Path file = new Path().of(dir).concat("tracked.d"); Path root = new Path().of(dir)) {
            final long fd = ff.openRW(file.$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            final long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(buf, 8, (byte) 1);
                Assert.assertEquals(8, ff.write(fd, buf, 8, 0));
                ff.fsync(fd);
            } finally {
                Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
                ff.close(fd);
            }
            Assert.assertEquals(1, ff.trackedFileCount());
            Assert.assertNotNull(ff.durableContentOf(file.toString()));
            Assert.assertTrue(ff.rmdir(root));
            Assert.assertEquals("dropped table trees must not retain crash snapshots", 0, ff.trackedFileCount());
            Assert.assertNull("dropped trees must evict retained byte[] snapshots", ff.durableContentOf(file.toString()));
        }
    }

    @Test
    public void testRemovedFileEvictsDurabilitySnapshot() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("tracked-file").getAbsolutePath();
        try (Path file = new Path().of(dir).concat("tracked.d")) {
            final long fd = ff.openRW(file.$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            final long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(buf, 8, (byte) 2);
                Assert.assertEquals(8, ff.write(fd, buf, 8, 0));
                ff.fsync(fd);
            } finally {
                Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
                ff.close(fd);
            }
            Assert.assertNotNull(ff.durableContentOf(file.toString()));
            ff.remove(file.$()); // FilesFacade.remove delegates through the overridden removeQuiet path.
            Assert.assertEquals(0, ff.trackedFileCount());
            Assert.assertNull("removed files must evict retained byte[] snapshots", ff.durableContentOf(file.toString()));
        }
    }

    @Test
    public void testOpenAppendIsTrackedForDurability() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("crashroot4").getAbsolutePath();
        try (Path path = new Path().of(dir).concat("e.d")) {
            long fd = ff.openAppend(path.$());
            Assert.assertTrue(fd > -1);
            long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(buf, 8, (byte) 9);
                Assert.assertEquals(8, ff.append(fd, buf, 8));
                ff.fsync(fd); // durable = 8
                Assert.assertEquals(8, ff.append(fd, buf, 8)); // grow to 16, not fsynced
            } finally {
                Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
                ff.close(fd);
            }
            ff.crash(dir);
            Assert.assertEquals("append-opened file must roll back to fsync'd size", 8, ff.length(path.$()));
        }
    }

    @Test
    public void testBaselineKeepsPriorDataAndTornTailZeroesRange() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("crashroot2").getAbsolutePath();
        try (Path path = new Path().of(dir).concat("b.d")) {
            long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(buf, 64, (byte) 7);
                Assert.assertEquals(64, ff.write(fd, buf, 64, 0)); // 64 bytes, never fsynced
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
                ff.close(fd);
            }
            ff.markDurableBaseline(dir);   // treat the 64 bytes as durable even though never fsynced
            ff.tornTail(path.$(), 60, 4);  // zero bytes [60,64)
            ff.crash(dir);
            Assert.assertEquals("baseline size preserved", 64, ff.length(path.$()));
            long rd = ff.openRO(path.$());
            long rb = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                Assert.assertEquals(64, ff.read(rd, rb, 64, 0));
                Assert.assertEquals((byte) 7, Unsafe.getUnsafe().getByte(rb + 59));
                Assert.assertEquals((byte) 0, Unsafe.getUnsafe().getByte(rb + 60));
                Assert.assertEquals((byte) 0, Unsafe.getUnsafe().getByte(rb + 63));
            } finally {
                Unsafe.free(rb, 64, MemoryTag.NATIVE_DEFAULT);
                ff.close(rd);
            }
        }
    }

    private static void assertFileBytes(Path file, byte value) throws Exception {
        final byte[] expected = new byte[8];
        java.util.Arrays.fill(expected, value);
        Assert.assertArrayEquals(expected, java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(file.toString())));
    }

    private static long openAndWrite(CrashFaultFilesFacade ff, Path file, byte value) {
        final long fd = ff.openRW(file.$(), CairoConfiguration.O_NONE);
        Assert.assertTrue(fd > -1);
        final long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(buf, 8, value);
            Assert.assertEquals(8, ff.write(fd, buf, 8, 0));
        } finally {
            Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
        }
        return fd;
    }

    private static void syncDirectory(CrashFaultFilesFacade ff, String dir) {
        try (Path path = new Path().of(dir)) {
            final long fd = ff.openRO(path.$());
            Assert.assertTrue(fd > -1);
            try {
                ff.fsync(fd);
            } finally {
                ff.close(fd);
            }
        }
    }

    private static void writeAndSync(CrashFaultFilesFacade ff, Path file, byte value) {
        final long fd = openAndWrite(ff, file, value);
        try {
            ff.fsync(fd);
        } finally {
            ff.close(fd);
        }
    }
}
