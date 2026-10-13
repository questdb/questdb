package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class CrashFaultFilesFacadeTest extends AbstractTest {

    @Before
    public void assumePosix() {
        // The crash harness is POSIX-only: its model rewrites files under live handles, which Windows
        // forbids, and relies on parent-directory fsync, which Windows lacks (see FilesFacade.renameDurable).
        Assume.assumeFalse("crash harness is POSIX-only", Os.isWindows());
    }

    @Test
    public void testCrashDropsNewFileWithoutParentFsync() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("namespace-new-unsynced").getAbsolutePath();
        ff.markDurableBaseline(dir);
        try (Path file = new Path().of(dir).concat("new.d")) {
            writeAndSync(ff, file, (byte) 1);
            ff.powerLoss();
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
            ff.powerLoss();
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
            ff.powerLoss();
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
            ff.powerLoss();
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
            ff.powerLoss();
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
            ff.powerLoss();
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
            ff.powerLoss();
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
            ff.powerLoss();
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
            ff.powerLoss();
            ff.crash(dir);
            Assert.assertFalse(java.nio.file.Files.exists(java.nio.file.Paths.get(oldFile.toString())));
            assertFileBytes(newFile, (byte) 6);
        }
    }

    @Test
    public void testSyncfsNonWideFlushesOnlyItsFile() throws Exception {
        assertSyncfsScope(false);
    }

    @Test
    public void testSyncfsWideFlushesBothFiles() throws Exception {
        assertSyncfsScope(true);
    }

    @Test
    public void testSyncfsPersistsNamespaceChanges() throws Exception {
        // The filesystem-wide syncfs of Linux also commits the new file's name.
        assertSyncfsNamespaceScope(true);
    }

    @Test
    public void testSyncfsPerFileKeepsNoNamespaceChanges() throws Exception {
        // Where syncfs flushes one file (macOS, Windows) it is an fsync: the bytes, not the new directory entry.
        assertSyncfsNamespaceScope(false);
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
            ff.powerLoss();
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
    public void testKillKeepsBytesRewrittenAfterTheirFlush() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String powerLossDir = temp.newFolder("rewrite-power-loss").getAbsolutePath();
        final String killDir = temp.newFolder("rewrite-kill").getAbsolutePath();
        try (
                Path powerLossFile = new Path().of(powerLossDir).concat("a.d");
                Path killFile = new Path().of(killDir).concat("a.d");
                Path createdLater = new Path().of(killDir).concat("created-later.d");
                Path removedLater = new Path().of(killDir).concat("removed-later.d")
        ) {
            // the same history in both directories: byte 1 written and flushed, then rewritten in place
            // with byte 2 and never flushed again
            for (String dir : new String[]{powerLossDir, killDir}) {
                try (Path file = new Path().of(dir).concat("a.d")) {
                    writeAndSync(ff, file, (byte) 1);
                    syncDirectory(ff, dir);
                    ff.markDurableBaseline(dir);
                    final long fd = openAndWrite(ff, file, (byte) 2);
                    ff.close(fd);
                }
            }

            // a power loss brings back the flushed byte
            ff.powerLoss();
            ff.crash(powerLossDir);
            assertFileBytes(powerLossFile, (byte) 1);

            // a kill keeps the page cache: the image is taken at the armed kill point, the rewritten
            // byte survives, and nothing the process does after the kill point does
            writeAndSync(ff, removedLater, (byte) 4);
            ff.armKillAt(ff.killPointCount() + 1, killDir);
            final long fd = openAndWrite(ff, killFile, (byte) 2);
            ff.close(fd); // the kill point: the image is taken before this close runs
            Assert.assertTrue(ff.isKillImageTaken());
            final long laterFd = openAndWrite(ff, killFile, (byte) 3);
            ff.fsync(laterFd);
            ff.close(laterFd);
            writeAndSync(ff, createdLater, (byte) 5);
            ff.remove(removedLater.$());
            ff.crash(killDir);
            Assert.assertFalse("the crash must consume the kill image", ff.isKillImageTaken());
            assertFileBytes(killFile, (byte) 2);
            assertFileBytes(removedLater, (byte) 4);
            Assert.assertFalse(java.nio.file.Files.exists(java.nio.file.Paths.get(createdLater.toString())));
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
            ff.powerLoss();
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
            ff.powerLoss();
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

    private void assertSyncfsNamespaceScope(boolean isWide) throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        ff.setSyncfsFileSystemWide(isWide);
        final String dir = temp.newFolder(isWide ? "namespace-syncfs-wide" : "namespace-syncfs-per-file").getAbsolutePath();
        ff.markDurableBaseline(dir);
        try (Path file = new Path().of(dir).concat("new.d")) {
            final long fd = openAndWrite(ff, file, (byte) 7);
            try {
                ff.syncfs(fd);
            } finally {
                ff.close(fd);
            }
            ff.powerLoss();
            ff.crash(dir);
            if (isWide) {
                assertFileBytes(file, (byte) 7);
            } else {
                Assert.assertFalse("a per-file syncfs does not make the new name durable", ff.exists(file.$()));
            }
        }
    }

    private void assertSyncfsScope(boolean isWide) throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        ff.setSyncfsFileSystemWide(isWide);
        final String dir = temp.newFolder("syncfs-scope-" + isWide).getAbsolutePath();
        try (Path a = new Path().of(dir).concat("a.d"); Path b = new Path().of(dir).concat("b.d")) {
            writeAndSync(ff, a, (byte) 1);
            writeAndSync(ff, b, (byte) 1);
            ff.markDurableBaseline(dir);
            final long aFd = openAndWrite(ff, a, (byte) 2);
            try {
                final long bFd = openAndWrite(ff, b, (byte) 3);
                ff.close(bFd);
                ff.syncfs(aFd);
                ff.powerLoss();
            } finally {
                ff.close(aFd);
            }
            ff.crash(dir);
            assertFileBytes(a, (byte) 2);
            assertFileBytes(b, isWide ? (byte) 3 : (byte) 1);
        }
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
