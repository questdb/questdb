package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

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
    public void testCrashKeepsHardLinkedFileWrittenThroughNewName() throws Exception {
        // RENAME COLUMN hard-links a column file that was never written (all its values inline), then writes through
        // the new name. The power loss must not empty the shared file through the old name, which no sync ever saw.
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        ff.setSyncfsFileSystemWide(true);
        final String root = newHardLinkRoot("hard-link-unwritten-source");
        ff.markDurableBaseline(root);
        try (
                Path old1 = new Path().of(root).concat("a").concat("x.d");
                Path new1 = new Path().of(root).concat("b").concat("x.d");
                Path old2 = new Path().of(root).concat("b").concat("y.d");
                Path new2 = new Path().of(root).concat("a").concat("y.d")
        ) {
            createEmptyFile(ff, old1);
            createEmptyFile(ff, old2);
            Assert.assertEquals(Files.FILES_RENAME_OK, ff.hardLink(old1.$(), new1.$()));
            Assert.assertEquals(Files.FILES_RENAME_OK, ff.hardLink(old2.$(), new2.$()));
            final long fd1 = openAndWrite(ff, new1, (byte) 1);
            final long fd2 = openAndWrite(ff, new2, (byte) 2);
            try {
                ff.syncfs(fd1);
            } finally {
                ff.close(fd1);
                ff.close(fd2);
            }
            ff.powerLoss();
            ff.crash(root);
            assertFileBytes(old1, (byte) 1);
            assertFileBytes(new1, (byte) 1);
            assertFileBytes(old2, (byte) 2);
            assertFileBytes(new2, (byte) 2);
        }
    }

    @Test
    public void testCrashRestoresHardLinkFromItsNewestSync() throws Exception {
        // Both names of the file are tracked. A sync through one name makes the file durable under both, so the
        // rollback restores the file from that sync, not from the other name's older one.
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String root = newHardLinkRoot("hard-link-newest-sync");
        try (
                Path old1 = new Path().of(root).concat("a").concat("x.d");
                Path new1 = new Path().of(root).concat("b").concat("x.d");
                Path old2 = new Path().of(root).concat("b").concat("y.d");
                Path new2 = new Path().of(root).concat("a").concat("y.d")
        ) {
            writeAndSync(ff, old1, (byte) 1);
            writeAndSync(ff, old2, (byte) 1);
            Assert.assertEquals(Files.FILES_RENAME_OK, ff.hardLink(old1.$(), new1.$()));
            Assert.assertEquals(Files.FILES_RENAME_OK, ff.hardLink(old2.$(), new2.$()));
            ff.markDurableBaseline(root);
            writeAndSync(ff, new1, (byte) 2);
            writeAndSync(ff, new2, (byte) 3);
            ff.powerLoss();
            ff.crash(root);
            assertFileBytes(old1, (byte) 2);
            assertFileBytes(new1, (byte) 2);
            assertFileBytes(old2, (byte) 3);
            assertFileBytes(new2, (byte) 3);
        }
    }

    @Test
    public void testCrashRestoresHardLinkFromNewestSyncOfEachKind() throws Exception {
        // Every sync that copies the file's bytes to the device cache through one name makes that name the newest, not
        // only fsync.
        for (SyncKind kind : SyncKind.values()) {
            final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
            final String root = newHardLinkRoot("hard-link-sync-kind-" + kind);
            try (
                    Path old1 = new Path().of(root).concat("a").concat("x.d");
                    Path new1 = new Path().of(root).concat("b").concat("x.d");
                    Path old2 = new Path().of(root).concat("b").concat("y.d");
                    Path new2 = new Path().of(root).concat("a").concat("y.d");
                    Path other = new Path().of(root).concat("other.d")
            ) {
                writeAndSync(ff, old1, (byte) 1);
                writeAndSync(ff, old2, (byte) 1);
                writeAndSync(ff, other, (byte) 1);
                Assert.assertEquals(Files.FILES_RENAME_OK, ff.hardLink(old1.$(), new1.$()));
                Assert.assertEquals(Files.FILES_RENAME_OK, ff.hardLink(old2.$(), new2.$()));
                ff.markDurableBaseline(root);
                syncThrough(ff, kind, new1, (byte) 2);
                syncThrough(ff, kind, new2, (byte) 3);
                // A journal commit and device flush through another file, which makes a sync_file_range durable.
                final long fd = ff.openRW(other.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(fd > -1);
                try {
                    ff.fdatasync(fd);
                } finally {
                    ff.close(fd);
                }
                ff.powerLoss();
                ff.crash(root);
                assertFileBytes(old1, (byte) 2);
                assertFileBytes(new1, (byte) 2);
                assertFileBytes(old2, (byte) 3);
                assertFileBytes(new2, (byte) 3);
            }
        }
    }

    @Test
    public void testCrashKeepsHardLinkSourceAfterNewNameRemoved() throws Exception {
        // The new name is removed after the flush, so only the old name, never written through, still points to the
        // file. The flush made the file durable, so the old name keeps its bytes.
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        ff.setSyncfsFileSystemWide(true);
        final String dir = temp.newFolder("hard-link-new-name-removed").getAbsolutePath();
        ff.markDurableBaseline(dir);
        try (Path oldName = new Path().of(dir).concat("old.d"); Path newName = new Path().of(dir).concat("new.d")) {
            createEmptyFile(ff, oldName);
            syncDirectory(ff, dir);
            Assert.assertEquals(Files.FILES_RENAME_OK, ff.hardLink(oldName.$(), newName.$()));
            final long fd = openAndWrite(ff, newName, (byte) 4);
            try {
                ff.syncfs(fd);
            } finally {
                ff.close(fd);
            }
            Assert.assertTrue(ff.removeQuiet(newName.$()));
            syncDirectory(ff, dir);
            ff.powerLoss();
            ff.crash(dir);
            assertFileBytes(oldName, (byte) 4);
            Assert.assertFalse(ff.exists(newName.$()));
        }
    }

    @Test
    public void testCrashRestoresFileSyncedThroughUnsavedHardLink() throws Exception {
        // An fsync through a new name makes the file's bytes durable even though the new name itself is lost (its
        // directory was never synced), so the old name shows what that fsync wrote.
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("hard-link-unsaved-new-name").getAbsolutePath();
        try (Path oldName = new Path().of(dir).concat("old.d"); Path newName = new Path().of(dir).concat("new.d")) {
            writeAndSync(ff, oldName, (byte) 5);
            ff.markDurableBaseline(dir);
            Assert.assertEquals(Files.FILES_RENAME_OK, ff.hardLink(oldName.$(), newName.$()));
            writeAndSync(ff, newName, (byte) 6);
            ff.powerLoss();
            ff.crash(dir);
            assertFileBytes(oldName, (byte) 6);
            Assert.assertFalse(ff.exists(newName.$()));
        }
    }

    @Test
    public void testCrashIgnoresHardLinkSyncsAfterPowerLoss() throws Exception {
        // A sync between the power loss and crash() does not count, also when it would decide which name of the file
        // holds its newest durable bytes.
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String root = newHardLinkRoot("hard-link-sync-after-power-loss");
        try (
                Path old1 = new Path().of(root).concat("a").concat("x.d");
                Path new1 = new Path().of(root).concat("b").concat("x.d");
                Path old2 = new Path().of(root).concat("b").concat("y.d");
                Path new2 = new Path().of(root).concat("a").concat("y.d")
        ) {
            writeAndSync(ff, old1, (byte) 1);
            writeAndSync(ff, old2, (byte) 1);
            Assert.assertEquals(Files.FILES_RENAME_OK, ff.hardLink(old1.$(), new1.$()));
            Assert.assertEquals(Files.FILES_RENAME_OK, ff.hardLink(old2.$(), new2.$()));
            ff.markDurableBaseline(root);
            writeAndSync(ff, new1, (byte) 2);
            writeAndSync(ff, new2, (byte) 3);
            ff.powerLoss();
            // A close-time flush through the old names, after the power loss.
            writeAndSync(ff, old1, (byte) 4);
            writeAndSync(ff, old2, (byte) 4);
            ff.crash(root);
            assertFileBytes(old1, (byte) 2);
            assertFileBytes(new1, (byte) 2);
            assertFileBytes(old2, (byte) 3);
            assertFileBytes(new2, (byte) 3);
        }
    }

    @Test
    public void testCrashRestoresHardLinkRenamedAfterItsSync() throws Exception {
        // A rename keeps the name's place in the sync order: the renamed name still holds the file's newest bytes.
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String root = newHardLinkRoot("hard-link-renamed-after-sync");
        try (
                Path old1 = new Path().of(root).concat("a").concat("x.d");
                Path new1 = new Path().of(root).concat("b").concat("x.d");
                Path renamed1 = new Path().of(root).concat("b").concat("z.d");
                Path old2 = new Path().of(root).concat("b").concat("y.d");
                Path new2 = new Path().of(root).concat("a").concat("y.d");
                Path renamed2 = new Path().of(root).concat("a").concat("z.d")
        ) {
            writeAndSync(ff, old1, (byte) 1);
            writeAndSync(ff, old2, (byte) 1);
            Assert.assertEquals(Files.FILES_RENAME_OK, ff.hardLink(old1.$(), new1.$()));
            Assert.assertEquals(Files.FILES_RENAME_OK, ff.hardLink(old2.$(), new2.$()));
            ff.markDurableBaseline(root);
            writeAndSync(ff, new1, (byte) 2);
            writeAndSync(ff, new2, (byte) 3);
            Assert.assertEquals(Files.FILES_RENAME_OK, ff.rename(new1.$(), renamed1.$()));
            Assert.assertEquals(Files.FILES_RENAME_OK, ff.rename(new2.$(), renamed2.$()));
            syncDirectory(ff, root + File.separator + "a");
            syncDirectory(ff, root + File.separator + "b");
            ff.powerLoss();
            ff.crash(root);
            assertFileBytes(old1, (byte) 2);
            assertFileBytes(renamed1, (byte) 2);
            assertFileBytes(old2, (byte) 3);
            assertFileBytes(renamed2, (byte) 3);
            Assert.assertFalse(ff.exists(new1.$()));
            Assert.assertFalse(ff.exists(new2.$()));
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
    public void testTruncateAfterPowerLossDoesNotShortenTheImage() throws Exception {
        // A failed commit's error path can truncate a file after the power loss (the WAL segment rollback cuts a column
        // back to its append offset). A real machine runs none of it, so the image keeps the file's synced size,
        // including a zero value stored through the mapping past the last non-zero byte, which the content model
        // cannot tell from unwritten space.
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("crashroot-truncate-after-power-loss").getAbsolutePath();
        try (Path path = new Path().of(dir).concat("v.d")) {
            final long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            try {
                Assert.assertTrue(ff.allocate(fd, 16));
                final long addr = ff.mmap(fd, 16, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
                Unsafe.getUnsafe().setMemory(addr, 8, (byte) 1); // the next 8 bytes are a stored zero
                ff.fdatasync(fd);
                ff.powerLoss();
                ff.munmap(addr, 16, MemoryTag.MMAP_DEFAULT);
                Assert.assertTrue(ff.truncate(fd, 0));
            } finally {
                ff.close(fd);
            }
            final byte[] expected = new byte[16];
            java.util.Arrays.fill(expected, 0, 8, (byte) 1);
            Assert.assertArrayEquals(expected, ff.powerLossImageOf(path.toString()));
            ff.crash(dir);
            Assert.assertArrayEquals(expected, java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path.toString())));
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

    private static void createEmptyFile(CrashFaultFilesFacade ff, Path file) {
        final long fd = ff.openRW(file.$(), CairoConfiguration.O_NONE);
        Assert.assertTrue(fd > -1);
        ff.close(fd);
    }

    // Two directories, a and b. Each test links one pair of names from a to b and another from b to a, so in any order
    // the directory walk takes, crash() reaches one pair's old name last and the other pair's new name last.
    private String newHardLinkRoot(String folder) throws Exception {
        final File root = temp.newFolder(folder);
        Assert.assertTrue(new File(root, "a").mkdir());
        Assert.assertTrue(new File(root, "b").mkdir());
        return root.getAbsolutePath();
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

    private static void syncThrough(CrashFaultFilesFacade ff, SyncKind kind, Path file, byte value) {
        if (kind == SyncKind.MSYNC || kind == SyncKind.SYNC_FILE_RANGE) {
            final long fd = ff.openRW(file.$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            final long addr = ff.mmap(fd, 8, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
            Assert.assertNotEquals(-1L, addr);
            try {
                Unsafe.getUnsafe().setMemory(addr, 8, value);
                if (kind == SyncKind.MSYNC) {
                    ff.msync(addr, 8, false);
                } else {
                    // MS_ASYNC lets sync_file_range write the mapped pages back; neither commits the journal.
                    ff.msync(addr, 8, true);
                    ff.syncFileRange(fd, 0, 8, Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER);
                }
            } finally {
                ff.munmap(addr, 8, MemoryTag.MMAP_DEFAULT);
                ff.close(fd);
            }
            return;
        }
        final long fd = openAndWrite(ff, file, value);
        switch (kind) {
            case FDATASYNC -> {
                ff.fdatasync(fd);
                ff.close(fd);
            }
            case FSYNC_AND_CLOSE -> ff.fsyncAndClose(fd);
            default -> {
                ff.close(fd);
                ff.markFileDurable(file.toString());
            }
        }
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

    // The syncs, other than fsync, that snapshot a file's bytes through one name.
    private enum SyncKind {
        FDATASYNC, FSYNC_AND_CLOSE, MARK_FILE_DURABLE, MSYNC, SYNC_FILE_RANGE
    }
}
