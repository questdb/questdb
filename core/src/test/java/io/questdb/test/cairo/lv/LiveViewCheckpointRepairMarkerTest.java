/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointRepairMarker;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Direct coverage for the durable prefix-preservation repair marker: its
 * durable write/read round trip, the base-generation staleness signal a restart
 * uses to tell a live repair from a marker a crash left behind after a
 * successful seal, and the conservative {@link Numbers#LONG_NULL} a torn or
 * absent marker reports.
 */
public class LiveViewCheckpointRepairMarkerTest extends AbstractCairoTest {

    private static final String LV_DIR = "lv_marker";

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            configuration.getFilesFacade().mkdirs(checkpointsDir(path).slash(), configuration.getMkDirMode());
        }
    }

    @Test
    public void testClearRemovesMarker() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path dir = new Path()) {
                checkpointsDir(dir);
                LiveViewCheckpointRepairMarker.write(configuration, dir, 1, 0, 7, 1_000);
                Assert.assertTrue(LiveViewCheckpointRepairMarker.exists(ff, dir));

                LiveViewCheckpointRepairMarker.clear(ff, dir);
                Assert.assertFalse(LiveViewCheckpointRepairMarker.exists(ff, dir));
                Assert.assertEquals(Numbers.LONG_NULL, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
                // Clearing an absent marker is a no-op.
                LiveViewCheckpointRepairMarker.clear(ff, dir);
                Assert.assertFalse(LiveViewCheckpointRepairMarker.exists(ff, dir));
            }
        });
    }

    @Test
    public void testFailedRewriteLeavesThePreviousMarkerReadable() throws Exception {
        // A rename that fails for any reason other than an existing destination
        // must leave the record already published under the final name alone.
        final AtomicBoolean rejectRename = new AtomicBoolean();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public int errno() {
                return rejectRename.get() ? CairoException.ERRNO_EACCES_LINUX : super.errno();
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                return rejectRename.get() ? Files.FILES_RENAME_ERR_OTHER : super.rename(from, to);
            }
        }, () -> {
            try (Path dir = new Path()) {
                checkpointsDir(dir);
                LiveViewCheckpointRepairMarker.write(configuration, dir, 11, 2, 8, 1_700_000_000L);
                Assert.assertEquals(8, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));

                rejectRename.set(true);
                try {
                    LiveViewCheckpointRepairMarker.write(configuration, dir, 11, 2, 20, 1_700_000_000L);
                    Assert.fail("expected the marker publication to fail");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not publish live view checkpoint repair marker");
                }
                rejectRename.set(false);
                Assert.assertTrue(LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
                Assert.assertEquals(8, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
            }
        });
    }

    @Test
    public void testMissingMarkerReadsNull() throws Exception {
        assertMemoryLeak(() -> {
            try (Path dir = new Path()) {
                checkpointsDir(dir);
                Assert.assertFalse(LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
                Assert.assertEquals(Numbers.LONG_NULL, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
            }
        });
    }

    @Test
    public void testRewriteFailureAfterUnlinkLeavesNoMarker() throws Exception {
        // The Windows rewrite unlinks the previous record before retrying the
        // rename, so a retry that then fails leaves neither the record nor the
        // staged sibling: write() removes the .tmp and throws. That empty state is
        // safe because the throw aborts the repair before any truncate, so there
        // is nothing for a restart to guard. The caller reports the second
        // rename's error, not the collision that triggered the retry.
        final AtomicBoolean failRetryRename = new AtomicBoolean();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            private int injectedErrno;

            @Override
            public int errno() {
                return injectedErrno != 0 ? injectedErrno : super.errno();
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (exists(to)) {
                    injectedErrno = CairoException.ERRNO_ALREADY_EXISTS_WIN;
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                if (failRetryRename.get()) {
                    injectedErrno = CairoException.ERRNO_EACCES_LINUX;
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                injectedErrno = 0;
                return super.rename(from, to);
            }
        }, () -> {
            try (Path dir = new Path()) {
                checkpointsDir(dir);
                LiveViewCheckpointRepairMarker.write(configuration, dir, 11, 2, 8, 1_700_000_000L);
                Assert.assertEquals(8, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));

                failRetryRename.set(true);
                try {
                    LiveViewCheckpointRepairMarker.write(configuration, dir, 11, 2, 20, 1_700_000_000L);
                    Assert.fail("expected the marker publication to fail");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not publish live view checkpoint repair marker");
                    Assert.assertEquals(CairoException.ERRNO_EACCES_LINUX, e.getErrno());
                }
                failRetryRename.set(false);

                Assert.assertFalse(LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
                Assert.assertEquals(Numbers.LONG_NULL, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));

                // With the fault gone the next repair publishes normally.
                LiveViewCheckpointRepairMarker.write(configuration, dir, 11, 2, 20, 1_700_000_000L);
                Assert.assertEquals(20, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
            }
        });
    }

    @Test
    public void testRewriteKeepsPreviousRecordWhenDestinationCannotBeCleared() throws Exception {
        // A collision retry that cannot unlink the previous record fails its
        // second rename for the same reason as the first, and that is the error
        // the caller reports. The record already published under the final name
        // stays fully readable, and write() removes the staged sibling.
        final AtomicBoolean rejectMarkerRemoval = new AtomicBoolean();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            private boolean renameRejected;

            @Override
            public int errno() {
                return renameRejected ? CairoException.ERRNO_ALREADY_EXISTS_WIN : super.errno();
            }

            @Override
            public boolean removeQuiet(LPSZ name) {
                if (rejectMarkerRemoval.get() && !Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.TMP_SUFFIX)) {
                    return false;
                }
                return super.removeQuiet(name);
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (exists(to)) {
                    renameRejected = true;
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                renameRejected = false;
                return super.rename(from, to);
            }
        }, () -> {
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path dir = new Path(); Path path = new Path()) {
                checkpointsDir(dir);
                LiveViewCheckpointRepairMarker.write(configuration, dir, 11, 2, 8, 1_700_000_000L);
                Assert.assertEquals(8, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));

                rejectMarkerRemoval.set(true);
                try {
                    LiveViewCheckpointRepairMarker.write(configuration, dir, 11, 2, 20, 1_700_000_000L);
                    Assert.fail("expected the marker publication to fail");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not publish live view checkpoint repair marker");
                    Assert.assertEquals(CairoException.ERRNO_ALREADY_EXISTS_WIN, e.getErrno());
                }
                rejectMarkerRemoval.set(false);

                // The previous record survives in full and the staged sibling is gone.
                Assert.assertEquals(8, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
                LiveViewCheckpointLayout.repairingMarkerPath(path, dir).put(LiveViewCheckpointLayout.TMP_SUFFIX);
                Assert.assertFalse(ff.exists(path.$()));

                // With the fault gone the rewrite replaces the record normally.
                LiveViewCheckpointRepairMarker.write(configuration, dir, 11, 2, 20, 1_700_000_000L);
                Assert.assertEquals(20, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
            }
        });
    }

    @Test
    public void testRewriteReplacesRecordWhenRenameRejectsAnExistingTarget() throws Exception {
        // Windows MoveFileW refuses an existing destination and reports
        // ERROR_ALREADY_EXISTS, where POSIX rename replaces it atomically. This
        // facade holds every platform to the Windows contract, so the
        // publication has to unlink and retry.
        assertMemoryLeak(new TestFilesFacadeImpl() {
            private boolean renameRejected;

            @Override
            public int errno() {
                return renameRejected ? CairoException.ERRNO_ALREADY_EXISTS_WIN : super.errno();
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (exists(to)) {
                    renameRejected = true;
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                renameRejected = false;
                return super.rename(from, to);
            }
        }, () -> {
            try (Path dir = new Path()) {
                checkpointsDir(dir);
                LiveViewCheckpointRepairMarker.write(configuration, dir, 11, 2, 8, 1_700_000_000L);
                Assert.assertEquals(8, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
                // A second repair rewrites the fixed-name marker in place.
                LiveViewCheckpointRepairMarker.write(configuration, dir, 11, 2, 20, 1_700_000_000L);
                Assert.assertEquals(20, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
            }
        });
    }

    @Test
    public void testStagedSiblingAloneStillReadsAsALiveMarker() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path dir = new Path(); Path path = new Path()) {
                checkpointsDir(dir);
                LiveViewCheckpointRepairMarker.write(configuration, dir, 11, 2, 8, 1_700_000_000L);
                // The shape a crash inside the Windows rewrite leaves: the previous
                // record already unlinked, the replacement not yet renamed over it.
                LiveViewCheckpointLayout.repairingMarkerPath(path, dir);
                path.put(LiveViewCheckpointLayout.TMP_SUFFIX);
                Assert.assertTrue(ff.touch(path.$()));
                LiveViewCheckpointLayout.repairingMarkerPath(path, dir);
                Assert.assertTrue(ff.removeQuiet(path.$()));

                // Losing the final name must not read as "no repair in flight":
                // LONG_NULL is what forces the conservative rebuild.
                Assert.assertTrue(LiveViewCheckpointRepairMarker.exists(ff, dir));
                Assert.assertEquals(Numbers.LONG_NULL, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));

                // And it costs one rebuild, not one per restart: clear removes both.
                LiveViewCheckpointRepairMarker.clear(ff, dir);
                Assert.assertFalse(LiveViewCheckpointRepairMarker.exists(ff, dir));
            }
        });
    }

    @Test
    public void testStalenessBoundary() throws Exception {
        assertMemoryLeak(() -> {
            try (Path dir = new Path()) {
                checkpointsDir(dir);
                // A repair that started at generation 5 truncates at 6 and seals at
                // 7, so a live restart observes generation <= 6 and a stale one > 6.
                LiveViewCheckpointRepairMarker.write(configuration, dir, 3, 1, 5, 42);
                final long base = LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir);
                Assert.assertEquals(5, base);
                // generation 5 (marker written, truncate not yet published) -> live
                Assert.assertFalse(5 > base + 1);
                // generation 6 (truncate published, seal not yet) -> live
                Assert.assertFalse(6 > base + 1);
                // generation 7 (seal published) -> stale, repair completed
                Assert.assertTrue(7 > base + 1);
            }
        });
    }

    @Test
    public void testTornMarkerReadsNull() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path dir = new Path(); Path markerPath = new Path()) {
                checkpointsDir(dir);
                LiveViewCheckpointRepairMarker.write(configuration, dir, 1, 0, 9, 100);
                Assert.assertEquals(9, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));

                // Corrupt the base-generation field: the CRC no longer matches, so a
                // restart cannot trust the value and must force a rebuild.
                LiveViewCheckpointLayout.repairingMarkerPath(markerPath, dir);
                final long fd = ff.openRW(markerPath.$(), configuration.getWriterFileOpenOpts());
                Assert.assertTrue(fd > 0);
                final long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putLong(buf, 0xDEAD_BEEFL);
                    ff.write(fd, buf, Long.BYTES, LiveViewCheckpointRepairMarker.BASE_GENERATION_OFFSET);
                } finally {
                    ff.close(fd);
                    Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
                Assert.assertTrue(LiveViewCheckpointRepairMarker.exists(ff, dir));
                Assert.assertEquals(Numbers.LONG_NULL, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
            }
        });
    }

    @Test
    public void testWriteReadRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            try (Path dir = new Path()) {
                checkpointsDir(dir);
                LiveViewCheckpointRepairMarker.write(configuration, dir, 11, 2, 8, 1_700_000_000L);
                Assert.assertTrue(LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
                Assert.assertEquals(8, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
                // A rewrite replaces the record in place.
                LiveViewCheckpointRepairMarker.write(configuration, dir, 11, 2, 20, 1_700_000_000L);
                Assert.assertEquals(20, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
            }
        });
    }

    private static Path checkpointsDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }
}
