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

package io.questdb.test.cairo.crash;

import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Self-checks for the crash model. A file renamed while its handles are still open must keep its
 * durability accounting.
 * <p>
 * {@code fdToPath} and {@code mmapAddrToPath} are both populated when the handle is created, so without
 * re-keying them on rename every later {@code write}/{@code fsync}/{@code fdatasync}/{@code msync} would be
 * attributed to a path that no longer exists. The renamed file would then look never written to the model
 * and {@code crash()} would truncate it to zero, although its bytes are on disk and barriered.
 * <p>
 * {@code WalWriter.renameColumnFiles} renames a WAL column file with its column memory and fd still open, so
 * an ALTER RENAME COLUMN in a crash sweep depends on the re-keying. Without it the sweep reports a false
 * "WAL segment column too short for committed row range [... actual=0]" and a suspended table, which reads
 * like a product durability bug.
 */
public class CrashModelRenameDurabilityTest extends AbstractCairoTest {

    @Test
    public void testDurableBytesSurviveARenameWithTheHandleStillOpen() throws Exception {
        // POSIX-only, same gate as AbstractCrashConsistencyTest.assumeCrashHarnessSupported(): the test
        // fsyncs an open parent directory handle (Windows has none to open) and ends in crash(), which
        // truncates files -- the harness's POSIX-only half. The re-keying under test is covered by the
        // Linux and macOS legs.
        org.junit.Assume.assumeFalse("crash harness is POSIX-only", io.questdb.std.Os.isWindows());
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        assertMemoryLeak(ff, () -> {
            final String root = engine.getConfiguration().getDbRoot().toString();
            try (Path from = new Path(); Path to = new Path()) {
                from.of(root).concat("rn_before.d");
                to.of(root).concat("rn_after.d");

                // Write and barrier under the original name, keeping the fd open across the rename -- the
                // shape WalWriter uses.
                final long fd = ff.openRW(from.$(), io.questdb.cairo.CairoConfiguration.O_NONE);
                Assert.assertTrue("could not open the source file", fd > -1);
                try {
                    final long buf = io.questdb.std.Unsafe.malloc(128, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                    try {
                        io.questdb.std.Vect.memset(buf, 128, 1);
                        Assert.assertEquals(128, ff.write(fd, buf, 128, 0));
                        ff.fsync(fd);            // durable under the original name
                        ff.markDurableBaseline(root);

                        // More durable bytes after the rename, through the same still-open fd.
                        Assert.assertEquals(Files.FILES_RENAME_OK, ff.rename(from.$(), to.$()));
                        // A rename publishes a dentry in the parent, which needs its own barrier before
                        // the new name is durable, as WalWriter.renameColumnFiles does. Without it crash()
                        // restores the original name and the renamed file legitimately does not exist.
                        try (Path dir = new Path()) {
                            dir.of(root);
                            final long dirFd = ff.openRO(dir.$());
                            Assert.assertTrue("could not open the parent directory", dirFd > -1);
                            ff.fsync(dirFd);
                            ff.close(dirFd);
                        }
                        Assert.assertEquals(128, ff.write(fd, buf, 128, 128));
                        ff.fsync(fd);            // must be attributed to the new name
                    } finally {
                        io.questdb.std.Unsafe.free(buf, 128, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    ff.close(fd);
                }

                ff.crash(root);

                Assert.assertEquals(
                        "crash() truncated a renamed file whose bytes were written AND fsync'd through a "
                                + "handle held across the rename. fdToPath/mmapAddrToPath are keyed when the "
                                + "handle is created, so without re-keying them on rename the durability "
                                + "accounting lands on the old path and the live file looks never-written. "
                                + "This fabricates 'WAL segment column too short ... actual=0' failures that "
                                + "look exactly like a product durability bug.",
                        256, ff.length(to.$()));
            }
        });
    }

    /**
     * The force-close reclaim must name itself when it strands a live handle.
     * <p>
     * Otherwise, using an fd that {@code reclaimLingeringNonCacheFds} already closed surfaces only as
     * {@code "Invalid fd=..., not found in cache"} raised deep inside {@code FdCache}, from whatever
     * unrelated code touches the descriptor next, possibly thousands of crash points away from the cause.
     * The writer-release ordering in {@code AbstractAdaptiveCrashTest#releaseEngineHandles} that could strand
     * such a handle needs the sweep's multi-iteration context and is not cheaply unit-testable, so this test
     * makes the first misuse name the reclaim instead.
     */
    @Test
    public void testUsingAForceClosedFdIsAttributedToTheReclaim() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        assertMemoryLeak(ff, () -> {
            final String root = engine.getConfiguration().getDbRoot().toString();
            try (Path p = new Path()) {
                p.of(root).concat("fc_probe.d");
                final long fd = ff.openRW(p.$(), io.questdb.cairo.CairoConfiguration.O_NONE);
                Assert.assertTrue("could not open the probe file", fd > -1);
                ff.forceClose(fd);   // what reclaimLingeringNonCacheFds does to a stranded fd

                AssertionError seen = null;
                try {
                    ff.fsync(fd);    // a live owner's next barrier
                } catch (AssertionError e) {
                    seen = e;
                }
                Assert.assertNotNull("using a force-closed fd must fail LOUDLY and name the reclaim, not "
                        + "surface later as an opaque FdCache assertion", seen);
                Assert.assertTrue("the failure must name the reclaim as the closer, got: " + seen.getMessage(),
                        seen.getMessage() != null && seen.getMessage().contains("USE AFTER FORCE-CLOSE"));
            }
        });
    }
}
