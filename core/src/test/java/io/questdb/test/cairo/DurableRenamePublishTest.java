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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Publishing a file by renaming it into place is durable only once the rename ITSELF is durable. On POSIX
 * the publisher takes that barrier by fsyncing the parent directory afterwards; on Windows
 * {@code FlushFileBuffers} accepts file handles only, so {@code TableUtils.fsyncDirDurable} skips the
 * barrier there and the rename had none at all -- a power loss could keep the fsynced file contents and the
 * durable pointer naming them while losing the name itself, inverting data-before-pointer.
 *
 * <p>{@code FilesFacade.renameDurable} closes that: on Windows the move is asked to write through, off
 * Windows it is exactly {@code rename}. These tests assert the two halves that can regress silently --
 * that the primitive keeps {@code rename}'s observable semantics, and that the publish SITES actually ask
 * for it (a durable primitive nobody calls is the same as no primitive at all).
 */
public class DurableRenamePublishTest extends AbstractCairoTest {

    /**
     * The durable variant must be a drop-in for {@code rename}: same success, same bytes, same
     * destination-exists behaviour (POSIX replaces, Windows fails -- and TableWriter depends on the
     * Windows half, so adding MOVEFILE_REPLACE_EXISTING would be a silent behaviour change).
     */
    @Test
    public void testDurableRenameMatchesPlainRenameSemantics() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path src = new Path(); Path dst = new Path()) {
                src.of(root).concat("durable-rename-src");
                dst.of(root).concat("durable-rename-dst");

                writeByte(ff, src, (byte) 7);
                Assert.assertEquals(Files.FILES_RENAME_OK, ff.renameDurable(src.$(), dst.$()));
                Assert.assertFalse(ff.exists(src.$()));
                Assert.assertTrue(ff.exists(dst.$()));
                Assert.assertEquals(1, ff.length(dst.$()));

                // Destination present: whatever plain rename does here, the durable one must do too.
                writeByte(ff, src, (byte) 9);
                final int durableResult = ff.renameDurable(src.$(), dst.$());
                if (Os.isWindows()) {
                    Assert.assertNotEquals("Windows rename must still refuse an existing destination",
                            Files.FILES_RENAME_OK, durableResult);
                    Assert.assertTrue(ff.exists(src.$()));
                    Assert.assertTrue(ff.removeQuiet(src.$()));
                } else {
                    Assert.assertEquals(Files.FILES_RENAME_OK, durableResult);
                    Assert.assertFalse(ff.exists(src.$()));
                }
                Assert.assertTrue(ff.removeQuiet(dst.$()));
            }
        });
    }

    /**
     * {@code FilesFacadeImpl.barrierFsync} and {@code fsyncDurable} must route through the OVERRIDABLE
     * {@code fdatasync}/{@code fsync} on EVERY platform -- on Darwin via the per-thread upgrade flag,
     * since the durable variants are different fcntls there. A facade that intercepts the weak method
     * without doing IO must fully absorb the durable call, or the crash harness's per-syscall model goes
     * quiet on exactly the platform where the barrier syscall differs.
     */
    @Test
    public void testFaultInjectingFacadesStillInterceptTheDurableFsyncs() {
        final class CountingSyncFacade extends TestFilesFacadeImpl {
            int fdatasyncCalls;
            int fsyncCalls;

            @Override
            public void fdatasync(long fd) {
                fdatasyncCalls++; // no IO: the delegation is the whole assertion
            }

            @Override
            public void fsync(long fd) {
                fsyncCalls++; // no IO: the delegation is the whole assertion
            }
        }
        final CountingSyncFacade ff = new CountingSyncFacade();
        // -1 is not a valid fd: if the durable variant bypasses the facade and reaches the real
        // syscall, it throws instead of counting.
        ff.barrierFsync(-1);
        Assert.assertEquals(1, ff.fdatasyncCalls);
        Assert.assertEquals(0, ff.fsyncCalls);
        ff.fsyncDurable(-1);
        Assert.assertEquals(1, ff.fsyncCalls);
        Assert.assertEquals(1, ff.fdatasyncCalls);
    }

    /**
     * The interface default and {@code FilesFacadeImpl} both route through the OVERRIDABLE
     * {@code rename} on EVERY platform -- on Windows via the per-thread upgrade flag, since the durable
     * move is a different syscall there -- so every fault-injecting facade that intercepts renames keeps
     * intercepting these. Without that the harness goes quiet instead of red -- the failure mode
     * {@code FilesFacadeImpl}'s {@code barrierFsync}/{@code fsyncDurable} comments already call out.
     */
    @Test
    public void testFaultInjectingFacadesStillInterceptTheDurableRename() {
        final class CountingDefaultFacade extends TestFilesFacadeImpl {
            int renameCalls;

            @Override
            public int rename(LPSZ from, LPSZ to) {
                renameCalls++;
                return Files.FILES_RENAME_OK; // no IO: the delegation is the whole assertion
            }
        }
        final CountingDefaultFacade ff = new CountingDefaultFacade();
        try (Path src = new Path(); Path dst = new Path()) {
            src.of(root).concat("a");
            dst.of(root).concat("b");
            Assert.assertEquals(Files.FILES_RENAME_OK, ff.renameDurable(src.$(), dst.$()));
        }
        // One rename call on every platform: the facade absorbed the rename without IO, so the real
        // syscall -- write-through move on Windows, rename(2) elsewhere -- must never have run (the
        // paths do not exist; the real syscall would have returned an error, failing the assert above).
        Assert.assertEquals(1, ff.renameCalls);
    }

    /**
     * The metadata swap ({@code _meta -> _meta.prev}, {@code _meta.swp -> _meta}) is a publish, and its
     * companion table-dir fsync is the barrier Windows does not get. So under a durability-promising commit
     * mode both renames must be the durable variant.
     */
    @Test
    public void testMetadataSwapPublishesWithADurableRename() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        final RenameRecordingFacade ff = new RenameRecordingFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY");
            ff.clear();
            execute("ALTER TABLE t ADD COLUMN extra LONG");

            Assert.assertTrue("_meta -> _meta.prev must publish durably, got: " + ff.dump(),
                    ff.durableCount("_meta.prev") > 0);
            Assert.assertTrue("_meta.swp -> _meta must publish durably, got: " + ff.dump(),
                    ff.durableCount("_meta.swp") > 0);
            Assert.assertEquals("no metadata rename may take the barrier-less path, got: " + ff.dump(),
                    0, ff.plainCount("_meta"));
        });
    }

    /**
     * ... and NOSYNC must keep costing nothing: it takes no directory fsync either, so asking the move to
     * write through would be a barrier the mode explicitly disclaims.
     */
    @Test
    public void testNosyncMetadataSwapTakesNoRenameBarrier() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        final RenameRecordingFacade ff = new RenameRecordingFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY");
            ff.clear();
            execute("ALTER TABLE t ADD COLUMN extra LONG");

            Assert.assertTrue("the swap must still happen, got: " + ff.dump(), ff.plainCount("_meta") > 0);
            Assert.assertEquals("NOSYNC must take no rename barrier, got: " + ff.dump(),
                    0, ff.durableCount("_meta"));
        });
    }

    private static void writeByte(FilesFacade ff, Path path, byte value) {
        final long fd = ff.openRW(path.$(), 0);
        Assert.assertTrue(fd > -1);
        try {
            final long buf = io.questdb.std.Unsafe.malloc(1, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            try {
                io.questdb.std.Unsafe.getUnsafe().putByte(buf, value);
                Assert.assertEquals(1, ff.write(fd, buf, 1, 0));
            } finally {
                io.questdb.std.Unsafe.free(buf, 1, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            ff.close(fd);
        }
    }

    /**
     * Records which rename variant each path went through. Observes only -- both calls delegate, so the
     * system under test behaves exactly as in production.
     * <p>
     * Note it extends {@link FilesFacadeImpl} through {@link TestFilesFacadeImpl} and overrides BOTH
     * variants: on POSIX {@code renameDurable} delegates to {@code rename}, so recording only in
     * {@code rename} could not tell the two apart.
     */
    private static final class RenameRecordingFacade extends TestFilesFacadeImpl {
        private final List<String> durable = new ArrayList<>();
        private final List<String> plain = new ArrayList<>();

        void clear() {
            synchronized (this) {
                plain.clear();
                durable.clear();
            }
        }

        String dump() {
            synchronized (this) {
                return "\nplain=" + plain + "\ndurable=" + durable;
            }
        }

        synchronized int durableCount(String pathContains) {
            return count(durable, pathContains);
        }

        synchronized int plainCount(String pathContains) {
            return count(plain, pathContains);
        }

        @Override
        public int rename(LPSZ from, LPSZ to) {
            record(plain, from, to);
            return super.rename(from, to);
        }

        @Override
        public int renameDurable(LPSZ from, LPSZ to) {
            record(durable, from, to);
            if (Os.isWindows()) {
                return Files.renameDurable(from, to);
            }
            // Off Windows the durable rename IS rename(2); call super.rename, NOT super.renameDurable,
            // which would re-enter this class's rename override and double-count the same call as plain.
            return super.rename(from, to);
        }

        private static int count(List<String> log, String pathContains) {
            int n = 0;
            for (int i = 0, size = log.size(); i < size; i++) {
                if (log.get(i).contains(pathContains)) {
                    n++;
                }
            }
            return n;
        }

        private static String pathToString(LPSZ name) {
            // NOT name.toString(): an LPSZ renders as its identity hash, so every contains() lookup would
            // silently return 0 and the assertions would go quiet instead of red.
            final int n = name.size();
            final StringBuilder sb = new StringBuilder(n);
            for (int i = 0; i < n; i++) {
                sb.append((char) (name.byteAt(i) & 0xFF));
            }
            return sb.toString();
        }

        private void record(List<String> log, LPSZ from, LPSZ to) {
            final String entry = pathToString(from) + " -> " + pathToString(to);
            synchronized (this) {
                log.add(entry);
            }
        }
    }
}
