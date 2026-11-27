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

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.std.LongHashSet;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A durable epoch generation is overwritten IN PLACE while it is still the older valid save point. This proves
 * that a crash part-way through that overwrite cannot be adopted.
 * <p>
 * The generations ping-pong, so the cut after next targets the older generation. Its file is truncated, the new
 * sections are written and synced, and only then the header. A crash before the header lands leaves the new
 * sections behind no valid header, so the generation is never selected.
 * <p>
 * The test pins the OUTCOME: with both generations unusable, recovery must refuse to open rather than adopt
 * the half-written generation or fall back to live state.
 */
public class AdaptiveEpochGenerationOverwriteCrashTest extends AbstractCairoTest {

    private static final int K = 4;
    private static final int M = 4;

    @Test
    public void testHalfOverwrittenGenerationIsNeverAdopted() throws Exception {
        final FailCopyFacade ff = new FailCopyFacade();
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1); // drive every cut explicitly
        try {
            assertMemoryLeak(ff, () -> {
                Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());

                execute("create table t (ts timestamp, sym symbol, v long) timestamp(ts) partition by day wal");
                for (int i = 0; i < K; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', 's" + i + "', " + i + ")");
                }
                drainWalQueue();
                final TableToken tt = engine.verifyTableName("t");

                // Cut 1 -> generation A.
                try (TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(1L);
                }
                final int genA = activeGeneration(tt);

                // A structural change, so generation A's OLD _meta and the one the next-but-one cut would
                // write into it genuinely differ. Without this the "mixture" is indistinguishable from a
                // consistent cut and the test would prove nothing.
                execute("alter table t add column sym2 symbol");
                for (int i = K; i < K + M; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', 's" + i + "', " + i + ", 'x')");
                }
                drainWalQueue();

                // Cut 2 -> generation B, which becomes the active candidate.
                try (TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(2L);
                }
                final int genB = activeGeneration(tt);
                Assert.assertNotEquals("the two cuts must land in different generations", genA, genB);

                // Cut 3 targets generation A again. Die after its sections have been rewritten but before its
                // header is -- exactly a crash inside the in-place overwrite.
                ff.failHeaderWrite();
                try (TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(3L);
                } catch (Throwable expected) {
                    // best-effort cut, dies at the injected copy failure
                } finally {
                    ff.disarm();
                }
                Assert.assertTrue("the injected header failure never fired, so no half-written generation was "
                        + "created and this test proves nothing", ff.failures > 0);
                Assert.assertEquals("the intact generation must still be current; the failed cut "
                        + "publishes nothing", genB, activeGeneration(tt));

                // Force recovery to CONSIDER the half-overwritten generation by damaging the intact one's
                // sections. Without this the fallback path is never entered.
                corruptGeneration(tt, genB);

                engine.releaseAllWriters();
                engine.releaseAllReaders();

                // With BOTH generations unusable, recovery must FAIL CLOSED rather than fall back to live
                // state or adopt the mixture. Reaching this exception is the proof that the
                // half-overwritten generation was refused: had it been accepted, recover() would have
                // returned normally and restored it.
                try {
                    new RecoveryCoordinator(engine).recover();
                    Assert.fail("recovery ACCEPTED a generation that a later cut had half-overwritten: its "
                            + "sections came from the new cut while no header for them landed.");
                } catch (io.questdb.cairo.CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "no trustworthy adaptive epoch generation");
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    @Test
    public void testSavePointAfterFallbackSparesTheRecoveredGeneration() throws Exception {
        // Two generations of one cut, generation 0 listed first; generation 0 is damaged, so a start falls back
        // to generation 1, the only intact one. The next save point must write into generation 0: a crash inside
        // a write into generation 1 would leave no trustworthy generation, from a state the design calls
        // supported. Reads from the recovered table are not under test here, only which file the write hits.
        final FailCopyFacade ff = new FailCopyFacade();
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            assertMemoryLeak(ff, () -> {
                execute("create table t (ts timestamp, sym symbol, v long) timestamp(ts) partition by day wal");
                for (int i = 0; i < K; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', 's" + i + "', " + i + ")");
                }
                drainWalQueue();
                final TableToken tt = engine.verifyTableName("t");
                try (TableWriter w = getWriter(tt)) {
                    // Generation 1, then generation 0 at the same cut, as the idle save point does.
                    w.advanceDurableEpoch(1L);
                    w.advanceDurableEpoch(2L);
                }
                Assert.assertEquals("on a tie generation 0 is listed first", 0, activeGeneration(tt));
                corruptGeneration(tt, 0);
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                new RecoveryCoordinator(engine).recover();

                // The next save point dies at its header write: the generation it targets is left unpublished.
                ff.failHeaderWrite();
                try (TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(3L);
                } catch (Throwable expected) {
                    // best-effort cut, dies at the injected failure
                } finally {
                    ff.disarm();
                }
                Assert.assertTrue("the injected header failure never fired, so this test proves nothing", ff.failures > 0);
                engine.releaseAllWriters();
                engine.releaseAllReaders();

                // The write went into the damaged generation, so the recovered one is still there: the start succeeds.
                new RecoveryCoordinator(engine).recover();
                Assert.assertEquals("the start must stand on the generation the fallback selected", 1, activeGeneration(tt));
                assertQuery("SELECT count() FROM t").noRandomAccess().expectSize().returns("count\n" + K + "\n");
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    private int activeGeneration(TableToken tt) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt);
            final DurableEpochManifest.Candidate[] saved = DurableEpochManifest.loadCandidates(
                    engine.getConfiguration().getFilesFacade(), p, p.size(), tt.getTableId());
            Assert.assertTrue("a save point must exist", saved.length > 0);
            return saved[0].generation;
        }
    }

    private void corruptGeneration(TableToken tt, int generation) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt)
                    .concat(DurableEpochManifest.FILE_NAME).put('.').put(generation);
            final io.questdb.std.FilesFacade ff = engine.getConfiguration().getFilesFacade();
            final long fd = ff.openRW(p.$(), engine.getConfiguration().getWriterFileOpenOpts());
            Assert.assertTrue("must open the intact generation's save point file", fd > 0);
            final long buf = io.questdb.std.Unsafe.malloc(1, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            try {
                // Flip the first byte of the _txn section: the header stays valid, the section checksum fails.
                Assert.assertEquals(1, ff.read(fd, buf, 1, DurableEpochManifest.FILE_SIZE));
                io.questdb.std.Unsafe.getUnsafe().putByte(buf, (byte) ~io.questdb.std.Unsafe.getUnsafe().getByte(buf));
                Assert.assertEquals(1, ff.write(fd, buf, 1, DurableEpochManifest.FILE_SIZE));
            } finally {
                io.questdb.std.Unsafe.free(buf, 1, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                ff.close(fd);
            }
        }
    }

    private static final class FailCopyFacade extends TestFilesFacadeImpl {
        private final LongHashSet manifestFds = new LongHashSet();
        int failures;
        private boolean isArmed;

        @Override
        public boolean close(long fd) {
            manifestFds.remove(fd);
            return super.close(fd);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (fd > -1 && Utf8s.containsAscii(name, DurableEpochManifest.FILE_NAME)) {
                manifestFds.add(fd);
            }
            return fd;
        }

        @Override
        public long write(long fd, long address, long len, long offset) {
            if (isArmed && offset == 0 && len == DurableEpochManifest.FILE_SIZE && manifestFds.contains(fd)) {
                failures++;
                return -1; // the header never lands; the cut aborts here
            }
            return super.write(fd, address, len, offset);
        }

        void disarm() {
            isArmed = false;
        }

        void failHeaderWrite() {
            isArmed = true;
            failures = 0;
        }
    }
}
