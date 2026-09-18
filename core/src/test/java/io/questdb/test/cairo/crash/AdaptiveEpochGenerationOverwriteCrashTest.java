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
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A durable epoch generation is overwritten IN PLACE while its own marker record is still a live recovery
 * candidate. This proves that a crash part-way through that overwrite cannot be adopted.
 * <p>
 * The generations ping-pong, so the cut after next targets the generation the PREVIOUS marker slot still
 * names, and {@code fsyncMaterializedState} rewrites its {@code _meta}/{@code _cv}/{@code _txn} payloads one
 * file at a time. {@link io.questdb.cairo.SnapshotMarker#loadCandidates()} returns BOTH slots, so that
 * generation remains selectable throughout. A crash between the payload writes therefore leaves it holding a
 * MIXTURE of two different cuts, and the marker record naming it is the older one -- which still matches the
 * older {@code _txn} that survived, so the seqTxn/txn identity checks all pass.
 * <p>
 * The window is nevertheless CLOSED, by THREE independent checks in {@code epochCopiesValid}. Measured by
 * disabling them one at a time:
 * <ol>
 *   <li>the {@code _txn.epoch} vs {@code _meta.epoch} metadataVersion comparison -- the one that actually
 *       fires. It is exact for this case: a mixture only differs from a consistent cut when {@code _meta}
 *       changed between the two cuts, and any such change bumps metadataVersion;</li>
 *   <li>with that disabled, the symbol-column cross-check catches it
 *       ("disagree on symbol columns [txnSymbolColumns=1, metaSymbolColumns=2]");</li>
 *   <li>the manifest is written AFTER the payloads, so a half-overwritten generation still carries the one
 *       from two cuts ago, describing the files being replaced.</li>
 * </ol>
 * This is why arming a "retire the marker slot before overwriting its payloads" guard measurably changed
 * NOTHING when it was tried: it closes a window that is already shut three times over. It was not shipped.
 * <p>
 * The test pins the OUTCOME, not any one of those mechanisms: with both generations unusable, recovery must
 * refuse to open rather than adopt the mixture or fall back to live state. It therefore stays green if one
 * layer is removed and goes red only if the last one is -- which is the correct sensitivity for a
 * defence-in-depth property.
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

                // Cut 3 targets generation A again. Die after its _meta.epoch has been rewritten but before
                // its _cv/_txn are -- exactly a crash inside the in-place overwrite.
                ff.failOn("_cv" + TableUtils.EPOCH_COPY_SUFFIX);
                try (TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(3L);
                } catch (Throwable expected) {
                    // best-effort cut, dies at the injected copy failure
                } finally {
                    ff.disarm();
                }
                Assert.assertTrue("the injected copy failure never fired, so no mixture was created and this "
                        + "test proves nothing", ff.failures > 0);
                Assert.assertEquals("the marker must still select the intact generation; the failed cut "
                        + "publishes nothing", genB, activeGeneration(tt));

                // Force recovery to CONSIDER the half-overwritten generation by making the intact one
                // unusable. Without this the fallback path is never entered.
                corruptTxnEpoch(tt, genB);

                engine.releaseAllWriters();
                engine.releaseAllReaders();

                // With BOTH generations unusable, recovery must FAIL CLOSED rather than fall back to live
                // state or adopt the mixture. Reaching this exception is the proof that the
                // half-overwritten generation was refused: had it been accepted, recover() would have
                // returned normally and restored it.
                try {
                    new RecoveryCoordinator(engine).recover();
                    Assert.fail("recovery ACCEPTED a generation that a later cut had half-overwritten: its "
                            + "_meta came from the new cut while its _cv/_txn are the previous one's. The "
                            + "marker cannot catch this -- the record naming that generation is the OLD one "
                            + "and matches the OLD _txn that survived, so every seqTxn/txn identity check "
                            + "passes. Only the manifest can refuse it, because it is written AFTER the "
                            + "payloads and therefore still describes the files being replaced.");
                } catch (io.questdb.cairo.CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "no trustworthy adaptive epoch generation");
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    private int activeGeneration(TableToken tt) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
            final io.questdb.cairo.SnapshotMarker marker =
                    new io.questdb.cairo.SnapshotMarker(engine.getConfiguration());
            try {
                marker.of(p.$());
                Assert.assertTrue("the epoch marker must load", marker.tryLoad());
                return marker.getGeneration();
            } finally {
                marker.close();
            }
        }
    }

    private void corruptTxnEpoch(TableToken tt, int generation) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt)
                    .concat(TableUtils.TXN_FILE_NAME).put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(generation);
            final io.questdb.std.FilesFacade ff = engine.getConfiguration().getFilesFacade();
            final long fd = ff.openRW(p.$(), engine.getConfiguration().getWriterFileOpenOpts());
            Assert.assertTrue("must open the intact generation's _txn.epoch", fd > 0);
            try {
                Assert.assertTrue("truncate it so this generation is rejected", ff.truncate(fd, 0));
            } finally {
                ff.close(fd);
            }
        }
    }

    private static final class FailCopyFacade extends TestFilesFacadeImpl {
        int failures;
        private String failSubstring;

        void disarm() {
            failSubstring = null;
        }

        void failOn(String substring) {
            failSubstring = substring;
            failures = 0;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            if (failSubstring != null) {
                final String path = io.questdb.std.str.Utf8String.newInstance(name).toString();
                final int nul = path.indexOf('\0');
                if ((nul > -1 ? path.substring(0, nul) : path).contains(failSubstring)) {
                    failures++;
                    return -1; // ENOENT-ish: the copy cannot be created, the cut aborts here
                }
            }
            return super.openRW(name, opts);
        }
    }
}
