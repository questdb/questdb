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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.crash.CrashFaultFilesFacade;
import io.questdb.test.std.SyncAttributingFilesFacade;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class WalChecksumSidecarSyncTest extends AbstractCairoTest {
    private final String commitMode;
    private final long groupWindowUs;
    private final int sequencerPartTxnCount;

    public WalChecksumSidecarSyncTest(String commitMode, long groupWindowUs, int sequencerPartTxnCount) {
        this.commitMode = commitMode;
        this.groupWindowUs = groupWindowUs;
        this.sequencerPartTxnCount = sequencerPartTxnCount;
    }

    @Parameterized.Parameters(name = "mode={0}, window={1}, partTxns={2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"nosync", 0L, 0}, {"nosync", 0L, 16},
                {"async", 0L, 0}, {"async", 0L, 16},
                {"sync", 0L, 0}, {"sync", 0L, 16},
                {"adaptive", 0L, 0}, {"adaptive", 0L, 16},
                {"adaptive", 50_000L, 0}, {"adaptive", 50_000L, 16}
        });
    }

    @Test
    public void testCommitSyncGrades() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, groupWindowUs);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, sequencerPartTxnCount);
        // Exercise one msync per file, independently of the optional adaptive writeback drain.
        node1.setProperty(PropertyKey.CAIRO_WAL_COMMIT_WRITEBACK_DRAIN, false);

        final SyncAttributingFilesFacade ff = new SyncAttributingFilesFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 0)");
            final boolean isNoSync = "nosync".equals(commitMode);
            final boolean isAdaptive = "adaptive".equals(commitMode);
            final boolean isSync = "sync".equals(commitMode) || isAdaptive && groupWindowUs == 0;
            final boolean isSidecarSync = isAdaptive && groupWindowUs == 0;
            final int syncCount = isNoSync ? 0 : 1;
            final int seqSidecarCount = sequencerPartTxnCount == 0 ? syncCount : 0;

            for (int i = 1; i <= 3; i++) {
                ff.clearCounters();
                execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', " + i + ")");

                assertSyncGrade(ff, "/_event.c", syncCount, isSidecarSync);
                assertSyncGrade(ff, "/_txnlog.c", seqSidecarCount, isSidecarSync);
                assertSyncGrade(ff, "/_event.i", syncCount, isSync);
                assertSyncGrade(ff, "/ts.d", syncCount, isSync);
                assertSyncGrade(ff, "/v.d", syncCount, isSync);
                // The facade matches substrings: exclude the sidecars/index to isolate the base files.
                Assert.assertEquals(syncCount, ff.msyncCount("/_event")
                        - ff.msyncCount("/_event.c") - ff.msyncCount("/_event.i"));
                Assert.assertEquals(isSync ? syncCount : 0, ff.msyncCount("/_event", false)
                        - ff.msyncCount("/_event.c", false) - ff.msyncCount("/_event.i", false));
                Assert.assertEquals(syncCount, ff.msyncCount("/_txnlog") - ff.msyncCount("/_txnlog.c"));
                Assert.assertEquals(isSync ? syncCount : 0,
                        ff.msyncCount("/_txnlog", false) - ff.msyncCount("/_txnlog.c", false));
                assertSyncGrade(ff, "/_txn_parts/", sequencerPartTxnCount == 0 ? 0 : syncCount, isSync);
                Assert.assertEquals(isAdaptive ? 1 : 0, ff.fsyncCount("/_event.c"));
                if (!isAdaptive) {
                    Assert.assertEquals(0, ff.fsyncCount("/_txnlog.c"));
                } else if (groupWindowUs == 0 && sequencerPartTxnCount == 0) {
                    Assert.assertTrue(ff.fsyncCount("/_txnlog.c") > 0);
                }
            }
            drainWalQueue();
            assertQuery("SELECT count(), sum(v) FROM x").noRandomAccess().expectSize().returns("count\tsum\n4\t6\n");
        });
    }

    @Test
    public void testRewrittenEventSurvivesCrash() throws Exception {
        Assume.assumeTrue("sync".equals(commitMode));
        Assume.assumeFalse(Os.isWindows());
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, sequencerPartTxnCount);
        final ChecksumWritebackFacade ff = new ChecksumWritebackFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("x");
            try (WalWriter writer = getWalWriter("x")) {
                TableWriter.Row row = writer.newRow(0);
                row.putLong(1, 1);
                row.append();
                writer.commit();
                ff.markDurableBaseline(root);

                row = writer.newRow(1);
                row.putLong(1, 2);
                row.append();
                execute("ALTER TABLE x ADD COLUMN s SYMBOL");
                ff.sidecarPath = Path.of(root, token.getDirName(), "wal1", "1", WalUtils.EVENT_CHECKSUM_FILE_NAME);
                writer.commit();
                Assert.assertTrue("must persist the pre-rewrite checksum", ff.isPersisted);
            }
            engine.releaseAllWalWriters();
            engine.releaseAllWriters();
            // Model loss of this sidecar's pending writeback only; the other files have landed.
            ff.crash(ff.sidecarPath.toString());
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT v, s FROM x ORDER BY v").expectSize().returns("v\ts\n1\t\n2\t\n");
        });
    }

    private static void assertSyncGrade(SyncAttributingFilesFacade ff, String path, int count, boolean isSync) {
        Assert.assertEquals(path + " total msyncs" + ff.debugDump(), count, ff.msyncCount(path));
        Assert.assertEquals(path + " synchronous msyncs", isSync ? count : 0, ff.msyncCount(path, false));
        Assert.assertEquals(path + " asynchronous msyncs", isSync ? 0 : count, ff.msyncCount(path, true));
    }

    private static class ChecksumWritebackFacade extends CrashFaultFilesFacade {
        private boolean isPersisted;
        private Path sidecarPath;

        @Override
        public void msync(long addr, long len, boolean async) {
            super.msync(addr, len, async);
            if (!isPersisted && sidecarPath != null && Files.exists(sidecarPath)) {
                try {
                    final byte[] bytes = Files.readAllBytes(sidecarPath);
                    if (bytes.length >= WalUtils.WALE_CHECKSUM_HEADER_SIZE + WalUtils.WALE_CHECKSUM_ENTRY_SIZE
                            && ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt(
                            WalUtils.WALE_CHECKSUM_HEADER_SIZE + WalUtils.WALE_CHECKSUM_ENTRY_LENGTH_OFFSET) > 0) {
                        // Independent OS writeback can land the sealed entry before ADD SYMBOL rewrites it.
                        markFileDurable(sidecarPath.toString());
                        isPersisted = true;
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }
}
