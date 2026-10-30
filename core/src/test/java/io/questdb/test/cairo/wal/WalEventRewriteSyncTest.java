/*+*****************************************************************************
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
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.str.LPSZ;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.SyncAttributingFilesFacade;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The barriers a commit pays on its WAL event files when a concurrent ALTER TABLE ADD COLUMN is replayed into it
 * (the NO_TXN path), and on every commit after that.
 * <ul>
 *   <li>ADD COLUMN SYMBOL rewrites the pending record in place. SYNC and ADAPTIVE give {@code _event} and
 *   {@code _event.i} a device barrier before the txn is sequenced, so a power loss never exposes a mix of
 *   original and rewritten files. ASYNC and NOSYNC make no call: an async sync is a no-op.</li>
 *   <li>A roll that moves the pending rows into a new segment re-creates the record there, and ADAPTIVE gives
 *   it the same pre-sequencing barrier.</li>
 *   <li>Nothing else is paid: a column that is not rewritten costs nothing, and the commits after the race pay
 *   exactly the per-commit grade of their mode.</li>
 * </ul>
 */
@RunWith(Parameterized.class)
public class WalEventRewriteSyncTest extends AbstractCairoTest {
    private static final String[] EVENT_FILES = {
            WalUtils.EVENT_FILE_NAME,
            WalUtils.EVENT_INDEX_FILE_NAME
    };
    private final String columnType;
    private final String commitMode;
    private final long groupWindowUs;
    private final boolean isOffLinux;
    private final boolean isRoll;

    public WalEventRewriteSyncTest(String commitMode, long groupWindowUs, boolean isRoll, String columnType, boolean isOffLinux) {
        this.commitMode = commitMode;
        this.groupWindowUs = groupWindowUs;
        this.isRoll = isRoll;
        this.columnType = columnType;
        this.isOffLinux = isOffLinux;
    }

    @Parameterized.Parameters(name = "mode={0}, window={1}, roll={2}, column={3}, offLinux={4}")
    public static Collection<Object[]> data() {
        final Object[][] modes = {
                {"nosync", 0L},
                {"async", 0L},
                {"sync", 0L},
                {"adaptive", 0L},
                {"adaptive", 50_000L}
        };
        final List<Object[]> params = new ArrayList<>();
        for (Object[] mode : modes) {
            for (boolean isRoll : new boolean[]{true, false}) {
                for (String columnType : new String[]{"SYMBOL", "INT"}) {
                    for (boolean isOffLinux : new boolean[]{false, true}) {
                        params.add(new Object[]{mode[0], mode[1], isRoll, columnType, isOffLinux});
                    }
                }
            }
        }
        return params;
    }

    @Test
    public void testEventBarriers() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, groupWindowUs);
        // One msync per file, independently of the optional adaptive writeback drain.
        node1.setProperty(PropertyKey.CAIRO_WAL_COMMIT_WRITEBACK_DRAIN, false);
        final BarrierFacade ff = new BarrierFacade();
        if (isOffLinux) {
            ff.isSyncfsFileSystemWide = false;
        }
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("x");
            final String segment = isRoll ? "1" : "0";
            final String segmentMarker = token.getDirName() + "/" + WalUtils.WAL_NAME_BASE + 1 + "/" + segment + "/";
            final Path eventFile = Paths.get(root, token.getDirName(), WalUtils.WAL_NAME_BASE + 1, segment, WalUtils.EVENT_FILE_NAME);
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            ff.segmentMarker = segmentMarker;
            ff.eventFile = eventFile;
            ff.tracker = tracker;
            final boolean isRewritten = "SYMBOL".equals(columnType);
            final boolean isAdaptive = "adaptive".equals(commitMode);
            // A flushed rewrite has an msync of its own only where the file flush does not write back the mapped
            // pages, off Linux (MemoryMA.flushFile). An async sync makes no msync call, so an ASYNC rewrite has none.
            final boolean isFlushedRewrite = isRewritten && ("sync".equals(commitMode) || isAdaptive);
            final boolean isMsyncedRewrite = isFlushedRewrite && !ff.isSyncfsFileSystemWide();
            final long seqTxn = isRoll ? 3 : 2;
            try (WalWriter writer = getWalWriter("x")) {
                if (isRoll) {
                    appendRow(writer, 0, 1);
                    writer.commit();
                }
                appendRow(writer, 1, 2);
                execute("ALTER TABLE x ADD COLUMN c " + columnType);

                ff.clearCounters();
                ff.isArmed = true;
                writer.commit();
                ff.isArmed = false;
                Assert.assertEquals(seqTxn, tracker.getSeqTxn());
                Assert.assertEquals(isRoll ? 1 : 0, writer.getSegmentId());

                // ADAPTIVE makes every record durable before sequencing it; after a roll that has to happen
                // again in the new segment. A rewrite needs it in SYNC too. Otherwise no barrier.
                final int recordLength = readRecordLength(eventFile);
                final boolean needsBarrier = isFlushedRewrite || isAdaptive;
                for (String file : EVENT_FILES) {
                    Assert.assertEquals(
                            file + " must get a barrier holding the final record before the txn is sequenced: "
                                    + ff.barriers,
                            needsBarrier,
                            ff.hasBarrierBeforeSequencing(file, recordLength, seqTxn)
                    );
                }
                // The NO_TXN commit: its own per-commit grade, plus a barrier per file for a flushed rewrite, with
                // an MS_ASYNC before it off Linux, and nothing more.
                assertEventBarriers(ff, segmentMarker, isMsyncedRewrite ? 1 : 0, isFlushedRewrite ? 1 : 0, isRoll);

                for (int i = 2; i < 4; i++) {
                    appendRow(writer, i, i + 1);
                    ff.clearCounters();
                    writer.commit();
                    assertEventBarriers(ff, segmentMarker, 0, 0, false);
                }
            }
            drainWalQueue();
            final String nullValue = isRewritten ? "" : "null";
            final StringBuilder expected = new StringBuilder("v\tc\n");
            for (int v = isRoll ? 1 : 2; v < 5; v++) {
                expected.append(v).append('\t').append(nullValue).append('\n');
            }
            assertQuery("SELECT v, c FROM x ORDER BY v").expectSize().returns(expected);
        });
    }

    private static void appendRow(WalWriter writer, long ts, long v) {
        final TableWriter.Row row = writer.newRow(ts);
        row.putLong(1, v);
        row.append();
    }

    private static int readRecordLength(Path eventFile) {
        try (FileChannel channel = FileChannel.open(eventFile, StandardOpenOption.READ)) {
            final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            if (channel.read(buf, WalUtils.WALE_HEADER_SIZE) < Integer.BYTES) {
                return Integer.MIN_VALUE;
            }
            return buf.getInt(0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Exact event-file barriers of one commit: the per-commit grade of the mode, plus {@code rewriteMsyncs}
     * MS_ASYNC and {@code rewriteBarriers} fdatasync per file. ADAPTIVE commits flush the event files at every
     * window, with an MS_ASYNC write-back first off Linux only (MemoryMA.flushFile); a record the roll
     * re-created in a new segment is synced by {@code WalEventWriter.sync(ADAPTIVE)}, which is MS_SYNC at W=0.
     */
    private void assertEventBarriers(
            SyncAttributingFilesFacade ff,
            String segmentMarker,
            int rewriteMsyncs,
            int rewriteBarriers,
            boolean isRolledRecord
    ) {
        final boolean isAdaptive = "adaptive".equals(commitMode);
        final boolean isAdaptiveW0 = isAdaptive && groupWindowUs == 0;
        // ASYNC syncs the event files with an async sync, which makes no msync call. ADAPTIVE msyncs them only
        // to write the mapped pages back off Linux, except the MS_SYNC of a rolled record at W=0.
        final int commitMsyncs;
        if ("nosync".equals(commitMode) || "async".equals(commitMode)) {
            commitMsyncs = 0;
        } else if (isAdaptive && !(isAdaptiveW0 && isRolledRecord)) {
            commitMsyncs = ff.isSyncfsFileSystemWide() ? 0 : 1;
        } else {
            commitMsyncs = 1;
        }
        final int commitFsyncs = isAdaptive ? 1 : 0;
        for (String file : EVENT_FILES) {
            final String path = segmentMarker + file;
            // The facade matches substrings, and "_event" is a prefix of "_event.i".
            final int msyncs = WalUtils.EVENT_FILE_NAME.equals(file)
                    ? ff.msyncCount(path) - ff.msyncCount(path + ".") : ff.msyncCount(path);
            final int syncMsyncs = WalUtils.EVENT_FILE_NAME.equals(file)
                    ? ff.msyncCount(path, false) - ff.msyncCount(path + ".", false) : ff.msyncCount(path, false);
            final int fsyncs = WalUtils.EVENT_FILE_NAME.equals(file)
                    ? ff.fsyncCount(path) - ff.fsyncCount(path + ".") : ff.fsyncCount(path);
            final boolean isSyncGrade = "sync".equals(commitMode) || (isAdaptiveW0 && isRolledRecord);
            final String what = file + ff.debugDump();
            Assert.assertEquals(what + " msyncs", commitMsyncs + rewriteMsyncs, msyncs);
            Assert.assertEquals(what + " MS_SYNC msyncs", isSyncGrade ? commitMsyncs : 0, syncMsyncs);
            Assert.assertEquals(what + " fsyncs", commitFsyncs + rewriteBarriers, fsyncs);
        }
    }

    /**
     * Records every fdatasync on the watched segment's event files, with the record length the mapping held
     * and the table's sequencer txn at that moment.
     */
    private static class BarrierFacade extends SyncAttributingFilesFacade {
        private final List<String> barriers = new ArrayList<>();
        private final Map<Long, String> fdToPath = new HashMap<>();
        private Path eventFile;
        private boolean isArmed;
        private boolean isSyncfsFileSystemWide = super.isSyncfsFileSystemWide();
        private String segmentMarker;
        private SeqTxnTracker tracker;

        @Override
        public synchronized void clearCounters() {
            super.clearCounters();
            barriers.clear();
        }

        @Override
        public void fdatasync(long fd) {
            super.fdatasync(fd);
            if (isArmed) {
                final String path;
                synchronized (this) {
                    path = fdToPath.get(fd);
                }
                if (path != null && path.contains(segmentMarker)) {
                    final String file = path.substring(path.indexOf(segmentMarker) + segmentMarker.length());
                    final String barrier = file + "|len=" + readRecordLength(eventFile) + "|seqTxn=" + tracker.getSeqTxn();
                    synchronized (this) {
                        barriers.add(barrier);
                    }
                }
            }
        }

        @Override
        public boolean isSyncfsFileSystemWide() {
            return isSyncfsFileSystemWide;
        }

        @Override
        public long openRO(LPSZ name) {
            final long fd = super.openRO(name);
            remember(fd, name);
            return fd;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            remember(fd, name);
            return fd;
        }

        private static String pathToString(LPSZ name) {
            final int n = name.size();
            final StringBuilder sb = new StringBuilder(n);
            for (int i = 0; i < n; i++) {
                final char c = (char) (name.byteAt(i) & 0xFF);
                sb.append(c == '\\' ? '/' : c);
            }
            return sb.toString();
        }

        private synchronized boolean hasBarrierBeforeSequencing(String file, int recordLength, long seqTxn) {
            for (int i = 0, n = barriers.size(); i < n; i++) {
                final String[] parts = barriers.get(i).split("\\|");
                if (parts[0].equals(file)
                        && parts[1].equals("len=" + recordLength)
                        && Long.parseLong(parts[2].substring("seqTxn=".length())) < seqTxn) {
                    return true;
                }
            }
            return false;
        }

        private synchronized void remember(long fd, LPSZ name) {
            if (fd > -1) {
                fdToPath.put(fd, pathToString(name));
            }
        }
    }
}
