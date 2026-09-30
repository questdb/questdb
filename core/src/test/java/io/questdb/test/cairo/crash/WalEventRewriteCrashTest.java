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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.str.LPSZ;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 * A WAL event record rewritten in place must read back after a power loss, in every commit mode, whatever
 * the kernel wrote back on its own.
 * <p>
 * When a concurrent ALTER TABLE ADD COLUMN ... SYMBOL is sequenced while a writer commits pending rows, the
 * writer's commit gets NO_TXN, replays the ALTER, and rewrites its already sealed record in place so that
 * the record carries the new column's null flag. The rewrite replaces bytes in three files that the kernel
 * writes back independently: {@code _event}, {@code _event.i} and {@code _event.c}. It can write any of them
 * back while it still holds the ORIGINAL record (between the first seal and the rewrite), and any of them
 * after the rewrite.
 * <p>
 * Each case promotes one subset of the three files to durable at the pre-rewrite point, the complementary
 * subset after the commit, and everything else the txn needs (sequencer, columns, directory entries), so
 * that the sequencer names the txn. {@code markFileDurable} models the kernel writeback. Without a barrier
 * after the rewrite, a pre-rewrite file next to post-rewrite ones is a mixed image: the reader condemns an
 * intact record as torn against the other version's sealed checksum entry, or finds it longer than its
 * stale index entry, and the table suspends. Where the original record survives in full, it applies without
 * the new column's null flag. The fix flushes all three files after the rewrite and before the txn is
 * sequenced, so the only image a sequenced txn can meet is the rewritten one.
 */
@RunWith(Parameterized.class)
public class WalEventRewriteCrashTest extends AbstractAdaptiveCrashTest {
    private static final String[] EVENT_FILES = {
            WalUtils.EVENT_FILE_NAME,
            WalUtils.EVENT_INDEX_FILE_NAME,
            WalUtils.EVENT_CHECKSUM_FILE_NAME
    };
    private final String commitMode;
    private final long groupWindowUs;
    private final boolean isRoll;
    private final int staleMask;

    public WalEventRewriteCrashTest(String commitMode, long groupWindowUs, boolean isRoll, int staleMask) {
        this.commitMode = commitMode;
        this.groupWindowUs = groupWindowUs;
        this.isRoll = isRoll;
        this.staleMask = staleMask;
    }

    @Parameterized.Parameters(name = "mode={0}, window={1}, roll={2}, stale={3}")
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
                // Bit i set: EVENT_FILES[i] was written back only while it held the original record.
                for (int staleMask = 0; staleMask < 1 << EVENT_FILES.length; staleMask++) {
                    params.add(new Object[]{mode[0], mode[1], isRoll, staleMask});
                }
            }
        }
        return params;
    }

    @Test
    public void testRewrittenRecordSurvivesIndependentWriteback() throws Exception {
        assumeCrashHarnessSupported();
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, groupWindowUs);
        node1.setProperty(PropertyKey.CAIRO_WAL_COMMIT_WRITEBACK_DRAIN, false);
        final WritebackFacade ff = new WritebackFacade();
        // Per-inode journaling: a barrier on one file must not incidentally make another durable.
        ff.modelSharedJournal = false;
        ff.setDbRoot(root);
        crashFf = ff;
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("x");
            final Path tableDir = Paths.get(root, token.getDirName()).toAbsolutePath();
            final Path segmentDir = tableDir.resolve(WalUtils.WAL_NAME_BASE + 1).resolve(isRoll ? "1" : "0");
            ff.segmentDir = segmentDir;
            ff.staleMask = staleMask;
            // Roll: the pending txn is not first in its segment, so the replayed ALTER moves it to a new
            // segment and rewrites it there. No roll: it is rewritten where it was first written.
            final long seqTxn = isRoll ? 3 : 2;
            try (WalWriter writer = getWalWriter("x")) {
                if (isRoll) {
                    appendRow(writer, 0, 1);
                    writer.commit();
                }
                markDurableBaseline();
                appendRow(writer, 1, 2);
                execute("ALTER TABLE x ADD COLUMN s SYMBOL");
                ff.isArmed = true;
                writer.commit();
                ff.isArmed = false;
            }
            Assert.assertTrue("the original record must be written back before the rewrite", ff.isCaptured);
            Assert.assertNotEquals(
                    "the capture must precede the rewrite",
                    ff.capturedLength,
                    readRecordLength(segmentDir.resolve(WalUtils.EVENT_FILE_NAME))
            );
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            Assert.assertEquals(seqTxn, tracker.getSeqTxn());
            if ("adaptive".equals(commitMode)) {
                // The writer went back to the pool, which runs the batched sequencer flush under W>0.
                Assert.assertTrue("the txn must be acknowledged durable", tracker.getLocalDurableSeqTxn() >= seqTxn);
            }

            engine.releaseAllWalWriters();
            for (int i = 0; i < EVENT_FILES.length; i++) {
                if ((staleMask & (1 << i)) == 0) {
                    ff.markFileDurable(segmentDir.resolve(EVENT_FILES[i]).toString());
                }
            }
            promoteEverythingElse(tableDir, segmentDir);

            recoverAfterCrash(new TableToken[]{token});

            Assert.assertFalse(
                    "table must not be suspended: " + engine.getTableSequencerAPI().getTxnTracker(token).getErrorMessage(),
                    engine.getTableSequencerAPI().isSuspended(token)
            );
            assertQuery("SELECT v, s FROM x ORDER BY v")
                    .expectSize()
                    .returns(isRoll ? "v\ts\n1\t\n2\t\n" : "v\ts\n2\t\n");
            // What only the rewritten record carries: the new column's null flag for the rows it holds.
            try (TableReader reader = getReader("x")) {
                Assert.assertTrue(
                        "the applied record must be the rewritten one",
                        reader.getSymbolMapReader(reader.getMetadata().getColumnIndex("s")).containsNullValue()
                );
            }
            releaseEngineHandles();
        });
    }

    private static void appendRow(WalWriter writer, long ts, long v) {
        final TableWriter.Row row = writer.newRow(ts);
        row.putLong(1, v);
        row.append();
    }

    private static boolean isEventFile(Path segmentDir, Path file) {
        if (!segmentDir.equals(file.getParent())) {
            return false;
        }
        final String name = file.getFileName().toString();
        for (String eventFile : EVENT_FILES) {
            if (eventFile.equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static int readRecordLength(Path eventFile) {
        if (!Files.exists(eventFile)) {
            return Integer.MIN_VALUE;
        }
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
     * The kernel writes back everything except the rewritten segment's event files, and journals every
     * directory entry. In NOSYNC and ASYNC nothing else would make the sequencer record or the rolled
     * segment durable; in the other modes this only repeats what the commit already made durable.
     */
    private void promoteEverythingElse(Path tableDir, Path segmentDir) {
        final List<Path> directories = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(tableDir)) {
            paths.forEach(p -> {
                if (Files.isDirectory(p)) {
                    directories.add(p);
                } else if (Files.isRegularFile(p) && !isEventFile(segmentDir, p)) {
                    crashFf.markFileDurable(p.toString());
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (io.questdb.std.str.Path path = new io.questdb.std.str.Path()) {
            for (int i = 0, n = directories.size(); i < n; i++) {
                TableUtils.fsyncDirDurable(crashFf, path.of(directories.get(i).toString()).$());
            }
        }
    }

    /**
     * Writes the chosen stale subset back on the first file operation that follows the first seal of the
     * record: the record is published (its length is stored last), and nothing has rewritten it yet.
     */
    private static class WritebackFacade extends CrashFaultFilesFacade {
        private int capturedLength;
        private boolean isArmed;
        private boolean isCaptured;
        private Path segmentDir;
        private int staleMask;

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            writeBackOriginalRecord();
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            writeBackOriginalRecord();
            return super.openRW(name, opts);
        }

        private void writeBackOriginalRecord() {
            if (!isArmed || isCaptured) {
                return;
            }
            final int length = readRecordLength(segmentDir.resolve(WalUtils.EVENT_FILE_NAME));
            if (length > 0) {
                isCaptured = true;
                capturedLength = length;
                for (int i = 0; i < EVENT_FILES.length; i++) {
                    if ((staleMask & (1 << i)) != 0) {
                        markFileDurable(segmentDir.resolve(EVENT_FILES[i]).toString());
                    }
                }
            }
        }
    }
}
