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

package io.questdb.test.cairo.wal.seq;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.CRC32;

/**
 * Power-loss sweep over the tail of the V1 sequencer txn log.
 * <p>
 * A real {@link TableTransactionLogV1} writes a window of commits. Before every durability call it makes
 * on {@code _txnlog}, and after every commit, the sweep builds every disk image a
 * power loss could leave at that moment: each 512-byte sector of each file holds the content it had at
 * its last durability call, or at any later moment the sweep sampled. That covers a page or a sector
 * persisted or not, a record that straddles a sector or a page boundary, and the header sector persisted
 * ahead of the record sectors. Each image is reopened, read back and appended to.
 * <p>
 * Recovery must return exactly the written records up to the first one that is not wholly on disk, keep
 * every acknowledged record, never hand out a torn record, never throw, and accept the next commit.
 */
public class TxnLogV1TornTailSweepTest extends AbstractCairoTest {
    private static final int APPEND_FAILED = 5;
    private static final int DROPPED_ACKED = 3;
    private static final int EXACT = 0;
    private static final long HEADER_SIZE = TableTransactionLogFile.HEADER_SIZE;
    private static final int MAX_COMBINATIONS_PER_CRASH_POINT = 4096;
    private static final int MAX_FAILURE_SAMPLES = 40;
    private static final String[] OUTCOME_NAMES = {"exact", "throws", "torn-read", "lost-ack", "over-drop", "no-append"};
    private static final int OVER_DROPPED = 4;
    private static final int PAGE_SIZE = 4096;
    private static final int RECORD_SIZE = (int) TableTransactionLogV1.RECORD_SIZE;
    private static final int SECTOR_SIZE = 512;
    private static final int THROWS = 1;
    private static final int TORN_READ = 2;
    private static final long TS_BASE = 1_759_000_000_000_000L;

    @Test
    public void testCreateOverSurvivingLineageLeavesZerosPastTheTail() throws Exception {
        // create() over a _txnlog that survived a WAL to non-WAL to WAL round trip writes the new lineage over
        // the old one's records. A power loss that lands the header publishing the next txns of the new
        // lineage, but not their records, must leave zeros there to cut at, never an old intact record.
        assertMemoryLeak(() -> {
            final int oldTxns = 40;
            final long[][] modes = {
                    {CommitMode.NOSYNC, 0},
                    {CommitMode.ASYNC, 0},
                    {CommitMode.SYNC, 0},
                    {CommitMode.ADAPTIVE, 0},
                    {CommitMode.ADAPTIVE, 1_000}
            };
            final SweepConfiguration configuration = new SweepConfiguration(root, null);
            int scenario = 0;
            for (long[] mode : modes) {
                // The first record of the new lineage that is not on disk: in the header's sector, straddling
                // the second sector, inside it, and the old lineage's last record.
                for (int newTxns : new int[]{0, 1, 15, 16, oldTxns - 1}) {
                    final String seqDir = root + "/lineage-" + (scenario++) + "/" + WalUtils.SEQ_DIR;
                    Files.createDirectories(Paths.get(seqDir));
                    final String logPath = seqDir + "/" + WalUtils.TXNLOG_FILE_NAME;
                    try (Path path = new Path().of(seqDir)) {
                        configuration.commitMode = CommitMode.NOSYNC;
                        configuration.groupWindowUs = 0;
                        try (TableTransactionLogV1 log = new TableTransactionLogV1(configuration)) {
                            log.create(path, TS_BASE);
                            for (int t = 1; t <= oldTxns; t++) {
                                log.addEntry(0, 100 + t, 0, t - 1, TS_BASE + t * 1_000L, 0, 0, 0);
                            }
                        }
                        configuration.commitMode = (int) mode[0];
                        configuration.groupWindowUs = mode[1];
                        try (TableTransactionLogV1 log = new TableTransactionLogV1(configuration)) {
                            log.create(path, TS_BASE);
                            for (int t = 1; t <= newTxns; t++) {
                                Assert.assertEquals(t, log.addEntry(0, 7, 0, t - 1, TS_BASE + 50_000L + t * 1_000L, 0, 0, 0));
                            }
                        }
                        final byte[] image = readFile(logPath);
                        for (int publishedTxn = newTxns + 1; publishedTxn <= oldTxns; publishedTxn++) {
                            ByteBuffer.wrap(image).order(ByteOrder.LITTLE_ENDIAN)
                                    .putLong((int) TableTransactionLogFile.MAX_TXN_OFFSET_64, publishedTxn);
                            writeFile(logPath, image);
                            try (TableTransactionLogV1 log = new TableTransactionLogV1(configuration)) {
                                log.open(path);
                                Assert.assertEquals(
                                        "records that never reached the disk must be cut, not read from the old lineage [mode="
                                                + CommitMode.toString((int) mode[0]) + ", window=" + mode[1] + ", newTxns=" + newTxns
                                                + ", publishedTxn=" + publishedTxn + ']',
                                        newTxns,
                                        log.lastTxn()
                                );
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testPowerLossInTheTailKeepsExactlyTheIntactPrefix() throws Exception {
        assertMemoryLeak(() -> {
            final Sweep sweep = new Sweep(root);
            for (Window window : Window.values()) {
                for (int txn : alignments()) {
                    sweep.run(window, txn, false);
                    sweep.run(window, txn, true);
                }
            }
            final String report = sweep.report();
            System.out.println(report);
            Assert.assertEquals(report, 0, sweep.failures);
        });
    }

    // The txn of the record the window is built around: one inside a sector, then for every split point
    // inside a record (a multiple of 4, since the header is 76 bytes) one record split by a sector boundary
    // and one split by a page boundary.
    private static List<Integer> alignments() {
        final List<Integer> txns = new ArrayList<>();
        // Record 20 lies inside the second sector, clear of the header's sector.
        txns.add(20);
        for (int split = 4; split < RECORD_SIZE; split += 4) {
            txns.add(firstTxnSplitAt(split, false));
            txns.add(firstTxnSplitAt(split, true));
        }
        return txns;
    }

    private static long crc(byte[] bytes) {
        final CRC32 crc = new CRC32();
        crc.update(bytes);
        return crc.getValue() * 31 + bytes.length;
    }

    private static int firstTxnSplitAt(int split, boolean atPage) {
        for (int txn = 3; ; txn++) {
            final long boundary = recordOffset(txn) + split;
            if (atPage ? boundary % PAGE_SIZE == 0 : boundary % SECTOR_SIZE == 0 && boundary % PAGE_SIZE != 0) {
                return txn;
            }
        }
    }

    private static byte[] readFile(String path) {
        try {
            return Files.readAllBytes(Paths.get(path));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Rec recordAt(byte[] log, long txn) {
        final long offset = recordOffset(txn);
        if (offset + RECORD_SIZE > log.length) {
            return null;
        }
        final ByteBuffer buf = ByteBuffer.wrap(log).order(ByteOrder.LITTLE_ENDIAN);
        final int o = (int) offset;
        return new Rec(
                buf.getLong(o + (int) TableTransactionLogFile.TX_LOG_STRUCTURE_VERSION_OFFSET),
                buf.getInt(o + (int) TableTransactionLogFile.TX_LOG_WAL_ID_OFFSET),
                buf.getInt(o + (int) TableTransactionLogFile.TX_LOG_SEGMENT_OFFSET),
                buf.getInt(o + (int) TableTransactionLogFile.TX_LOG_SEGMENT_TXN_OFFSET),
                buf.getLong(o + (int) TableTransactionLogFile.TX_LOG_COMMIT_TIMESTAMP_OFFSET)
        );
    }

    private static long recordOffset(long txn) {
        return HEADER_SIZE + (txn - 1) * RECORD_SIZE;
    }

    private static int sectorCount(int length) {
        return (length + SECTOR_SIZE - 1) / SECTOR_SIZE;
    }

    private static byte[] sectorOf(byte[] content, int sector) {
        final byte[] bytes = new byte[SECTOR_SIZE];
        final int from = sector * SECTOR_SIZE;
        if (from < content.length) {
            System.arraycopy(content, from, bytes, 0, Math.min(SECTOR_SIZE, content.length - from));
        }
        return bytes;
    }

    private static void writeFile(String path, byte[] content) {
        try {
            Files.write(Paths.get(path), content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private enum Window {
        ADAPTIVE_DATA("ADAPTIVE W=0, data commit", CommitMode.ADAPTIVE, 0, "D", false),
        ADAPTIVE_STRUCTURE("ADAPTIVE W=0, structure change", CommitMode.ADAPTIVE, 0, "S", false),
        ADAPTIVE_GROUP("ADAPTIVE W>0, batch of three", CommitMode.ADAPTIVE, 1_000, "DDD", true),
        NOSYNC_BATCH("NOSYNC, three commits", CommitMode.NOSYNC, 0, "DDD", false),
        // The commit mode changes between restarts: a NOSYNC open scans records no flush covered, and the next
        // start is ADAPTIVE.
        NOSYNC_REOPEN_THEN_ADAPTIVE("NOSYNC, three commits and a reopen, reopened ADAPTIVE", CommitMode.NOSYNC, 0, "DDD", false, true, CommitMode.ADAPTIVE);

        final int commitMode;
        final boolean flushAtEnd;
        final long groupWindowUs;
        final String label;
        final String ops;
        final boolean reopenAtEnd;
        final int verifyMode;

        Window(String label, int commitMode, long groupWindowUs, String ops, boolean flushAtEnd) {
            this(label, commitMode, groupWindowUs, ops, flushAtEnd, false, commitMode);
        }

        Window(String label, int commitMode, long groupWindowUs, String ops, boolean flushAtEnd, boolean reopenAtEnd, int verifyMode) {
            this.label = label;
            this.commitMode = commitMode;
            this.groupWindowUs = groupWindowUs;
            this.ops = ops;
            this.flushAtEnd = flushAtEnd;
            this.reopenAtEnd = reopenAtEnd;
            this.verifyMode = verifyMode;
        }

        boolean isAckedPerCommit() {
            return commitMode == CommitMode.ADAPTIVE && groupWindowUs == 0;
        }
    }

    private static class FileModel {
        final List<byte[]> history = new ArrayList<>();
        final String name;
        final String path;
        int[] durableFrom = new int[0];

        FileModel(String path) {
            this.path = Paths.get(path).normalize().toString();
            this.name = Paths.get(path).getFileName().toString();
            history.add(readFile(path));
        }

        int durableFrom(int sector) {
            return sector < durableFrom.length ? durableFrom[sector] : 0;
        }

        void markDurable(long from, long to) {
            final byte[] content = readFile(path);
            history.add(content);
            final int snapshot = history.size() - 1;
            final int sectors = sectorCount(content.length);
            if (durableFrom.length < sectors) {
                durableFrom = Arrays.copyOf(durableFrom, sectors);
            }
            // msync works on whole pages.
            final long hi = Math.min(to == Long.MAX_VALUE ? to : (to + PAGE_SIZE - 1) / PAGE_SIZE * PAGE_SIZE, content.length);
            for (int s = (int) (from / SECTOR_SIZE); (long) s * SECTOR_SIZE < hi; s++) {
                durableFrom[s] = snapshot;
            }
        }
    }

    private record Rec(long structureVersion, int walId, int segmentId, int segmentTxn, long commitTimestamp) {
    }

    private static class Sweep {
        final SweepConfiguration configuration;
        final StringBuilder failureSamples = new StringBuilder();
        final SweepFacade ff = new SweepFacade();
        final Random rnd = new Random(42);
        final String root;
        final Map<String, int[]> rows = new TreeMap<>();
        final Set<Long> seen = new HashSet<>();
        final SweepConfiguration verifyConfiguration;
        final String verifyDir;
        int acked;
        String activity;
        List<Rec> expected;
        int failureSampleCount;
        int failures;
        int images;
        FileModel logModel;
        int sampledCrashPoints;
        int scenario;
        boolean staleTail;
        int txn;
        Window window;

        Sweep(CharSequence root) throws IOException {
            this.root = root.toString();
            this.configuration = new SweepConfiguration(root, ff);
            this.verifyConfiguration = new SweepConfiguration(root, ff);
            this.verifyDir = this.root + "/torn-tail-verify/" + WalUtils.SEQ_DIR;
            Files.createDirectories(Paths.get(verifyDir));
        }

        String report() {
            final StringBuilder sb = new StringBuilder();
            sb.append("V1 txnlog torn-tail sweep: ").append(images).append(" images verified, ")
                    .append(sampledCrashPoints).append(" crash points sampled rather than enumerated, ")
                    .append(failures).append(" failures\n");
            sb.append(String.format("%-110s", "window | bytes past the tail | first record not wholly on disk"));
            for (String name : OUTCOME_NAMES) {
                sb.append(String.format("%11s", name));
            }
            sb.append('\n');
            for (Map.Entry<String, int[]> e : rows.entrySet()) {
                sb.append(String.format("%-110s", e.getKey()));
                for (int count : e.getValue()) {
                    sb.append(String.format("%11d", count));
                }
                sb.append('\n');
            }
            if (failureSampleCount > 0) {
                sb.append("first failures:\n").append(failureSamples);
            }
            return sb.toString();
        }

        void run(Window window, int txn, boolean staleTail) throws IOException {
            this.window = window;
            this.txn = txn;
            this.staleTail = staleTail;
            seen.clear();
            final String seqDir = root + "/torn-tail-" + (scenario++) + "/" + WalUtils.SEQ_DIR;
            Files.createDirectories(Paths.get(seqDir));
            final String logPath = seqDir + "/" + WalUtils.TXNLOG_FILE_NAME;
            final int first = txn - (window.ops.length() > 1 ? 1 : 0);
            expected = new ArrayList<>();
            try (Path path = new Path().of(seqDir)) {
                // Everything before the window is written without syncs and counts as on disk once the
                // window starts, as if the kernel had written it back long ago.
                configuration.commitMode = CommitMode.NOSYNC;
                configuration.groupWindowUs = 0;
                TableTransactionLogV1 log = new TableTransactionLogV1(configuration);
                try {
                    log.create(path, TS_BASE);
                    for (int t = 1; t < first; t++) {
                        append(log, t == 1 ? 'S' : 'D', t);
                    }
                    if (staleTail) {
                        // An earlier run appended the window's txns with other content and lost power before
                        // the header sector reached the disk: the records sit past the tail.
                        final byte[] headerSector = sectorOf(readFile(logPath), 0);
                        for (int i = 0; i < window.ops.length(); i++) {
                            log.addEntry(lastStructureVersion(), 9, 9, 9, TS_BASE + 7_777_777 + i, 0, 0, 0);
                        }
                        log.close();
                        log = null;
                        final byte[] image = readFile(logPath);
                        System.arraycopy(headerSector, 0, image, 0, SECTOR_SIZE);
                        writeFile(logPath, image);
                    }
                } finally {
                    Misc.free(log);
                }

                configuration.commitMode = window.commitMode;
                configuration.groupWindowUs = window.groupWindowUs;
                logModel = new FileModel(logPath);
                acked = first - 1;
                ff.sweep = this;
                log = new TableTransactionLogV1(configuration);
                try {
                    activity = "open";
                    log.open(path);
                    crashPoint("after open");
                    for (int i = 0, n = window.ops.length(); i < n; i++) {
                        activity = "commit " + (first + i);
                        append(log, window.ops.charAt(i), first + i);
                        if (window.isAckedPerCommit()) {
                            acked = first + i;
                        }
                        crashPoint("after commit " + (first + i));
                    }
                    if (window.flushAtEnd) {
                        activity = "batch flush";
                        log.fdatasyncTxnLog();
                        acked = first + window.ops.length() - 1;
                        crashPoint("after batch flush");
                    }
                    if (window.reopenAtEnd) {
                        activity = "reopen";
                        log.close();
                        log = new TableTransactionLogV1(configuration);
                        log.open(path);
                        crashPoint("after reopen");
                    }
                } finally {
                    ff.sweep = null;
                    Misc.free(log);
                }
            }
        }

        private void append(TableTransactionLogV1 log, char kind, int txn) {
            final long ts = TS_BASE + txn * 1_000L;
            final Rec rec;
            if (kind == 'S') {
                rec = new Rec(lastStructureVersion() + 1, TableTransactionLogFile.STRUCTURAL_CHANGE_WAL_ID, -1, -1, ts);
                // A crash point inside the call may find the record published.
                expected.add(rec);
                // Same call sequence as TableTransactionLog.endMetadataChangeEntry().
                log.beginMetadataChangeEntry(rec.structureVersion, null, null, ts);
                log.fullSync();
                log.endMetadataChangeEntry();
                if (CommitMode.isPowerLossDurable(configuration.commitMode)) {
                    log.fullSync();
                }
            } else {
                rec = new Rec(lastStructureVersion(), 1 + txn % 3, txn % 2, txn % 100, ts);
                expected.add(rec);
                Assert.assertEquals(txn, log.addEntry(rec.structureVersion, rec.walId, rec.segmentId, rec.segmentTxn, ts, 0, 0, 0));
            }
            Assert.assertEquals(txn, log.lastTxn());
        }

        private int classify(List<Rec> read, int intactPrefix) {
            for (int i = 0, n = read.size(); i < n; i++) {
                if (i >= expected.size() || !read.get(i).equals(expected.get(i))) {
                    return TORN_READ;
                }
            }
            if (read.size() < acked) {
                return DROPPED_ACKED;
            }
            if (read.size() < intactPrefix) {
                return OVER_DROPPED;
            }
            return EXACT;
        }

        private void crashPoint(String label) {
            final FileModel[] models = {logModel};
            final byte[][] current = new byte[models.length][];
            final List<int[]> choiceAt = new ArrayList<>(); // {model, sector}
            final List<List<byte[]>> choices = new ArrayList<>();
            long combinations = 1;
            for (int m = 0; m < models.length; m++) {
                final FileModel model = models[m];
                current[m] = readFile(model.path);
                for (int s = 0, n = sectorCount(current[m].length); s < n; s++) {
                    final List<byte[]> candidates = new ArrayList<>();
                    for (int h = model.durableFrom(s), hn = model.history.size(); h <= hn; h++) {
                        final byte[] candidate = sectorOf(h < hn ? model.history.get(h) : current[m], s);
                        boolean isNew = true;
                        for (int c = 0, cn = candidates.size(); c < cn; c++) {
                            if (Arrays.equals(candidates.get(c), candidate)) {
                                isNew = false;
                                break;
                            }
                        }
                        if (isNew) {
                            candidates.add(candidate);
                        }
                    }
                    if (candidates.size() > 1) {
                        choiceAt.add(new int[]{m, s});
                        choices.add(candidates);
                        combinations = Math.min(combinations * candidates.size(), Long.MAX_VALUE / 64);
                    }
                }
                model.history.add(current[m]);
            }
            final boolean sample = combinations > MAX_COMBINATIONS_PER_CRASH_POINT;
            if (sample) {
                sampledCrashPoints++;
            }
            final long n = sample ? MAX_COMBINATIONS_PER_CRASH_POINT : combinations;
            final int[] pick = new int[choices.size()];
            for (long i = 0; i < n; i++) {
                long rest = i;
                for (int c = 0, cn = choices.size(); c < cn; c++) {
                    final int size = choices.get(c).size();
                    pick[c] = sample ? rnd.nextInt(size) : (int) (rest % size);
                    rest /= size;
                }
                final byte[][] image = new byte[models.length][];
                for (int m = 0; m < models.length; m++) {
                    image[m] = current[m].clone();
                }
                for (int c = 0, cn = choices.size(); c < cn; c++) {
                    final int[] at = choiceAt.get(c);
                    final byte[] sector = choices.get(c).get(pick[c]);
                    final int from = at[1] * SECTOR_SIZE;
                    System.arraycopy(sector, 0, image[at[0]], from, Math.min(SECTOR_SIZE, image[at[0]].length - from));
                }
                verify(image[0], label);
            }
        }

        private int lastStructureVersion() {
            return expected.isEmpty() ? 0 : (int) expected.get(expected.size() - 1).structureVersion;
        }

        private void onSync(String path, long from, long to) {
            final FileModel model = logModel != null && logModel.path.equals(path) ? logModel : null;
            if (model == null) {
                return;
            }
            crashPoint("before " + model.name + " sync in " + activity);
            model.markDurable(from, to);
        }

        private List<Rec> readAll(TableTransactionLogV1 log, Path path) {
            final List<Rec> records = new ArrayList<>();
            final TransactionLogCursor cursor = log.getCursor(0, path);
            try {
                while (cursor.hasNext()) {
                    records.add(new Rec(
                            cursor.getStructureVersion(),
                            cursor.getWalId(),
                            cursor.getSegmentId(),
                            cursor.getSegmentTxn(),
                            cursor.getCommitTimestamp()
                    ));
                }
            } finally {
                Misc.free(cursor);
            }
            return records;
        }

        private String shape(byte[] log, int txn) {
            final long offset = recordOffset(txn);
            final Rec want = expected.get(txn - 1);
            final byte[] wantBytes = ByteBuffer.allocate(RECORD_SIZE).order(ByteOrder.LITTLE_ENDIAN)
                    .putLong(want.structureVersion).putInt(want.walId).putInt(want.segmentId)
                    .putInt(want.segmentTxn).putLong(want.commitTimestamp).array();
            boolean stale = false;
            int split = 0;
            for (int i = 1; i < RECORD_SIZE; i++) {
                if ((offset + i) % SECTOR_SIZE == 0) {
                    split = i;
                }
            }
            boolean firstNew = true;
            boolean lastNew = true;
            for (int i = 0; i < RECORD_SIZE; i++) {
                final byte b = offset + i < log.length ? log[(int) (offset + i)] : 0;
                if (b != wantBytes[i]) {
                    if (b != 0) {
                        stale = true;
                    }
                    if (split > 0 && i >= split) {
                        lastNew = false;
                    } else {
                        firstNew = false;
                    }
                }
            }
            final String rest = stale ? "stale bytes" : "zeros";
            if (split == 0) {
                return "whole record, " + rest;
            }
            final String boundary = (offset + split) % PAGE_SIZE == 0 ? "page" : "sector";
            if (firstNew) {
                return String.format("first %2d bytes new, %s after (%s split)", split, rest, boundary);
            }
            if (lastNew) {
                return String.format("last %2d bytes new, %s before (%s split)", RECORD_SIZE - split, rest, boundary);
            }
            return "whole record, " + rest + " (" + boundary + " split)";
        }

        private void verify(byte[] log, String label) {
            if (!seen.add(crc(log) * 1_000_003L + acked)) {
                return;
            }
            verify(log, label, window.verifyMode);
            if (window.commitMode != CommitMode.NOSYNC) {
                // The commit mode may change between restarts.
                verify(log, label, CommitMode.NOSYNC);
            }
        }

        private void verify(byte[] logImage, String label, int reopenMode) {
            images++;
            final String logPath = verifyDir + "/" + WalUtils.TXNLOG_FILE_NAME;
            writeFile(logPath, logImage);

            final long published = ByteBuffer.wrap(logImage).order(ByteOrder.LITTLE_ENDIAN)
                    .getLong((int) TableTransactionLogFile.MAX_TXN_OFFSET_64);
            int intactPrefix = (int) published;
            for (int t = 1; t <= published; t++) {
                if (t > expected.size() || !expected.get(t - 1).equals(recordAt(logImage, t))) {
                    intactPrefix = t - 1;
                    break;
                }
            }
            final String shape = intactPrefix < published
                    ? (intactPrefix < expected.size() ? shape(logImage, intactPrefix + 1) : "record never written")
                    : "every published record intact";

            int outcome;
            String detail = "";
            int readCount = -1;
            verifyConfiguration.commitMode = reopenMode;
            verifyConfiguration.groupWindowUs = window.groupWindowUs;
            final TableTransactionLogV1 log = new TableTransactionLogV1(verifyConfiguration);
            try (Path path = new Path().of(verifyDir)) {
                log.open(path);
                final List<Rec> read = readAll(log, path);
                readCount = read.size();
                outcome = classify(read, intactPrefix);
                if (outcome == EXACT) {
                    final Rec next = new Rec(
                            read.isEmpty() ? 0 : read.get(read.size() - 1).structureVersion,
                            7, 1, 1, TS_BASE + 5_555_555L
                    );
                    final long txn = log.addEntry(next.structureVersion, next.walId, next.segmentId, next.segmentTxn, next.commitTimestamp, 0, 0, 0);
                    final List<Rec> after = readAll(log, path);
                    if (txn != read.size() + 1
                            || after.size() != read.size() + 1
                            || !after.get(read.size()).equals(next)
                            || !after.subList(0, read.size()).equals(read)) {
                        outcome = APPEND_FAILED;
                        detail = "txn=" + txn + ", readAfterAppend=" + after.size();
                    }
                }
            } catch (CairoException e) {
                outcome = THROWS;
                detail = e.getFlyweightMessage().toString();
            } finally {
                log.close();
            }

            final String key = window.label
                    + (reopenMode != window.verifyMode ? ", reopened NOSYNC" : "")
                    + " | " + (staleTail ? "stale records" : "zeros")
                    + " | " + shape;
            rows.computeIfAbsent(key, k -> new int[OUTCOME_NAMES.length])[outcome]++;
            if (outcome != EXACT) {
                failures++;
                if (failureSampleCount++ < MAX_FAILURE_SAMPLES) {
                    failureSamples.append("  ").append(OUTCOME_NAMES[outcome])
                            .append(": ").append(key)
                            .append(" [txn=").append(txn)
                            .append(", crash=").append(label)
                            .append(", published=").append(published)
                            .append(", intactPrefix=").append(intactPrefix)
                            .append(", acked=").append(acked)
                            .append(", read=").append(readCount)
                            .append(", detail=").append(detail)
                            .append("]\n");
                }
            }
        }
    }

    private static class SweepConfiguration extends DefaultTestCairoConfiguration {
        private final FilesFacade ff;
        private int commitMode;
        private long groupWindowUs;

        SweepConfiguration(CharSequence root, FilesFacade ff) {
            super(root);
            this.ff = ff;
        }

        @Override
        public long getAdaptiveCommitGroupWindowUs() {
            return groupWindowUs;
        }

        @Override
        public int getCommitMode() {
            return commitMode;
        }

        @Override
        public @NotNull FilesFacade getFilesFacade() {
            // The super constructor asks before the field is set.
            return ff != null ? ff : super.getFilesFacade();
        }
    }

    // Reports every durability call on a file this sweep models, and skips the real call: the sweep models
    // durability instead of performing it.
    private static class SweepFacade extends TestFilesFacadeImpl {
        private final Map<Long, String> fdPaths = new HashMap<>();
        private final Map<Long, Long> mappings = new HashMap<>();
        private Sweep sweep;

        @Override
        public boolean close(long fd) {
            fdPaths.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fdatasync(long fd) {
            onSync(fdPaths.get(fd), 0, Long.MAX_VALUE);
        }

        @Override
        public void fsync(long fd) {
            onSync(fdPaths.get(fd), 0, Long.MAX_VALUE);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            final long address = super.mmap(fd, len, offset, flags, memoryTag);
            if (address != FilesFacade.MAP_FAILED && offset == 0) {
                mappings.put(address, fd);
            }
            return address;
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            final long address = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
            if (address != FilesFacade.MAP_FAILED) {
                mappings.remove(addr);
                if (offset == 0) {
                    mappings.put(address, fd);
                }
            }
            return address;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (!async) {
                final Long fd = mappings.get(addr);
                if (fd != null) {
                    onSync(fdPaths.get(fd), 0, len);
                }
            }
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            mappings.remove(address);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (fd > -1) {
                fdPaths.put(fd, Paths.get(Utf8s.stringFromUtf8Bytes(name)).normalize().toString());
            }
            return fd;
        }

        private void onSync(String path, long from, long to) {
            if (sweep != null && path != null) {
                sweep.onSync(path, from, to);
            }
        }
    }
}
