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

package io.questdb.test.cairo.idx;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.Files;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Counts the msync calls on POSTING index files ({@code .pv}, {@code .pcN}, {@code .pci}, {@code .pk}) while
 * seals run, per commit mode. A seal flushes the files it wrote only under a mode that promises to survive a power
 * loss: SYNC and ADAPTIVE wait for each file, NOSYNC and ASYNC leave them to the OS, as their commits do. Under
 * ADAPTIVE a seal of the commit being applied waits for the key file alone once the table holds a save point that
 * flushes the whole filesystem: its other files reach the disk with that save point.
 */
@RunWith(Parameterized.class)
public class PostingSealFlushCostTest extends AbstractCairoTest {
    private static final long DAY = 86_400_000_000L;
    private static final int DAYS = 5;
    private static final long HOUR = 3_600_000_000L;
    // msync calls on index files by kind: pv, pcN, pci, pk, with " async" for an asynchronous one
    private static final Map<String, Integer> MSYNCS = new TreeMap<>();
    private static int sequencerLogFlushes;
    private final String commitMode;

    public PostingSealFlushCostTest(String commitMode) {
        this.commitMode = commitMode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{"nosync"}, {"async"}, {"sync"}, {"adaptive"}});
    }

    @Override
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        // No save point inside a measured operation: it flushes the open indexers itself. A table's first apply
        // batch still ends with one.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1_000_000_000L);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, Long.MAX_VALUE);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
    }

    // ALTER TABLE ADD INDEX builds and seals the index of every partition. A replay of the build after a power
    // loss trusts the files it finds, so the build waits for every file, save point or not.
    @Test
    public void testAddIndex() throws Exception {
        assertMemoryLeak(new CountingFacade(), () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k SYMBOL, c0 LONG, c1 LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            insertAndApply(rows(0));
            clearFlushes();
            execute("ALTER TABLE t ALTER COLUMN k ADD INDEX TYPE POSTING INCLUDE (c0, c1)");
            drainWalQueue();
            assertSealFlushes(DAYS);
            assertIndexMatchesScan();
        });
    }

    // With merge-append on, as in the test harness, a commit appends one generation to the index of the partition
    // it extends, and the commit that brings the generations to their limit seals them.
    @Test
    public void testAppendsUpToTheGenerationLimit() throws Exception {
        // many commits, and the other modes flush here as in every other test
        Assume.assumeTrue(isAdaptive());
        assertMemoryLeak(new CountingFacade(), () -> {
            createIndexedTable();
            insertAndApply("(0::TIMESTAMP, 'k0', 0, 0)");
            clearFlushes();
            // the first commit left one sealed generation
            for (int i = 1; i < PostingIndexUtils.MAX_GEN_COUNT; i++) {
                insertAndApply("(" + i + "::TIMESTAMP, 'k" + (i % 3) + "', " + i + ", " + -i + ")");
            }
            assertSealFlushesWithSavePoint(1, 1);
            assertIndexMatchesScan();
        });
    }

    // A writer takes its owner's word that the save point covers a seal for one stamp: the next stamp, a reopen and
    // the seal of an emergency close flush every file again.
    @Test
    public void testCoverEndsWithItsStamp() throws Exception {
        assertMemoryLeak(new CountingFacade(), () -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int rootLen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "k", COLUMN_NAME_TXN_NONE)) {
                    appendTwoGenerations(writer, 0);
                    writer.setNextTxnAtSeal(1, () -> true);
                    clearFlushes();
                    writer.seal();
                    assertPlainSealFlushes(true);

                    appendTwoGenerations(writer, 16);
                    writer.setNextTxnAtSeal(2);
                    clearFlushes();
                    writer.seal();
                    assertPlainSealFlushes(false);

                    appendTwoGenerations(writer, 32);
                    writer.setNextTxnAtSeal(3, () -> true);
                    writer.of(path.trimTo(rootLen), "k", COLUMN_NAME_TXN_NONE);
                    clearFlushes();
                    writer.seal();
                    assertPlainSealFlushes(false);

                    appendTwoGenerations(writer, 48);
                    writer.setNextTxnAtSeal(4, () -> true);
                    clearFlushes();
                    writer.closeNoTruncate();
                    assertPlainSealFlushes(false);
                }
            }
        });
    }

    // A table's first apply batch runs before its first save point, so nothing would cut its seals after a power
    // loss: they wait for every file.
    @Test
    public void testFirstBatchOfNewTable() throws Exception {
        assertMemoryLeak(new CountingFacade(), () -> {
            createIndexedTable();
            clearFlushes();
            insertAndApply(rows(0));
            assertSealFlushes(DAYS);
            assertIndexMatchesScan();
        });
    }

    // Where the save point flushes only the partitions it knows were written, as on macOS and Windows, a seal
    // waits for every file.
    @Test
    public void testNoFileSystemWideSyncfs() throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        final CountingFacade ff = new CountingFacade();
        ff.isSyncfsFileSystemWide = false;
        assertMemoryLeak(ff, () -> {
            createIndexedTable();
            insertAndApply(rows(0));
            clearFlushes();
            insertAndApply(rows(500_000L));
            assertSealFlushes(DAYS);
            assertIndexMatchesScan();
        });
    }

    // A table without a WAL takes no save point, and its seals ask nothing of the WAL.
    @Test
    public void testNonWalTable() throws Exception {
        assertMemoryLeak(new CountingFacade(), () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1), c0 LONG, c1 LONG)" +
                    " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES " + rows(0));
            clearFlushes();
            execute("INSERT INTO t VALUES " + rows(500_000L));
            assertSealFlushes(DAYS);
            Assert.assertNull(engine.getTableSequencerAPI().getTxnTrackerIfExists(engine.verifyTableName("t")));
            assertIndexMatchesScan();
        });
    }

    // The first commit into a new partition seals the index it starts there.
    @Test
    public void testPartitionSwitchAfterAppends() throws Exception {
        assertMemoryLeak(new CountingFacade(), () -> {
            createIndexedTable();
            for (int i = 0; i < 40; i++) {
                insertAndApply("(" + i + "::TIMESTAMP, 'k" + (i % 3) + "', " + i + ", " + -i + ")");
            }
            clearFlushes();
            insertAndApply("(" + DAY + "::TIMESTAMP, 'k1', 40, -40)");
            assertSealFlushesWithSavePoint(1, 1);
            assertIndexMatchesScan();
        });
    }

    // An index without INCLUDE columns has a value file and a key file. The commit that takes its generations
    // past the seal threshold seals them.
    @Test
    public void testPlainIndex() throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_POSTING_SEAL_GEN_THRESHOLD, 1);
        assertMemoryLeak(new CountingFacade(), () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k SYMBOL INDEX TYPE POSTING, c0 LONG, c1 LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            insertAndApply("(0::TIMESTAMP, 'k0', 0, 0), (" + DAY + "::TIMESTAMP, 'k1', 1, -1)");
            clearFlushes();
            // after the rows of the older partition
            insertAndApply("(1::TIMESTAMP, 'k2', 2, -2)");
            assertPlainSealFlushes(isSealCoveredBySavePoint());
            assertIndexMatchesScan();
        });
    }

    // A rewrite of a partition builds its index again. When the build outgrows its memory budget it seals what it
    // spilled, before the seal that adds the INCLUDE columns: two seals for the partition.
    @Test
    public void testRebuildOverMemoryBudget() throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 1);
        assertMemoryLeak(new CountingFacade(), () -> {
            createIndexedTable();
            // more rows of one key than its buffer holds
            final StringBuilder sql = new StringBuilder();
            for (int i = 1; i <= 20; i++) {
                sql.append(i == 1 ? "" : ", ").append('(').append(i * 1_000_000L).append("::TIMESTAMP, 'k0', ").append(i).append(", ").append(-i).append(')');
            }
            insertAndApply(sql.toString());
            clearFlushes();
            insertAndApply("(500_000::TIMESTAMP, 'k0', 0, 0)");
            assertSealFlushesWithSavePoint(1, 2);
            assertIndexMatchesScan();
        });
    }

    // A demotion to a replica that lands while a commit applies: a replica takes no save point, so from then on a
    // seal waits for every file.
    @Test
    public void testRoleSwitchWhileCommitApplies() throws Exception {
        Assume.assumeTrue(isAdaptive());
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        final CountingFacade ff = new CountingFacade();
        assertMemoryLeak(ff, () -> {
            createIndexedTable();
            insertAndApply(rows(0));
            clearFlushes();
            // the rewrite of the first partition opens its new value file before any seal
            ff.onValueFileOpen = () -> engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            try {
                insertAndApply(rows(500_000L));
                Assert.assertNull("the role switch must land in the commit", ff.onValueFileOpen);
                assertSealFlushes(DAYS);
            } finally {
                ff.onValueFileOpen = null;
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            }
            assertIndexMatchesScan();
        });
    }

    // A commit of a WAL writer that stays open waits for its group flush, so the apply can run before the
    // sequencer record is on disk. A seal then flushes the sequencer log, once, instead of its own files: recovery
    // cuts index state above the save point only when the WAL on disk holds a transaction after it. A commit that
    // seals nothing flushes nothing.
    @Test
    public void testSealOfCommitThatWaitsForItsGroupFlush() throws Exception {
        Assume.assumeTrue(isAdaptive());
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        // no group flush before the writer closes
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 600_000_000L);
        assertMemoryLeak(new CountingFacade(), () -> {
            createIndexedTable();
            insertAndApply(rows(0));
            final TableToken token = engine.verifyTableName("t");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            try (WalWriter walWriter = engine.getWalWriter(token)) {
                // after every row of the last partition: the commit appends a generation and seals nothing
                appendRow(walWriter, (DAYS - 1) * DAY + 20 * HOUR, 100);
                walWriter.commit();
                Assert.assertTrue("the commit must wait for its group flush", tracker.getLocalDurableSeqTxn() < tracker.getSeqTxn());
                clearFlushes();
                drainWalQueue();
                Assert.assertEquals("msync calls on index files", "{}", msyncs().toString());
                Assert.assertEquals("flushes of the sequencer log", 0, sequencerLogFlushes);

                appendRows(walWriter, 500_000L);
                walWriter.commit();
                clearFlushes();
                drainWalQueue();
                assertSealFlushesWithSavePoint(DAYS, DAYS);
                Assert.assertEquals("flushes of the sequencer log", isSealCoveredBySavePoint() ? 1 : 0, sequencerLogFlushes);

                // the same save point: the log is flushed past it already
                appendRows(walWriter, 600_000L);
                walWriter.commit();
                Assert.assertTrue("the commit must wait for its group flush", tracker.getLocalDurableSeqTxn() < tracker.getSeqTxn());
                clearFlushes();
                drainWalQueue();
                assertSealFlushesWithSavePoint(DAYS, DAYS);
                Assert.assertEquals("flushes of the sequencer log", 0, sequencerLogFlushes);
            }
            assertIndexMatchesScan();
        });
    }

    // The seal's flush of the sequencer log fails as a flush of one of its own files does: the commit is not
    // applied and the engine stops, rather than apply on a log that may have lost pages.
    @Test
    public void testSealWhenSequencerLogFlushFails() throws Exception {
        final CountingFacade ff = new CountingFacade();
        // the seal flushes the log only where the save point covers it
        Assume.assumeTrue(isAdaptive() && ff.isSyncfsFileSystemWide());
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 600_000_000L);
        assertMemoryLeak(ff, () -> {
            createIndexedTable();
            insertAndApply(rows(0));
            final TableToken token = engine.verifyTableName("t");
            final WalWriter walWriter = engine.getWalWriter(token);
            try {
                appendRows(walWriter, 500_000L);
                walWriter.commit();
                ff.isSequencerLogFlushFailing = true;
                try {
                    drainWalQueue();
                    Assert.fail("the apply must stop");
                } catch (CairoError e) {
                    Assert.assertTrue("the stop must carry the failed flush: " + e, CairoException.isDataSyncFailure(e));
                }
                Assert.assertTrue(engine.isDurabilityFailed());
                Assert.assertTrue(engine.getTableSequencerAPI().getTxnTracker(token).isDurableFrontierFenced());
            } finally {
                ff.isSequencerLogFlushFailing = false;
                engine.resetDurabilityFailure();
                // the close of the writer would flush its commit against the fenced frontier and stop the engine
                walWriter.simulatePowerLossDropPending();
                walWriter.close();
            }
            assertQuery("SELECT count() FROM t").noLeakCheck().noRandomAccess().expectSize().returns("count\n" + 4 * DAYS + "\n");
            assertIndexMatchesScan();
        });
    }

    // A flush of the sequencer log that fails fences the table's durable frontier, and the kernel reports the lost
    // pages to that flush alone. A seal that flushes the log after it must not take its own success for a durable
    // log: it waits for every file.
    @Test
    public void testSealWhenSequencerLogFlushProvesNothing() throws Exception {
        Assume.assumeTrue(isAdaptive());
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 600_000_000L);
        final CountingFacade ff = new CountingFacade();
        assertMemoryLeak(ff, () -> {
            createIndexedTable();
            insertAndApply(rows(0));
            final TableToken token = engine.verifyTableName("t");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            final WalWriter walWriter = engine.getWalWriter(token);
            try {
                appendRows(walWriter, 500_000L);
                walWriter.commit();
                // as when the flush of another writer of the table failed right before this one
                ff.onSequencerLogFlush = () -> tracker.fenceDurableFrontier(CairoException.dataSyncFailureCarry(5, "fdatasync"));
                clearFlushes();
                drainWalQueue();
                assertSealFlushes(DAYS);
                Assert.assertEquals("the seal must flush the sequencer log", isSealCoveredBySavePoint(), sequencerLogFlushes > 0);
            } finally {
                ff.onSequencerLogFlush = null;
                // the close of the writer would flush its commit against the fenced frontier and stop the engine
                walWriter.simulatePowerLossDropPending();
                walWriter.close();
            }
            assertIndexMatchesScan();
        });
    }

    // A squash reseals the partition it folds into, with no WAL transaction of its own. With none after the save
    // point either, recovery would keep the seal after a power loss, so it waits for every file, and the sequencer
    // log holds nothing to flush.
    @Test
    public void testSquashWithNoTransactionAfterSavePoint() throws Exception {
        assertSquashWaitsForEveryFile(false);
    }

    // After a start the table's tracker can run ahead of the sequencer log, until the log is opened. The flush of
    // the log tells what it holds: no transaction after the save point, so the seal waits for every file.
    @Test
    public void testSquashWithTrackerAheadOfSequencerLog() throws Exception {
        assertSquashWaitsForEveryFile(true);
    }

    // A WAL commit into several new partitions seals the covering index of each.
    @Test
    public void testWalCommitIntoNewPartitions() throws Exception {
        assertMemoryLeak(new CountingFacade(), () -> {
            createIndexedTable();
            insertAndApply("(0::TIMESTAMP, 'k0', 0, 0)");
            clearFlushes();
            insertAndApply(rows(DAY));
            assertSealFlushesWithSavePoint(DAYS, DAYS);
            assertIndexMatchesScan();
        });
    }

    // A WAL commit with rows between the committed rows of older partitions rewrites them and reseals their index.
    @Test
    public void testWalCommitIntoOlderPartitions() throws Exception {
        // the production default: such a commit rewrites the partition rather than appending to it in place
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(new CountingFacade(), () -> {
            createIndexedTable();
            insertAndApply(rows(0));
            clearFlushes();
            insertAndApply(rows(500_000L));
            assertSealFlushesWithSavePoint(DAYS, DAYS);
            Assert.assertEquals("flushes of the sequencer log", 0, sequencerLogFlushes);
            assertIndexMatchesScan();
        });
    }

    private static void appendRow(WalWriter walWriter, long timestamp, int i) {
        final TableWriter.Row row = walWriter.newRow(timestamp);
        row.putSym(1, "k" + (i % 3));
        row.putLong(2, i);
        row.putLong(3, -i);
        row.append();
    }

    // the rows of rows(shiftMicros)
    private static void appendRows(WalWriter walWriter, long shiftMicros) {
        for (int i = 0; i < 4 * DAYS; i++) {
            appendRow(walWriter, (i % DAYS) * DAY + (i / DAYS) * HOUR + shiftMicros, i);
        }
    }

    // two generations of four keys, of the values from base
    private static void appendTwoGenerations(PostingIndexWriter writer, int base) {
        for (int generation = 0; generation < 2; generation++) {
            for (int value = base + generation * 8; value < base + generation * 8 + 8; value++) {
                writer.add(value & 3, value);
            }
            writer.setMaxValue(base + generation * 8 + 7);
            writer.commit();
        }
    }

    private static void clearFlushes() {
        synchronized (MSYNCS) {
            MSYNCS.clear();
            sequencerLogFlushes = 0;
        }
    }

    private static void createIndexedTable() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, k SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1), c0 LONG, c1 LONG)" +
                " TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private static String fileName(String path) {
        return path.substring(Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\')) + 1);
    }

    // k.pv.1 -> pv, k.pc1.0.0.1 -> pcN, k.pci.0 -> pci, k.pk -> pk; null for a file that is not an index file
    private static String indexFileKind(String path) {
        if (path == null) {
            return null;
        }
        final String[] parts = fileName(path).split("\\.");
        final String kind = parts.length > 1 && parts[0].equals("k") ? parts[1].replaceAll("^pc[0-9]+$", "pcN") : "";
        return switch (kind) {
            case "pv", "pcN", "pci", "pk" -> kind;
            default -> null;
        };
    }

    // The sequencer log flushes counted after it are those of the apply, not of the commit.
    private static void insertAndApply(String values) throws Exception {
        execute("INSERT INTO t VALUES " + values);
        synchronized (MSYNCS) {
            sequencerLogFlushes = 0;
        }
        drainWalQueue();
    }

    private static Map<String, Integer> msyncs() {
        synchronized (MSYNCS) {
            return new TreeMap<>(MSYNCS);
        }
    }

    // four rows a day in each of DAYS day partitions, each row shiftMicros past a whole hour
    private static String rows(long shiftMicros) {
        final StringBuilder sql = new StringBuilder();
        for (int i = 0; i < 4 * DAYS; i++) {
            sql.append(i == 0 ? "" : ", ").append('(').append((i % DAYS) * DAY + (i / DAYS) * HOUR + shiftMicros)
                    .append("::TIMESTAMP, 'k").append(i % 3).append("', ").append(i).append(", ").append(-i).append(')');
        }
        return sql.toString();
    }

    private void assertIndexMatchesScan() throws Exception {
        for (int k = 0; k < 3; k++) {
            assertSqlCursors(
                    "SELECT ts, k, c0, c1 FROM t WHERE k::STRING = 'k" + k + "'",
                    "SELECT ts, k, c0, c1 FROM t WHERE k = 'k" + k + "'"
            );
        }
    }

    // The one seal of an index without INCLUDE columns: of its key file alone when the save point covers it, else
    // of its value file as well.
    private void assertPlainSealFlushes(boolean isCovered) {
        final Map<String, Integer> msyncs = msyncs();
        if (!CommitMode.isPowerLossDurable(engine.getConfiguration().getCommitMode())) {
            Assert.assertEquals("msync calls on index files under " + commitMode, "{}", msyncs.toString());
        } else if (isCovered) {
            Assert.assertEquals("msync calls on index files under " + commitMode, "{pk=1}", msyncs.toString());
        } else {
            Assert.assertTrue(
                    "msync calls on index files under " + commitMode + ' ' + msyncs,
                    msyncs.getOrDefault("pv", 0) >= 1 && msyncs.getOrDefault("pk", 0) >= 1
            );
        }
    }

    private void assertSealFlushes(int sealedPartitions) {
        Assert.assertEquals(CommitMode.fromString(commitMode), engine.getConfiguration().getCommitMode());
        final Map<String, Integer> msyncs = msyncs();
        if (!CommitMode.isPowerLossDurable(engine.getConfiguration().getCommitMode())) {
            Assert.assertEquals("msync calls on index files under " + commitMode, "{}", msyncs.toString());
            return;
        }
        // The seal of each partition waits for its value file, the cover files of c0, c1 and the designated
        // timestamp, the cover info file and the key file.
        final int[] perPartition = {1, 3, 1, 1};
        final String[] kinds = {"pv", "pcN", "pci", "pk"};
        for (int i = 0; i < kinds.length; i++) {
            Assert.assertTrue(
                    "msync calls on " + kinds[i] + " files under " + commitMode + ' ' + msyncs,
                    msyncs.getOrDefault(kinds[i], 0) >= perPartition[i] * sealedPartitions
            );
        }
    }

    // The seals of a commit applied once the table holds a save point: where the save point covers them they wait
    // for their key files alone, keyFileFlushes in all.
    private void assertSealFlushesWithSavePoint(int sealedPartitions, int keyFileFlushes) {
        if (isSealCoveredBySavePoint()) {
            Assert.assertEquals("msync calls on index files under " + commitMode, "{pk=" + keyFileFlushes + "}", msyncs().toString());
        } else {
            assertSealFlushes(sealedPartitions);
        }
    }

    private void assertSquashWaitsForEveryFile(boolean isTrackerAhead) throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(new CountingFacade(), () -> {
            createIndexedTable();
            final StringBuilder sql = new StringBuilder();
            for (int i = 1; i <= 400; i++) {
                sql.append(i == 1 ? "" : ", ").append('(').append(i * 1_000_000L).append("::TIMESTAMP, 'k").append(i % 3)
                        .append("', ").append(i).append(", ").append(-i).append(')');
            }
            insertAndApply(sql.toString());
            insertAndApply("(" + 20 * HOUR + "::TIMESTAMP, 'k0', 401, -401)");
            // between the rows of the partition: splits it
            insertAndApply("(" + 19 * HOUR + "::TIMESTAMP, 'k1', 402, -402)");
            final TableToken token = engine.verifyTableName("t");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            final long seqTxn = tracker.getSeqTxn();
            try (TableWriter writer = getWriter(token)) {
                Assert.assertTrue("the partition must be split", writer.getPartitionCount() > 1);
                if (isAdaptive()) {
                    writer.advanceDurableEpoch(1);
                    Assert.assertEquals("the save point must be at the last transaction", seqTxn, tracker.getDurableEpochSeqTxn());
                }
                if (isTrackerAhead) {
                    tracker.notifyOnCheck(seqTxn + 1);
                }
                clearFlushes();
                try {
                    writer.squashPartitions();
                } finally {
                    tracker.lowerSeqTxn(seqTxn);
                }
                Assert.assertEquals("the squash must fold the partition", 1, writer.getPartitionCount());
                assertSealFlushes(1);
                Assert.assertEquals(
                        "flushes of the sequencer log: " + sequencerLogFlushes,
                        isTrackerAhead && isSealCoveredBySavePoint(),
                        sequencerLogFlushes > 0
                );
            }
            assertIndexMatchesScan();
        });
    }

    private boolean isAdaptive() {
        return CommitMode.fromString(commitMode) == CommitMode.ADAPTIVE;
    }

    // The save point flushes every file of the filesystem on Linux; elsewhere it leaves a seal's files to the seal.
    private boolean isSealCoveredBySavePoint() {
        return isAdaptive() && engine.getConfiguration().getFilesFacade().isSyncfsFileSystemWide();
    }

    private static class CountingFacade extends TestFilesFacadeImpl {
        private boolean isSequencerLogFlushFailing;
        private boolean isSyncfsFileSystemWide = super.isSyncfsFileSystemWide();
        // run by each flush of a sequencer log, after the flush
        private Runnable onSequencerLogFlush;
        // run once, by the next open of a value file
        private Runnable onValueFileOpen;

        @Override
        public void fdatasync(long fd) {
            final String path = Files.findPathOfFd(fd);
            final boolean isSequencerLog = path != null && fileName(path).equals("_txnlog");
            if (isSequencerLog && isSequencerLogFlushFailing) {
                throw CairoException.dataSyncFailureOfFd(this, fd, 5, "fdatasync", "injected failure");
            }
            super.fdatasync(fd);
            if (isSequencerLog) {
                synchronized (MSYNCS) {
                    sequencerLogFlushes++;
                }
                if (onSequencerLogFlush != null) {
                    onSequencerLogFlush.run();
                }
            }
        }

        @Override
        public boolean isSyncfsFileSystemWide() {
            return isSyncfsFileSystemWide;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            super.msync(addr, len, async);
            final String kind = indexFileKind(Files.findPathOfMapping(addr));
            if (kind != null) {
                synchronized (MSYNCS) {
                    MSYNCS.merge(async ? kind + " async" : kind, 1, Integer::sum);
                }
            }
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            if (onValueFileOpen != null && Utf8s.containsAscii(name, ".pv.")) {
                final Runnable hook = onValueFileOpen;
                onValueFileOpen = null;
                hook.run();
            }
            return super.openRW(name, opts);
        }
    }
}
