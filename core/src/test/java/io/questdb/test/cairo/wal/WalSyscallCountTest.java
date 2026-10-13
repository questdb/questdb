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

import io.questdb.BuildInformationHolder;
import io.questdb.DefaultFactoryProvider;
import io.questdb.PropServerConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerConfigurationException;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cutlass.json.JsonException;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Os;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.NanosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.jetbrains.annotations.Nullable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The file system calls of a 1-row WAL commit and of the apply pass that applies it. NOSYNC, ASYNC and SYNC
 * make the calls of the base version (the WAL without ADAPTIVE): the same kinds and counts on the same files,
 * except the one extra SYNC msync noted in {@code expected}. The apply pass also runs with STRING, VARCHAR and
 * ARRAY columns to cover variable-width mapping work. In every mode,
 * ADAPTIVE included, a WAL table keeps only the files the base version writes in its WAL segments and in
 * {@code txn_seq}: a mapped write makes no system call on a NOSYNC commit, so only the file set sees it.
 */
@RunWith(Parameterized.class)
public class WalSyscallCountTest extends AbstractCairoTest {
    // A WAL column file: .d, plus .i for var-size columns.
    private static final Pattern COLUMN_FILE = Pattern.compile("[a-z]+\\.[di]");
    private static final Properties COUNT_PROPERTIES = new Properties();
    private static final int MEASURED = 20;
    // The files base writes in a WAL segment: the event file and its index, the segment _meta and the column
    // files (.d, plus .i for var-size columns).
    private static final Pattern SEGMENT_FILE = Pattern.compile("_event|_event\\.i|_meta|[a-z]+\\.[di]");
    // The files base writes in txn_seq: the txn log, its metadata index and data, the sequencer _meta and its
    // versions, the WAL index, and the numbered V2 part files.
    private static final Pattern SEQ_FILE = Pattern.compile("_txnlog|_txnlog\\.meta\\.[di]|_meta(\\.[0-9]+)?|_wal_index\\.d|[0-9]+");
    private static final int WARMUP = 5;
    private static CairoConfigurationWrapper countConfiguration;
    private final String commitMode;
    private final boolean isV2;

    public WalSyscallCountTest(String commitMode, boolean isV2) {
        this.commitMode = commitMode;
        this.isV2 = isV2;
    }

    @Parameterized.Parameters(name = "mode={0}, v2={1}")
    public static Collection<Object[]> data() {
        final List<Object[]> params = new ArrayList<>();
        for (String mode : new String[]{"nosync", "async", "sync", "adaptive"}) {
            params.add(new Object[]{mode, false});
            params.add(new Object[]{mode, true});
        }
        return params;
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        COUNT_PROPERTIES.clear();
        configurationFactory = (dbRoot, telemetry, overrides) -> {
            // Only disposable paths, the harness clock and the counting facade differ from production.
            countConfiguration = new CairoConfigurationWrapper(createProductionConfiguration(dbRoot)) {
                @Override
                public CharSequence getCheckpointRoot() {
                    return dbRoot + String.valueOf(io.questdb.std.Files.SEPARATOR) + TableUtils.CHECKPOINT_DIRECTORY;
                }

                @Override
                public FilesFacade getFilesFacade() {
                    final FilesFacade facade = overrides.getFilesFacade();
                    return facade != null ? facade : super.getFilesFacade();
                }

                @Override
                public MicrosecondClock getMicrosecondClock() {
                    return overrides.getTestMicrosClock();
                }

                @Override
                public MillisecondClock getMillisecondClock() {
                    return () -> overrides.getTestMicrosClock().getTicks() / 1000L;
                }

                @Override
                public NanosecondClock getNanosecondClock() {
                    return () -> overrides.getTestMicrosClock().getTicks() * 1000L;
                }
            };
            return countConfiguration;
        };
        AbstractCairoTest.setUpStatic();
    }

    @AfterClass
    public static void tearDownStatic() {
        try {
            AbstractCairoTest.tearDownStatic();
        } finally {
            countConfiguration = null;
            COUNT_PROPERTIES.clear();
        }
    }

    @Before
    @Override
    public void setUp() {
        COUNT_PROPERTIES.clear();
        countConfiguration.setDelegate(createProductionConfiguration(root));
        super.setUp();
    }

    @Test
    public void testApplyPass() throws Exception {
        assertCalls(false, false);
    }

    @Test
    public void testApplyPassMappedIO() throws Exception {
        setCountProperty(PropertyKey.DEBUG_CAIRO_ALLOW_MIXED_IO, false);
        assertCalls(false, false);
    }

    @Test
    public void testApplyPassVarSize() throws Exception {
        assertCalls(false, true);
    }

    @Test
    public void testApplyPassVarSizeMappedIO() throws Exception {
        setCountProperty(PropertyKey.DEBUG_CAIRO_ALLOW_MIXED_IO, false);
        assertCalls(false, true);
    }

    /**
     * How a WAL column releases a page: ingest crossing many pages of every column file and a segment roll.
     * NOSYNC releases without an msync, ASYNC with MS_ASYNC, SYNC with MS_SYNC. ADAPTIVE on Linux releases
     * with MS_ASYNC, because the commit's fdatasync of each column file writes the released pages back;
     * elsewhere it keeps MS_SYNC.
     */
    @Test
    public void testColumnPageRelease() throws Exception {
        Assume.assumeFalse("the sequencer log format does not touch column pages", isV2);
        setCountProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        setCountProperty(PropertyKey.CAIRO_WAL_WRITER_DATA_APPEND_PAGE_SIZE, io.questdb.std.Files.PAGE_SIZE);
        // The counts below hold for any OS page size: the rows scale with it, 16 KiB pages on macOS take 4 times
        // the rows of 4 KiB ones, 64 KiB pages on Windows 16 times, and every varchar has the same length.
        final int scale = (int) (io.questdb.std.Files.PAGE_SIZE / 4096);
        setCountProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 1_000 * scale);
        final CountingFacade ff = new CountingFacade();
        ff.isCountingPageReleases = true;
        assertMemoryLeak(ff, () -> {
            Assert.assertEquals(commitMode, CommitMode.toString(engine.getConfiguration().getCommitMode()));
            Assert.assertEquals(io.questdb.std.Files.PAGE_SIZE, engine.getConfiguration().getWalDataAppendPageSize());
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG, vc VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (WalWriter writer = getWalWriter("x")) {
                ff.start(true);
                for (int i = 0; i < 1_500 * scale; i++) {
                    final TableWriter.Row row = writer.newRow((i + 1) * 1_000_000L);
                    row.putLong(1, i);
                    row.putVarchar(2, new Utf8String("varchar too long to inline " + (1_000_000 + i)));
                    row.append();
                    if (i % (300 * scale) == 300 * scale - 1) {
                        writer.commit();
                    }
                }
                ff.stop();
                Assert.assertEquals("the ingest must roll to a second segment", 1, writer.getSegmentId());
            }
            final String grade = switch (commitMode) {
                case "nosync" -> "noMsync";
                case "async" -> "msyncAsync";
                case "sync" -> "msyncSync";
                default -> Os.isLinux() ? "msyncAsync" : "msyncSync";
            };
            // 23 releases as the pages fill the two segments and 4 as the segment roll closes ts.d, v.d, vc.d and
            // vc.i: the same in every mode.
            Assert.assertEquals("column page releases [mode=" + commitMode + ']', grade + "=27", CountingFacade.join(ff.pageReleases));
        });
    }

    @Test
    public void testCommit() throws Exception {
        assertCalls(true, false);
    }

    @Test
    public void testDdl() throws Exception {
        Assume.assumeFalse("ADAPTIVE makes its own calls; only its file set must match base", "adaptive".equals(commitMode));
        setCountProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        setCountProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, isV2 ? 1000 : 0);
        final CountingFacade ff = new CountingFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES (1_000_000, 1)");
            drainWalQueue();
            for (int i = 0; i < WARMUP + MEASURED; i++) {
                ff.start(i >= WARMUP);
                execute("ALTER TABLE x ADD COLUMN c" + i + " INT");
                ff.stop();
                drainWalQueue();
            }
            final List<String> calls = new ArrayList<>();
            for (String summary : ff.summaries) {
                if (!calls.contains(summary)) {
                    calls.add(summary);
                }
            }
            Assert.assertEquals("DDLs measured", MEASURED, ff.summaries.size());
            // Base msyncs the txn log's metadata index and data, the txn log header and the sequencer _meta
            // twice, in every mode. SYNC also syncs the header after it publishes the change, and a V2 log the
            // part file holding the record, ahead of the header both times.
            final int msyncs = "sync".equals(commitMode) ? (isV2 ? 8 : 6) : 5;
            Assert.assertEquals(
                    "DDL calls differ from base [mode=" + commitMode + ", v2=" + isV2 + ']',
                    "calls: exists=2 msyncSync=" + msyncs + "; opened: ",
                    String.join("\n", calls)
            );
        });
    }

    @Test
    public void testOnlyBaseFiles() throws Exception {
        setCountProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        setCountProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, isV2 ? 1000 : 0);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG, s VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (int i = 0; i < 3; i++) {
                execute("INSERT INTO x VALUES (" + (i + 1) * 1_000_000L + ", " + i + ", 'a" + i + "')");
            }
            try (WalWriter writer = getWalWriter("x")) {
                appendRow(writer, 3, null);
                // A concurrent ADD COLUMN ... SYMBOL makes the commit rewrite its pending event.
                execute("ALTER TABLE x ADD COLUMN c SYMBOL");
                writer.commit();
            }
            execute("UPDATE x SET v = v + 1 WHERE v > 1");
            drainWalQueue();
            engine.releaseInactive();
            engine.getTableSequencerAPI().releaseAll();
            // Reopen the sequencer and a WAL writer: a file written only on open must show up too.
            execute("INSERT INTO x (ts, v) VALUES (5_000_000, 4)");
            drainWalQueue();
            engine.releaseInactive();
            engine.getTableSequencerAPI().releaseAll();

            final Path tableDir = Paths.get(root, engine.verifyTableName("x").getDirName());
            final Set<String> foreign = new TreeSet<>();
            try (Stream<Path> dirs = Files.list(tableDir)) {
                for (Path walDir : dirs.filter(d -> Files.isDirectory(d) && d.getFileName().toString().startsWith(WalUtils.WAL_NAME_BASE)).toList()) {
                    addForeignFiles(tableDir, walDir, SEGMENT_FILE, foreign);
                }
            }
            addForeignFiles(tableDir, tableDir.resolve(WalUtils.SEQ_DIR), SEQ_FILE, foreign);
            Assert.assertEquals("files base does not write [mode=" + commitMode + ", v2=" + isV2 + ']', "[]", foreign.toString());
        });
    }

    /**
     * The flushes of a Parquet partition. Base flushes its _pm in every mode but NOSYNC and never the data file;
     * only the power-loss-durable modes flush the data file ahead of _pm, so a durable _pm never names data still
     * in the page cache. A fresh partition flushes _pm in Java; an O3 merge into an existing one flushes it in
     * Rust, out of the facade's sight.
     */
    @Test
    public void testParquetPartitionFlushes() throws Exception {
        Assume.assumeFalse("the sequencer log format does not change the partition writes", isV2);
        setCountProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        final CountingFacade ff = new CountingFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 60_000_000L), x FROM long_sequence(10)");
            ff.startSyncLog();
            drainWalQueue();
            ff.stopSyncLog();
            final List<String> fresh = new ArrayList<>(ff.syncLog);
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:30.000000Z', 100)");
            ff.startSyncLog();
            drainWalQueue();
            ff.stopSyncLog();
            final List<String> merge = new ArrayList<>(ff.syncLog);
            // The enclosing leak check covers this query; at the production reader TTL its pooled reader outlives it.
            assertQuery("SELECT count() FROM x").noLeakCheck().noRandomAccess().expectSize().returns("count()\n11\n");
            final boolean flushesData = CommitMode.isPowerLossDurable(CommitMode.fromString(commitMode));
            Assert.assertEquals("a fresh Parquet partition's data file flush " + fresh,
                    flushesData, flushesOf(fresh, TableUtils.PARQUET_PARTITION_NAME) > 0);
            Assert.assertEquals("a fresh Parquet partition's _pm flush " + fresh,
                    !"nosync".equals(commitMode), flushesOf(fresh, TableUtils.PARQUET_METADATA_FILE_NAME) > 0);
            Assert.assertEquals("an O3 merge into a Parquet partition's data file flush " + merge,
                    flushesData, flushesOf(merge, TableUtils.PARQUET_PARTITION_NAME) > 0);
        });
    }

    /**
     * The sync calls of one periodic save point after its column flush: the live {@code _cv} and {@code _txn},
     * then the generation's one file twice (its sections, then its header, which publishes it). The generation
     * rewritten here already held a valid header, so its name is durable and the table directory is not synced.
     */
    @Test
    public void testPeriodicSavePointSyncs() throws Exception {
        assertPeriodicSavePointSyncs(new CountingFacade());
    }

    /**
     * {@link #testPeriodicSavePointSyncs} where syncfs flushes one file, as on macOS and Windows.
     */
    @Test
    public void testPeriodicSavePointSyncsWithoutFsWideSyncfs() throws Exception {
        assertPeriodicSavePointSyncs(new CountingFacade() {
            @Override
            public boolean isSyncfsFileSystemWide() {
                return false;
            }
        });
    }

    @Test
    public void testProductionDefaults() throws Exception {
        assertMemoryLeak(() -> {
            final var cfg = engine.getConfiguration();
            Assert.assertEquals(30, cfg.getWalMaxSegmentFileDescriptorsCache());
            Assert.assertEquals(1000, cfg.getWalApplyTableTimeQuota());
            Assert.assertEquals(200, cfg.getWalApplyLookAheadTransactionCount());
            Assert.assertEquals(-1, cfg.getWalMaxLagTxnCount());
            Assert.assertEquals(300000, cfg.getIdleCheckInterval());
            Assert.assertEquals(120000, cfg.getInactiveReaderTTL());
            Assert.assertEquals(600000, cfg.getInactiveWriterTTL());
            Assert.assertEquals(120000, cfg.getInactiveWalWriterTTL());
            Assert.assertEquals(1024, cfg.getMetadataPoolCapacity());
            Assert.assertEquals(1, cfg.getPartitionPurgeListCapacity());
            Assert.assertEquals(50000, cfg.getAdaptiveCommitGroupWindowUs());
            Assert.assertEquals(5000000, cfg.getAdaptiveEpochMaxRows());
            Assert.assertEquals(0, cfg.getDefaultSeqPartTxnCount());
            Assert.assertEquals(60000, cfg.getAdaptiveEpochIntervalMs());
            Assert.assertEquals(200000, cfg.getWalSegmentRolloverRowCount());
        });
    }

    private static void addForeignFiles(Path tableDir, Path dir, Pattern baseFile, Set<String> foreign) throws IOException {
        try (Stream<Path> files = Files.walk(dir)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                if (!baseFile.matcher(file.getFileName().toString()).matches()) {
                    foreign.add(tableDir.relativize(file).toString());
                }
            }
        }
    }

    private static void appendRow(WalWriter writer, long i, @Nullable DirectArray array) {
        final TableWriter.Row row = writer.newRow((i + 1) * 1_000_000L);
        row.putLong(1, i);
        if (array != null) {
            row.putStr(2, "string " + i);
            row.putVarchar(3, new Utf8String("varchar too long to inline " + i));
            row.putArray(4, array);
        }
        row.append();
    }

    private static CairoConfiguration createProductionConfiguration(CharSequence dbRoot) {
        final Properties properties = new Properties();
        properties.putAll(COUNT_PROPERTIES);
        properties.setProperty(PropertyKey.CAIRO_ROOT.getPropertyPath(), dbRoot.toString());
        try {
            // Exclude host QDB_* overrides and unrelated HTTP MIME loading, not Cairo defaults.
            final PropServerConfiguration server = new PropServerConfiguration(
                    Paths.get(dbRoot.toString()).getParent().toString(), properties, null, Map.of(), LOG,
                    new BuildInformationHolder(), FilesFacadeImpl.INSTANCE, MicrosecondClockImpl.INSTANCE,
                    (_, _, _) -> DefaultFactoryProvider.INSTANCE, false);
            server.reinit(DefaultFactoryProvider.INSTANCE);
            return server.getCairoConfiguration();
        } catch (ServerConfigurationException | JsonException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void setCountProperty(PropertyKey key, Object value) {
        COUNT_PROPERTIES.setProperty(key.getPropertyPath(), String.valueOf(value));
        countConfiguration.setDelegate(createProductionConfiguration(root));
    }

    private static int flushesOf(List<String> syncLog, String file) {
        int n = 0;
        for (String entry : syncLog) {
            if (entry.endsWith(" " + file)) {
                n++;
            }
        }
        return n;
    }

    private static String expected(String commitMode, boolean isV2, boolean isCommit, boolean isVarSize, boolean isMixedIO) {
        if (isCommit) {
            // A V2 commit in SYNC msyncs the open part file ahead of the header: one more than base, which
            // syncs the header alone and leaves the record it publishes to writeback.
            return switch (commitMode) {
                case "async" -> "calls: msyncAsync=5; opened: ";
                case "sync" -> "calls: msyncSync=" + (isV2 ? 6 : 5) + "; opened: ";
                default -> "calls: ; opened: ";
            };
        }
        // The var-size columns add six files to open, close and sync, and three maps; base reads none of them.
        final int files = isVarSize ? 6 : 0;
        final String msyncs = switch (commitMode) {
            case "async" -> " msyncAsync=" + (3 + files);
            case "sync" -> " msyncSync=" + (3 + files);
            default -> "";
        };
        // Without mixed I/O, copying a WAL column maps its lazy source instead of copying through file descriptors.
        // Destination pages are already warm; source WAL v.d and the three variable-size columns need more maps.
        final int maps = 3 + files / 2 + (isMixedIO ? 0 : 1 + files / 2);
        // The event reader asks for random access, and maps _event outside the mmap cache for it. Off Linux the
        // OS takes no advice (POSIX_MADV_RANDOM is -1), and it maps _event through the cache like the rest.
        final String eventMaps = io.questdb.std.Files.POSIX_MADV_RANDOM != -1 ? maps + " mmapNoCache=1" : String.valueOf(maps + 1);
        // A V2 apply pass also opens the sequencer part file "0".
        return "calls: close=" + ((isV2 ? 11 : 10) + 2 * files) + " madvise=3 mmap=" + eventMaps
                + " mremap=1" + msyncs + " munmap=" + (maps + 1) + " openRO=" + ((isV2 ? 6 : 5) + files)
                + " readNonNegativeLong=3; opened: " + (isV2 ? "0=1 " : "") + "_event=1 _event.i=1 _txnlog=1 "
                + (isVarSize ? "a.d=1 a.i=1 s.d=1 s.i=1 ts.d=1 v.d=1 vc.d=1 vc.i=1" : "ts.d=1 v.d=1");
    }

    private void assertCalls(boolean isCommit, boolean isVarSize) throws Exception {
        Assume.assumeFalse("ADAPTIVE makes its own calls; only its file set must match base", "adaptive".equals(commitMode));
        setCountProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        setCountProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, isV2 ? 1000 : 0);
        final CountingFacade ff = new CountingFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG" + (isVarSize ? ", s STRING, vc VARCHAR, a DOUBLE[]" : "")
                    + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("x");
            final List<String> calls = new ArrayList<>();
            try (
                    ApplyWal2TableJob job = new ApplyWal2TableJob(engine, 0);
                    WalWriter writer = getWalWriter("x");
                    DirectArray array = isVarSize ? new DirectArray() : null
            ) {
                if (array != null) {
                    array.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
                    array.setDimLen(0, 2);
                    array.applyShape();
                    array.putDouble(0, 1.5);
                    array.putDouble(1, 2.5);
                }
                for (int i = 0; i < WARMUP + MEASURED; i++) {
                    appendRow(writer, i, array);
                    ff.start(isCommit && i >= WARMUP);
                    writer.commit();
                    ff.start(!isCommit && i >= WARMUP);
                    job.drain(0);
                    ff.stop();
                    Assert.assertEquals(
                            "each pass applies the txn just committed",
                            engine.getTableSequencerAPI().lastTxn(token),
                            engine.getTableSequencerAPI().getTxnTracker(token).getWriterTxn()
                    );
                }
            }
            for (String summary : ff.summaries) {
                if (!calls.contains(summary)) {
                    calls.add(summary);
                }
            }
            Assert.assertEquals("passes measured", MEASURED, ff.summaries.size());
            final String actual = String.join("\n", calls);
            Assert.assertEquals(
                    (isCommit ? "commit" : "apply pass") + " calls differ from base [mode=" + commitMode + ", v2=" + isV2
                            + ", varSize=" + isVarSize + ']',
                    expected(commitMode, isV2, isCommit, isVarSize, configuration.isWriterMixedIOEnabled()),
                    actual
            );
        });
    }

    private void assertPeriodicSavePointSyncs(CountingFacade ff) throws Exception {
        Assume.assumeTrue("only ADAPTIVE takes save points", "adaptive".equals(commitMode));
        Assume.assumeFalse("Windows syncs no directory", Os.isWindows());
        setCountProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        setCountProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, isV2 ? 1000 : 0);
        setCountProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        assertMemoryLeak(ff, () -> {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES (1_000_000, 1)");
            drainWalQueue();
            try (TableWriter writer = getWriter("x")) {
                // A writer's first save point also flushes every partition; the test counts the next one.
                writer.advanceDurableEpoch(1L);
                ff.startSyncLog();
                writer.advanceDurableEpoch(2L);
                ff.stopSyncLog();
            }
            final List<String> log = ff.syncLog;
            int columnFlushEnd = -1;
            for (int i = 0, n = log.size(); i < n; i++) {
                if (log.get(i).startsWith("syncfs")) {
                    columnFlushEnd = i;
                }
            }
            Assert.assertTrue("the column flush ends with syncfs: " + log, columnFlushEnd > -1);
            // Where syncfs flushes one file, the save point makes the sequencer files durable itself.
            final String sequencerSyncs = ff.isSyncfsFileSystemWide() ? "" : (isV2 ? "fdatasync 0\n" : "") + "fdatasync _txnlog\n";
            Assert.assertEquals(
                    "sync calls after the column flush",
                    """
                            msyncSync
                            fsync _cv
                            msyncSync
                            fsync _txn
                            """ + sequencerSyncs + """
                            fsync _epoch.manifest.0
                            fsync _epoch.manifest.0""",
                    String.join("\n", log.subList(columnFlushEnd + 1, log.size()))
            );
        });
    }

    /**
     * Counts the file system calls made on the measuring thread while armed, and the names of the files
     * opened.
     */
    private static class CountingFacade extends TestFilesFacadeImpl {
        private final Map<String, int[]> calls = new TreeMap<>();
        // The mappings of WAL column files and the files' fds, to tell a column page release (an msync, then
        // the munmap of the same address) from the other unmaps.
        private final Map<Long, String> columnFileByAddr = new HashMap<>();
        private final Map<Long, String> columnFileByFd = new HashMap<>();
        private final Map<Long, String> fdNames = new HashMap<>();
        private final Map<String, int[]> opened = new TreeMap<>();
        private final Map<String, int[]> pageReleases = new TreeMap<>();
        private final List<String> summaries = new ArrayList<>();
        private final List<String> syncLog = new ArrayList<>();
        private boolean isCountingPageReleases;
        private long lastMsyncAddr;
        private boolean lastMsyncAsync;
        private Thread measured;
        private Thread syncLogged;

        @Override
        public boolean allocate(long fd, long size) {
            count("allocate");
            return super.allocate(fd, size);
        }

        @Override
        public boolean allocateRange(long fd, long offset, long size) {
            count("allocateRange");
            return super.allocateRange(fd, offset, size);
        }

        @Override
        public void barrierFsync(long fd) {
            count("barrierFsync");
            super.barrierFsync(fd);
        }

        @Override
        public boolean close(long fd) {
            count("close");
            columnFileByFd.remove(fd);
            fdNames.remove(fd);
            return super.close(fd);
        }

        @Override
        public boolean exists(LPSZ path) {
            count("exists");
            return super.exists(path);
        }

        @Override
        public void fdatasync(long fd) {
            count("fdatasync");
            logSync("fdatasync", fd);
            super.fdatasync(fd);
        }

        @Override
        public void fsync(long fd) {
            count("fsync");
            logSync("fsync", fd);
            super.fsync(fd);
        }

        @Override
        public void fsyncAndClose(long fd) {
            count("fsyncAndClose");
            logSync("fsyncAndClose", fd);
            fdNames.remove(fd);
            super.fsyncAndClose(fd);
        }

        @Override
        public void fsyncDurable(long fd) {
            count("fsyncDurable");
            // The superclass delegates to fsync, which logs the actual flush once.
            super.fsyncDurable(fd);
        }

        @Override
        public long length(long fd) {
            count("length");
            return super.length(fd);
        }

        @Override
        public long length(LPSZ name) {
            count("lengthByName");
            return super.length(name);
        }

        @Override
        public void madvise(long address, long len, int advise) {
            count("madvise");
            super.madvise(address, len, advise);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            count("mmap");
            final long addr = super.mmap(fd, len, offset, flags, memoryTag);
            final String columnFile = columnFileByFd.get(fd);
            if (columnFile != null) {
                columnFileByAddr.put(addr, columnFile);
            }
            return addr;
        }

        @Override
        public long mmapNoCache(long fd, long len, long offset, int flags, int memoryTag) {
            count("mmapNoCache");
            return super.mmapNoCache(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            count("mremap");
            return super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        }

        @Override
        public long mremapNoCache(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            count("mremapNoCache");
            return super.mremapNoCache(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            count(async ? "msyncAsync" : "msyncSync");
            lastMsyncAddr = addr;
            lastMsyncAsync = async;
            if (!async && Thread.currentThread() == syncLogged) {
                syncLog.add("msyncSync");
            }
            super.msync(addr, len, async);
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            count("munmap");
            if (columnFileByAddr.remove(address) != null && Thread.currentThread() == measured) {
                increment(pageReleases, address != lastMsyncAddr ? "noMsync" : lastMsyncAsync ? "msyncAsync" : "msyncSync");
            }
            lastMsyncAddr = 0;
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openCleanRW(LPSZ name, long size) {
            countOpen("openCleanRW", name);
            return named(super.openCleanRW(name, size), name);
        }

        @Override
        public long openRO(LPSZ name) {
            countOpen("openRO", name);
            return named(super.openRO(name), name);
        }

        @Override
        public long openRONoCache(LPSZ path) {
            countOpen("openRONoCache", path);
            return named(super.openRONoCache(path), path);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            countOpen("openRW", name);
            final long fd = named(super.openRW(name, opts), name);
            if (isCountingPageReleases && fd > -1) {
                final String file = fileName(name);
                if (COLUMN_FILE.matcher(file).matches()) {
                    columnFileByFd.put(fd, file);
                }
            }
            return fd;
        }

        @Override
        public long openRWNoCache(LPSZ name, int opts) {
            countOpen("openRWNoCache", name);
            return named(super.openRWNoCache(name, opts), name);
        }

        @Override
        public long read(long fd, long buf, long size, long offset) {
            count("read");
            return super.read(fd, buf, size, offset);
        }

        @Override
        public long readIntAsUnsignedLong(long fd, long offset) {
            count("readIntAsUnsignedLong");
            return super.readIntAsUnsignedLong(fd, offset);
        }

        @Override
        public byte readNonNegativeByte(long fd, long offset) {
            count("readNonNegativeByte");
            return super.readNonNegativeByte(fd, offset);
        }

        @Override
        public int readNonNegativeInt(long fd, long offset) {
            count("readNonNegativeInt");
            return super.readNonNegativeInt(fd, offset);
        }

        @Override
        public long readNonNegativeLong(long fd, long offset) {
            count("readNonNegativeLong");
            return super.readNonNegativeLong(fd, offset);
        }

        @Override
        public int syncFileRange(long fd, long offset, long nbytes, int flags) {
            count("syncFileRange");
            return super.syncFileRange(fd, offset, nbytes, flags);
        }

        @Override
        public void syncfs(long fd) {
            count("syncfs");
            logSync("syncfs", fd);
            super.syncfs(fd);
        }

        @Override
        public boolean truncate(long fd, long size) {
            count("truncate");
            return super.truncate(fd, size);
        }

        @Override
        public long write(long fd, long address, long len, long offset) {
            count("write");
            return super.write(fd, address, len, offset);
        }

        private static String fileName(LPSZ name) {
            final StringBuilder file = new StringBuilder();
            for (int i = 0, n = name.size(); i < n; i++) {
                final char c = (char) (name.byteAt(i) & 0xFF);
                if (c == '/' || c == '\\') {
                    if (i < n - 1) {
                        file.setLength(0);
                    }
                } else {
                    file.append(c);
                }
            }
            return file.toString();
        }

        private static void increment(Map<String, int[]> counts, String key) {
            counts.computeIfAbsent(key, k -> new int[1])[0]++;
        }

        private static String join(Map<String, int[]> counts) {
            final StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, int[]> e : counts.entrySet()) {
                if (!sb.isEmpty()) {
                    sb.append(' ');
                }
                sb.append(e.getKey()).append('=').append(e.getValue()[0]);
            }
            return sb.toString();
        }

        private void assertConfiguration() {
            final var cfg = engine.getConfiguration();
            Assert.assertSame(this, cfg.getFilesFacade());
            Assert.assertEquals(30, cfg.getWalMaxSegmentFileDescriptorsCache());
            Assert.assertEquals(COUNT_PROPERTIES.getProperty(PropertyKey.CAIRO_COMMIT_MODE.getPropertyPath()), CommitMode.toString(cfg.getCommitMode()));
            Assert.assertEquals(Integer.parseInt(COUNT_PROPERTIES.getProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT.getPropertyPath(), "0")), cfg.getDefaultSeqPartTxnCount());
        }

        private void count(String call) {
            if (Thread.currentThread() == measured) {
                increment(calls, call);
            }
        }

        private void countOpen(String call, LPSZ name) {
            if (Thread.currentThread() == measured) {
                increment(calls, call);
                increment(opened, fileName(name));
            }
        }

        private void logSync(String call, long fd) {
            if (Thread.currentThread() == syncLogged) {
                syncLog.add(call + ' ' + fdNames.get(fd));
            }
        }

        private long named(long fd, LPSZ name) {
            if (fd > -1) {
                fdNames.put(fd, fileName(name));
            }
            return fd;
        }

        private void start(boolean isArmed) {
            assertConfiguration();
            stop();
            measured = isArmed ? Thread.currentThread() : null;
        }

        private void startSyncLog() {
            assertConfiguration();
            syncLog.clear();
            syncLogged = Thread.currentThread();
        }

        private void stop() {
            if (measured != null) {
                summaries.add("calls: " + join(calls) + "; opened: " + join(opened));
                calls.clear();
                opened.clear();
                measured = null;
            }
        }

        private void stopSyncLog() {
            syncLogged = null;
        }
    }
}
