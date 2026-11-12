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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SymbolCountProvider;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.*;

/**
 * The fast-path {@code _txn} commit assembles its body checksum from a cached partition-table hash instead of
 * hashing every partition. These tests pin that the cached value is always the one a full recomputation over
 * the written bytes gives, across every kind of commit and writer lifecycle event, that the fast path hashes
 * no partition table at all, that a full-record commit hashes only the partition-table entries it changes, and
 * that a torn partition table under a fast-path record is still detected.
 * <p>
 * {@code TxWriter} also checks every cached hash against a full recomputation, with assertions enabled, as the
 * test JVM runs, wherever it drops them: on close, reopen, rollback and reload.
 */
public class TxnBodyChecksumCacheTest extends AbstractCairoTest {
    private static final long DAY_2024_01_01 = 1_704_067_200_000_000L;
    private static final Log LOG = LogFactory.getLog(TxnBodyChecksumCacheTest.class);

    @Test
    public void testClearPartitionStampFlagsKeepsChecksumExact() throws Exception {
        // Snapshot restore clears REMOTE and the seal stamp in the live record, with no commit, and the next open
        // verifies that record. The write lands in the partition table, so the record's checksum must follow it,
        // and so must a cached hash of its area, which the writer's later commits reuse.
        assertMemoryLeak(() -> {
            final TableToken token = createHourTable("stamps");
            final ObjList<SymbolCountProvider> symbols = new ObjList<>();
            try (Path path = txnPath(token)) {
                try (TxWriter writer = new TxWriter(configuration.getFilesFacade(), configuration)) {
                    writer.ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.HOUR);
                    writer.setCommitMode(CommitMode.NOSYNC);
                    populate(writer, symbols);
                    for (int i = 0; i < 4; i++) {
                        writer.setPartitionRemote(i, true);
                        writer.setPartitionSealStamped(i, true);
                    }
                    writer.bumpPartitionTableVersion();
                    writer.commit(symbols);
                    // Both slots' hashes cached.
                    appendAndCommit(writer, symbols);
                    appendAndCommit(writer, symbols);

                    writer.clearPartitionStampFlagsUnsafe(0, true);
                    writer.clearPartitionStampFlagsUnsafe(1, false);
                    assertAreasVerify(path.$());
                    assertFreshReaderVerifies(path.$(), writer.getTxn());
                    // The commits that reuse the cached hash of the area, and the close, which checks it.
                    for (int i = 0; i < 2; i++) {
                        appendAndCommit(writer, symbols);
                        assertAreasVerify(path.$());
                    }
                }

                // As the restore runs it: a writer opened for the clear and closed after it.
                try (TxWriter writer = new TxWriter(configuration.getFilesFacade(), configuration)) {
                    writer.ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.HOUR);
                    writer.clearPartitionStampFlagsUnsafe(2, true);
                    writer.clearPartitionStampFlagsUnsafe(3, false);
                }
                assertAreasVerify(path.$());
                try (TxReader reader = new TxReader(configuration.getFilesFacade())) {
                    reader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.HOUR);
                    TxReader.resetBodyChecksumFallbackCount();
                    Assert.assertTrue(reader.unsafeLoadAll());
                    Assert.assertEquals(0, TxReader.getBodyChecksumFallbackCount());
                    for (int i = 0; i < 4; i++) {
                        Assert.assertFalse("REMOTE of partition " + i, reader.isPartitionRemote(i));
                        Assert.assertEquals("seal stamp of partition " + i, i == 1 || i == 3, reader.isPartitionSealStamped(i));
                    }
                }
            }
        });
    }

    @Test
    public void testCombinedChecksumMatchesFullChecksumForEveryGeometry() {
        // The split must reproduce calculateTxnBodyChecksum() bit for bit for every partition-table length
        // remainder (0-7 bytes past a whole word), for an empty or inverted partition-table range, and for
        // every symbol-count offset. The term-by-term reference pins the on-disk value independently.
        final long capacity = 1024;
        final long addr = Unsafe.malloc(capacity, MemoryTag.NATIVE_DEFAULT);
        try {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (int round = 0; round < 20; round++) {
                for (long i = 0; i < capacity; i += Long.BYTES) {
                    Unsafe.putLong(addr + i, rnd.nextLong());
                }
                for (int symbolCount = 0; symbolCount < 4; symbolCount++) {
                    final long partitionTableStart = getPartitionTableSizeOffset(symbolCount);
                    for (long recordSize = partitionTableStart - 9; recordSize <= partitionTableStart + 300; recordSize++) {
                        final long expected = referenceChecksum(addr, recordSize, partitionTableStart);
                        Assert.assertEquals(expected, calculateTxnBodyChecksum(addr, recordSize, partitionTableStart));
                        Assert.assertEquals(
                                "recordSize=" + recordSize + ", partitionTableStart=" + partitionTableStart,
                                expected,
                                combineTxnBodyChecksum(
                                        hashTxnBodyHeader(addr),
                                        txnBodyPartitionTablePower(recordSize, partitionTableStart),
                                        hashTxnBodyPartitionTable(addr, recordSize, partitionTableStart)
                                )
                        );
                    }
                }
            }
        } finally {
            Unsafe.free(addr, capacity, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testFastPathCommitHashesNoPartitionTable() throws Exception {
        // The fast path's cost must not depend on the partition count: after the two full-record commits that
        // arm it, a single-row append commit hashes no partition-table term at all, at 10 partitions or 1,000.
        // The count includes every partition-table hash, whoever computes it, so a full recomputation on this
        // path, even one only assertions run and one that bypasses TxWriter, fails it.
        assertMemoryLeak(() -> {
            for (int partitions : new int[]{10, 1_000}) {
                final String tableName = "x" + partitions;
                execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL");
                execute("INSERT INTO " + tableName + " SELECT timestamp_sequence(0, " + Micros.HOUR_MICROS + ") ts, x v FROM long_sequence(" + partitions + ")");
                long ts = (partitions - 1) * Micros.HOUR_MICROS;
                try (TableWriter writer = getWriter(tableName)) {
                    for (int i = 0; i < 3; i++) {
                        appendRow(writer, ++ts, i);
                    }
                    final long terms = getTxnBodyPartitionTableHashTermCount();
                    for (int i = 0; i < 100; i++) {
                        appendRow(writer, ++ts, i);
                    }
                    Assert.assertEquals("fast-path commits must not hash the partition table", terms, getTxnBodyPartitionTableHashTermCount());

                    // A partition switch is a full-record commit with a longer partition table, which it hashes
                    // once.
                    appendRow(writer, partitions * Micros.HOUR_MICROS, 0);
                    Assert.assertEquals(terms + fullHashTerms(partitions + 1), getTxnBodyPartitionTableHashTermCount());
                }
                assertTxnVerifies(tableName, PartitionBy.HOUR);
                assertQuery("SELECT count() FROM " + tableName).noLeakCheck().expectSize().noRandomAccess().returns("count\n" + (partitions + 104) + "\n");
            }
        });
    }

    @Test
    public void testFastPathOverrunIntoPartitionTableKeepsChecksumExact() throws Exception {
        // Neither of these happens on a healthy writer, and both write a partition-table byte of an area whose
        // hash is cached: a fast-path commit given more symbol counts than the record holds, and an in-place
        // transient symbol count one past the record's symbols. Whatever lands there, the stored checksum must
        // stay the one a full recomputation over the bytes gives.
        assertMemoryLeak(() -> {
            final TableToken token = createHourTable("overrun");
            final ObjList<SymbolCountProvider> symbols = new ObjList<>();
            symbols.add(new MutableSymbolCount(3));
            try (Path path = txnPath(token); TxWriter writer = new TxWriter(configuration.getFilesFacade(), configuration)) {
                writer.ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.HOUR);
                writer.setCommitMode(CommitMode.NOSYNC);
                populate(writer, symbols);
                appendAndCommit(writer, symbols);
                appendAndCommit(writer, symbols);

                final ObjList<SymbolCountProvider> tooMany = new ObjList<>();
                tooMany.add(new MutableSymbolCount(3));
                tooMany.add(new MutableSymbolCount(0x5a5a5a5a));
                writer.append();
                writer.updateMaxTimestamp(writer.getMaxTimestamp() + 1);
                final long termsBeforeOverrun = getTxnBodyPartitionTableHashTermCount();
                writer.commit(tooMany);
                Assert.assertEquals(termsBeforeOverrun + fullHashTerms(8), getTxnBodyPartitionTableHashTermCount());
                assertAreasVerify(path.$());
                appendAndCommit(writer, symbols);
                appendAndCommit(writer, symbols);
                assertAreasVerify(path.$());

                // This one changes a covered byte of the published live record, whose own checksum then
                // mismatches until its area is republished two commits later. The republishing commit must hash
                // the bytes in place rather than reuse the entry cached before the change.
                final long terms = getTxnBodyPartitionTableHashTermCount();
                writer.collectValueCount(symbols.size(), 0x3c3c3c3c);
                appendAndCommit(writer, symbols);
                appendAndCommit(writer, symbols);
                Assert.assertEquals(terms + fullHashTerms(8), getTxnBodyPartitionTableHashTermCount());
                assertAreasVerify(path.$());
                for (int i = 0; i < 2; i++) {
                    appendAndCommit(writer, symbols);
                    assertAreasVerify(path.$());
                }
            }
        });
    }

    @Test
    public void testPartitionTableHashCountPerCommitKind() throws Exception {
        // Pins, in hash terms, when each A/B slot's cached hash is filled, updated and reused: a full-record
        // commit hashes the whole table it writes when its slot holds no entry for that geometry and otherwise
        // folds in only the ints it changes, a fast-path commit reuses the entry of the slot it republishes (the
        // two slots alternate and are independent), and reload/truncate/reopen drop entries rather than trust
        // them.
        assertMemoryLeak(() -> {
            final TableToken token = createHourTable("per_kind");
            final ObjList<SymbolCountProvider> symbols = new ObjList<>();
            symbols.add(new MutableSymbolCount(1));
            try (Path path = txnPath(token); TxWriter writer = new TxWriter(configuration.getFilesFacade(), configuration)) {
                writer.ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.HOUR);
                writer.setCommitMode(CommitMode.NOSYNC);
                populate(writer, symbols);

                // A record-structure change: two full-record commits, then the fast path alternates A/B.
                writer.bumpPartitionTableVersion();
                assertHashesPerCommit(writer, path, symbols, fullHashTerms(8), fullHashTerms(8), 0, 0, 0, 0);

                // New symbol values change only excluded bytes: in place in the live area, and in the counts
                // the fast path rewrites.
                ((MutableSymbolCount) symbols.getQuick(0)).count = 7;
                writer.collectValueCount(0, 7);
                assertHashesPerCommit(writer, path, symbols, 0, 0);

                // An out-of-order size update rewrites the partition table with the same geometry: each of the
                // two full-record commits folds in only the low int of that partition's size, the one int that
                // differs from its slot's bytes.
                writer.updatePartitionSizeByTimestamp(3 * Micros.HOUR_MICROS, writer.getPartitionSize(3) + 1);
                assertHashesPerCommit(writer, path, symbols, 1, 1, 0, 0);

                // Rollback reloads the writer from disk and drops both entries; the fast path then re-hashes each
                // slot once and reuses it from then on.
                writer.append();
                Assert.assertTrue(writer.unsafeLoadAll());
                assertHashesPerCommit(writer, path, symbols, fullHashTerms(8), fullHashTerms(8), 0, 0);

                // Truncate writes a whole record, then the table grows back through full-record commits.
                writer.truncate(writer.getColumnVersion(), symbols);
                assertAreasVerify(path.$());
                final long ts = 20 * Micros.HOUR_MICROS;
                writer.setMinTimestamp(ts);
                writer.initLastPartition(ts);
                writer.append();
                writer.updateMaxTimestamp(ts);
                assertHashesPerCommit(writer, path, symbols, fullHashTerms(1), fullHashTerms(1), 0, 0);

                // Reopen, also as a TxWriter reused for the same file.
                writer.close();
                writer.ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.HOUR);
                writer.setCommitMode(CommitMode.NOSYNC);
                assertHashesPerCommit(writer, path, symbols, fullHashTerms(1), fullHashTerms(1), 0, 0);
                writer.ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.HOUR);
                writer.setCommitMode(CommitMode.NOSYNC);
                assertHashesPerCommit(writer, path, symbols, fullHashTerms(1), fullHashTerms(1), 0, 0);
            }
        });
    }

    @Test
    public void testPartitionTableIntDeltaMatchesFullHash() {
        // A full-record commit folds each partition-table int it changes into the cached hash instead of hashing
        // the table again. Random edits of the size int and of partition longs -- small steps, flag bits, the
        // top bit, and the last long, whose high int is the sign-extended trailing term -- at every table start
        // alignment must keep the folded hash equal to a full hash of the bytes.
        final int maxLongCount = 64;
        final long capacity = 16 + Long.BYTES + (long) maxLongCount * Long.BYTES;
        final long addr = Unsafe.malloc(capacity, MemoryTag.NATIVE_DEFAULT);
        try {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (int round = 0; round < 1_000; round++) {
                for (long i = 0; i < capacity; i += Long.BYTES) {
                    Unsafe.putLong(addr + i, rnd.nextLong());
                }
                final int longCount = rnd.nextInt(maxLongCount + 1);
                final long start = rnd.nextInt(16);
                final long end = start + Integer.BYTES + (long) longCount * Long.BYTES;
                Unsafe.putInt(addr + start, longCount * Long.BYTES);
                long hash = hashTxnBodyPartitionTable(addr, end, start);
                for (int edit = 0; edit < 20; edit++) {
                    if (longCount == 0 || rnd.nextInt(10) == 0) {
                        final int oldSize = Unsafe.getInt(addr + start);
                        final int newSize = rnd.nextInt();
                        Unsafe.putInt(addr + start, newSize);
                        hash += txnBodyPartitionTableIntDelta(longCount, 0, oldSize, newSize);
                    } else {
                        final int index = rnd.nextBoolean() ? longCount - 1 : rnd.nextInt(longCount);
                        final long offset = Integer.BYTES + (long) index * Long.BYTES;
                        final long oldValue = Unsafe.getLong(addr + start + offset);
                        final long newValue = switch (rnd.nextInt(4)) {
                            case 0 -> oldValue + 1;
                            case 1 -> oldValue ^ (1L << rnd.nextInt(Long.SIZE));
                            case 2 -> oldValue | Long.MIN_VALUE;
                            default -> rnd.nextLong();
                        };
                        Unsafe.putLong(addr + start + offset, newValue);
                        hash += txnBodyPartitionTableIntDelta(longCount, offset, (int) oldValue, (int) newValue);
                        hash += txnBodyPartitionTableIntDelta(longCount, offset + Integer.BYTES, (int) (oldValue >>> 32), (int) (newValue >>> 32));
                    }
                    Assert.assertEquals("round " + round + ", edit " + edit, hashTxnBodyPartitionTable(addr, end, start), hash);
                }
            }
        } finally {
            Unsafe.free(addr, capacity, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRandomCommitSequencesKeepChecksumExact() throws Exception {
        // Random interleavings of every TxWriter mutation that reaches _txn -- appends on the fast path,
        // partition switches, out-of-order size updates, drop and attach, partition-table and column version
        // bumps, new symbol values and symbol columns, truncate, rollback, reopen and reuse on another table --
        // with every A/B area on disk checked against a full recomputation after each commit.
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final FilesFacade ff = configuration.getFilesFacade();
            final ObjList<TableToken> tokens = new ObjList<>();
            tokens.add(createHourTable("rnd_a"));
            tokens.add(createHourTable("rnd_b"));
            final ObjList<ObjList<SymbolCountProvider>> tableSymbols = new ObjList<>();
            tableSymbols.add(new ObjList<>());
            tableSymbols.add(new ObjList<>());
            int current = 0;
            int commits = 0;
            int cachedCommits = 0;
            try (Path path = txnPath(tokens.getQuick(current)); TxWriter writer = new TxWriter(ff, configuration)) {
                for (int t = 1; t > -1; t--) {
                    path.of(configuration.getDbRoot()).concat(tokens.getQuick(t)).concat(TXN_FILE_NAME);
                    writer.ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.HOUR);
                    writer.setCommitMode(CommitMode.NOSYNC);
                    populate(writer, tableSymbols.getQuick(t));
                }
                ObjList<SymbolCountProvider> symbols = tableSymbols.getQuick(current);

                for (int step = 0; step < 1_500; step++) {
                    final long terms = getTxnBodyPartitionTableHashTermCount();
                    final int op = rnd.nextInt(20);
                    switch (op) {
                        case 0 -> {
                            // Partition switch.
                            final long ts = writer.getLastPartitionTimestamp() + Micros.HOUR_MICROS;
                            writer.switchPartitions(ts);
                            writer.append();
                            writer.updateMaxTimestamp(ts);
                            writer.commit(symbols);
                        }
                        case 1 -> {
                            // Out-of-order write into an earlier partition.
                            if (writer.getPartitionCount() > 1) {
                                final int index = rnd.nextInt(writer.getPartitionCount() - 1);
                                writer.updatePartitionSizeByTimestamp(writer.getPartitionTimestampByIndex(index), writer.getPartitionSize(index) + 1);
                            }
                            writer.commit(symbols);
                        }
                        case 2 -> {
                            // Drop an earlier partition.
                            if (writer.getPartitionCount() > 2) {
                                writer.removeAttachedPartitions(writer.getPartitionTimestampByIndex(rnd.nextInt(writer.getPartitionCount() - 1)));
                            }
                            writer.commit(symbols);
                        }
                        case 3 -> {
                            // Attach a partition into the first gap, if any.
                            for (long ts = writer.getPartitionTimestampByIndex(0); ts < writer.getLastPartitionTimestamp(); ts += Micros.HOUR_MICROS) {
                                if (!writer.attachedPartitionsContains(ts)) {
                                    writer.updatePartitionSizeByTimestamp(ts, 1 + rnd.nextInt(100));
                                    break;
                                }
                            }
                            writer.commit(symbols);
                        }
                        case 4 -> {
                            writer.bumpPartitionTableVersion();
                            writer.commit(symbols);
                        }
                        case 5 -> {
                            writer.setColumnVersion(writer.getColumnVersion() + 1);
                            writer.commit(symbols);
                        }
                        case 6 -> {
                            // A new symbol column: a structure-version commit with a longer symbol region, which
                            // moves the partition table.
                            if (symbols.size() < 5) {
                                symbols.add(new MutableSymbolCount(0));
                            }
                            writer.bumpMetadataAndColumnStructureVersion(symbols);
                        }
                        case 7 -> {
                            // New symbol values: in place in the live area, then in the next commit's counts.
                            if (symbols.size() > 0) {
                                final int index = rnd.nextInt(symbols.size());
                                final MutableSymbolCount count = (MutableSymbolCount) symbols.getQuick(index);
                                count.count += 1 + rnd.nextInt(3);
                                writer.collectValueCount(index, count.count);
                            }
                            appendAndCommit(writer, symbols);
                        }
                        case 8 -> {
                            if (rnd.nextInt(8) == 0) {
                                writer.truncate(writer.getColumnVersion(), symbols);
                                assertAreasVerify(path.$());
                                final long ts = (1_000 + step) * Micros.HOUR_MICROS;
                                writer.setMinTimestamp(ts);
                                writer.initLastPartition(ts);
                                writer.append();
                                writer.updateMaxTimestamp(ts);
                            }
                            writer.commit(symbols);
                        }
                        case 9 -> {
                            // Rollback of uncommitted appends, sometimes across a partition switch.
                            writer.append();
                            writer.updateMaxTimestamp(writer.getMaxTimestamp() + 1);
                            if (rnd.nextBoolean()) {
                                final long ts = writer.getLastPartitionTimestamp() + Micros.HOUR_MICROS;
                                writer.switchPartitions(ts);
                                writer.append();
                                writer.updateMaxTimestamp(ts);
                            }
                            Assert.assertTrue(writer.unsafeLoadAll());
                            appendAndCommit(writer, symbols);
                        }
                        case 10 -> {
                            // Reopen the same table, or reuse the writer for the other one.
                            if (rnd.nextBoolean()) {
                                writer.close();
                            } else {
                                current = 1 - current;
                                symbols = tableSymbols.getQuick(current);
                            }
                            path.of(configuration.getDbRoot()).concat(tokens.getQuick(current)).concat(TXN_FILE_NAME);
                            writer.ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.HOUR);
                            writer.setCommitMode(CommitMode.NOSYNC);
                            appendAndCommit(writer, symbols);
                        }
                        default -> appendAndCommit(writer, symbols);
                    }
                    commits++;
                    if (getTxnBodyPartitionTableHashTermCount() == terms) {
                        cachedCommits++;
                    }
                    assertAreasVerify(path.$());
                    assertFreshReaderVerifies(path.$(), writer.getTxn());
                }
            }
            LOG.info().$("random commit sequence [commits=").$(commits).$(", cachedCommits=").$(cachedCommits).I$();
            Assert.assertTrue("the fast path must be exercised, cachedCommits=" + cachedCommits, cachedCommits > commits / 4);
        });
    }

    @Test
    public void testRangeHashMatchesTermByTermFold() {
        // The _txn, _meta and _cv checksums share one range hash, which folds several words per step. Its value
        // must stay the term-by-term fold's, as stored on disk, for every start alignment and every length
        // remainder, short and long.
        final long capacity = 40_000;
        final long addr = Unsafe.malloc(capacity, MemoryTag.NATIVE_DEFAULT);
        try {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (long i = 0; i < capacity; i += Long.BYTES) {
                Unsafe.putLong(addr + i, rnd.nextLong());
            }
            for (int lo = 0; lo < 16; lo++) {
                for (int len = 0; len < 700; len++) {
                    Assert.assertEquals("lo=" + lo + ", len=" + len, referenceAreaChecksum(addr + lo, len), calculateCvAreaChecksum(addr + lo, len));
                }
                for (int len = 32_000; len < 32_040; len++) {
                    Assert.assertEquals("lo=" + lo + ", len=" + len, referenceAreaChecksum(addr + lo, len), calculateCvAreaChecksum(addr + lo, len));
                }
            }
        } finally {
            Unsafe.free(addr, capacity, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testStaleEntryFailsAtClose() throws Exception {
        // A self-check of the guard, not a production model: a partition-table byte of a slot whose hash is
        // cached changes behind the writer, as a write that did not keep the entry exact would leave it. The
        // writer checks its cached hashes when it drops them, so closing it must fail, and still unmap _txn.
        assertMemoryLeak(() -> {
            final TableToken token = createHourTable("stale");
            final ObjList<SymbolCountProvider> symbols = new ObjList<>();
            try (Path path = txnPath(token)) {
                final TxWriter writer = new TxWriter(configuration.getFilesFacade(), configuration);
                try {
                    writer.ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.HOUR);
                    writer.setCommitMode(CommitMode.NOSYNC);
                    populate(writer, symbols);
                    appendAndCommit(writer, symbols);
                    appendAndCommit(writer, symbols);
                    final long terms = getTxnBodyPartitionTableHashTermCount();
                    appendAndCommit(writer, symbols);
                    Assert.assertEquals("the live slot's hash must be cached", terms, getTxnBodyPartitionTableHashTermCount());

                    final MemoryCMARW mem = openWholeTxnFile(path.$());
                    try {
                        final boolean isA = (mem.getLong(TX_BASE_OFFSET_VERSION_64) & 1) == 0;
                        final long areaOffset = mem.getInt(isA ? TX_BASE_OFFSET_A_32 : TX_BASE_OFFSET_B_32);
                        final long partitionByteOffset = areaOffset + getPartitionTableSizeOffset(symbols.size()) + Integer.BYTES;
                        final byte original = mem.getByte(partitionByteOffset);
                        mem.putByte(partitionByteOffset, (byte) (original ^ 0x01));
                        try {
                            writer.close();
                            Assert.fail("closing the writer must report the stale hash");
                        } catch (AssertionError e) {
                            TestUtils.assertContains(e.getMessage(), "stale _txn partition-table hash [slot=" + (isA ? 'A' : 'B'));
                        } finally {
                            mem.putByte(partitionByteOffset, original);
                        }
                    } finally {
                        mem.close(false);
                    }
                } finally {
                    writer.close();
                }
                assertAreasVerify(path.$());
            }
        });
    }

    @Test
    public void testTableOperationsKeepChecksumExact() throws Exception {
        // The same guarantee through TableWriter and SQL on one pooled writer, whose cached hashes live across
        // statements: O3, drop, detach and attach, add a symbol column, rollback, writer reopen and truncate,
        // each followed by single-row fast-path commits.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(
                    "INSERT INTO x SELECT timestamp_sequence(" + DAY_2024_01_01 + ", 6 * " + Micros.HOUR_MICROS + ") ts, x v, rnd_symbol('s0', 's1', 's2') s " +
                            "FROM long_sequence(40)"
            );
            long rows = 40;
            rows += appendFastPathRows("x", 20);

            execute("INSERT INTO x VALUES ('2024-01-03T01:00:00.000000Z', -1, 'o3')");
            rows++;
            assertTxnVerifies("x", PartitionBy.DAY);
            rows += appendFastPathRows("x", 20);

            execute("ALTER TABLE x DROP PARTITION LIST '2024-01-02'");
            rows -= 4;
            assertTxnVerifies("x", PartitionBy.DAY);
            rows += appendFastPathRows("x", 20);

            execute("ALTER TABLE x DETACH PARTITION LIST '2024-01-04'");
            rows -= 4;
            assertTxnVerifies("x", PartitionBy.DAY);
            rows += appendFastPathRows("x", 20);
            try (Path detached = new Path(); Path attachable = new Path()) {
                final TableToken token = engine.verifyTableName("x");
                detached.of(configuration.getDbRoot()).concat(token).concat("2024-01-04").put(DETACHED_DIR_MARKER);
                attachable.of(configuration.getDbRoot()).concat(token).concat("2024-01-04").put(configuration.getAttachPartitionSuffix());
                Assert.assertTrue(Files.rename(detached.$(), attachable.$()) > -1);
            }
            execute("ALTER TABLE x ATTACH PARTITION LIST '2024-01-04'");
            rows += 4;
            assertTxnVerifies("x", PartitionBy.DAY);
            rows += appendFastPathRows("x", 20);

            execute("ALTER TABLE x ADD COLUMN s2 SYMBOL");
            assertTxnVerifies("x", PartitionBy.DAY);
            rows += appendFastPathRows("x", 20);

            try (TableWriter writer = getWriter("x")) {
                for (int i = 0; i < 5; i++) {
                    final TableWriter.Row row = writer.newRow(writer.getMaxTimestamp() + 1);
                    row.putLong(1, i);
                    row.putSym(2, "rolled_back");
                    row.append();
                }
                writer.rollback();
            }
            assertTxnVerifies("x", PartitionBy.DAY);
            rows += appendFastPathRows("x", 20);

            engine.releaseAllWriters();
            rows += appendFastPathRows("x", 20);
            assertQuery("SELECT count() FROM x").noLeakCheck().expectSize().noRandomAccess().returns("count\n" + rows + "\n");
            assertQuery("SELECT count() FROM x WHERE s = 'rolled_back'").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");

            execute("TRUNCATE TABLE x");
            assertTxnVerifies("x", PartitionBy.DAY);
            appendFastPathRows("x", 20);
            assertQuery("SELECT count(), sum(v), min(ts), max(ts) FROM x").noLeakCheck().expectSize().noRandomAccess().returns(
                    """
                            count\tsum\tmin\tmax
                            20\t210\t2024-01-01T00:00:00.000001Z\t2024-01-01T00:00:00.000020Z
                            """
            );
        });
    }

    @Test
    public void testTornPartitionTableUnderFastPathRecordIsDetected() throws Exception {
        // The fast path republishes an area whose partition table was written earlier and, under NOSYNC or
        // ADAPTIVE, not flushed. Its checksum is assembled from the cached partition-table hash, yet it is
        // the same number a full recomputation gives, so flipping any covered byte of that record -- header or
        // partition table, including the partition-table length and the trailing half-word term -- must still
        // read as torn and send readers to the previous area.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE torn (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL");
            execute("INSERT INTO torn SELECT timestamp_sequence(0, " + Micros.HOUR_MICROS + ") ts, x v FROM long_sequence(5)");
            final long liveTxn;
            try (TableWriter writer = getWriter("torn")) {
                long ts = 4 * Micros.HOUR_MICROS;
                for (int i = 0; i < 3; i++) {
                    appendRow(writer, ++ts, i);
                }
                final long terms = getTxnBodyPartitionTableHashTermCount();
                for (int i = 0; i < 3; i++) {
                    appendRow(writer, ++ts, i);
                }
                Assert.assertEquals("the live record must come from a cached fast-path commit", terms, getTxnBodyPartitionTableHashTermCount());
                liveTxn = writer.getTxWriter().getTxn();
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            assertTxnVerifies("torn", PartitionBy.HOUR);

            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = txnPath(engine.verifyTableName("torn"))) {
                final LPSZ txnPath = path.$();
                final long version = RawFileAccess.peekLong(ff, txnPath, TX_BASE_OFFSET_VERSION_64);
                final boolean isA = (version & 1) == 0;
                final long areaOffset = RawFileAccess.peekInt(ff, txnPath, isA ? TX_BASE_OFFSET_A_32 : TX_BASE_OFFSET_B_32);
                final int symbolsSize = RawFileAccess.peekInt(ff, txnPath, isA ? TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 : TX_BASE_OFFSET_SYMBOLS_SIZE_B_32);
                final int partitionsSize = RawFileAccess.peekInt(ff, txnPath, isA ? TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TX_BASE_OFFSET_PARTITIONS_SIZE_B_32);
                final long recordSize = calculateTxRecordSize(symbolsSize, partitionsSize);
                final long partitionTableStart = getPartitionTableSizeOffset(symbolsSize / Long.BYTES);
                Assert.assertEquals(liveTxn, RawFileAccess.peekLong(ff, txnPath, areaOffset + TX_OFFSET_TXN_64));
                Assert.assertEquals(5 * LONGS_PER_TX_ATTACHED_PARTITION * Long.BYTES, partitionsSize);

                // Each reader gets a mapping of the whole file, so both the diagnosis and the A/B fallback see
                // every area whatever the mapping a cold reader would have grown to.
                try (TxReader reader = new TxReader(ff)) {
                    reader.initRO(openWholeTxnFile(txnPath));
                    reader.initPartitionBy(ColumnType.TIMESTAMP, PartitionBy.HOUR);
                    Assert.assertFalse("the untouched record must verify", reader.unsafeIsLiveAreaTorn());
                }
                int flipped = 0;
                for (long offset = 0; offset < recordSize; offset++) {
                    final boolean isCovered = offset < TX_OFFSET_STRUCT_VERSION_64
                            || (offset >= TX_OFFSET_DATA_VERSION_64 && offset < TX_OFFSET_SEQ_TXN_64)
                            || offset >= partitionTableStart;
                    if (!isCovered) {
                        continue;
                    }
                    try (TxReader reader = new TxReader(ff)) {
                        final MemoryCMARW mem = openWholeTxnFile(txnPath);
                        reader.initRO(mem);
                        reader.initPartitionBy(ColumnType.TIMESTAMP, PartitionBy.HOUR);
                        final byte original = mem.getByte(areaOffset + offset);
                        mem.putByte(areaOffset + offset, (byte) (original ^ 0x10));
                        try {
                            Assert.assertTrue("flip at offset " + offset + " must read as torn", reader.unsafeIsLiveAreaTorn());
                            if (offset >= partitionTableStart) {
                                TxReader.resetBodyChecksumFallbackCount();
                                Assert.assertTrue(reader.unsafeLoadAll());
                                Assert.assertEquals("reader must fall back to the previous area", liveTxn - 1, reader.getTxn());
                                Assert.assertEquals(1, TxReader.getBodyChecksumFallbackCount());
                            }
                        } finally {
                            mem.putByte(areaOffset + offset, original);
                        }
                    }
                    flipped++;
                }
                Assert.assertEquals(72 + recordSize - partitionTableStart, flipped);
            }
            assertTxnVerifies("torn", PartitionBy.HOUR);
            assertQuery("SELECT count(), sum(v) FROM torn").noLeakCheck().expectSize().noRandomAccess().returns("count\tsum\n11\t21\n");
        });
    }

    @Test
    public void testWalApplyHashesOnlyChangedEntries() throws Exception {
        // Every applied WAL transaction is a full-record commit, even a one-row append to the last partition.
        // It rewrites the whole partition table, of which only a few longs change, and must hash only those:
        // a handful of terms per transaction, not the 4,001 a full hash of 1,000 partitions takes.
        assertMemoryLeak(() -> {
            final int partitions = 1_000;
            execute("CREATE TABLE w (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("INSERT INTO w SELECT timestamp_sequence(0, " + Micros.HOUR_MICROS + ") ts, x v FROM long_sequence(" + partitions + ")");
            drainWalQueue();
            long ts = (partitions - 1) * Micros.HOUR_MICROS;
            // The first applies after the bulk insert write each A/B slot with a new geometry, a full hash each.
            for (int i = 0; i < 3; i++) {
                execute("INSERT INTO w VALUES (" + (++ts) + ", " + i + ")");
                drainWalQueue();
            }
            final int applies = 50;
            final long middlePartitionTs = (partitions / 2) * Micros.HOUR_MICROS;
            for (boolean isO3 : new boolean[]{false, true}) {
                final TxWriter txWriter;
                final long txn;
                final long terms;
                try (TableWriter writer = getWriter("w")) {
                    txWriter = writer.getTxWriter();
                    txn = txWriter.getTxn();
                    terms = getTxnBodyPartitionTableHashTermCount();
                }
                for (int i = 0; i < applies; i++) {
                    final long rowTs = isO3 ? middlePartitionTs + 1 + i : ++ts;
                    execute("INSERT INTO w VALUES (" + rowTs + ", " + i + ")");
                    drainWalQueue();
                }
                try (TableWriter writer = getWriter("w")) {
                    Assert.assertSame("the apply job must have used the pooled writer", txWriter, writer.getTxWriter());
                    Assert.assertEquals("one commit per applied transaction", applies, txWriter.getTxn() - txn);
                    final long applyTerms = getTxnBodyPartitionTableHashTermCount() - terms;
                    LOG.info().$("WAL apply partition-table hash terms [o3=").$(isO3).$(", applies=").$(applies).$(", terms=").$(applyTerms).I$();
                    Assert.assertTrue("o3=" + isO3 + ", terms=" + applyTerms, applyTerms <= 4L * applies);
                }
                assertTxnVerifies("w", PartitionBy.HOUR);
            }
            assertQuery("SELECT count() FROM w").noLeakCheck().expectSize().noRandomAccess().returns("count\n" + (partitions + 3 + 2 * applies) + "\n");
        });
    }

    @Test
    public void testWalTableOperationsKeepChecksumExact() throws Exception {
        // The WAL twin of testTableOperationsKeepChecksumExact. The apply job commits every transaction as a
        // full-record commit that folds the partition-table entries it changes into the pooled writer's cached
        // hashes, so those hashes live across the whole sequence: O3 split and squash, drop, detach and attach,
        // conversion to Parquet and back, and truncate, each followed by appends. Every A/B area must verify
        // after each step, and each writer release checks the cached hashes against a full recomputation.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            // The sequence drives a partition split and a squash, which merge-append never produces: an O3
            // insert into the last partition becomes a piece of it instead. Pin the production default, which
            // the test harness flips on, so the sequence keeps covering the split and squash commits.
            setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
            execute("CREATE TABLE w (ts TIMESTAMP, v LONG, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // 144 rows a day, 2024-01-01 to 2024-01-05.
            execute(
                    "INSERT INTO w SELECT timestamp_sequence(" + DAY_2024_01_01 + ", 10 * " + Micros.MINUTE_MICROS + ") ts, x v, rnd_symbol('s0', 's1', 's2') s " +
                            "FROM long_sequence(720)"
            );
            drainWalQueue();
            long rows = 720;
            Assert.assertEquals(5, countPartitions("w"));

            // A large prefix before the O3 rows and a short suffix after them make the last partition split.
            execute("INSERT INTO w SELECT timestamp_sequence('2024-01-05T22:00:00.000001Z', 1_000_000L) ts, -x v, 'o3' s FROM long_sequence(5)");
            drainWalQueue();
            rows += 5;
            Assert.assertEquals("the O3 insert must split the last partition", 6, countPartitions("w"));
            assertTxnVerifies("w", PartitionBy.DAY);
            long ts = DAY_2024_01_01 + 5 * Micros.DAY_MICROS;
            appendWalRows("w", ts, 5);
            ts += 5;
            rows += 5;

            execute("ALTER TABLE w SQUASH PARTITIONS");
            drainWalQueue();
            Assert.assertEquals(6, countPartitions("w"));
            assertTxnVerifies("w", PartitionBy.DAY);
            appendWalRows("w", ts, 5);
            ts += 5;
            rows += 5;

            execute("ALTER TABLE w DROP PARTITION LIST '2024-01-02'");
            drainWalQueue();
            rows -= 144;
            assertTxnVerifies("w", PartitionBy.DAY);
            appendWalRows("w", ts, 5);
            ts += 5;
            rows += 5;

            execute("ALTER TABLE w DETACH PARTITION LIST '2024-01-04'");
            drainWalQueue();
            rows -= 144;
            assertTxnVerifies("w", PartitionBy.DAY);
            appendWalRows("w", ts, 5);
            ts += 5;
            rows += 5;
            try (Path detached = new Path(); Path attachable = new Path()) {
                final TableToken token = engine.verifyTableName("w");
                detached.of(configuration.getDbRoot()).concat(token).concat("2024-01-04").put(DETACHED_DIR_MARKER);
                attachable.of(configuration.getDbRoot()).concat(token).concat("2024-01-04").put(configuration.getAttachPartitionSuffix());
                Assert.assertTrue(Files.rename(detached.$(), attachable.$()) > -1);
            }
            execute("ALTER TABLE w ATTACH PARTITION LIST '2024-01-04'");
            drainWalQueue();
            rows += 144;
            assertTxnVerifies("w", PartitionBy.DAY);
            appendWalRows("w", ts, 5);
            ts += 5;
            rows += 5;

            execute("ALTER TABLE w CONVERT PARTITION TO PARQUET LIST '2024-01-03'");
            drainWalQueue();
            assertTxnVerifies("w", PartitionBy.DAY);
            appendWalRows("w", ts, 5);
            ts += 5;
            rows += 5;
            execute("ALTER TABLE w CONVERT PARTITION TO NATIVE LIST '2024-01-03'");
            drainWalQueue();
            assertTxnVerifies("w", PartitionBy.DAY);
            appendWalRows("w", ts, 5);
            ts += 5;
            rows += 5;

            engine.releaseAllWriters();
            appendWalRows("w", ts, 5);
            ts += 5;
            rows += 5;
            assertQuery("SELECT count() FROM w").noLeakCheck().expectSize().noRandomAccess().returns("count\n" + rows + "\n");
            assertQuery("SELECT count() FROM w WHERE s = 'o3'").noLeakCheck().expectSize().noRandomAccess().returns("count\n5\n");

            execute("TRUNCATE TABLE w");
            drainWalQueue();
            assertTxnVerifies("w", PartitionBy.DAY);
            appendWalRows("w", ts, 5);
            engine.releaseAllWriters();
            assertQuery("SELECT count(), sum(v) FROM w").noLeakCheck().expectSize().noRandomAccess().returns("count\tsum\n5\t15\n");
        });
    }

    private static void appendAndCommit(TxWriter writer, ObjList<SymbolCountProvider> symbols) {
        writer.append();
        writer.updateMaxTimestamp(writer.getMaxTimestamp() + 1);
        writer.commit(symbols);
    }

    private static void appendRow(TableWriter writer, long ts, long v) {
        final TableWriter.Row row = writer.newRow(ts);
        row.putLong(1, v);
        row.append();
        writer.commit();
    }

    // Applies one-row WAL transactions after fromTs, each through the apply job, then checks _txn.
    private static void appendWalRows(String tableName, long fromTs, int count) throws SqlException {
        for (int i = 1; i <= count; i++) {
            execute("INSERT INTO " + tableName + " VALUES (" + (fromTs + i) + ", " + i + ", 'w" + (i % 3) + "')");
            drainWalQueue();
        }
        assertTxnVerifies(tableName, PartitionBy.DAY);
    }

    // Reads the whole _txn file and checks both A/B areas: wherever an area's stamp names its record, the stored
    // checksum must equal a full recomputation over that area's bytes. The live area must carry a stamp.
    private static void assertAreasVerify(LPSZ txnPath) {
        final FilesFacade ff = configuration.getFilesFacade();
        final long fd = ff.openRO(txnPath);
        Assert.assertTrue(fd > -1);
        final long len = ff.length(fd);
        final long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(len, ff.read(fd, buf, len, 0));
            final long version = Unsafe.getLong(buf + TX_BASE_OFFSET_VERSION_64);
            for (int slot = 0; slot < 2; slot++) {
                final boolean isA = slot == 0;
                final int areaOffset = Unsafe.getInt(buf + (isA ? TX_BASE_OFFSET_A_32 : TX_BASE_OFFSET_B_32));
                final int symbolsSize = Unsafe.getInt(buf + (isA ? TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 : TX_BASE_OFFSET_SYMBOLS_SIZE_B_32));
                final int partitionsSize = Unsafe.getInt(buf + (isA ? TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TX_BASE_OFFSET_PARTITIONS_SIZE_B_32));
                final long recordSize = calculateTxRecordSize(symbolsSize, partitionsSize);
                final boolean isLive = (version & 1) == slot;
                if (!isLive && (areaOffset < TX_BASE_HEADER_SIZE || areaOffset + recordSize > len)) {
                    continue;
                }
                Assert.assertTrue(areaOffset >= TX_BASE_HEADER_SIZE && areaOffset + recordSize <= len);
                final long area = buf + areaOffset;
                final long txn = Unsafe.getLong(area + TX_OFFSET_TXN_64);
                final boolean isStamped = Unsafe.getInt(area + TX_OFFSET_BODY_CHECKSUM_STAMP_32) == ((int) txn ^ TX_BODY_CHECKSUM_STAMP_XOR);
                if (isLive) {
                    Assert.assertEquals(version, txn);
                    Assert.assertTrue("live area must carry a checksum stamp", isStamped);
                }
                if (isStamped) {
                    Assert.assertEquals(
                            "stored checksum of area " + (isA ? 'A' : 'B') + " [txn=" + txn + ", live=" + isLive + ']',
                            calculateTxnBodyChecksum(area, recordSize, getPartitionTableSizeOffset(symbolsSize / Long.BYTES)),
                            Unsafe.getLong(area + TX_OFFSET_BODY_CHECKSUM_64)
                    );
                }
            }
        } finally {
            Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    private static void assertFreshReaderVerifies(LPSZ txnPath, long expectedTxn) {
        TxReader.resetBodyChecksumFallbackCount();
        try (TxReader reader = new TxReader(configuration.getFilesFacade())) {
            reader.ofRO(txnPath, ColumnType.TIMESTAMP, PartitionBy.HOUR);
            Assert.assertTrue(reader.unsafeLoadAll());
            Assert.assertEquals(expectedTxn, reader.getTxn());
            Assert.assertFalse(reader.unsafeIsLiveAreaTorn());
        }
        Assert.assertEquals(0, TxReader.getBodyChecksumFallbackCount());
    }

    // Commits one after another, asserting how many partition-table hash terms each one computed.
    private static void assertHashesPerCommit(TxWriter writer, Path path, ObjList<SymbolCountProvider> symbols, long... expectedTerms) {
        for (int i = 0; i < expectedTerms.length; i++) {
            final long terms = getTxnBodyPartitionTableHashTermCount();
            if (i == 0) {
                // The caller has already made whatever change the first commit publishes.
                writer.commit(symbols);
            } else {
                appendAndCommit(writer, symbols);
            }
            Assert.assertEquals("commit " + i, expectedTerms[i], getTxnBodyPartitionTableHashTermCount() - terms);
            assertAreasVerify(path.$());
            assertFreshReaderVerifies(path.$(), writer.getTxn());
        }
    }

    private static void assertTxnVerifies(String tableName, int partitionBy) {
        final TableToken token = engine.verifyTableName(tableName);
        try (Path path = txnPath(token)) {
            assertAreasVerify(path.$());
            TxReader.resetBodyChecksumFallbackCount();
            try (TxReader reader = new TxReader(configuration.getFilesFacade())) {
                reader.ofRO(path.$(), ColumnType.TIMESTAMP, partitionBy);
                Assert.assertTrue(reader.unsafeLoadAll());
                Assert.assertFalse(reader.unsafeIsLiveAreaTorn());
            }
            Assert.assertEquals(0, TxReader.getBodyChecksumFallbackCount());
        }
    }

    private static long countPartitions(String tableName) {
        try (TableWriter writer = getWriter(tableName)) {
            return writer.getPartitionCount();
        }
    }

    private static TableToken createHourTable(String name) {
        return create(new TableModel(configuration, name, PartitionBy.HOUR).timestamp());
    }

    // The terms a full hash of a partition table of this many partitions folds: one per long, plus the
    // trailing int.
    private static long fullHashTerms(int partitions) {
        return (long) partitions * LONGS_PER_TX_ATTACHED_PARTITION + 1;
    }

    // A writable mapping of the whole file, positioned at its end so that closing it does not truncate it.
    private static MemoryCMARW openWholeTxnFile(LPSZ txnPath) {
        final FilesFacade ff = configuration.getFilesFacade();
        final MemoryCMARW mem = Vm.getSmallCMARWInstance(ff, txnPath, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
        try {
            mem.jumpTo(ff.length(mem.getFd()));
            return mem;
        } catch (Throwable e) {
            mem.close(false);
            throw e;
        }
    }

    // Seeds 8 hourly partitions through two full-record commits, the second of which arms the fast path.
    private static void populate(TxWriter writer, ObjList<SymbolCountProvider> symbols) {
        for (int i = 0; i < 8; i++) {
            writer.updatePartitionSizeByTimestamp(i * Micros.HOUR_MICROS, 1 + i);
        }
        writer.setMinTimestamp(0);
        writer.reset(28, 8, 7 * Micros.HOUR_MICROS, symbols);
        writer.commit(symbols);
    }

    private static long referenceAreaChecksum(long addr, long size) {
        return referenceAvalanche(referenceFold(addr, 0, size, 0));
    }

    private static long referenceAvalanche(long h) {
        h ^= h >>> 37;
        h *= 0x165667919E3779F9L;
        h ^= h >>> 32;
        return h != 0 ? h : 1L;
    }

    // The same term-by-term fold hashTxnBodyRange() documents: 8-byte words, then one 4-byte int, then single
    // bytes, all sign-extended, with the xxh3 avalanche on top.
    private static long referenceChecksum(long addr, long recordSize, long partitionTableStart) {
        long h = referenceFold(addr, 0, TX_OFFSET_STRUCT_VERSION_64, 0);
        h = referenceFold(addr, TX_OFFSET_DATA_VERSION_64, TX_OFFSET_SEQ_TXN_64, h);
        if (partitionTableStart < recordSize) {
            h = referenceFold(addr, partitionTableStart, recordSize, h);
        }
        return referenceAvalanche(h);
    }

    private static long referenceFold(long addr, long lo, long hi, long h) {
        final long m = 0x517cc1b727220a95L;
        long i = lo;
        for (; i + Long.BYTES <= hi; i += Long.BYTES) {
            h = h * m + Unsafe.getLong(addr + i);
        }
        if (i + Integer.BYTES <= hi) {
            h = h * m + Unsafe.getInt(addr + i);
            i += Integer.BYTES;
        }
        for (; i < hi; i++) {
            h = h * m + Unsafe.getByte(addr + i);
        }
        return h;
    }

    private static Path txnPath(TableToken token) {
        return new Path().of(configuration.getDbRoot()).concat(token).concat(TXN_FILE_NAME);
    }

    // Appends single-row commits to the last partition through the pooled writer. The first commits after a
    // structure change are full-record commits; every later one must reuse the cached partition-table hash.
    private long appendFastPathRows(String tableName, int count) {
        try (TableWriter writer = getWriter(tableName)) {
            final boolean hasSecondSymbol = writer.getMetadata().getColumnCount() > 3;
            long ts = writer.getMaxTimestamp() == Long.MIN_VALUE ? DAY_2024_01_01 : writer.getMaxTimestamp();
            long terms = 0;
            for (int i = 0; i < count; i++) {
                if (i == 3) {
                    terms = getTxnBodyPartitionTableHashTermCount();
                }
                final TableWriter.Row row = writer.newRow(++ts);
                row.putLong(1, i + 1);
                row.putSym(2, "f" + (i % 7));
                if (hasSecondSymbol) {
                    row.putSym(3, "g" + i);
                }
                row.append();
                writer.commit();
            }
            Assert.assertEquals("fast-path commits must reuse the cached partition-table hash", terms, getTxnBodyPartitionTableHashTermCount());
        }
        assertTxnVerifies(tableName, PartitionBy.DAY);
        return count;
    }

    private static class MutableSymbolCount implements SymbolCountProvider {
        private int count;

        private MutableSymbolCount(int count) {
            this.count = count;
        }

        @Override
        public int getSymbolCount() {
            return count;
        }
    }
}
