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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SymbolCountProvider;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Vect;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class TxnTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(TxnTest.class);

    @Test
    public void testClearedSlotStoresCleanZeroNotMinusOne() throws Exception {
        // The cleared write must store a clean 0 -- value and every flag bit 0 -- not the all-bits-set
        // -1, which sets bit 63 (REMOTE) and the reserved bits and reads as cleared only because every
        // reader folds it. Asserts the RAW offset-3 word (before the fold) is 0 for a fresh slot
        // (initPartitionAt) and an explicit clean clear.
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            String tableName = "clearedClean";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
            model.timestamp();
            AbstractCairoTest.create(model);
            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                int tsType = TableUtils.getTimestampType(model);
                try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), tsType, PartitionBy.DAY)) {
                    tw.updatePartitionSizeByTimestamp(0, 1);                  // fresh -> initPartitionAt
                    tw.updatePartitionSizeByTimestamp(Micros.DAY_MICROS, 1);
                    tw.setPartitionSeqTxn(1, 4096L);                         // stash a value...
                    tw.setPartitionSeqTxn(1, 0L);                            // ...then explicitly clear it
                    tw.updateMaxTimestamp(2 * Micros.DAY_MICROS + 1);
                    tw.finishPartitionSizeUpdate();
                    tw.commit(new ObjList<>());
                }
                try (RawOffset3Reader tr = new RawOffset3Reader(ff)) {
                    tr.ofRO(path.$(), tsType, PartitionBy.DAY);
                    tr.unsafeLoadAll();
                    Assert.assertEquals("fresh slot must store a clean 0, not -1", 0L, tr.rawOffset3(0));
                    Assert.assertEquals("cleared slot must store a clean 0, not -1", 0L, tr.rawOffset3(1));
                }
            }
        });
    }

    @Test
    public void testDumpToClearsParquetGeneratedForNativePartitionOnly() throws Exception {
        // dumpTo is the _txn serializer for a backup/checkpoint snapshot (its only production caller is
        // DatabaseCheckpointAgent). A native partition's generated data.parquet only duplicates the
        // native columns and is omitted from a backup, so the snapshot must not claim it. This asserts
        // dumpTo clears parquet_generated for a native partition, leaves a parquet-format partition (whose
        // data.parquet IS backed up) alone, and -- critically -- never alters the live _txn.
        TestUtils.assertMemoryLeak(() -> {
            final FilesFacade ff = engine.getConfiguration().getFilesFacade();
            final String tableName = "txnDumpParquetGenerated";
            final TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
            model.timestamp();
            AbstractCairoTest.create(model);

            try (Path path = new Path(); Path dumpPath = new Path()) {
                final TableToken tableToken = engine.verifyTableName(tableName);
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                dumpPath.of(configuration.getDbRoot()).concat(tableToken).concat("_txn.backup").$();
                final int tsType = TableUtils.getTimestampType(model);

                // idx : state                       F  G  R  U   off-3   dumpTo clears G?
                //  0  : native + generated          0  1  0  0   seqTxn  yes (upload-while-native)
                //  1  : native + generated + sealed  0  1  1  1   seqTxn  yes (read-only/remote must not matter)
                //  2  : parquet-local + generated    1  1  0  0   size    no  (its data.parquet is backed up)
                //  3  : cold / remotely served       1  0  1  1   size    no  (already cleared)
                //  4  : plain native                 0  0  0  0   seqTxn  no  (never had it)
                try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), tsType, PartitionBy.DAY)) {
                    for (int i = 0; i < 5; i++) {
                        tw.updatePartitionSizeByTimestamp(i * Micros.DAY_MICROS, i + 1);
                    }
                    tw.updateMaxTimestamp(5 * Micros.DAY_MICROS + 1);
                    tw.finishPartitionSizeUpdate();
                    applyVersionState(tw, 0, false, true, false, false, 101L);
                    applyVersionState(tw, 1, false, true, true, true, 102L);
                    applyVersionState(tw, 2, true, true, false, false, 4096L);
                    applyVersionState(tw, 3, true, false, true, true, 8192L);
                    applyVersionState(tw, 4, false, false, false, false, 105L);
                    tw.commit(new ObjList<>());
                }

                try (TxReader live = new TxReader(ff)) {
                    live.ofRO(path.$(), tsType, PartitionBy.DAY);
                    live.unsafeLoadAll();

                    // The live _txn (what a running table reads) keeps every flag: a commit never
                    // touches parquet_generated.
                    assertVersionState(live, 0, false, true, false, false, 101L);
                    assertVersionState(live, 1, false, true, true, true, 102L);
                    assertVersionState(live, 2, true, true, false, false, 4096L);
                    assertVersionState(live, 3, true, false, true, true, 8192L);
                    assertVersionState(live, 4, false, false, false, false, 105L);

                    try (MemoryCMARW dumpMem = Vm.getCMARWInstance()) {
                        dumpMem.smallFile(ff, dumpPath.$(), MemoryTag.MMAP_DEFAULT);
                        live.dumpTo(dumpMem);
                        // dumpTo writes at absolute offsets without advancing the append pointer;
                        // close(false) flushes without truncating the file to that (zero) pointer.
                        dumpMem.close(false);
                    }

                    // dumpTo writes into the passed-in buffer; the live reader is untouched.
                    assertVersionState(live, 0, false, true, false, false, 101L);
                    assertVersionState(live, 1, false, true, true, true, 102L);
                }

                // The backup snapshot clears parquet_generated only for the native partitions (0, 1),
                // independent of read-only/remote, and leaves every other flag and offset-3 value intact.
                try (TxReader backup = new TxReader(ff)) {
                    backup.ofRO(dumpPath.$(), tsType, PartitionBy.DAY);
                    backup.unsafeLoadAll();
                    assertVersionState(backup, 0, false, false, false, false, 101L);
                    assertVersionState(backup, 1, false, false, true, true, 102L);
                    assertVersionState(backup, 2, true, true, false, false, 4096L);
                    assertVersionState(backup, 3, true, false, true, true, 8192L);
                    assertVersionState(backup, 4, false, false, false, false, 105L);
                }
            }
        });
    }

    @Test
    public void testDumpToScrubsUntrustedNativeOffset3() throws Exception {
        // A backup snapshot must never carry a native offset-3 word lacking the VALID bit: restored,
        // such a word would re-enter as the released-base file-size poison -- and the G-clear above
        // even strips the one bit hinting at its provenance. Untrusted native words scrub to the
        // cleared 0, REMOTE included (a native slot cannot legitimately be REMOTE without a valid
        // stamp); stamped native words and parquet file sizes (valid without the bit) survive.
        TestUtils.assertMemoryLeak(() -> {
            final FilesFacade ff = engine.getConfiguration().getFilesFacade();
            final String tableName = "txnDumpScrub";
            final TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
            model.timestamp();
            AbstractCairoTest.create(model);

            try (Path path = new Path(); Path dumpPath = new Path()) {
                final TableToken tableToken = engine.verifyTableName(tableName);
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                dumpPath.of(configuration.getDbRoot()).concat(tableToken).concat("_txn.backup").$();
                final int tsType = TableUtils.getTimestampType(model);

                // idx : live state                                       backup offset-3
                //  0  : native, G=1, file-size poison (poked, no VALID)  scrubbed to 0 (G cleared too)
                //  1  : native, stamped (VALID)                          kept verbatim
                //  2  : parquet, plain file size (no VALID)              kept verbatim
                //  3  : native, legacy -1 (poked)                        canonical 0
                //  4  : native, poison + REMOTE (poked, no VALID)        whole word scrubbed to 0
                try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), tsType, PartitionBy.DAY)) {
                    for (int i = 0; i < 5; i++) {
                        tw.updatePartitionSizeByTimestamp(i * Micros.DAY_MICROS, 1);
                    }
                    tw.updateMaxTimestamp(5 * Micros.DAY_MICROS + 1);
                    tw.finishPartitionSizeUpdate();
                    tw.setPartitionParquetGenerated(0, true);
                    tw.setPartitionSeqTxn(1, 101L);
                    tw.setPartitionParquet(2 * Micros.DAY_MICROS, 4096L);
                    tw.commit(new ObjList<>());
                }

                try (RawOffset3Reader live = new RawOffset3Reader(ff)) {
                    live.ofRO(path.$(), tsType, PartitionBy.DAY);
                    live.unsafeLoadAll();
                    live.pokeRawOffset3(0, 50_000_000L);
                    live.pokeRawOffset3(3, -1L);
                    live.pokeRawOffset3(4, 50_000_000L | TxReader.PARTITION_REMOTE_BIT);

                    try (MemoryCMARW dumpMem = Vm.getCMARWInstance()) {
                        dumpMem.smallFile(ff, dumpPath.$(), MemoryTag.MMAP_DEFAULT);
                        live.dumpTo(dumpMem);
                        dumpMem.close(false);
                    }
                }

                try (RawOffset3Reader backup = new RawOffset3Reader(ff)) {
                    backup.ofRO(dumpPath.$(), tsType, PartitionBy.DAY);
                    backup.unsafeLoadAll();
                    Assert.assertEquals("the poison must scrub to the canonical cleared 0", 0L, backup.rawOffset3(0));
                    Assert.assertFalse("G clears alongside (the existing dumpTo rule)",
                            backup.isPartitionParquetGenerated(0));
                    Assert.assertEquals("a stamped word survives the dump", 101L, backup.getNativePartitionSeqTxn(1));
                    Assert.assertEquals("a parquet file size is valid without the bit",
                            4096L, backup.getPartitionParquetFileSize(2));
                    Assert.assertEquals("the legacy -1 scrubs to the canonical 0", 0L, backup.rawOffset3(3));
                    Assert.assertEquals("REMOTE goes with the untrusted word", 0L, backup.rawOffset3(4));
                }
            }
        });
    }

    @Test
    public void testFailedTxWriterDoesNotCorruptTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade errorFf = new TestFilesFacadeImpl() {
                @Override
                public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
                    return -1;
                }
            };

            FilesFacadeImpl cleanFf = new TestFilesFacadeImpl();
            assertMemoryLeak(() -> {
                String tableName = "txntest";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    int testPartitionCount = 3000;
                    try (TxWriter txWriter = new TxWriter(cleanFf, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        // Add lots of partitions
                        for (int i = 0; i < testPartitionCount; i++) {
                            txWriter.updatePartitionSizeByTimestamp(i * Micros.DAY_MICROS, i + 1);
                        }
                        txWriter.updateMaxTimestamp(testPartitionCount * Micros.DAY_MICROS + 1);
                        txWriter.finishPartitionSizeUpdate();
                        txWriter.commit(new ObjList<>());
                    }

                    // Reopen without OS errors
                    try (TxWriter txWriter = new TxWriter(cleanFf, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        // Read lots of partitions
                        Assert.assertEquals(testPartitionCount, txWriter.getPartitionCount());
                        for (int i = 0; i < testPartitionCount - 1; i++) {
                            Assert.assertEquals(i + 1, txWriter.getPartitionSize(i));
                        }
                    }

                    // Open with OS error to file extend
                    try (TxWriter ignored = new TxWriter(errorFf, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        Assert.fail("Should not be able to extend on opening");
                    } catch (CairoException ex) {
                        // expected
                    }

                    // Reopen without OS errors
                    try (TxWriter txWriter = new TxWriter(cleanFf, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        // Read lots of partitions
                        Assert.assertEquals(testPartitionCount, txWriter.getPartitionCount());
                        for (int i = 0; i < testPartitionCount - 1; i++) {
                            Assert.assertEquals(i + 1, txWriter.getPartitionSize(i));
                        }
                    }
                }
            });
        });
    }

    @Test
    public void testGetPartitionVersionFoldsClearedAndRealWords() throws Exception {
        // Reader-level pin of the offset-3 fold + bit-strip over the four canonical words, one per
        // partition: the two cleared sentinels (raw 0 and raw -1) fold to -1, a real parquet file size
        // reads through verbatim, and the same size with bit 63 set reads back stripped (size, not a
        // negative word) while only the bit-63 word reports isPartitionRemote. The 0 word arrives via a
        // real disk round-trip (a fresh native partition); the legacy -1 is poked into the loaded
        // snapshot, since the value-masking writer cannot produce it on disk.
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            String tableName = "verFold";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
            model.timestamp();
            AbstractCairoTest.create(model);

            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                int tsType = TableUtils.getTimestampType(model);

                // idx 0: 4096 (parquet); idx 1: 4096 | REMOTE (parquet, remote);
                // idx 2: raw 0 (fresh native, cleared); idx 3: raw -1 (native, poked after load).
                try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), tsType, PartitionBy.DAY)) {
                    for (int i = 0; i < 4; i++) {
                        tw.updatePartitionSizeByTimestamp(i * Micros.DAY_MICROS, 1);
                    }
                    tw.updateMaxTimestamp(4 * Micros.DAY_MICROS + 1);
                    tw.finishPartitionSizeUpdate();
                    tw.setPartitionParquet(0, 4096L);
                    tw.setPartitionParquet(Micros.DAY_MICROS, 4096L);
                    tw.setPartitionRemote(1, true);
                    // idx 2 and idx 3 left native with the fresh raw 0 offset-3 word
                    tw.commit(new ObjList<>());
                }

                try (RawOffset3Reader tr = new RawOffset3Reader(ff)) {
                    tr.ofRO(path.$(), tsType, PartitionBy.DAY);
                    tr.unsafeLoadAll();
                    Assert.assertEquals(4, tr.getPartitionCount());
                    // the legacy all-bits-set sentinel only ever originates from an old binary
                    Assert.assertEquals("raw 0 survives the disk round-trip", 0L, tr.rawOffset3(2));
                    tr.pokeRawOffset3(3, -1L);

                    // real parquet words read through; bit 63 is stripped to the size, not negative
                    Assert.assertEquals(4096L, tr.getPartitionVersion(0));
                    Assert.assertEquals(4096L, tr.getPartitionVersion(1));
                    Assert.assertEquals(4096L, tr.getPartitionParquetFileSize(0));
                    Assert.assertEquals(4096L, tr.getPartitionParquetFileSize(1));
                    // both cleared words fold to -1; native, so read via the unified accessor
                    Assert.assertEquals("raw 0 folds to -1", -1L, tr.getPartitionVersion(2));
                    Assert.assertEquals("raw -1 folds to -1", -1L, tr.getPartitionVersion(3));

                    Assert.assertFalse("plain file size is not remote", tr.isPartitionRemote(0));
                    Assert.assertTrue("bit 63 word is remote", tr.isPartitionRemote(1));
                    Assert.assertFalse("raw 0 is not remote", tr.isPartitionRemote(2));
                    Assert.assertFalse("raw -1 is not remote", tr.isPartitionRemote(3));
                }
            }
        });
    }

    @Test
    public void testLoadAllFrom() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            assertMemoryLeak(() -> {
                String tableName = "txntest";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    int testPartitionCount = 2;
                    try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                        txWriter.ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY);
                        for (int i = 0; i < testPartitionCount; i++) {
                            txWriter.updatePartitionSizeByTimestamp(i * Micros.DAY_MICROS, i + 1);
                        }
                        txWriter.updateMaxTimestamp(testPartitionCount * Micros.DAY_MICROS + 1);
                        txWriter.finishPartitionSizeUpdate();
                        txWriter.commit(new ObjList<>());
                    }

                    try (
                            TxReader txReader = new TxReader(ff);
                            MemoryCARW dumpMem = Vm.getCARWInstance(ff.getPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                            TxReader txCopyReader = new TxReader(ff);
                            MemoryCARW dumpCopyMem = Vm.getCARWInstance(ff.getPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
                    ) {
                        txReader.ofRO(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY);

                        txReader.unsafeLoadAll();
                        final String expected = """
                                {txn: 1, attachedPartitions: [
                                {ts: '1970-01-01T00:00:00.000000Z', rowCount: 1, nameTxn: -1},
                                {ts: '1970-01-02T00:00:00.000000Z', rowCount: 2, nameTxn: -1}
                                ], transientRowCount: 2, fixedRowCount: 1, minTimestamp: '294247-01-10T04:00:54.775807Z', maxTimestamp: '1970-01-03T00:00:00.000001Z', dataVersion: 0, structureVersion: 0, partitionTableVersion: 0, columnVersion: 0, truncateVersion: 0, seqTxn: 0, symbolColumnCount: 0, lagRowCount: 0, lagMinTimestamp: '294247-01-10T04:00:54.775807Z', lagMaxTimestamp: '', lagTxnCount: 0, lagOrdered: true}""";
                        Assert.assertEquals(expected, txReader.toString());

                        txCopyReader.loadAllFrom(txReader);
                        Assert.assertEquals(expected, txCopyReader.toString());

                        Assert.assertTrue(txReader.getRecordSize() > 0);
                        Assert.assertEquals(txReader.getRecordSize(), txCopyReader.getRecordSize());

                        // Make sure to zero the memory before the dump to avoid garbage bytes in paddings.
                        dumpMem.jumpTo(txReader.getRecordSize());
                        dumpMem.zero();
                        dumpCopyMem.jumpTo(txCopyReader.getRecordSize());
                        dumpCopyMem.zero();

                        txReader.dumpTo(dumpMem);
                        txCopyReader.dumpTo(dumpCopyMem);
                        Assert.assertTrue(Vect.memeq(dumpMem.addressOf(0), dumpCopyMem.addressOf(0), txReader.getRecordSize()));
                    }
                }
            });
        });
    }

    @Test
    public void testLoadTxn() throws IOException {
        try (Path p = new Path()) {
            final String incrementalLoad;
            try (TxWriter tw = new TxWriter(engine.getConfiguration().getFilesFacade(), engine.getConfiguration())) {
                loadTxnWriter(tw, p, "/txn/sys.acl_entities~1/_txn");
                loadTxnWriter(tw, p, "/txn/sys.acl_passwords~5/_txn");
                incrementalLoad = tw.toString();
            }

            try (TxWriter tw = new TxWriter(engine.getConfiguration().getFilesFacade(), engine.getConfiguration())) {
                loadTxnWriter(tw, p, "/txn/sys.acl_passwords~5/_txn");
                TestUtils.assertEquals(incrementalLoad, tw.toString());
            }
        }
    }

    @Test
    public void testNativeSeqTxnValidBitQuarantinesUntrustedWords() throws Exception {
        // Offset-3 of a native partition reads as a seqTxn only when the word carries
        // PARTITION_SEQ_TXN_VALID_BIT. The released base stored the generated data.parquet FILE SIZE
        // there (no flag bits) -- by value indistinguishable from a seqTxn -- so an unflagged word is
        // quarantined to -1 instead of trusted. The legacy all-ones -1 word has bit 62 SET: the
        // cleared-sentinel fold must run before the bit test, or it would read as a colossal "valid"
        // value. getPartitionVersion stays ungated: it is the raw staleness identity, never a seqTxn.
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            String tableName = "verValidBit";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
            model.timestamp();
            AbstractCairoTest.create(model);

            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                int tsType = TableUtils.getTimestampType(model);

                // idx 0: stamped + REMOTE (VALID rides under bit 63); idx 1: legacy file-size poison
                // (poked); idx 2: legacy -1 (poked); idx 3: corrupt VALID-with-zero (poked); idx 4: fresh 0.
                try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), tsType, PartitionBy.DAY)) {
                    for (int i = 0; i < 5; i++) {
                        tw.updatePartitionSizeByTimestamp(i * Micros.DAY_MICROS, 1);
                    }
                    tw.updateMaxTimestamp(5 * Micros.DAY_MICROS + 1);
                    tw.finishPartitionSizeUpdate();
                    tw.setPartitionSeqTxn(0, 4096L);
                    tw.setPartitionRemote(0, true);
                    tw.commit(new ObjList<>());
                }

                try (RawOffset3Reader tr = new RawOffset3Reader(ff)) {
                    tr.ofRO(path.$(), tsType, PartitionBy.DAY);
                    tr.unsafeLoadAll();
                    Assert.assertEquals(5, tr.getPartitionCount());

                    // the stamp wrote VALID; REMOTE rides along without disturbing the read
                    Assert.assertNotEquals("a positive stamp must carry VALID",
                            0L, tr.rawOffset3(0) & TxReader.PARTITION_SEQ_TXN_VALID_BIT);
                    Assert.assertTrue(tr.isPartitionRemote(0));
                    Assert.assertEquals(4096L, tr.getNativePartitionSeqTxn(0));

                    // released-base poison: a file size with no flag bits
                    tr.pokeRawOffset3(1, 50_000_000L);
                    Assert.assertEquals("an unflagged word must be quarantined", -1L, tr.getNativePartitionSeqTxn(1));
                    Assert.assertEquals("the identity accessor stays raw", 50_000_000L, tr.getPartitionVersion(1));

                    // legacy -1: bit 62 is set as part of all-ones; the cleared fold must win
                    tr.pokeRawOffset3(2, -1L);
                    Assert.assertEquals("the cleared fold must precede the bit test", -1L, tr.getNativePartitionSeqTxn(2));

                    // corrupt "valid zero": no writer produces it; the value fold quarantines it
                    tr.pokeRawOffset3(3, TxReader.PARTITION_SEQ_TXN_VALID_BIT);
                    Assert.assertEquals("a non-positive valid word must fold to -1", -1L, tr.getNativePartitionSeqTxn(3));

                    // fresh slot reads as no version, exactly as before
                    Assert.assertEquals(-1L, tr.getNativePartitionSeqTxn(4));
                }
            }
        });
    }

    @Test
    public void testOffset3ValueMaskBoundary() throws Exception {
        // The offset-3 value occupies the low 56 bits. A value equal to
        // PARTITION_VERSION_VALUE_MASK round-trips, while caller-supplied reserved bits 56..61 must
        // be absent from the raw stored word, not merely hidden by the masked value accessor.
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            assertMemoryLeak(() -> {
                String tableName = "verMaskBoundary";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        long ts = 0;
                        tw.updatePartitionSizeByTimestamp(ts, 1);

                        // the largest in-range value round-trips through the parquet accessor
                        tw.setPartitionParquet(ts, TxReader.PARTITION_VERSION_VALUE_MASK);
                        Assert.assertEquals(TxReader.PARTITION_VERSION_VALUE_MASK, tw.getPartitionParquetFileSize(0));
                        Assert.assertFalse(tw.isPartitionRemote(0));

                        for (int bit = 56; bit <= 61; bit++) {
                            tw.setPartitionParquet(ts, 4096L | (1L << bit));
                            Assert.assertEquals(
                                    "reserved input bit " + bit + " must not persist in the raw word",
                                    4096L,
                                    RawOffset3Reader.rawOffset3Of(tw, 0)
                            );
                            Assert.assertEquals(
                                    "reserved input bit " + bit + " must be masked off the value",
                                    4096L,
                                    tw.getPartitionParquetFileSize(0)
                            );
                            Assert.assertFalse("reserved input bits must not bleed into REMOTE", tw.isPartitionRemote(0));
                        }
                    }
                }
            });
        });
    }

    @Test
    public void testPartitionRemoteBitDefaultsFalseAndSentinelSafe() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            assertMemoryLeak(() -> {
                String tableName = "txnRemoteDefault";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        long ts0 = 0;
                        long ts1 = Micros.DAY_MICROS;
                        tw.updatePartitionSizeByTimestamp(ts0, 1);
                        tw.updatePartitionSizeByTimestamp(ts1, 1);

                        // partition 0: native (no parquet) -> no native seqTxn yet
                        Assert.assertEquals(-1L, tw.getNativePartitionSeqTxn(0));
                        Assert.assertFalse(tw.isPartitionRemote(0));
                        Assert.assertFalse(tw.isPartitionRemoteByPartitionTimestamp(ts0));

                        // partition 1: parquet with file size 1024 -> default REMOTE=false
                        tw.setPartitionParquet(ts1, 1024L);
                        Assert.assertEquals(1024L, tw.getPartitionParquetFileSize(1));
                        Assert.assertFalse(tw.isPartitionRemote(1));
                        Assert.assertFalse(tw.isPartitionRemoteByPartitionTimestamp(ts1));
                    }
                }
            });
        });
    }

    @Test
    public void testPartitionRemoteBitIndependentFromReadOnly() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            assertMemoryLeak(() -> {
                String tableName = "txnRemoteReadOnly";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        long ts = 0;
                        tw.updatePartitionSizeByTimestamp(ts, 1);
                        tw.setPartitionParquet(ts, 4096L);

                        // flip read_only -> REMOTE still 0
                        tw.setPartitionReadOnlyByTimestamp(ts, true);
                        Assert.assertTrue(tw.isPartitionReadOnly(0));
                        Assert.assertFalse(tw.isPartitionRemote(0));
                        Assert.assertEquals(4096L, tw.getPartitionParquetFileSize(0));

                        // flip REMOTE -> read_only still 1, size still 4096
                        tw.setPartitionRemote(0, true);
                        Assert.assertTrue(tw.isPartitionReadOnly(0));
                        Assert.assertTrue(tw.isPartitionRemote(0));
                        Assert.assertEquals(4096L, tw.getPartitionParquetFileSize(0));

                        // clear read_only -> REMOTE still 1
                        tw.setPartitionReadOnlyByTimestamp(ts, false);
                        Assert.assertFalse(tw.isPartitionReadOnly(0));
                        Assert.assertTrue(tw.isPartitionRemote(0));
                        Assert.assertEquals(4096L, tw.getPartitionParquetFileSize(0));
                    }
                }
            });
        });
    }

    @Test
    public void testPartitionRemoteBitToggleStripsBitFromGetter() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            assertMemoryLeak(() -> {
                String tableName = "txnRemoteToggle";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        long ts = 0;
                        long fileLength = 1L << 30; // 1 GiB - non-trivial value, bit 63 still 0
                        tw.updatePartitionSizeByTimestamp(ts, 1);
                        tw.setPartitionParquet(ts, fileLength);

                        // set REMOTE -> getter strips bit, returns original size
                        tw.setPartitionRemote(0, true);
                        Assert.assertTrue(tw.isPartitionRemote(0));
                        Assert.assertEquals(fileLength, tw.getPartitionParquetFileSize(0));

                        // clear REMOTE -> size still preserved
                        tw.setPartitionRemoteByTimestamp(ts, false);
                        Assert.assertFalse(tw.isPartitionRemote(0));
                        Assert.assertEquals(fileLength, tw.getPartitionParquetFileSize(0));

                        // round-trip via raw index variant
                        tw.setPartitionRemoteByRawIndex(0, true);
                        Assert.assertTrue(tw.isPartitionRemoteByRawIndex(0));
                        Assert.assertEquals(fileLength, tw.getPartitionParquetFileSize(0));
                    }
                }
            });
        });
    }

    @Test
    public void testPartitionVersionTupleSurvivesTxnReopen() throws Exception {
        // The most safety-critical round-trip: every [F G R U] flag and the offset-3 value must
        // survive serialization to _txn and a fresh TxReader load. Writes a representative state per
        // partition via the raw mutators, commits, reopens a raw TxReader (which loads _txn without
        // opening partition files), and asserts the full tuple. Partitions 1 and 4 are remote, so
        // their offset-3 word has bit 63 (the sign bit) set on disk -- this also proves REMOTE and a
        // negative-looking word survive serialization and still read back as a non-negative value.
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            String tableName = "verPersist";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
            model.timestamp();
            AbstractCairoTest.create(model);

            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                int tsType = TableUtils.getTimestampType(model);

                // idx : state                F  G  R  U   off-3 value
                //  0  : NATIVE               0  0  0  0   seqTxn
                //  1  : NATIVE_REMOTE        0  0  0  1   seqTxn  (bit 63 set on disk)
                //  2  : NATIVE_SEALED        0  0  1  1   seqTxn
                //  3  : PARQUET_LOCAL        1  1  0  0   file size
                //  4  : PARQUET_REMOTE       1  1  0  1   file size  (bit 63 set on disk)
                //  5  : PARQUET_SEALED       1  1  1  1   file size
                //  6  : REMOTELY_SERVED      1  0  1  1   file size
                //  7  : cleared native       0  0  0  0   -1 (no version)
                try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), tsType, PartitionBy.DAY)) {
                    for (int i = 0; i < 8; i++) {
                        tw.updatePartitionSizeByTimestamp(i * Micros.DAY_MICROS, i + 1);
                    }
                    tw.updateMaxTimestamp(8 * Micros.DAY_MICROS + 1);
                    tw.finishPartitionSizeUpdate();
                    applyVersionState(tw, 0, false, false, false, false, 101L);
                    applyVersionState(tw, 1, false, false, false, true, 102L);
                    applyVersionState(tw, 2, false, false, true, true, 103L);
                    applyVersionState(tw, 3, true, true, false, false, 4096L);
                    applyVersionState(tw, 4, true, true, false, true, 8192L);
                    applyVersionState(tw, 5, true, true, true, true, 9000L);
                    applyVersionState(tw, 6, true, false, true, true, 10_000L);
                    // partition 7 left untouched -> cleared (-1)
                    tw.commit(new ObjList<>());
                }

                try (TxReader tr = new TxReader(ff)) {
                    tr.ofRO(path.$(), tsType, PartitionBy.DAY);
                    tr.unsafeLoadAll();
                    Assert.assertEquals(8, tr.getPartitionCount());
                    assertVersionState(tr, 0, false, false, false, false, 101L);
                    assertVersionState(tr, 1, false, false, false, true, 102L);
                    assertVersionState(tr, 2, false, false, true, true, 103L);
                    assertVersionState(tr, 3, true, true, false, false, 4096L);
                    assertVersionState(tr, 4, true, true, false, true, 8192L);
                    assertVersionState(tr, 5, true, true, true, true, 9000L);
                    assertVersionState(tr, 6, true, false, true, true, 10_000L);
                    // cleared partition: no version, no flags
                    Assert.assertEquals(-1L, tr.getPartitionVersion(7));
                    Assert.assertFalse(tr.isPartitionParquet(7));
                    Assert.assertFalse(tr.isPartitionRemote(7));
                }
            }
        });
    }

    @Test
    public void testPrimitiveMutatorMatrixPinsTuple() throws Exception {
        // Side-by-side matrix of the offset-3 mutators not covered elsewhere, asserting the full
        // (format, generated, readOnly, remote, value) tuple after every step. Pins keep-vs-reset and
        // the format-vs-data split: a format flip preserves REMOTE in BOTH directions, while the
        // parquet_generated toggle leaves offset 3 (value + REMOTE) untouched.
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            assertMemoryLeak(() -> {
                String tableName = "verMatrix";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        long ts = 0;
                        tw.updatePartitionSizeByTimestamp(ts, 1);

                        // stamp native seqTxn: native, no generated, no remote, value 7
                        tw.setPartitionSeqTxnByRawIndex(0, 7L);
                        Assert.assertFalse("native after stamp", tw.isPartitionParquet(0));
                        Assert.assertFalse("stamp clears generated", tw.isPartitionParquetGenerated(0));
                        Assert.assertFalse("stamp clears remote", tw.isPartitionRemote(0));
                        Assert.assertEquals(7L, tw.getNativePartitionSeqTxn(0));

                        // mark remote, then flip native -> parquet: REMOTE survives, value becomes the file size
                        tw.setPartitionRemote(0, true);
                        tw.setPartitionParquet(ts, 4096L);
                        Assert.assertTrue("parquet after format flip", tw.isPartitionParquet(0));
                        Assert.assertFalse("setPartitionParquet leaves generated", tw.isPartitionParquetGenerated(0));
                        Assert.assertFalse(tw.isPartitionReadOnly(0));
                        Assert.assertTrue("remote preserved native -> parquet", tw.isPartitionRemote(0));
                        Assert.assertEquals(4096L, tw.getPartitionParquetFileSize(0));

                        // flip parquet -> native: REMOTE still survives, value becomes the seqTxn
                        tw.setPartitionNative(ts, 9L);
                        Assert.assertFalse("native after second flip", tw.isPartitionParquet(0));
                        Assert.assertFalse(tw.isPartitionParquetGenerated(0));
                        Assert.assertFalse(tw.isPartitionReadOnly(0));
                        Assert.assertTrue("remote preserved parquet -> native", tw.isPartitionRemote(0));
                        Assert.assertEquals(9L, tw.getNativePartitionSeqTxn(0));

                        // toggle parquet_generated (offset 1): offset 3 value and REMOTE untouched
                        tw.setPartitionParquetGenerated(0, true);
                        Assert.assertTrue(tw.isPartitionParquetGenerated(0));
                        Assert.assertTrue("remote untouched by generated set", tw.isPartitionRemote(0));
                        Assert.assertEquals(9L, tw.getNativePartitionSeqTxn(0));

                        tw.setPartitionParquetGeneratedByRawIndex(0, false);
                        Assert.assertFalse(tw.isPartitionParquetGenerated(0));
                        Assert.assertTrue("remote untouched by generated reset", tw.isPartitionRemote(0));
                        Assert.assertEquals(9L, tw.getNativePartitionSeqTxn(0));
                    }
                }
            });
        });
    }

    @Test
    public void testReadsLegacyOffset3Words() throws Exception {
        // Backward compatibility: a pre-feature _txn carries raw offset-3 words with no flag bits set.
        // A current reader upgrades them in place. The disk round-trip covers what a writer can emit -- a
        // legacy all-clear word (raw 0), a plain parquet file size, and a manual CONVERT TO PARQUET
        // (parquet + generated, not remote, which a backup keeps locally). The legacy -1 sentinel, which
        // the value-masking writer can no longer produce, is poked into the loaded snapshot. NB: bit 63
        // would read as a negative size to an old binary, so setting REMOTE is a one-way upgrade hazard.
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            String tableName = "verLegacy";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
            model.timestamp();
            AbstractCairoTest.create(model);

            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                int tsType = TableUtils.getTimestampType(model);
                long ts2 = 2 * Micros.DAY_MICROS;
                long ts3 = 3 * Micros.DAY_MICROS;

                // idx 0: raw 0 (legacy all-clear, fresh native); idx 1: raw -1 (poked after load);
                // idx 2: plain parquet file size, no flag bits; idx 3: manual CONVERT TO PARQUET.
                try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), tsType, PartitionBy.DAY)) {
                    for (int i = 0; i < 4; i++) {
                        tw.updatePartitionSizeByTimestamp(i * Micros.DAY_MICROS, 1);
                    }
                    tw.updateMaxTimestamp(4 * Micros.DAY_MICROS + 1);
                    tw.finishPartitionSizeUpdate();
                    // idx 0 and idx 1 left native with the fresh raw 0 offset-3 word
                    tw.setPartitionParquet(ts2, 4096L);
                    tw.setPartitionParquet(ts3, 4096L);
                    tw.setPartitionParquetGenerated(3, true);
                    tw.commit(new ObjList<>());
                }

                try (RawOffset3Reader tr = new RawOffset3Reader(ff)) {
                    tr.ofRO(path.$(), tsType, PartitionBy.DAY);
                    tr.unsafeLoadAll();
                    Assert.assertEquals(4, tr.getPartitionCount());
                    Assert.assertEquals("legacy all-clear word survives on disk as 0", 0L, tr.rawOffset3(0));
                    tr.pokeRawOffset3(1, -1L);

                    // legacy cleared words: native, no version, upgradable in place
                    Assert.assertFalse(tr.isPartitionParquet(0));
                    Assert.assertFalse(tr.isPartitionParquet(1));
                    Assert.assertEquals("legacy 0 reads as no version", -1L, tr.getPartitionVersion(0));
                    Assert.assertEquals("legacy -1 reads as no version", -1L, tr.getPartitionVersion(1));
                    Assert.assertFalse(tr.isPartitionRemote(0));
                    Assert.assertFalse(tr.isPartitionRemote(1));

                    // plain parquet file size: no flag bits set, so not remote
                    Assert.assertTrue(tr.isPartitionParquet(2));
                    Assert.assertEquals(4096L, tr.getPartitionParquetFileSize(2));
                    Assert.assertFalse("plain file size has no REMOTE bit", tr.isPartitionRemote(2));

                    // manual CONVERT TO PARQUET: parquet + generated, not remote -> not remotely served
                    Assert.assertTrue(tr.isPartitionParquet(3));
                    Assert.assertTrue(tr.isPartitionParquetGenerated(3));
                    Assert.assertFalse(tr.isPartitionRemote(3));
                    Assert.assertFalse("a backup keeps the local data.parquet for a generated partition",
                            tr.isPartitionRemotelyServed(3));
                }
            }
        });
    }

    @Test
    public void testRemotelyServedFormatGatesNativeSealedVsRemoteParquet() throws Exception {
        // The format bit discriminates the [0011] NATIVE_SEALED vs [1011] remotely-served near-collision. Both are
        // [generated=false, readOnly=true, remote=true]; only the parquet-format partition is
        // remotely served. A predicate testing only !generated && remote would wrongly treat the native
        // partition as remotely served.
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            String tableName = "verRemotelyServed";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
            model.timestamp();
            AbstractCairoTest.create(model);

            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                int tsType = TableUtils.getTimestampType(model);

                // idx 0: NATIVE_SEALED [0011] seqTxn; idx 1: REMOTELY_SERVED [1011] parquet file size.
                try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), tsType, PartitionBy.DAY)) {
                    tw.updatePartitionSizeByTimestamp(0, 1);
                    tw.updatePartitionSizeByTimestamp(Micros.DAY_MICROS, 1);
                    tw.updateMaxTimestamp(2 * Micros.DAY_MICROS + 1);
                    tw.finishPartitionSizeUpdate();
                    applyVersionState(tw, 0, false, false, true, true, 103L);
                    applyVersionState(tw, 1, true, false, true, true, 4096L);
                    tw.commit(new ObjList<>());
                }

                try (TxReader tr = new TxReader(ff)) {
                    tr.ofRO(path.$(), tsType, PartitionBy.DAY);
                    tr.unsafeLoadAll();
                    assertVersionState(tr, 0, false, false, true, true, 103L);
                    assertVersionState(tr, 1, true, false, true, true, 4096L);
                    Assert.assertFalse("native sealed is not remotely served", tr.isPartitionRemotelyServed(0));
                    Assert.assertTrue("the [1011] partition is remotely served", tr.isPartitionRemotelyServed(1));
                }
            }
        });
    }

    @Test
    public void testRowCountUpdatePreservesFlagsAndOffset3() throws Exception {
        // A row-count write (updatePartitionSizeByTimestamp on an existing partition) must not be
        // mistaken for a data-version write: it rewrites only the offset-1 size bits, preserving the
        // format/generated/readOnly/remote flags and leaving the offset-3 file size untouched.
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            String tableName = "verRowCount";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
            model.timestamp();
            AbstractCairoTest.create(model);

            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                int tsType = TableUtils.getTimestampType(model);
                long ts = 0;

                try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), tsType, PartitionBy.DAY)) {
                    tw.updatePartitionSizeByTimestamp(ts, 1);
                    tw.updateMaxTimestamp(Micros.DAY_MICROS + 1);
                    tw.finishPartitionSizeUpdate();
                    // fully-flagged parquet tuple: parquet, generated, readOnly, remote, file size 4096
                    applyVersionState(tw, 0, true, true, true, true, 4096L);

                    tw.updatePartitionSizeByTimestamp(ts, 5);

                    Assert.assertEquals("row count updated", 5L, tw.getPartitionSize(0));
                    Assert.assertTrue("format preserved by row-count write", tw.isPartitionParquet(0));
                    Assert.assertTrue("generated preserved by row-count write", tw.isPartitionParquetGenerated(0));
                    Assert.assertTrue("read-only preserved by row-count write", tw.isPartitionReadOnly(0));
                    Assert.assertTrue("remote preserved by row-count write", tw.isPartitionRemote(0));
                    Assert.assertEquals("offset-3 file size untouched by row-count write",
                            4096L, tw.getPartitionParquetFileSize(0));
                }
            }
        });
    }

    @Test
    public void testSetPartitionRemoteOnClearedSlotNormalizes() throws Exception {
        // Setting REMOTE on a cleared slot must not throw and must not strand the bit on the
        // sentinel's stray bits (e.g. -1 | REMOTE == -1, still read as cleared): it normalizes the
        // slot to 0 first, so REMOTE reads back true while the version still reads -1 -- the word
        // carries no VALID stamp, and REMOTE alone must not manufacture a trusted version. Covers
        // both cleared words -- the -1 sentinel and a plain 0 slot.
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            assertMemoryLeak(() -> {
                String tableName = "txnRemoteClearedNormalizes";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        long ts = 0;
                        tw.updatePartitionSizeByTimestamp(ts, 1);

                        // Slot starts at the cleared sentinel (no setPartitionParquet).
                        Assert.assertEquals(-1L, tw.getNativePartitionSeqTxn(0));
                        tw.setPartitionRemote(0, true);
                        Assert.assertTrue("REMOTE set on the -1 slot reads back true", tw.isPartitionRemote(0));
                        Assert.assertEquals("an unstamped slot reads no-version even with REMOTE set",
                                -1L, tw.getNativePartitionSeqTxn(0));

                        // Clearing REMOTE returns the slot to the cleared state.
                        tw.setPartitionRemote(0, false);
                        Assert.assertFalse(tw.isPartitionRemote(0));
                        Assert.assertEquals(-1L, tw.getNativePartitionSeqTxn(0));

                        // Plant a clean 0 word (a stamped seqTxn of 0 masks to 0); same normalization.
                        tw.setPartitionSeqTxnByRawIndex(0, 0L);
                        Assert.assertEquals(-1L, tw.getNativePartitionSeqTxn(0));
                        tw.setPartitionRemote(0, true);
                        Assert.assertTrue("REMOTE set on the 0 slot reads back true", tw.isPartitionRemote(0));
                    }
                }
            });
        });
    }

    @Test
    public void testSetPartitionRemoteOnStampedNativePartition() throws Exception {
        // A stamped native partition carries its seqTxn in the file-size slot (offset 3 != -1),
        // so setPartitionParquetRemote must no longer trip the "no parquet" (raw == -1) guard, and the
        // seqTxn value bits must survive toggling REMOTE.
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            assertMemoryLeak(() -> {
                String tableName = "txnRemoteOnStamped";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        long ts = 0;
                        tw.updatePartitionSizeByTimestamp(ts, 1);
                        tw.setPartitionSeqTxnByRawIndex(0, 7L);
                        Assert.assertEquals(7L, tw.getNativePartitionSeqTxn(0));

                        // does not throw, unlike the -1 sentinel case
                        tw.setPartitionRemote(0, true);
                        Assert.assertTrue(tw.isPartitionRemote(0));
                        Assert.assertEquals("seqTxn survives setting REMOTE", 7L, tw.getNativePartitionSeqTxn(0));
                        // Invariant: REMOTE lives in bit 63 (the sign bit) of offset 3, so the raw
                        // word is negative -- but every value reader masks it off. Consumers that treat
                        // "version < 0" as "no version" (e.g. markPartitionParquetReady) must never
                        // misread a remote partition as unstamped.
                        Assert.assertTrue("a remote partition's seqTxn must read non-negative",
                                tw.getNativePartitionSeqTxn(0) >= 0);

                        tw.setPartitionRemote(0, false);
                        Assert.assertFalse(tw.isPartitionRemote(0));
                        Assert.assertEquals("seqTxn survives clearing REMOTE", 7L, tw.getNativePartitionSeqTxn(0));
                    }
                }
            });
        });
    }

    @Test
    public void testSquashCounterOverflow() throws IOException {
        try (Path p = new Path()) {
            try (TxWriter tw = new TxWriter(engine.getConfiguration().getFilesFacade(), engine.getConfiguration())) {
                loadTxnWriter(tw, p, "/txn/sys.acl_entities~1/_txn");
                //noinspection StatementWithEmptyBody
                while (tw.incrementPartitionSquashCounter(0)) {
                }

                Assert.assertEquals(TxReader.PARTITION_SQUASH_COUNTER_MAX, tw.getPartitionSquashCount(0));

                Assert.assertFalse(tw.incrementPartitionSquashCounter(0));
                Assert.assertEquals(TxReader.PARTITION_SQUASH_COUNTER_MAX, tw.getPartitionSquashCount(0));
            }
        }
    }

    @Test
    public void testStampPartitionSeqTxnClearsRemoteAndGenerated() throws Exception {
        // stampPartitionSeqTxnByRawIndex writes the seqTxn with bit 63 masked off and clears
        // parquet_generated, leaving a plain native partition whose version reads back.
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            assertMemoryLeak(() -> {
                String tableName = "txnStampSeqTxn";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        long ts = 0;
                        tw.updatePartitionSizeByTimestamp(ts, 1);
                        // fresh native partition: unknown version
                        Assert.assertEquals(-1L, tw.getNativePartitionSeqTxn(0));

                        tw.setPartitionSeqTxnByRawIndex(0, 123L);

                        Assert.assertEquals(123L, tw.getNativePartitionSeqTxn(0));
                        Assert.assertFalse("stamp clears REMOTE", tw.isPartitionRemote(0));
                        Assert.assertFalse("stamp clears parquet_generated", tw.isPartitionParquetGenerated(0));
                        Assert.assertFalse("partition stays native", tw.isPartitionParquet(0));
                    }
                }
            });
        });
    }

    @Test
    public void testStampPartitionSeqTxnRestampClearsRemote() throws Exception {
        // The WAL-apply invariant: any write (a re-stamp) advances the version and clears a
        // previously-set REMOTE bit in the same store.
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            assertMemoryLeak(() -> {
                String tableName = "txnRestampSeqTxn";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        long ts = 0;
                        tw.updatePartitionSizeByTimestamp(ts, 1);
                        tw.setPartitionSeqTxnByRawIndex(0, 5L);
                        tw.setPartitionRemote(0, true);
                        Assert.assertTrue(tw.isPartitionRemote(0));

                        tw.setPartitionSeqTxnByRawIndex(0, 9L);

                        Assert.assertEquals(9L, tw.getNativePartitionSeqTxn(0));
                        Assert.assertFalse("re-stamp clears REMOTE", tw.isPartitionRemote(0));
                    }
                }
            });
        });
    }

    @Test
    public void testToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            assertMemoryLeak(() -> {

                String tableName = "txntest";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    int testPartitionCount = 2;
                    try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                        txWriter.ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY);
                        for (int i = 0; i < testPartitionCount; i++) {
                            txWriter.updatePartitionSizeByTimestamp(i * Micros.DAY_MICROS, i + 1);
                        }
                        TestUtils.assertContains(txWriter.toString(), """
                                [
                                {ts: '1970-01-01T00:00:00.000000Z', rowCount: 1, nameTxn: -1},
                                {ts: '1970-01-02T00:00:00.000000Z', rowCount: 2, nameTxn: -1}
                                ]""");
                    }
                }
            });
        });
    }

    @Test
    public void testTxReadTruncateConcurrent() throws Throwable {
        TestUtils.assertMemoryLeak(() -> {
            int readerThreads = 2;

            CyclicBarrier start = new CyclicBarrier(readerThreads + 1);
            AtomicInteger done = new AtomicInteger();
            AtomicInteger reloadCount = new AtomicInteger();
            int iterations = 1000;
            ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
            Rnd rnd = TestUtils.generateRandom(LOG);

            String tableName = "testTxReadWriteConcurrent";
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            int maxPartitionCount = Math.max((int) (Files.PAGE_SIZE / 8 / 4), 4096);
            int maxSymbolCount = (int) (Files.PAGE_SIZE / 8 / 4);
            AtomicInteger partitionCountCheck = new AtomicInteger();

            TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR);
            model.timestamp();
            AbstractCairoTest.create(model);
            int truncateIteration = 33;
            Thread writerThread = createWriterThread(
                    start,
                    done,
                    iterations,
                    exceptions,
                    rnd,
                    tableName,
                    ff,
                    maxPartitionCount,
                    maxSymbolCount,
                    partitionCountCheck,
                    truncateIteration,
                    TableUtils.getTimestampType(model)
            );

            Rnd readerRnd = new Rnd(rnd.nextLong(), rnd.nextLong());
            Thread[] readers = new Thread[readerThreads];
            for (int th = 0; th < readerThreads; th++) {
                Thread readerThread = new Thread(() -> {
                    try (
                            Path path = new Path();
                            TxReader txReader = new TxReader(ff)
                    ) {
                        TableToken tableToken = engine.verifyTableName(tableName);
                        path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                        txReader.ofRO(path.$(), TableUtils.getTimestampType(model), PartitionBy.HOUR);
                        MillisecondClock clock = engine.getConfiguration().getMillisecondClock();
                        long duration = 5_000;
                        start.await();
                        while (done.get() == 0 || partitionCountCheck.get() != txReader.getPartitionCount() - 1) {
                            TableUtils.safeReadTxn(txReader, clock, duration);
                            long txn = txReader.getTxn();

                            // Each writer iteration creates 2 txn commits.
                            // Every truncateIteration writer iteration truncates.
                            if (((txn - 1) / 2) % truncateIteration == 0) {
                                // must be truncated
                                if (txReader.getPartitionCount() > 1) {
                                    Assert.assertTrue(
                                            "Txn " + txn + " not read as truncated. Partition count: " + txReader.getPartitionCount(),
                                            txReader.getPartitionCount() < 2);
                                }
                            } else if (txReader.getPartitionCount() > 2) {
                                reloadCount.incrementAndGet();
                            }
                            if (readerRnd.nextBoolean()) {
                                txReader.ofRO(path.$(), TableUtils.getTimestampType(model), PartitionBy.HOUR);
                            }
                            Os.pause();
                        }

                    } catch (Throwable e) {
                        exceptions.add(e);
                        LOG.error().$(e).$();
                    }
                });
                readers[th] = readerThread;
                readerThread.start();
            }

            writerThread.start();

            writerThread.join();
            for (int th = 0; th < readerThreads; th++) {
                readers[th].join();
            }

            if (!exceptions.isEmpty()) {
                Assert.fail(exceptions.poll().toString());
            }
            Assert.assertTrue(reloadCount.get() > 10);
            LOG.infoW().$("total reload count ").$(reloadCount.get()).$();
        });
    }

    @Test
    public void testTxReadWriteConcurrent() throws Throwable {
        TestUtils.assertMemoryLeak(() -> {
            int readerThreads = 4;

            CyclicBarrier start = new CyclicBarrier(readerThreads + 1);
            AtomicInteger done = new AtomicInteger();
            AtomicInteger reloadCount = new AtomicInteger();
            int iterations = 1000;
            ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
            Rnd rnd = TestUtils.generateRandom(LOG);

            String tableName = "testTxReadWriteConcurrent";
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            int maxPartitionCount = Math.max((int) (Files.PAGE_SIZE / 8 / 4), 4096);
            int maxSymbolCount = (int) (Files.PAGE_SIZE / 8 / 4);
            AtomicInteger partitionCountCheck = new AtomicInteger();

            TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR);
            model.timestamp();
            AbstractCairoTest.create(model);
            Thread writerThread = createWriterThread(
                    start,
                    done,
                    iterations,
                    exceptions,
                    rnd,
                    tableName,
                    ff,
                    maxPartitionCount,
                    maxSymbolCount,
                    partitionCountCheck,
                    Integer.MAX_VALUE,
                    TableUtils.getTimestampType(model)
            );

            Rnd readerRnd = TestUtils.generateRandom(LOG);

            Thread[] readers = new Thread[readerThreads];
            for (int th = 0; th < readerThreads; th++) {
                Thread readerThread = new Thread(() -> {
                    try (
                            Path path = new Path();
                            TxReader txReader = new TxReader(ff)
                    ) {
                        TableToken tableToken = engine.verifyTableName(tableName);
                        path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                        txReader.ofRO(path.$(), TableUtils.getTimestampType(model), PartitionBy.HOUR);
                        MillisecondClock clock = engine.getConfiguration().getMillisecondClock();
                        long duration = 30_000;
                        start.await();
                        while (done.get() == 0) {
                            TableUtils.safeReadTxn(txReader, clock, duration);
                            reloadCount.incrementAndGet();
                            Assert.assertTrue(txReader.getPartitionCount() <= maxPartitionCount);
                            Assert.assertTrue(txReader.getSymbolColumnCount() <= maxSymbolCount);

                            for (int i = txReader.getSymbolColumnCount() - 1; i > -1; i--) {
                                if (i != txReader.getSymbolColumnCount()) {
                                    String trace = String.format(
                                            "[txn=%d, structureVersion=%d, partitionCount=%d, symbolCount=%d] ",
                                            txReader.getTxn(),
                                            txReader.getMetadataVersion(),
                                            txReader.getPartitionCount(),
                                            txReader.getSymbolColumnCount()
                                    );
                                    Assert.assertEquals(trace, i, txReader.getSymbolValueCount(i));
                                }
                            }

                            long offset = txReader.getTxn() - txReader.getMetadataVersion();
                            for (int i = txReader.getPartitionCount() - 2; i > -1; i--) {
                                if (offset + i != txReader.getPartitionSize(i)) {
                                    String trace = String.format(
                                            "[txn=%d, structureVersion=%d, partitionCount=%d, symbolCount=%d] ",
                                            txReader.getTxn(),
                                            txReader.getMetadataVersion(),
                                            txReader.getPartitionCount(),
                                            txReader.getSymbolColumnCount()
                                    );
                                    Assert.assertEquals(trace + buildActualSizes(txReader), offset + i, txReader.getPartitionSize(i));
                                }
                            }

                            if (readerRnd.nextBoolean()) {
                                // Reopen txn file
                                txReader.ofRO(path.$(), TableUtils.getTimestampType(model), PartitionBy.HOUR);
                            }
                        }
                        TableUtils.safeReadTxn(txReader, clock, duration);
                        Assert.assertEquals(partitionCountCheck.get(), txReader.getPartitionCount() - 1);

                    } catch (Throwable e) {
                        LOG.error().$(e).$();
                        exceptions.add(e);
                    }
                });
                readers[th] = readerThread;
                readerThread.start();
            }

            writerThread.start();

            writerThread.join();
            for (int th = 0; th < readerThreads; th++) {
                readers[th].join();
            }

            if (!exceptions.isEmpty()) {
                Assert.fail(exceptions.poll().toString());
            }
            Assert.assertTrue(reloadCount.get() > 10);
            LOG.infoW().$("total reload count ").$(reloadCount.get()).$();
        });
    }

    @Test
    public void testValidBitLifecycleAcrossFormatFlips() throws Exception {
        // VALID is managed by the stamping setters and setPartitionFormat: set with every positive
        // native stamp (the flip to native included), cleared on the flip to parquet (a file size is
        // valid without it), preserved by setPartitionRemote, and never set by a 0 stamp -- the 0
        // stamp writes the cleared word, so "valid with value 0" is unrepresentable by a writer.
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            String tableName = "verValidFlips";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
            model.timestamp();
            AbstractCairoTest.create(model);

            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                int tsType = TableUtils.getTimestampType(model);
                try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), tsType, PartitionBy.DAY)) {
                    long ts = 0;
                    tw.updatePartitionSizeByTimestamp(ts, 1);

                    // positive stamp -> VALID
                    tw.setPartitionSeqTxn(0, 7L);
                    Assert.assertNotEquals(0L, RawOffset3Reader.rawOffset3Of(tw, 0) & TxReader.PARTITION_SEQ_TXN_VALID_BIT);
                    Assert.assertEquals(7L, tw.getNativePartitionSeqTxn(0));

                    // REMOTE preserves VALID; the stamp stays trusted
                    tw.setPartitionRemote(0, true);
                    Assert.assertNotEquals(0L, RawOffset3Reader.rawOffset3Of(tw, 0) & TxReader.PARTITION_SEQ_TXN_VALID_BIT);
                    Assert.assertEquals(7L, tw.getNativePartitionSeqTxn(0));

                    // flip to parquet: VALID cleared, REMOTE preserved, the file size reads
                    tw.setPartitionParquet(ts, 4096L);
                    Assert.assertEquals(0L, RawOffset3Reader.rawOffset3Of(tw, 0) & TxReader.PARTITION_SEQ_TXN_VALID_BIT);
                    Assert.assertTrue(tw.isPartitionRemote(0));
                    Assert.assertEquals(4096L, tw.getPartitionParquetFileSize(0));

                    // flip back to native with a real seqTxn: VALID set, reads through
                    tw.setPartitionNative(ts, 9L);
                    Assert.assertNotEquals(0L, RawOffset3Reader.rawOffset3Of(tw, 0) & TxReader.PARTITION_SEQ_TXN_VALID_BIT);
                    Assert.assertEquals(9L, tw.getNativePartitionSeqTxn(0));

                    // 0 stamp: the cleared word, no VALID -- reads as the -1 sentinel
                    tw.setPartitionSeqTxn(0, 0L);
                    Assert.assertEquals(0L, RawOffset3Reader.rawOffset3Of(tw, 0));
                    Assert.assertEquals(-1L, tw.getNativePartitionSeqTxn(0));

                    // flip to native with seqTxn 0 (the non-WAL convert-back): cleared, untrusted
                    tw.setPartitionParquet(ts, 4096L);
                    tw.setPartitionNative(ts, 0L);
                    Assert.assertEquals(0L, RawOffset3Reader.rawOffset3Of(tw, 0) & TxReader.PARTITION_SEQ_TXN_VALID_BIT);
                    Assert.assertEquals(-1L, tw.getNativePartitionSeqTxn(0));
                }
            }
        });
    }

    private static void applyVersionState(TxWriter tw, int idx, boolean parquet, boolean generated, boolean readOnly, boolean remote, long value) {
        long ts = tw.getPartitionTimestampByIndex(idx);
        if (parquet) {
            // parquet-format: the value is the file size, written via setPartitionParquet
            tw.setPartitionParquet(ts, value);
        } else {
            // native-format: the value is the seqTxn (the stamp clears generated + REMOTE)
            tw.setPartitionSeqTxnByRawIndex(idx * TableUtils.LONGS_PER_TX_ATTACHED_PARTITION, value);
        }
        tw.setPartitionParquetGenerated(idx, generated);
        tw.setPartitionReadOnly(idx, readOnly);
        // set REMOTE last: every value write above clears it by construction
        tw.setPartitionRemote(idx, remote);
    }

    private static void assertVersionState(TxReader tr, int idx, boolean parquet, boolean generated, boolean readOnly, boolean remote, long value) {
        Assert.assertEquals("format @" + idx, parquet, tr.isPartitionParquet(idx));
        Assert.assertEquals("generated @" + idx, generated, tr.isPartitionParquetGenerated(idx));
        Assert.assertEquals("read-only @" + idx, readOnly, tr.isPartitionReadOnly(idx));
        Assert.assertEquals("remote @" + idx, remote, tr.isPartitionRemote(idx));
        long readValue = parquet ? tr.getPartitionParquetFileSize(idx) : tr.getNativePartitionSeqTxn(idx);
        Assert.assertEquals("offset-3 value @" + idx, value, readValue);
        // the value reader masks bit 63, so a remote partition never reads as a negative version
        Assert.assertTrue("version reads non-negative @" + idx, readValue >= 0);
    }

    private static void loadTxnWriter(TxWriter tw, Path p, String resourceFile) throws IOException {
        try (final InputStream is = TxnTest.class.getResourceAsStream(resourceFile)) {
            // Create temp file
            java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("test-", ".tmp");
            tempFile.toFile().deleteOnExit(); // Ensure it is deleted on exit

            // Copy resource content to temp file
            java.nio.file.Files.copy(Objects.requireNonNull(is), tempFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

            p.of(tempFile.toString()).$();
            tw.ofRW(p.$(), ColumnType.TIMESTAMP, PartitionBy.MONTH);
            tw.unsafeLoadAll();
        }
    }

    private String buildActualSizes(TxReader txReader) {
        StringSink ss = new StringSink();
        for (int i = 0; i < txReader.getPartitionCount() - 1; i++) {
            if (i > 0) {
                ss.put(',');
            }
            ss.put(txReader.getPartitionSize(i));
        }
        return ss.toString();
    }

    @NotNull
    private Thread createWriterThread(
            CyclicBarrier start,
            AtomicInteger done,
            int iterations,
            ConcurrentLinkedQueue<Throwable> exceptions,
            Rnd rnd,
            String tableName,
            FilesFacade ff,
            int maxPartitionCount,
            int maxSymbolCount,
            AtomicInteger partitionCountCheck,
            int truncateIteration,
            int timestampType
    ) {
        ObjList<SymbolCountProvider> symbolCounts = new ObjList<>();
        ObjList<SymbolCountProvider> zeroSymbolCounts = new ObjList<>();
        return new Thread(() -> {
            try (
                    Path path = new Path();
                    TxWriter txWriter = new TxWriter(ff, configuration)
            ) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                txWriter.ofRW(path.$(), timestampType, PartitionBy.HOUR);

                start.await();
                for (int j = 0; j < iterations; j++) {
                    if (j % truncateIteration == 0) {
                        txWriter.truncate(0, zeroSymbolCounts);
                        LOG.info().$("writer truncated at ").$(txWriter.getTxn()).$();
                        // Create last partition back.
                        txWriter.setMaxTimestamp((maxPartitionCount + 1) * Micros.HOUR_MICROS);
                        txWriter.updatePartitionSizeByTimestamp(txWriter.getMaxTimestamp() * Micros.HOUR_MICROS, 1);
                        txWriter.commit(symbolCounts);
                        partitionCountCheck.set(0);
                    } else {
                        // Set txn file with random number of symbols and random number of partitions
                        int symbolCount = rnd.nextInt(maxSymbolCount);
                        if (symbolCount > symbolCounts.size()) {
                            for (int i = symbolCounts.size(); i < symbolCount; i++) {
                                symbolCounts.add(new SymbolCountProviderImpl(i));
                                zeroSymbolCounts.add(new SymbolCountProviderImpl(0));
                            }
                        } else {
                            symbolCounts.setPos(symbolCount);
                            zeroSymbolCounts.setPos(symbolCount);
                        }
                        txWriter.bumpMetadataAndColumnStructureVersion(symbolCounts);

                        // Set random number of partitions
                        int partitionCount = rnd.nextInt(maxPartitionCount);
                        int partitions = txWriter.getPartitionCount() - 1; // Last partition always stays

                        long offset = txWriter.getTxn() + 1 - txWriter.getMetadataVersion();
                        // Add / Update
                        for (int i = 0; i < partitionCount; i++) {
                            txWriter.updatePartitionSizeByTimestamp(i * Micros.HOUR_MICROS, offset + i);
                        }
                        // Remove from the end
                        for (int i = partitionCount; i < partitions; i++) {
                            txWriter.removeAttachedPartitions(i * Micros.HOUR_MICROS);
                        }
                        txWriter.bumpPartitionTableVersion();
                        assert txWriter.getPartitionCount() - 1 == partitionCount;

                        txWriter.setMaxTimestamp(partitionCount * Micros.HOUR_MICROS);
                        txWriter.commit(symbolCounts);
                        partitionCountCheck.set(partitionCount);
                    }

                    if (rnd.nextBoolean()) {
                        // Reopen txn file for writing
                        txWriter.ofRW(path.$(), timestampType, PartitionBy.HOUR);
                    }

                    if (!exceptions.isEmpty()) {
                        break;
                    }
                }
            } catch (Throwable e) {
                exceptions.add(e);
                LOG.error().$(e).$();
            } finally {
                done.incrementAndGet();
            }
        });
    }

    // Exposes the raw offset-3 word (before isPartitionOffset3Cleared folds -1/0), so a test can tell
    // apart a clean 0 cleared write from the legacy all-bits-set -1. The poke plants a raw word verbatim
    // (no value-mask, no fold) into a loaded snapshot -- the only way to synthesize the legacy -1
    // sentinel, which the writer (final, value-masking) cannot produce on disk.
    private static final class RawOffset3Reader extends TxReader {
        RawOffset3Reader(FilesFacade ff) {
            super(ff);
        }

        // Raw offset-3 read on any reader/writer instance via the public partition-info dump;
        // TxWriter is final, so the in-memory poke route only exists on this reader subclass.
        static long rawOffset3Of(TxReader tx, int partitionIndex) {
            final LongList raw = new LongList();
            tx.dumpRawTxPartitionInfo(raw);
            return raw.getQuick(partitionIndex * TableUtils.LONGS_PER_TX_ATTACHED_PARTITION + PARTITION_VERSION_OFFSET);
        }

        void pokeRawOffset3(int partitionIndex, long word) {
            attachedPartitions.setQuick(partitionIndex * TableUtils.LONGS_PER_TX_ATTACHED_PARTITION + PARTITION_VERSION_OFFSET, word);
        }

        long rawOffset3(int partitionIndex) {
            return attachedPartitions.getQuick(partitionIndex * TableUtils.LONGS_PER_TX_ATTACHED_PARTITION + PARTITION_VERSION_OFFSET);
        }
    }


    static class SymbolCountProviderImpl implements SymbolCountProvider {
        private final int count;

        SymbolCountProviderImpl(int count) {
            this.count = count;
        }

        @Override
        public int getSymbolCount() {
            return count;
        }
    }

}
