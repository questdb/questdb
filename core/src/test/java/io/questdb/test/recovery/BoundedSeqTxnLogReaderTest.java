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

package io.questdb.test.recovery;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.cairo.wal.seq.TableTransactionLogV2;
import io.questdb.griffin.SqlException;
import io.questdb.recovery.BoundedSeqTxnLogReader;
import io.questdb.recovery.DiscoveredTable;
import io.questdb.recovery.RecoveryIssueCode;
import io.questdb.recovery.SeqTxnLogState;
import io.questdb.recovery.SeqTxnRecord;
import io.questdb.recovery.TableDiscoveryState;
import io.questdb.recovery.TxnState;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

public class BoundedSeqTxnLogReaderTest extends AbstractCairoTest {
    private static final FilesFacade FF = TestFilesFacadeImpl.INSTANCE;
    private static final long V1_RECORD_SIZE = TableTransactionLogV1.RECORD_SIZE;
    private static final long V2_RECORD_SIZE = TableTransactionLogV2.RECORD_SIZE;


    @Test
    public void testReadV1TxnLog() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_v1";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putLong(0, 100L);
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows(tableName, 1);

            SeqTxnLogState state = readSeqTxnLogState(tableName);
            Assert.assertEquals(WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1, state.getVersion());
            Assert.assertTrue(state.getMaxTxn() >= 1);
            Assert.assertTrue(state.getRecords().size() >= 1);

            // find a non-DDL record
            SeqTxnRecord dataRec = findFirstDataRecord(state);
            Assert.assertNotNull("expected at least one data record", dataRec);
            Assert.assertTrue(dataRec.getWalId() >= 1);
            // V1 records have no min/max timestamps or rowCount
            Assert.assertEquals(TxnState.UNSET_LONG, dataRec.getMinTimestamp());
            Assert.assertEquals(TxnState.UNSET_LONG, dataRec.getMaxTimestamp());
            Assert.assertEquals(TxnState.UNSET_LONG, dataRec.getRowCount());
            Assert.assertEquals(0, state.getIssues().size());
        });
    }

    @Test
    public void testReadV1WithMultipleCommits() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_v1_multi";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                for (int i = 0; i < 5; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                    row.putLong(0, i);
                    row.append();
                    walWriter.commit();
                }
            }
            waitForAppliedRows(tableName, 5);

            SeqTxnLogState state = readSeqTxnLogState(tableName);
            Assert.assertTrue(state.getMaxTxn() >= 5);
            Assert.assertTrue(state.getRecords().size() >= 5);
            Assert.assertEquals(0, state.getIssues().size());

            // verify txns are sequential and 1-based
            for (int i = 0; i < state.getRecords().size(); i++) {
                Assert.assertEquals(i + 1, state.getRecords().getQuick(i).getTxn());
            }
        });
    }

    @Test
    public void testReadV1WithMultipleWalWriters() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_v1_multi_wal";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            // open both writers concurrently so they get distinct walIds
            try (WalWriter walWriter1 = getWalWriter(tableName);
                 WalWriter walWriter2 = getWalWriter(tableName)) {
                TableWriter.Row row1 = walWriter1.newRow(0);
                row1.putLong(0, 1L);
                row1.append();
                walWriter1.commit();

                TableWriter.Row row2 = walWriter2.newRow(Micros.DAY_MICROS);
                row2.putLong(0, 2L);
                row2.append();
                walWriter2.commit();
            }
            waitForAppliedRows(tableName, 2);

            SeqTxnLogState state = readSeqTxnLogState(tableName);
            Assert.assertTrue(state.getRecords().size() >= 2);

            boolean foundDifferent = false;
            int firstWalId = -999;
            for (int i = 0, n = state.getRecords().size(); i < n; i++) {
                SeqTxnRecord rec = state.getRecords().getQuick(i);
                if (!rec.isDdlChange()) {
                    if (firstWalId == -999) {
                        firstWalId = rec.getWalId();
                    } else if (rec.getWalId() != firstWalId) {
                        foundDifferent = true;
                        break;
                    }
                }
            }
            Assert.assertTrue("expected records from different WAL writers", foundDifferent);
            Assert.assertEquals(0, state.getIssues().size());
        });
    }


    @Test
    public void testReadV2TxnLog() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            final String tableName = "seq_v2";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putLong(0, 100L);
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows(tableName, 1);

            SeqTxnLogState state = readSeqTxnLogState(tableName);
            Assert.assertEquals(WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2, state.getVersion());
            Assert.assertTrue(state.getMaxTxn() >= 1);
            Assert.assertTrue(state.getRecords().size() >= 1);
            Assert.assertTrue(state.getPartSize() > 0);

            SeqTxnRecord dataRec = findFirstDataRecord(state);
            Assert.assertNotNull("expected at least one data record", dataRec);
            Assert.assertTrue(dataRec.getWalId() >= 1);
            Assert.assertEquals(0, state.getIssues().size());
        });
    }

    @Test
    public void testReadV2RecordTimestampsPresent() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            final String tableName = "seq_v2_ts";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                TableWriter.Row row = walWriter.newRow(1000L);
                row.putLong(0, 42L);
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows(tableName, 1);

            SeqTxnLogState state = readSeqTxnLogState(tableName);
            SeqTxnRecord dataRec = findFirstDataRecord(state);
            Assert.assertNotNull("expected at least one data record", dataRec);
            Assert.assertNotEquals(TxnState.UNSET_LONG, dataRec.getMinTimestamp());
            Assert.assertNotEquals(TxnState.UNSET_LONG, dataRec.getMaxTimestamp());
            Assert.assertTrue(dataRec.getRowCount() > 0);
            Assert.assertNotEquals(TxnState.UNSET_LONG, dataRec.getCommitTimestamp());
        });
    }

    @Test
    public void testReadV2WithMultipleCommits() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            final String tableName = "seq_v2_multi";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                for (int i = 0; i < 5; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                    row.putLong(0, i);
                    row.append();
                    walWriter.commit();
                }
            }
            waitForAppliedRows(tableName, 5);

            SeqTxnLogState state = readSeqTxnLogState(tableName);
            Assert.assertEquals(WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2, state.getVersion());
            Assert.assertTrue(state.getMaxTxn() >= 5);
            Assert.assertTrue(state.getRecords().size() >= 5);
            Assert.assertEquals(0, state.getIssues().size());

            for (int i = 0; i < state.getRecords().size(); i++) {
                Assert.assertEquals(i + 1, state.getRecords().getQuick(i).getTxn());
            }
        });
    }

    @Test
    public void testV2RecordsSpanMultiplePartFiles() throws Exception {
        assertMemoryLeak(() -> {
            // Manually construct a V2 txnlog with partSize=2, 5 records spanning 3 part files.
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                String dir = temp.getRoot().getAbsolutePath() + "/v2_multi_part";
                path.of(dir).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));

                // Create seq directory structure
                path.of(dir).concat(WalUtils.TXNLOG_PARTS_DIR).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));

                // Write _txnlog header
                path.of(dir).concat("_txnlog").$();
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);
                mem.putInt(WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2); // version
                mem.putLong(5L);    // maxTxn = 5
                mem.putLong(1000L); // createTimestamp
                mem.putInt(2);      // partSize = 2
                for (long i = 24; i < TableTransactionLogFile.HEADER_SIZE; i += 4) {
                    mem.putInt(0);
                }
                mem.close();

                // Write 3 part files: part 0 (records 0,1), part 1 (records 2,3), part 2 (record 4)
                for (int partId = 0; partId < 3; partId++) {
                    int recordsInPart = (partId < 2) ? 2 : 1;
                    long partFileSize = recordsInPart * V2_RECORD_SIZE;
                    long buf = Unsafe.malloc(partFileSize, MemoryTag.NATIVE_DEFAULT);
                    try {
                        Unsafe.getUnsafe().setMemory(buf, partFileSize, (byte) 0);
                        for (int r = 0; r < recordsInPart; r++) {
                            int globalIdx = partId * 2 + r;
                            long recOff = buf + r * V2_RECORD_SIZE;
                            Unsafe.getUnsafe().putLong(recOff, 0L);          // structureVersion
                            Unsafe.getUnsafe().putInt(recOff + 8, 1);        // walId
                            Unsafe.getUnsafe().putInt(recOff + 12, 0);       // segmentId
                            Unsafe.getUnsafe().putInt(recOff + 16, globalIdx); // segmentTxn
                            Unsafe.getUnsafe().putLong(recOff + 20, (globalIdx + 1) * 1000L); // commitTimestamp
                            Unsafe.getUnsafe().putLong(recOff + 28, globalIdx * 100L); // minTimestamp
                            Unsafe.getUnsafe().putLong(recOff + 36, (globalIdx + 1) * 100L); // maxTimestamp
                            Unsafe.getUnsafe().putLong(recOff + 44, 10L + globalIdx); // rowCount
                        }

                        path.of(dir).concat(WalUtils.TXNLOG_PARTS_DIR).slash().put(partId).$();
                        long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                        Assert.assertTrue(fd > -1);
                        try {
                            Assert.assertEquals(partFileSize, FF.write(fd, buf, partFileSize, 0));
                            Assert.assertTrue(FF.truncate(fd, partFileSize));
                        } finally {
                            FF.close(fd);
                        }
                    } finally {
                        Unsafe.free(buf, partFileSize, MemoryTag.NATIVE_DEFAULT);
                    }
                }

                // Read the txnlog
                path.of(dir).concat("_txnlog").$();
                SeqTxnLogState state = new BoundedSeqTxnLogReader(FF).read(path.$());

                Assert.assertEquals(WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2, state.getVersion());
                Assert.assertEquals(5, state.getMaxTxn());
                Assert.assertEquals(2, state.getPartSize());
                Assert.assertEquals(5, state.getRecords().size());
                Assert.assertEquals(0, state.getIssues().size());

                // Verify all records were read correctly across 3 part files
                for (int i = 0; i < 5; i++) {
                    SeqTxnRecord rec = state.getRecords().getQuick(i);
                    Assert.assertEquals(i + 1, rec.getTxn());
                    Assert.assertEquals(1, rec.getWalId());
                    Assert.assertEquals(i, rec.getSegmentTxn());
                    Assert.assertEquals((i + 1) * 1000L, rec.getCommitTimestamp());
                    Assert.assertEquals(i * 100L, rec.getMinTimestamp());
                    Assert.assertEquals((i + 1) * 100L, rec.getMaxTimestamp());
                    Assert.assertEquals(10L + i, rec.getRowCount());
                }
            }
        });
    }

    @Test
    public void testV2TruncatedPartFile() throws Exception {
        assertMemoryLeak(() -> {
            // V2 txnlog with partSize=10, maxTxn=1, but part file shorter than V2_RECORD_SIZE.
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                String dir = temp.getRoot().getAbsolutePath() + "/v2_trunc_part";
                path.of(dir).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));

                path.of(dir).concat(WalUtils.TXNLOG_PARTS_DIR).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));

                // Write _txnlog header with maxTxn=1, partSize=10
                path.of(dir).concat("_txnlog").$();
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);
                mem.putInt(WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2);
                mem.putLong(1L);    // maxTxn = 1
                mem.putLong(1000L); // createTimestamp
                mem.putInt(10);     // partSize = 10
                for (long i = 24; i < TableTransactionLogFile.HEADER_SIZE; i += 4) {
                    mem.putInt(0);
                }
                mem.close();

                // Write a truncated part file (only 10 bytes, less than V2_RECORD_SIZE=60)
                path.of(dir).concat(WalUtils.TXNLOG_PARTS_DIR).slash().put(0).$();
                long truncatedSize = 10;
                long buf = Unsafe.malloc(truncatedSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().setMemory(buf, truncatedSize, (byte) 0);
                    long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                    Assert.assertTrue(fd > -1);
                    try {
                        Assert.assertEquals(truncatedSize, FF.write(fd, buf, truncatedSize, 0));
                        Assert.assertTrue(FF.truncate(fd, truncatedSize));
                    } finally {
                        FF.close(fd);
                    }
                } finally {
                    Unsafe.free(buf, truncatedSize, MemoryTag.NATIVE_DEFAULT);
                }

                // Read the txnlog — should detect the record can't be read
                path.of(dir).concat("_txnlog").$();
                SeqTxnLogState state = new BoundedSeqTxnLogReader(FF).read(path.$());

                Assert.assertEquals(WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2, state.getVersion());
                Assert.assertEquals(1, state.getMaxTxn());
                // Record read should fail because part file is too short
                Assert.assertEquals(0, state.getRecords().size());
                Assert.assertTrue(
                        "expected OUT_OF_RANGE issue, got: " + dumpIssues(state),
                        hasIssue(state, RecoveryIssueCode.OUT_OF_RANGE)
                );
            }
        });
    }

    @Test
    public void testReadV2MissingPartFile() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            final String tableName = "seq_v2_miss_part";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putLong(0, 1L);
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows(tableName, 1);

            // delete the _txn_parts/0 file
            TableToken tableToken = engine.verifyTableName(tableName);
            try (Path partPath = new Path()) {
                partPath.of(engine.getConfiguration().getDbRoot())
                        .concat(tableToken)
                        .concat(WalUtils.SEQ_DIR)
                        .concat(WalUtils.TXNLOG_PARTS_DIR)
                        .slash()
                        .put(0)
                        .$();
                Assert.assertTrue(FF.removeQuiet(partPath.$()));
            }

            SeqTxnLogState state = readSeqTxnLogState(tableName);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.MISSING_FILE) || hasIssue(state, RecoveryIssueCode.IO_ERROR));
        });
    }

    @Test
    public void testIoFailureOnV2PartFileOpen() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            final String tableName = "seq_v2_part_open";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putLong(0, 1L);
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows(tableName, 1);

            FilesFacade failPartOpenFf = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Block opening any file containing "_txn_parts" in its path
                    CharSequence seq = name.asAsciiCharSequence();
                    for (int i = 0, n = seq.length() - WalUtils.TXNLOG_PARTS_DIR.length(); i <= n; i++) {
                        boolean match = true;
                        for (int j = 0; j < WalUtils.TXNLOG_PARTS_DIR.length() && match; j++) {
                            match = seq.charAt(i + j) == WalUtils.TXNLOG_PARTS_DIR.charAt(j);
                        }
                        if (match) {
                            return -1;
                        }
                    }
                    return super.openRO(name);
                }
            };

            try (Path txnlogPath = new Path()) {
                txnlogPathOf(tableName, txnlogPath);
                SeqTxnLogState state = new BoundedSeqTxnLogReader(failPartOpenFf).read(txnlogPath.$());
                Assert.assertTrue(
                        "issues=" + dumpIssues(state),
                        hasIssue(state, RecoveryIssueCode.IO_ERROR) || hasIssue(state, RecoveryIssueCode.MISSING_FILE)
                );
                Assert.assertEquals(0, state.getRecords().size());
            }
        });
    }

    @Test
    public void testIoFailureOnV2RecordField() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            final String tableName = "seq_v2_rec_fail";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putLong(0, 1L);
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows(tableName, 1);

            // fail on the V2 record's minTimestamp read inside the part file
            FilesFacade failV2Ff = new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == TableTransactionLogV2.MIN_TIMESTAMP_OFFSET && len == Long.BYTES) {
                        return -1;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            try (Path txnlogPath = new Path()) {
                txnlogPathOf(tableName, txnlogPath);
                SeqTxnLogState state = new BoundedSeqTxnLogReader(failV2Ff).read(txnlogPath.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
                Assert.assertTrue(hasIssueContaining(state, "record.minTimestamp"));
            }
        });
    }


    @Test
    public void testReadForTableFallsBackToDeprecatedSeqDir() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_deprecated_dir";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putLong(0, 42L);
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows(tableName, 1);

            TableToken tableToken = engine.verifyTableName(tableName);
            CharSequence dbRoot = engine.getConfiguration().getDbRoot();

            // rename txn_seq -> seq to simulate an older database layout
            try (Path src = new Path(); Path dst = new Path()) {
                LPSZ srcLpsz = src.of(dbRoot).concat(tableToken).concat(WalUtils.SEQ_DIR).$();
                LPSZ dstLpsz = dst.of(dbRoot).concat(tableToken).concat(WalUtils.SEQ_DIR_DEPRECATED).$();
                Assert.assertEquals(0, FF.rename(srcLpsz, dstLpsz));
            }

            DiscoveredTable table = new DiscoveredTable(
                    tableName,
                    tableToken.getDirName(),
                    TableDiscoveryState.HAS_TXN,
                    true,
                    true,
                    TableUtils.TABLE_TYPE_WAL
            );
            SeqTxnLogState state = new BoundedSeqTxnLogReader(FF).readForTable(dbRoot, table);

            Assert.assertEquals(WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1, state.getVersion());
            Assert.assertTrue("expected at least 1 txn, got " + state.getMaxTxn(), state.getMaxTxn() >= 1);
            Assert.assertEquals(0, state.getIssues().size());
        });
    }

    @Test
    public void testReadForTable() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_read_for_table";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putLong(0, 42L);
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows(tableName, 1);

            TableToken tableToken = engine.verifyTableName(tableName);
            DiscoveredTable table = new DiscoveredTable(
                    tableName,
                    tableToken.getDirName(),
                    TableDiscoveryState.HAS_TXN,
                    true,
                    true,
                    TableUtils.TABLE_TYPE_WAL
            );
            CharSequence dbRoot = engine.getConfiguration().getDbRoot();
            SeqTxnLogState state = new BoundedSeqTxnLogReader(FF).readForTable(dbRoot, table);

            Assert.assertEquals(WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1, state.getVersion());
            Assert.assertTrue(state.getMaxTxn() >= 1);
            Assert.assertEquals(0, state.getIssues().size());
        });
    }


    @Test
    public void testDdlEntriesHaveWalIdMinusOne() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_ddl";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putLong(0, 1L);
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows(tableName, 1);

            execute("alter table " + tableName + " add column extra int");
            drainWalQueue(engine);

            SeqTxnLogState state = readSeqTxnLogState(tableName);
            Assert.assertTrue(state.getRecords().size() >= 2);

            boolean foundDdl = false;
            for (int i = 0, n = state.getRecords().size(); i < n; i++) {
                SeqTxnRecord rec = state.getRecords().getQuick(i);
                if (rec.isDdlChange()) {
                    Assert.assertEquals(WalUtils.METADATA_WALID, rec.getWalId());
                    foundDdl = true;
                }
            }
            Assert.assertTrue("expected at least one DDL record", foundDdl);
            Assert.assertEquals(0, state.getIssues().size());
        });
    }

    @Test
    public void testStructureVersionIncrements() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_struct_ver";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putLong(0, 1L);
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows(tableName, 1);

            long structVerBefore = -1;
            SeqTxnLogState stateBefore = readSeqTxnLogState(tableName);
            for (int i = 0, n = stateBefore.getRecords().size(); i < n; i++) {
                structVerBefore = Math.max(structVerBefore, stateBefore.getRecords().getQuick(i).getStructureVersion());
            }

            execute("alter table " + tableName + " add column extra int");
            drainWalQueue(engine);

            SeqTxnLogState stateAfter = readSeqTxnLogState(tableName);
            long structVerAfter = -1;
            for (int i = 0, n = stateAfter.getRecords().size(); i < n; i++) {
                structVerAfter = Math.max(structVerAfter, stateAfter.getRecords().getQuick(i).getStructureVersion());
            }
            Assert.assertTrue("structure version should increase after DDL", structVerAfter > structVerBefore);
        });
    }


    @Test
    public void testRecordFieldsPresentV1() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_fields_v1";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putLong(0, 42L);
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows(tableName, 1);

            SeqTxnLogState state = readSeqTxnLogState(tableName);
            SeqTxnRecord dataRec = findFirstDataRecord(state);
            Assert.assertNotNull("expected at least one data record", dataRec);
            Assert.assertTrue(dataRec.getWalId() >= 1);
            Assert.assertTrue(dataRec.getSegmentId() >= 0);
            Assert.assertTrue(dataRec.getSegmentTxn() >= 0);
            Assert.assertNotEquals(TxnState.UNSET_LONG, dataRec.getCommitTimestamp());
            // V1 does not carry min/max timestamps or rowCount
            Assert.assertEquals(TxnState.UNSET_LONG, dataRec.getMinTimestamp());
            Assert.assertEquals(TxnState.UNSET_LONG, dataRec.getMaxTimestamp());
            Assert.assertEquals(TxnState.UNSET_LONG, dataRec.getRowCount());
        });
    }

    @Test
    public void testRecordFieldsPresentV2() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            final String tableName = "seq_fields_v2";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putLong(0, 42L);
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows(tableName, 1);

            SeqTxnLogState state = readSeqTxnLogState(tableName);
            SeqTxnRecord dataRec = findFirstDataRecord(state);
            Assert.assertNotNull("expected at least one data record", dataRec);
            Assert.assertTrue(dataRec.getWalId() >= 1);
            Assert.assertTrue(dataRec.getSegmentId() >= 0);
            Assert.assertTrue(dataRec.getSegmentTxn() >= 0);
            Assert.assertNotEquals(TxnState.UNSET_LONG, dataRec.getCommitTimestamp());
            Assert.assertNotEquals(TxnState.UNSET_LONG, dataRec.getMinTimestamp());
            Assert.assertNotEquals(TxnState.UNSET_LONG, dataRec.getMaxTimestamp());
            Assert.assertNotEquals(TxnState.UNSET_LONG, dataRec.getRowCount());
            Assert.assertTrue(dataRec.getRowCount() > 0);
        });
    }

    @Test
    public void testTxnIsOneBased() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_txn_one";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putLong(0, 1L);
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows(tableName, 1);

            SeqTxnLogState state = readSeqTxnLogState(tableName);
            Assert.assertTrue(state.getRecords().size() >= 1);
            Assert.assertEquals(1, state.getRecords().getQuick(0).getTxn());
        });
    }


    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_empty";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            SeqTxnLogState state = readSeqTxnLogState(tableName);
            Assert.assertEquals(0, state.getMaxTxn());
            Assert.assertEquals(0, state.getRecords().size());
            Assert.assertEquals(0, state.getIssues().size());
        });
    }

    @Test
    public void testMaxRecordsCap() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_cap";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                    row.putLong(0, i);
                    row.append();
                    walWriter.commit();
                }
            }
            waitForAppliedRows(tableName, 10);

            SeqTxnLogState state = readSeqTxnLogStateWithReader(tableName, new BoundedSeqTxnLogReader(FF, 2));
            Assert.assertEquals(2, state.getRecords().size());
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.TRUNCATED_OUTPUT));
        });
    }

    @Test
    public void testMaxRecordsClampedToOne() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_clamp";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                for (int i = 0; i < 5; i++) {
                    TableWriter.Row row = walWriter.newRow(i * Micros.DAY_MICROS);
                    row.putLong(0, i);
                    row.append();
                    walWriter.commit();
                }
            }
            waitForAppliedRows(tableName, 5);

            // maxRecords=0 should be clamped to 1
            SeqTxnLogState state = readSeqTxnLogStateWithReader(tableName, new BoundedSeqTxnLogReader(FF, 0));
            Assert.assertEquals(1, state.getRecords().size());
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.TRUNCATED_OUTPUT));
        });
    }

    @Test
    public void testDefaultConstructor() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_default_ctor";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putLong(0, 1L);
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows(tableName, 1);

            SeqTxnLogState state = readSeqTxnLogStateWithReader(tableName, new BoundedSeqTxnLogReader(FF));
            Assert.assertTrue(state.getMaxTxn() >= 1);
            Assert.assertEquals(0, state.getIssues().size());
        });
    }


    @Test
    public void testCreateTimestampPopulated() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_create_ts";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            SeqTxnLogState state = readSeqTxnLogState(tableName);
            Assert.assertNotEquals(TxnState.UNSET_LONG, state.getCreateTimestamp());
            Assert.assertEquals(0, state.getIssues().size());
        });
    }

    @Test
    public void testPartSizeForV1() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_partsize_v1";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            SeqTxnLogState state = readSeqTxnLogState(tableName);
            // V1 format has partSize = 0
            Assert.assertEquals(0, state.getPartSize());
            Assert.assertEquals(0, state.getIssues().size());
        });
    }

    @Test
    public void testPartSizeForV2() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            final String tableName = "seq_partsize_v2";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            SeqTxnLogState state = readSeqTxnLogState(tableName);
            Assert.assertTrue(state.getPartSize() > 0);
            Assert.assertEquals(0, state.getIssues().size());
        });
    }

    @Test
    public void testTxnlogPathSet() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_path";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            SeqTxnLogState state = readSeqTxnLogState(tableName);
            Assert.assertNotNull(state.getTxnlogPath());
            Assert.assertTrue(state.getTxnlogPath().contains(WalUtils.TXNLOG_FILE_NAME));
        });
    }


    @Test
    public void testMissingTxnlogFile() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                path.of(temp.getRoot().getAbsolutePath()).concat("nonexistent").concat("_txnlog").$();
                SeqTxnLogState state = new BoundedSeqTxnLogReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.MISSING_FILE));
            }
        });
    }

    @Test
    public void testTruncatedHeader() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                String dir = temp.getRoot().getAbsolutePath() + "/truncated_header";
                path.of(dir).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));

                path.of(dir).concat("_txnlog").$();
                // write a small file (20 bytes), less than HEADER_SIZE (76)
                writeRawBytes(path.$(), 20);

                SeqTxnLogState state = new BoundedSeqTxnLogReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.SHORT_FILE));
            }
        });
    }

    @Test
    public void testCorruptVersion() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                String dir = temp.getRoot().getAbsolutePath() + "/corrupt_ver";
                path.of(dir).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));
                path.of(dir).concat("_txnlog").$();

                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);
                mem.putInt(99);     // invalid version
                mem.putLong(0L);    // maxTxn
                mem.putLong(0L);    // createTimestamp
                mem.putInt(0);      // partSize
                for (long i = 24; i < TableTransactionLogFile.HEADER_SIZE; i += 4) {
                    mem.putInt(0);
                }
                mem.close();

                SeqTxnLogState state = new BoundedSeqTxnLogReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.CORRUPT_SEQ_TXNLOG));
                Assert.assertTrue(hasIssueContaining(state, "unsupported _txnlog version"));
            }
        });
    }

    @Test
    public void testNegativeMaxTxn() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                String dir = temp.getRoot().getAbsolutePath() + "/neg_maxtxn";
                path.of(dir).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));
                path.of(dir).concat("_txnlog").$();

                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);
                mem.putInt(WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2);
                mem.putLong(-5L);
                mem.putLong(0L);
                mem.putInt(1000);
                for (long i = 24; i < TableTransactionLogFile.HEADER_SIZE; i += 4) {
                    mem.putInt(0);
                }
                mem.close();

                SeqTxnLogState state = new BoundedSeqTxnLogReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.CORRUPT_SEQ_TXNLOG));
                Assert.assertTrue(hasIssueContaining(state, "negative maxTxn"));
            }
        });
    }

    @Test
    public void testV2NonPositivePartSize() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                String dir = temp.getRoot().getAbsolutePath() + "/zero_partsize";
                path.of(dir).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));
                path.of(dir).concat("_txnlog").$();

                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);
                mem.putInt(WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2);
                mem.putLong(5L);    // positive maxTxn
                mem.putLong(0L);
                mem.putInt(0);      // partSize = 0
                for (long i = 24; i < TableTransactionLogFile.HEADER_SIZE; i += 4) {
                    mem.putInt(0);
                }
                mem.close();

                SeqTxnLogState state = new BoundedSeqTxnLogReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.CORRUPT_SEQ_TXNLOG));
                Assert.assertTrue(hasIssueContaining(state, "non-positive partSize"));
            }
        });
    }


    @Test
    public void testTruncatedV1Records() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                String dir = temp.getRoot().getAbsolutePath() + "/truncated_v1";
                path.of(dir).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));
                path.of(dir).concat("_txnlog").$();

                // Write header + 2 V1 records, but header claims 10 records
                long headerSize = TableTransactionLogFile.HEADER_SIZE;
                long fileSize = headerSize + 2 * V1_RECORD_SIZE;
                long buf = Unsafe.malloc(fileSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().setMemory(buf, fileSize, (byte) 0);
                    // version = V1
                    Unsafe.getUnsafe().putInt(buf, WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1);
                    // maxTxn = 10
                    Unsafe.getUnsafe().putLong(buf + 4, 10L);
                    // write 2 V1 records at HEADER_SIZE offset
                    for (int r = 0; r < 2; r++) {
                        long recOff = buf + headerSize + r * V1_RECORD_SIZE;
                        Unsafe.getUnsafe().putLong(recOff, 0L);  // structureVersion
                        Unsafe.getUnsafe().putInt(recOff + 8, 1); // walId
                    }

                    long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                    Assert.assertTrue(fd > -1);
                    try {
                        Assert.assertEquals(fileSize, FF.write(fd, buf, fileSize, 0));
                        Assert.assertTrue(FF.truncate(fd, fileSize));
                    } finally {
                        FF.close(fd);
                    }
                } finally {
                    Unsafe.free(buf, fileSize, MemoryTag.NATIVE_DEFAULT);
                }

                SeqTxnLogState state = new BoundedSeqTxnLogReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.SHORT_FILE));
                Assert.assertEquals(2, state.getRecords().size());
            }
        });
    }


    @Test
    public void testV1EmptyRecords() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                String dir = temp.getRoot().getAbsolutePath() + "/v1_empty";
                path.of(dir).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));
                path.of(dir).concat("_txnlog").$();

                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);
                mem.putInt(WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1);
                mem.putLong(0L);   // maxTxn = 0
                mem.putLong(0L);   // createTimestamp
                mem.putInt(0);     // partSize
                for (long i = 24; i < TableTransactionLogFile.HEADER_SIZE; i += 4) {
                    mem.putInt(0);
                }
                mem.close();

                SeqTxnLogState state = new BoundedSeqTxnLogReader(FF).read(path.$());
                Assert.assertEquals(WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1, state.getVersion());
                Assert.assertEquals(0, state.getMaxTxn());
                Assert.assertEquals(0, state.getRecords().size());
                Assert.assertEquals(0, state.getIssues().size());
            }
        });
    }

    @Test
    public void testV1RecordsParsed() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                String dir = temp.getRoot().getAbsolutePath() + "/v1_parsed";
                path.of(dir).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));
                path.of(dir).concat("_txnlog").$();

                long headerSize = TableTransactionLogFile.HEADER_SIZE;
                long fileSize = headerSize + 2 * V1_RECORD_SIZE;
                long buf = Unsafe.malloc(fileSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().setMemory(buf, fileSize, (byte) 0);
                    Unsafe.getUnsafe().putInt(buf, WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1);
                    Unsafe.getUnsafe().putLong(buf + 4, 2L);     // maxTxn
                    Unsafe.getUnsafe().putLong(buf + 12, 1000L); // createTimestamp

                    // record 1: data commit
                    long rec1 = buf + headerSize;
                    Unsafe.getUnsafe().putLong(rec1, 0L);        // structureVersion
                    Unsafe.getUnsafe().putInt(rec1 + 8, 1);      // walId
                    Unsafe.getUnsafe().putInt(rec1 + 12, 0);     // segmentId
                    Unsafe.getUnsafe().putInt(rec1 + 16, 0);     // segmentTxn
                    Unsafe.getUnsafe().putLong(rec1 + 20, 2000L); // commitTimestamp

                    // record 2: DDL
                    long rec2 = buf + headerSize + V1_RECORD_SIZE;
                    Unsafe.getUnsafe().putLong(rec2, 1L);                     // structureVersion
                    Unsafe.getUnsafe().putInt(rec2 + 8, WalUtils.METADATA_WALID); // walId = -1
                    Unsafe.getUnsafe().putInt(rec2 + 12, 0);
                    Unsafe.getUnsafe().putInt(rec2 + 16, 0);
                    Unsafe.getUnsafe().putLong(rec2 + 20, 3000L);

                    long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                    Assert.assertTrue(fd > -1);
                    try {
                        Assert.assertEquals(fileSize, FF.write(fd, buf, fileSize, 0));
                        Assert.assertTrue(FF.truncate(fd, fileSize));
                    } finally {
                        FF.close(fd);
                    }
                } finally {
                    Unsafe.free(buf, fileSize, MemoryTag.NATIVE_DEFAULT);
                }

                SeqTxnLogState state = new BoundedSeqTxnLogReader(FF).read(path.$());
                Assert.assertEquals(WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1, state.getVersion());
                Assert.assertEquals(2, state.getMaxTxn());
                Assert.assertEquals(1000L, state.getCreateTimestamp());
                Assert.assertEquals(2, state.getRecords().size());

                SeqTxnRecord rec1 = state.getRecords().getQuick(0);
                Assert.assertEquals(1, rec1.getTxn());
                Assert.assertEquals(0, rec1.getStructureVersion());
                Assert.assertEquals(1, rec1.getWalId());
                Assert.assertFalse(rec1.isDdlChange());
                Assert.assertEquals(2000L, rec1.getCommitTimestamp());
                Assert.assertEquals(TxnState.UNSET_LONG, rec1.getMinTimestamp());
                Assert.assertEquals(TxnState.UNSET_LONG, rec1.getMaxTimestamp());
                Assert.assertEquals(TxnState.UNSET_LONG, rec1.getRowCount());

                SeqTxnRecord rec2 = state.getRecords().getQuick(1);
                Assert.assertEquals(2, rec2.getTxn());
                Assert.assertTrue(rec2.isDdlChange());
                Assert.assertEquals(1, rec2.getStructureVersion());
                Assert.assertEquals(0, state.getIssues().size());
            }
        });
    }

    @Test
    public void testV1RecordsCapApplied() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                String dir = temp.getRoot().getAbsolutePath() + "/v1_cap";
                path.of(dir).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));
                path.of(dir).concat("_txnlog").$();

                long headerSize = TableTransactionLogFile.HEADER_SIZE;
                long fileSize = headerSize + 10 * V1_RECORD_SIZE;
                long buf = Unsafe.malloc(fileSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().setMemory(buf, fileSize, (byte) 0);
                    Unsafe.getUnsafe().putInt(buf, WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1);
                    Unsafe.getUnsafe().putLong(buf + 4, 10L);

                    for (int r = 0; r < 10; r++) {
                        long recOff = buf + headerSize + r * V1_RECORD_SIZE;
                        Unsafe.getUnsafe().putLong(recOff, 0L);
                        Unsafe.getUnsafe().putInt(recOff + 8, 1);
                    }

                    long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                    Assert.assertTrue(fd > -1);
                    try {
                        Assert.assertEquals(fileSize, FF.write(fd, buf, fileSize, 0));
                        Assert.assertTrue(FF.truncate(fd, fileSize));
                    } finally {
                        FF.close(fd);
                    }
                } finally {
                    Unsafe.free(buf, fileSize, MemoryTag.NATIVE_DEFAULT);
                }

                SeqTxnLogState state = new BoundedSeqTxnLogReader(FF, 3).read(path.$());
                Assert.assertEquals(3, state.getRecords().size());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.TRUNCATED_OUTPUT));
            }
        });
    }

    @Test
    public void testV1RecordsCapReadsNewestNotOldest() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                String dir = temp.getRoot().getAbsolutePath() + "/v1_cap_tail";
                path.of(dir).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));
                path.of(dir).concat("_txnlog").$();

                int totalRecords = 10;
                int maxRecords = 3;
                long headerSize = TableTransactionLogFile.HEADER_SIZE;
                long fileSize = headerSize + totalRecords * V1_RECORD_SIZE;
                long buf = Unsafe.malloc(fileSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().setMemory(buf, fileSize, (byte) 0);
                    Unsafe.getUnsafe().putInt(buf, WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1);
                    Unsafe.getUnsafe().putLong(buf + 4, totalRecords);

                    // write records with distinct walIds (r+1) so we can identify which are loaded
                    for (int r = 0; r < totalRecords; r++) {
                        long recOff = buf + headerSize + r * V1_RECORD_SIZE;
                        Unsafe.getUnsafe().putLong(recOff + TableTransactionLogFile.TX_LOG_STRUCTURE_VERSION_OFFSET, 0L);
                        Unsafe.getUnsafe().putInt(recOff + TableTransactionLogFile.TX_LOG_WAL_ID_OFFSET, r + 1);
                        Unsafe.getUnsafe().putInt(recOff + TableTransactionLogFile.TX_LOG_SEGMENT_OFFSET, 0);
                        Unsafe.getUnsafe().putInt(recOff + TableTransactionLogFile.TX_LOG_SEGMENT_TXN_OFFSET, 0);
                        Unsafe.getUnsafe().putLong(recOff + TableTransactionLogFile.TX_LOG_COMMIT_TIMESTAMP_OFFSET, (r + 1) * 1000L);
                    }

                    long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                    Assert.assertTrue(fd > -1);
                    try {
                        Assert.assertEquals(fileSize, FF.write(fd, buf, fileSize, 0));
                        Assert.assertTrue(FF.truncate(fd, fileSize));
                    } finally {
                        FF.close(fd);
                    }
                } finally {
                    Unsafe.free(buf, fileSize, MemoryTag.NATIVE_DEFAULT);
                }

                SeqTxnLogState state = new BoundedSeqTxnLogReader(FF, maxRecords).read(path.$());
                Assert.assertEquals(maxRecords, state.getRecords().size());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.TRUNCATED_OUTPUT));

                // should have the NEWEST 3 records: txns 8, 9, 10 (walIds 8, 9, 10)
                Assert.assertEquals(8, state.getRecords().getQuick(0).getTxn());
                Assert.assertEquals(8, state.getRecords().getQuick(0).getWalId());
                Assert.assertEquals(9, state.getRecords().getQuick(1).getTxn());
                Assert.assertEquals(9, state.getRecords().getQuick(1).getWalId());
                Assert.assertEquals(10, state.getRecords().getQuick(2).getTxn());
                Assert.assertEquals(10, state.getRecords().getQuick(2).getWalId());
            }
        });
    }

    @Test
    public void testV2RecordsCapReadsNewestNotOldest() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                String dir = temp.getRoot().getAbsolutePath() + "/v2_cap_tail";
                path.of(dir).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));

                int totalRecords = 10;
                int maxRecords = 3;
                int partSize = 4; // 4 records per part file

                // write the _txnlog header
                path.of(dir).concat("_txnlog").$();
                long headerSize = TableTransactionLogFile.HEADER_SIZE;
                long headerBuf = Unsafe.malloc(headerSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().setMemory(headerBuf, headerSize, (byte) 0);
                    Unsafe.getUnsafe().putInt(headerBuf, WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2);
                    Unsafe.getUnsafe().putLong(headerBuf + 4, totalRecords);
                    Unsafe.getUnsafe().putInt(headerBuf + TableTransactionLogFile.HEADER_SEQ_PART_SIZE_32, partSize);

                    long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                    Assert.assertTrue(fd > -1);
                    try {
                        Assert.assertEquals(headerSize, FF.write(fd, headerBuf, headerSize, 0));
                        Assert.assertTrue(FF.truncate(fd, headerSize));
                    } finally {
                        FF.close(fd);
                    }
                } finally {
                    Unsafe.free(headerBuf, headerSize, MemoryTag.NATIVE_DEFAULT);
                }

                // create _txn_parts directory
                path.of(dir).concat(WalUtils.TXNLOG_PARTS_DIR).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));

                // write part files: part 0 has records 0-3, part 1 has 4-7, part 2 has 8-9
                int numParts = (totalRecords + partSize - 1) / partSize;
                for (int p = 0; p < numParts; p++) {
                    int startRec = p * partSize;
                    int recsInPart = Math.min(partSize, totalRecords - startRec);
                    long partFileSize = recsInPart * V2_RECORD_SIZE;
                    long partBuf = Unsafe.malloc(partFileSize, MemoryTag.NATIVE_DEFAULT);
                    try {
                        Unsafe.getUnsafe().setMemory(partBuf, partFileSize, (byte) 0);
                        for (int r = 0; r < recsInPart; r++) {
                            int globalIdx = startRec + r;
                            long recOff = partBuf + r * V2_RECORD_SIZE;
                            Unsafe.getUnsafe().putLong(recOff + TableTransactionLogFile.TX_LOG_STRUCTURE_VERSION_OFFSET, 0L);
                            Unsafe.getUnsafe().putInt(recOff + TableTransactionLogFile.TX_LOG_WAL_ID_OFFSET, globalIdx + 1);
                            Unsafe.getUnsafe().putInt(recOff + TableTransactionLogFile.TX_LOG_SEGMENT_OFFSET, 0);
                            Unsafe.getUnsafe().putInt(recOff + TableTransactionLogFile.TX_LOG_SEGMENT_TXN_OFFSET, 0);
                            Unsafe.getUnsafe().putLong(recOff + TableTransactionLogFile.TX_LOG_COMMIT_TIMESTAMP_OFFSET, (globalIdx + 1) * 1000L);
                            Unsafe.getUnsafe().putLong(recOff + TableTransactionLogV2.MIN_TIMESTAMP_OFFSET, globalIdx * 100L);
                            Unsafe.getUnsafe().putLong(recOff + TableTransactionLogV2.MAX_TIMESTAMP_OFFSET, globalIdx * 100L + 50L);
                            Unsafe.getUnsafe().putLong(recOff + TableTransactionLogV2.ROW_COUNT_OFFSET, globalIdx + 10L);
                        }

                        path.of(dir).concat(WalUtils.TXNLOG_PARTS_DIR).slash().put(p).$();
                        long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                        Assert.assertTrue(fd > -1);
                        try {
                            Assert.assertEquals(partFileSize, FF.write(fd, partBuf, partFileSize, 0));
                            Assert.assertTrue(FF.truncate(fd, partFileSize));
                        } finally {
                            FF.close(fd);
                        }
                    } finally {
                        Unsafe.free(partBuf, partFileSize, MemoryTag.NATIVE_DEFAULT);
                    }
                }

                path.of(dir).concat("_txnlog").$();
                SeqTxnLogState state = new BoundedSeqTxnLogReader(FF, maxRecords).read(path.$());
                Assert.assertEquals(maxRecords, state.getRecords().size());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.TRUNCATED_OUTPUT));

                // should have the NEWEST 3 records: txns 8, 9, 10 (walIds 8, 9, 10)
                Assert.assertEquals(8, state.getRecords().getQuick(0).getTxn());
                Assert.assertEquals(8, state.getRecords().getQuick(0).getWalId());
                Assert.assertEquals(700L, state.getRecords().getQuick(0).getMinTimestamp());

                Assert.assertEquals(9, state.getRecords().getQuick(1).getTxn());
                Assert.assertEquals(9, state.getRecords().getQuick(1).getWalId());
                Assert.assertEquals(800L, state.getRecords().getQuick(1).getMinTimestamp());

                Assert.assertEquals(10, state.getRecords().getQuick(2).getTxn());
                Assert.assertEquals(10, state.getRecords().getQuick(2).getWalId());
                Assert.assertEquals(900L, state.getRecords().getQuick(2).getMinTimestamp());
            }
        });
    }

    @Test
    public void testTableDropEntry() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                String dir = temp.getRoot().getAbsolutePath() + "/drop_entry";
                path.of(dir).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));
                path.of(dir).concat("_txnlog").$();

                long headerSize = TableTransactionLogFile.HEADER_SIZE;
                long fileSize = headerSize + V1_RECORD_SIZE;
                long buf = Unsafe.malloc(fileSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().setMemory(buf, fileSize, (byte) 0);
                    Unsafe.getUnsafe().putInt(buf, WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1);
                    Unsafe.getUnsafe().putLong(buf + 4, 1L);

                    long recOff = buf + headerSize;
                    Unsafe.getUnsafe().putLong(recOff, 0L);
                    Unsafe.getUnsafe().putInt(recOff + 8, WalUtils.DROP_TABLE_WAL_ID);

                    long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                    Assert.assertTrue(fd > -1);
                    try {
                        Assert.assertEquals(fileSize, FF.write(fd, buf, fileSize, 0));
                        Assert.assertTrue(FF.truncate(fd, fileSize));
                    } finally {
                        FF.close(fd);
                    }
                } finally {
                    Unsafe.free(buf, fileSize, MemoryTag.NATIVE_DEFAULT);
                }

                SeqTxnLogState state = new BoundedSeqTxnLogReader(FF).read(path.$());
                Assert.assertEquals(1, state.getRecords().size());
                SeqTxnRecord rec = state.getRecords().getQuick(0);
                Assert.assertTrue(rec.isTableDrop());
                Assert.assertFalse(rec.isDdlChange());
                Assert.assertEquals(WalUtils.DROP_TABLE_WAL_ID, rec.getWalId());
            }
        });
    }


    @Test
    public void testIoFailureOnOpen() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_io_open";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            try (Path txnlogPath = new Path()) {
                txnlogPathOf(tableName, txnlogPath);
                final String txnlogPathStr = txnlogPath.$().toString();

                FilesFacade failOpenFf = new TestFilesFacadeImpl() {
                    @Override
                    public long openRO(LPSZ name) {
                        if (name.toString().equals(txnlogPathStr)) {
                            return -1;
                        }
                        return super.openRO(name);
                    }
                };

                SeqTxnLogState state = new BoundedSeqTxnLogReader(failOpenFf).read(txnlogPath.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
                Assert.assertTrue(hasIssueContaining(state, "cannot open"));
            }
        });
    }

    @Test
    public void testIoFailureOnVersionRead() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_io_ver";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            FilesFacade failReadFf = new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == TableTransactionLogFile.TX_LOG_STRUCTURE_VERSION_OFFSET && len == Integer.BYTES) {
                        return -1;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            try (Path txnlogPath = new Path()) {
                txnlogPathOf(tableName, txnlogPath);
                SeqTxnLogState state = new BoundedSeqTxnLogReader(failReadFf).read(txnlogPath.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
            }
        });
    }

    @Test
    public void testIoFailureOnMaxTxnRead() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_io_maxtxn";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            FilesFacade failReadFf = new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == TableTransactionLogFile.MAX_TXN_OFFSET_64 && len == Long.BYTES) {
                        return -1;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            try (Path txnlogPath = new Path()) {
                txnlogPathOf(tableName, txnlogPath);
                SeqTxnLogState state = new BoundedSeqTxnLogReader(failReadFf).read(txnlogPath.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
                Assert.assertEquals(TxnState.UNSET_LONG, state.getMaxTxn());
            }
        });
    }

    @Test
    public void testIoFailureOnCreateTimestampRead() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_io_create_ts";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            FilesFacade failReadFf = new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == TableTransactionLogFile.TABLE_CREATE_TIMESTAMP_OFFSET_64 && len == Long.BYTES) {
                        return -1;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            try (Path txnlogPath = new Path()) {
                txnlogPathOf(tableName, txnlogPath);
                SeqTxnLogState state = new BoundedSeqTxnLogReader(failReadFf).read(txnlogPath.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
                Assert.assertEquals(TxnState.UNSET_LONG, state.getCreateTimestamp());
            }
        });
    }

    @Test
    public void testIoFailureOnPartSizeRead() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_io_partsize";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            FilesFacade failReadFf = new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == TableTransactionLogFile.HEADER_SEQ_PART_SIZE_32 && len == Integer.BYTES) {
                        return -1;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            try (Path txnlogPath = new Path()) {
                txnlogPathOf(tableName, txnlogPath);
                SeqTxnLogState state = new BoundedSeqTxnLogReader(failReadFf).read(txnlogPath.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
                Assert.assertEquals(TxnState.UNSET_INT, state.getPartSize());
            }
        });
    }

    @Test
    public void testIoFailureOnV1RecordField() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                String dir = temp.getRoot().getAbsolutePath() + "/v1_io_fail";
                path.of(dir).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));
                path.of(dir).concat("_txnlog").$();

                long headerSize = TableTransactionLogFile.HEADER_SIZE;
                long fileSize = headerSize + 2 * V1_RECORD_SIZE;
                long buf = Unsafe.malloc(fileSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().setMemory(buf, fileSize, (byte) 0);
                    Unsafe.getUnsafe().putInt(buf, WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1);
                    Unsafe.getUnsafe().putLong(buf + 4, 2L);

                    for (int r = 0; r < 2; r++) {
                        long recOff = buf + headerSize + r * V1_RECORD_SIZE;
                        Unsafe.getUnsafe().putLong(recOff, 0L);
                        Unsafe.getUnsafe().putInt(recOff + 8, 1);
                    }

                    long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                    Assert.assertTrue(fd > -1);
                    try {
                        Assert.assertEquals(fileSize, FF.write(fd, buf, fileSize, 0));
                        Assert.assertTrue(FF.truncate(fd, fileSize));
                    } finally {
                        FF.close(fd);
                    }
                } finally {
                    Unsafe.free(buf, fileSize, MemoryTag.NATIVE_DEFAULT);
                }

                // fail on reading the first record's structureVersion
                final long firstRecordOffset = headerSize + TableTransactionLogFile.TX_LOG_STRUCTURE_VERSION_OFFSET;
                FilesFacade failReadFf = new TestFilesFacadeImpl() {
                    @Override
                    public long read(long fd, long buf, long len, long offset) {
                        if (offset == firstRecordOffset && len == Long.BYTES) {
                            return -1;
                        }
                        return super.read(fd, buf, len, offset);
                    }
                };

                SeqTxnLogState state = new BoundedSeqTxnLogReader(failReadFf).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
                Assert.assertEquals(0, state.getRecords().size());
            }
        });
    }


    @Test
    public void testShortReadOnVersionField() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_short_ver";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            FilesFacade shortReadFf = new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == TableTransactionLogFile.TX_LOG_STRUCTURE_VERSION_OFFSET && len == Integer.BYTES) {
                        return 2;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            try (Path txnlogPath = new Path()) {
                txnlogPathOf(tableName, txnlogPath);
                SeqTxnLogState state = new BoundedSeqTxnLogReader(shortReadFf).read(txnlogPath.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.SHORT_FILE));
            }
        });
    }

    @Test
    public void testShortReadOnMaxTxnField() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "seq_short_maxtxn";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            FilesFacade shortReadFf = new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == TableTransactionLogFile.MAX_TXN_OFFSET_64 && len == Long.BYTES) {
                        return 4;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            try (Path txnlogPath = new Path()) {
                txnlogPathOf(tableName, txnlogPath);
                SeqTxnLogState state = new BoundedSeqTxnLogReader(shortReadFf).read(txnlogPath.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.SHORT_FILE));
            }
        });
    }


    @Test
    public void testUnsetStateOnMissingFile() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                path.of(temp.getRoot().getAbsolutePath()).concat("no_such_file").concat("_txnlog").$();
                SeqTxnLogState state = new BoundedSeqTxnLogReader(FF).read(path.$());
                Assert.assertEquals(TxnState.UNSET_INT, state.getVersion());
                Assert.assertEquals(TxnState.UNSET_LONG, state.getMaxTxn());
                Assert.assertEquals(TxnState.UNSET_LONG, state.getCreateTimestamp());
                Assert.assertEquals(TxnState.UNSET_INT, state.getPartSize());
                Assert.assertEquals(0, state.getRecords().size());
                Assert.assertTrue(state.getIssues().size() > 0);
            }
        });
    }


    private static String dumpIssues(SeqTxnLogState state) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0, n = state.getIssues().size(); i < n; i++) {
            if (i > 0) sb.append("; ");
            sb.append(state.getIssues().getQuick(i).code()).append(':').append(state.getIssues().getQuick(i).message());
        }
        return sb.toString();
    }

    private static SeqTxnRecord findFirstDataRecord(SeqTxnLogState state) {
        for (int i = 0, n = state.getRecords().size(); i < n; i++) {
            SeqTxnRecord rec = state.getRecords().getQuick(i);
            if (!rec.isDdlChange()) {
                return rec;
            }
        }
        return null;
    }

    private static long getRowCount(String tableName) throws SqlException {
        try (RecordCursorFactory factory = select("select count() from " + tableName)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }

    private static boolean hasIssue(SeqTxnLogState state, RecoveryIssueCode issueCode) {
        for (int i = 0, n = state.getIssues().size(); i < n; i++) {
            if (state.getIssues().getQuick(i).code() == issueCode) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasIssueContaining(SeqTxnLogState state, String substring) {
        for (int i = 0, n = state.getIssues().size(); i < n; i++) {
            if (state.getIssues().getQuick(i).message().contains(substring)) {
                return true;
            }
        }
        return false;
    }

    private static SeqTxnLogState readSeqTxnLogState(String tableName) {
        return readSeqTxnLogStateWithReader(tableName, new BoundedSeqTxnLogReader(FF));
    }

    private static SeqTxnLogState readSeqTxnLogStateWithReader(String tableName, BoundedSeqTxnLogReader reader) {
        try (Path txnlogPath = new Path()) {
            return reader.read(txnlogPathOf(tableName, txnlogPath).$());
        }
    }

    private static Path txnlogPathOf(String tableName, Path path) {
        TableToken tableToken = engine.verifyTableName(tableName);
        return path.of(engine.getConfiguration().getDbRoot())
                .concat(tableToken)
                .concat(WalUtils.SEQ_DIR)
                .concat(WalUtils.TXNLOG_FILE_NAME);
    }

    private static void waitForAppliedRows(String tableName, int expectedRows) throws SqlException {
        for (int i = 0; i < 20; i++) {
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            drainWalQueue(engine);
            if (getRowCount(tableName) == expectedRows) {
                return;
            }
        }
        Assert.assertEquals(expectedRows, getRowCount(tableName));
    }

    private static void writeRawBytes(LPSZ filePath, int size) {
        long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(buf, size, (byte) 0);
            long fd = FF.openRW(filePath, CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            try {
                Assert.assertEquals(size, FF.write(fd, buf, size, 0));
                Assert.assertTrue(FF.truncate(fd, size));
            } finally {
                FF.close(fd);
            }
        } finally {
            Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
