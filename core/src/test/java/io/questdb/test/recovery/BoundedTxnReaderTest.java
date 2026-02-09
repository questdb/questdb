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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.griffin.SqlException;
import io.questdb.recovery.BoundedTxnReader;
import io.questdb.recovery.RecoveryIssueCode;
import io.questdb.recovery.TxnPartitionState;
import io.questdb.recovery.TxnState;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

public class BoundedTxnReaderTest extends AbstractCairoTest {
    private static final FilesFacade FF = TestFilesFacadeImpl.INSTANCE;

    @Test
    public void testCapsLargePartitionOutput() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_large";
            createTableWithTxnPartitions(tableName, 512);

            TxnState state = readTxnState(tableName, new BoundedTxnReader(FF, 128, 17));
            Assert.assertEquals(17, state.getPartitions().size());
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.TRUNCATED_OUTPUT));
        });
    }

    @Test
    public void testCapsLargeSymbolOutput() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_cap_sym";
            execute("create table " + tableName + " (s1 symbol, s2 symbol, s3 symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter(tableName)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putSym(0, "A");
                row.putSym(1, "B");
                row.putSym(2, "C");
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows(tableName, 1);

            TxnState state = readTxnState(tableName, new BoundedTxnReader(FF, 1, BoundedTxnReader.DEFAULT_MAX_PARTITIONS));
            Assert.assertEquals(1, state.getSymbols().size());
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.TRUNCATED_OUTPUT));
        });
    }

    @Test
    public void testConstructorClampsZeroCaps() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_clamp";
            createTableWithTxnPartitions(tableName, 4);

            TxnState state = readTxnState(tableName, new BoundedTxnReader(FF, 0, 0));
            Assert.assertEquals(1, state.getPartitions().size());
            Assert.assertEquals(1, state.getSymbols().size());
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.TRUNCATED_OUTPUT));
        });
    }

    @Test
    public void testCorruptNegativeMapWriterCount() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_neg_mwc";
            createTableWithTxnPartitions(tableName, 2);

            try (Path txnPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                Path path = txnPathOf(tableName, txnPath);
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);

                long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                boolean isA = (version & 1L) == 0L;
                int baseOffset = mem.getInt(isA ? TableUtils.TX_BASE_OFFSET_A_32 : TableUtils.TX_BASE_OFFSET_B_32);
                mem.putInt(baseOffset + TableUtils.TX_OFFSET_MAP_WRITER_COUNT_32, -1);

                TxnState state = new BoundedTxnReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.INVALID_COUNT));
                Assert.assertTrue(hasIssueContaining(state, "map writer count is negative"));
            }
        });
    }

    @Test
    public void testCorruptNegativePartitionSegmentSize() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_neg_pseg";
            createTableWithTxnPartitions(tableName, 2);

            try (Path txnPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                Path path = txnPathOf(tableName, txnPath);
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);

                long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                boolean isA = (version & 1L) == 0L;
                long partSizeOffset = isA ? TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_B_32;
                mem.putInt(partSizeOffset, -32);

                TxnState state = new BoundedTxnReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.INVALID_COUNT));
                Assert.assertTrue(hasIssueContaining(state, "partition segment size is negative"));
                Assert.assertEquals(0, state.getPartitions().size());
            }
        });
    }

    @Test
    public void testCorruptNegativeSymbolsSegmentSize() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_neg_sseg";
            createTableWithTxnPartitions(tableName, 2);

            try (Path txnPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                Path path = txnPathOf(tableName, txnPath);
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);

                long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                boolean isA = (version & 1L) == 0L;
                long symSizeOffset = isA ? TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 : TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_B_32;
                mem.putInt(symSizeOffset, -8);

                TxnState state = new BoundedTxnReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.INVALID_COUNT));
                Assert.assertTrue(hasIssueContaining(state, "symbol segment size is negative"));
                Assert.assertEquals(0, state.getSymbols().size());
            }
        });
    }

    @Test
    public void testCorruptNonAlignedPartitionSegmentSize() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_nalign_pseg";
            createTableWithTxnPartitions(tableName, 2);

            try (Path txnPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                Path path = txnPathOf(tableName, txnPath);
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);

                long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                boolean isA = (version & 1L) == 0L;
                long partSizeOffset = isA ? TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_B_32;
                mem.putInt(partSizeOffset, 13);

                TxnState state = new BoundedTxnReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.INVALID_COUNT));
                Assert.assertTrue(hasIssueContaining(state, "not long aligned"));
            }
        });
    }

    @Test
    public void testCorruptNonAlignedSymbolsSegmentSize() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_nalign_sseg";
            createTableWithTxnPartitions(tableName, 2);

            try (Path txnPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                Path path = txnPathOf(tableName, txnPath);
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);

                long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                boolean isA = (version & 1L) == 0L;
                long symSizeOffset = isA ? TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 : TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_B_32;
                mem.putInt(symSizeOffset, 5);

                TxnState state = new BoundedTxnReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.INVALID_COUNT));
                Assert.assertTrue(hasIssueContaining(state, "not long aligned"));
            }
        });
    }

    @Test
    public void testCorruptNonEntryAlignedPartitionLongCount() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_nentry_pseg";
            createTableWithTxnPartitions(tableName, 2);

            try (Path txnPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                Path path = txnPathOf(tableName, txnPath);
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);

                long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                boolean isA = (version & 1L) == 0L;
                long partSizeOffset = isA ? TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_B_32;
                // 24 bytes = 3 longs, not divisible by LONGS_PER_TX_ATTACHED_PARTITION (4)
                mem.putInt(partSizeOffset, 24);

                TxnState state = new BoundedTxnReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.INVALID_COUNT));
                Assert.assertTrue(hasIssueContaining(state, "not partition-entry aligned"));
            }
        });
    }

    @Test
    public void testCorruptPartitionFlagsParquetAndReadOnly() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_flags";
            createTableWithTxnPartitions(tableName, 2);

            try (Path txnPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                Path path = txnPathOf(tableName, txnPath);
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);

                long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                boolean isA = (version & 1L) == 0L;
                int baseOffset = mem.getInt(isA ? TableUtils.TX_BASE_OFFSET_A_32 : TableUtils.TX_BASE_OFFSET_B_32);
                int symbolsSegmentSize = mem.getInt(isA ? TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 : TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_B_32);
                int symbolCount = symbolsSegmentSize / Long.BYTES;

                // Compute offset of the first partition entry's maskedSize (second long in entry)
                long partitionDataOffset = baseOffset + TableUtils.getPartitionTableIndexOffset(
                        TableUtils.getPartitionTableSizeOffset(symbolCount), 0
                );
                long maskedSizeOffset = partitionDataOffset + Long.BYTES;

                // Set parquet (bit 61) and read-only (bit 62) flags, preserving row count
                long originalMaskedSize = mem.getLong(maskedSizeOffset);
                mem.putLong(maskedSizeOffset, originalMaskedSize | (1L << 61) | (1L << 62));

                TxnState state = new BoundedTxnReader(FF).read(path.$());
                Assert.assertTrue(state.getPartitions().size() >= 1);
                TxnPartitionState first = state.getPartitions().getQuick(0);
                Assert.assertTrue(first.isParquetFormat());
                Assert.assertTrue(first.isReadOnly());
                // Row count should be preserved (low 44 bits unchanged)
                Assert.assertEquals(originalMaskedSize & ((1L << 44) - 1), first.getRowCount());
            }
        });
    }

    @Test
    public void testCorruptZeroPartitionSegmentSize() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_zero_pseg";
            createTableWithTxnPartitions(tableName, 2);

            try (Path txnPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                Path path = txnPathOf(tableName, txnPath);
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);

                long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                boolean isA = (version & 1L) == 0L;
                long partSizeOffset = isA ? TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_B_32;
                mem.putInt(partSizeOffset, 0);

                TxnState state = new BoundedTxnReader(FF).read(path.$());
                Assert.assertEquals(0, state.getPartitions().size());
                Assert.assertFalse(hasIssue(state, RecoveryIssueCode.INVALID_COUNT));
            }
        });
    }

    @Test
    public void testDetectsMapWriterCountMismatch() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_map_mismatch";
            createTableWithTxnPartitions(tableName, 4);

            try (Path txnPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                Path path = txnPathOf(tableName, txnPath);
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);

                long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                boolean isA = (version & 1L) == 0L;
                int baseOffset = mem.getInt(isA ? TableUtils.TX_BASE_OFFSET_A_32 : TableUtils.TX_BASE_OFFSET_B_32);
                mem.putInt(baseOffset + TableUtils.TX_OFFSET_MAP_WRITER_COUNT_32, 8);

                TxnState state = new BoundedTxnReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.META_COLUMN_COUNT_MISMATCH));
            }
        });
    }

    @Test
    public void testDetectsMissingTxnFile() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_missing";
            createTableWithTxnPartitions(tableName, 1);

            try (Path txnPath = new Path()) {
                Path path = txnPathOf(tableName, txnPath);
                Assert.assertTrue(FF.removeQuiet(path.$()));

                TxnState state = new BoundedTxnReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.MISSING_FILE));
            }
        });
    }

    @Test
    public void testDetectsRecordOutsideFileAfterTruncate() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_truncated";
            createTableWithTxnPartitions(tableName, 5);

            try (Path txnPath = new Path()) {
                Path path = txnPathOf(tableName, txnPath);
                long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(fd > -1);
                try {
                    Assert.assertTrue(FF.truncate(fd, 96));
                } finally {
                    FF.close(fd);
                }

                TxnState state = new BoundedTxnReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.PARTIAL_READ));
            }
        });
    }

    @Test
    public void testDetectsShortFile() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_short";
            createTableWithTxnPartitions(tableName, 2);

            try (Path txnPath = new Path()) {
                Path path = txnPathOf(tableName, txnPath);
                long fd = FF.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(fd > -1);
                try {
                    Assert.assertTrue(FF.truncate(fd, 8));
                } finally {
                    FF.close(fd);
                }

                TxnState state = new BoundedTxnReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.SHORT_FILE));
            }
        });
    }

    @Test
    public void testFileOpenFailureWithExistingFile() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_open_fail";
            createTableWithTxnPartitions(tableName, 1);

            try (Path txnPath = new Path()) {
                Path path = txnPathOf(tableName, txnPath);
                final String txnPathStr = path.$().toString();

                FilesFacade failOpenFf = new TestFilesFacadeImpl() {
                    @Override
                    public long openRO(LPSZ name) {
                        if (name.toString().equals(txnPathStr)) {
                            return -1;
                        }
                        return super.openRO(name);
                    }
                };

                TxnState state = new BoundedTxnReader(failOpenFf).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
                Assert.assertTrue(hasIssueContaining(state, "cannot open"));
            }
        });
    }

    @Test
    public void testReadFailureIoErrorOnIntField() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_ioerr_int";
            createTableWithTxnPartitions(tableName, 2);
            final int baseOffset = readBaseOffset(tableName);
            final long targetOffset = baseOffset + TableUtils.TX_OFFSET_MAP_WRITER_COUNT_32;

            FilesFacade failReadFf = new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == targetOffset && len == Integer.BYTES) {
                        return -1;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            TxnState state = readTxnStateWithFf(tableName, failReadFf);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
            Assert.assertTrue(hasIssueContaining(state, "mapWriterCount"));
        });
    }

    @Test
    public void testReadFailureIoErrorOnLongField() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_ioerr_long";
            createTableWithTxnPartitions(tableName, 2);
            final int baseOffset = readBaseOffset(tableName);
            final long targetOffset = baseOffset + TableUtils.TX_OFFSET_TXN_64;

            FilesFacade failReadFf = new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == targetOffset && len == Long.BYTES) {
                        return -1;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            TxnState state = readTxnStateWithFf(tableName, failReadFf);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
            Assert.assertTrue(hasIssueContaining(state, "txn"));
        });
    }

    @Test
    public void testReadFailureOnBaseOffsetField() throws Exception {
        // Exercises readTxn line 316: early return after base.offset read failure.
        // The base.offset field is in the 64-byte base header (not the record),
        // so there is no way to make this fail via file truncation — the SHORT_FILE
        // check at line 68 ensures the full base header is present before readTxn
        // is ever called.  A mock is the only option.
        assertMemoryLeak(() -> {
            final String tableName = "txn_fail_baseoff";
            createTableWithTxnPartitions(tableName, 1);

            // Determine which slot is active so we target the right offset
            final long activeBaseOffsetAddr = readActiveSlotIsA(tableName)
                    ? TableUtils.TX_BASE_OFFSET_A_32
                    : TableUtils.TX_BASE_OFFSET_B_32;

            FilesFacade failReadFf = new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == activeBaseOffsetAddr && len == Integer.BYTES) {
                        return -1;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            TxnState state = readTxnStateWithFf(tableName, failReadFf);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
            Assert.assertTrue(hasIssueContaining(state, "base.offset"));
            Assert.assertEquals(TxnState.UNSET_INT, state.getRecordBaseOffset());
        });
    }

    @Test
    public void testReadFailureOnBaseVersionField() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_fail_ver";
            createTableWithTxnPartitions(tableName, 1);

            FilesFacade failReadFf = new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == TableUtils.TX_BASE_OFFSET_VERSION_64 && len == Long.BYTES) {
                        return -1;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            TxnState state = readTxnStateWithFf(tableName, failReadFf);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
            Assert.assertEquals(TxnState.UNSET_LONG, state.getBaseVersion());
        });
    }

    @Test
    public void testReadFailureOnColumnVersionField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(13, "columnVersion"));
    }

    @Test
    public void testReadFailureOnDataVersionField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(11, "dataVersion"));
    }

    @Test
    public void testReadFailureOnLagMaxTimestampField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(20, "lagMaxTimestamp"));
    }

    @Test
    public void testReadFailureOnLagMinTimestampField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(19, "lagMinTimestamp"));
    }

    @Test
    public void testReadFailureOnLagRowCountField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(18, "lagRowCount"));
    }

    @Test
    public void testReadFailureOnLagTxnCountField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(17, "lagTxnCount"));
    }

    @Test
    public void testReadFailureOnMaxTimestampField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(9, "maxTimestamp"));
    }

    @Test
    public void testReadFailureOnMidHeaderField() throws Exception {
        // Exercises readHeader line 406: early return when fixedRowCount read fails.
        // readHeader has 16 sequential field reads with identical early-return guards;
        // this test covers the pattern for a field in the middle of the sequence.
        // The whole record fits in the file (line 357 check passes), so there is no
        // way to make individual field reads fail via truncation — a mock is needed.
        assertMemoryLeak(() -> {
            final String tableName = "txn_fail_mid";
            createTableWithTxnPartitions(tableName, 2);
            final int baseOffset = readBaseOffset(tableName);
            final long targetOffset = baseOffset + TableUtils.TX_OFFSET_FIXED_ROW_COUNT_64;

            FilesFacade failReadFf = new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == targetOffset && len == Long.BYTES) {
                        return -1;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            TxnState state = readTxnStateWithFf(tableName, failReadFf);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
            Assert.assertTrue(hasIssueContaining(state, "fixedRowCount"));
            // txn (before fixedRowCount) was read successfully
            Assert.assertNotEquals(TxnState.UNSET_LONG, state.getTxn());
            // minTimestamp (after fixedRowCount) was never reached
            Assert.assertEquals(TxnState.UNSET_LONG, state.getMinTimestamp());
        });
    }

    @Test
    public void testReadFailureOnMinTimestampField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(8, "minTimestamp"));
    }

    @Test
    public void testReadFailureOnPartitionField() throws Exception {
        // Exercises readPartitions line 199: early return when a partition entry
        // field read fails.  The whole record fits in the file (validated at line 357),
        // so all partition offsets pass isRangeReadable.  Only an ff.read() failure
        // can trigger this branch — a mock is needed.
        assertMemoryLeak(() -> {
            final String tableName = "txn_fail_part";
            createTableWithTxnPartitions(tableName, 2);
            final int baseOffset = readBaseOffset(tableName);
            final int symbolsSegmentSize = readSymbolsSegmentSize(tableName);
            final int symbolCount = symbolsSegmentSize / Long.BYTES;

            // First partition entry's timestamp (first long in entry)
            final long targetOffset = baseOffset + TableUtils.getPartitionTableIndexOffset(
                    TableUtils.getPartitionTableSizeOffset(symbolCount), 0
            );

            FilesFacade failReadFf = new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == targetOffset && len == Long.BYTES) {
                        return -1;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            TxnState state = readTxnStateWithFf(tableName, failReadFf);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
            Assert.assertTrue(hasIssueContaining(state, "partition.ts"));
            Assert.assertEquals(0, state.getPartitions().size());
        });
    }

    @Test
    public void testReadFailureOnPartitionMaskedSizeField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(25, "partition.maskedSize"));
    }

    @Test
    public void testReadFailureOnPartitionNameTxnField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(26, "partition.nameTxn"));
    }

    @Test
    public void testReadFailureOnPartitionParquetSizeField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(27, "partition.parquetFileSize"));
    }

    @Test
    public void testReadFailureOnPartitionSegmentSizeField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(4, "base.partitionSegmentSize"));
    }

    @Test
    public void testReadFailureOnPartitionTableVersionField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(12, "partitionTableVersion"));
    }

    @Test
    public void testReadFailureOnSeqTxnField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(15, "seqTxn"));
    }

    @Test
    public void testReadFailureOnStructureVersionField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(10, "structureVersion"));
    }

    @Test
    public void testReadFailureOnSymbolField() throws Exception {
        // Exercises readSymbols line 287: early return when a symbol entry
        // field read fails.  Same reasoning as testReadFailureOnPartitionField —
        // isRangeReadable passes but ff.read() fails.
        assertMemoryLeak(() -> {
            final String tableName = "txn_fail_sym";
            createTableWithTxnPartitions(tableName, 2);
            final int baseOffset = readBaseOffset(tableName);

            // First symbol entry's count (first int in entry)
            final long targetOffset = baseOffset + TableUtils.getSymbolWriterIndexOffset(0);

            FilesFacade failReadFf = new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == targetOffset && len == Integer.BYTES) {
                        return -1;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            TxnState state = readTxnStateWithFf(tableName, failReadFf);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.IO_ERROR));
            Assert.assertTrue(hasIssueContaining(state, "symbol.count"));
            Assert.assertEquals(0, state.getSymbols().size());
        });
    }

    @Test
    public void testReadFailureOnSymbolsSegmentSizeField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(3, "base.symbolsSegmentSize"));
    }

    @Test
    public void testReadFailureOnSymbolTransientCountField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(23, "symbol.transientCount"));
    }

    @Test
    public void testReadFailureOnTransientRowCountField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(6, "transientRowCount"));
    }

    @Test
    public void testReadFailureOnTruncateVersionField() throws Exception {
        assertMemoryLeak(() -> assertReadFailureOnNthCall(14, "truncateVersion"));
    }

    @Test
    public void testReadFailureShortReadOnIntField() throws Exception {
        // Exercises readIntValue line 503: bytesRead >= 0 && bytesRead < Integer.BYTES
        // → SHORT_FILE (as opposed to IO_ERROR for bytesRead < 0).
        assertMemoryLeak(() -> {
            final String tableName = "txn_short_int";
            createTableWithTxnPartitions(tableName, 2);
            final int baseOffset = readBaseOffset(tableName);
            final long targetOffset = baseOffset + TableUtils.TX_OFFSET_MAP_WRITER_COUNT_32;

            FilesFacade failReadFf = new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == targetOffset && len == Integer.BYTES) {
                        return 2;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            TxnState state = readTxnStateWithFf(tableName, failReadFf);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.SHORT_FILE));
            Assert.assertTrue(hasIssueContaining(state, "mapWriterCount"));
        });
    }

    @Test
    public void testReadFailureShortReadOnLongField() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_short_long";
            createTableWithTxnPartitions(tableName, 2);
            final int baseOffset = readBaseOffset(tableName);
            final long targetOffset = baseOffset + TableUtils.TX_OFFSET_TXN_64;

            FilesFacade failReadFf = new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (offset == targetOffset && len == Long.BYTES) {
                        return 4;
                    }
                    return super.read(fd, buf, len, offset);
                }
            };

            TxnState state = readTxnStateWithFf(tableName, failReadFf);
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.SHORT_FILE));
            Assert.assertTrue(hasIssueContaining(state, "txn"));
        });
    }

    @Test
    public void testReadsValidTxn() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_valid";
            createTableWithTxnPartitions(tableName, 4);

            TxnState state = readTxnState(tableName, new BoundedTxnReader(FF));
            Assert.assertEquals(4, state.getRowCount());
            Assert.assertEquals(4, state.getPartitions().size());
            Assert.assertEquals(1, state.getSymbols().size());
            Assert.assertEquals(0, state.getIssues().size());
            Assert.assertTrue(state.getTxn() >= 1);
            Assert.assertTrue(state.getSeqTxn() >= 0);
        });
    }

    @Test
    public void testReadsValidTxnWithNoSymbolColumns() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_no_sym";
            execute("create table " + tableName + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");

            try (WalWriter walWriter = getWalWriter(tableName)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putLong(0, 42L);
                row.append();
                walWriter.commit();
            }
            waitForAppliedRows(tableName, 1);

            TxnState state = readTxnState(tableName, new BoundedTxnReader(FF));
            Assert.assertEquals(0, state.getSymbols().size());
            Assert.assertEquals(0, state.getIssues().size());
            Assert.assertEquals(1, state.getPartitions().size());
        });
    }

    @Test
    public void testRejectsInvalidBaseOffset() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_invalid_base";
            createTableWithTxnPartitions(tableName, 3);

            try (Path txnPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                Path path = txnPathOf(tableName, txnPath);
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);

                long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                boolean isA = (version & 1L) == 0L;
                mem.putInt(isA ? TableUtils.TX_BASE_OFFSET_A_32 : TableUtils.TX_BASE_OFFSET_B_32, 16);

                TxnState state = new BoundedTxnReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.INVALID_OFFSET));
            }
        });
    }

    @Test
    public void testSlotBActiveVersion() throws Exception {
        // Exercises readTxn lines 307, 313, 326, 339: the B-slot code paths.
        // After WAL draining the active slot may be A or B depending on how
        // many internal commits occurred.  We read from the active slot, copy
        // the values to the inactive slot, then flip the version parity so the
        // reader switches to the slot we just populated.
        assertMemoryLeak(() -> {
            final String tableName = "txn_slot_b";
            createTableWithTxnPartitions(tableName, 2);

            try (Path txnPath = new Path()) {
                Path path = txnPathOf(tableName, txnPath);

                boolean wasA;
                try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                    mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);

                    long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                    wasA = (version & 1L) == 0L;

                    // Read from active slot
                    int baseOff = mem.getInt(wasA ? TableUtils.TX_BASE_OFFSET_A_32 : TableUtils.TX_BASE_OFFSET_B_32);
                    int symSize = mem.getInt(wasA ? TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 : TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_B_32);
                    int partSize = mem.getInt(wasA ? TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_B_32);

                    // Copy to inactive slot
                    mem.putInt(wasA ? TableUtils.TX_BASE_OFFSET_B_32 : TableUtils.TX_BASE_OFFSET_A_32, baseOff);
                    mem.putInt(wasA ? TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_B_32 : TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_A_32, symSize);
                    mem.putInt(wasA ? TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_B_32 : TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_A_32, partSize);

                    // Flip version parity so the inactive slot becomes active
                    mem.putLong(TableUtils.TX_BASE_OFFSET_VERSION_64, version ^ 1L);
                }

                TxnState state = new BoundedTxnReader(FF).read(path.$());
                // The reader should now use the opposite slot from what was originally active
                Assert.assertEquals(wasA, (state.getBaseVersion() & 1L) == 1L);
                for (int i = 0, n = state.getIssues().size(); i < n; i++) {
                    Assert.fail("unexpected issue: " + state.getIssues().getQuick(i).getMessage());
                }
                Assert.assertEquals(2, state.getPartitions().size());
                Assert.assertEquals(1, state.getSymbols().size());
            }
        });
    }

    @Test
    public void testCorruptBaseOffsetExceedsFileSize() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_base_beyond";
            createTableWithTxnPartitions(tableName, 2);

            try (Path txnPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                Path path = txnPathOf(tableName, txnPath);
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);

                long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                boolean isA = (version & 1L) == 0L;
                // set base offset to a huge value beyond any reasonable file
                mem.putInt(isA ? TableUtils.TX_BASE_OFFSET_A_32 : TableUtils.TX_BASE_OFFSET_B_32, Integer.MAX_VALUE / 2);

                TxnState state = new BoundedTxnReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.PARTIAL_READ));
            }
        });
    }

    @Test
    public void testCorruptPartitionSegmentExceedsRemainingFile() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_pseg_beyond";
            createTableWithTxnPartitions(tableName, 2);

            try (Path txnPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                Path path = txnPathOf(tableName, txnPath);
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);

                long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                boolean isA = (version & 1L) == 0L;
                // set partition segment size to a huge value that would exceed the file
                mem.putInt(
                        isA ? TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_B_32,
                        100_000
                );

                TxnState state = new BoundedTxnReader(FF).read(path.$());
                Assert.assertTrue(hasIssue(state, RecoveryIssueCode.PARTIAL_READ));
            }
        });
    }

    @Test
    public void testExtremeTimestampValues() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "txn_extreme_ts";
            createTableWithTxnPartitions(tableName, 2);

            try (Path txnPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                Path path = txnPathOf(tableName, txnPath);
                mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);

                long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                boolean isA = (version & 1L) == 0L;
                int baseOffset = mem.getInt(isA ? TableUtils.TX_BASE_OFFSET_A_32 : TableUtils.TX_BASE_OFFSET_B_32);
                // set min timestamp to Long.MIN_VALUE and max to Long.MAX_VALUE
                mem.putLong(baseOffset + TableUtils.TX_OFFSET_MIN_TIMESTAMP_64, Long.MIN_VALUE);
                mem.putLong(baseOffset + TableUtils.TX_OFFSET_MAX_TIMESTAMP_64, Long.MAX_VALUE);

                TxnState state = new BoundedTxnReader(FF).read(path.$());
                Assert.assertEquals(Long.MIN_VALUE, state.getMinTimestamp());
                Assert.assertEquals(Long.MAX_VALUE, state.getMaxTimestamp());
                // should parse without error (timestamps are opaque values)
                Assert.assertFalse(hasIssue(state, RecoveryIssueCode.IO_ERROR));
            }
        });
    }

    private static void assertReadFailureOnNthCall(int failAtCall, String expectedField) throws Exception {
        final String tableName = "txn_nth_" + failAtCall;
        createTableWithTxnPartitions(tableName, 2);
        TxnState state = readTxnStateWithFf(tableName, failOnNthReadFf(failAtCall));
        Assert.assertTrue("expected IO_ERROR for " + expectedField, hasIssue(state, RecoveryIssueCode.IO_ERROR));
        Assert.assertTrue("expected issue containing '" + expectedField + "'", hasIssueContaining(state, expectedField));
    }

    private static void createTableWithTxnPartitions(String tableName, int partitionCount) throws SqlException {
        execute("create table " + tableName + " (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");

        try (WalWriter walWriter = getWalWriter(tableName)) {
            long ts = 0;
            for (int i = 0; i < partitionCount; i++) {
                TableWriter.Row row = walWriter.newRow(ts);
                row.putSym(0, (i & 1) == 0 ? "AA" : "BB");
                row.append();
                ts += Micros.DAY_MICROS;
            }
            walWriter.commit();
        }

        waitForAppliedRows(tableName, partitionCount);
    }

    private static FilesFacade failOnNthReadFf(int failAtCall) {
        return new TestFilesFacadeImpl() {
            int callCount = 0;

            @Override
            public long read(long fd, long buf, long len, long offset) {
                if (++callCount == failAtCall) {
                    return -1;
                }
                return super.read(fd, buf, len, offset);
            }
        };
    }

    private static long getRowCount(String tableName) throws SqlException {
        try (RecordCursorFactory factory = select("select count() from " + tableName)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }

    private static boolean hasIssue(TxnState state, RecoveryIssueCode issueCode) {
        for (int i = 0, n = state.getIssues().size(); i < n; i++) {
            if (state.getIssues().getQuick(i).getCode() == issueCode) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasIssueContaining(TxnState state, String substring) {
        for (int i = 0, n = state.getIssues().size(); i < n; i++) {
            if (state.getIssues().getQuick(i).getMessage().contains(substring)) {
                return true;
            }
        }
        return false;
    }

    private static boolean readActiveSlotIsA(String tableName) {
        try (Path txnPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
            Path path = txnPathOf(tableName, txnPath);
            mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);
            long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
            return (version & 1L) == 0L;
        }
    }

    private static int readBaseOffset(String tableName) {
        try (Path txnPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
            Path path = txnPathOf(tableName, txnPath);
            mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);
            long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
            boolean isA = (version & 1L) == 0L;
            return mem.getInt(isA ? TableUtils.TX_BASE_OFFSET_A_32 : TableUtils.TX_BASE_OFFSET_B_32);
        }
    }

    private static int readSymbolsSegmentSize(String tableName) {
        try (Path txnPath = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
            Path path = txnPathOf(tableName, txnPath);
            mem.smallFile(FF, path.$(), MemoryTag.MMAP_DEFAULT);
            long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
            boolean isA = (version & 1L) == 0L;
            return mem.getInt(isA ? TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 : TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_B_32);
        }
    }

    private static TxnState readTxnState(String tableName, BoundedTxnReader reader) {
        try (Path txnPath = new Path()) {
            return reader.read(txnPathOf(tableName, txnPath).$());
        }
    }

    private static TxnState readTxnStateWithFf(String tableName, FilesFacade customFf) {
        try (Path txnPath = new Path()) {
            return new BoundedTxnReader(customFf).read(txnPathOf(tableName, txnPath).$());
        }
    }

    private static Path txnPathOf(String tableName, Path path) {
        TableToken tableToken = engine.verifyTableName(tableName);
        return path.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME);
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
}
