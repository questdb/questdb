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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TableTransactionLogV2;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.wal.seq.TableTransactionLogFile.HEADER_SEQ_PART_SIZE_32;
import static io.questdb.cairo.wal.seq.TableTransactionLogV2.RECORD_SIZE;
import static io.questdb.cairo.wal.seq.TableTransactionLogV2.RESERVED_OFFSET;

/**
 * Checks the per-record checksum of the V2 sequencer txnlog.
 * <p>
 * Each V2 txnlog record ends with a reserved trailing {@code long} at {@code RESERVED_OFFSET}, where the
 * writer stores {@code calculateCvAreaChecksum(body)}. The reader verifies it on every {@code hasNext()}
 * call; a mismatch suspends the table. A stored 0 means "no checksum" and the record is read unverified.
 */
public class TableTransactionLogV2CrcTest extends AbstractCairoTest {

    // A small part size keeps the test fast and every record within part 0.
    private static final int SEQ_PART_TXN_COUNT = 16;

    /**
     * After several WAL inserts with V2 txnlog enabled, reads the raw {@code _txn_parts/0} file and asserts
     * that every record's reserved slot is non-zero and equals {@code calculateCvAreaChecksum} over the
     * record body {@code [i*RECORD_SIZE, i*RECORD_SIZE+RESERVED_OFFSET)}.
     */
    @Test
    public void testWriterStoresCrcInReservedSlot() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, SEQ_PART_TXN_COUNT);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            // Commit a few rows to produce several seq txn records.
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            execute("insert into x values ('2024-01-01T00:01:00.000000Z', 2)");
            execute("insert into x values ('2024-01-01T00:02:00.000000Z', 3)");
            drainWalQueue();

            TableToken tt = engine.verifyTableName("x");
            engine.releaseInactive(); // close writer mmaps so we can read the part file independently

            CairoConfiguration cfg = engine.getConfiguration();
            // Path: <dbRoot>/<dirName>/txn_seq/_txn_parts/0
            try (Path path = new Path()) {
                path.of(cfg.getDbRoot())
                        .concat(tt.getDirName())
                        .concat(WalUtils.SEQ_DIR)
                        .concat(WalUtils.TXNLOG_PARTS_DIR)
                        .slash().put(0L);

                long fd = cfg.getFilesFacade().openRO(path.$());
                Assert.assertTrue("part file must exist: " + path, fd > -1);
                try {
                    // mmap the part file (fixed size = SEQ_PART_TXN_COUNT * RECORD_SIZE)
                    final long partSize = (long) SEQ_PART_TXN_COUNT * RECORD_SIZE;
                    long addr = cfg.getFilesFacade().mmap(fd, partSize, 0, Files.MAP_RO, MemoryTag.MMAP_TX_LOG_CURSOR);
                    Assert.assertTrue("mmap failed", addr != -1L);
                    try {
                        // 3 committed rows -> at least 3 seq txns (txn 1..3 = indices 0..2 in part 0).
                        // (txn is 1-based, index in part = (txn-1) % partCount)
                        int txnCount = 3;
                        for (int i = 0; i < txnCount; i++) {
                            long recordBase = addr + (long) i * RECORD_SIZE;
                            long storedCrc = Unsafe.getLong(recordBase + RESERVED_OFFSET);
                            Assert.assertNotEquals(
                                    "record " + i + ": reserved slot must be non-zero (CRC must be written)",
                                    0L, storedCrc
                            );
                            long expectedCrc = TableUtils.calculateCvAreaChecksum(recordBase, RESERVED_OFFSET);
                            Assert.assertEquals(
                                    "record " + i + ": stored CRC must match computed checksum over body",
                                    expectedCrc, storedCrc
                            );
                        }
                    } finally {
                        cfg.getFilesFacade().munmap(addr, partSize, MemoryTag.MMAP_TX_LOG_CURSOR);
                    }
                } finally {
                    cfg.getFilesFacade().close(fd);
                }
            }
        });
    }

    /**
     * Flips a byte of the first seq txn record's structureVersion field, which the CRC covers but which is
     * not the reserved CRC slot itself, then drains the WAL queue and asserts the table is suspended.
     */
    @Test
    public void testCorruptedBodyByteSuspendsTable() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, SEQ_PART_TXN_COUNT);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            execute("insert into x values ('2024-01-01T00:01:00.000000Z', 2)");

            TableToken tt = engine.verifyTableName("x");
            engine.releaseInactive();

            CairoConfiguration cfg = engine.getConfiguration();
            try (Path path = new Path()) {
                path.of(cfg.getDbRoot())
                        .concat(tt.getDirName())
                        .concat(WalUtils.SEQ_DIR)
                        .concat(WalUtils.TXNLOG_PARTS_DIR)
                        .slash().put(0L);

                // Corrupt the first byte of the first record's structureVersion field (offset 0 in record 0).
                // This is a pure body byte: covered by the CRC but not the reserved slot itself.
                pokeByte(cfg, path.$(), 0L);
            }

            // ApplyWal2TableJob reads the cursor; hasNext() must detect the CRC mismatch and throw,
            // which propagates to the exception handler that calls suspendTable().
            drainWalQueue();

            Assert.assertTrue(
                    "table must be suspended after a torn (corrupt body) V2 txnlog record",
                    engine.getTableSequencerAPI().isSuspended(tt)
            );
        });
    }

    /**
     * A zero in the reserved slot is read as "no checksum" and the record goes unverified.
     * <p>
     * That is the one detection V2 gives up. A QuestDB that predates the checksum writes a literal zero into
     * this slot, so a zero cannot be told from a checksum a torn write erased, and treating it as damage would
     * suspend the table on records an older binary wrote correctly. A file-level "checksummed from txn N"
     * watermark does not help: an older binary cannot withdraw it.
     * <p>
     * {@link #testCorruptedBodyByteSuspendsTable()} covers a corrupt (non-zero, wrong) V2 checksum.
     */
    @Test
    public void testZeroReservedSlotIsLegacyAndNotSuspended() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, SEQ_PART_TXN_COUNT);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            execute("insert into x values ('2024-01-01T00:01:00.000000Z', 2)");

            TableToken tt = engine.verifyTableName("x");
            engine.releaseInactive();

            CairoConfiguration cfg = engine.getConfiguration();
            try (Path path = new Path()) {
                path.of(cfg.getDbRoot())
                        .concat(tt.getDirName())
                        .concat(WalUtils.SEQ_DIR)
                        .concat(WalUtils.TXNLOG_PARTS_DIR)
                        .slash().put(0L);

                // Zero the reserved/CRC slot in the first record (offset = RESERVED_OFFSET in record 0).
                pokeLong(cfg, path.$(), RESERVED_OFFSET, 0L);

            }

            drainWalQueue();

            Assert.assertFalse(
                    "table must NOT be suspended when the reserved slot is 0 (legacy record)",
                    engine.getTableSequencerAPI().isSuspended(tt)
            );
        });
    }

    /**
     * Flip the first byte at {@code byteOffset} in the file at {@code path}.
     * Uses a raw write, so the CRC slot keeps its old value.
     */
    private static void pokeByte(CairoConfiguration cfg, io.questdb.std.str.LPSZ path, long byteOffset) {
        long fd = cfg.getFilesFacade().openRW(path, CairoConfiguration.O_NONE);
        Assert.assertTrue("failed to open part file for write: " + path, fd > -1);
        long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
        try {
            // Read current byte
            Assert.assertEquals(1L, cfg.getFilesFacade().read(fd, buf, 1, byteOffset));
            // Flip it
            Unsafe.putByte(buf, (byte) (Unsafe.getByte(buf) ^ 0xFF));
            // Write back
            Assert.assertEquals(1L, cfg.getFilesFacade().write(fd, buf, 1, byteOffset));
            cfg.getFilesFacade().fsync(fd);
        } finally {
            Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
            cfg.getFilesFacade().close(fd);
        }
    }

    /**
     * Write a {@code long} at {@code byteOffset} in the file at {@code path}.
     */
    private static void pokeLong(CairoConfiguration cfg, io.questdb.std.str.LPSZ path, long byteOffset, long value) {
        long fd = cfg.getFilesFacade().openRW(path, CairoConfiguration.O_NONE);
        Assert.assertTrue("failed to open part file for write: " + path, fd > -1);
        long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putLong(buf, value);
            Assert.assertEquals(Long.BYTES, cfg.getFilesFacade().write(fd, buf, Long.BYTES, byteOffset));
            cfg.getFilesFacade().fsync(fd);
        } finally {
            Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            cfg.getFilesFacade().close(fd);
        }
    }
}
