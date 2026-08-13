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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TxnLogCrcSidecar;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * V1 keeps its per-record CRC in the additive {@code _txnlog.c} sidecar. These pin the two verdicts
 * that matter: a record whose bytes no longer match its CRC is torn and fatal, and a table written
 * before the sidecar existed still replays unverified.
 */
public class TxnLogV1CrcVerifyTest extends AbstractCairoTest {

    @Override
    public void setUp() {
        super.setUp();
        // These are V1 tests. V1 is the default today, but say so explicitly rather than inheriting it:
        // under V2 the CRC lives in-record, there is no _txnlog.c to corrupt or delete, and every
        // assertion here would pass for the wrong reason if the default ever moves.
        node1.setProperty(io.questdb.PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 0);
    }

    @Test
    public void testCorruptedV1RecordIsTorn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table v1_rot (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into v1_rot values ('2024-01-01T00:00:00.000000Z', 1)");
            execute("insert into v1_rot values ('2024-01-01T00:01:00.000000Z', 2)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("v1_rot");
            // Flip a byte INSIDE the record body so the stored CRC no longer describes it. The
            // sidecar is left alone: this is bit-rot in _txnlog, which is exactly what the CRC exists
            // to catch.
            flipByteInRecord(token, lastTxn(token));

            try {
                replayFromScratch(token);
                Assert.fail("expected a torn V1 txnlog record to be rejected");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "torn sequencer txnlog record");
            }
        });
    }

    @Test
    public void testStampedButWrongCrcIsStillTorn() throws Exception {
        // The other side of the seqlock line. A missing or unstamped entry is "not applicable" and the
        // record is read unverified -- that is what stops a crash-truncated sidecar from condemning
        // intact records. But an entry whose STAMP names this txn is proof the pair landed whole, so
        // its CRC is authoritative and a disagreement is real corruption. Without this test, the crash
        // fix would have quietly turned every bad CRC into "fine".
        assertMemoryLeak(() -> {
            execute("create table v1_gap (ts timestamp, v long) timestamp(ts) partition by day wal");
            for (int i = 0; i < 6; i++) {
                execute("insert into v1_gap values ('2024-01-0" + (i + 1) + "T00:00:00.000000Z', " + i + ")");
            }
            drainWalQueue();

            final TableToken token = engine.verifyTableName("v1_gap");
            // Zero the CRC for txn 2 while later entries stay populated.
            zeroSidecarEntry(token, 2L);

            try {
                replayFromScratch(token);
                Assert.fail("a stamped entry whose CRC disagrees must still be reported as torn");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "sequencer txnlog record");
            }
        });
    }

    @Test
    public void testPreSidecarV1RecordsStillReplay() throws Exception {
        // The false-positive control: records written before the sidecar existed carry no CRC and MUST
        // still replay. Without the capability watermark this is the case that would throw on every
        // healthy pre-upgrade table.
        assertMemoryLeak(() -> {
            execute("create table v1_legacy (ts timestamp, v long) timestamp(ts) partition by day wal");
            for (int i = 0; i < 5; i++) {
                execute("insert into v1_legacy values ('2024-01-0" + (i + 1) + "T00:00:00.000000Z', " + i + ")");
            }
            drainWalQueue();

            final TableToken token = engine.verifyTableName("v1_legacy");
            assertIsV1(token);
            deleteSidecar(token);

            final int walked = replayFromScratch(token); // must not throw
            Assert.assertTrue(
                    "the replay must actually advance over records, or 'it did not throw' proves nothing",
                    walked > 0
            );
            assertQuery("select count() from v1_legacy").noRandomAccess().expectSize().returns("count\n5\n");
        });
    }

    private long lastTxn(TableToken token) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot())
                    .concat(token)
                    .concat(WalUtils.SEQ_DIR)
                    .concat(WalUtils.TXNLOG_FILE_NAME);
            final long fd = ff.openRO(path.$());
            Assert.assertTrue(fd > -1);
            try {
                return ff.readNonNegativeLong(fd, TableTransactionLogFile.MAX_TXN_OFFSET_64);
            } finally {
                ff.close(fd);
            }
        }
    }

    private void assertIsV1(TableToken token) {
        // These tests are ONLY meaningful against V1: V2 carries its CRC in-record and has no
        // _txnlog.c at all, so a V2 table would make every assertion here pass for the wrong reason.
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot())
                    .concat(token)
                    .concat(WalUtils.SEQ_DIR)
                    .concat(WalUtils.TXNLOG_FILE_NAME);
            final long fd = ff.openRO(path.$());
            Assert.assertTrue(fd > -1);
            try {
                Assert.assertEquals("this test requires the V1 txnlog",
                        WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1, ff.readNonNegativeInt(fd, 0));
            } finally {
                ff.close(fd);
            }
        }
    }

    private void zeroSidecarEntry(TableToken token, long txn) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot())
                    .concat(token)
                    .concat(WalUtils.SEQ_DIR)
                    .concat(WalUtils.TXNLOG_CRC_FILE_NAME);
            final long fd = ff.openRW(path.$(), io.questdb.cairo.CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            try {
                final long buf = io.questdb.std.Unsafe.malloc(Long.BYTES, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                try {
                    // Entry is [crc:8][stamp:8]; watermark is 1, so entry index == txn - 1. Zero the
                    // CRC only, LEAVING the stamp, so the entry still claims to describe this txn.
                    final long offset = TxnLogCrcSidecar.BODY_OFFSET
                            + (txn - 1) * TxnLogCrcSidecar.ENTRY_SIZE;
                    io.questdb.std.Unsafe.getUnsafe().putLong(buf, 0L);
                    Assert.assertEquals(Long.BYTES, ff.write(fd, buf, Long.BYTES, offset));
                } finally {
                    io.questdb.std.Unsafe.free(buf, Long.BYTES, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                ff.close(fd);
            }
        }
    }

    private void deleteSidecar(TableToken token) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot())
                    .concat(token)
                    .concat(WalUtils.SEQ_DIR)
                    .concat(WalUtils.TXNLOG_CRC_FILE_NAME);
            Assert.assertTrue("sidecar should exist before deletion", ff.exists(path.$()));
            // Check the removal AND that it stayed removed. Without this the test passes vacuously:
            // the sidecar survives, the reader verifies against real CRCs, and "a legacy table still
            // replays" is never actually exercised.
            ff.remove(path.$());
            Assert.assertFalse("the sidecar must be gone for this to be the legacy case",
                    ff.exists(path.$()));
        }
    }

    private void flipByteInRecord(TableToken token, long txn) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot())
                    .concat(token)
                    .concat(WalUtils.SEQ_DIR)
                    .concat(WalUtils.TXNLOG_FILE_NAME);
            // Flip the LAST byte of the record (the tail of the commit timestamp), never byte 0 --
            // byte 0 is structureVersion, and corrupting it makes the sequencer fail to open before
            // the CRC check ever runs, which would test the wrong thing.
            final long offset = TableTransactionLogFile.HEADER_SIZE
                    + (txn - 1) * io.questdb.cairo.wal.seq.TableTransactionLogV1.RECORD_SIZE
                    + io.questdb.cairo.wal.seq.TableTransactionLogV1.RECORD_SIZE - 1;
            final long fd = ff.openRW(path.$(), io.questdb.cairo.CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            try {
                final long buf = io.questdb.std.Unsafe.malloc(1, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                try {
                    Assert.assertEquals(1, ff.read(fd, buf, 1, offset));
                    final byte b = io.questdb.std.Unsafe.getUnsafe().getByte(buf);
                    io.questdb.std.Unsafe.getUnsafe().putByte(buf, (byte) (b ^ 0x01));
                    Assert.assertEquals(1, ff.write(fd, buf, 1, offset));
                } finally {
                    io.questdb.std.Unsafe.free(buf, 1, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                ff.close(fd);
            }
        }
    }

    /**
     * Walks the sequencer log from txn 0 with a fresh cursor, so the verification runs against what is
     * actually on disk rather than anything cached.
     */
    /**
     * Walks the sequencer log from txn 1 with a fresh cursor and returns how many records it advanced
     * over. The COUNT is the point: verification only runs on advance, so a walk that never advances
     * proves nothing, and a caller asserting "this did not throw" over zero records is vacuous.
     */
    private int replayFromScratch(TableToken token) {
        engine.releaseInactive();
        int walked = 0;
        try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(token, 1)) {
            while (cursor.hasNext()) {
                walked++;
            }
        }
        return walked;
    }
}
