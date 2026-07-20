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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.UnsupportedWalTxnTypeHandler;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.BinarySequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WalCustomEventTest extends AbstractCairoTest {

    private static final byte CUSTOM_TYPE_LONG = 64;
    private static final byte CUSTOM_TYPE_LONG_BIN = 67;
    private static final byte CUSTOM_TYPE_SECOND = 65;

    @Test
    public void testApplyOssSuspendsTableOnCustomEvent() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable("custom_oss_apply");
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                appendOneRow(walWriter, 1);
                walWriter.commit();
                walWriter.appendCustomEvent(CUSTOM_TYPE_LONG, mem -> mem.putLong(123L));
            }
            drainWalQueue();
            assertTrue(
                    "table should be suspended after default sentinel throws on custom event",
                    engine.getTableSequencerAPI().isSuspended(tableToken)
            );
        });
    }

    @Test
    public void testCustomEventBinRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            final long ts = 0xCAFEBABEDEADBEEFL;
            final byte[] payloadBytes = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

            TableToken tableToken = createTable("custom_bin");
            int walId;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walId = walWriter.getWalId();
                walWriter.appendCustomEvent(CUSTOM_TYPE_LONG_BIN, mem -> {
                    mem.putLong(ts);
                    mem.putBin(new ByteArrayBinarySequence(payloadBytes));
                });
            }

            try (Path path = new Path();
                 WalEventReader reader = new WalEventReader(configuration)) {
                segmentPath(path, tableToken, walId);
                WalEventCursor cursor = reader.of(path, 0);

                assertEquals(0, cursor.getTxn());
                assertEquals(CUSTOM_TYPE_LONG_BIN, cursor.getType());

                WalEventCursor.UnknownInfo info = cursor.getUnknownInfo();
                assertEquals(CUSTOM_TYPE_LONG_BIN, info.getType());
                // payload = long(8) + bin-len-prefix(8) + bin-bytes(N)
                assertEquals(Long.BYTES + Long.BYTES + payloadBytes.length, info.getPayloadSize());

                // ts at +0
                assertEquals(ts, Unsafe.getUnsafe().getLong(info.getPayloadAddr()));
                // bin length prefix at +8 (Long.BYTES)
                assertEquals(payloadBytes.length, Unsafe.getUnsafe().getLong(info.getPayloadAddr() + Long.BYTES));
                // bin bytes at +16
                long binAddr = info.getPayloadAddr() + Long.BYTES + Long.BYTES;
                for (int i = 0; i < payloadBytes.length; i++) {
                    assertEquals(payloadBytes[i], Unsafe.getUnsafe().getByte(binAddr + i));
                }
            }
        });
    }

    @Test
    public void testCustomEventCommitsPriorPendingRows() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable("custom_pending_data");
            int walId;
            long customSeqTxn;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walId = walWriter.getWalId();
                appendOneRow(walWriter, 1);
                customSeqTxn = walWriter.appendCustomEvent(CUSTOM_TYPE_LONG, mem -> mem.putLong(42L));
                // appendCustomEvent must have committed the preceding row, so this is a no-op.
                walWriter.commit();
            }

            assertEquals(
                    "custom event must be the final sequencer transaction",
                    customSeqTxn,
                    engine.getTableSequencerAPI().getTxnTracker(tableToken).getSeqTxn()
            );
            try (Path path = new Path();
                 WalEventReader reader = new WalEventReader(configuration)) {
                segmentPath(path, tableToken, walId);
                WalEventCursor cursor = reader.of(path, 0);
                assertEquals("pending DATA must be sequenced first", WalTxnType.DATA, cursor.getType());
                assertTrue(cursor.hasNext());
                assertEquals("custom event must follow pending DATA", CUSTOM_TYPE_LONG, cursor.getType());
                assertFalse(cursor.hasNext());
            }
        });
    }

    @Test
    public void testCustomEventRejectsInvalidTypeBeforeCommittingPendingRows() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable("custom_invalid_type");
            final boolean[] payloadInvoked = {false};
            final long seqTxnBefore = engine.getTableSequencerAPI().getTxnTracker(tableToken).getSeqTxn();
            int walId;
            long customSeqTxn;

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walId = walWriter.getWalId();
                appendOneRow(walWriter, 1);

                IllegalArgumentException error = assertThrows(
                        IllegalArgumentException.class,
                        () -> walWriter.appendCustomEvent(WalTxnType.SQL, mem -> payloadInvoked[0] = true)
                );
                assertFalse("an invalid type must be rejected before invoking its payload", payloadInvoked[0]);
                assertFalse("an invalid argument must not distress the WAL writer", walWriter.isDistressed());
                assertEquals(
                        "an invalid type must not commit rows appended before the call",
                        seqTxnBefore,
                        engine.getTableSequencerAPI().getTxnTracker(tableToken).getSeqTxn()
                );
                assertTrue(error.getMessage(), error.getMessage().contains("reserved range 64..127"));

                customSeqTxn = walWriter.appendCustomEvent(CUSTOM_TYPE_LONG, mem -> mem.putLong(42L));
            }

            assertEquals(seqTxnBefore + 2, customSeqTxn);
            try (Path path = new Path();
                 WalEventReader reader = new WalEventReader(configuration)) {
                segmentPath(path, tableToken, walId);
                WalEventCursor cursor = reader.of(path, 0);
                assertEquals(WalTxnType.DATA, cursor.getType());
                assertTrue(cursor.hasNext());
                assertEquals(CUSTOM_TYPE_LONG, cursor.getType());
                assertFalse(cursor.hasNext());
            }
        });
    }

    @Test
    public void testCustomEventEmptyPayloadRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable("custom_empty");
            int walId;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walId = walWriter.getWalId();
                // An empty payload writer must still round-trip, surfacing payloadSize 0 rather than erroring.
                walWriter.appendCustomEvent(CUSTOM_TYPE_LONG, mem -> {
                });
            }

            try (Path path = new Path();
                 WalEventReader reader = new WalEventReader(configuration)) {
                segmentPath(path, tableToken, walId);
                WalEventCursor cursor = reader.of(path, 0);

                assertEquals(0, cursor.getTxn());
                assertEquals(CUSTOM_TYPE_LONG, cursor.getType());

                WalEventCursor.UnknownInfo info = cursor.getUnknownInfo();
                assertNotNull(info);
                assertEquals(CUSTOM_TYPE_LONG, info.getType());
                assertEquals(0, info.getPayloadSize());

                assertFalse(cursor.hasNext());
            }
        });
    }

    @Test
    public void testCustomEventInterleavedWithData() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable("custom_interleaved");
            int walId;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walId = walWriter.getWalId();
                appendOneRow(walWriter, 1);
                walWriter.commit();
                walWriter.appendCustomEvent(CUSTOM_TYPE_LONG, mem -> mem.putLong(42L));
                appendOneRow(walWriter, 2);
                walWriter.commit();
                walWriter.appendCustomEvent(CUSTOM_TYPE_SECOND, mem -> mem.putLong(99L));
            }

            try (Path path = new Path();
                 WalEventReader reader = new WalEventReader(configuration)) {
                segmentPath(path, tableToken, walId);
                WalEventCursor cursor = reader.of(path, 0);

                // record 0: DATA
                assertEquals(WalTxnType.DATA, cursor.getType());

                // record 1: custom 64
                assertTrue(cursor.hasNext());
                assertEquals(CUSTOM_TYPE_LONG, cursor.getType());
                assertEquals(42L, Unsafe.getUnsafe().getLong(cursor.getUnknownInfo().getPayloadAddr()));

                // record 2: DATA
                assertTrue(cursor.hasNext());
                assertEquals(WalTxnType.DATA, cursor.getType());

                // record 3: custom 65
                assertTrue(cursor.hasNext());
                assertEquals(CUSTOM_TYPE_SECOND, cursor.getType());
                assertEquals(99L, Unsafe.getUnsafe().getLong(cursor.getUnknownInfo().getPayloadAddr()));

                assertFalse(cursor.hasNext());
            }
        });
    }

    @Test
    public void testCustomEventLongRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            final long payload = 0x0123456789ABCDEFL;

            TableToken tableToken = createTable("custom_long");
            int walId;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walId = walWriter.getWalId();
                walWriter.appendCustomEvent(CUSTOM_TYPE_LONG, mem -> mem.putLong(payload));
            }

            try (Path path = new Path();
                 WalEventReader reader = new WalEventReader(configuration)) {
                segmentPath(path, tableToken, walId);
                WalEventCursor cursor = reader.of(path, 0);

                assertEquals(0, cursor.getTxn());
                assertEquals(CUSTOM_TYPE_LONG, cursor.getType());

                WalEventCursor.UnknownInfo info = cursor.getUnknownInfo();
                assertNotNull(info);
                assertEquals(CUSTOM_TYPE_LONG, info.getType());
                assertEquals(Long.BYTES, info.getPayloadSize());
                assertEquals(payload, Unsafe.getUnsafe().getLong(info.getPayloadAddr()));

                assertFalse(cursor.hasNext());
            }
        });
    }

    @Test
    public void testCustomEventPayloadFailureDistressesAndExpelsWriter() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable("custom_payload_failure");
            long seqTxnBefore = engine.getTableSequencerAPI().getTxnTracker(tableToken).getSeqTxn();
            RuntimeException injected = new RuntimeException("injected custom WAL payload failure");

            int failedWalId;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                failedWalId = walWriter.getWalId();
                RuntimeException thrown = assertThrows(
                        RuntimeException.class,
                        () -> walWriter.appendCustomEvent(CUSTOM_TYPE_LONG, mem -> {
                            // Leave a real partial native record before failing: txn/type and this
                            // payload word are present, but length/sentinel/index are not finalized.
                            mem.putLong(0x0123456789ABCDEFL);
                            throw injected;
                        })
                );
                assertSame("the payload callback's exception must propagate unchanged", injected, thrown);
                assertTrue("a partial custom record must distress the writer", walWriter.isDistressed());
                assertEquals(
                        "a failed payload must not publish a sequencer transaction",
                        seqTxnBefore,
                        engine.getTableSequencerAPI().getTxnTracker(tableToken).getSeqTxn()
                );

                boolean[] secondPayloadInvoked = {false};
                CairoException distressed = assertThrows(
                        CairoException.class,
                        () -> walWriter.appendCustomEvent(CUSTOM_TYPE_SECOND, mem -> secondPayloadInvoked[0] = true)
                );
                assertFalse(
                        "a distressed writer must reject before invoking another payload",
                        secondPayloadInvoked[0]
                );
                assertTrue(distressed.getMessage(), distressed.getMessage().contains("WAL writer is distressed"));
            }

            int replacementWalId;
            long replacementSeqTxn;
            try (WalWriter replacement = engine.getWalWriter(tableToken)) {
                replacementWalId = replacement.getWalId();
                assertFalse("the replacement writer must be healthy", replacement.isDistressed());
                replacementSeqTxn = replacement.appendCustomEvent(
                        CUSTOM_TYPE_SECOND,
                        mem -> mem.putLong(0x0FEDCBA987654321L)
                );
            }
            assertTrue(
                    "the distressed pooled writer must be expelled, not handed out again",
                    replacementWalId != failedWalId
            );
            assertEquals(
                    "only the replacement event may advance the sequencer",
                    seqTxnBefore + 1,
                    replacementSeqTxn
            );

            try (Path path = new Path();
                 WalEventReader reader = new WalEventReader(configuration)) {
                segmentPath(path, tableToken, replacementWalId);
                WalEventCursor cursor = reader.of(path, 0);
                assertEquals(CUSTOM_TYPE_SECOND, cursor.getType());
                assertEquals(0x0FEDCBA987654321L, Unsafe.getUnsafe().getLong(cursor.getUnknownInfo().getPayloadAddr()));
                assertFalse(cursor.hasNext());
            }
        });
    }

    @Test
    public void testDefaultEngineHandlerIsSentinel() {
        assertSame(UnsupportedWalTxnTypeHandler.INSTANCE, engine.getWalTxnTypeHandler());
    }

    @Test
    public void testGetUnknownInfoThrowsForKnownType() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable("unknown_info_for_data");
            int walId;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walId = walWriter.getWalId();
                appendOneRow(walWriter, 1);
                walWriter.commit();
            }

            try (Path path = new Path();
                 WalEventReader reader = new WalEventReader(configuration)) {
                segmentPath(path, tableToken, walId);
                WalEventCursor cursor = reader.of(path, 0);

                assertEquals(WalTxnType.DATA, cursor.getType());
                CairoException ex = assertThrows(CairoException.class, cursor::getUnknownInfo);
                assertTrue(ex.getMessage(), ex.getMessage().contains("not unknown"));
            }
        });
    }

    @Test
    public void testIsDownstreamTypeBoundaries() {
        assertFalse(WalTxnType.isDownstreamType((byte) -1));
        assertFalse(WalTxnType.isDownstreamType((byte) 0));
        assertFalse(WalTxnType.isDownstreamType((byte) 5));
        assertFalse(WalTxnType.isDownstreamType((byte) 63));
        assertTrue(WalTxnType.isDownstreamType((byte) 64));
        assertTrue(WalTxnType.isDownstreamType((byte) 100));
        assertTrue(WalTxnType.isDownstreamType((byte) 127));
    }

    @Test
    public void testReadRejectsCustomEventFrameShorterThanHeader() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable("corrupt_custom_event_length");
            int walId;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walId = walWriter.getWalId();
                walWriter.appendCustomEvent(CUSTOM_TYPE_LONG, mem -> mem.putLong(1L));
            }

            // A custom frame must contain [length:int][txn:long][type:byte]. Setting its
            // length to 12 makes the declared end precede the already-consumed type byte.
            corruptFirstRecordLength(tableToken, walId, Integer.BYTES + Long.BYTES);
            try (Path path = new Path();
                 WalEventReader reader = new WalEventReader(configuration)) {
                segmentPath(path, tableToken, walId);
                CairoException ex = assertThrows(CairoException.class, () -> reader.of(path, 0));
                assertEquals(CairoException.METADATA_VALIDATION, ex.getErrno());
                assertEquals(
                        "[-100] corrupt WAL event frame, payload size is negative [offset=21, nextOffset=20]",
                        ex.getMessage()
                );
            }
        });
    }

    @Test
    public void testReadRejectsUnknownTypeBelowDownstreamRange() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable("corrupt_event_type");
            int walId;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walId = walWriter.getWalId();
                walWriter.appendCustomEvent(CUSTOM_TYPE_LONG, mem -> mem.putLong(1L));
            }

            // Bytes 6..63 are neither a known OSS type (0..5) nor a reserved downstream
            // type (64..127): a corrupt type byte in this gap must be a hard read error,
            // not silently surfaced as an unknown/custom event.
            for (byte corruptType : new byte[]{6, 32, 63}) {
                corruptFirstRecordType(tableToken, walId, corruptType);
                try (Path path = new Path();
                     WalEventReader reader = new WalEventReader(configuration)) {
                    segmentPath(path, tableToken, walId);
                    CairoException ex = assertThrows(
                            "type " + corruptType + " must be rejected",
                            CairoException.class,
                            () -> reader.of(path, 0)
                    );
                    assertEquals(CairoException.METADATA_VALIDATION, ex.getErrno());
                    assertTrue(ex.getMessage(), ex.getMessage().contains("Unsupported WAL event type"));
                }
            }
        });
    }

    @Test
    public void testUnsupportedHandlerThrowsMetadataValidation() {
        try {
            UnsupportedWalTxnTypeHandler.INSTANCE.applyUnknownWalTxn((byte) 64, null, null, 0);
            fail("expected CairoException");
        } catch (CairoException e) {
            assertEquals(CairoException.METADATA_VALIDATION, e.getErrno());
            assertTrue(e.getMessage(), e.getMessage().contains("Unsupported WAL event type"));
        }
    }

    private static void appendOneRow(WalWriter walWriter, int v) {
        TableWriter.Row row = walWriter.newRow(0);
        row.putByte(0, (byte) v);
        row.append();
    }

    private static void corruptFirstRecordLength(TableToken tableToken, int walId, int newLength) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            segmentPath(path, tableToken, walId).concat(WalUtils.EVENT_FILE_NAME);
            final long fd = TableUtils.openRW(ff, path.$(), LOG, configuration.getWriterFileOpenOpts());
            try {
                final long fileSize = ff.length(fd);
                final long mem = TableUtils.mapRW(ff, fd, fileSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putInt(mem + WalUtils.WALE_HEADER_SIZE, newLength);
                } finally {
                    ff.munmap(mem, fileSize, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                ff.close(fd);
            }
        }
    }

    private static void corruptFirstRecordType(TableToken tableToken, int walId, byte newType) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            segmentPath(path, tableToken, walId).concat(WalUtils.EVENT_FILE_NAME);
            final long fd = TableUtils.openRW(ff, path.$(), LOG, configuration.getWriterFileOpenOpts());
            try {
                final long fileSize = ff.length(fd);
                final long mem = TableUtils.mapRW(ff, fd, fileSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    // First record's type byte sits past the header and the per-record [len:int][txn:long] prefix.
                    Unsafe.getUnsafe().putByte(mem + WalUtils.WALE_HEADER_SIZE + Integer.BYTES + Long.BYTES, newType);
                } finally {
                    ff.munmap(mem, fileSize, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                ff.close(fd);
            }
        }
    }

    private static TableToken createTable(String name) {
        return createTable(new TableModel(configuration, name, PartitionBy.HOUR)
                .col("a", ColumnType.BYTE)
                .timestamp("ts")
                .wal());
    }

    private static Path segmentPath(Path path, TableToken tableToken, int walId) {
        return path.of(configuration.getDbRoot())
                .concat(tableToken)
                .concat("wal").put(walId)
                .slash().put(0);
    }

    private static final class ByteArrayBinarySequence implements BinarySequence {
        private final byte[] bytes;

        ByteArrayBinarySequence(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public byte byteAt(long index) {
            return bytes[(int) index];
        }

        @Override
        public long length() {
            return bytes.length;
        }
    }
}
