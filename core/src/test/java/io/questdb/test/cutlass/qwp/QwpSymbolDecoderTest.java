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

package io.questdb.test.cutlass.qwp;

import io.questdb.client.cutlass.qwp.client.QwpBufferWriter;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpNullBitmap;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpSymbolColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.cutlass.qwp.server.QwpStreamingDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class QwpSymbolDecoderTest {

    @Test
    public void testDecodeEmptySymbolColumn() throws Exception {
        // no null bitmap + empty dictionary (varint 0), 0 rows.
        int allocSize = 1 + QwpVarint.encodedLength(0); // flag byte + dictionary size
        long address = Unsafe.malloc(allocSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            QwpVarint.encode(address + 1, 0); // empty dictionary
            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            int consumed = cursor.of(address, allocSize, 0);
            Assert.assertEquals(allocSize, consumed);
        } finally {
            Unsafe.free(address, allocSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeMultipleSymbols() throws Exception {
        String[] values = {"apple", "banana", "cherry", "date"};
        assertRoundTrip(values, null);
    }

    @Test
    public void testDecodeSingleSymbol() throws Exception {
        String[] values = {"symbol_a"};
        assertRoundTrip(values, null);
    }

    @Test
    public void testDeltaSymbolDictContiguousAppendAccepted() throws Exception {
        // The exact boundary the gap check sits on: deltaStartId == size() adds
        // no hole and must be accepted. An off-by-one here would reject every
        // incremental delta a healthy client sends, i.e. the whole feature.
        assertMemoryLeak(() -> {
            QwpMessageCursor cursor = new QwpMessageCursor();
            ObjList<String> dict = new ObjList<>();
            Assert.assertFalse(decodeDeltaDict(cursor, dict, 0, "sym_a", "sym_b"));
            Assert.assertFalse(decodeDeltaDict(cursor, dict, 2, "sym_c"));
            Assert.assertEquals(3, dict.size());
            Assert.assertEquals("sym_c", dict.getQuick(2));
        });
    }

    @Test
    public void testDeltaSymbolDictExcessiveStartId() throws Exception {
        // A malicious client sends deltaStartId = 100_001, deltaCount = 0.
        // The while loop in parseDeltaSymbolDict() grows connectionSymbolDict
        // to 100K entries without question. With larger values (e.g. 2 billion),
        // this exhausts heap memory (DoS). The parser should reject oversized
        // delta dictionaries with QwpParseException.
        assertMemoryLeak(() -> {
            int deltaStartId = 1_000_001;
            int deltaCount = 0;

            int payloadSize = QwpVarint.encodedLength(deltaStartId) + QwpVarint.encodedLength(deltaCount);
            int totalSize = HEADER_SIZE + payloadSize;
            long address = Unsafe.malloc(totalSize, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putInt(address + HEADER_OFFSET_MAGIC, MAGIC_MESSAGE);
                Unsafe.putByte(address + HEADER_OFFSET_VERSION, VERSION);
                Unsafe.putByte(address + HEADER_OFFSET_FLAGS, FLAG_DELTA_SYMBOL_DICT);
                Unsafe.putShort(address + HEADER_OFFSET_TABLE_COUNT, (short) 0);
                Unsafe.putInt(address + HEADER_OFFSET_PAYLOAD_LENGTH, payloadSize);

                long pos = address + HEADER_SIZE;
                pos = QwpVarint.encode(pos, deltaStartId);
                QwpVarint.encode(pos, deltaCount);

                QwpMessageCursor cursor = new QwpMessageCursor();
                ObjList<String> connectionDict = new ObjList<>();
                try {
                    cursor.of(address, totalSize, connectionDict);
                    Assert.fail("Expected QwpParseException for excessive delta symbol dictionary size");
                } catch (QwpParseException e) {
                    Assert.assertTrue(e.getMessage().contains("delta symbol dictionary"));
                }
            } finally {
                Unsafe.free(address, totalSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDeltaSymbolDictGapRejected() throws Exception {
        // A delta starting above the server's coverage leaves ids
        // [size, deltaStartId) undefined. The parser used to null-pad them,
        // which inflated size() past the referencing index and so slipped the
        // frame through QwpSymbolColumnCursor's idx >= dictLimit guard -- the
        // row then read back null and landed a NULL symbol instead of failing.
        // Reject it at parse time, while it is still attributable.
        assertMemoryLeak(() -> {
            QwpMessageCursor cursor = new QwpMessageCursor();
            ObjList<String> dict = new ObjList<>();
            Assert.assertFalse(decodeDeltaDict(cursor, dict, 0, "sym_a", "sym_b"));

            try {
                decodeDeltaDict(cursor, dict, 3, "sym_d"); // id 2 was never defined
                Assert.fail("Expected QwpParseException for a gapped delta symbol dictionary");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.DELTA_DICT_GAP, e.getErrorCode());
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("delta symbol dictionary gap"));
            }
            // The rejected frame must not have grown the dictionary on its way out.
            Assert.assertEquals(2, dict.size());
        });
    }

    @Test
    public void testDeltaSymbolDictGapUsesItsOwnErrorCode() throws Exception {
        // The gap verdict depends on connectionSymbolDict.size() -- server state, not
        // frame bytes -- so it must not share an error code with the row-index bounds
        // check in QwpSymbolColumnCursor, whose terminal classification rests on
        // "malformed bytes never parse". The identical frame succeeds after a catch-up.
        assertMemoryLeak(() -> {
            QwpMessageCursor cursor = new QwpMessageCursor();
            ObjList<String> dict = new ObjList<>();
            Assert.assertFalse(decodeDeltaDict(cursor, dict, 0, "sym_a", "sym_b"));
            try {
                decodeDeltaDict(cursor, dict, 3, "sym_d");
                Assert.fail("Expected a gap rejection");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.DELTA_DICT_GAP, e.getErrorCode());
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("delta symbol dictionary gap"));
            }
            Assert.assertEquals(2, dict.size());
        });
    }

    @Test
    public void testDeltaSymbolDictCapIsTwoMillion() throws Exception {
        // Pins the ingress ceiling at 2,000,000 with literals rather than the constant:
        // MAX_SYMBOL_DICTIONARY_SIZE is a WIRE constant, so a silent move desyncs every
        // already-shipped sender, which enforces the same number at registration time.
        // Both frames declare their count in the header and carry one entry, which keeps
        // this cheap -- the cap gate runs BEFORE extendPos, so neither ever allocates a
        // cap-sized backing array.
        assertMemoryLeak(() -> {
            QwpMessageCursor cursor = new QwpMessageCursor();
            ObjList<String> dict = new ObjList<>();

            // Exactly the cap clears the gate and falls through to the payload-bytes
            // check. Asserting the LATER message is what proves the gate admitted it:
            // a cap that had refused this frame would report "exceeds limit" instead.
            try {
                decodeDeltaDictDeclaring(cursor, dict, 0, 2_000_000, "x");
                Assert.fail("Expected rejection on the payload-bytes check, not the cap");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
                Assert.assertTrue(e.getMessage(),
                        e.getMessage().contains("delta symbol dictionary declares 2000000 entries"));
            }

            // One past the cap: the gate itself refuses it.
            try {
                decodeDeltaDictDeclaring(cursor, dict, 0, 2_000_001, "x");
                Assert.fail("Expected rejection on the dictionary cap");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
                Assert.assertTrue(e.getMessage(),
                        e.getMessage().contains("delta symbol dictionary size exceeds limit: 2000001"));
            }

            // The gate sums the two fields, so a start id can straddle the cap on its
            // own -- deltaCount 1 is refused when deltaStartId already sits at the cap.
            try {
                decodeDeltaDictDeclaring(cursor, dict, 2_000_000, 1, "x");
                Assert.fail("Expected rejection on the dictionary cap");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
                Assert.assertTrue(e.getMessage(),
                        e.getMessage().contains("delta symbol dictionary size exceeds limit: 2000001"));
            }

            Assert.assertEquals("a rejected frame must not grow the connection dictionary", 0, dict.size());
        });
    }

    @Test
    public void testDeltaSymbolDictDecodesIdsAboveTheOldCap() throws Exception {
        // The raised cap is only real if ids the old 1,000,000 ceiling refused now
        // decode. Pre-extending the connection dictionary to 1,500,000 puts the append
        // above that ceiling for the price of one reference array instead of 1.5M
        // decoded entries; deltaStartId == size() keeps it a pure append, so the
        // gap check passes and no slot is overwritten.
        assertMemoryLeak(() -> {
            QwpMessageCursor cursor = new QwpMessageCursor();
            ObjList<String> dict = new ObjList<>();
            dict.extendPos(1_500_000);

            decodeDeltaDict(cursor, dict, 1_500_000, "above", "the_old_cap");

            Assert.assertEquals(1_500_002, dict.size());
            Assert.assertEquals("above", dict.getQuick(1_500_000));
            Assert.assertEquals("the_old_cap", dict.getQuick(1_500_001));
        });
    }

    @Test
    public void testDeltaSymbolDictClaimedCountExceedsPayloadRejected() throws Exception {
        // A frame can be complete -- payloadEnd is the true end of the message -- and
        // still claim far more entries than it carries: every entry costs at least its
        // own length varint, so deltaCount must not exceed the payload bytes left after
        // the header. Before this check, this exact input reached extendPos and grew the
        // connection dictionary to a MAX_SYMBOL_DICTIONARY_SIZE-sized backing array before
        // failing on the first entry with "truncated delta symbol entry".
        assertMemoryLeak(() -> {
            QwpMessageCursor cursor = new QwpMessageCursor();
            ObjList<String> dict = new ObjList<>();

            try {
                decodeDeltaDictDeclaring(cursor, dict, 0, 1_000_000);
                Assert.fail("Expected rejection for deltaCount exceeding the remaining payload");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
                Assert.assertTrue(e.getMessage(),
                        e.getMessage().contains("delta symbol dictionary declares 1000000 entries"));
            }
            Assert.assertEquals("a rejected frame must not grow the connection dictionary", 0, dict.size());
        });
    }

    @Test
    public void testParseErrorRoutingDiscriminatesTheGap() {
        // A malformed row index must stay terminal; only the gap becomes retriable.
        Assert.assertEquals(QwpIngressProcessorState.Status.DICTIONARY_GAP,
                QwpIngressProcessorState.statusForParseError(
                        QwpParseException.ErrorCode.DELTA_DICT_GAP));
        Assert.assertEquals(QwpIngressProcessorState.Status.PARSE_ERROR,
                QwpIngressProcessorState.statusForParseError(
                        QwpParseException.ErrorCode.INVALID_DICTIONARY_INDEX));
        Assert.assertEquals(QwpIngressProcessorState.Status.SCHEMA_MISMATCH,
                QwpIngressProcessorState.statusForParseError(
                        QwpParseException.ErrorCode.SCHEMA_MISMATCH));
        Assert.assertEquals(0x0D, STATUS_DICTIONARY_GAP);
    }

    @Test
    public void testClientSymbolDictionaryCapDoesNotExceedTheServer() {
        int clientCap = io.questdb.client.cutlass.qwp.protocol.QwpConstants.MAX_SYMBOL_DICTIONARY_SIZE;
        Assert.assertTrue(
                "client symbol-dictionary cap (" + clientCap + ") must not exceed the server's ("
                        + MAX_SYMBOL_DICTIONARY_SIZE + "): a client that admits ids this decoder"
                        + " rejects strands its store-and-forward backlog",
                clientCap <= MAX_SYMBOL_DICTIONARY_SIZE);
    }

    @Test
    public void testDeltaSymbolDictIntegerOverflow() throws Exception {
        assertMemoryLeak(() -> {
            long deltaStartId = Integer.MAX_VALUE;
            int deltaCount = 1;

            byte[] symbolBytes = "x".getBytes(StandardCharsets.UTF_8);
            int payloadSize = QwpVarint.encodedLength(deltaStartId)
                    + QwpVarint.encodedLength(deltaCount)
                    + QwpVarint.encodedLength(symbolBytes.length)
                    + symbolBytes.length;
            int totalSize = HEADER_SIZE + payloadSize;
            long address = Unsafe.malloc(totalSize, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putInt(address + HEADER_OFFSET_MAGIC, MAGIC_MESSAGE);
                Unsafe.putByte(address + HEADER_OFFSET_VERSION, VERSION);
                Unsafe.putByte(address + HEADER_OFFSET_FLAGS, FLAG_DELTA_SYMBOL_DICT);
                Unsafe.putShort(address + HEADER_OFFSET_TABLE_COUNT, (short) 0);
                Unsafe.putInt(address + HEADER_OFFSET_PAYLOAD_LENGTH, payloadSize);

                long pos = address + HEADER_SIZE;
                pos = QwpVarint.encode(pos, deltaStartId);
                pos = QwpVarint.encode(pos, deltaCount);
                pos = QwpVarint.encode(pos, symbolBytes.length);
                for (byte b : symbolBytes) {
                    Unsafe.putByte(pos++, b);
                }

                QwpMessageCursor cursor = new QwpMessageCursor();
                ObjList<String> connectionDict = new ObjList<>();
                try {
                    cursor.of(address, totalSize, connectionDict);
                    Assert.fail("Expected QwpParseException for integer overflow in delta symbol dictionary");
                } catch (QwpParseException e) {
                    Assert.assertTrue(e.getMessage().contains("delta symbol dictionary"));
                }
            } finally {
                Unsafe.free(address, totalSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDeltaSymbolDictLeavesNoNullEntries() throws Exception {
        // The invariant the gap check buys: because deltaStartId <= size(),
        // every slot the pre-sizing appends falls inside the range the entry
        // loop then overwrites, so a parsed connection dictionary can never
        // hold a null. A surviving null is indistinguishable from a defined
        // symbol to QwpSymbolColumnCursor's bounds check and reads back as a
        // NULL value. Covers a pure append and an overlap-plus-extend.
        assertMemoryLeak(() -> {
            QwpMessageCursor cursor = new QwpMessageCursor();
            ObjList<String> dict = new ObjList<>();
            decodeDeltaDict(cursor, dict, 0, "sym_a", "sym_b");
            decodeDeltaDict(cursor, dict, 2, "sym_c");
            decodeDeltaDict(cursor, dict, 1, "sym_b2", "sym_c2", "sym_d");
            Assert.assertEquals(4, dict.size());
            for (int i = 0; i < dict.size(); i++) {
                Assert.assertNotNull("dictionary slot " + i + " must be defined", dict.getQuick(i));
            }
        });
    }

    @Test
    public void testTruncatedDeltaLeavesNoNullResidueOnTheConnection() throws Exception {
        // The invariant the method claims -- "no null can survive" -- is only true if a
        // frame that fails PARTWAY leaves nothing behind. The pre-sizing runs before the
        // entry loop, and reject() does NOT close the connection or clear the dictionary
        // (state.clear() preserves connectionSymbolDict for the next message), so a null
        // pre-filled for an entry the loop never reached would persist onto the connection,
        // inflate size(), and slip a later row past QwpSymbolColumnCursor's idx >= dictLimit
        // guard to read back as a silent NULL symbol. A frame declaring more entries than it
        // carries is the reachable trigger (a torn store-and-forward dictionary, a buggy or
        // third-party client). The rejected frame must leave the dictionary exactly as it
        // was: same size, no nulls.
        assertMemoryLeak(() -> {
            QwpMessageCursor cursor = new QwpMessageCursor();
            ObjList<String> dict = new ObjList<>();
            decodeDeltaDict(cursor, dict, 0, "sym_a", "sym_b");

            // deltaStartId == size() (a legal contiguous append), but declares 5 entries
            // while carrying only 1 -- the entry loop throws on the second, after
            // extendPos has already grown the dictionary without filling the new slots.
            try {
                decodeDeltaDictDeclaring(cursor, dict, 2, 5, "sym_c");
                Assert.fail("Expected a truncated-entry parse error");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
            }

            // The rejected frame must have left the connection dictionary untouched.
            Assert.assertEquals("a rejected frame must not grow the connection dictionary",
                    2, dict.size());
            for (int i = 0; i < dict.size(); i++) {
                Assert.assertNotNull("dictionary slot " + i + " must be defined", dict.getQuick(i));
            }

            // And the connection must still accept the correct frame afterwards, landing
            // the ids where they belong (no null residue shifted the dense map).
            Assert.assertFalse(decodeDeltaDict(cursor, dict, 2, "sym_c", "sym_d"));
            Assert.assertEquals(4, dict.size());
            Assert.assertEquals("sym_c", dict.getQuick(2));
            Assert.assertEquals("sym_d", dict.getQuick(3));
        });
    }

    @Test
    public void testTruncatedOverlapDeltaRestoresOverwrittenEntries() throws Exception {
        // The overlap twin of testTruncatedDeltaLeavesNoNullResidueOnTheConnection.
        // setPos restores the SIZE only; for deltaStartId < size() the entry loop has
        // already overwritten pre-existing slots, and reject() neither closes the
        // connection nor clears the dictionary. A later bare row referencing id 0 then
        // resolves against the failed frame's symbol -- silent misattribution, with
        // symbolDictRedefined (success-path only) discarded alongside the exception.
        assertMemoryLeak(() -> {
            QwpMessageCursor cursor = new QwpMessageCursor();
            ObjList<String> dict = new ObjList<>();
            Assert.assertFalse(decodeDeltaDict(cursor, dict, 0, "sym_a", "sym_b", "sym_c"));

            // deltaStartId == 0 over a 3-entry dictionary: declares 3, carries 2.
            // requiredSize == sizeBefore, so the pre-sizing is a no-op and setPos cannot
            // undo anything. Slots 0 and 1 are rewritten, then the loop throws.
            try {
                decodeDeltaDictDeclaring(cursor, dict, 0, 3, "sym_x", "sym_y");
                Assert.fail("Expected a truncated-entry parse error");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
            }

            Assert.assertEquals(3, dict.size());
            Assert.assertEquals("sym_a", dict.getQuick(0));
            Assert.assertEquals("sym_b", dict.getQuick(1));
            Assert.assertEquals("sym_c", dict.getQuick(2));
        });
    }

    @Test
    public void testTruncatedMidDictionaryOverlapRestoresAtTheRightOffset() throws Exception {
        // Pins the i - deltaStartId scratch offset in the rollback: every other
        // rollback test runs with deltaStartId == 0, where the offset is a no-op.
        // Here the failed frame starts at id 1, so restoring scratch[j] into slot
        // 1 + j is load-bearing -- a mutant dropping the offset restores the wrong
        // entries and (with assertions off) reads past the scratch content.
        assertMemoryLeak(() -> {
            QwpMessageCursor cursor = new QwpMessageCursor();
            ObjList<String> dict = new ObjList<>();
            Assert.assertFalse(decodeDeltaDict(cursor, dict, 0, "sym_a", "sym_b", "sym_c", "sym_d"));

            // Overlap [1, 4): declares 3 entries, carries 2 -- the loop overwrites
            // ids 1 and 2, then throws on the missing third.
            try {
                decodeDeltaDictDeclaring(cursor, dict, 1, 3, "sym_x", "sym_y");
                Assert.fail("Expected a truncated-entry parse error");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
            }

            Assert.assertEquals(4, dict.size());
            Assert.assertEquals("sym_a", dict.getQuick(0));
            Assert.assertEquals("sym_b", dict.getQuick(1));
            Assert.assertEquals("sym_c", dict.getQuick(2));
            Assert.assertEquals("sym_d", dict.getQuick(3));
        });
    }

    @Test
    public void testSmallOverlapRollbackAfterLargeOverlapCommit() throws Exception {
        // Pins the prefix-release discipline of dictRollbackScratch: a large
        // committed overlap grows the scratch's capacity, and a later small
        // failing overlap must restore correctly from scratch[0..pos) alone,
        // independent of the stale capacity above pos.
        assertMemoryLeak(() -> {
            QwpMessageCursor cursor = new QwpMessageCursor();
            ObjList<String> dict = new ObjList<>();
            // Register 8 ids, then re-send all 8 (full-dict overlap): the
            // committed frame drives scratch pos, and capacity, to 8.
            Assert.assertFalse(decodeDeltaDict(cursor, dict, 0,
                    "s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7"));
            Assert.assertFalse(decodeDeltaDict(cursor, dict, 0,
                    "s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7"));
            Assert.assertEquals(8, dict.size());

            // A small overlapping frame fails mid-loop: declares 3 at id 2,
            // carries 2. The loop overwrites ids 2 and 3, then throws; the
            // rollback must restore both from THIS frame's scratch prefix.
            try {
                decodeDeltaDictDeclaring(cursor, dict, 2, 3, "x2", "x3");
                Assert.fail("Expected a truncated-entry parse error");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
            }
            Assert.assertEquals(8, dict.size());
            for (int i = 0; i < 8; i++) {
                Assert.assertEquals("s" + i, dict.getQuick(i));
            }

            // And the connection still takes a clean delta afterwards.
            Assert.assertFalse(decodeDeltaDict(cursor, dict, 8, "s8"));
            Assert.assertEquals(9, dict.size());
            Assert.assertEquals("s8", dict.getQuick(8));
        });
    }

    @Test
    public void testRolledBackFrameDoesNotLeakStaleRedefinition() throws Exception {
        // extendPos grows pos WITHOUT null-filling, and the rollback's setPos does not
        // clear, so slots above the restored size can still hold a failed frame's
        // strings. A later delta extending into that range must not read them as a
        // "previous value": that raises symbolDictRedefined and forces a spurious
        // symbolCache.clear() on a healthy frame. This is the regression guard for
        // that non-null-filling extendPos behaviour.
        assertMemoryLeak(() -> {
            QwpMessageCursor cursor = new QwpMessageCursor();
            ObjList<String> dict = new ObjList<>();
            Assert.assertFalse(decodeDeltaDict(cursor, dict, 0, "sym_a"));

            try {
                decodeDeltaDictDeclaring(cursor, dict, 1, 3, "stale_x");
                Assert.fail("Expected a truncated-entry parse error");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
            }
            Assert.assertEquals(1, dict.size());

            Assert.assertFalse(decodeDeltaDict(cursor, dict, 1, "sym_b", "sym_c"));
            Assert.assertEquals(3, dict.size());
            Assert.assertEquals("sym_b", dict.getQuick(1));
            Assert.assertEquals("sym_c", dict.getQuick(2));
        });
    }

    @Test
    public void testFailedFirstDeltaLeavesNoResidueOnThePooledConnection() throws Exception {
        // A FIRST frame that fails mid-entry-loop rolls back to size 0 -- and
        // ObjList.clear() is a no-op at pos == 0 (it only Arrays.fill's when
        // pos > 0), so the connection-teardown clear cannot release anything the
        // failed frame wrote above pos. The rollback itself must null the slots
        // it grew into, or the parsed Strings stay strongly reachable on the
        // pooled context until the next successful overlapping parse --
        // indefinitely, for a client that only ever sends failing frames.
        assertMemoryLeak(() -> {
            QwpMessageCursor cursor = new QwpMessageCursor();
            ObjList<String> dict = new ObjList<>();

            // First frame on the connection: declares 3, carries 2 -- the loop
            // writes ids 0 and 1, then throws. sizeBefore == 0, so setPos(0) is
            // the entire rollback unless the tail is nulled explicitly.
            try {
                decodeDeltaDictDeclaring(cursor, dict, 0, 3, "sym_a", "sym_b");
                Assert.fail("Expected a truncated-entry parse error");
            } catch (QwpParseException e) {
                Assert.assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
            }
            Assert.assertEquals(0, dict.size());

            // Model the next frame's pre-sizing: extendPos re-exposes the slots
            // the failed frame grew into, without writing them. They must hold
            // null, not the failed frame's Strings.
            dict.extendPos(3);
            for (int i = 0; i < 3; i++) {
                Assert.assertNull("slot " + i + " must not retain the failed frame's symbol",
                        dict.getQuick(i));
            }

            // And the connection still accepts a correct first frame afterwards.
            dict.setPos(0);
            Assert.assertFalse(decodeDeltaDict(cursor, dict, 0, "sym_a", "sym_b"));
            Assert.assertEquals(2, dict.size());
            Assert.assertEquals("sym_a", dict.getQuick(0));
            Assert.assertEquals("sym_b", dict.getQuick(1));
        });
    }

    @Test
    public void testSymbolDictRedefinitionDetected() throws Exception {
        // Guards the orphan-adoption symbol-corruption fix: when a connection's
        // delta symbol dictionary remaps an already-defined client symbol ID to
        // a different string (a different sender's dict-from-0 replayed into the
        // connection), QwpMessageCursor must flag it so the clientSymbolId ->
        // tableSymbolId cache is dropped. An identical re-send or a pure append
        // must NOT be flagged, so the cache stays warm in the common case.
        assertMemoryLeak(() -> {
            QwpMessageCursor cursor = new QwpMessageCursor();
            ObjList<String> dict = new ObjList<>();

            // First definition of ids 0..2 — every entry is new, not a remap.
            Assert.assertFalse(decodeDeltaDict(cursor, dict, 0, "sym_a", "sym_b", "sym_c"));

            // Identical dict-from-0 re-send (the steady-state case within one
            // sender): entries overwritten with equal values, not a remap.
            Assert.assertFalse(decodeDeltaDict(cursor, dict, 0, "sym_a", "sym_b", "sym_c"));

            // Pure append of a fresh id (a true incremental delta) — not a remap.
            Assert.assertFalse(decodeDeltaDict(cursor, dict, 3, "sym_d"));

            // Orphan-adoption shape: a different sender's dict-from-0 remaps id 0
            // (sym_a -> sym_x). This MUST be flagged.
            Assert.assertTrue(decodeDeltaDict(cursor, dict, 0, "sym_x", "sym_b", "sym_c"));

            // Once the remap is absorbed, an identical re-send is stable again.
            Assert.assertFalse(decodeDeltaDict(cursor, dict, 0, "sym_x", "sym_b", "sym_c"));
        });
    }

    // Builds a delta-symbol-dictionary-only QWP message (no table blocks),
    // decodes it through the supplied cursor against the accumulating
    // connection dictionary, and returns whether the decode flagged a
    // client-symbol-ID remap. The cursor and dict are reused across calls to
    // model one connection receiving successive messages.
    private static boolean decodeDeltaDict(
            QwpMessageCursor cursor,
            ObjList<String> connectionDict,
            int deltaStartId,
            String... symbols
    ) throws Exception {
        return decodeDeltaDictDeclaring(cursor, connectionDict, deltaStartId, symbols.length, symbols);
    }

    /**
     * As {@link #decodeDeltaDict}, but stamps {@code declaredCount} into the frame's
     * deltaCount header field independently of how many {@code symbols} are actually
     * written. With {@code declaredCount > symbols.length} the decoder's entry loop runs
     * off the end of the payload and throws -- the reachable trigger for a torn or
     * mismatched delta frame.
     */
    private static boolean decodeDeltaDictDeclaring(
            QwpMessageCursor cursor,
            ObjList<String> connectionDict,
            int deltaStartId,
            int declaredCount,
            String... symbols
    ) throws Exception {
        // The header advertises declaredCount; the payload carries only the symbols
        // actually supplied. When declaredCount > symbols.length the frame is genuinely
        // short and the decoder's entry loop runs off the end.
        byte[][] symbolBytes = new byte[symbols.length][];
        int payloadSize = QwpVarint.encodedLength(deltaStartId) + QwpVarint.encodedLength(declaredCount);
        for (int i = 0; i < symbols.length; i++) {
            symbolBytes[i] = symbols[i].getBytes(StandardCharsets.UTF_8);
            payloadSize += QwpVarint.encodedLength(symbolBytes[i].length) + symbolBytes[i].length;
        }
        int totalSize = HEADER_SIZE + payloadSize;
        long address = Unsafe.malloc(totalSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putInt(address + HEADER_OFFSET_MAGIC, MAGIC_MESSAGE);
            Unsafe.putByte(address + HEADER_OFFSET_VERSION, VERSION);
            Unsafe.putByte(address + HEADER_OFFSET_FLAGS, FLAG_DELTA_SYMBOL_DICT);
            Unsafe.putShort(address + HEADER_OFFSET_TABLE_COUNT, (short) 0);
            Unsafe.putInt(address + HEADER_OFFSET_PAYLOAD_LENGTH, payloadSize);

            long pos = address + HEADER_SIZE;
            pos = QwpVarint.encode(pos, deltaStartId);
            pos = QwpVarint.encode(pos, declaredCount);
            for (int i = 0; i < symbols.length; i++) {
                pos = QwpVarint.encode(pos, symbolBytes[i].length);
                for (byte b : symbolBytes[i]) {
                    Unsafe.putByte(pos++, b);
                }
            }

            cursor.of(address, totalSize, connectionDict);
            return cursor.isSymbolDictRedefined();
        } finally {
            Unsafe.free(address, totalSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDictionaryEmpty() throws Exception {
        int rowCount = 3;
        int size = 100;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;

            // null bitmap present
            Unsafe.putByte(pos, (byte) 1);
            pos++;

            // Null bitmap (all nulls)
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            QwpNullBitmapTestUtil.fillAllNull(pos, rowCount);
            pos += bitmapSize;

            // Empty dictionary (size = 0)
            pos = QwpVarint.encode(pos, 0);

            // No value indices needed: all rows are null (skipped by bitmap)

            int actualSize = (int) (pos - address);
            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            cursor.of(address, actualSize, rowCount);

            for (int i = 0; i < rowCount; i++) {
                boolean isNull = cursor.advanceRow();
                Assert.assertTrue("Row " + i + " should be null", isNull);
                Assert.assertTrue(cursor.isNull());
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDictionaryLarge() throws Exception {
        int dictSize = 1000;
        String[] values = new String[dictSize];
        for (int i = 0; i < dictSize; i++) {
            values[i] = "symbol_" + i;
        }
        assertRoundTrip(values, null);
    }

    @Test
    public void testDictionaryParsing() throws Exception {
        String[] values = {"a", "b", "a", "b", "c", "a"};
        assertRoundTrip(values, null);
    }

    @Test
    public void testDictionarySizeRejectsNegativeVarintValue() {
        int size = 32;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;
            Unsafe.putByte(pos++, (byte) 0);
            pos = QwpVarint.encode(pos, Long.MIN_VALUE);

            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            try {
                cursor.of(address, (int) (pos - address), 0);
                Assert.fail("Expected QwpParseException for negative dictionary size varint");
            } catch (QwpParseException e) {
                Assert.assertTrue(e.getMessage().contains("dictionary size out of int range"));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDictionaryStringLengthRejectsNegativeVarintValue() {
        int size = 64;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;
            Unsafe.putByte(pos++, (byte) 0);
            pos = QwpVarint.encode(pos, 1);
            pos = QwpVarint.encode(pos, -1L);

            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            try {
                cursor.of(address, (int) (pos - address), 0);
                Assert.fail("Expected QwpParseException for negative dictionary string length varint");
            } catch (QwpParseException e) {
                Assert.assertTrue(e.getMessage().contains("dictionary string length out of int range"));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testInsufficientDataForDictionary() {
        int size = 6;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;
            // no null bitmap
            Unsafe.putByte(pos, (byte) 0);
            pos++;
            // Dictionary size = 1
            pos = QwpVarint.encode(pos, 1);
            // String length = 100 (but we don't have that much data)
            QwpVarint.encode(pos, 100);

            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            cursor.of(address, size, 1);
            Assert.fail("Expected QwpParseException");
        } catch (QwpParseException e) {
            Assert.assertFalse(e.getMessage().isEmpty());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testInvalidDictionaryIndex() {
        int size = 100;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;

            // no null bitmap
            Unsafe.putByte(pos, (byte) 0);
            pos++;

            // Dictionary: 1 entry
            pos = QwpVarint.encode(pos, 1);

            // Entry "a"
            byte[] aBytes = "a".getBytes(StandardCharsets.UTF_8);
            pos = QwpVarint.encode(pos, aBytes.length);
            for (byte b : aBytes) {
                Unsafe.putByte(pos++, b);
            }

            // Value: index 5 (invalid, only 1 entry in dictionary)
            pos = QwpVarint.encode(pos, 5);

            int actualSize = (int) (pos - address);
            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            cursor.of(address, actualSize, 1);
            cursor.advanceRow();
            Assert.fail("Expected QwpParseException for out-of-bounds dictionary index");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INVALID_DICTIONARY_INDEX, e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("symbol index out of range"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testInvalidDictionaryIndexDeltaMode() {
        int size = 100;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;

            // no null bitmap
            Unsafe.putByte(pos, (byte) 0);
            pos++;

            // Value: index 3 (invalid, connection dictionary has only 2 entries)
            pos = QwpVarint.encode(pos, 3);

            int actualSize = (int) (pos - address);
            ObjList<String> connectionDict = new ObjList<>();
            connectionDict.add("alpha");
            connectionDict.add("beta");

            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            cursor.of(address, actualSize, 1, connectionDict);
            cursor.advanceRow();
            Assert.fail("Expected QwpParseException for out-of-bounds delta dictionary index");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INVALID_DICTIONARY_INDEX, e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("symbol index out of range"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSymbolIndexMapping() throws Exception {
        String[] values = {"same", "same", "same", "same"};
        assertRoundTrip(values, null);
    }

    @Test
    public void testSymbolIndexRejectsNegativeVarintValue() {
        int size = 64;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;

            Unsafe.putByte(pos++, (byte) 0);
            pos = QwpVarint.encode(pos, 1);
            pos = QwpVarint.encode(pos, 1);
            Unsafe.putByte(pos++, (byte) 'a');
            pos = QwpVarint.encode(pos, Long.MIN_VALUE);

            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            // Validation now happens in of() instead of advanceRow()
            cursor.of(address, (int) (pos - address), 1);
            Assert.fail("Expected QwpParseException for negative symbol index varint");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INVALID_DICTIONARY_INDEX, e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("symbol index out of int range"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSymbolLargeColumn() throws Exception {
        int rowCount = 10_000;
        int uniqueSymbols = 100;
        String[] values = new String[rowCount];
        for (int i = 0; i < rowCount; i++) {
            values[i] = "sym_" + (i % uniqueSymbols);
        }
        assertRoundTrip(values, null);
    }

    @Test
    public void testSymbolNullIndex() {
        int size = 100;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long pos = address;

            // no null bitmap
            Unsafe.putByte(pos, (byte) 0);
            pos++;

            // Dictionary: 2 entries
            pos = QwpVarint.encode(pos, 2);

            // Entry "a"
            byte[] aBytes = "a".getBytes(StandardCharsets.UTF_8);
            pos = QwpVarint.encode(pos, aBytes.length);
            for (byte b : aBytes) {
                Unsafe.putByte(pos++, b);
            }

            // Entry "b"
            byte[] bBytes = "b".getBytes(StandardCharsets.UTF_8);
            pos = QwpVarint.encode(pos, bBytes.length);
            for (byte b : bBytes) {
                Unsafe.putByte(pos++, b);
            }

            // Values: index 0 for row 0, then index 1 for row 1
            pos = QwpVarint.encode(pos, 0);
            pos = QwpVarint.encode(pos, 1);

            int actualSize = (int) (pos - address);
            QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
            cursor.of(address, actualSize, 2);

            // Row 0: "a"
            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());
            Assert.assertEquals("a", Objects.toString(cursor.getSymbolCharSequence()));
            Assert.assertEquals(0, cursor.getSymbolIndex());

            // Row 1: "b"
            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());
            Assert.assertEquals("b", Objects.toString(cursor.getSymbolCharSequence()));
            Assert.assertEquals(1, cursor.getSymbolIndex());
        } catch (QwpParseException e) {
            Assert.fail("Unexpected parse exception: " + e.getMessage());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSymbolRepeatedValues() throws Exception {
        String[] symbols = {"low", "medium", "high"};
        int rowCount = 1000;
        String[] values = new String[rowCount];
        for (int i = 0; i < rowCount; i++) {
            values[i] = symbols[i % 3];
        }
        assertRoundTrip(values, null);
    }

    @Test
    public void testSymbolUtf8() throws Exception {
        String[] values = {"日本語", "中文", "한국어", "Ελληνικά"};
        assertRoundTrip(values, null);
    }

    @Test
    public void testSymbolWithNulls() throws Exception {
        String[] values = {"a", null, "b", null};
        boolean[] nulls = {false, true, false, true};
        assertRoundTrip(values, nulls);
    }

    private static int findSymbolColumnIndex(QwpTableBlockCursor table) {
        for (int c = 0; c < table.getColumnCount(); c++) {
            if (table.getColumnDef(c).getTypeCode() == TYPE_SYMBOL) {
                return c;
            }
        }
        return -1;
    }

    private static @NotNull QwpTableBuffer getQwpTableBuffer(String[] values, boolean[] nulls, boolean useNullBitmap) {
        QwpTableBuffer buffer = new QwpTableBuffer("test_symbol");
        QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_SYMBOL, useNullBitmap);
        QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
        for (int i = 0; i < values.length; i++) {
            if (useNullBitmap && nulls[i]) {
                col.addNull();
            } else {
                col.addSymbol(values[i]);
            }
            tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
            buffer.nextRow();
        }
        return buffer;
    }

    private void assertRoundTrip(String[] values, boolean[] nulls) throws Exception {
        assertMemoryLeak(() -> {
            boolean useNullBitmap = nulls != null;
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = getQwpTableBuffer(values, nulls, useNullBitmap);
                int size = encoder.encode(buffer);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();
                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();
                    Assert.assertEquals(values.length, table.getRowCount());
                    int colIdx = findSymbolColumnIndex(table);
                    Assert.assertNotEquals("SYMBOL column not found", -1, colIdx);
                    for (int i = 0; i < values.length; i++) {
                        Assert.assertTrue(table.hasNextRow());
                        table.nextRow();
                        if (useNullBitmap && nulls[i]) {
                            Assert.assertTrue("Row " + i + " should be null",
                                    table.isColumnNull(colIdx));
                        } else {
                            Assert.assertFalse("Row " + i + " should not be null",
                                    table.isColumnNull(colIdx));
                            QwpSymbolColumnCursor cursor = table.getSymbolColumn(colIdx);
                            Assert.assertEquals("Row " + i + " value mismatch",
                                    values[i], Objects.toString(cursor.getSymbolCharSequence()));
                        }
                    }
                    Assert.assertFalse(table.hasNextRow());
                }
            }
        });
    }
}
