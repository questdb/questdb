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

import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.qwp.codec.QwpEgressColumnDef;
import io.questdb.cutlass.qwp.codec.QwpEgressConnSymbolDict;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.codec.QwpResultBatchBuffer;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.cutlass.qwp.server.egress.QwpEgressRequestDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Regression coverage for verifiable CodeRabbit findings on PR 6991.
 *
 * <p>Each test asserts the <em>correct post-fix</em> behavior: tests fail on
 * the current buggy code and pass once the corresponding fix lands. That
 * keeps them useful as regression guards.
 */
public class QwpEgressReviewFindingsTest {

    /**
     * Finding #5: {@code QwpEgressRequestDecoder.decodeCredit} returns the varint
     * unmodified. A varint with the sign bit set decodes to a negative long,
     * which then flows into flow-control accounting via
     * {@code QwpEgressProcessorState.addStreamingCredit}.
     *
     * <p>Expected after fix: decodeCredit rejects a negative budget the same
     * way {@code decodeQueryRequest} rejects a negative {@code initial_credit}.
     */
    @Test
    public void testDecodeCreditRejectsNegativeBudget() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int bufSize = 64;
            long buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            try {
                long p = buf;
                Unsafe.putByte(p++, QwpEgressMsgKind.CREDIT);
                Unsafe.putLong(p, 1L);
                p += 8;
                // A varint whose decoded long is Long.MIN_VALUE -- sign bit set.
                p = QwpVarint.encode(p, Long.MIN_VALUE);
                int total = (int) (p - buf);

                QwpEgressRequestDecoder decoder = new QwpEgressRequestDecoder();
                try {
                    long credit = decoder.decodeCredit(buf, total);
                    Assert.fail("decodeCredit accepted a negative budget (" + credit
                            + "). It should throw QwpParseException, matching the"
                            + " decodeQueryRequest check on initial_credit.");
                } catch (QwpParseException expected) {
                    Assert.assertTrue(
                            "QwpParseException message should mention CREDIT: "
                                    + expected.getFlyweightMessage(),
                            expected.getFlyweightMessage().toString().toUpperCase().contains("CREDIT")
                    );
                }
            } finally {
                Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Finding #4: {@code emitTableBlock} only guards the single name byte
     * ({@code p >= wireLimit}) before writing two varints, each up to
     * {@link QwpVarint#MAX_VARINT_BYTES} bytes. On a tight wireLimit the
     * varint encode walks past the declared limit.
     *
     * <p>Test setup: place a guard byte at the wireLimit boundary. A correct
     * preflight returns -1 and leaves the guard intact. The current code
     * returns a positive byte count and overwrites the guard.
     */
    @Test
    public void testEmitTableBlockRespectsTightWireLimit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int bufCapacity = 64;
            long buf = Unsafe.malloc(bufCapacity, MemoryTag.NATIVE_DEFAULT);
            try {
                ObjList<QwpEgressColumnDef> cols = new ObjList<>();
                QwpEgressColumnDef def = new QwpEgressColumnDef();
                def.of("x", ColumnType.INT);
                cols.add(def);
                cols.add(def);
                cols.add(def);

                try (QwpResultBatchBuffer batch = new QwpResultBatchBuffer();
                     QwpEgressConnSymbolDict dict = new QwpEgressConnSymbolDict()) {
                    batch.beginBatch(cols, null, dict);

                    Unsafe.setMemory(buf, bufCapacity, (byte) 0);
                    final byte guard = (byte) 0xAB;
                    // Leave 2 bytes of usable wire ([wireBuf .. wireLimit)):
                    //   byte 0: name length  (written unconditionally)
                    //   byte 1: rowCount varint (written without any preflight)
                    //   byte 2: columnCount varint   <-- must not be written
                    long wireLimit = buf + 2;
                    Unsafe.putByte(wireLimit, guard);

                    // writeFullSchema=false isolates the bug: the reference-mode
                    // branch has its own preflight, the prelude (name + row + col
                    // varints) does not.
                    int written = batch.emitTableBlock(buf, wireLimit, 0L, false);

                    byte guardAfter = Unsafe.getByte(wireLimit);
                    Assert.assertEquals(
                            "emitTableBlock returned " + written + " on a wireLimit that"
                                    + " cannot fit the name byte + both varints; it should"
                                    + " return -1.",
                            -1, written
                    );
                    Assert.assertEquals(
                            "emitTableBlock clobbered the byte at wireLimit with 0x"
                                    + Integer.toHexString(guardAfter & 0xFF)
                                    + " (guard was 0x" + Integer.toHexString(guard & 0xFF) + ").",
                            guard, guardAfter
                    );
                }
            } finally {
                Unsafe.free(buf, bufCapacity, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Finding #2: {@code QwpColumnScratch.encodeUtf8} falls through to the
     * 3-byte branch for lone surrogates, emitting sequences prohibited by
     * RFC 3629. A standards-compliant encoder substitutes U+FFFD or '?' --
     * see {@code Utf8s.encodeUtf16Surrogate} for the project convention.
     *
     * <p>Test strictly decodes the emitted bytes via the JDK's strict UTF-8
     * decoder; a surrogate code point encoded directly fails with
     * {@link CharacterCodingException}.
     */
    @Test
    public void testEncodeUtf8HandlesLoneHighSurrogate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int bufSize = 16;
            long buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            try {
                // Lone high surrogate at end of input: no low surrogate follows, so
                // Character.isHighSurrogate+next-char branch is skipped, else branch
                // hits the 3-byte encoding.
                String s = "a\uD800";
                int written = invokeEncodeUtf8(s, buf);

                byte[] emitted = new byte[written];
                for (int i = 0; i < written; i++) {
                    emitted[i] = Unsafe.getByte(buf + i);
                }
                assertValidUtf8(emitted);
            } finally {
                Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Finding #2 (companion): lone low surrogate. Same failure mode as the
     * high-surrogate case -- the 3-byte branch emits {@code ED B0 80} for
     * U+DC00, which is not valid UTF-8.
     */
    @Test
    public void testEncodeUtf8HandlesLoneLowSurrogate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int bufSize = 16;
            long buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            try {
                String s = "\uDC00";
                int written = invokeEncodeUtf8(s, buf);

                byte[] emitted = new byte[written];
                for (int i = 0; i < written; i++) {
                    emitted[i] = Unsafe.getByte(buf + i);
                }
                assertValidUtf8(emitted);
            } finally {
                Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void assertValidUtf8(byte[] bytes) {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            decoder.decode(ByteBuffer.wrap(bytes));
        } catch (CharacterCodingException e) {
            StringBuilder hex = new StringBuilder();
            for (byte b : bytes) {
                if (!hex.isEmpty()) hex.append(' ');
                hex.append(String.format("%02X", b & 0xFF));
            }
            Assert.fail("encodeUtf8 emitted invalid UTF-8: [" + hex + "] -- "
                    + e.getMessage() + ". RFC 3629 forbids direct encoding of"
                    + " surrogate code points; substitute U+FFFD or '?'.");
        }
    }

    /**
     * Reflection helper. {@code QwpColumnScratch#encodeUtf8} is private static;
     * reach it without widening production visibility. Always encodes from the
     * start of the heap buffer.
     */
    private static int invokeEncodeUtf8(CharSequence cs, long heapAddr) throws Exception {
        Class<?> cls = Class.forName("io.questdb.cutlass.qwp.codec.QwpColumnScratch");
        Method m = cls.getDeclaredMethod("encodeUtf8", CharSequence.class, long.class, int.class);
        m.setAccessible(true);
        return (int) m.invoke(null, cs, heapAddr, 0);
    }
}
