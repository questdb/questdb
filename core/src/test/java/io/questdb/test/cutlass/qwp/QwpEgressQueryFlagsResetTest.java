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

import io.questdb.cutlass.qwp.codec.QwpEgressConnSymbolDict;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.cutlass.qwp.server.egress.QwpEgressProcessorState;
import io.questdb.cutlass.qwp.server.egress.QwpEgressRequestDecoder;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * Integration coverage tying a decoded {@code QUERY_REQUEST} query_flags trailer
 * to the actual connection-scoped SYMBOL dict reset. It reproduces the server
 * sequence from {@code QwpEgressUpgradeProcessor.handleQueryRequest} /
 * {@code applyCacheResetForUpcomingQuery} without the HTTP/WebSocket plumbing:
 * <pre>
 *   decoder.decodeQueryRequest(...)
 *   forceDictReset = (decoder.queryFlags &amp; QUERY_FLAG_RESET_DICT) != 0
 *   mask = state.computeCacheResetMask(forceDictReset)
 *   if (mask != 0) { state.applyCacheReset(mask); state.mergePendingCacheResetMask(mask); }
 * </pre>
 * What these tests pin is the decode -&gt; reset effect: a real QUERY_REQUEST
 * built in native memory drives the real {@link QwpEgressRequestDecoder} and a
 * real {@link QwpEgressProcessorState}. The over-the-wire {@code CACHE_RESET}
 * frame and client-side symbol resolution are covered separately by
 * {@link QwpEgressCacheResetWireTest}; the per-field decoding by
 * {@link QwpEgressRequestDecoderTest}; the mask boundaries by
 * {@link QwpEgressProcessorStateCacheResetTest}.
 */
public class QwpEgressQueryFlagsResetTest extends AbstractCairoTest {

    @Test
    public void testFlagAbsentKeepsConnDict() throws Exception {
        runWithReq(64, (buf, bindVars, decoder, state) -> {
            QwpEgressConnSymbolDict dict = state.getConnSymbolDict();
            dict.addEntry("a");
            dict.addEntry("b");
            dict.addEntry("c");

            int len = writeQueryRequest(buf, "SELECT 1", false, 0L);
            decoder.decodeQueryRequest(buf, len, bindVars);
            Assert.assertEquals("baseline frame carries no flags", 0L, decoder.queryFlags);

            byte mask = applyServerSideReset(state, decoder.queryFlags);
            Assert.assertEquals("no flag, below cap => no reset", 0, mask);
            Assert.assertEquals("dict must survive a query that did not ask for a reset", 3, dict.size());
            Assert.assertEquals(0, state.getPendingCacheResetMask());
        });
    }

    @Test
    public void testFlagSetButEmptyDictIsNoOp() throws Exception {
        runWithReq(64, (buf, bindVars, decoder, state) -> {
            // Dict starts empty: the flag asks for a reset, but there is nothing
            // to clear, so the empty-dict guard suppresses the CACHE_RESET frame.
            int len = writeQueryRequest(buf, "SELECT 1", true, QwpEgressMsgKind.QUERY_FLAG_RESET_DICT);
            decoder.decodeQueryRequest(buf, len, bindVars);
            Assert.assertEquals((long) QwpEgressMsgKind.QUERY_FLAG_RESET_DICT, decoder.queryFlags);

            byte mask = applyServerSideReset(state, decoder.queryFlags);
            Assert.assertEquals("empty dict needs no reset frame", 0, mask);
            Assert.assertEquals(0, state.getConnSymbolDict().size());
            Assert.assertEquals(0, state.getPendingCacheResetMask());
        });
    }

    @Test
    public void testFlagSetClearsConnDict() throws Exception {
        runWithReq(64, (buf, bindVars, decoder, state) -> {
            QwpEgressConnSymbolDict dict = state.getConnSymbolDict();
            dict.addEntry("a");
            dict.addEntry("b");
            dict.addEntry("c");

            int len = writeQueryRequest(buf, "SELECT 1", true, QwpEgressMsgKind.QUERY_FLAG_RESET_DICT);
            decoder.decodeQueryRequest(buf, len, bindVars);
            Assert.assertEquals((long) QwpEgressMsgKind.QUERY_FLAG_RESET_DICT, decoder.queryFlags);

            byte mask = applyServerSideReset(state, decoder.queryFlags);
            Assert.assertEquals(QwpEgressMsgKind.RESET_MASK_DICT, mask);
            Assert.assertEquals("forced reset must flush the conn dict before the query streams",
                    0, dict.size());
            Assert.assertEquals("the CACHE_RESET frame must be staged for emission",
                    QwpEgressMsgKind.RESET_MASK_DICT, state.getPendingCacheResetMask());
        });
    }

    @Test
    public void testUnknownFlagBitDoesNotReset() throws Exception {
        runWithReq(64, (buf, bindVars, decoder, state) -> {
            QwpEgressConnSymbolDict dict = state.getConnSymbolDict();
            dict.addEntry("a");
            dict.addEntry("b");
            dict.addEntry("c");

            // A reserved bit that is NOT RESET_DICT must be ignored by the reset
            // path: the dict keeps growing across the query as before.
            int len = writeQueryRequest(buf, "SELECT 1", true, 0x02L);
            decoder.decodeQueryRequest(buf, len, bindVars);
            Assert.assertEquals(0x02L, decoder.queryFlags);

            byte mask = applyServerSideReset(state, decoder.queryFlags);
            Assert.assertEquals("a non-RESET_DICT flag must not trigger a reset", 0, mask);
            Assert.assertEquals(3, dict.size());
            Assert.assertEquals(0, state.getPendingCacheResetMask());
        });
    }

    /**
     * Mirrors {@code QwpEgressUpgradeProcessor.applyCacheResetForUpcomingQuery}:
     * derive the force bit from the decoded flags, compute the mask, and -- when
     * non-zero -- apply it locally and stage the pending CACHE_RESET. Returns the
     * computed mask so callers can assert on it.
     */
    private static byte applyServerSideReset(QwpEgressProcessorState state, long queryFlags) {
        boolean forceDictReset = (queryFlags & QwpEgressMsgKind.QUERY_FLAG_RESET_DICT) != 0;
        byte mask = state.computeCacheResetMask(forceDictReset);
        if (mask != 0) {
            state.applyCacheReset(mask);
            state.mergePendingCacheResetMask(mask);
        }
        return mask;
    }

    private void runWithReq(int bufSize, ReqBody body) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            BindVariableServiceImpl bindVars = new BindVariableServiceImpl(configuration);
            QwpEgressRequestDecoder decoder = new QwpEgressRequestDecoder();
            long buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration)) {
                body.run(buf, bindVars, decoder, state);
            } finally {
                Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Builds a QUERY_REQUEST frame (no binds) in native memory:
     * {@code msg_kind + request_id + sql_len + sql + initial_credit + bind_count}
     * plus an optional {@code query_flags} trailer. Returns the total byte length.
     */
    private static int writeQueryRequest(long buf, String sqlText, boolean withTrailer, long flags) {
        byte[] sql = sqlText.getBytes(StandardCharsets.UTF_8);
        long p = buf;
        Unsafe.putByte(p++, QwpEgressMsgKind.QUERY_REQUEST);
        Unsafe.putLong(p, 1L); // request_id
        p += 8;
        p = QwpVarint.encode(p, sql.length);
        for (byte b : sql) {
            Unsafe.putByte(p++, b);
        }
        p = QwpVarint.encode(p, 0L); // initial_credit
        p = QwpVarint.encode(p, 0L); // bind_count
        if (withTrailer) {
            p = QwpVarint.encode(p, flags);
        }
        return (int) (p - buf);
    }

    @FunctionalInterface
    private interface ReqBody {
        void run(long buf, BindVariableServiceImpl bindVars, QwpEgressRequestDecoder decoder, QwpEgressProcessorState state)
                throws Exception;
    }
}
