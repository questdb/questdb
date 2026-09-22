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

import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.cutlass.qwp.server.QwpStreamingDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_DOUBLE_ARRAY;

public class QwpIngressCacheRetentionTest extends AbstractCairoTest {

    private static final long ARRAY_CACHE_BASELINE_BYTES = 64L * (Long.BYTES + 2L * Integer.BYTES);

    @Test
    public void testDictionaryGapRetainsInflatedDecoderCache() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                QwpMessageCursor message = inflateArrayCache(state.getStreamingDecoder());
                long retainedCacheBytes = message.getRetainedCacheBytes();
                Assert.assertTrue(retainedCacheBytes > ARRAY_CACHE_BASELINE_BYTES);

                try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                    encoder.beginMessage(0, new GlobalSymbolDictionary(), 1, 1);
                    int size = encoder.finishMessage();
                    long address = encoder.getBuffer().getBufferPtr();
                    state.addData(address, address + size);
                }
                state.processMessage();

                Assert.assertEquals(QwpIngressProcessorState.Status.DICTIONARY_GAP, state.getStatus());
                Assert.assertEquals(retainedCacheBytes, message.getRetainedCacheBytes());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testDisconnectRetainsInflatedDecoderCache() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                QwpStreamingDecoder decoder = state.getStreamingDecoder();
                QwpMessageCursor message = inflateArrayCache(decoder);
                long retainedCacheBytes = message.getRetainedCacheBytes();
                Assert.assertTrue(retainedCacheBytes > ARRAY_CACHE_BASELINE_BYTES);

                state.onDisconnected();

                Assert.assertEquals(retainedCacheBytes, message.getRetainedCacheBytes());
                state.of(2, AllowAllSecurityContext.INSTANCE);
                Assert.assertSame(decoder, state.getStreamingDecoder());
                Assert.assertSame(message, inflateArrayCache(state.getStreamingDecoder()));
                Assert.assertEquals(retainedCacheBytes, message.getRetainedCacheBytes());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testParseFailureReleasesInflatedDecoderCaches() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                QwpMessageCursor message = inflateArrayCache(state.getStreamingDecoder());
                Assert.assertTrue(message.getRetainedCacheBytes() > ARRAY_CACHE_BASELINE_BYTES);

                long malformedMessage = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.putByte(malformedMessage, (byte) 0);
                    state.addData(malformedMessage, malformedMessage + 1);
                } finally {
                    Unsafe.free(malformedMessage, 1, MemoryTag.NATIVE_DEFAULT);
                }
                state.processMessage();

                Assert.assertEquals(QwpIngressProcessorState.Status.PARSE_ERROR, state.getStatus());
                Assert.assertEquals(ARRAY_CACHE_BASELINE_BYTES, message.getRetainedCacheBytes());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    private static QwpMessageCursor inflateArrayCache(QwpStreamingDecoder decoder) throws Exception {
        try (QwpTableBuffer buffer = new QwpTableBuffer("cache_release");
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer.ColumnBuffer column = buffer.getOrCreateColumn("a", TYPE_DOUBLE_ARRAY, true);
            for (int i = 0; i < 65; i++) {
                column.addDoubleArray(new double[]{i});
                buffer.nextRow();
            }
            int size = encoder.encode(buffer);
            QwpMessageCursor message = decoder.decode(encoder.getBuffer().getBufferPtr(), size);
            message.nextTable();
            return message;
        }
    }
}
