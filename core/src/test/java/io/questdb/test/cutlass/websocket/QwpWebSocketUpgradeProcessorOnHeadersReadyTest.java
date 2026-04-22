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

package io.questdb.test.cutlass.websocket;

import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpException;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRawSocket;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.qwp.server.QwpProcessorState;
import io.questdb.cutlass.qwp.server.QwpWebSocketUpgradeProcessor;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

public class QwpWebSocketUpgradeProcessorOnHeadersReadyTest extends AbstractCairoTest {
    private static final int TINY_BUFFER_SIZE = 64;

    @Test
    public void testOnHeadersReadyFailsHardWhenBadRequestResponseDoesNotFitBuffer() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);
            LocalValue<QwpProcessorState> lv = getLV();

            long bufferAddr = Unsafe.malloc(TINY_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (
                    MockHttpRequestHeader header = new MockHttpRequestHeader();
                    TestableContext context = new TestableContext(httpConfig, header, new MockRawSocket(bufferAddr, TINY_BUFFER_SIZE))
            ) {
                header.setHeader("Upgrade", "websocket");
                header.setHeader("Connection", "Upgrade");
                header.setHeader("Sec-WebSocket-Version", "13");

                assertBufferTooSmallFailure(processor, context, "400 bad request response");

                Assert.assertEquals(0, context.getMockRawSocket().sentSize);
                Assert.assertFalse(context.isSwitchProtocolCalled());
                Assert.assertNull(lv.get(context));
            } finally {
                Unsafe.free(bufferAddr, TINY_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testOnHeadersReadyFailsHardWhenHandshakeResponseDoesNotFitBuffer() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);
            LocalValue<QwpProcessorState> lv = getLV();

            long bufferAddr = Unsafe.malloc(TINY_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (
                    MockHttpRequestHeader header = new MockHttpRequestHeader();
                    TestableContext context = new TestableContext(httpConfig, header, new MockRawSocket(bufferAddr, TINY_BUFFER_SIZE))
            ) {
                header.setHeader("Upgrade", "websocket");
                header.setHeader("Connection", "Upgrade");
                header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
                header.setHeader("Sec-WebSocket-Version", "13");

                assertBufferTooSmallFailure(processor, context, "101 handshake response");

                Assert.assertEquals(0, context.getMockRawSocket().sentSize);
                Assert.assertFalse(context.isSwitchProtocolCalled());
                Assert.assertNull(lv.get(context));
            } finally {
                Unsafe.free(bufferAddr, TINY_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testOnHeadersReadyFailsHardWhenUpgradeRequiredResponseDoesNotFitBuffer() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);
            LocalValue<QwpProcessorState> lv = getLV();

            long bufferAddr = Unsafe.malloc(TINY_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (
                    MockHttpRequestHeader header = new MockHttpRequestHeader();
                    TestableContext context = new TestableContext(httpConfig, header, new MockRawSocket(bufferAddr, TINY_BUFFER_SIZE))
            ) {
                header.setHeader("Upgrade", "websocket");
                header.setHeader("Connection", "Upgrade");
                header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
                header.setHeader("Sec-WebSocket-Version", "12");

                assertBufferTooSmallFailure(processor, context, "426 upgrade response");

                Assert.assertEquals(0, context.getMockRawSocket().sentSize);
                Assert.assertFalse(context.isSwitchProtocolCalled());
                Assert.assertNull(lv.get(context));
            } finally {
                Unsafe.free(bufferAddr, TINY_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testOnHeadersReadyTreatsMissingVersionAsUpgradeRequiredWhenResponseDoesNotFitBuffer() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);
            LocalValue<QwpProcessorState> lv = getLV();

            long bufferAddr = Unsafe.malloc(TINY_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (
                    MockHttpRequestHeader header = new MockHttpRequestHeader();
                    TestableContext context = new TestableContext(httpConfig, header, new MockRawSocket(bufferAddr, TINY_BUFFER_SIZE))
            ) {
                header.setHeader("Upgrade", "websocket");
                header.setHeader("Connection", "Upgrade");
                header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");

                assertBufferTooSmallFailure(processor, context, "426 upgrade response");

                Assert.assertEquals(0, context.getMockRawSocket().sentSize);
                Assert.assertFalse(context.isSwitchProtocolCalled());
                Assert.assertNull(lv.get(context));
            } finally {
                Unsafe.free(bufferAddr, TINY_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void assertBufferTooSmallFailure(
            QwpWebSocketUpgradeProcessor processor,
            TestableContext context,
            CharSequence expectedResponseType
    )
            throws PeerDisconnectedException {
        try {
            processor.onHeadersReady(context);
            Assert.fail("Expected HttpException");
        } catch (HttpException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedResponseType);
            TestUtils.assertContains(e.getFlyweightMessage(), "does not fit send buffer");
        }
    }

    @SuppressWarnings("unchecked")
    private static LocalValue<QwpProcessorState> getLV() throws Exception {
        Field lvField = QwpWebSocketUpgradeProcessor.class.getDeclaredField("LV");
        lvField.setAccessible(true);
        return (LocalValue<QwpProcessorState>) lvField.get(null);
    }

    private static class MockHttpRequestHeader implements HttpRequestHeader, AutoCloseable {
        private final ObjList<Long> allocatedMemory = new ObjList<>();
        private final ObjList<Utf8String> headerNames = new ObjList<>();
        private final ObjList<DirectUtf8String> headerValues = new ObjList<>();

        @Override
        public void close() {
            for (int i = 0; i < allocatedMemory.size(); i += 2) {
                long ptr = allocatedMemory.get(i);
                long len = allocatedMemory.get(i + 1);
                Unsafe.free(ptr, len, MemoryTag.NATIVE_DEFAULT);
            }
            allocatedMemory.clear();
            headerNames.clear();
            headerValues.clear();
        }

        @Override
        public DirectUtf8Sequence getBoundary() {
            return null;
        }

        @Override
        public DirectUtf8Sequence getCharset() {
            return null;
        }

        @Override
        public DirectUtf8Sequence getContentDisposition() {
            return null;
        }

        @Override
        public DirectUtf8Sequence getContentDispositionFilename() {
            return null;
        }

        @Override
        public DirectUtf8Sequence getContentDispositionName() {
            return null;
        }

        @Override
        public long getContentLength() {
            return -1;
        }

        @Override
        public DirectUtf8Sequence getContentType() {
            return null;
        }

        @Override
        public DirectUtf8Sequence getHeader(Utf8Sequence name) {
            for (int i = 0; i < headerNames.size(); i++) {
                if (name.toString().equalsIgnoreCase(headerNames.get(i).toString())) {
                    return headerValues.get(i);
                }
            }
            return null;
        }

        @Override
        public ObjList<? extends Utf8Sequence> getHeaderNames() {
            return headerNames;
        }

        @Override
        public DirectUtf8Sequence getMethod() {
            return null;
        }

        @Override
        public DirectUtf8Sequence getMethodLine() {
            return null;
        }

        @Override
        public @Nullable DirectUtf8String getQuery() {
            return null;
        }

        @Override
        public long getStatementTimeout() {
            return 0;
        }

        @Override
        public DirectUtf8String getUrl() {
            return null;
        }

        @Override
        public DirectUtf8Sequence getUrlParam(Utf8Sequence name) {
            return null;
        }

        @Override
        public boolean isGetRequest() {
            return false;
        }

        @Override
        public boolean isPostRequest() {
            return false;
        }

        @Override
        public boolean isPutRequest() {
            return false;
        }

        void setHeader(String name, String value) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.putByte(ptr + i, bytes[i]);
            }
            allocatedMemory.add(ptr);
            allocatedMemory.add((long) bytes.length);

            DirectUtf8String directValue = new DirectUtf8String().of(ptr, ptr + bytes.length);
            for (int i = 0; i < headerNames.size(); i++) {
                if (name.equalsIgnoreCase(headerNames.get(i).toString())) {
                    headerValues.set(i, directValue);
                    return;
                }
            }
            headerNames.add(new Utf8String(name));
            headerValues.add(directValue);
        }
    }

    private static class MockRawSocket implements HttpRawSocket {
        private final long bufferAddress;
        private final int bufferSize;
        private int sentSize;

        private MockRawSocket(long bufferAddress, int bufferSize) {
            this.bufferAddress = bufferAddress;
            this.bufferSize = bufferSize;
        }

        @Override
        public long getBufferAddress() {
            return bufferAddress;
        }

        @Override
        public int getBufferSize() {
            return bufferSize;
        }

        @Override
        public void send(int size) {
            sentSize = size;
        }
    }

    private static class TestableContext extends HttpConnectionContext {
        private final MockRawSocket rawSocket;
        private final MockHttpRequestHeader requestHeader;
        private boolean switchProtocolCalled;

        private TestableContext(
                HttpFullFatServerConfiguration config,
                MockHttpRequestHeader requestHeader,
                MockRawSocket rawSocket
        ) {
            super(config, PlainSocketFactory.INSTANCE);
            this.requestHeader = requestHeader;
            this.rawSocket = rawSocket;
        }

        @Override
        public HttpRawSocket getRawResponseSocket() {
            return rawSocket;
        }

        @Override
        public HttpRequestHeader getRequestHeader() {
            return requestHeader;
        }

        @Override
        public void switchProtocol() {
            switchProtocolCalled = true;
        }

        MockRawSocket getMockRawSocket() {
            return rawSocket;
        }

        boolean isSwitchProtocolCalled() {
            return switchProtocolCalled;
        }
    }
}
