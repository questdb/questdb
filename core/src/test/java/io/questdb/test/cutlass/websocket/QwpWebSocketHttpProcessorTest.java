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

import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.qwp.server.QwpWebSocketHttpProcessor;
import io.questdb.std.ObjList;
import io.questdb.std.Utf8SequenceObjHashMap;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

/**
 * Tests for QWP v1 WebSocket HTTP processor, which handles WebSocket
 * upgrade requests on the /write/v4 endpoint.
 * Note: Full processor instantiation tests require CairoEngine and configuration
 * These are covered by integration tests (QwpWebSocketHandshakeTest, etc.)
 */
public class QwpWebSocketHttpProcessorTest extends AbstractWebSocketTest {

    @Test
    public void testGetWebSocketKey() throws Exception {
        assertMemoryLeak(() -> {
            try (MockHttpRequestHeader header = new MockHttpRequestHeader()) {
                header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");

                Utf8Sequence key = QwpWebSocketHttpProcessor.getWebSocketKey(header);
                Assert.assertNotNull(key);
                Assert.assertEquals("dGhlIHNhbXBsZSBub25jZQ==", key.toString());
            }
        });
    }

    @Test
    public void testGetWebSocketKeyMissing() throws Exception {
        assertMemoryLeak(() -> {
            try (MockHttpRequestHeader header = new MockHttpRequestHeader()) {
                Utf8Sequence key = QwpWebSocketHttpProcessor.getWebSocketKey(header);
                Assert.assertNull(key);
            }
        });
    }

    @Test
    public void testValidateHandshakeInvalidKey() throws Exception {
        assertMemoryLeak(() -> {
            try (MockHttpRequestHeader header = new MockHttpRequestHeader()) {
                header.setHeader("Upgrade", "websocket");
                header.setHeader("Connection", "Upgrade");
                header.setHeader("Sec-WebSocket-Key", "tooshort");
                header.setHeader("Sec-WebSocket-Version", "13");

                String error = QwpWebSocketHttpProcessor.validateHandshake(header);
                Assert.assertNotNull(error);
                Assert.assertTrue(error.contains("key"));
            }
        });
    }

    @Test
    public void testValidateHandshakeInvalidVersion() throws Exception {
        assertMemoryLeak(() -> {
            try (MockHttpRequestHeader header = new MockHttpRequestHeader()) {
                header.setHeader("Upgrade", "websocket");
                header.setHeader("Connection", "Upgrade");
                header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
                header.setHeader("Sec-WebSocket-Version", "12");

                String error = QwpWebSocketHttpProcessor.validateHandshake(header);
                Assert.assertNotNull(error);
                Assert.assertTrue(error.contains("version"));
            }
        });
    }

    @Test
    public void testValidateHandshakeMissingConnection() throws Exception {
        assertMemoryLeak(() -> {
            try (MockHttpRequestHeader header = new MockHttpRequestHeader()) {
                header.setHeader("Upgrade", "websocket");
                header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
                header.setHeader("Sec-WebSocket-Version", "13");

                String error = QwpWebSocketHttpProcessor.validateHandshake(header);
                Assert.assertNotNull(error);
                Assert.assertTrue(error.contains("Connection"));
            }
        });
    }

    @Test
    public void testValidateHandshakeMissingKey() throws Exception {
        assertMemoryLeak(() -> {
            try (MockHttpRequestHeader header = new MockHttpRequestHeader()) {
                header.setHeader("Upgrade", "websocket");
                header.setHeader("Connection", "Upgrade");
                header.setHeader("Sec-WebSocket-Version", "13");

                String error = QwpWebSocketHttpProcessor.validateHandshake(header);
                Assert.assertNotNull(error);
                Assert.assertTrue(error.contains("Sec-WebSocket-Key"));
            }
        });
    }

    @Test
    public void testValidateHandshakeMissingUpgrade() throws Exception {
        assertMemoryLeak(() -> {
            try (MockHttpRequestHeader header = new MockHttpRequestHeader()) {
                header.setHeader("Connection", "Upgrade");
                header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
                header.setHeader("Sec-WebSocket-Version", "13");

                String error = QwpWebSocketHttpProcessor.validateHandshake(header);
                Assert.assertNotNull(error);
                Assert.assertTrue(error.contains("Upgrade"));
            }
        });
    }

    @Test
    public void testValidateHandshakeMissingVersion() throws Exception {
        assertMemoryLeak(() -> {
            try (MockHttpRequestHeader header = new MockHttpRequestHeader()) {
                header.setHeader("Upgrade", "websocket");
                header.setHeader("Connection", "Upgrade");
                header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");

                String error = QwpWebSocketHttpProcessor.validateHandshake(header);
                Assert.assertNotNull(error);
                Assert.assertTrue(error.contains("Sec-WebSocket-Version"));
            }
        });
    }

    @Test
    public void testValidateHandshakeRejectsOriginHeader() throws Exception {
        assertMemoryLeak(() -> {
            try (MockHttpRequestHeader header = new MockHttpRequestHeader()) {
                header.setHeader("Upgrade", "websocket");
                header.setHeader("Connection", "Upgrade");
                header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
                header.setHeader("Sec-WebSocket-Version", "13");
                header.setHeader("Origin", "https://evil-site.com");

                String error = QwpWebSocketHttpProcessor.validateHandshake(header);
                Assert.assertNotNull(error);
                Assert.assertTrue(error.contains("Origin"));
            }
        });
    }

    @Test
    public void testValidateHandshakeSuccess() throws Exception {
        assertMemoryLeak(() -> {
            try (MockHttpRequestHeader header = new MockHttpRequestHeader()) {
                header.setHeader("Upgrade", "websocket");
                header.setHeader("Connection", "Upgrade");
                header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
                header.setHeader("Sec-WebSocket-Version", "13");

                String error = QwpWebSocketHttpProcessor.validateHandshake(header);
                Assert.assertNull(error);
            }
        });
    }

    /**
     * Mock implementation of HttpRequestHeader for testing.
     * Uses native memory allocation for header values to match the real implementation.
     */
    private static class MockHttpRequestHeader implements HttpRequestHeader, AutoCloseable {
        private final ObjList<Long> allocatedMemory = new ObjList<>();
        private final ObjList<Utf8String> headerNames = new ObjList<>();
        private final ObjList<DirectUtf8String> headerValues = new ObjList<>();

        @Override
        public void close() {
            for (int i = 0; i < allocatedMemory.size(); i += 2) {
                long ptr = allocatedMemory.get(i);
                long len = allocatedMemory.get(i + 1);
                io.questdb.std.Unsafe.free(ptr, len, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
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
        public Utf8SequenceObjHashMap<DirectUtf8String> getUrlParams() {
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
            byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            long ptr = io.questdb.std.Unsafe.malloc(bytes.length, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            for (int i = 0; i < bytes.length; i++) {
                io.questdb.std.Unsafe.putByte(ptr + i, bytes[i]);
            }
            allocatedMemory.add(ptr);
            allocatedMemory.add((long) bytes.length);

            DirectUtf8String directValue = new DirectUtf8String().of(ptr, ptr + bytes.length);

            // Check if header already exists (case-insensitive)
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
}
