/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.http;

import io.questdb.cutlass.http.ex.RetryOperationException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjectPool;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HttpMultipartContentParserTest {
    private final static ObjectPool<DirectByteCharSequence> pool = new ObjectPool<>(DirectByteCharSequence::new, 32);
    private final static StringSink sink = new StringSink();
    private final static TestHttpMultipartContentListener LISTENER = new TestHttpMultipartContentListener();

    @Before
    public void setUp() {
        sink.clear();
    }

    @Test
    public void testEmpty() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (HttpMultipartContentParser multipartContentParser = new HttpMultipartContentParser(new HttpHeaderParser(1024, pool))) {
                final String content = "------WebKitFormBoundaryxFKYDBybTLu2rb8P--\r\n";
                final String expected = "";

                int len = content.length();
                long p = TestUtils.toMemory(content);
                try {
                    String boundary = "\r\n------WebKitFormBoundaryxFKYDBybTLu2rb8P";
                    long pBoundary = TestUtils.toMemory(boundary);
                    DirectByteCharSequence boundaryCs = new DirectByteCharSequence().of(pBoundary, pBoundary + boundary.length());
                    try {
                        for (int i = 0; i < len; i++) {
                            sink.clear();
                            multipartContentParser.clear();
                            multipartContentParser.of(boundaryCs);
                            multipartContentParser.parse(p, p + i, LISTENER);
                            multipartContentParser.parse(p + i, p + i + 1, LISTENER);
                            if (len > i + 1) {
                                multipartContentParser.parse(p + i + 1, p + len, LISTENER);
                            }
                            TestUtils.assertEquals(expected, sink);
                        }
                    } finally {
                        Unsafe.free(pBoundary, boundary.length(), MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Unsafe.free(p, len, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testMalformedAtEnd() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (HttpMultipartContentParser multipartContentParser = new HttpMultipartContentParser(new HttpHeaderParser(1024, pool))) {
                final String content = "------WebKitFormBoundaryxFKYDBybTLu2rb8PA--\r\n";
                int len = content.length();
                long p = TestUtils.toMemory(content);
                try {
                    String boundary = "\r\n------WebKitFormBoundaryxFKYDBybTLu2rb8P";
                    long pBoundary = TestUtils.toMemory(boundary);
                    DirectByteCharSequence boundaryCs = new DirectByteCharSequence().of(pBoundary, pBoundary + boundary.length());
                    try {
                        multipartContentParser.of(boundaryCs);
                        multipartContentParser.parse(p, p + len, LISTENER);
                        Assert.fail();
                    } catch (HttpException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Malformed start boundary");
                    } finally {
                        Unsafe.free(pBoundary, boundary.length(), MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Unsafe.free(p, len, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testSimple() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (HttpMultipartContentParser multipartContentParser = new HttpMultipartContentParser(new HttpHeaderParser(1024, pool))) {
                final String content = "------WebKitFormBoundaryxFKYDBybTLu2rb8P\r\n" +
                        "Content-Disposition: form-data; name=\"textline\"\r\n" +
                        "\r\n" +
                        "value1" +
                        "\r\n" +
                        "------WebKitFormBoundaryxF" +
                        "\r\n" +
                        "------WebKitFormBoundaryxFKYDBybTLu2rb8P\r\n" +
                        "Content-Disposition: form-data; name=\"textline2\"\n" +
                        "\r\n" +
                        "value2\r\n" +
                        "------WebKitFormBoundaryxFKYDBybTLu2rb8PZ" +
                        "\r\n" +
                        "------WebKitFormBoundaryxFKYDBybTLu2rb8P\r\n" +
                        "Content-Disposition: form-data; name=\"datafile\"; filename=\"pom.xml\"\r\n" +
                        "Content-Type: text/xml\r\n" +
                        "\r\n" +
                        "this is a file" +
                        "\r\n" +
                        "------WebKitFormBoundaryxFKYDBybTLu2rb8P--\r\n";

                String expected = "Content-Disposition: form-data; name=\"textline\"\r\n" +
                        "\r\n" +
                        "value1" +
                        "\r\n" +
                        "------WebKitFormBoundaryxF" +
                        "\r\n" +
                        "-----------------------------\r\n" +
                        "Content-Disposition: form-data; name=\"textline2\"\r\n" +
                        "\r\n" +
                        "value2\r\n" +
                        "------WebKitFormBoundaryxFKYDBybTLu2rb8PZ\r\n" +
                        "-----------------------------\r\n" +
                        "Content-Disposition: form-data; name=\"datafile\"; filename=\"pom.xml\"\r\n" +
                        "Content-Type: text/xml\r\n" +
                        "\r\n" +
                        "this is a file\r\n" +
                        "-----------------------------\r\n";

                int len = content.length();
                long p = TestUtils.toMemory(content);
                try {
                    String boundary = "\r\n------WebKitFormBoundaryxFKYDBybTLu2rb8P";
                    long pBoundary = TestUtils.toMemory(boundary);
                    DirectByteCharSequence boundaryCs = new DirectByteCharSequence().of(pBoundary, pBoundary + boundary.length());
                    try {
                        for (int i = 0; i < len; i++) {
                            sink.clear();
                            multipartContentParser.clear();
                            multipartContentParser.of(boundaryCs);
                            multipartContentParser.parse(p, p + i, LISTENER);
                            multipartContentParser.parse(p + i, p + i + 1, LISTENER);
                            if (len > i + 1) {
                                multipartContentParser.parse(p + i + 1, p + len, LISTENER);
                            }
                            TestUtils.assertEquals(expected, sink);
                        }
                    } finally {
                        Unsafe.free(pBoundary, boundary.length(), MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Unsafe.free(p, len, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testWrongStartBoundary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (HttpMultipartContentParser multipartContentParser = new HttpMultipartContentParser(new HttpHeaderParser(1024, pool))) {
                final String content = "------WebKitFormBoundaryxSLJiij2s--\r\n";
                int len = content.length();
                long p = TestUtils.toMemory(content);
                try {
                    String boundary = "\r\n------WebKitFormBoundaryxFKYDBybTLu2rb8P";
                    long pBoundary = TestUtils.toMemory(boundary);
                    DirectByteCharSequence boundaryCs = new DirectByteCharSequence().of(pBoundary, pBoundary + boundary.length());
                    try {
                        multipartContentParser.of(boundaryCs);
                        multipartContentParser.parse(p, p + len, LISTENER);
                        Assert.fail();
                    } catch (HttpException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Malformed start boundary");
                    } finally {
                        Unsafe.free(pBoundary, boundary.length(), MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Unsafe.free(p, len, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testBreaksNearFinalBoundary() throws Exception {
        for (int i = 0; i < 500; i++) {
            try {
                sink.clear();
                testBreaksCsvImportAt(i, null);
            } catch (Exception e) {
                System.out.println("i=" + i);
                throw e;
            }
        }
    }

    @Test
    public void testRetriesNearFinalBoundary() throws Exception {
        for (int i = 0; i < 500; i++) {
                sink.clear();
                testBreaksCsvImportAt(i, RetryOperationException.INSTANCE);
        }
    }

    private void testBreaksCsvImportAt(int breakAt, RuntimeException onChunkException) throws Exception {
        TestHttpMultipartContentListener listener = new TestHttpMultipartContentListener(onChunkException);
        TestUtils.assertMemoryLeak(() -> {
            try (HttpMultipartContentParser multipartContentParser = new HttpMultipartContentParser(new HttpHeaderParser(1024, pool))) {
                String boundaryToken = "------------------------27d997ca93d2689d";
                String boundary = "\r\n--" + boundaryToken;
                final String content = "--" + boundaryToken + "\r\n" +
                        "Content-Disposition: form-data; name=\"data\"; filename=\"02.csv\"\r\n" +
                        "\r\n" +
                        "B00014,,,\r\n" +
                        "--" + boundaryToken + "--";
                final String expected =
                        "Content-Disposition: form-data; name=\"data\"; filename=\"02.csv\"\r\n" +
                                "\r\n" +
                                "B00014,,,\r\n" +
                                "-----------------------------\r\n";

                if (breakAt >= content.length()) return;

                int len = content.length();
                long p = TestUtils.toMemory(content);
                try {
                    long pBoundary = TestUtils.toMemory(boundary);
                    DirectByteCharSequence boundaryCs = new DirectByteCharSequence().of(pBoundary, pBoundary + boundary.length());
                    try {
                        multipartContentParser.clear();
                        multipartContentParser.of(boundaryCs);
                        long breakPoint = p + len - breakAt;
                        long hi = p + len;
                        boolean result = parseWithRetry(listener, multipartContentParser, p, breakPoint);
                        if (hi > breakPoint) {
                            result = parseWithRetry(listener, multipartContentParser, breakPoint, hi);
                        }
                        Assert.assertEquals("Break at " + breakAt, expected, sink.toString());
                        Assert.assertTrue("Break at " + breakAt, result);
                    } finally {
                        Unsafe.free(pBoundary, boundary.length(), MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Unsafe.free(p, len, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    private boolean parseWithRetry(TestHttpMultipartContentListener listener, HttpMultipartContentParser multipartContentParser, long breakPoint, long hi) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        boolean result;
        try {
            result = multipartContentParser.parse(breakPoint, hi, listener);
        } catch (RetryOperationException e) {
            result = multipartContentParser.parse(multipartContentParser.getResumePtr(), hi, listener);
        }
        return result;
    }

    private static class TestHttpMultipartContentListener implements HttpMultipartContentListener {
        private final RuntimeException firstChunkException;
        private int onChunkCount;

        public TestHttpMultipartContentListener() {
            this(null);
        }

        public TestHttpMultipartContentListener(RuntimeException firstChunkException) {
            this.firstChunkException = firstChunkException;
        }

        @Override
        public void onChunk(long lo, long hi) {
            onChunkCount++;
            for (long p = lo; p < hi; p++) {
                sink.put((char) Unsafe.getUnsafe().getByte(p));
            }
            if (firstChunkException != null && onChunkCount == 1) {
                throw firstChunkException;
            }
        }

        @Override
        public void onPartBegin(HttpRequestHeader partHeader) {

            final CharSequence name = partHeader.getContentDispositionName();
            sink.put("Content-Disposition: ").put(partHeader.getContentDisposition());
            if (name != null) {
                sink.put("; name=\"").put(name).put('"');
            }

            final CharSequence fileName = partHeader.getContentDispositionFilename();
            if (fileName != null) {
                sink.put("; filename=\"").put(fileName).put('"');
            }

            // terminate Content-Disposition
            sink.put("\r\n");

            final CharSequence contentType = partHeader.getContentType();
            if (contentType != null) {
                sink.put("Content-Type: ").put(contentType).put("\r\n");
            }

            // terminate header
            sink.put("\r\n");
        }

        @Override
        public void onPartEnd() {
            sink.put("\r\n");
            sink.put("-----------------------------\r\n");
        }
    }
}