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

import io.questdb.std.ObjectPool;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
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
                        Unsafe.free(pBoundary, boundary.length());
                    }
                } finally {
                    Unsafe.free(p, len);
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
                        TestUtils.assertContains(e.getMessage(), "Malformed start boundary");
                    } finally {
                        Unsafe.free(pBoundary, boundary.length());
                    }
                } finally {
                    Unsafe.free(p, len);
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
                        Unsafe.free(pBoundary, boundary.length());
                    }
                } finally {
                    Unsafe.free(p, len);
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
                        TestUtils.assertContains(e.getMessage(), "Malformed start boundary");
                    } finally {
                        Unsafe.free(pBoundary, boundary.length());
                    }
                } finally {
                    Unsafe.free(p, len);
                }
            }
        });
    }


    @Test
    @Ignore // TODO: fix HttpMultipartContentParser
    public void testBreaksOnBoundary() throws Exception {
        testBreaksCsvImportAt(17);
    }

    @Test
    public void testNoBreaksOnBoundary() throws Exception {
        testBreaksCsvImportAt(0);
    }

    private void testBreaksCsvImportAt(int breakAt) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (HttpMultipartContentParser multipartContentParser = new HttpMultipartContentParser(new HttpHeaderParser(1024, pool))) {
                String boundaryToken = "--------------------------27d997ca93d2689d";
                String boundary = "\r\n" + boundaryToken;
                final String content = boundaryToken + "\r\n" +
                        "Content-Disposition: form-data; name=\"data\"; filename=\"fhv_tripdata_2017-02.csv\"\r\n" +
                        "\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        boundary + "--";
                final String expected =
                        "Content-Disposition: form-data; name=\"data\"; filename=\"fhv_tripdata_2017-02.csv\"\r\n" +
                                "\r\n" +
                                "B00014,2017-02-01 00:45:00,,,\r\n" +
                                "\r\n" +
                                "-----------------------------\r\n";

                int len = content.length();
                long p = TestUtils.toMemory(content);
                try {
                    long pBoundary = TestUtils.toMemory(boundary);
                    DirectByteCharSequence boundaryCs = new DirectByteCharSequence().of(pBoundary, pBoundary + boundary.length());
                    try {
                        multipartContentParser.clear();
                        multipartContentParser.of(boundaryCs);
                        boolean result = multipartContentParser.parse(p, p + len - breakAt, LISTENER);
                        if (breakAt > 0) {
                            result = multipartContentParser.parse(p + len - breakAt + 1, p + len, LISTENER);
                        }
                        Assert.assertEquals(expected, sink.toString());
                        Assert.assertTrue(result);
                    } finally {
                        Unsafe.free(pBoundary, boundary.length());
                    }
                } finally {
                    Unsafe.free(p, len);
                }
            }
        });
    }


    private static class TestHttpMultipartContentListener implements HttpMultipartContentListener {
        @Override
        public void onChunk(long lo, long hi) {
            for (long p = lo; p < hi; p++) {
                sink.put((char) Unsafe.getUnsafe().getByte(p));
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