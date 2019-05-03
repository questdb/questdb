/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cutlass.http;

import com.questdb.std.ObjectPool;
import com.questdb.std.Unsafe;
import com.questdb.std.str.DirectByteCharSequence;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class HttpHeaderParserTest {
    private final static String request = "GET /status?x=1&a=%26b&c&d=x HTTP/1.1\r\n" +
            "Host: localhost:9000\r\n" +
            "Connection: keep-alive\r\n" +
            "Cache-Control: max-age=0\r\n" +
            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
            "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
            "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryQ3pdBTBXxEFUWDML\r\n" +
            "Accept-Encoding: gzip,deflate,sdch\r\n" +
            "Accept-Language: en-US,en;q=0.8\r\n" +
            "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
            "\r\n";

    private static ObjectPool<DirectByteCharSequence> pool = new ObjectPool<>(DirectByteCharSequence.FACTORY, 64);

    @Test
    public void testContentDisposition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "Content-Disposition: form-data; name=\"hello\"\r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), false);
                TestUtils.assertEquals("hello", hp.getContentDispositionName());
                Assert.assertNull(hp.getContentDispositionFilename());
            } finally {
                Unsafe.free(p, v.length());
            }
        });
    }

    @Test
    public void testContentDispositionAndFileName() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "Content-Disposition: form-data; name=\"hello\"; filename=\"xyz.dat\"\r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), false);
                TestUtils.assertEquals("hello", hp.getContentDispositionName());
                TestUtils.assertEquals("xyz.dat", hp.getContentDispositionFilename());
                TestUtils.assertEquals("form-data", hp.getContentDisposition());
            } finally {
                Unsafe.free(p, v.length());
            }
        });
    }

    @Test
    public void testContentDispositionDangling() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "Content-Disposition: form-data; name=\"hello\";\r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), false);
                Assert.fail();
            } catch (HttpException e) {
                TestUtils.assertContains(e.getMessage(), "Malformed Content-Disposition header");
            } finally {
                Unsafe.free(p, v.length());
            }
        });
    }

    @Test
    public void testContentDispositionMissingValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "Content-Disposition: form-data; name=\r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), false);
                Assert.fail();
            } catch (HttpException e) {
                TestUtils.assertContains(e.getMessage(), "missing value [key=name]");
            } finally {
                Unsafe.free(p, v.length());
            }
        });
    }

    @Test
    public void testContentDispositionUnclosedQuote() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "Content-Disposition: form-data; name=\"hello\r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), false);
                Assert.fail();
            } catch (HttpException e) {
                TestUtils.assertContains(e.getMessage(), "unclosed quote");
            } finally {
                Unsafe.free(p, v.length());
            }
        });
    }

    @Test
    public void testContentDispositionUnknown() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "Content-Disposition: form-data; name=\"hello\"; tag=xyz\r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), false);
                TestUtils.assertEquals("hello", hp.getContentDispositionName());
            } finally {
                Unsafe.free(p, v.length());
            }
        });
    }

    @Test
    public void testContentDispositionUnquoted() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "Content-Disposition: form-data; name=hello\r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), false);
                TestUtils.assertEquals("hello", hp.getContentDispositionName());
                TestUtils.assertEquals("form-data", hp.getContentDisposition());
                Assert.assertNull(hp.getContentDispositionFilename());
            } finally {
                Unsafe.free(p, v.length());
            }
        });
    }

    @Test
    public void testContentTypeAndCharset() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "Content-Type: text/html; charset=utf-8\r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), false);
                TestUtils.assertEquals("text/html", hp.getContentType());
                TestUtils.assertEquals("utf-8", hp.getCharset());
            } finally {
                Unsafe.free(p, v.length());
            }
        });
    }

    @Test
    public void testContentTypeAndCharsetAndDanglingBoundary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "Content-Type: text/html; charset=utf-8; \r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), false);
                Assert.fail();
            } catch (HttpException e) {
                TestUtils.assertContains(e.getMessage(), "Malformed Content-Type header");
            } finally {
                Unsafe.free(p, v.length());

            }
        });
    }

    @Test
    public void testContentTypeAndUnknown() {
        String v = "Content-Type: text/html; encoding=abc\r\n" +
                "\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), false);
            TestUtils.assertEquals("text/html", hp.getContentType());
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testContentTypeBoundaryAndUnknown() {
        String v = "Content-Type: text/html; boundary=----WebKitFormBoundaryQ3pdBTBXxEFUWDML; encoding=abc\r\n" +
                "\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), false);
            TestUtils.assertEquals("text/html", hp.getContentType());
            TestUtils.assertEquals("\r\n------WebKitFormBoundaryQ3pdBTBXxEFUWDML", hp.getBoundary());
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testContentTypeNoCharset() {
        String v = "Content-Type: text/html\r\n" +
                "\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), false);
            TestUtils.assertEquals("text/html", hp.getContentType());
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testDanglingUrlParamWithoutValue() {
        String request = "GET /status?accept HTTP/1.1\r\n" +
                "Host: localhost:9000\r\n" +
                "\r\n";
        try (HttpHeaderParser hp = new HttpHeaderParser(4 * 1024, pool)) {
            long p = TestUtils.toMemory(request);
            try {
                hp.parse(p, p + request.length(), true);
                Assert.assertNull(hp.getUrlParam("accept"));
            } finally {
                Unsafe.free(p, request.length());
            }
        }
    }

    @Test
    public void testHeaderTooLarge() {
        String v = "GET /ok?x=a&y=b+c&z=123 HTTP/1.1\r\n" +
                "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryQ3pdBTBXxEFUWDML\r\n" +
                "\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(64, pool)) {
            hp.parse(p, p + v.length(), true);
            Assert.fail();
        } catch (HttpException e) {
            TestUtils.assertContains(e.getMessage(), "header is too large");
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testMethodTooLarge() {
        String v = "GET /xyzadadadjlkjqeljqasdqweqeasdasdasdawqeadadsqweqeweqdadsasdadadasdadasdqadqw HTTP/1.1";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(64, pool)) {
            hp.parse(p, p + v.length(), true);
            Assert.fail();
        } catch (HttpException e) {
            TestUtils.assertContains(e.getMessage(), "url is too long");
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testQueryDanglingEncoding() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "GET /status?x=1&a=% HTTP/1.1\r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), true);
                Assert.fail();
            } catch (HttpException e) {
                TestUtils.assertContains(e.getMessage(), "invalid query encoding");
            } finally {
                Unsafe.free(p, v.length());
            }
        });
    }

    @Test
    public void testQueryInvalidEncoding() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "GET /status?x=1&a=%i6b&c&d=x HTTP/1.1\r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), true);
                Assert.fail();
            } catch (HttpException e) {
                TestUtils.assertContains(e.getMessage(), "invalid query encoding");
            } finally {
                Unsafe.free(p, v.length());
            }
        });
    }

    @Test
    public void testSplitWrite() {
        try (HttpHeaderParser hp = new HttpHeaderParser(4 * 1024, pool)) {
            long p = TestUtils.toMemory(request);
            try {
                for (int i = 0, n = request.length(); i < n; i++) {
                    hp.clear();
                    hp.parse(p, p + i, true);
                    Assert.assertTrue(hp.isIncomplete());
                    hp.parse(p + i, p + n, true);
                    assertHeaders(hp);
                }
            } finally {
                Unsafe.free(p, request.length());
            }
        }
    }

    @Test
    public void testUrlNoQuery() {
        String v = "GET /xyz HTTP/1.1";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true);
            TestUtils.assertEquals("/xyz", hp.getUrl());
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testUrlParamSingleQuote() {
        String v = "GET /ip?x=%27a%27&y==b HTTP/1.1";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true);
            TestUtils.assertEquals("'a'", hp.getUrlParam("x"));
            TestUtils.assertEquals("b", hp.getUrlParam("y"));
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testUrlParamsDecode() {
        String v = "GET /test?x=a&y=b+c%26&z=ab%20ba&w=2 HTTP/1.1";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true);
            TestUtils.assertEquals("a", hp.getUrlParam("x"));
            TestUtils.assertEquals("b c&", hp.getUrlParam("y"));
            TestUtils.assertEquals("ab ba", hp.getUrlParam("z"));
            TestUtils.assertEquals("2", hp.getUrlParam("w"));
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testUrlParamsDecodeSpace() {
        String v = "GET /ok?x=a&y=b+c&z=123 HTTP/1.1";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true);
            TestUtils.assertEquals("a", hp.getUrlParam("x"));
            TestUtils.assertEquals("b c", hp.getUrlParam("y"));
            TestUtils.assertEquals("123", hp.getUrlParam("z"));
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testUrlParamsDecodeTrailingSpace() {
        String v = "GET /xyz?x=a&y=b+c HTTP/1.1";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true);
            TestUtils.assertEquals("a", hp.getUrlParam("x"));
            TestUtils.assertEquals("b c", hp.getUrlParam("y"));
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testUrlParamsDuplicateAmp() {
        String v = "GET /query?x=a&&y==b HTTP/1.1";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true);
            TestUtils.assertEquals("a", hp.getUrlParam("x"));
            TestUtils.assertEquals("b", hp.getUrlParam("y"));
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testUrlParamsSimple() {
        String v = "GET /query?x=a&y=b HTTP/1.1";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true);
            TestUtils.assertEquals("a", hp.getUrlParam("x"));
            TestUtils.assertEquals("b", hp.getUrlParam("y"));
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testUrlParamsTrailingEmpty() {
        String v = "GET /ip?x=a&y=b&z= HTTP/1.1";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true);
            TestUtils.assertEquals("a", hp.getUrlParam("x"));
            TestUtils.assertEquals("b", hp.getUrlParam("y"));
            Assert.assertNull(hp.getUrlParam("z"));
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    //"GET /status?x=1&a=%26b&c&d=x HTTP/1.1\r\n"

    @Test
    public void testUrlParamsTrailingNull() {
        String v = "GET /opi?x=a&y=b& HTTP/1.1";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true);
            TestUtils.assertEquals("a", hp.getUrlParam("x"));
            TestUtils.assertEquals("b", hp.getUrlParam("y"));
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testWrite() {
        try (HttpHeaderParser hp = new HttpHeaderParser(4 * 1024, pool)) {
            long p = TestUtils.toMemory(request);
            try {
                hp.parse(p, p + request.length(), true);
                assertHeaders(hp);
            } finally {
                Unsafe.free(p, request.length());
            }
        }
    }

    private void assertHeaders(HttpHeaderParser hp) {
        Assert.assertFalse(hp.isIncomplete());
        TestUtils.assertEquals("GET", hp.getMethod());
        TestUtils.assertEquals("/status", hp.getUrl());
        TestUtils.assertEquals("GET /status?x=1&a=&b&c&d=x HTTP/1.1", hp.getMethodLine());
        Assert.assertEquals(9, hp.size());
        TestUtils.assertEquals("localhost:9000", hp.getHeader("Host"));
        TestUtils.assertEquals("keep-alive", hp.getHeader("Connection"));
        TestUtils.assertEquals("max-age=0", hp.getHeader("Cache-Control"));
        TestUtils.assertEquals("text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8", hp.getHeader("Accept"));
        TestUtils.assertEquals("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36", hp.getHeader("User-Agent"));
        TestUtils.assertEquals("multipart/form-data; boundary=----WebKitFormBoundaryQ3pdBTBXxEFUWDML", hp.getHeader("Content-Type"));
        TestUtils.assertContains(hp.getBoundary(), "----WebKitFormBoundaryQ3pdBTBXxEFUWDML");
        TestUtils.assertEquals("gzip,deflate,sdch", hp.getHeader("Accept-Encoding"));
        TestUtils.assertEquals("en-US,en;q=0.8", hp.getHeader("Accept-Language"));
        TestUtils.assertEquals("textwrapon=false; textautoformat=false; wysiwyg=textarea", hp.getHeader("Cookie"));
        TestUtils.assertEquals("1", hp.getUrlParam("x"));
        TestUtils.assertEquals("&b", hp.getUrlParam("a"));
        Assert.assertNull(hp.getUrlParam("c"));
        Assert.assertNull(hp.getHeader("xxx"));
    }
}