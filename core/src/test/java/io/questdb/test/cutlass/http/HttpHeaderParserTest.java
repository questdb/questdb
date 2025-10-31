/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cutlass.http;

import io.questdb.cutlass.http.HttpCookie;
import io.questdb.cutlass.http.HttpException;
import io.questdb.cutlass.http.HttpHeaderParser;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjectPool;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;

public class HttpHeaderParserTest {

    private static final ObjectPool<DirectUtf8String> pool = new ObjectPool<>(DirectUtf8String.FACTORY, 64);
    private static final String request = "GET /status?x=1&a=%26b&c&d=x HTTP/1.1\r\n" +
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
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(10 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Test
    public void testContentDisposition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "Content-Disposition: form-data; name=\"hello\"\r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), false, false);
                TestUtils.assertEquals("hello", hp.getContentDispositionName());
                Assert.assertNull(hp.getContentDispositionFilename());
            } finally {
                Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
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
                hp.parse(p, p + v.length(), false, false);
                TestUtils.assertEquals("hello", hp.getContentDispositionName());
                TestUtils.assertEquals("xyz.dat", hp.getContentDispositionFilename());
                TestUtils.assertEquals("form-data", hp.getContentDisposition());
            } finally {
                Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
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
                hp.parse(p, p + v.length(), false, false);
                Assert.fail();
            } catch (HttpException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Malformed Content-Disposition header");
            } finally {
                Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
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
                hp.parse(p, p + v.length(), false, false);
                Assert.fail();
            } catch (HttpException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "missing value [key=name]");
            } finally {
                Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
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
                hp.parse(p, p + v.length(), false, false);
                Assert.fail();
            } catch (HttpException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "unclosed quote");
            } finally {
                Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
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
                hp.parse(p, p + v.length(), false, false);
                TestUtils.assertEquals("hello", hp.getContentDispositionName());
            } finally {
                Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
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
                hp.parse(p, p + v.length(), false, false);
                TestUtils.assertEquals("hello", hp.getContentDispositionName());
                TestUtils.assertEquals("form-data", hp.getContentDisposition());
                Assert.assertNull(hp.getContentDispositionFilename());
            } finally {
                Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testContentLengthLarge() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "Content-Length: 81136060058\r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), false, false);
                Assert.assertEquals(81136060058L, hp.getContentLength());
            } finally {
                Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testContentTypeAndCharset() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "Content-Type: text/html; charset=utf-8\r\n" +
                    "\r\n";
            assertContentType(v, "text/html", "utf-8", null);
        });
    }

    @Test
    public void testContentTypeAndCharsetAndDanglingBoundary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "Content-Type: text/html; charset=utf-8; \r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), false, false);
                Assert.fail();
            } catch (HttpException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Malformed Content-Type header");
            } finally {
                Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);

            }
        });
    }

    @Test
    public void testContentTypeAndUnknown() {
        String v = "Content-Type: text/html; encoding=abc\r\n" +
                "\r\n";
        assertContentType(v, "text/html", null, null);
    }

    @Test
    public void testContentTypeBoundaryAndUnknown() {
        String v = "Content-Type: text/html; boundary=----WebKitFormBoundaryQ3pdBTBXxEFUWDML; encoding=abc\r\n" +
                "\r\n";
        assertContentType(v, "text/html", null, "\r\n------WebKitFormBoundaryQ3pdBTBXxEFUWDML");
    }

    @Test
    public void testContentTypeBoundaryQuoted() {
        String v = "Content-Type: multipart/mixed; boundary=\"gc0pJq0M:08jU5\\\"34c0p\"\r\n" +
                "\r\n";
        assertContentType(v, "multipart/mixed", null, "\r\n--gc0pJq0M:08jU5\"34c0p");
    }

    @Test
    public void testContentTypeSemantics() {
        assertContentType("Content-Type:   text/html \r\n\r\n", "text/html", null, null);
        assertContentType("Content-Type:  text/html ; charset = utf-8\r\n\r\n", "text/html", "utf-8", null);
        assertContentType("Content-Type: application/problem+json; charset = \"utf-8\" ; boundary = \"a;\\\"\\\\b\" \r\n\r\n", "application/problem+json", "utf-8", "\r\n--a;\"\\b");
    }

    private void assertContentType(@NotNull String header, @NotNull String expectedType, String expectedCharset, String expectedBoundary) {
        long p = TestUtils.toMemory(header);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + header.length(), false, false);
            TestUtils.assertEquals(expectedType, hp.getContentType());
            if (expectedBoundary != null) {
                TestUtils.assertEquals(expectedBoundary, hp.getBoundary());
            }
            if (expectedCharset != null) {
                TestUtils.assertEquals(expectedCharset, hp.getCharset());
            }
        } finally {
            Unsafe.free(p, header.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testContentTypeNoCharset() {
        String v = "Content-Type: text/html\r\n" +
                "\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), false, false);
            TestUtils.assertEquals("text/html", hp.getContentType());
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCookieError() {
        assertMalformedCookieIgnored(
                "Set-Cookie: =123; Domain=hello.com; Path=/; Secure; Partitioned; HttpOnly; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n"
        );

        assertMalformedCookieIgnored(
                "Set-Cookie: 123; Domain=hello.com; Path=/; Secure; Partitioned; HttpOnly; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n"
        );

        assertMalformedCookieIgnored(
                "Set-Cookie: ; Domain=hello.com; Path=/; Secure; Partitioned; HttpOnly; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n"
        );

        assertMalformedCookieIgnored(
                "Set-Cookie: HttpOnly; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n"
        );

        assertMalformedCookieIgnored(
                "Set-Cookie: =; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n"
        );

        assertMalformedCookieIgnored(
                "Set-Cookie: something\r\n"
        );
    }

    @Test
    public void testCookieIgnoresUnknownAttribute() {
        StringSink sink = new StringSink();
        // unknown attribute, boolean - starts with 'P' (along known alphabet)
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", domain=\"hello.com\", path=\"/\", secure=true, httpOnly=true, partitioned=false, expires=1445412480000000, maxAge=1234545, sameSite=\"strict\"}]",
                "Set-Cookie: a=b; Domain=hello.com; Path=/; Secure; Part?; HttpOnly; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // Path missing '='
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", domain=\"hello.com\", secure=false, httpOnly=false, partitioned=false, expires=1445412480000000, maxAge=1234545, sameSite=\"strict\"}]",
                "Set-Cookie: a=b; Domain=hello.com; Path/; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // another Path variation
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", domain=\"hello.com\", secure=false, httpOnly=false, partitioned=false, expires=1445412480000000, maxAge=1234545, sameSite=\"strict\"}]",
                "Set-Cookie: a=b; Domain=hello.com; Ph=/; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // prefixed "Path" with alphabet
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", domain=\"hello.com\", secure=true, httpOnly=true, partitioned=false, expires=1445412480000000, maxAge=1234545, sameSite=\"strict\"}]",
                "Set-Cookie: a=b; Domain=hello.com; dPath=/; Secure; HttpOnly; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // prefixed "Path" with non-alphabet
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", domain=\"hello.com\", secure=true, httpOnly=true, partitioned=false, expires=1445412480000000, maxAge=1234545, sameSite=\"strict\"}]",
                "Set-Cookie: a=b; Domain=hello.com; xPath=/; Secure; HttpOnly; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // unknown attribute, boolean - starts with 'D'  (along known alphabet)
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", path=\"/\", secure=true, httpOnly=true, partitioned=false, expires=1445412480000000, maxAge=1234545, sameSite=\"strict\"}]",
                "Set-Cookie: a=b; Do4main=hello.com; Path=/; Secure; Part?; HttpOnly; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // prefixed "Domain" with alphabet
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", path=\"/\", secure=true, httpOnly=true, partitioned=false, expires=1445412480000000, maxAge=1234545, sameSite=\"strict\"}]",
                "Set-Cookie: a=b; sDomain=hello.com; Path=/; Secure; Part?; HttpOnly; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // prefixed "Domain" with nont-alphabet
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", path=\"/\", secure=true, httpOnly=true, partitioned=false, expires=1445412480000000, maxAge=1234545, sameSite=\"strict\"}]",
                "Set-Cookie: a=b; xDomain=hello.com; Path=/; Secure; Part?; HttpOnly; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // unknown attribute, boolean - starts with 'S'  (along known alphabet)
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", domain=\"hello.com\", path=\"/\", secure=false, httpOnly=true, partitioned=false, expires=1445412480000000, maxAge=1234545, sameSite=\"strict\"}]",
                "Set-Cookie: a=b; Domain=hello.com; Path=/; Secre; HttpOnly; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // SameSite is malformed
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", secure=false, httpOnly=false, partitioned=false, expires=1445412480000000, maxAge=0}]",
                "Set-Cookie: a=b; SameSitestrict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // SameSite is prefixed with alphabet
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", secure=false, httpOnly=false, partitioned=false, expires=1445412480000000, maxAge=0}]",
                "Set-Cookie: a=b; pSameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // SameSite is prefixed with non-alphabet
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", secure=false, httpOnly=false, partitioned=false, expires=1445412480000000, maxAge=0}]",
                "Set-Cookie: a=b; xSameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // another variation with S
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", secure=false, httpOnly=false, partitioned=false, expires=1445412480000000, maxAge=0}]",
                "Set-Cookie: a=b; Sam; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // unknown attribute, boolean - starts with 'H'  (along known alphabet)
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", domain=\"hello.com\", path=\"/\", secure=false, httpOnly=false, partitioned=false, expires=1445412480000000, maxAge=1234545, sameSite=\"strict\"}]",
                "Set-Cookie: a=b; Domain=hello.com; Path=/; Htt; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // prefixed HttpOnly with alphabet
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", domain=\"hello.com\", path=\"/\", secure=false, httpOnly=false, partitioned=false, expires=1445412480000000, maxAge=1234545, sameSite=\"strict\"}]",
                "Set-Cookie: a=b; Domain=hello.com; Path=/; dHttpOnly; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // prefixed HttpOnly with non-alphabet
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", domain=\"hello.com\", path=\"/\", secure=false, httpOnly=false, partitioned=false, expires=1445412480000000, maxAge=1234545, sameSite=\"strict\"}]",
                "Set-Cookie: a=b; Domain=hello.com; Path=/; dHttpOnly; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // unknown attribute, boolean - starts with 'M'  (along known alphabet)
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", domain=\"hello.com\", secure=false, httpOnly=false, partitioned=false, expires=1445412480000000, maxAge=0, sameSite=\"strict\"}]",
                "Set-Cookie: a=b; Domain=hello.com; Max=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // Max-Age is invalid number
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", domain=\"hello.com\", secure=false, httpOnly=false, partitioned=false, expires=1445412480000000, maxAge=0, sameSite=\"strict\"}]",
                "Set-Cookie: a=b; Domain=hello.com; Max-Age=hello; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // Max-Age is prefixed with alphabet
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", domain=\"hello.com\", secure=false, httpOnly=false, partitioned=false, expires=1445412480000000, maxAge=0, sameSite=\"strict\"}]",
                "Set-Cookie: a=b; Domain=hello.com; sMax-Age=10; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // Max-Age is prefixed with non-alphabet
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", domain=\"hello.com\", secure=false, httpOnly=false, partitioned=false, expires=1445412480000000, maxAge=0, sameSite=\"strict\"}]",
                "Set-Cookie: a=b; Domain=hello.com; xMax-Age=10; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // unknown attribute, boolean - starts with 'E' (along known alphabet)
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", secure=false, httpOnly=false, partitioned=false, expires=-1, maxAge=0}]",
                "Set-Cookie: a=b; ExpiresWed, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // Expires is prefixed with alphabet
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", secure=true, httpOnly=false, partitioned=false, expires=-1, maxAge=0}]",
                "Set-Cookie: a=b; hExpires=Wed, 21 Oct 2015 07:28:00 GMT; secure\r\n",
                sink
        );

        // Expires is prefixed with non-alphabet
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", secure=true, httpOnly=false, partitioned=false, expires=-1, maxAge=0}]",
                "Set-Cookie: a=b; xExpires=Wed, 21 Oct 2015 07:28:00 GMT; secure\r\n",
                sink
        );

        // Expires is prefixed with non-alphabet
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", secure=false, httpOnly=false, partitioned=true, expires=-1, maxAge=0}]",
                "Set-Cookie: a=b; xExpires=Wed, 21 Oct 2015 07:28:00 GMT; partitioned\r\n",
                sink
        );

        assertCookies(
                "[{cookieName=\"a\", value=\"b\", secure=false, httpOnly=true, partitioned=false, expires=-1, maxAge=0}]",
                "Set-Cookie: a=b; xExpires=Wed, 21 Oct 2015 07:28:00 GMT; httponly\r\n",
                sink
        );

        // malformed date in "Expires" attribute
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", secure=false, httpOnly=false, partitioned=false, expires=-1, maxAge=0}]",
                "Set-Cookie: a=b; Expires=ok, 21 Oct 2015 07:28:00 GMT\r\n",
                sink
        );

        // unknown attribute "ZT", boolean (unknown alphabet)
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", secure=false, httpOnly=false, partitioned=false, expires=-1, maxAge=0}]",
                "Set-Cookie: a=b; ZT\r\n",
                sink
        );

        // unknown attribute "ZT with value
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", secure=false, httpOnly=false, partitioned=false, expires=-1, maxAge=0}]",
                "Set-Cookie: a=b; ZT=123\r\n",
                sink
        );

        // no attributes
        assertCookies(
                "[{cookieName=\"a\", value=\"b\", secure=false, httpOnly=false, partitioned=false, expires=-1, maxAge=0}]",
                "Set-Cookie: a=b\r\n",
                sink
        );
    }

    @Test
    public void testCookiesNameHasKeywordSecure() {
        String v = "GET /ok?x=a&y=b+c&z=123 HTTP/1.1\r\n" +
                "Set-Cookie: Secure=123; Domain=hello.com; Path=/; Secure; Partitioned; HttpOnly; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n" +
                "\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            HttpCookie cookie = hp.getCookie(new Utf8String("Secure"));
            Assert.assertNotNull(cookie);
            TestUtils.assertEquals("123", cookie.value);
            TestUtils.assertEquals("hello.com", cookie.domain);
            TestUtils.assertEquals("/", cookie.path);
            Assert.assertTrue(cookie.secure);
            Assert.assertTrue(cookie.partitioned);
            Assert.assertTrue(cookie.httpOnly);
            Assert.assertEquals(1234545, cookie.maxAge);
            TestUtils.assertEquals("strict", cookie.sameSite);
            Assert.assertEquals(1445412480000000L, cookie.expires);

        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCookiesUnrecognisedAttribute() {
        // SameSite
        assertCaseInsensitivity("GET /ok?x=a&y=b+c&z=123 HTTP/1.1\r\n" +
                "Set-Cookie: _gh_sess=HSVQNiqqkeSqpG%2B7x9fBrnGqXk4nI%2BW2j9BITSM7WLy53vJNNeFqLpfiDH9TyA%2BUa%2FX3%2FrfzQgidqybd36Lh9wsADt3GQP2VQh7pBSAlsGicsqSe2oYK9%2F2y1K3L8gCiYDNtSNk4zdBsTYNLRG72D82X2JvK3ArL79zLkBg6qys45Fou39r33iNH9DxfCisqGS2zvDw0MiJ2H%2FzVD85GB7iXeuznThBI107uPHLJxzpgUAgqj4gLr8ocbDgkFeBuiiWHYRaT9b4wZmHIHMnDz%2BU0Pu45spvs6PLSvCoePzpIazmAqvVvvh5SQ1hqZuCn5ffl3x777xHiUU9z--akaypyDbIgToO26U--D%2BUL6IEkoc6dkRNuUkoosQ%3D%3D; Path=/; Secure; HttpOnly; saMesite=Lax\r\n" +
                "\r\n");

        // Secure
        assertCaseInsensitivity("GET /ok?x=a&y=b+c&z=123 HTTP/1.1\r\n" +
                "Set-Cookie: _gh_sess=HSVQNiqqkeSqpG%2B7x9fBrnGqXk4nI%2BW2j9BITSM7WLy53vJNNeFqLpfiDH9TyA%2BUa%2FX3%2FrfzQgidqybd36Lh9wsADt3GQP2VQh7pBSAlsGicsqSe2oYK9%2F2y1K3L8gCiYDNtSNk4zdBsTYNLRG72D82X2JvK3ArL79zLkBg6qys45Fou39r33iNH9DxfCisqGS2zvDw0MiJ2H%2FzVD85GB7iXeuznThBI107uPHLJxzpgUAgqj4gLr8ocbDgkFeBuiiWHYRaT9b4wZmHIHMnDz%2BU0Pu45spvs6PLSvCoePzpIazmAqvVvvh5SQ1hqZuCn5ffl3x777xHiUU9z--akaypyDbIgToO26U--D%2BUL6IEkoc6dkRNuUkoosQ%3D%3D; Path=/; secuRe; HttpOnly; SameSite=Lax\r\n" +
                "\r\n");

        // Path
        assertCaseInsensitivity("GET /ok?x=a&y=b+c&z=123 HTTP/1.1\r\n" +
                "Set-Cookie: _gh_sess=HSVQNiqqkeSqpG%2B7x9fBrnGqXk4nI%2BW2j9BITSM7WLy53vJNNeFqLpfiDH9TyA%2BUa%2FX3%2FrfzQgidqybd36Lh9wsADt3GQP2VQh7pBSAlsGicsqSe2oYK9%2F2y1K3L8gCiYDNtSNk4zdBsTYNLRG72D82X2JvK3ArL79zLkBg6qys45Fou39r33iNH9DxfCisqGS2zvDw0MiJ2H%2FzVD85GB7iXeuznThBI107uPHLJxzpgUAgqj4gLr8ocbDgkFeBuiiWHYRaT9b4wZmHIHMnDz%2BU0Pu45spvs6PLSvCoePzpIazmAqvVvvh5SQ1hqZuCn5ffl3x777xHiUU9z--akaypyDbIgToO26U--D%2BUL6IEkoc6dkRNuUkoosQ%3D%3D; PATH=/; secure; HttpOnly; SameSite=Lax\r\n" +
                "\r\n");
    }

    @Test
    public void testCookiesVanilla() {
        assertCookieVanilla(
                "GET /ok?x=a&y=b+c&z=123 HTTP/1.1\r\n" +
                        "Set-Cookie: id=123; Domain=hello.com; Path=/; Secure; Partitioned; HttpOnly; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n" +
                        "\r\n"
        );
        // reorder the cookie attributes to make sure they don't affect each other
        assertCookieVanilla(
                "GET /ok?x=a&y=b+c&z=123 HTTP/1.1\r\n" +
                        "Set-Cookie: id=123; Expires=Wed, 21 Oct 2015 07:28:00 GMT; Path=/; Secure; Partitioned; HttpOnly; Max-Age=1234545; SameSite=strict; Domain=hello.com\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testCookiesWithTwoDigitYear() {
        // HTTP 1.0 format with 2-digit year (e.g., Mon, 20-Oct-25 15:57:56 GMT)
        String v = "GET /ok HTTP/1.1\r\n" +
                "Set-Cookie: sessionid=abc123; Domain=example.com; Path=/; Expires=Mon, 20-Oct-25 15:57:56 GMT\r\n" +
                "\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            HttpCookie cookie = hp.getCookie(new Utf8String("sessionid"));
            Assert.assertNotNull(cookie);
            TestUtils.assertEquals("abc123", cookie.value);
            TestUtils.assertEquals("example.com", cookie.domain);
            TestUtils.assertEquals("/", cookie.path);
            Assert.assertEquals(1760975876000000L, cookie.expires);
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCookiesWithAnsiCFormat() {
        // ANSI C asctime format (e.g., Mon Oct 20 15:57:56 2025)
        String v = "GET /ok HTTP/1.1\r\n" +
                "Set-Cookie: token=xyz789; Domain=test.org; Path=/api; Expires=Sun Nov  6 08:49:37 1994\r\n" +
                "\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            HttpCookie cookie = hp.getCookie(new Utf8String("token"));
            Assert.assertNotNull(cookie);
            TestUtils.assertEquals("xyz789", cookie.value);
            TestUtils.assertEquals("test.org", cookie.domain);
            TestUtils.assertEquals("/api", cookie.path);
            Assert.assertEquals(784111777000000L, cookie.expires);
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCookiesWithTheSameKeys() {
        // reorder the cookie attributes to make sure they don't affect each other
        assertPreferredCookie(
                "GET /ok?x=a&y=b+c&z=123 HTTP/1.1\r\n" +
                        "Set-Cookie: id=124; Expires=Wed, 21 Oct 2015 07:28:00 GMT; Path=/aaaa; Secure; Partitioned; HttpOnly; Max-Age=1234545; SameSite=strict; Domain=hello.com\r\n" +
                        "Set-Cookie: id=123; Expires=Wed, 21 Oct 2015 07:28:00 GMT; Path=/; Secure; Partitioned; HttpOnly; Max-Age=1234545; SameSite=strict; Domain=hello.com\r\n" +
                        "\r\n",
                1445412480000000L
        );

        assertPreferredCookie(
                "GET /ok?x=a&y=b+c&z=123 HTTP/1.1\r\n" +
                        "Set-Cookie: id=123; Expires=Wed, 21 Oct 2015 07:28:00 GMT; Path=/; Secure; Partitioned; HttpOnly; Max-Age=1234545; SameSite=strict; Domain=hello.com\r\n" +
                        "Set-Cookie: id=124; Expires=Wed, 21 Oct 2015 07:28:00 GMT; Path=/aaaa; Secure; Partitioned; HttpOnly; Max-Age=1234545; SameSite=strict; Domain=hello.com\r\n" +
                        "\r\n",
                1445412480000000L
        );

        assertPreferredCookie(
                "GET /ok?x=a&y=b+c&z=123 HTTP/1.1\r\n" +
                        "Set-Cookie: id=123; Expires=Wed, 21 Oct 2015 07:28:00 GMT; Path=/aaaa; Secure; Partitioned; HttpOnly; Max-Age=1234545; SameSite=strict; Domain=hello.com\r\n" +
                        "Set-Cookie: id=124; Expires=Wed, 21 Oct 2017 07:28:00 GMT; Path=/aaaa; Secure; Partitioned; HttpOnly; Max-Age=1234545; SameSite=strict; Domain=hello.com\r\n" +
                        "\r\n",
                1508570880000000L
        );

        assertPreferredCookie(
                "GET /ok?x=a&y=b+c&z=123 HTTP/1.1\r\n" +
                        "Set-Cookie: id=124; Expires=Wed, 21 Oct 2017 07:28:00 GMT; Path=/aaaa; Secure; Partitioned; HttpOnly; Max-Age=1234545; SameSite=strict; Domain=hello.com\r\n" +
                        "Set-Cookie: id=123; Expires=Wed, 21 Oct 2015 07:28:00 GMT; Path=/aaaa; Secure; Partitioned; HttpOnly; Max-Age=1234545; SameSite=strict; Domain=hello.com\r\n" +
                        "\r\n",
                1508570880000000L
        );
    }

    @Test
    public void testDanglingUrlParamWithoutValue() {
        String request = "GET /status?accept HTTP/1.1\r\n" +
                "Host: localhost:9000\r\n" +
                "\r\n";
        try (HttpHeaderParser hp = new HttpHeaderParser(4 * 1024, pool)) {
            long p = TestUtils.toMemory(request);
            try {
                hp.parse(p, p + request.length(), true, false);
                Assert.assertNull(hp.getUrlParam(new Utf8String("accept")));
            } finally {
                Unsafe.free(p, request.length(), MemoryTag.NATIVE_DEFAULT);
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
            hp.parse(p, p + v.length(), true, false);
            Assert.fail();
        } catch (HttpException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "header is too large");
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testMethodTooLarge() {
        String v = "GET /xyzadadadjlkjqeljqasdqweqeasdasdasdawqeadadsqweqeweqdadsasdadadasdadasdqadqw HTTP/1.1";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(64, pool)) {
            hp.parse(p, p + v.length(), true, false);
            Assert.fail();
        } catch (HttpException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "url is too long");
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testProtocolLine() {
        String v = "HTTP/3 200 OK\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), false, true);
            TestUtils.assertEquals("200", hp.getStatusCode());
            TestUtils.assertEquals("OK", hp.getStatusText());
            TestUtils.assertEquals("HTTP/3 200 OK", hp.getProtocolLine());
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testProtocolLineFuzz() {
        String v = "HTTP/3 200 OK\r\n";
        long p = TestUtils.toMemory(v);
        long s1 = System.currentTimeMillis();
        long s2 = System.nanoTime();
        Rnd rnd = new Rnd(s1, s2);
        int steps = rnd.nextInt(v.length()) + 1;
        int stepMax = rnd.nextInt(v.length() / steps + 1) + 1;
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            int total = 0;
            for (int i = 0; i < steps; i++) {
                int n = rnd.nextInt(stepMax);
                hp.parse(p + total, p + total + n, false, true);
                total += n;
            }
            if (total < v.length()) {
                hp.parse(p + total, p + v.length(), false, true);
            }
            TestUtils.assertEquals("200", hp.getStatusCode());
            TestUtils.assertEquals("OK", hp.getStatusText());
            TestUtils.assertEquals("HTTP/3 200 OK", hp.getProtocolLine());
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testQueryDanglingEncoding() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String v = "GET /status?x=1&a=% HTTP/1.1\r\n" +
                    "\r\n";
            long p = TestUtils.toMemory(v);
            try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
                hp.parse(p, p + v.length(), true, false);
                Assert.fail();
            } catch (HttpException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid query encoding");
            } finally {
                Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
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
                hp.parse(p, p + v.length(), true, false);
                Assert.fail();
            } catch (HttpException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid query encoding");
            } finally {
                Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
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
                    hp.parse(p, p + i, true, false);
                    Assert.assertTrue(hp.isIncomplete());
                    hp.parse(p + i, p + n, true, false);
                    assertHeaders(hp);
                }
            } finally {
                Unsafe.free(p, request.length(), MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testUrlNoQuery() {
        String v = "GET /xyz HTTP/1.1";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            TestUtils.assertEquals("/xyz", hp.getUrl());
            Assert.assertNull(hp.getQuery());
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUrlParamSingleQuote() {
        String v = "GET /ip?x=%27a%27&y==b HTTP/1.1\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            TestUtils.assertEquals("'a'", hp.getUrlParam(new Utf8String("x")));
            TestUtils.assertEquals("b", hp.getUrlParam(new Utf8String("y")));
            TestUtils.assertEquals("x=%27a%27&y==b", hp.getQuery());
            TestUtils.assertEquals("GET /ip?x=%27a%27&y==b HTTP/1.1", hp.getMethodLine());
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUrlParamsDecode() {
        String v = "GET /test?x=a&y=b+c%26&z=ab%20ba&w=2 HTTP/1.1\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            TestUtils.assertEquals("a", hp.getUrlParam(new Utf8String("x")));
            TestUtils.assertEquals("b c&", hp.getUrlParam(new Utf8String("y")));
            TestUtils.assertEquals("ab ba", hp.getUrlParam(new Utf8String("z")));
            TestUtils.assertEquals("2", hp.getUrlParam(new Utf8String("w")));
            TestUtils.assertEquals("x=a&y=b+c%26&z=ab%20ba&w=2", hp.getQuery());
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUrlParamsDecodeSpace() {
        String v = "GET /ok?x=a&y=b+c&z=123 HTTP/1.1\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            TestUtils.assertEquals("a", hp.getUrlParam(new Utf8String("x")));
            TestUtils.assertEquals("b c", hp.getUrlParam(new Utf8String("y")));
            TestUtils.assertEquals("123", hp.getUrlParam(new Utf8String("z")));
            TestUtils.assertEquals("x=a&y=b+c&z=123", hp.getQuery());
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUrlParamsDecodeTrailingSpace() {
        String v = "GET /xyz?x=a&y=b+c HTTP/1.1\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            TestUtils.assertEquals("a", hp.getUrlParam(new Utf8String("x")));
            TestUtils.assertEquals("b c", hp.getUrlParam(new Utf8String("y")));
            TestUtils.assertEquals("x=a&y=b+c", hp.getQuery());
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUrlParamsDuplicateAmp() {
        String v = "GET /query?x=a&&y==b HTTP/1.1\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            TestUtils.assertEquals("a", hp.getUrlParam(new Utf8String("x")));
            TestUtils.assertEquals("b", hp.getUrlParam(new Utf8String("y")));
            TestUtils.assertEquals("x=a&&y==b", hp.getQuery());
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUrlParamsSimple() {
        String v = "GET /query?x=a&y=b HTTP/1.1\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            TestUtils.assertEquals("a", hp.getUrlParam(new Utf8String("x")));
            TestUtils.assertEquals("b", hp.getUrlParam(new Utf8String("y")));
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUrlParamsTrailingEmpty() {
        String v = "GET /ip?x=a&y=b&z= HTTP/1.1\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            TestUtils.assertEquals("a", hp.getUrlParam(new Utf8String("x")));
            TestUtils.assertEquals("b", hp.getUrlParam(new Utf8String("y")));
            Assert.assertNull(hp.getUrlParam(new Utf8String("z")));
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUrlParamsTrailingNull() {
        String v = "GET /opi?x=a&y=b& HTTP/1.1\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            TestUtils.assertEquals("a", hp.getUrlParam(new Utf8String("x")));
            TestUtils.assertEquals("b", hp.getUrlParam(new Utf8String("y")));
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWrite() {
        try (HttpHeaderParser hp = new HttpHeaderParser(4 * 1024, pool)) {
            long p = TestUtils.toMemory(request);
            try {
                hp.parse(p, p + request.length(), true, false);
                assertHeaders(hp);
            } finally {
                Unsafe.free(p, request.length(), MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private static void assertCaseInsensitivity(String request) {
        long p = TestUtils.toMemory(request);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + request.length(), true, false);
            HttpCookie cookie = hp.getCookie(new Utf8String("_gh_sess"));
            Assert.assertNotNull(cookie);
            TestUtils.assertEquals("HSVQNiqqkeSqpG%2B7x9fBrnGqXk4nI%2BW2j9BITSM7WLy53vJNNeFqLpfiDH9TyA%2BUa%2FX3%2FrfzQgidqybd36Lh9wsADt3GQP2VQh7pBSAlsGicsqSe2oYK9%2F2y1K3L8gCiYDNtSNk4zdBsTYNLRG72D82X2JvK3ArL79zLkBg6qys45Fou39r33iNH9DxfCisqGS2zvDw0MiJ2H%2FzVD85GB7iXeuznThBI107uPHLJxzpgUAgqj4gLr8ocbDgkFeBuiiWHYRaT9b4wZmHIHMnDz%2BU0Pu45spvs6PLSvCoePzpIazmAqvVvvh5SQ1hqZuCn5ffl3x777xHiUU9z--akaypyDbIgToO26U--D%2BUL6IEkoc6dkRNuUkoosQ%3D%3D", cookie.value);
            Assert.assertNull(cookie.domain);
            TestUtils.assertEquals("/", cookie.path);
            Assert.assertTrue(cookie.secure);
            Assert.assertFalse(cookie.partitioned);
            Assert.assertTrue(cookie.httpOnly);
            Assert.assertEquals(0, cookie.maxAge);
            TestUtils.assertEquals("Lax", cookie.sameSite);
            Assert.assertEquals(-1, cookie.expires);

        } finally {
            Unsafe.free(p, request.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertCookieVanilla(String v) {
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            HttpCookie cookie = hp.getCookie(new Utf8String("id"));
            Assert.assertNotNull(cookie);
            TestUtils.assertEquals("123", cookie.value);
            TestUtils.assertEquals("hello.com", cookie.domain);
            TestUtils.assertEquals("/", cookie.path);
            Assert.assertTrue(cookie.secure);
            Assert.assertTrue(cookie.partitioned);
            Assert.assertTrue(cookie.httpOnly);
            Assert.assertEquals(1234545, cookie.maxAge);
            TestUtils.assertEquals("strict", cookie.sameSite);
            Assert.assertEquals(1445412480000000L, cookie.expires);

        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertCookies(CharSequence expected, String response, StringSink sink) {
        sink.clear();
        response = "GET /ok?x=a&y=b+c&z=123 HTTP/1.1\r\n" + response + "\r\n";
        long p = TestUtils.toMemory(response);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + response.length(), true, false);
            hp.getCookieList().toSink(sink);
            TestUtils.assertEquals(expected, sink);
        } finally {
            Unsafe.free(p, response.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertMalformedCookieIgnored(String malformedCookie) {
        String v = "GET /ok?x=a&y=b+c&z=123 HTTP/1.1\r\n" +
                "Set-Cookie: a=123; Domain=hello.com; Path=/; Secure; Partitioned; HttpOnly; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n" +
                malformedCookie +
                "Set-Cookie: b=123; Domain=hello.com; Path=/; Secure; Partitioned; HttpOnly; Max-Age=1234545; SameSite=strict; Expires=Wed, 21 Oct 2015 07:28:00 GMT\r\n" +
                "\r\n";
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            Assert.assertEquals(2, hp.getCookieList().size());
            Assert.assertNotNull(hp.getCookie(new Utf8String("a")));
            Assert.assertNotNull(hp.getCookie(new Utf8String("b")));
            Assert.assertEquals(1, hp.getIgnoredCookieCount());
        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertPreferredCookie(String v, long expiresTimestamp) {
        long p = TestUtils.toMemory(v);
        try (HttpHeaderParser hp = new HttpHeaderParser(1024, pool)) {
            hp.parse(p, p + v.length(), true, false);
            HttpCookie cookie = hp.getCookie(new Utf8String("id"));
            Assert.assertNotNull(cookie);
            TestUtils.assertEquals("124", cookie.value);
            TestUtils.assertEquals("hello.com", cookie.domain);
            TestUtils.assertEquals("/aaaa", cookie.path);
            Assert.assertTrue(cookie.secure);
            Assert.assertTrue(cookie.partitioned);
            Assert.assertTrue(cookie.httpOnly);
            Assert.assertEquals(1234545, cookie.maxAge);
            TestUtils.assertEquals("strict", cookie.sameSite);
            Assert.assertEquals(expiresTimestamp, cookie.expires);

        } finally {
            Unsafe.free(p, v.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertHeaders(HttpHeaderParser hp) {
        Assert.assertFalse(hp.isIncomplete());
        TestUtils.assertEquals("GET", hp.getMethod());
        TestUtils.assertEquals("/status", hp.getUrl());
        TestUtils.assertEquals("GET /status?x=1&a=%26b&c&d=x HTTP/1.1", hp.getMethodLine());
        TestUtils.assertEquals("x=1&a=%26b&c&d=x", hp.getQuery());
        Assert.assertEquals(9, hp.size());
        TestUtils.assertEquals("localhost:9000", hp.getHeader(new Utf8String("Host")));
        TestUtils.assertEquals("keep-alive", hp.getHeader(new Utf8String("Connection")));
        TestUtils.assertEquals("max-age=0", hp.getHeader(new Utf8String("Cache-Control")));
        TestUtils.assertEquals("text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8", hp.getHeader(new Utf8String("Accept")));
        TestUtils.assertEquals("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36", hp.getHeader(new Utf8String("User-Agent")));
        TestUtils.assertEquals("multipart/form-data; boundary=----WebKitFormBoundaryQ3pdBTBXxEFUWDML", hp.getHeader(new Utf8String("Content-Type")));
        TestUtils.assertContains(hp.getBoundary().asAsciiCharSequence(), "----WebKitFormBoundaryQ3pdBTBXxEFUWDML");
        TestUtils.assertEquals("gzip,deflate,sdch", hp.getHeader(new Utf8String("Accept-Encoding")));
        TestUtils.assertEquals("en-US,en;q=0.8", hp.getHeader(new Utf8String("Accept-Language")));
        TestUtils.assertEquals("textwrapon=false; textautoformat=false; wysiwyg=textarea", hp.getHeader(new Utf8String("Cookie")));
        TestUtils.assertEquals("1", hp.getUrlParam(new Utf8String("x")));
        TestUtils.assertEquals("&b", hp.getUrlParam(new Utf8String("a")));
        Assert.assertNull(hp.getUrlParam(new Utf8String("c")));
        Assert.assertNull(hp.getHeader(new Utf8String("merge_copy_var_column")));
    }
}
