/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.net.http;

import com.nfsdb.misc.Unsafe;
import com.nfsdb.std.DirectByteCharSequence;
import com.nfsdb.std.ObjectPool;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class RequestHeaderBufferTest {
    private final static String request = "GET /status?x=1&a=%26b HTTP/1.1\r\n" +
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

    @Test
    public void testSplitWrite() throws Exception {
        ObjectPool<DirectByteCharSequence> pool = new ObjectPool<>(DirectByteCharSequence.FACTORY, 64);
        try (RequestHeaderBuffer hb = new RequestHeaderBuffer(4 * 1024, pool)) {
            long p = TestUtils.toMemory(request);
            try {
                for (int i = 0, n = request.length(); i < n; i++) {
                    hb.clear();
                    hb.write(p, i, true);
                    hb.write(p + i, n - i, true);
                    assertHeaders(hb);
                }
            } finally {
                Unsafe.getUnsafe().freeMemory(p);
            }
        }
    }

    @Test
    public void testWrite() throws Exception {
        ObjectPool<DirectByteCharSequence> pool = new ObjectPool<>(DirectByteCharSequence.FACTORY, 64);
        try (RequestHeaderBuffer hb = new RequestHeaderBuffer(4 * 1024, pool)) {
            long p = TestUtils.toMemory(request);
            try {
                hb.write(p, request.length(), true);
                assertHeaders(hb);
            } finally {
                Unsafe.getUnsafe().freeMemory(p);
            }
        }
    }

    private void assertHeaders(RequestHeaderBuffer hb) {
        TestUtils.assertEquals("GET", hb.getMethod());
        TestUtils.assertEquals("/status", hb.getUrl());
        Assert.assertEquals(9, hb.size());
        TestUtils.assertEquals("localhost:9000", hb.get("Host"));
        TestUtils.assertEquals("keep-alive", hb.get("Connection"));
        TestUtils.assertEquals("max-age=0", hb.get("Cache-Control"));
        TestUtils.assertEquals("text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8", hb.get("Accept"));
        TestUtils.assertEquals("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36", hb.get("User-Agent"));
        TestUtils.assertEquals("multipart/form-data; boundary=----WebKitFormBoundaryQ3pdBTBXxEFUWDML", hb.get("Content-Type"));
        TestUtils.assertEquals("gzip,deflate,sdch", hb.get("Accept-Encoding"));
        TestUtils.assertEquals("en-US,en;q=0.8", hb.get("Accept-Language"));
        TestUtils.assertEquals("textwrapon=false; textautoformat=false; wysiwyg=textarea", hb.get("Cookie"));
        TestUtils.assertEquals("1", hb.getUrlParam("x"));
        TestUtils.assertEquals("&b", hb.getUrlParam("a"));
        Assert.assertNull(hb.get("xxx"));
    }
}