/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.net.http;

import com.questdb.misc.Unsafe;
import com.questdb.std.DirectByteCharSequence;
import com.questdb.std.ObjectPool;
import com.questdb.test.tools.TestUtils;
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
        TestUtils.assertEquals("GET /status?x=1&a=&b6b HTTP/1.1", hb.getMethodLine());
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