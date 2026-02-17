/*******************************************************************************
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

package io.questdb.test.cutlass.http;

import io.questdb.cutlass.http.HttpServer;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class UrlNormalisationTest {

    private static final DirectUtf8String utf8Url = new DirectUtf8String();

    @Test
    public void testSimple() {
        assertNormalisation("", "");
        assertNormalisation("/", "/");
        assertNormalisation("/", "////");
        assertNormalisation("/a/b/c", "///a/b/c");
        assertNormalisation("/a/", "///a///");
        assertNormalisation("/hello/world/c/", "///hello////////world/c///////");
        assertNormalisation("/correct/path/", "/correct/path/");
        assertNormalisation("/also/correct/path", "/also/correct/path");
        assertNormalisation("relative/path/", "relative/path/////////////");
        assertNormalisation("relative/broken/middle/path", "relative/broken////////////////middle/path");
    }

    private void assertNormalisation(String expected, String url) {
        long ptr = TestUtils.toMemory(url);
        try {
            utf8Url.of(ptr, ptr + url.length());
            TestUtils.assertEquals(expected, HttpServer.normalizeUrl(utf8Url));
        } finally {
            Unsafe.free(ptr, url.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }
}