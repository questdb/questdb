/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cutlass.http.HttpException;
import io.questdb.cutlass.http.MimeTypesCache;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MimeTypesCacheTest extends AbstractTest {

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(10 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Test()
    public void testCannotOpen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                path.of("/tmp/sdrqwhlkkhlkhasdlkahdoiquweoiuweoiqwe.ok").$();
                try {
                    new MimeTypesCache(TestFilesFacadeImpl.INSTANCE, path);
                    Assert.fail();
                } catch (HttpException e) {
                    Assert.assertTrue(Chars.startsWith(e.getMessage(), "could not open"));
                }
            }
        });
    }

    @Test()
    public void testCannotRead1() throws Exception {
        AtomicInteger closeCount = new AtomicInteger(0);
        testFailure(new TestFilesFacadeImpl() {
            @Override
            public boolean close(int fd) {
                closeCount.incrementAndGet();
                return true;
            }

            @Override
            public long length(int fd) {
                return 1024;
            }

            @Override
            public int openRO(LPSZ name) {
                return 123;
            }

            @Override
            public long read(int fd, long buf, long len, long offset) {
                return -1;
            }
        }, "could not read");

        Assert.assertEquals(1, closeCount.get());
    }

    @Test()
    public void testCannotRead2() throws Exception {
        AtomicInteger closeCount = new AtomicInteger(0);
        testFailure(new TestFilesFacadeImpl() {

            @Override
            public boolean close(int fd) {
                closeCount.incrementAndGet();
                return true;
            }

            @Override
            public long length(int fd) {
                return 1024;
            }

            @Override
            public int openRO(LPSZ name) {
                return 123;
            }

            @Override
            public long read(int fd, long buf, long len, long offset) {
                return 128;
            }
        }, "could not read");

        Assert.assertEquals(1, closeCount.get());
    }

    @Test
    public void testSimple() throws Exception {
        TestUtils.assertMemoryLeak(new TestUtils.LeakProneCode() {
            @Override
            public void run() {
                try (Path path = new Path()) {
                    String filePath;
                    if (Os.isWindows()) {
                        filePath = Files.getResourcePath(getClass().getResource("/mime.types")).substring(1);
                    } else {
                        filePath = Files.getResourcePath(getClass().getResource("/mime.types"));
                    }
                    path.of(filePath).$();
                    assertMimeTypes(new MimeTypesCache(TestFilesFacadeImpl.INSTANCE, path));
                }
            }
        });
    }

    @Test
    public void testSimpleResource() throws Exception {
        TestUtils.assertMemoryLeak(new TestUtils.LeakProneCode() {
            @Override
            public void run() throws Exception {
                try (InputStream inputStream = getClass().getResourceAsStream("/mime.types")) {
                    assertMimeTypes(new MimeTypesCache(inputStream));
                }
            }
        });
    }

    @Test()
    public void testWrongFileSize() throws Exception {
        AtomicInteger closeCount = new AtomicInteger();
        testFailure(new TestFilesFacadeImpl() {
            @Override
            public boolean close(int fd) {
                closeCount.incrementAndGet();
                return true;
            }

            @Override
            public long length(int fd) {
                return 0;
            }

            @Override
            public int openRO(LPSZ name) {
                return 123;
            }
        }, "wrong file size");

        Assert.assertEquals(1, closeCount.get());
    }

    @Test()
    public void testWrongFileSize2() throws Exception {
        AtomicInteger closeCount = new AtomicInteger(0);
        testFailure(new TestFilesFacadeImpl() {
            @Override
            public boolean close(int fd) {
                closeCount.incrementAndGet();
                return true;
            }

            @Override
            public long length(int fd) {
                return -1;
            }

            @Override
            public int openRO(LPSZ name) {
                return 123;
            }
        }, "wrong file size");

        Assert.assertEquals(1, closeCount.get());
    }

    @Test()
    public void testWrongFileSize4() throws Exception {
        AtomicInteger closeCount = new AtomicInteger();
        testFailure(new TestFilesFacadeImpl() {
            @Override
            public boolean close(int fd) {
                closeCount.incrementAndGet();
                return true;
            }

            @Override
            public long length(int fd) {
                return 1024 * 1024 * 2;
            }

            @Override
            public int openRO(LPSZ name) {
                return 123;
            }
        }, "wrong file size");

        Assert.assertEquals(1, closeCount.get());
    }

    private static void assertMimeTypes(MimeTypesCache mimeTypes) {
        Assert.assertEquals(981, mimeTypes.size());
        TestUtils.assertEquals("application/andrew-inset", mimeTypes.get("ez"));
        TestUtils.assertEquals("application/inkml+xml", mimeTypes.get("ink"));
        TestUtils.assertEquals("application/inkml+xml", mimeTypes.get("inkml"));
        TestUtils.assertEquals("application/mp21", mimeTypes.get("m21"));
        TestUtils.assertEquals("application/mp21", mimeTypes.get("mp21"));
        TestUtils.assertEquals("application/mp4", mimeTypes.get("mp4s"));
        TestUtils.assertEquals("x-conference/x-cooltalk", mimeTypes.get("ice"));
    }

    private void testFailure(FilesFacade ff, CharSequence startsWith) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                try {
                    new MimeTypesCache(ff, path);
                    Assert.fail();
                } catch (HttpException e) {
                    Assert.assertTrue(Chars.startsWith(e.getMessage(), startsWith));
                }
            }
        });
    }
}
