/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MimeTypesCacheTest {

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
                    new MimeTypesCache(FilesFacadeImpl.INSTANCE, path);
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
        testFailure(new FilesFacadeImpl() {
            @Override
            public boolean close(long fd) {
                closeCount.incrementAndGet();
                return true;
            }

            @Override
            public long length(long fd) {
                return 1024;
            }

            @Override
            public long openRO(LPSZ name) {
                return 123L;
            }

            @Override
            public long read(long fd, long buf, long len, long offset) {
                return -1;
            }
        }, "could not read");

        Assert.assertEquals(1, closeCount.get());
    }

    @Test()
    public void testCannotRead2() throws Exception {
        AtomicInteger closeCount = new AtomicInteger(0);
        testFailure(new FilesFacadeImpl() {

            @Override
            public boolean close(long fd) {
                closeCount.incrementAndGet();
                return true;
            }

            @Override
            public long length(long fd) {
                return 1024;
            }

            @Override
            public long openRO(LPSZ name) {
                return 123L;
            }

            @Override
            public long read(long fd, long buf, long len, long offset) {
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
                    if (Os.type == Os.WINDOWS) {
                        filePath = this.getClass().getResource("/mime.types").getFile().substring(1);
                    } else {
                        filePath = this.getClass().getResource("/mime.types").getFile();
                    }
                    path.of(filePath).$();
                    MimeTypesCache mimeTypes = new MimeTypesCache(FilesFacadeImpl.INSTANCE, path);
                    Assert.assertEquals(980, mimeTypes.size());
                    TestUtils.assertEquals("application/andrew-inset", mimeTypes.get("ez"));
                    TestUtils.assertEquals("application/inkml+xml", mimeTypes.get("ink"));
                    TestUtils.assertEquals("application/inkml+xml", mimeTypes.get("inkml"));
                    TestUtils.assertEquals("application/mp21", mimeTypes.get("m21"));
                    TestUtils.assertEquals("application/mp21", mimeTypes.get("mp21"));
                    TestUtils.assertEquals("application/mp4", mimeTypes.get("mp4s"));
                }
            }
        });
    }

    @Test()
    public void testWrongFileSize() throws Exception {
        AtomicInteger closeCount = new AtomicInteger();
        testFailure(new FilesFacadeImpl() {
            @Override
            public long length(long fd) {
                return 0;
            }

            @Override
            public long openRO(LPSZ name) {
                return 123L;
            }

            @Override
            public boolean close(long fd) {
                closeCount.incrementAndGet();
                return true;
            }
        }, "wrong file size");

        Assert.assertEquals(1, closeCount.get());
    }

    @Test()
    public void testWrongFileSize2() throws Exception {
        AtomicInteger closeCount = new AtomicInteger(0);
        testFailure(new FilesFacadeImpl() {
            @Override
            public long length(long fd) {
                return -1;
            }

            @Override
            public long openRO(LPSZ name) {
                return 123L;
            }

            @Override
            public boolean close(long fd) {
                closeCount.incrementAndGet();
                return true;
            }


        }, "wrong file size");

        Assert.assertEquals(1, closeCount.get());
    }

    @Test()
    public void testWrongFileSize4() throws Exception {
        AtomicInteger closeCount = new AtomicInteger();
        testFailure(new FilesFacadeImpl() {
            @Override
            public long length(long fd) {
                return 1024 * 1024 * 2;
            }

            @Override
            public long openRO(LPSZ name) {
                return 123L;
            }

            @Override
            public boolean close(long fd) {
                closeCount.incrementAndGet();
                return true;
            }
        }, "wrong file size");

        Assert.assertEquals(1, closeCount.get());
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
