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

import com.questdb.std.Chars;
import com.questdb.std.FilesFacade;
import com.questdb.std.FilesFacadeImpl;
import com.questdb.std.Os;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.Path;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class MimeTypesCacheTest {
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