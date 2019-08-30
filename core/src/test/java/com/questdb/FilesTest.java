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

package com.questdb;

import com.questdb.std.*;
import com.questdb.std.str.DirectCharSequence;
import com.questdb.std.str.NativeLPSZ;
import com.questdb.std.str.Path;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class FilesTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static void touch(File file) throws IOException {
        FileOutputStream fos = new FileOutputStream(file);
        fos.close();
    }

    @Test
    public void testDeleteDir2() throws Exception {
        File r = temporaryFolder.newFolder("to_delete");
        Assert.assertTrue(new File(r, "a/b/c").mkdirs());
        Assert.assertTrue(new File(r, "d/e/f").mkdirs());
        touch(new File(r, "d/1.txt"));
        touch(new File(r, "a/b/2.txt"));
        try (Path path = new Path().of(r.getAbsolutePath()).$()) {
            Assert.assertTrue(Files.rmdir(path));
            Assert.assertFalse(r.exists());
        }
    }

    @Test
    public void testDeleteOpenFile() throws Exception {
        try (Path path = new Path()) {
            File f = temporaryFolder.newFile();
            long fd = Files.openRW(path.of(f.getAbsolutePath()).$());
            Assert.assertTrue(Files.exists(fd));
            Assert.assertTrue(Files.remove(path));
            Assert.assertFalse(Files.exists(fd));
            Files.close(fd);
        }
    }

    @Test
    public void testLastModified() throws IOException, NumericException {
        try (Path path = new Path()) {
            assertLastModified(path, DateFormatUtils.parseDateTime("2015-10-17T10:00:00.000Z"));
            assertLastModified(path, 122222212222L);
        }
    }

    private void assertLastModified(Path path, long t) throws IOException {
        File f = temporaryFolder.newFile();
        Assert.assertTrue(Files.touch(path.of(f.getAbsolutePath()).$()));
        Assert.assertTrue(Files.setLastModified(path, t));
        Assert.assertEquals(t, Files.getLastModified(path));
    }

    @Test
    public void testListDir() {
        String temp = temporaryFolder.getRoot().getAbsolutePath();
        ObjList<String> names = new ObjList<>();
        try (Path path = new Path().of(temp).$()) {
            try (Path cp = new Path()) {
                Assert.assertTrue(Files.touch(cp.of(temp).concat("a.txt").$()));
                NativeLPSZ name = new NativeLPSZ();
                long pFind = Files.findFirst(path);
                Assert.assertTrue(pFind != 0);
                try {
                    do {
                        names.add(name.of(Files.findName(pFind)).toString());
                    } while (Files.findNext(pFind) > 0);
                } finally {
                    Files.findClose(pFind);
                }
            }
        }

        names.sort(Chars::compare);

        Assert.assertEquals("[.,..,a.txt]", names.toString());
    }

    @Test
    public void testListNonExistingDir() {
        String temp = temporaryFolder.getRoot().getAbsolutePath();
        try (Path path = new Path().of(temp).concat("xyz")) {
            long pFind = Files.findFirst(path);
            Assert.assertEquals(0, pFind);
        }
    }

    @Test
    public void testMkdirs() throws Exception {
        File r = temporaryFolder.newFolder("to_delete");
        try (Path path = new Path().of(r.getAbsolutePath())) {
            path.concat("a").concat("b").concat("c").concat("f.text").$();
            Assert.assertEquals(0, Files.mkdirs(path, 509));
        }

        try (Path path = new Path().of(r.getAbsolutePath())) {
            path.concat("a").concat("b").concat("c").$();
            Assert.assertTrue(Files.exists(path));
        }
    }

    @Test
    public void testRemove() throws Exception {
        try (Path path = new Path().of(temporaryFolder.newFile().getAbsolutePath()).$()) {
            Assert.assertTrue(Files.touch(path));
            Assert.assertTrue(Files.exists(path));
            Assert.assertTrue(Files.remove(path));
            Assert.assertFalse(Files.exists(path));
        }
    }

    @Test
    public void testTruncate() throws Exception {
        File temp = temporaryFolder.newFile();
        TestUtils.writeStringToFile(temp, "abcde");
        try (Path path = new Path().of(temp.getAbsolutePath()).$()) {
            Assert.assertTrue(Files.exists(path));
            Assert.assertEquals(5, Files.length(path));

            long fd = Files.openRW(path);
            try {
                Files.truncate(fd, 3);
                Assert.assertEquals(3, Files.length(path));
                Files.truncate(fd, 0);
                Assert.assertEquals(0, Files.length(path));
            } finally {
                Files.close(fd);
            }
        }
    }

    @Test
    public void testWrite() throws Exception {
        try (Path path = new Path()) {
            File f = temporaryFolder.newFile();
            long fd = Files.openRW(path.of(f.getAbsolutePath()).$());
            try {
                Assert.assertTrue(fd > 0);

                ByteBuffer buf = ByteBuffer.allocateDirect(1024).order(ByteOrder.LITTLE_ENDIAN);
                try {
                    ByteBuffers.putStr(buf, "hello from java");
                    int len = buf.position();
                    Assert.assertEquals(len, Files.write(fd, ByteBuffers.getAddress(buf), len, 0));

                    buf.clear();

                    ByteBuffers.putStr(buf, ", awesome");
                    Files.write(fd, ByteBuffers.getAddress(buf), buf.position(), len);
                } finally {
                    ByteBuffers.release(buf);
                }
            } finally {
                Files.close(fd);
            }

            fd = Files.openRO(path);
            try {
                Assert.assertTrue(fd > 0);
                ByteBuffer buf = ByteBuffer.allocateDirect(1024).order(ByteOrder.LITTLE_ENDIAN);
                try {
                    int len = (int) Files.length(path);
                    long ptr = ByteBuffers.getAddress(buf);
                    Assert.assertEquals(48, Files.read(fd, ptr, len, 0));
                    DirectCharSequence cs = new DirectCharSequence().of(ptr, ptr + len);
                    TestUtils.assertEquals("hello from java, awesome", cs);
                } finally {
                    ByteBuffers.release(buf);
                }
            } finally {
                Files.close(fd);
            }

            Assert.assertTrue(Files.exists(path));
            Assert.assertFalse(Files.exists(path.of("/x/yz/1/2/3")));
        }
    }
}
