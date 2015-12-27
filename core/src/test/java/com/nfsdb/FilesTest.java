/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb;

import com.nfsdb.collections.DirectCharSequence;
import com.nfsdb.collections.Path;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.NumericException;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Dates;
import com.nfsdb.misc.Files;
import com.nfsdb.misc.Os;
import com.nfsdb.test.tools.TestUtils;
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

    @Test
    public void testDeleteDir() throws Exception {
        File r = temporaryFolder.newFolder("to_delete");
        Assert.assertTrue(new File(r, "a/b/c").mkdirs());
        Assert.assertTrue(new File(r, "d/e/f").mkdirs());
        touch(new File(r, "d/1.txt"));
        touch(new File(r, "a/b/2.txt"));
        Assert.assertTrue(Files.delete(r));
        Assert.assertFalse(r.exists());
    }

    @Test
    public void testLastModified() throws IOException, NumericException {
        if (Os.nativelySupported) {
            Path path = new Path();
            File f = temporaryFolder.newFile();
            Assert.assertTrue(Files.touch(path.of(f.getAbsolutePath())));
            long t = Dates.parseDateTime("2015-10-17T10:00:00.000Z");
            Assert.assertTrue(Files.setLastModified(path, t));
            Assert.assertEquals(t, Files.getLastModified(path));
        }
    }

    @Test
    public void testWrite() throws Exception {
        if (Os.nativelySupported) {
            Path path = new Path();
            File f = temporaryFolder.newFile();
            long fd = Files.openRW(path.of(f.getAbsolutePath()));
            try {
                Assert.assertTrue(fd > 0);

                ByteBuffer buf = ByteBuffer.allocateDirect(1024).order(ByteOrder.LITTLE_ENDIAN);
                try {
                    ByteBuffers.putStr(buf, "hello from java");
                    int len = buf.position();
                    Files.write(fd, ByteBuffers.getAddress(buf), len, 0);

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

    @Test
    public void testWriteStringToFile() throws IOException, JournalException {
        File f = temporaryFolder.newFile();
        Files.writeStringToFile(f, "TEST123");
        Assert.assertEquals("TEST123", Files.readStringFromFile(f));
    }

    private static void touch(File file) throws IOException {
        FileOutputStream fos = new FileOutputStream(file);
        fos.close();
    }
}
