/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 *
 ******************************************************************************/

package com.nfsdb;

import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.NumericException;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Dates;
import com.nfsdb.misc.Files;
import com.nfsdb.std.DirectCharSequence;
import com.nfsdb.std.Path;
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
        Path path = new Path();
        File f = temporaryFolder.newFile();
        Assert.assertTrue(Files.touch(path.of(f.getAbsolutePath())));
        long t = Dates.parseDateTime("2015-10-17T10:00:00.000Z");
        Assert.assertTrue(Files.setLastModified(path, t));
        Assert.assertEquals(t, Files.getLastModified(path));
    }

    @Test
    public void testWrite() throws Exception {
        Path path = new Path();
        File f = temporaryFolder.newFile();
        long fd = Files.openRW(path.of(f.getAbsolutePath()));
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
