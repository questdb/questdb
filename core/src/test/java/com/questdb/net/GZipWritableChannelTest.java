/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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
 ******************************************************************************/

package com.questdb.net;

import com.questdb.misc.Os;
import com.questdb.test.tools.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.GZIPInputStream;

public class GZipWritableChannelTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testGzip() throws Exception {

        File expected = new File(GZipWritableChannelTest.class.getResource("/large.csv").getFile());
        File compressed = temp.newFile();
        try (
                FileInputStream fis = new FileInputStream(expected);
                FileOutputStream fos = new FileOutputStream(compressed);
                FileChannel in = fis.getChannel();
                FileChannel out = fos.getChannel();
                GZipWritableChannel<FileChannel> gzip = new GZipWritableChannel<FileChannel>(64 * 1024).of(out)
        ) {
            ByteBuffer buf = in.map(FileChannel.MapMode.READ_ONLY, 0, fis.available());
            gzip.write(buf);
            gzip.flush();
        }

        File actual = temp.newFile();
        try (
                GZIPInputStream is = new GZIPInputStream(new FileInputStream(compressed));
                FileOutputStream fos = new FileOutputStream(actual)
        ) {
            byte[] buf = new byte[16 * 1024];

            int l;

            while ((l = is.read(buf)) > 0) {
                fos.write(buf, 0, l);
            }
        }

        TestUtils.assertEquals(expected, actual);
    }

    static {
        Os.init();
    }
}