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

package com.nfsdb.net;

import com.nfsdb.misc.Os;
import com.nfsdb.test.tools.TestUtils;
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
                GZipWritableChannel<FileChannel> gzip = new GZipWritableChannel<FileChannel>().of(out)
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