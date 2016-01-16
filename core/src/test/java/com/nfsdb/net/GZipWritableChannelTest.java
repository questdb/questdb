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

        if (Os.nativelySupported) {
            System.out.println("ok");
        }

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
}