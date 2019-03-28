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

package com.questdb.std;

import com.questdb.std.ex.FatalError;
import com.questdb.std.str.Path;
import com.questdb.test.tools.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.zip.GZIPInputStream;

public class ZipTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testGzip() throws Exception {
        try (Path path = new Path()) {
            File outFile = temp.newFile("x");
            File expected = new File(ZipTest.class.getResource("/large.csv").getFile());

            final int available = 64 * 1024;
            long in = Unsafe.malloc(available);
            long out = Unsafe.malloc(available / 2);
            try {
                long strm = Zip.deflateInit();
                try {

                    long pIn = 0;
                    long pOut = 0;
                    long fdIn = Files.openRO(path.of(expected.getAbsolutePath()).$());
                    try {
                        long fdOut = Files.openRW(path.of(outFile.getAbsolutePath()).$());
                        try {
                            // header
                            Files.write(fdOut, Zip.gzipHeader, Zip.gzipHeaderLen, pOut);
                            pOut += Zip.gzipHeaderLen;

                            int len;
                            int crc = 0;
                            while ((len = (int) Files.read(fdIn, in, available, pIn)) > 0) {
                                pIn += len;
                                Zip.setInput(strm, in, len);
                                crc = Zip.crc32(crc, in, len);
                                do {
                                    int ret;
                                    if ((ret = Zip.deflate(strm, out, available, false)) < 0) {
                                        throw new FatalError("Error in deflator: " + ret);
                                    }

                                    int have = available - Zip.availOut(strm);
                                    if (have > 0) {
                                        Files.write(fdOut, out, have, pOut);
                                        pOut += have;
                                    }

                                } while (Zip.availIn(strm) > 0);
                            }

                            int ret;
                            do {
                                if ((ret = Zip.deflate(strm, out, available, true)) < 0) {
                                    throw new FatalError("Error in deflator: " + ret);
                                }

                                int have = available - Zip.availOut(strm);
                                if (have > 0) {
                                    Files.write(fdOut, out, have, pOut);
                                    pOut += have;
                                }
                            } while (ret != 1);

                            // write trailer
                            Unsafe.getUnsafe().putInt(out, crc);
                            Unsafe.getUnsafe().putInt(out + 4, (int) pIn);
                            Files.write(fdOut, out, 8, pOut);
                        } finally {
                            Files.close(fdOut);
                        }
                    } finally {
                        Files.close(fdIn);
                    }
                } finally {
                    Zip.deflateEnd(strm);
                }
            } finally {
                Unsafe.free(in, available);
                Unsafe.free(out, available / 2);
            }


            // ok. read what we produced

            File actual = temp.newFile();


            try (
                    GZIPInputStream is = new GZIPInputStream(new FileInputStream(outFile));
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
}