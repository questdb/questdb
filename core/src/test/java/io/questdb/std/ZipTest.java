/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.std;

import io.questdb.std.ex.FatalError;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
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
            File expected = new File(ZipTest.class.getResource("/zip-test/large.csv").getFile());

            final int available = 64 * 1024;
            long in = Unsafe.malloc(available, MemoryTag.NATIVE_DEFAULT);
            long out = Unsafe.malloc(available / 2, MemoryTag.NATIVE_DEFAULT);
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
                Unsafe.free(in, available, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(out, available / 2, MemoryTag.NATIVE_DEFAULT);
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