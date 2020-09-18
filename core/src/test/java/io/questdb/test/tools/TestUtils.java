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

package io.questdb.test.tools;

import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.junit.Assert;

import java.io.*;

public final class TestUtils {

    private TestUtils() {
    }

    public static void assertContains(CharSequence _this, CharSequence that) {
        if (Chars.contains(_this, that)) {
            return;
        }
        Assert.fail("'" + _this.toString() + "' does not contain: " + that);
    }

    public static void assertEquals(File a, File b) {
        try (Path path = new Path()) {
            path.of(a.getAbsolutePath()).$();
            long fda = Files.openRO(path);
            Assert.assertNotEquals(-1, fda);

            try {
                path.of(b.getAbsolutePath()).$();
                long fdb = Files.openRO(path);
                Assert.assertNotEquals(-1, fdb);
                try {

                    Assert.assertEquals(Files.length(fda), Files.length(fdb));

                    long bufa = Unsafe.malloc(4096);
                    long bufb = Unsafe.malloc(4096);

                    long offset = 0;
                    try {

                        while (true) {
                            long reada = Files.read(fda, bufa, 4096, offset);
                            long readb = Files.read(fdb, bufb, 4096, offset);
                            Assert.assertEquals(reada, readb);

                            if (reada == 0) {
                                break;
                            }

                            offset += reada;

                            for (int i = 0; i < reada; i++) {
                                Assert.assertEquals(Unsafe.getUnsafe().getByte(bufa + i), Unsafe.getUnsafe().getByte(bufb + i));
                            }
                        }
                    } finally {
                        Unsafe.free(bufa, 4096);
                        Unsafe.free(bufb, 4096);
                    }
                } finally {
                    Files.close(fdb);
                }
            } finally {
                Files.close(fda);
            }
        }
    }

    public static void assertEquals(File a, CharSequence actual) {
        try (Path path = new Path()) {
            path.of(a.getAbsolutePath()).$();
            long fda = Files.openRO(path);
            Assert.assertNotEquals(-1, fda);

            try {
                long bufa = Unsafe.malloc(4096);
                final long str = toMemory(actual);
                long offset = 0;
                long strp = str;
                try {

                    while (true) {
                        long reada = Files.read(fda, bufa, 4096, offset);

                        if (reada == 0) {
                            break;
                        }

                        for (int i = 0; i < reada; i++) {
                            byte b = Unsafe.getUnsafe().getByte(bufa + i);
                            if (b == 13) {
                                continue;
                            }
                            Assert.assertEquals(b, Unsafe.getUnsafe().getByte(strp++));
                        }

                        offset += reada;
                    }
                } finally {
                    Unsafe.free(bufa, 4096);
                    Unsafe.free(str, actual.length());
                }
            } finally {
                Files.close(fda);
            }
        }
    }

    public static void assertEquals(CharSequence expected, CharSequence actual) {
        if (expected == null && actual == null) {
            return;
        }

        if (expected != null && actual == null) {
            Assert.fail("Expected: \n`" + expected + "`but have NULL");
        }

        if (expected == null) {
            Assert.fail("Expected: NULL but have \n`" + actual + "`\n");
        }

        if (expected.length() != actual.length()) {
            Assert.fail("Expected: \n`" + expected + "`\n but have \n`" + actual + "`\n (length: " + expected.length() + " vs " + actual.length() + ")");
        }
        Assert.assertEquals(expected.length(), actual.length());
        for (int i = 0; i < expected.length(); i++) {
            if (expected.charAt(i) != actual.charAt(i)) {
                Assert.fail("At: " + i + ". Expected: `" + expected + "`, actual: `" + actual + '`');
            }
        }
    }

    public static void assertEquals(BinarySequence bs, BinarySequence actBs, long actualLen) {
        if (bs == null) {
            Assert.assertNull(actBs);
            Assert.assertEquals(-1, actualLen);
        } else {
            Assert.assertEquals(bs.length(), actBs.length());
            Assert.assertEquals(bs.length(), actualLen);
            for (long l = 0, z = bs.length(); l < z; l++) {
                byte b1 = bs.byteAt(l);
                byte b2 = actBs.byteAt(l);
                if (b1 != b2) {
                    Assert.fail("Failed comparison at [" + l + "], expected: " + b1 + ", actual: " + b2);
                }
                Assert.assertEquals(bs.byteAt(l), actBs.byteAt(l));
            }
        }
    }

    public static void assertMemoryLeak(LeakProneCode runnable) throws Exception {
        long mem = Unsafe.getMemUsed();
        long fileCount = Files.getOpenFileCount();
        runnable.run();
        Assert.assertEquals(mem, Unsafe.getMemUsed());
        Assert.assertEquals(fileCount, Files.getOpenFileCount());
    }

    public static void copyMimeTypes(String targetDir) throws IOException {
        try (InputStream stream = TestUtils.class.getResourceAsStream("/site/conf/mime.types")) {
            Assert.assertNotNull(stream);
            final File target = new File(targetDir, "conf/mime.types");
            target.getParentFile().mkdirs();
            try (FileOutputStream fos = new FileOutputStream(target)) {
                byte[] buffer = new byte[1024 * 1204];
                int len;
                while ((len = stream.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
            }
        }
    }

    public static String readStringFromFile(File file) {
        try {
            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] buffer
                        = new byte[(int) fis.getChannel().size()];
                int totalRead = 0;
                int read;
                while (totalRead < buffer.length
                        && (read = fis.read(buffer, totalRead, buffer.length - totalRead)) > 0) {
                    totalRead += read;
                }
                return new String(buffer, Files.UTF_8);
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot read from " + file.getAbsolutePath(), e);
        }
    }

    public static long toMemory(CharSequence sequence) {
        long ptr = Unsafe.malloc(sequence.length());
        Chars.asciiStrCpy(sequence, sequence.length(), ptr);
        return ptr;
    }

    // used in tests
    public static void writeStringToFile(File file, String s) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(s.getBytes(Files.UTF_8));
        }
    }

    @FunctionalInterface
    public interface LeakProneCode {
        void run() throws Exception;
    }
}
