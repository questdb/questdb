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

package io.questdb.test.tools;

import io.questdb.std.*;
import org.junit.Assert;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class TestUtils {

    private TestUtils() {
    }

    public static void assertContains(CharSequence _this, CharSequence that) {
        if (Chars.contains(_this, that)) {
            return;
        }
        Assert.fail("\'" + _this.toString() + "\' does not contain: " + that);
    }

    public static void assertEquals(File a, File b) throws IOException {
        try (RandomAccessFile rafA = new RandomAccessFile(a, "r")) {
            try (RandomAccessFile rafB = new RandomAccessFile(b, "r")) {
                try (FileChannel chA = rafA.getChannel()) {
                    try (FileChannel chB = rafB.getChannel()) {
                        Assert.assertEquals(chA.size(), chB.size());
                        ByteBuffer bufA = chA.map(FileChannel.MapMode.READ_ONLY, 0, chA.size());
                        try {
                            ByteBuffer bufB = chB.map(FileChannel.MapMode.READ_ONLY, 0, chB.size());
                            try {
                                long pa = ByteBuffers.getAddress(bufA);
                                long pb = ByteBuffers.getAddress(bufB);
                                long lim = pa + bufA.limit();

                                while (pa + 8 < lim) {
                                    if (Unsafe.getUnsafe().getLong(pa) != Unsafe.getUnsafe().getLong(pb)) {
                                        Assert.fail();
                                    }
                                    pa += 8;
                                    pb += 8;
                                }

                                while (pa < lim) {
                                    if (Unsafe.getUnsafe().getByte(pa++) != Unsafe.getUnsafe().getByte(pb++)) {
                                        Assert.fail();
                                    }
                                }

                            } finally {
                                ByteBuffers.release(bufB);
                            }

                        } finally {
                            ByteBuffers.release(bufA);
                        }
                    }
                }
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

    public static long toMemory(CharSequence sequence) {
        long ptr = Unsafe.malloc(sequence.length());
        Chars.strcpy(sequence, sequence.length(), ptr);
        return ptr;
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
