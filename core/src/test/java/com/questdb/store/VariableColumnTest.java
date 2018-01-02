/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.store;

import com.questdb.std.ByteBuffers;
import com.questdb.std.DirectInputStream;
import com.questdb.std.Rnd;
import com.questdb.std.Unsafe;
import com.questdb.std.ex.JournalException;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class VariableColumnTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private MemoryFile file;
    private MemoryFile file2;
    private MemoryFile indexFile;
    private MemoryFile indexFile2;

    @After
    public void cleanup() {
        file.close();
        file2.close();
        indexFile.close();
        indexFile2.close();
    }

    @Before
    public void setUp() throws JournalException {
        file = new MemoryFile(new File(temporaryFolder.getRoot(), "col.d"), 22, JournalMode.APPEND, false);
        // it is important to keep bit hint small, so that file2 has small buffers. This would made test go via both pathways.
        // large number will result in tests not covering all of execution path.
        file2 = new MemoryFile(new File(temporaryFolder.getRoot(), "col2.d"), 18, JournalMode.APPEND, false);
        indexFile = new MemoryFile(new File(temporaryFolder.getRoot(), "col.i"), 22, JournalMode.APPEND, false);
        indexFile2 = new MemoryFile(new File(temporaryFolder.getRoot(), "col2.i"), 18, JournalMode.APPEND, false);
    }

    @Test
    public void testBin1() throws Exception {

        int N = 1000;
        int SZ = 4096;
        ByteBuffer buf = ByteBuffer.allocateDirect(SZ);
        long addr = ByteBuffers.getAddress(buf);

        try {
            Rnd rnd = new Rnd();

            try (MemoryFile smallFile = new MemoryFile(new File(temporaryFolder.getRoot(), "small.d"), 8, JournalMode.APPEND, false)) {
                try (VariableColumn col = new VariableColumn(smallFile, indexFile)) {

                    for (int i = 0; i < N; i++) {
                        long p = addr;
                        int n = (rnd.nextPositiveInt() % (SZ - 1)) / 8;
                        for (int j = 0; j < n; j++) {
                            Unsafe.getUnsafe().putLong(p, rnd.nextLong());
                            p += 8;
                        }
                        buf.limit(n * 8);
                        col.putBin(buf);
                        col.commit();
                        buf.clear();
                    }

                    Assert.assertEquals(N, col.size());

                    rnd = new Rnd();
                    for (int i = 0; i < N; i++) {

                        long len = col.getBinLen(i);
                        DirectInputStream is = col.getBin(i);
                        is.copyTo(addr, 0, len);

                        long p = addr;
                        int n = (rnd.nextPositiveInt() % (SZ - 1)) / 8;
                        for (int j = 0; j < n; j++) {
                            Assert.assertEquals(rnd.nextLong(), Unsafe.getUnsafe().getLong(p));
                            p += 8;
                        }
                    }
                }
            }
        } finally {
            ByteBuffers.release(buf);
        }
    }

    @Test
    public void testCopyBinaryColumnData() throws Exception {
        int bitHint = 8;
        try (MemoryFile smallFile = new MemoryFile(new File(temporaryFolder.getRoot(), "small.d"), bitHint, JournalMode.APPEND, false)) {
            VariableColumn col1 = new VariableColumn(smallFile, indexFile);

            int max = (int) Math.pow(2, bitHint) * 10 + 1;
            OutputStream writeStream = col1.putBin();
            for (int i = 0; i < max; i++) {
                writeStream.write(i % 255);
            }
            writeStream.flush();
            col1.commit();

            int shift = (int) Math.ceil(max / 4.0);
            for (int offset = 0; offset < max; offset += shift) {
                int readLen = max - offset;
                DirectInputStream readStream = col1.getBin(0);

                long readAddress = Unsafe.malloc(readLen);
                readStream.copyTo(readAddress, offset, readLen);
                for (int i = 0; i < readLen; i++) {
                    byte expected = (byte) ((offset + i) % 255);
                    byte actual = Unsafe.getUnsafe().getByte(readAddress + i);
                    Assert.assertEquals(String.format("difference at index %d with read offset %d", i, offset), expected, actual);
                }
                Unsafe.free(readAddress, readLen);
            }
        }
    }

    @Test
    public void testReadBinaryColumnData() throws Exception {
        int bitHint = 8;
        try (MemoryFile smallFile = new MemoryFile(new File(temporaryFolder.getRoot(), "small.d"), bitHint, JournalMode.APPEND, false)) {
            VariableColumn col1 = new VariableColumn(smallFile, indexFile);

            int max = (int) Math.pow(2, bitHint) * 10 + 1;

            Rnd rnd = new Rnd();
            OutputStream writeStream = col1.putBin();
            for (int i = 0; i < max; i++) {
                writeStream.write(rnd.nextInt());
            }
            writeStream.flush();
            col1.commit();

            DirectInputStream readStream = col1.getBin(0);

            byte b;
            int count = 0;
            Rnd exp = new Rnd();

            while ((b = (byte) readStream.read()) != -1) {
                Assert.assertEquals(String.format("difference at index %d", count), (byte) exp.nextInt(), b);
                count++;
            }
        }
    }
}