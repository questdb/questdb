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

package com.nfsdb.net.ha;

import com.nfsdb.JournalMode;
import com.nfsdb.ex.JournalException;
import com.nfsdb.misc.Rnd;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.net.ha.comsumer.VariableColumnDeltaConsumer;
import com.nfsdb.net.ha.producer.VariableColumnDeltaProducer;
import com.nfsdb.std.DirectInputStream;
import com.nfsdb.store.MemoryFile;
import com.nfsdb.store.VariableColumn;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.OutputStream;

public class VariableColumnTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private MemoryFile file;
    private MemoryFile file2;
    private MemoryFile indexFile;
    private MemoryFile indexFile2;
    private MockByteChannel channel;

    @After
    public void cleanup() {
        file.close();
        file2.close();
        indexFile.close();
        indexFile2.close();
    }

    @Before
    public void setUp() throws JournalException {
        file = new MemoryFile(new File(temporaryFolder.getRoot(), "col.d"), 22, JournalMode.APPEND);
        // it is important to keep bit hint small, so that file2 has small buffers. This would made test go via both pathways.
        // large number will result in tests not covering all of execution path.
        file2 = new MemoryFile(new File(temporaryFolder.getRoot(), "col2.d"), 18, JournalMode.APPEND);
        indexFile = new MemoryFile(new File(temporaryFolder.getRoot(), "col.i"), 22, JournalMode.APPEND);
        indexFile2 = new MemoryFile(new File(temporaryFolder.getRoot(), "col2.i"), 18, JournalMode.APPEND);
        channel = new MockByteChannel();
    }

    @Test
    public void testConsumerEqualToProducer() throws Exception {
        VariableColumn col1 = new VariableColumn(file, indexFile);
        VariableColumn col2 = new VariableColumn(file2, indexFile2);
        VariableColumnDeltaProducer producer = new VariableColumnDeltaProducer(col1);

        int max = 150000;

        for (int i = 0; i < max; i++) {
            col1.putStr("test123" + (max - i));
            col1.commit();
        }

        for (int i = 0; i < max; i++) {
            col2.putStr("test123" + (max - i));
            col2.commit();
        }

        producer.configure(col2.size(), col1.size());
        Assert.assertFalse(producer.hasContent());
        Assert.assertEquals(col1.size(), col2.size());

        for (int i = 0; i < max; i++) {
            Assert.assertEquals("test123" + (max - i), col2.getStr(i));
        }
    }

    @Test
    public void testConsumerLargerThanProducer() throws Exception {
        VariableColumn col1 = new VariableColumn(file, indexFile);
        VariableColumn col2 = new VariableColumn(file2, indexFile2);
        VariableColumnDeltaProducer producer = new VariableColumnDeltaProducer(col1);

        int max = 150000;

        for (int i = 0; i < max - 50000; i++) {
            col1.putStr("test123" + (max - i));
            col1.commit();
        }

        for (int i = 0; i < max; i++) {
            col2.putStr("test123" + (max - i));
            col2.commit();
        }

        producer.configure(col2.size(), col1.size());
        Assert.assertFalse(producer.hasContent());
    }

    @Test
    public void testConsumerReset() throws Exception {
        VariableColumn col1 = new VariableColumn(file, indexFile);
        VariableColumn col2 = new VariableColumn(file2, indexFile2);
        ChannelConsumer consumer = new VariableColumnDeltaConsumer(col2);
        VariableColumnDeltaProducer producer = new VariableColumnDeltaProducer(col1);

        int max = 150000;

        for (int i = 0; i < max; i++) {
            col1.putStr("test123" + (max - i));
            col1.commit();
        }

        for (int i = 0; i < max - 50000; i++) {
            col2.putStr("test123" + (max - i));
            col2.commit();
        }

        producer.configure(col2.size(), col1.size());
        Assert.assertTrue(producer.hasContent());
        producer.write(channel);
        consumer.read(channel);
        col2.commit();

        Assert.assertEquals(col1.size(), col2.size());

        for (int i = 0; i < 1000; i++) {
            col1.putStr("test123" + (max + 1000 - i));
            col1.commit();
        }

        producer.configure(col2.size(), col1.size());
        Assert.assertTrue(producer.hasContent());
        producer.write(channel);
        consumer.read(channel);
        col2.commit();

        Assert.assertEquals(col1.size(), col2.size());

        for (int i = 0; i < max; i++) {
            Assert.assertEquals("test123" + (max - i), col2.getStr(i));
        }

        for (int i = 0; i < 1000; i++) {
            Assert.assertEquals("test123" + (max + 1000 - i), col2.getStr(i + max));
        }
    }

    @Test
    public void testConsumerSmallerThanProducer() throws Exception {
        VariableColumn col1 = new VariableColumn(file, indexFile);
        VariableColumn col2 = new VariableColumn(file2, indexFile2);
        ChannelConsumer consumer = new VariableColumnDeltaConsumer(col2);
        VariableColumnDeltaProducer producer = new VariableColumnDeltaProducer(col1);

        int max = 150000;

        for (int i = 0; i < max; i++) {
            col1.putStr("test123" + (max - i));
            col1.commit();
        }

        for (int i = 0; i < max - 50000; i++) {
            col2.putStr("test123" + (max - i));
            col2.commit();
        }

        producer.configure(col2.size(), col1.size());
        Assert.assertTrue(producer.hasContent());
        producer.write(channel);
        consumer.read(channel);
        col2.commit();

        Assert.assertEquals(col1.size(), col2.size());

        for (int i = 0; i < max; i++) {
            Assert.assertEquals("test123" + (max - i), col2.getStr(i));
        }
    }

    @Test
    public void testCopyBinaryColumnData() throws Exception {
        int bitHint = 8;
        try (MemoryFile smallFile = new MemoryFile(new File(temporaryFolder.getRoot(), "small.d"), bitHint, JournalMode.APPEND)) {
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

                long readAddress = Unsafe.getUnsafe().allocateMemory(readLen);
                readStream.copyTo(readAddress, offset, readLen);
                for (int i = 0; i < readLen; i++) {
                    byte expected = (byte) ((offset + i) % 255);
                    byte actual = Unsafe.getUnsafe().getByte(readAddress + i);
                    Assert.assertEquals(String.format("difference at index %d with read offset %d", i, offset), expected, actual);
                }
                Unsafe.getUnsafe().freeMemory(readAddress);
            }
        }
    }

    @Test
    public void testEmptyConsumerAndPopulatedProducer() throws Exception {
        VariableColumn col1 = new VariableColumn(file, indexFile);
        VariableColumn col2 = new VariableColumn(file2, indexFile2);
        ChannelConsumer consumer = new VariableColumnDeltaConsumer(col2);
        VariableColumnDeltaProducer producer = new VariableColumnDeltaProducer(col1);

        int max = 150000;

        for (int i = 0; i < max; i++) {
            col1.putStr("test123" + (max - i));
            col1.commit();
        }

        producer.configure(col2.size(), col1.size());
        Assert.assertTrue(producer.hasContent());
        producer.write(channel);
        consumer.read(channel);
        col2.commit();

        Assert.assertEquals(col1.size(), col2.size());

        for (int i = 0; i < max; i++) {
            Assert.assertEquals("test123" + (max - i), col2.getStr(i));
        }
    }

    @Test
    public void testEmptyConsumerAndProducer() throws Exception {
        VariableColumn col1 = new VariableColumn(file, indexFile);
        VariableColumn col2 = new VariableColumn(file2, indexFile2);
        VariableColumnDeltaProducer producer = new VariableColumnDeltaProducer(col1);

        producer.configure(col2.size(), col1.size());
        Assert.assertFalse(producer.hasContent());
        Assert.assertEquals(col1.size(), col2.size());
    }

    @Test
    public void testNulls() throws Exception {
        VariableColumn col1 = new VariableColumn(file, indexFile);
        VariableColumn col2 = new VariableColumn(file2, indexFile2);

        ChannelConsumer consumer = new VariableColumnDeltaConsumer(col2);
        VariableColumnDeltaProducer producer = new VariableColumnDeltaProducer(col1);

        col1.putNull();
        col1.commit();

        producer.configure(col2.size(), col1.size());
        Assert.assertTrue(producer.hasContent());
        producer.write(channel);
        consumer.read(channel);
        col2.commit();

        Assert.assertEquals(1, col1.size());
        Assert.assertEquals(1, col2.size());
    }

    @Test
    public void testReadBinaryColumnData() throws Exception {
        int bitHint = 8;
        try (MemoryFile smallFile = new MemoryFile(new File(temporaryFolder.getRoot(), "small.d"), bitHint, JournalMode.APPEND)) {
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