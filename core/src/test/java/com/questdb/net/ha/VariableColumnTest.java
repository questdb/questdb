/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.net.ha;

import com.questdb.JournalMode;
import com.questdb.ex.JournalException;
import com.questdb.net.ha.comsumer.VariableColumnDeltaConsumer;
import com.questdb.net.ha.producer.VariableColumnDeltaProducer;
import com.questdb.store.MemoryFile;
import com.questdb.store.VariableColumn;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;

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
        file = new MemoryFile(new File(temporaryFolder.getRoot(), "col.d"), 22, JournalMode.APPEND, false);
        // it is important to keep bit hint small, so that file2 has small buffers. This would made test go via both pathways.
        // large number will result in tests not covering all of execution path.
        file2 = new MemoryFile(new File(temporaryFolder.getRoot(), "col2.d"), 18, JournalMode.APPEND, false);
        indexFile = new MemoryFile(new File(temporaryFolder.getRoot(), "col.i"), 22, JournalMode.APPEND, false);
        indexFile2 = new MemoryFile(new File(temporaryFolder.getRoot(), "col2.i"), 18, JournalMode.APPEND, false);
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
}