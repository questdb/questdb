/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.ha;

import com.nfsdb.JournalMode;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.ha.comsumer.VariableColumnDeltaConsumer;
import com.nfsdb.ha.producer.VariableColumnDeltaProducer;
import com.nfsdb.storage.MemoryFile;
import com.nfsdb.storage.VariableColumn;
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
        ChannelConsumer consumer = new VariableColumnDeltaConsumer(col2);
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

        consumer.reset();
        producer.configure(col2.size(), col1.size());
        Assert.assertFalse(producer.hasContent());
        Assert.assertEquals(col1.size(), col2.size());
    }

    @Test
    public void testConsumerLargerThanProducer() throws Exception {
        VariableColumn col1 = new VariableColumn(file, indexFile);
        VariableColumn col2 = new VariableColumn(file2, indexFile2);
        ChannelConsumer consumer = new VariableColumnDeltaConsumer(col2);
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

        consumer.reset();
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

        consumer.reset();
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

        consumer.reset();
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

        consumer.reset();
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

        consumer.reset();
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
        ChannelConsumer consumer = new VariableColumnDeltaConsumer(col2);
        VariableColumnDeltaProducer producer = new VariableColumnDeltaProducer(col1);

        consumer.reset();
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

        consumer.reset();
        producer.configure(col2.size(), col1.size());
        Assert.assertTrue(producer.hasContent());
        producer.write(channel);
        consumer.read(channel);
        col2.commit();

        Assert.assertEquals(1, col1.size());
        Assert.assertEquals(1, col2.size());
    }
}