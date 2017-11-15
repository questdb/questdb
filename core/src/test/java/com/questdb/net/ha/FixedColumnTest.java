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

import com.questdb.net.ha.comsumer.FixedColumnDeltaConsumer;
import com.questdb.net.ha.producer.FixedColumnDeltaProducer;
import com.questdb.std.ex.JournalException;
import com.questdb.store.FixedColumn;
import com.questdb.store.JournalMode;
import com.questdb.store.MemoryFile;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public class FixedColumnTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private MemoryFile file;
    private MemoryFile file2;
    private MockByteChannel channel;

    @After
    public void cleanup() {
        file.delete();
        file2.delete();
    }

    @Before
    public void setUp() throws JournalException {
        file = new MemoryFile(new File(temporaryFolder.getRoot(), "col.d"), 22, JournalMode.APPEND, false);
        // it is important to keep bit hint small, so that file2 has small buffers. This would made test go via both pathways.
        // large number will result in tests not covering all of execution path.
        file2 = new MemoryFile(new File(temporaryFolder.getRoot(), "col2.d"), 18, JournalMode.APPEND, false);
        channel = new MockByteChannel();
    }

    @After
    public void tearDown() {
        file.close();
        file2.close();
    }

    @Test
    public void testConsumerEqualToProducer() throws Exception {
        FixedColumn col1 = new FixedColumn(file, 4);
        FixedColumn col2 = new FixedColumn(file2, 4);

        FixedColumnDeltaProducer producer = new FixedColumnDeltaProducer(col1);

        int max = 1500000;

        for (int i = 0; i < max; i++) {
            col1.putInt(max - i);
            col1.commit();
        }

        for (int i = 0; i < max; i++) {
            col2.putInt(max - i);
            col2.commit();
        }

        producer.configure(col2.size(), col1.size());

        // hasNext() can be true, because of compulsory header
        // however, if column doesn't have data, hasContent() must be false.
        Assert.assertFalse(producer.hasContent());
        Assert.assertEquals(col1.size(), col2.size());

        for (int i = 0; i < max; i++) {
            Assert.assertEquals(max - i, col2.getInt(i));
        }
    }

    @Test
    public void testConsumerLargerThanProducer() throws Exception {
        FixedColumn col1 = new FixedColumn(file, 4);
        FixedColumn col2 = new FixedColumn(file2, 4);

        FixedColumnDeltaProducer producer = new FixedColumnDeltaProducer(col1);

        int max = 1500000;

        for (int i = 0; i < max - 500000; i++) {
            col1.putInt(max - i);
            col1.commit();
        }

        for (int i = 0; i < max; i++) {
            col2.putInt(max - i);
            col2.commit();
        }

        producer.configure(col2.size(), col1.size());
        Assert.assertFalse(producer.hasContent());
    }

    @Test
    public void testConsumerReset() throws Exception {
        FixedColumn col1 = new FixedColumn(file, 4);
        FixedColumn col2 = new FixedColumn(file2, 4);

        FixedColumnDeltaProducer producer = new FixedColumnDeltaProducer(col1);
        ChannelConsumer consumer = new FixedColumnDeltaConsumer(col2);

        int max = 1500000;

        for (int i = 0; i < max; i++) {
            col1.putInt(max - i);
            col1.commit();
        }

        for (int i = 0; i < max - 500000; i++) {
            col2.putInt(max - i);
            col2.commit();
        }

        producer.configure(col2.size(), col1.size());
        Assert.assertTrue(producer.hasContent());
        producer.write(channel);
        consumer.read(channel);
        col2.commit();

        Assert.assertEquals(col1.size(), col2.size());

        for (int i = 0; i < 10000; i++) {
            col1.putInt(max + 10000 - i);
            col1.commit();
        }

        producer.configure(col2.size(), col1.size());
        Assert.assertTrue(producer.hasContent());
        producer.write(channel);
        consumer.read(channel);
        col2.commit();

        Assert.assertEquals(col1.size(), col2.size());

        for (int i = 0; i < max; i++) {
            Assert.assertEquals(max - i, col2.getInt(i));
        }

        for (int i = max; i < max + 10000; i++) {
            Assert.assertEquals(max + max + 10000 - i, col2.getInt(i));
        }
    }

    @Test
    public void testConsumerSmallerThanProducer() throws Exception {
        FixedColumn col1 = new FixedColumn(file, 4);
        FixedColumn col2 = new FixedColumn(file2, 4);

        FixedColumnDeltaProducer producer = new FixedColumnDeltaProducer(col1);
        ChannelConsumer consumer = new FixedColumnDeltaConsumer(col2);

        int max = 1500000;

        for (int i = 0; i < max; i++) {
            col1.putInt(max - i);
            col1.commit();
        }

        for (int i = 0; i < max - 500000; i++) {
            col2.putInt(max - i);
            col2.commit();
        }

        producer.configure(col2.size(), col1.size());
        Assert.assertTrue(producer.hasContent());
        producer.write(channel);
        consumer.read(channel);
        col2.commit();

        Assert.assertEquals(col1.size(), col2.size());

        for (int i = 0; i < max; i++) {
            Assert.assertEquals(max - i, col2.getInt(i));
        }
    }

    @Test
    public void testEmptyConsumerAndPopulatedProducer() throws Exception {
        FixedColumn col1 = new FixedColumn(file, 4);
        FixedColumn col2 = new FixedColumn(file2, 4);

        FixedColumnDeltaProducer producer = new FixedColumnDeltaProducer(col1);
        ChannelConsumer consumer = new FixedColumnDeltaConsumer(col2);

        int max = 1500000;

        for (int i = 0; i < max; i++) {
            col1.putInt(max - i);
            col1.commit();
        }

        producer.configure(col2.size(), col1.size());

        // hasNext() can be true, because of compulsory header
        // however, if column doesn't have data, hasContent() must be false.
        Assert.assertTrue(producer.hasContent());
        producer.write(channel);
        consumer.read(channel);
        col2.commit();

        Assert.assertEquals(col1.size(), col2.size());

        for (int i = 0; i < max; i++) {
            Assert.assertEquals(max - i, col2.getInt(i));
        }
    }

    @Test
    public void testEmptyConsumerAndProducer() throws Exception {
        FixedColumn col1 = new FixedColumn(file, 4);
        FixedColumn col2 = new FixedColumn(file2, 4);

        FixedColumnDeltaProducer producer = new FixedColumnDeltaProducer(col1);
        producer.configure(col2.size(), col1.size());

        // hasNext() can be true, because of compulsory header
        // however, if column doesn't have data, hasContent() must be false.
        Assert.assertFalse(producer.hasContent());
        Assert.assertEquals(col1.size(), col2.size());
    }
}
