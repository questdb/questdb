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

package com.nfsdb.journal.net;

import com.nfsdb.journal.JournalKey;
import com.nfsdb.journal.PartitionType;
import com.nfsdb.journal.net.model.IndexedJournalKey;
import com.nfsdb.journal.net.protocol.commands.SetKeyRequestConsumer;
import com.nfsdb.journal.net.protocol.commands.SetKeyRequestProducer;
import com.nfsdb.journal.test.model.Quote;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SetKeyTest {

    private MockByteChannel channel;

    @Before
    public void setUp() throws Exception {
        channel = new MockByteChannel();
    }

    @Test
    public void testProducerConsumer() throws Exception {
        SetKeyRequestProducer producer = new SetKeyRequestProducer();
        SetKeyRequestConsumer consumer = new SetKeyRequestConsumer();

        IndexedJournalKey key = new IndexedJournalKey(0, new JournalKey<>(Quote.class, "loc1", PartitionType.DAY, 100, true));
        producer.setValue(key);
        producer.write(channel);
        consumer.read(channel);
        Assert.assertEquals(key, consumer.getValue());

        IndexedJournalKey key2 = new IndexedJournalKey(1, new JournalKey<>(Quote.class, "longer_location", PartitionType.DAY, 1000, true));
        producer.setValue(key2);
        producer.write(channel);
        consumer.reset();
        consumer.read(channel);
        Assert.assertEquals(key2, consumer.getValue());

        IndexedJournalKey key3 = new IndexedJournalKey(2, new JournalKey<>(Quote.class, "shorter_loc", PartitionType.DAY, 1000, true));
        producer.setValue(key3);
        producer.write(channel);
        consumer.reset();
        consumer.read(channel);
        Assert.assertEquals(key3, consumer.getValue());
    }
}
