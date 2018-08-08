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

package com.questdb.net.ha;

import com.questdb.model.Quote;
import com.questdb.net.ha.model.IndexedJournalKey;
import com.questdb.net.ha.protocol.commands.SetKeyRequestConsumer;
import com.questdb.net.ha.protocol.commands.SetKeyRequestProducer;
import com.questdb.store.JournalKey;
import com.questdb.store.PartitionBy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SetKeyTest {

    private MockByteChannel channel;

    @Before
    public void setUp() {
        channel = new MockByteChannel();
    }

    @Test
    public void testProducerConsumer() throws Exception {
        SetKeyRequestProducer producer = new SetKeyRequestProducer();
        SetKeyRequestConsumer consumer = new SetKeyRequestConsumer();

        IndexedJournalKey key = new IndexedJournalKey(0, new JournalKey<>(Quote.class, "loc1", PartitionBy.DAY, 100, true));
        producer.setValue(key);
        producer.write(channel);
        consumer.read(channel);
        Assert.assertEquals(key, consumer.getValue());

        IndexedJournalKey key2 = new IndexedJournalKey(1, new JournalKey<>(Quote.class, "longer_location", PartitionBy.DAY, 1000, true));
        producer.setValue(key2);
        producer.write(channel);
        consumer.read(channel);
        Assert.assertEquals(key2, consumer.getValue());

        IndexedJournalKey key3 = new IndexedJournalKey(2, new JournalKey<>(Quote.class, "shorter_loc", PartitionBy.DAY, 1000, true));
        producer.setValue(key3);
        producer.write(channel);
        consumer.read(channel);
        Assert.assertEquals(key3, consumer.getValue());
    }
}
