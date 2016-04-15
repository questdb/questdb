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

import com.nfsdb.JournalKey;
import com.nfsdb.PartitionType;
import com.nfsdb.model.Quote;
import com.nfsdb.net.ha.model.IndexedJournalKey;
import com.nfsdb.net.ha.protocol.commands.SetKeyRequestConsumer;
import com.nfsdb.net.ha.protocol.commands.SetKeyRequestProducer;
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

        IndexedJournalKey key = new IndexedJournalKey(0, new JournalKey<>(Quote.class, "loc1", PartitionType.DAY, 100, true));
        producer.setValue(key);
        producer.write(channel);
        consumer.read(channel);
        Assert.assertEquals(key, consumer.getValue());

        IndexedJournalKey key2 = new IndexedJournalKey(1, new JournalKey<>(Quote.class, "longer_location", PartitionType.DAY, 1000, true));
        producer.setValue(key2);
        producer.write(channel);
        consumer.read(channel);
        Assert.assertEquals(key2, consumer.getValue());

        IndexedJournalKey key3 = new IndexedJournalKey(2, new JournalKey<>(Quote.class, "shorter_loc", PartitionType.DAY, 1000, true));
        producer.setValue(key3);
        producer.write(channel);
        consumer.read(channel);
        Assert.assertEquals(key3, consumer.getValue());
    }
}
