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

import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.ha.comsumer.JournalClientStateConsumer;
import com.nfsdb.ha.comsumer.JournalDeltaConsumer;
import com.nfsdb.ha.model.IndexedJournal;
import com.nfsdb.ha.producer.JournalClientStateProducer;
import com.nfsdb.ha.producer.JournalDeltaProducer;
import com.nfsdb.model.Quote;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;

public abstract class AbstractJournalTest extends AbstractTest {

    JournalWriter<Quote> origin;
    JournalWriter<Quote> master;
    JournalWriter<Quote> slave;

    private MockByteChannel channel;

    private JournalDeltaProducer journalDeltaProducer;
    private JournalDeltaConsumer journalDeltaConsumer;
    private JournalClientStateProducer journalClientStateProducer;
    private JournalClientStateConsumer journalClientStateConsumer;

    @Before
    public void setUp() throws Exception {
        origin = factory.writer(Quote.class, "origin");
        slave = factory.writer(Quote.class, "slave");
        master = factory.writer(Quote.class, "master");

        journalClientStateProducer = new JournalClientStateProducer();
        journalClientStateConsumer = new JournalClientStateConsumer();

        journalDeltaProducer = new JournalDeltaProducer(factory.reader(Quote.class, "master"));
        journalDeltaConsumer = new JournalDeltaConsumer(slave);
        channel = new MockByteChannel();
    }

    protected void executeSequence(boolean expectContent) throws JournalNetworkException, JournalException {
        slave.refresh();
        journalClientStateProducer.write(channel, new IndexedJournal(0, slave));
        journalClientStateConsumer.reset();
        journalClientStateConsumer.read(channel);

        journalDeltaProducer.configure(journalClientStateConsumer.getValue().getTxn(), journalClientStateConsumer.getValue().getTxPin());
        Assert.assertEquals(expectContent, journalDeltaProducer.hasContent());
        if (expectContent) {
            journalDeltaProducer.write(channel);
            journalDeltaConsumer.reset();
            journalDeltaConsumer.read(channel);
            TestUtils.assertEquals(master, slave);
        }
    }
}
