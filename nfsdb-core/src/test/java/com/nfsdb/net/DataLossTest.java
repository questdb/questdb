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

package com.nfsdb.net;

import com.nfsdb.JournalWriter;
import com.nfsdb.model.Quote;
import com.nfsdb.net.config.ClientConfig;
import com.nfsdb.net.config.ServerConfig;
import com.nfsdb.net.config.ServerNode;
import com.nfsdb.storage.TxListener;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DataLossTest extends AbstractTest {

    @Test
    public void testDiscardFile() throws Exception {

        // create master journal
        JournalWriter<Quote> master = factory.writer(Quote.class, "master");
        TestUtils.generateQuoteData(master, 300, master.getMaxTimestamp());
        master.commit();

        // publish master out
        JournalServer server = new JournalServer(
                new ServerConfig() {{
                    addNode(new ServerNode(0, "localhost"));
                    setEnableMultiCast(false);
                }}
                , factory);
        server.publish(master);
        server.start();

        final AtomicInteger counter = new AtomicInteger();

        // equalize slave
        JournalClient client = new JournalClient(new ClientConfig("localhost") {{
            setEnableMultiCast(false);
        }}, factory);
        client.subscribe(Quote.class, "master", "slave", new TxListener() {
            @Override
            public void onCommit() {
                counter.incrementAndGet();
            }

            @Override
            public void onError() {

            }
        });
        client.start();

        TestUtils.assertCounter(counter, 1, 1, TimeUnit.SECONDS);

        // stop client to be able to add to slave manually
        client.halt();


        // add more data to slave
        JournalWriter<Quote> slave = factory.writer(Quote.class, "slave");
        TestUtils.generateQuoteData(slave, 200, slave.getMaxTimestamp());
        slave.commit();
        slave.close();

        // synchronise slave again
        client = new JournalClient(new ClientConfig("localhost"), factory);
        client.subscribe(Quote.class, "master", "slave", new TxListener() {
            @Override
            public void onCommit() {
                counter.incrementAndGet();
            }

            @Override
            public void onError() {

            }
        });
        client.start();

        TestUtils.generateQuoteData(master, 145, master.getMaxTimestamp());
        master.commit();

        TestUtils.assertCounter(counter, 2, 1, TimeUnit.SECONDS);
        client.halt();

        slave = factory.writer(Quote.class, "slave");
        TestUtils.assertDataEquals(master, slave);
        Assert.assertEquals(master.getTxn(), slave.getTxn());
        Assert.assertEquals(master.getTxPin(), slave.getTxPin());

        server.halt();
    }
}
