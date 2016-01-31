/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.net.ha;

import com.nfsdb.Journal;
import com.nfsdb.JournalKey;
import com.nfsdb.JournalWriter;
import com.nfsdb.PartitionType;
import com.nfsdb.ex.JournalException;
import com.nfsdb.misc.Rnd;
import com.nfsdb.model.Quote;
import com.nfsdb.net.ha.config.ClientConfig;
import com.nfsdb.net.ha.config.ServerConfig;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestData;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class ScenarioTest extends AbstractTest {

    private final ServerConfig serverConfig = new ServerConfig() {{
        setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(300));
        setEnableMultiCast(false);
    }};

    private final ClientConfig clientConfig = new ClientConfig("localhost");

    @Test
    public void testLagTrickle() throws Exception {

        // prepare test data
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin");
        TestData.appendQuoteData2(origin);

        final JournalWriter<Quote> randomOrigin = factory.writer(new JournalKey<>(Quote.class, "origin-rnd", PartitionType.NONE, false));
        randomOrigin.append(origin.query().all().asResultSet().shuffle(new Rnd()));
        origin.close();

        final JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");
        final Journal<Quote> remoteReader = factory.reader(Quote.class, "remote");

        // create empty journal
        Journal<Quote> local = factory.writer(Quote.class, "local");
        local.close();

        // setup local where data should be trickling from client
        local = factory.reader(Quote.class, "local");
        Assert.assertEquals(0, local.size());

        JournalServer server = new JournalServer(serverConfig, factory);
        JournalClient client = new JournalClient(clientConfig, factory);

        server.publish(remote);
        server.start();

        client.subscribe(Quote.class, "remote", "local");
        client.start();

        lagIteration(randomOrigin, remote, 0, 100);
        lagIteration(randomOrigin, remote, 100, 200);
        lagIteration(randomOrigin, remote, 200, 300);
        lagIteration(randomOrigin, remote, 300, 400);

        Thread.sleep(200);

        server.halt();
        client.halt();

        local.refresh();
        remoteReader.refresh();
        TestUtils.assertEquals(remoteReader, local);
    }

    @Test
    public void testSingleJournalTrickle() throws Exception {
        JournalServer server = new JournalServer(serverConfig, factory);
        JournalClient client = new JournalClient(clientConfig, factory);

        // prepare test data
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin");
        TestData.appendQuoteData1(origin);
        Assert.assertEquals(100, origin.size());

        // setup remote we will be trickling test data into
        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");

        // create empty journal
        Journal<Quote> local = factory.writer(Quote.class, "local");
        local.close();

        // setup local where data should be trickling from client
        local = factory.reader(Quote.class, "local");
        Assert.assertEquals(0, local.size());

        server.publish(remote);
        server.start();

        client.subscribe(Quote.class, "remote", "local");
        client.start();

        try {
            iteration("2013-02-10T10:03:20.000Z\tALDW\t0.32885755937534\t0.5741201360255567\t1836077773\t693649102\tFast trading\tSK\n" +
                            "2013-02-10T10:06:40.000Z\tAMD\t0.16781047061245025\t0.4831627617900026\t1423050407\t141794980\tFast trading\tGR\n" +
                            "2013-02-10T10:07:30.000Z\tHSBA.L\t0.04724340267969518\t0.5988337212476811\t178180342\t1522085049\tFast trading\tSK\n",
                    origin, remote, local, 0, 10
            );

            iteration("2013-02-10T10:15:50.000Z\tALDW\t0.7976166367363274\t0.06448758069572669\t1436005581\t1897226585\tFast trading\tGR\n" +
                            "2013-02-10T10:15:00.000Z\tAMD\t0.6789043827286667\t0.771921575501964\t580589771\t1159590077\tFast trading\tLXE\n" +
                            "2013-02-10T10:14:10.000Z\tHSBA.L\t0.984512894941384\t0.2664006899723862\t1288300070\t838312365\tFast trading\tLXE\n",
                    origin, remote, local, 10, 20
            );

            iteration("2013-02-10T10:24:10.000Z\tALDW\t0.26008876203627374\t0.04354393444455451\t25334630\t1835685418\tFast trading\tGR\n" +
                            "2013-02-10T10:23:20.000Z\tAMD\t0.9757637204046299\t0.7654386171943978\t23937995\t992860510\tFast trading\tLXE\n" +
                            "2013-02-10T10:21:40.000Z\tHSBA.L\t0.5630111081489209\t0.4222995146933318\t1534594684\t1153925552\tFast trading\tLN\n",
                    origin, remote, local, 20, 30
            );

        }
        finally {

            client.halt();
            server.halt();
        }
    }

    private static void iteration(String expected, Journal<Quote> origin, JournalWriter<Quote> remote, Journal<Quote> local, int lo, int hi) throws Exception {
        remote.append(origin.query().all().asResultSet().subset(lo, hi));
        remote.commit();

        int count = 0;
        do {
            Thread.sleep(100);
            if (count++ > 10){
                Assert.fail("Refresh is too slow!");
            }
        }
        while (!local.refresh());

        TestUtils.assertEquals(expected, local.query().head().withKeys().asResultSet());
    }

    private void lagIteration(final Journal<Quote> origin, final JournalWriter<Quote> remote, final int lo, final int hi) throws JournalException {
        remote.mergeAppend(new ArrayList<Quote>() {{
            for (Quote q : origin.query().all().asResultSet().subset(lo, hi).sort("timestamp")) {
                add(q);
            }
        }});
        remote.commit();
    }
}
