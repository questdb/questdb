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
import com.questdb.net.ha.config.ClientConfig;
import com.questdb.net.ha.config.ServerConfig;
import com.questdb.std.Rnd;
import com.questdb.std.ex.JournalException;
import com.questdb.store.*;
import com.questdb.store.factory.configuration.Constants;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestData;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Ignore
public class ScenarioTest extends AbstractTest {

    private final ServerConfig serverConfig = new ServerConfig() {{
        setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(300));
        setEnableMultiCast(false);
    }};

    private final ClientConfig clientConfig = new ClientConfig("localhost");

    @Test
    public void testLagTrickle() throws Exception {

        // prepare test data
        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin")) {
            TestData.appendQuoteData2(origin);

            try (final JournalWriter<Quote> randomOrigin = getFactory().writer(new JournalKey<>(Quote.class, "origin-rnd", PartitionBy.NONE, Constants.NULL_RECORD_HINT, false))) {
                randomOrigin.append(origin.query().all().asResultSet().shuffle(new Rnd()));


                try (final JournalWriter<Quote> remote = getFactory().writer(Quote.class, "remote")) {
                    try (final Journal<Quote> remoteReader = getFactory().reader(Quote.class, "remote")) {

                        // create empty journal
                        getFactory().writer(Quote.class, "local").close();

                        // setup local where data should be trickling from client
                        try (final Journal<Quote> local = getFactory().reader(Quote.class, "local")) {
                            Assert.assertEquals(0, local.size());

                            JournalServer server = new JournalServer(serverConfig, getFactory());
                            JournalClient client = new JournalClient(clientConfig, getFactory());

                            server.publish(remote);
                            server.start();

                            final AtomicInteger errors = new AtomicInteger();

                            final CountDownLatch ready = new CountDownLatch(1);
                            client.subscribe(Quote.class, "remote", "local", new JournalListener() {
                                @Override
                                public void onCommit() {
                                    try {
                                        if (local.refresh() && local.size() == 33) {
                                            ready.countDown();
                                        }
                                    } catch (JournalException e) {
                                        errors.incrementAndGet();
                                        e.printStackTrace();
                                    }
                                }

                                @Override
                                public void onEvent(int event) {
                                    if (event != JournalEvents.EVT_JNL_SUBSCRIBED) {
                                        errors.incrementAndGet();
                                    }
                                }
                            });


                            client.start();

                            int n = 0;
                            while (n < 400) {
                                lagIteration(randomOrigin, remote, n, n + 10);
                                n += 10;
                            }

                            Assert.assertTrue(ready.await(10, TimeUnit.SECONDS));

                            server.halt();
                            client.halt();

                            local.refresh();
                            remoteReader.refresh();
                            TestUtils.assertEquals(remoteReader, local);

                            Assert.assertEquals(0, errors.get());
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testSingleJournalTrickle() throws Exception {
        JournalServer server = new JournalServer(serverConfig, getFactory());
        JournalClient client = new JournalClient(clientConfig, getFactory());

        // prepare test data
        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin")) {
            TestData.appendQuoteData1(origin);
            Assert.assertEquals(100, origin.size());

            // setup remote we will be trickling test data into
            try (JournalWriter<Quote> remote = getFactory().writer(Quote.class, "remote")) {

                getFactory().writer(Quote.class, "local").close();

                // setup local where data should be trickling from client
                try (Journal<Quote> local = getFactory().reader(Quote.class, "local")) {
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


                    } finally {
                        client.halt();
                        server.halt();
                    }
                }
            }
        }
    }

    private static void iteration(String expected, Journal<Quote> origin, JournalWriter<Quote> remote, Journal<Quote> local, int lo, int hi) throws Exception {
        remote.append(origin.query().all().asResultSet().subset(lo, hi));
        remote.commit();

        int count = 0;
        do {
            Thread.sleep(100);
            if (count++ > 10) {
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
