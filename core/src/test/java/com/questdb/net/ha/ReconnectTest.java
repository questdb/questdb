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
import com.questdb.std.ex.JournalException;
import com.questdb.store.Journal;
import com.questdb.store.JournalListener;
import com.questdb.store.JournalRuntimeException;
import com.questdb.store.JournalWriter;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ReconnectTest extends AbstractTest {

    @Test
    @Ignore
    public void testServerRestart() throws Exception {
        final int size = 10000;
        try (JournalWriter<Quote> remote = getFactory().writer(Quote.class, "remote", 2 * size)) {

            // start server #1
            JournalServer server = newServer();
            server.publish(remote);
            server.start();

            final CountDownLatch connectedLatch = new CountDownLatch(1);
            JournalClient client = new JournalClient(
                    new ClientConfig("localhost") {{
                        getReconnectPolicy().setLoginRetryCount(3);
                        getReconnectPolicy().setRetryCount(5);
                        getReconnectPolicy().setSleepBetweenRetriesMillis(TimeUnit.SECONDS.toMillis(1));
                    }}, getFactory(), null,
                    evt -> {
                        if (evt == JournalClientEvents.EVT_CONNECTED) {
                            connectedLatch.countDown();
                        }
                    }
            );

            // subscribe client, waiting for complete set of data
            // when data arrives client triggers latch
            final CountDownLatch latch = new CountDownLatch(1);
            // create empty "local"
            getFactory().writer(Quote.class, "local").close();
            try (final Journal<Quote> local = getFactory().reader(Quote.class, "local")) {
                client.subscribe(Quote.class, "remote", "local", 2 * size, new JournalListener() {
                    @Override
                    public void onCommit() {
                        try {
                            if (local.refresh() && local.size() == 2 * size) {
                                latch.countDown();
                            }
                        } catch (JournalException e) {
                            throw new JournalRuntimeException(e);
                        }
                    }

                    @Override
                    public void onEvent(int event) {

                    }
                });


                client.start();

                Assert.assertTrue(connectedLatch.await(5, TimeUnit.SECONDS));
                // generate first batch
                TestUtils.generateQuoteData(remote, size, System.currentTimeMillis(), 1);
                remote.commit();

                // stop server
                server.halt();

                // start server #2
                server = newServer();
                server.publish(remote);
                server.start();

                // generate second batch
                TestUtils.generateQuoteData(remote, size, System.currentTimeMillis() + 2 * size, 1);
                remote.commit();

                // wait for client to get full set
                latch.await();

                // stop client and server
                client.halt();
                server.halt();

                // assert client state
                TestUtils.assertDataEquals(remote, local);
            }
        }
    }

    private JournalServer newServer() {
        return new JournalServer(new ServerConfig() {{
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(100));
            setEnableMultiCast(false);
        }}, getFactory());
    }
}
