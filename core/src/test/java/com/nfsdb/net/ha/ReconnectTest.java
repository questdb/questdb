/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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

import com.nfsdb.Journal;
import com.nfsdb.JournalWriter;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.model.Quote;
import com.nfsdb.net.ha.config.ClientConfig;
import com.nfsdb.net.ha.config.ServerConfig;
import com.nfsdb.store.TxListener;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ReconnectTest extends AbstractTest {
    private JournalClient client;

    @Before
    public void setUp() {
        client = new JournalClient(
                new ClientConfig("localhost") {{
                    getReconnectPolicy().setLoginRetryCount(3);
                    getReconnectPolicy().setRetryCount(5);
                    getReconnectPolicy().setSleepBetweenRetriesMillis(TimeUnit.SECONDS.toMillis(1));
                }}
                , factory
        );
    }

    @Test
    public void testServerRestart() throws Exception {
        int size = 100000;
        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote", 2 * size);

        // start server #1
        JournalServer server = newServer();
        server.publish(remote);
        server.start();

        // subscribe client, waiting for complete set of data
        // when data arrives client triggers latch

        final CountDownLatch latch = new CountDownLatch(1);
        final Journal<Quote> local = factory.reader(Quote.class, "local");
        client.subscribe(Quote.class, "remote", "local", 2 * size, new TxListener() {
            @Override
            public void onCommit() {
                try {
                    if (local.refresh() && local.size() == 200000) {
                        latch.countDown();
                    }
                } catch (JournalException e) {
                    throw new JournalRuntimeException(e);
                }
            }

            @Override
            public void onError() {

            }
        });


        client.start();

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

    private JournalServer newServer() {
        return new JournalServer(new ServerConfig() {{
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(100));
            setEnableMultiCast(false);
        }}, factory);
    }
}
