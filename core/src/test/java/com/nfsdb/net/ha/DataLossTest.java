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

import com.nfsdb.JournalWriter;
import com.nfsdb.model.Quote;
import com.nfsdb.net.ha.config.ClientConfig;
import com.nfsdb.net.ha.config.ServerConfig;
import com.nfsdb.net.ha.config.ServerNode;
import com.nfsdb.store.TxListener;
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
                    setHeartbeatFrequency(50);
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

        TestUtils.assertCounter(counter, 2, 5, TimeUnit.SECONDS);
        client.halt();

        slave = factory.writer(Quote.class, "slave");
        TestUtils.assertDataEquals(master, slave);
        Assert.assertEquals(master.getTxn(), slave.getTxn());
        Assert.assertEquals(master.getTxPin(), slave.getTxPin());

        server.halt();
    }
}
