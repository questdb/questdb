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

import com.questdb.net.ha.config.ClientConfig;
import com.questdb.net.ha.config.ServerConfig;
import com.questdb.net.ha.config.ServerNode;
import com.questdb.net.ha.mcast.AbstractOnDemandSender;
import com.questdb.net.ha.mcast.OnDemandAddressPoller;
import com.questdb.net.ha.mcast.OnDemandAddressSender;
import com.questdb.std.ex.JournalNetworkException;
import com.questdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.net.Inet6Address;
import java.net.InterfaceAddress;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Ignore
public class MulticastTest extends AbstractTest {

    private boolean multicastDisabled;

    public MulticastTest() throws JournalNetworkException, SocketException {
        multicastDisabled = isMulticastDisabled();
    }

    @Test
    public void testAllNics() throws Exception {
        if (multicastDisabled) {
            return;
        }
        assertMulticast();
    }

    @Test
    public void testDefaultNICBehaviour() throws Exception {
        if (multicastDisabled) {
            return;
        }
        assertMulticast();
    }

    @Test
    public void testIPV4Forced() throws Exception {
        if (multicastDisabled) {
            return;
        }
        System.setProperty("java.net.preferIPv4Stack", "true");
        assertMulticast();
    }

    @Test
    public void testIPv6() throws Exception {
        if (multicastDisabled || !hasIPv6()) {
            return;
        }

        JournalServer server = new JournalServer(new ServerConfig() {{
            addNode(new ServerNode(0, "[0:0:0:0:0:0:0:0]"));
            setHeartbeatFrequency(100);
        }}, getFactory(), null, 0);

        final CountDownLatch connected = new CountDownLatch(1);
        JournalClient client = new JournalClient(new ClientConfig(), getFactory(), null, evt -> {
            if (evt == JournalClientEvents.EVT_CONNECTED) {
                connected.countDown();
            }
        });


        server.start();
        client.start();

        connected.await(3, TimeUnit.SECONDS);

        client.halt();
        server.halt();
    }

    @Test
    public void testLocalhostBehaviour() throws Exception {

        if (multicastDisabled) {
            return;
        }

        assertMulticast();
    }

    private static boolean isMulticastDisabled() throws JournalNetworkException, SocketException {
        return !new ServerConfig().getMultiCastInterface(0).supportsMulticast();
    }

    private static boolean hasIPv6() throws JournalNetworkException {
        List<InterfaceAddress> ifs = new ServerConfig().getMultiCastInterface(0).getInterfaceAddresses();
        for (int i = 0; i < ifs.size(); i++) {
            if (ifs.get(i).getAddress() instanceof Inet6Address) {
                return true;
            }
        }
        return false;
    }

    private void assertMulticast() throws JournalNetworkException, InterruptedException {
        AbstractOnDemandSender sender = new OnDemandAddressSender(new ServerConfig(), 120, 150, 0);
        sender.start();

        Thread.sleep(1000L);
        OnDemandAddressPoller poller = new OnDemandAddressPoller(new ClientConfig(), 150, 120);
        ServerNode address = poller.poll(2, 500, TimeUnit.MILLISECONDS);
        Assert.assertNotNull(address);
        sender.halt();
    }
}
