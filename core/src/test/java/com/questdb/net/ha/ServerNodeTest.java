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

import com.questdb.net.ha.config.ServerNode;
import com.questdb.store.JournalRuntimeException;
import org.junit.Assert;
import org.junit.Test;

public class ServerNodeTest {

    @Test(expected = JournalRuntimeException.class)
    public void testAddressValidation() {
        new ServerNode(0, "192.168:x");
    }

    @Test
    public void testHostAndPort() {
        ServerNode node = new ServerNode(0, "github.questdb.org:8080");
        Assert.assertEquals("github.questdb.org", node.getHostname());
        Assert.assertEquals(8080, node.getPort());
    }

    @Test
    public void testIPv4() {
        ServerNode node = new ServerNode(0, "192.168.1.10");
        Assert.assertEquals("192.168.1.10", node.getHostname());
    }

    @Test
    public void testIPv4AndPort() {
        ServerNode node = new ServerNode(0, "192.168.1.10:8080");
        Assert.assertEquals("192.168.1.10", node.getHostname());
        Assert.assertEquals(8080, node.getPort());
    }

    @Test
    public void testIPv6() {
        ServerNode node = new ServerNode(0, "[fe80::5fc:43c:eef0:5b8e%3]");
        Assert.assertEquals("fe80::5fc:43c:eef0:5b8e%3", node.getHostname());
    }

    @Test
    public void testIPv6AndPort() {
        ServerNode node = new ServerNode(0, "[fe80::5fc:43c:eef0:5b8e%3]:7090");
        Assert.assertEquals("fe80::5fc:43c:eef0:5b8e%3", node.getHostname());
        Assert.assertEquals(7090, node.getPort());
    }
}
