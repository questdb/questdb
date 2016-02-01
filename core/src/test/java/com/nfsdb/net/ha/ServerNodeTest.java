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

import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.net.ha.config.ServerNode;
import org.junit.Assert;
import org.junit.Test;

public class ServerNodeTest {

    @Test(expected = JournalRuntimeException.class)
    public void testAddressValidation() throws Exception {
        new ServerNode(0, "192.168:x");
    }

    @Test
    public void testHostAndPort() throws Exception {
        ServerNode node = new ServerNode(0, "github.nfsdb.org:8080");
        Assert.assertEquals("github.nfsdb.org", node.getHostname());
        Assert.assertEquals(8080, node.getPort());
    }

    @Test
    public void testIPv4() throws Exception {
        ServerNode node = new ServerNode(0, "192.168.1.10");
        Assert.assertEquals("192.168.1.10", node.getHostname());
    }

    @Test
    public void testIPv4AndPort() throws Exception {
        ServerNode node = new ServerNode(0, "192.168.1.10:8080");
        Assert.assertEquals("192.168.1.10", node.getHostname());
        Assert.assertEquals(8080, node.getPort());
    }

    @Test
    public void testIPv6() throws Exception {
        ServerNode node = new ServerNode(0, "[fe80::5fc:43c:eef0:5b8e%3]");
        Assert.assertEquals("fe80::5fc:43c:eef0:5b8e%3", node.getHostname());
    }

    @Test
    public void testIPv6AndPort() throws Exception {
        ServerNode node = new ServerNode(0, "[fe80::5fc:43c:eef0:5b8e%3]:7090");
        Assert.assertEquals("fe80::5fc:43c:eef0:5b8e%3", node.getHostname());
        Assert.assertEquals(7090, node.getPort());
    }
}
