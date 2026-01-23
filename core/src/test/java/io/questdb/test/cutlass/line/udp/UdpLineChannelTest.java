/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cutlass.line.udp;

import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.udp.UdpLineChannel;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;

public class UdpLineChannelTest {
    private static final NetworkFacade FAILS_SET_SET_TTL_NET_FACADE = new NetworkFacadeImpl() {
        @Override
        public int setMulticastTtl(long fd, int ttl) {
            return -1;
        }
    };
    private static final NetworkFacade FAILS_TO_SET_MULTICAST_IFACE_NET_FACADE = new NetworkFacadeImpl() {
        @Override
        public int setMulticastInterface(long fd, int ipv4Address) {
            return -1;
        }
    };
    private static final NetworkFacade FD_EXHAUSTED_NET_FACADE = new NetworkFacadeImpl() {
        @Override
        public long socketUdp() {
            return -1;
        }
    };
    private static final Log LOG = LogFactory.getLog(UdpLineChannelTest.class);

    @BeforeClass
    public static void setUpStatic() {
        // it is necessary to initialise logger before tests start
        // logger doesn't relinquish memory until JVM stops
        // which causes memory leak detector to fail should logger be
        // created mid-test
        LOG.info().$("setup logger").$();
    }

    @Test
    public void testConstructorLeak_DescriptorsExhausted() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                new UdpLineChannel(FD_EXHAUSTED_NET_FACADE, 1, 1, 9000, 10);
                fail("the channel should fail to instantiate when NetworkFacade fails to create a new socket");
            } catch (LineSenderException ignored) {
                // expected
            }
        });
    }

    @Test
    public void testConstructorLeak_FailsToSendInterface() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                new UdpLineChannel(FAILS_TO_SET_MULTICAST_IFACE_NET_FACADE, 1, 1, 9000, 10);
                fail("the channel should fail to instantiate when NF fails to set multicast interface");
            } catch (LineSenderException ignored) {
                // expected
            }
        });
    }

    @Test
    public void testConstructorLeak_FailsToSetTTL() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                new UdpLineChannel(FAILS_SET_SET_TTL_NET_FACADE, 1, 1, 9000, 10);
                fail("the channel should fail to instantiate when NF fails to set multicast interface");
            } catch (LineSenderException ignored) {
                // expected
            }
        });
    }
}
