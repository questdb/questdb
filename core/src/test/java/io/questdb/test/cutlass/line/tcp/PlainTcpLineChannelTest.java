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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.tcp.PlainTcpLineChannel;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static org.junit.Assert.fail;

public class PlainTcpLineChannelTest extends AbstractLineTcpReceiverTest {
    private static final NetworkFacade FD_EXHAUSTED_NET_FACADE = new NetworkFacadeImpl() {
        @Override
        public long socketTcp(boolean blocking) {
            return -1;
        }
    };

    @Test
    public void testConstructorLeak_Hostname_CannotConnect() throws Exception {
        NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
        TestUtils.assertMemoryLeak(() -> {
            try {
                new PlainTcpLineChannel(nf, "localhost", 1000, 1000);
                fail("there should be nothing listening on the port 1000, the channel should have failed to connect");
            } catch (LineSenderException ignored) {
                // expected
            }
        });
    }

    @Test
    public void testConstructorLeak_Hostname_CannotResolveHost() throws Exception {
        NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
        TestUtils.assertMemoryLeak(() -> {
            try {
                new PlainTcpLineChannel(nf, "nonsense-fails-to-resolve", 1000, 1000);
                fail("the host should not resolved and the channel should have failed to connect");
            } catch (LineSenderException ignored) {
                // expected
            }
        });
    }

    @Test
    public void testConstructorLeak_Hostname_DescriptorsExhausted() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                new PlainTcpLineChannel(FD_EXHAUSTED_NET_FACADE, "localhost", 1000, 1000);
                fail("the channel should fail to instantiate when NF fails to create a new socket");
            } catch (LineSenderException ignored) {
                // expected
            }
        });
    }

    @Test
    public void testConstructorLeak_IP_CannotConnect() throws Exception {
        NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
        TestUtils.assertMemoryLeak(() -> {
            try {
                new PlainTcpLineChannel(nf, -1, 1000, 1000);
                fail("the channel should have failed to connect to address -1");
            } catch (LineSenderException ignored) {
                // expected
            }
        });
    }

    @Test
    public void testConstructorLeak_IP_DescriptorsExhausted() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                new PlainTcpLineChannel(FD_EXHAUSTED_NET_FACADE, -1, 1000, 1000);
                fail("the channel should fail to instantiate when NF fails to create a new socket");
            } catch (LineSenderException ignored) {
                // expected
            }
        });
    }
}
