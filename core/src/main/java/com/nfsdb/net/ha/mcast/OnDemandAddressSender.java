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

package com.nfsdb.net.ha.mcast;

import com.nfsdb.ex.JournalNetworkException;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.net.ha.config.ServerConfig;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class OnDemandAddressSender extends AbstractOnDemandSender {
    private final ServerConfig serverConfig;

    public OnDemandAddressSender(ServerConfig networkConfig, int inMessageCode, int outMessageCode, int instance) {
        super(networkConfig, inMessageCode, outMessageCode, instance);
        this.serverConfig = networkConfig;
    }

    @Override
    protected void prepareBuffer(ByteBuffer buf) throws JournalNetworkException {
        InetSocketAddress address = serverConfig.getSocketAddress(instance);
        ByteBuffers.putStringW(buf, address.getAddress().getHostAddress());
        buf.put((byte) (serverConfig.getSslConfig().isSecure() ? 1 : 0));
        buf.putInt(address.getPort());
        buf.flip();
    }
}
