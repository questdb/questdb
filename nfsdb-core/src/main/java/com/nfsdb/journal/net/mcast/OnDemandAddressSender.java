/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.journal.net.mcast;

import com.nfsdb.journal.exceptions.JournalNetworkException;
import com.nfsdb.journal.net.config.ServerConfig;
import com.nfsdb.journal.utils.ByteBuffers;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class OnDemandAddressSender extends AbstractOnDemandSender {
    private final ServerConfig serverConfig;

    public OnDemandAddressSender(ServerConfig networkConfig, int inMessageCode, int outMessageCode) throws JournalNetworkException {
        super(networkConfig, inMessageCode, outMessageCode);
        this.serverConfig = networkConfig;
    }

    @Override
    protected void prepareBuffer(ByteBuffer buf) throws JournalNetworkException {
        InetSocketAddress address = serverConfig.getInterfaceSocketAddress();
        ByteBuffers.putStringW(buf, address.getAddress().toString());
        buf.put((byte) (serverConfig.getSslConfig().isSecure() ? 1 : 0));
        buf.putInt(address.getPort());
        buf.flip();
    }
}
