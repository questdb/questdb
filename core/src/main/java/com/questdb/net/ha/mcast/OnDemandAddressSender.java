/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.net.ha.mcast;

import com.questdb.net.ha.config.ServerConfig;
import com.questdb.std.ByteBuffers;
import com.questdb.std.ex.JournalNetworkException;

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
