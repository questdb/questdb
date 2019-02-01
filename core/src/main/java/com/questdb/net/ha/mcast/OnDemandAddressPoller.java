/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.net.ha.config.ClientConfig;
import com.questdb.net.ha.config.ServerNode;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class OnDemandAddressPoller extends AbstractOnDemandPoller<ServerNode> {

    private static final Log LOG = LogFactory.getLog(OnDemandAddressPoller.class);

    public OnDemandAddressPoller(ClientConfig clientConfig, int inMessageCode, int outMessageCode) {
        super(clientConfig, inMessageCode, outMessageCode);
    }

    @Override
    protected ServerNode transform(ByteBuffer buf, InetSocketAddress sa) {
        char[] chars = new char[buf.getChar()];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = buf.getChar();
        }
        // skip SSL byte
        buf.get();
        int port = buf.getInt();
        String addr = new String(chars);
        try {
            if (InetAddress.getByName(addr).isAnyLocalAddress() && sa != null) {
                return new ServerNode(0, sa.getAddress().getHostAddress(), port);
            }
        } catch (UnknownHostException e) {
            LOG.error().$("Got bad address [").$(addr).$("] from: ").$(sa).$();
        }
        return new ServerNode(0, addr, port);
    }
}
