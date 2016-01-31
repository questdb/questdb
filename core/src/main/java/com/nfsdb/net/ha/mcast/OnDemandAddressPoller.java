/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

import com.nfsdb.logging.Log;
import com.nfsdb.logging.LogFactory;
import com.nfsdb.net.ha.config.ClientConfig;
import com.nfsdb.net.ha.config.ServerNode;

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
