/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.line;

import io.questdb.cutlass.line.tcp.AuthDb;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.Net;
import io.questdb.network.NetworkError;

import javax.security.auth.DestroyFailedException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivateKey;

public class LineTcpSender extends AbstractLineSender {
    private static final Log LOG = LogFactory.getLog(LineTcpSender.class);
    private static final int MIN_BUFFER_SIZE_FOR_AUTH = 512 + 1; // challenge size + 1;

    public LineTcpSender(int sendToIPv4Address, int sendToPort, int bufferCapacity) {
        super(0, sendToIPv4Address, sendToPort, bufferCapacity, 0, LOG);
    }

    private static int checkBufferCapacity(int capacity) {
        if (capacity < MIN_BUFFER_SIZE_FOR_AUTH) {
            throw new IllegalArgumentException("Minimal buffer capacity is " + capacity + ". Requested buffer capacity: " + capacity);
        }
        return capacity;
    }

    public LineTcpSender(int sendToIPv4Address, int sendToPort, int bufferCapacity, String authKey, PrivateKey privateKey) {
        super(0, sendToIPv4Address, sendToPort, checkBufferCapacity(bufferCapacity), 0, LOG);
        authenticate(authKey, privateKey);
    }

    @Override
    protected long createSocket(int interfaceIPv4Address, int ttl, long sockaddr) throws NetworkError {
        long fd = nf.socketTcp(true);
        if (nf.connect(fd, sockaddr) != 0) {
            throw NetworkError.instance(nf.errno(), "could not connect to ").ip(interfaceIPv4Address);
        }
        int orgSndBufSz = nf.getSndBuf(fd);
        nf.setSndBuf(fd, 2 * capacity);
        int newSndBufSz = nf.getSndBuf(fd);
        LOG.info().$("Send buffer size change from ").$(orgSndBufSz).$(" to ").$(newSndBufSz).$();
        return fd;
    }

    @Override
    protected void sendToSocket(long fd, long lo, long sockaddr, int len) throws NetworkError {
        if (nf.send(fd, lo, len) != len) {
            throw NetworkError.instance(nf.errno()).put("send error");
        }
    }

    @Override
    public void flush() {
        sendAll();
    }

    public static LineSenderBuilder builder() {
        return new LineSenderBuilder();
    }

    @Override
    protected void send00() {
        sendAll();
    }

    public static final class LineSenderBuilder {
        // indicates buffer capacity was not set explicitly
        private static final byte BUFFER_CAPACITY_DEFAULT = 0;

        private static final int  DEFAULT_BUFFER_CAPACITY = 256 * 1024;

        private int port;
        private int host;
        private String keyId;
        private PrivateKey privateKey;
        private boolean shouldDestroyPrivKey;
        private int bufferCapacity = BUFFER_CAPACITY_DEFAULT;

        private LineSenderBuilder() {

        }

        public LineSenderBuilder host(InetAddress host) {
            if (!(host instanceof Inet4Address)) {
                throw new IllegalArgumentException("only IPv4 addresses are supported");
            }
            if (this.host != 0) {
                throw new IllegalStateException("host address is already configured");
            }
            for (byte b : host.getAddress()) {
                this.host = (this.host << 8 | b);
            }
            return this;
        }

        public LineSenderBuilder address(String address) {
            if (host != 0) {
                throw new IllegalStateException("host address is already configured");
            }
            try {
                // optimistically assume it's just IP address
                host = Net.parseIPv4(address);
            } catch (NetworkError e) {
                int portIndex = address.indexOf(':');
                if (portIndex + 1 == address.length()) {
                    throw new IllegalArgumentException("cannot parse address " + address + ". address cannot ends with :");
                }
                String hostname;
                if (portIndex != -1) {
                    if (port != 0) {
                        throw new IllegalStateException("address " + address + " contains a port, but a port was already set to " + port);
                    }
                    hostname = address.substring(0, portIndex);
                    port = Integer.parseInt(address.substring(portIndex + 1));
                } else {
                    hostname = address;
                }
                try {
                    Inet4Address inet4Address = (Inet4Address) Inet4Address.getByName(hostname);
                    return host(inet4Address);
                } catch (UnknownHostException ex) {
                    throw new IllegalArgumentException("cannot parse address " + address, ex);
                }
            }
            return this;
        }

        public LineSenderBuilder port(int port) {
            if (this.port != 0) {
                throw new IllegalStateException("post is already configured to " + this.port);
            }
            this.port = port;
            return this;
        }

        public AuthBuilder enableAuth(String keyId) {
            if (this.keyId != null) {
                throw new IllegalStateException("authentication keyId was already set");
            }
            this.keyId = keyId;
            return new AuthBuilder();
        }

        public LineSenderBuilder bufferCapacity(int bufferCapacity) {
            if (this.bufferCapacity != BUFFER_CAPACITY_DEFAULT) {
                throw new IllegalStateException("buffer capacity was already set to " + this.bufferCapacity);
            }
            this.bufferCapacity = bufferCapacity;
            return this;
        }

        public LineTcpSender build() {
            if (host == 0) {
                throw new IllegalStateException("questdb server host not set");
            }
            if (port == 0) {
                throw new IllegalStateException("questdb server port not set");
            }
            if (bufferCapacity == BUFFER_CAPACITY_DEFAULT) {
                bufferCapacity = DEFAULT_BUFFER_CAPACITY;
            }

            if (privateKey == null) {
                return new LineTcpSender(host, port, bufferCapacity);
            } else {
                LineTcpSender sender = new LineTcpSender(host, port, bufferCapacity, keyId, privateKey);
                if (shouldDestroyPrivKey) {
                    try {
                        privateKey.destroy();
                    } catch (DestroyFailedException e) {
                        // not much we can do
                    }
                }
                return sender;
            }
        }

        public class AuthBuilder {
            public LineSenderBuilder privateKey(PrivateKey privateKey) {
                if (LineSenderBuilder.this.privateKey != null) {
                    throw new IllegalStateException("private key was already set");
                }
                LineSenderBuilder.this.privateKey = privateKey;
                return LineSenderBuilder.this;
            }

            public LineSenderBuilder token(String token) {
                if (LineSenderBuilder.this.privateKey != null) {
                    throw new IllegalStateException("token was already set");
                }
                LineSenderBuilder.this.privateKey = AuthDb.importPrivateKey(token);
                LineSenderBuilder.this.shouldDestroyPrivKey = true;
                return LineSenderBuilder.this;
            }
        }
    }
}
