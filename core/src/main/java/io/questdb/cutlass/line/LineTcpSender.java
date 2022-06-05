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
import io.questdb.cutlass.line.tcp.DelegatingTlsChannel;
import io.questdb.cutlass.line.tcp.PlanTcpLineChannel;
import io.questdb.network.Net;
import io.questdb.network.NetworkError;
import io.questdb.network.NetworkFacadeImpl;

import javax.security.auth.DestroyFailedException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivateKey;

public class LineTcpSender extends AbstractLineSender {
    private static final int MIN_BUFFER_SIZE_FOR_AUTH = 512 + 1; // challenge size + 1;

    public LineTcpSender(int sendToIPv4Address, int sendToPort, int bufferCapacity) {
        super(new PlanTcpLineChannel(NetworkFacadeImpl.INSTANCE, sendToIPv4Address, sendToPort, bufferCapacity * 2), bufferCapacity);
    }

    private LineTcpSender(LineChannel channel, int bufferCapacity) {
        super(channel, bufferCapacity);
    }

    public static LineTcpSender authenticatedPlainTextSender(int sendToIPv4Address, int sendToPort, int bufferCapacity, String authKey, PrivateKey privateKey) {
        checkBufferCapacity(bufferCapacity);
        LineTcpSender sender = new LineTcpSender(sendToIPv4Address, sendToPort, bufferCapacity);
        sender.authenticate(authKey, privateKey);
        return sender;
    }

    public static LineTcpSender authenticatedTlsSender(int sendToIPv4Address, int sendToPort, int bufferCapacity, String authKey, PrivateKey privateKey) {
        checkBufferCapacity(bufferCapacity);
        LineChannel plainTcpChannel = new PlanTcpLineChannel(NetworkFacadeImpl.INSTANCE, sendToIPv4Address, sendToPort, bufferCapacity * 2);
        LineChannel tlsChannel = new DelegatingTlsChannel(plainTcpChannel);
        LineTcpSender sender = new LineTcpSender(tlsChannel, bufferCapacity);
        sender.authenticate(authKey, privateKey);
        return sender;
    }

    private static void checkBufferCapacity(int capacity) {
        if (capacity < MIN_BUFFER_SIZE_FOR_AUTH) {
            throw new IllegalArgumentException("Minimal buffer capacity is " + capacity + ". Requested buffer capacity: " + capacity);
        }
    }

    @Override
    public void flush() {
        sendAll();
    }

    @Override
    protected void send00() {
        sendAll();
    }

    public static LineSenderBuilder builder() {
        return new LineSenderBuilder();
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
        private boolean tlsEnabled;

        private LineSenderBuilder() {

        }

        public LineSenderBuilder host(InetAddress host) {
            if (!(host instanceof Inet4Address)) {
                throw new IllegalArgumentException("only IPv4 addresses are supported");
            }
            if (this.host != 0) {
                throw new IllegalStateException("host address is already configured");
            }

            byte[] addrBytes = host.getAddress();
            int address  = addrBytes[3] & 0xFF;
            address |= ((addrBytes[2] << 8) & 0xFF00);
            address |= ((addrBytes[1] << 16) & 0xFF0000);
            address |= ((addrBytes[0] << 24) & 0xFF000000);
            this.host = address;
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

        public LineSenderBuilder enableTls() {
            tlsEnabled = true;
            return this;
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
                if (tlsEnabled) {
                    throw new IllegalStateException("tls is only supported together with authentication. for now.");
                }
                return new LineTcpSender(host, port, bufferCapacity);
            } else {
                LineTcpSender sender;
                if (tlsEnabled) {
                    sender = LineTcpSender.authenticatedTlsSender(host, port, bufferCapacity, keyId, privateKey);
                } else {
                    sender = LineTcpSender.authenticatedPlainTextSender(host, port, bufferCapacity, keyId, privateKey);
                }
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
