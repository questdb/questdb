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
import io.questdb.network.Net;
import io.questdb.network.NetworkError;

import javax.security.auth.DestroyFailedException;
import java.io.Closeable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivateKey;

public interface Sender extends Closeable {
    Sender table(String table);
    Sender column(String name, long value);
    Sender column(String name, String value);
    Sender column(String name, double value);
    Sender column(String name, boolean value);
    Sender symbol(String tag, String value);
    void atNow();
    void at(long ts);
    void flush();

    static LineSenderBuilder builder() {
        return new LineSenderBuilder();
    }

    final class LineSenderBuilder {
        // indicates buffer capacity was not set explicitly
        private static final byte BUFFER_CAPACITY_DEFAULT = 0;

        private static final int  DEFAULT_BUFFER_CAPACITY = 256 * 1024;

        private int port;
        private int host;
        private PrivateKey privateKey;
        private boolean shouldDestroyPrivKey;
        private int bufferCapacity = BUFFER_CAPACITY_DEFAULT;
        private boolean tlsEnabled;
        private String trustStorePath;
        private String user;
        private char[] trustStorePassword;

        private LineSenderBuilder() {

        }

        public LineSenderBuilder host(InetAddress host) {
            if (!(host instanceof Inet4Address)) {
                throw new LineSenderException("only IPv4 addresses are supported");
            }
            if (this.host != 0) {
                throw new LineSenderException("host address is already configured");
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
                throw new LineSenderException("host address is already configured");
            }
            try {
                // optimistically assume it's just IP address
                host = Net.parseIPv4(address);
            } catch (NetworkError e) {
                int portIndex = address.indexOf(':');
                if (portIndex + 1 == address.length()) {
                    throw new LineSenderException("cannot parse address " + address + ". address cannot ends with :");
                }
                String hostname;
                if (portIndex != -1) {
                    if (port != 0) {
                        throw new LineSenderException("address " + address + " contains a port, but a port was already set to " + port);
                    }
                    hostname = address.substring(0, portIndex);
                    port = Integer.parseInt(address.substring(portIndex + 1));
                } else {
                    hostname = address;
                }
                try {
                    InetAddress inet4Address = Inet4Address.getByName(hostname);
                    return host(inet4Address);
                } catch (UnknownHostException ex) {
                    throw new LineSenderException("bad address " + address, ex);
                }
            }
            return this;
        }

        public LineSenderBuilder port(int port) {
            if (this.port != 0) {
                throw new LineSenderException("post is already configured to " + this.port);
            }
            this.port = port;
            return this;
        }

        public LineSenderBuilder.AuthBuilder enableAuth(String user) {
            if (this.user != null) {
                throw new LineSenderException("authentication keyId was already set");
            }
            this.user = user;
            return new LineSenderBuilder.AuthBuilder();
        }

        public LineSenderBuilder enableTls() {
            tlsEnabled = true;
            return this;
        }

        public LineSenderBuilder bufferCapacity(int bufferCapacity) {
            if (this.bufferCapacity != BUFFER_CAPACITY_DEFAULT) {
                throw new LineSenderException("buffer capacity was already set to " + this.bufferCapacity);
            }
            this.bufferCapacity = bufferCapacity;
            return this;
        }

        public LineSenderBuilder customTrustStore(String trustStorePath, char[] trustStorePassword) {
            if (this.trustStorePath != null) {
                throw new LineSenderException("custom trust store was already set to " + this.trustStorePath);
            }
            this.trustStorePath = trustStorePath;
            this.trustStorePassword = trustStorePassword;
            return this;
        }

        public Sender build() {
            if (host == 0) {
                throw new LineSenderException("questdb server host not set");
            }
            if (port == 0) {
                throw new LineSenderException("questdb server port not set");
            }
            if (bufferCapacity == BUFFER_CAPACITY_DEFAULT) {
                bufferCapacity = DEFAULT_BUFFER_CAPACITY;
            }

            if (privateKey == null) {
                // unauthenticated path
                if (tlsEnabled) {
                    return LineTcpSender.tlsSender(host, port, bufferCapacity * 2, trustStorePath, trustStorePassword);
                }
                return new LineTcpSender(host, port, bufferCapacity);
            } else {
                // authenticated path
                LineTcpSender sender;
                if (tlsEnabled) {
                    assert (trustStorePath == null) == (trustStorePassword == null); //either both null or both non-null
                    sender = LineTcpSender.authenticatedTlsSender(host, port, bufferCapacity, user, privateKey, trustStorePath, trustStorePassword);
                } else {
                    sender = LineTcpSender.authenticatedPlainTextSender(host, port, bufferCapacity, user, privateKey);
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
                    throw new LineSenderException("private key was already set");
                }
                LineSenderBuilder.this.privateKey = privateKey;
                return LineSenderBuilder.this;
            }

            public LineSenderBuilder token(String token) {
                if (LineSenderBuilder.this.privateKey != null) {
                    throw new LineSenderException("token was already set");
                }
                try {
                    LineSenderBuilder.this.privateKey = AuthDb.importPrivateKey(token);
                } catch (IllegalArgumentException e) {
                    throw new LineSenderException("cannot import token", e);
                }
                LineSenderBuilder.this.shouldDestroyPrivKey = true;
                return LineSenderBuilder.this;
            }
        }
    }
}