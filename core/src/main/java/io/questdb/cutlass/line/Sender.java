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

import javax.security.auth.DestroyFailedException;
import java.io.Closeable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivateKey;

/**
 * ILP client to feed data to a remote QuestDB instance.
 *
 * Use {@link #builder()} method to create a new instance.
 * <br>
 * How to use the Sender:
 * <ol>
 *     <li>Obtain an instance via {@link #builder()}</li>
 *     <li>Use {@link #table(String)} to select a table</li>
 *     <li>Use {@link #symbol(String, String)} to add all symbols. You must add symbols before adding other columns.</li>
 *     <li>Use {@link #column(String, String)}, {@link #column(String, long)}, {@link #column(String, double)},
 *     {@link #column(String, boolean)} to add remaining columns columns</li>
 *     <li>Use {@link #at(long)} to finish a row with an explicit timestamp.Alternatively, you can use use
 *     {@link #atNow()} which will add a timestamp on a server.</li>
 *     <li>Optionally: You can use {@link #flush()} to send locally buffered data into a server</li>
 * </ol>
 * <br>
 *
 * Sender implements the <code>java.io.Closeable</code> interface. Thus, you must call the {@link #close()} method
 * when you no longer need it.
 * <br>
 * Thread-safety: Sender is not thread-safe. Each thread-safe needs its own instance or you have to implement
 * a mechanism for passing Sender instances among thread. An object pool could have this role.
 * <br>
 * Error-handling: Most errors throw an instance of {@link LineSenderException}.
 *
 */
public interface Sender extends Closeable {

    /**
     * Select the table for a new row. This is always the first method to start a error. It's an error to call other
     * methods without calling this method first.
     *
     * @param table name of the table
     * @return this instance for method chaining
     */
    Sender table(String table);

    /**
     * Add a column with an integer value.
     *
     * @param name name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    Sender column(String name, long value);

    /**
     * Add a column with a string value
     * @param name name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    Sender column(String name, String value);

    /**
     * Add a column with a floating point value
     * @param name name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    Sender column(String name, double value);

    /**
     * Add a column with a boolean value
     * @param name name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    Sender column(String name, boolean value);

    /**
     * Add a column with a symbol value. You must call add symbols before adding any other column types
     *
     * @param name name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    Sender symbol(String name, String value);

    /**
     * Finalize the current row and let QuestDB server to assign a timestamp. If you need to set timestamp
     * explicitly then see {@link #at(long)}
     * <br>
     * After calling this method you can start a new row by calling {@link #table(String)} again.
     *
     */
    void atNow();

    /**
     *  Finalize the current row and assign an explicit timestamp in Epoch nanoseconds.
     *  After calling this method you can start a new row by calling {@link #table(String)} again.
     *
     * @param timestamp timestamp in Epoch nanoseconds.
     */
    void at(long timestamp);

    /**
     * Force sending internal buffers to a server.
     * <br>
     * This is an optional method, it's not strictly necessary to call it. Normally, messages are accumulated in
     * internal buffers and Sender itself is flushing them automatically as it sees fit. This method is useful when you
     * need a fine control over this behaviour.
     *
     */
    void flush();

    /**
     * Construct a Builder object to create a new Sender instance.
     *
     * @return Builder object to create a new Sender instance.
     */
    static LineSenderBuilder builder() {
        return new LineSenderBuilder();
    }

    /**
     * Builder class to construct a new instance of a Sender.
     * <br>
     * Example usage:
     * <pre>{@code
     * try (Sender sender = Sender.builder()
     *  .address("localhost:9001")
     *  .build()) {
     *      sender.table(tableName).column("value", 42).atNow();
     *  }
     * }</pre>
     *
     */
    final class LineSenderBuilder {
        // indicates buffer capacity was not set explicitly
        private static final byte BUFFER_CAPACITY_DEFAULT = 0;

        private static final int  DEFAULT_BUFFER_CAPACITY = 64 * 1024;

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

        /**
         * Set address of an QuestDB server. InetAddress my represent IPv4 address.
         * After using this method you have to explicitly set a port by calling {@link LineSenderBuilder#port(int)}
         * Alternatively, you can use {@link LineSenderBuilder#address(String)} to set both address and port in one call.
         *
         * @param host host address to set
         * @return this instance for method chaining
         */
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

        /**
         * Set address of a QuestDB server. It can be either a domain name or a textual representation of an IP address.
         * Only IPv4 addresses are supported.
         * <br>
         * Optionally, you can also include a port. In this can you separate a port from the address by using a colon.
         * Example: my.example.org:54321.
         *
         * If you can include a port then you must not call {@link LineSenderBuilder#port(int)}.
         *
         * @param address address of a QuestDB server
         * @return this instance for method chaining.
         */
        public LineSenderBuilder address(String address) {
            if (host != 0) {
                throw new LineSenderException("host address is already configured");
            }
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

        /**
         * Set port where a QuestDB server is listening on.
         *
         * @param port port where a QuestDB server is listening on.
         * @return this instance for method chaining
         */
        public LineSenderBuilder port(int port) {
            if (this.port != 0) {
                throw new LineSenderException("post is already configured to " + this.port);
            }
            this.port = port;
            return this;
        }

        /**
         * Configure authentication. This is needed when QuestDB server required clients to authenticate.
         *
         * @param user keyId the client will send to a server.
         * @return an instance of {@link AuthBuilder}. As to finish authentication configuration.
         */
        public LineSenderBuilder.AuthBuilder enableAuth(String user) {
            if (this.user != null) {
                throw new LineSenderException("authentication keyId was already set");
            }
            this.user = user;
            return new LineSenderBuilder.AuthBuilder();
        }

        /**
         * Instruct a client to use TLS when connecting to a QuestDB server
         *
         * @return this instance for method chaining.
         */
        public LineSenderBuilder enableTls() {
            tlsEnabled = true;
            return this;
        }

        /**
         * Configure capacity of an internal buffer.
         *
         * @param bufferCapacity buffer capacity in bytes.
         * @return this instance for method chaining
         */
        public LineSenderBuilder bufferCapacity(int bufferCapacity) {
            if (this.bufferCapacity != BUFFER_CAPACITY_DEFAULT) {
                throw new LineSenderException("buffer capacity was already set to " + this.bufferCapacity);
            }
            this.bufferCapacity = bufferCapacity;
            return this;
        }

        /**
         * Configure a custom truststore. This is only needed when using {@link #enableTls()} when your default
         * truststore does not contain certificate chain used by a server. Most users should not need it.
         * <br>
         * The path can be either a path on a local filesystem. Or you can prefix it with "classpath:" to instruct
         * the Sender to load a trust store from a classpath.
         *
         *
         * @param trustStorePath a path to a trust store.
         * @param trustStorePassword a password to for the trustore
         * @return this instance for method chaining
         */
        public LineSenderBuilder customTrustStore(String trustStorePath, char[] trustStorePassword) {
            if (this.trustStorePath != null) {
                throw new LineSenderException("custom trust store was already set to " + this.trustStorePath);
            }
            this.trustStorePath = trustStorePath;
            this.trustStorePassword = trustStorePassword;
            return this;
        }

        /**
         * Build a Sender instance. This method construct a Sender instance.
         *
         * @return returns a configured instance of Sender.
         */
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

        /**
         * Auxiliary class to configure client -> server authentication.
         * If you have an instance of {@link PrivateKey} then you can pass it directly.
         * Alternative a private key encoded as a string token can be used too.
         *
         */
        public class AuthBuilder {

            /**
             * Configures a private key for authentication.
             *
             * @param privateKey privateKey to use for authentication
             * @return an instance of LineSenderBuilder for further configuration
             */
            public LineSenderBuilder privateKey(PrivateKey privateKey) {
                if (LineSenderBuilder.this.privateKey != null) {
                    throw new LineSenderException("private key was already set");
                }
                LineSenderBuilder.this.privateKey = privateKey;
                return LineSenderBuilder.this;
            }

            /**
             * Create a private key out of a base64 encoded token
             *
             * @param token base64 encoded private key
             * @return an instance of LineSenderBuilder for further configuration
             */
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