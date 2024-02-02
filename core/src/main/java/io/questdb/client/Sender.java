/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.client;

import io.questdb.ClientTlsConfiguration;
import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.HttpClientConfiguration;
import io.questdb.client.impl.ConfStringParser;
import io.questdb.cutlass.auth.AuthUtils;
import io.questdb.cutlass.line.LineChannel;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.LineTcpSender;
import io.questdb.cutlass.line.http.LineHttpSender;
import io.questdb.cutlass.line.tcp.DelegatingTlsChannel;
import io.questdb.cutlass.line.tcp.PlainTcpLineChannel;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

import javax.security.auth.DestroyFailedException;
import java.io.Closeable;
import java.security.PrivateKey;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

/**
 * Influx Line Protocol client to feed data to a remote QuestDB instance.
 * <p>
 * This client supports both HTTP and TCP protocols. In most cases you should prefer HTTP protocol as it provides
 * stronger transactional guarantees and better feedback in case of errors.
 * <p>
 * Use {@link #builder()} method to create a new instance.
 * <br>
 * How to use the Sender:
 * <ol>
 *     <li>Obtain an instance via {@link #builder()}</li>
 *     <li>Use {@link #table(CharSequence)} to select a table</li>
 *     <li>Use {@link #symbol(CharSequence, CharSequence)} to add all symbols. You must add symbols before adding other columns.</li>
 *     <li>Use {@link #stringColumn(CharSequence, CharSequence)}, {@link #longColumn(CharSequence, long)},
 *     {@link #doubleColumn(CharSequence, double)}, {@link #boolColumn(CharSequence, boolean)},
 *     {@link #timestampColumn(CharSequence, long, ChronoUnit)} to add remaining columns columns</li>
 *     <li>Use {@link #at(long, ChronoUnit)} (long)} to finish a row with an explicit timestamp.Alternatively, you can use use
 *     {@link #atNow()} which will add a timestamp on a server.</li>
 *     <li>Optionally: You can use {@link #flush()} to send locally buffered data into a server</li>
 * </ol>
 * <br>
 * Sender supports both HTTP and TCP transports. It defaults to TCP transport. To use HTTP transport you must call
 * {@link LineSenderBuilder#http()} method.
 * <p>
 * Sender implements the <code>java.io.Closeable</code> interface. Thus, you must call the {@link #close()} method
 * when you no longer need it.
 * <br>
 * Thread-safety: Sender is not thread-safe. Each thread-safe needs its own instance or you have to implement
 * a mechanism for passing Sender instances among thread. An object pool could have this role.
 * <br>
 * Error-handling: Most errors throw an instance of {@link LineSenderException}.
 */
public interface Sender extends Closeable {

    /**
     * Construct a Builder object to create a new Sender instance.
     *
     * @return Builder object to create a new Sender instance.
     */
    static LineSenderBuilder builder() {
        return new LineSenderBuilder();
    }

    /**
     * Create a Sender instance from a configuration string.
     * <br>
     * Configuration string fully describes Sender configuration.
     * <p>
     * <b>Example 1</b><br>
     * This example creates a Sender instance that connects to a QuestDB server over HTTP transport. The created Sender
     * will auto-flush data when number of buffered rows reaches 1000.
     * <code>http::addr=localhost:9000;auto_flush_rows=1000;</code>
     * <br>
     * <b>Example 2</b><br>
     * This example creates a Sender instance that connects to a QuestDB server over TCP transport.
     * <code>tcp::addr=localhost:9009;</code>
     * <p>
     * Refer to <a href="https://questdb.io/docs/reference/clients/overview/">QuestDB documentation</a> for a full list
     * of configuration options.
     *
     * @param configurationString configuration string
     * @return Sender instance
     * @see #fromEnv()
     * @see LineSenderBuilder#fromConfig(CharSequence)
     */
    static Sender fromConfig(CharSequence configurationString) {
        return builder().fromConfig(configurationString).build();
    }

    /**
     * Create a new Sender instance described by a configuration string available as an environment variable.
     * <br>
     * It obtains a string from an environment variable <code>QDB_CLIENT_CONF</code> and then calls
     * {@link #fromConfig(CharSequence)}.
     * <br>
     * This is a convenience method suitable for Cloud environments.
     * <br>
     * <b>Example</b><br>
     * 1. Export a configuration string as an environment variable:
     * <pre>{@code export QDB_CLIENT_CONF="http::addr=localhost:9000;auto_flush_rows=100;"}</pre>
     * 2. Create and use a Sender:
     * <pre>{@code
     * try (Sender sender = Sender.fromEnv()) {
     *  for (int i = 0; i < 1000; i++) {
     *    sender.table("my_table").longColumn("value", i).atNow();
     *  }
     * }
     * }</pre>
     *
     * @return Sender instance
     * @see #fromConfig(CharSequence)
     */
    static Sender fromEnv() {
        String configString = System.getenv("QDB_CLIENT_CONF");
        if (Chars.isBlank(configString)) {
            throw new LineSenderException("QDB_CLIENT_CONF environment variable is not set");
        }
        return fromConfig(configString);
    }

    /**
     * Finalize the current row and assign an explicit timestamp.
     * After calling this method you can start a new row by calling {@link #table(CharSequence)} again.
     *
     * @param timestamp timestamp value since epoch
     * @param unit      timestamp unit
     */
    void at(long timestamp, ChronoUnit unit);

    /**
     * Finalize the current row and assign an explicit timestamp.
     * After calling this method you can start a new row by calling {@link #table(CharSequence)} again.
     *
     * @param timestamp timestamp value
     */
    void at(Instant timestamp);

    /**
     * Finalize the current row and let QuestDB server assign a timestamp. If you need to set timestamp
     * explicitly then see {@link #at(long, ChronoUnit)}.
     * <br>
     * After calling this method you can start a new row by calling {@link #table(CharSequence)} again.
     */
    void atNow();

    /**
     * Add a column with a boolean value.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    Sender boolColumn(CharSequence name, boolean value);

    /**
     * Close this Sender.
     * <br>
     * This must be called before dereferencing Sender, otherwise resources might leak.
     * Upon returning from this method the Sender is closed and cannot be used anymore.
     * Close method is idempotent, calling this method multiple times has no effect.
     * Calling any other on a closed Sender will throw {@link LineSenderException}
     * <br>
     */
    @Override
    void close();

    /**
     * Add a column with a floating point value.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    Sender doubleColumn(CharSequence name, double value);

    /**
     * Force flushing internal buffers to a server.
     * <br>
     * You should also call this method when you expect a period of quiescence during which no data will be written.
     * Otherwise, previously buffered data would not be sent to a server.
     * <br>
     * This method is also useful when you need a fine control over Sender batching behaviour. Buffer flushing reduces
     * the batching effect. This means it can lower the overall throughput, as each batch has a certain fixed cost
     * component, but it can decrease maximum latency as messages spend less time waiting in buffers and waiting for
     * automatic flush.
     *
     * @see LineSenderBuilder#bufferCapacity(int)
     * @see LineSenderBuilder#maxBufferCapacity(int)
     * @see LineSenderBuilder#autoFlushRows(int)
     */
    void flush();

    /**
     * Add a column with an integer value.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    Sender longColumn(CharSequence name, long value);

    /**
     * Add a column with a string value.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    Sender stringColumn(CharSequence name, CharSequence value);

    /**
     * Add a column with a symbol value. You must call add symbols before adding any other column types.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    Sender symbol(CharSequence name, CharSequence value);

    /**
     * Select the table for a new row. This is always the first method to start a error. It's an error to call other
     * methods without calling this method first.
     *
     * @param table name of the table
     * @return this instance for method chaining
     */
    Sender table(CharSequence table);

    /**
     * Add a column with a non-designated timestamp value.
     *
     * @param name  name of the column
     * @param value timestamp value since epoch
     * @param unit  timestamp value unit
     * @return this instance for method chaining
     */
    Sender timestampColumn(CharSequence name, long value, ChronoUnit unit);

    /**
     * Add a column with a non-designated timestamp value.
     *
     * @param name  name of the column
     * @param value timestamp value
     * @return this instance for method chaining
     */
    Sender timestampColumn(CharSequence name, Instant value);

    /**
     * Configure TLS mode.
     * Most users should not need to use anything but the default mode.
     */
    enum TlsValidationMode {

        /**
         * Sender validates a server certificate chain and throws an exception
         * when a certificate is not trusted.
         */
        DEFAULT,

        /**
         * Suitable for testing. In this mode Sender does not validate a server certificate chain.
         * This is inherently insecure and should never be used in a production environment.
         * Useful in test environments with self-signed certificates.
         */
        INSECURE
    }

    /**
     * Builder class to construct a new instance of a Sender.
     * <br>
     * Example usage for HTTP transport:
     * <pre>{@code
     * try (Sender sender = Sender.builder()
     *  .address("localhost:9000")
     *  .http()
     *  .build()) {
     *      sender.table(tableName).column("value", 42).atNow();
     *      sender.flush();
     *  }
     * }</pre>
     * <br>
     * Example usage for HTTP transport and TLS:
     * <pre>{@code
     * try (Sender sender = Sender.builder()
     *  .address("localhost:9000")
     *  .http()
     *  .enableTls()
     *  .build()) {
     *    sender.table(tableName).column("value", 42).atNow();
     *    sender.flush();
     *   }
     * }</pre>
     * <br>
     * Example usage for TCP transport and TLS:
     * <pre>{@code
     * try (Sender sender = Sender.builder()
     *  .address("localhost:9000")
     *  .http()
     *  .enableTls()
     *  .build()) {
     *    sender.table(tableName).column("value", 42).atNow();
     *    sender.flush();
     *   }
     * }</pre>
     */
    final class LineSenderBuilder {
        private static final int AUTO_FLUSH_DISABLED = 0;
        private static final int DEFAULT_AUTO_FLUSH_ROWS = 600;
        private static final int DEFAULT_BUFFER_CAPACITY = 64 * 1024;
        private static final int DEFAULT_HTTP_PORT = 9000;
        private static final int DEFAULT_HTTP_TIMEOUT = 30_000;
        private static final int DEFAULT_MAXIMUM_BUFFER_CAPACITY = 100 * 1024 * 1024;
        private static final long DEFAULT_MAX_RETRY_NANOS = TimeUnit.SECONDS.toNanos(10); // keep sync with the contract of the configuration method
        private static final int DEFAULT_TCP_PORT = 9009;
        private static final int MIN_BUFFER_SIZE = 512 + 1; // challenge size + 1;
        // The PARAMETER_NOT_SET_EXPLICITLY constant is used to detect if a parameter was set explicitly in configuration parameters
        // where it matters. This is needed to detect invalid combinations of parameters. Why?
        // We want to fail-fast even when an explicitly configured options happens to be same value as the default value,
        // because this still indicates a user error and silently ignoring it could lead to hard to debug issues.
        private static final int PARAMETER_NOT_SET_EXPLICITLY = -1;
        private static final int PROTOCOL_HTTP = 1;
        private static final int PROTOCOL_TCP = 0;
        private int autoFlushRows = PARAMETER_NOT_SET_EXPLICITLY;
        private int bufferCapacity = PARAMETER_NOT_SET_EXPLICITLY;
        private String host;
        private int httpTimeout = PARAMETER_NOT_SET_EXPLICITLY;
        private String httpToken;
        private String keyId;
        private int maximumBufferCapacity = PARAMETER_NOT_SET_EXPLICITLY;
        private final HttpClientConfiguration httpClientConfiguration = new DefaultHttpClientConfiguration() {
            @Override
            public int getInitialRequestBufferSize() {
                return bufferCapacity == PARAMETER_NOT_SET_EXPLICITLY ? DEFAULT_BUFFER_CAPACITY : bufferCapacity;
            }

            @Override
            public int getMaximumRequestBufferSize() {
                return maximumBufferCapacity == PARAMETER_NOT_SET_EXPLICITLY ? DEFAULT_MAXIMUM_BUFFER_CAPACITY : maximumBufferCapacity;
            }

            @Override
            public int getTimeout() {
                return httpTimeout == PARAMETER_NOT_SET_EXPLICITLY ? DEFAULT_HTTP_TIMEOUT : httpTimeout;
            }
        };
        private String password;
        private int port = PARAMETER_NOT_SET_EXPLICITLY;
        private PrivateKey privateKey;
        private int protocol = PARAMETER_NOT_SET_EXPLICITLY;
        private int retryTimeoutMillis = PARAMETER_NOT_SET_EXPLICITLY;
        private boolean shouldDestroyPrivKey;
        private boolean tlsEnabled;
        private TlsValidationMode tlsValidationMode;
        private char[] trustStorePassword;
        private String trustStorePath;
        private String username;

        private LineSenderBuilder() {

        }

        /**
         * Set address of a QuestDB server. It can be either a domain name or a textual representation of an IP address.
         * Only IPv4 addresses are supported.
         * <br>
         * Optionally, you can also include a port. In this can you separate a port from the address by using a colon.
         * Example: my.example.org:54321.
         * <p>
         * If you can include a port then you must not call {@link LineSenderBuilder#port(int)}.
         *
         * @param address address of a QuestDB server
         * @return this instance for method chaining.
         */
        public LineSenderBuilder address(CharSequence address) {
            if (this.host != null) {
                throw new LineSenderException("server address is already configured ")
                        .put("[address=").put(this.host).put("]");
            }
            if (Chars.isBlank(address)) {
                throw new LineSenderException("address cannot be empty nor null");
            }
            int portIndex = Chars.indexOf(address, ':');
            if (portIndex + 1 == address.length()) {
                throw new LineSenderException("invalid address, use IPv4 address or a domain name [address=").put(address).put("]");
            }
            if (portIndex != -1) {
                if (port != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("address contains a port, but a port was already configured ")
                            .put("[address=").put(address)
                            .put(", port=").put(port)
                            .put("]");
                }
                this.host = address.subSequence(0, portIndex).toString();
                try {
                    port(Numbers.parseInt(address, portIndex + 1, address.length()));
                } catch (NumericException e) {
                    throw new LineSenderException("cannot parse a port from the address, use IPv4 address or a domain name")
                            .put(" [address=").put(address).put("]");
                }
            } else {
                this.host = address.toString();
            }
            return this;
        }

        /**
         * Advanced TLS configuration. Most users should not need to use this.
         *
         * @return instance of {@link AdvancedTlsSettings} to advanced TLS configuration
         */
        public AdvancedTlsSettings advancedTls() {
            if (LineSenderBuilder.this.trustStorePath != null) {
                throw new LineSenderException("custom trust store was already configured ")
                        .put("[path=").put(LineSenderBuilder.this.trustStorePath).put("]");
            }
            if (tlsValidationMode == TlsValidationMode.INSECURE) {
                throw new LineSenderException("TLS validation was already disabled");
            }
            return new AdvancedTlsSettings();
        }

        /**
         * Set the maximum number of rows that are buffered locally before they are automatically sent to a server.
         * <br>
         * This is only used when communicating over HTTP transport, and it's illegal to call this method when
         * communicating over TCP transport.
         * <br>
         * The Sender automatically flushes its buffer when the number of accumulated rows reaches the configured value.
         * You must make sure that the buffer has sufficient capacity to accommodate all locally buffered data.
         * Otherwise, the Sender will throw an exception.
         * <br>
         * Setting this to 1 means that the Sender will send each row to a server immediately after it is added. This
         * effectively disables batching and may lead to a significant performance degradation.
         * <br>
         * You cannot set this value when auto-flush is disabled. See {@link #disableAutoFlush()}.
         *
         * @param autoFlushRows maximum number of rows that can be buffered locally before they are sent to a server.
         * @return this instance for method chaining
         * @see #flush()
         * @see #disableAutoFlush()
         * @see #maxBufferCapacity(int)
         */
        public LineSenderBuilder autoFlushRows(int autoFlushRows) {
            if (this.autoFlushRows > 0) {
                throw new LineSenderException("auto flush rows was already configured ")
                        .put("[autoFlushRows=").put(this.autoFlushRows).put("]");
            } else if (this.autoFlushRows == AUTO_FLUSH_DISABLED) {
                throw new LineSenderException("cannot set auto flush rows when auto-flush is disabled");
            }
            if (autoFlushRows < 1) {
                throw new LineSenderException("auto flush rows has to be positive ")
                        .put("[autoFlushRows=").put(autoFlushRows).put("]");
            }
            this.autoFlushRows = autoFlushRows;
            return this;
        }

        /**
         * Configure capacity of an internal buffer.
         * <p>
         * When communicating over HTTP protocol this buffer size is treated as the initial buffer capacity. Buffer can
         * grow up to {@link #maxBufferCapacity(int)}. You should call {@link #flush()} to send buffered data to
         * a server. Otherwise, data will be sent automatically when number of buffered rows reaches {@link #autoFlushRows(int)}.
         * <br>
         * When communicating over TCP protocol this buffer size is treated as the maximum buffer capacity. The Sender
         * will automatically flush the buffer when it reaches this capacity.
         *
         * @param bufferCapacity buffer capacity in bytes.
         * @return this instance for method chaining
         * @see Sender#flush()
         */
        public LineSenderBuilder bufferCapacity(int bufferCapacity) {
            if (this.bufferCapacity != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("buffer capacity was already configured ")
                        .put("[capacity=").put(this.bufferCapacity).put("]");
            }
            if (bufferCapacity < 0) {
                throw new LineSenderException("buffer capacity cannot be negative ")
                        .put("[capacity=").put(bufferCapacity).put("]");
            }
            this.bufferCapacity = bufferCapacity;
            return this;
        }

        /**
         * Build a Sender instance. This method construct a Sender instance.
         * <br>
         * You are responsible for calling {@link #close()} when you no longer need the Sender instance.
         *
         * @return returns a configured instance of Sender.
         */
        public Sender build() {
            configureDefaults();
            validateParameters();

            NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            if (protocol == PROTOCOL_HTTP) {
                int actualAutoFlushRows = autoFlushRows == PARAMETER_NOT_SET_EXPLICITLY ? DEFAULT_AUTO_FLUSH_ROWS : autoFlushRows;
                long actualMaxRetriesNanos = retryTimeoutMillis == PARAMETER_NOT_SET_EXPLICITLY ? DEFAULT_MAX_RETRY_NANOS : retryTimeoutMillis * 1_000_000L;
                ClientTlsConfiguration tlsConfig = null;
                if (tlsEnabled) {
                    assert (trustStorePath == null) == (trustStorePassword == null); //either both null or both non-null
                    tlsConfig = new ClientTlsConfiguration(trustStorePath, trustStorePassword, tlsValidationMode == TlsValidationMode.DEFAULT ? ClientTlsConfiguration.TLS_VALIDATION_MODE_FULL : ClientTlsConfiguration.TLS_VALIDATION_MODE_NONE);
                }
                return new LineHttpSender(host, port, httpClientConfiguration, tlsConfig, actualAutoFlushRows, httpToken, username, password, actualMaxRetriesNanos);
            }
            assert protocol == PROTOCOL_TCP;
            LineChannel channel = new PlainTcpLineChannel(nf, host, port, bufferCapacity * 2);
            LineTcpSender sender;
            if (tlsEnabled) {
                DelegatingTlsChannel tlsChannel;
                try {
                    tlsChannel = new DelegatingTlsChannel(channel, trustStorePath, trustStorePassword, tlsValidationMode, host);
                } catch (Throwable t) {
                    channel.close();
                    throw rethrow(t);
                }
                channel = tlsChannel;
            }
            try {
                sender = new LineTcpSender(channel, bufferCapacity);
            } catch (Throwable t) {
                channel.close();
                throw rethrow(t);
            }
            if (privateKey != null) {
                try {
                    sender.authenticate(keyId, privateKey);
                } catch (Throwable t) {
                    sender.close();
                    throw rethrow(t);
                } finally {
                    if (shouldDestroyPrivKey) {
                        try {
                            privateKey.destroy();
                        } catch (DestroyFailedException e) {
                            // not much we can do
                        }
                    }
                }
            }
            return sender;
        }

        /**
         * Disables automatic flushing of buffered data.
         * <p>
         * The Sender buffers data locally before flushing it to a server. This method disables automatic flushing, requiring
         * explicit invocation of {@link #flush()} to send buffered data to the server. It also disables automatic flushing
         * upon closing. This grants fine control over batching behavior.
         * <br>
         * The QuestDB server processes a batch as a single transaction, provided all rows in the batch target the same table.
         * Therefore, you can use this method to explicitly control transaction boundaries and ensure atomic processing of all
         * data in a batch. To maintain atomicity, ensure that all data in a batch is sent to the same table.
         * <p>
         * It is essential to ensure the maximum buffer capacity is sufficient to accommodate all locally buffered data.
         * <p>
         * This method should only be used when communicating via the HTTP protocol. Calling this method is illegal when
         * communicating over the TCP protocol.
         *
         * @return this instance for method chaining
         * @see #autoFlushRows(int)
         * @see #maxBufferCapacity(int)
         */
        public LineSenderBuilder disableAutoFlush() {
            if (this.autoFlushRows != -1) {
                throw new LineSenderException("auto flush rows was already configured ")
                        .put("[autoFlushRows=").put(this.autoFlushRows).put("]");
            }
            this.autoFlushRows = AUTO_FLUSH_DISABLED;
            return this;
        }

        /**
         * Configure authentication. This is needed when QuestDB server required clients to authenticate.
         * <br>
         * This is only used when communicating over TCP transport, and it's illegal to call this method when
         * communicating over HTTP transport.
         *
         * @param keyId keyId the client will send to a server.
         * @return an instance of {@link AuthBuilder}. As to finish authentication configuration.
         * @see #httpToken(String)
         * @see #httpUsernamePassword(String, String)
         */
        public LineSenderBuilder.AuthBuilder enableAuth(String keyId) {
            if (this.keyId != null) {
                throw new LineSenderException("authentication keyId was already configured ")
                        .put("[keyId=").put(this.keyId).put("]");
            }
            if (Chars.isBlank(keyId)) {
                throw new LineSenderException("keyId cannot be empty nor null");
            }
            this.keyId = keyId;
            return new LineSenderBuilder.AuthBuilder();
        }

        /**
         * Instruct a client to use TLS when connecting to a QuestDB server
         *
         * @return this instance for method chaining.
         */
        public LineSenderBuilder enableTls() {
            if (tlsEnabled) {
                throw new LineSenderException("tls was already enabled");
            }
            tlsEnabled = true;
            return this;
        }

        /**
         * Configure SenderBuilder from a configuration string.
         * <br>
         * This allows to use a configuration string as a template and amend it with additional configuration options.
         * <br>
         * It does not allow to override already configured options and throws an exception if you try to do so.
         * <br>
         *
         * @param configurationString configuration string
         * @return this instance for method chaining
         * @see #fromConfig(CharSequence)
         */
        public LineSenderBuilder fromConfig(CharSequence configurationString) {
            if (Chars.isBlank(configurationString)) {
                throw new LineSenderException("configuration string cannot be empty nor null");
            }
            StringSink sink = new StringSink();
            int pos = ConfStringParser.of(configurationString, sink);
            if (pos < 0) {
                throw new LineSenderException("invalid configuration string: ").put(sink);
            }
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("protocol was already configured ")
                        .put("[protocol=")
                        .put(protocol == PROTOCOL_HTTP ? "http" : "tcp").put("]");
            }
            if (host != null) {
                throw new LineSenderException("server address was already configured ")
                        .put("[address=").put(host).put("]");
            }
            if (port != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("server port was already configured ")
                        .put("[port=").put(port).put("]");
            }
            if (Chars.equals("http", sink)) {
                if (tlsEnabled) {
                    throw new LineSenderException("cannot use http protocol when TLS is enabled. use https instead");
                }
                http();
            } else if (Chars.equals("tcp", sink)) {
                if (tlsEnabled) {
                    throw new LineSenderException("cannot use tcp protocol when TLS is enabled. use tcps instead");
                }
                tcp();
            } else if (Chars.equals("https", sink)) {
                http();
                tlsEnabled = true;
            } else if (Chars.equals("tcps", sink)) {
                tcp();
                tlsEnabled = true;
            } else {
                throw new LineSenderException("invalid schema [schema=").put(sink).put(", supported-schemas=[http, https, tcp, tcps]]");
            }

            String tcpToken = null;
            String user = null;
            String password = null;
            while (ConfStringParser.hasNext(configurationString, pos)) {
                pos = ConfStringParser.nextKey(configurationString, pos, sink);
                if (pos < 0) {
                    throw new LineSenderException("invalid configuration string [error=").put(sink).put(']');
                }
                if (Chars.equals("addr", sink)) {
                    pos = getValue(configurationString, pos, sink, "address");
                    address(sink);
                    if (port == PARAMETER_NOT_SET_EXPLICITLY) {
                        port(protocol == PROTOCOL_TCP ? DEFAULT_TCP_PORT : DEFAULT_HTTP_PORT);
                    }
                } else if (Chars.equals("user", sink)) {
                    pos = getValue(configurationString, pos, sink, "user");
                    user = sink.toString();
                } else if (Chars.equals("pass", sink)) {
                    pos = getValue(configurationString, pos, sink, "pass");
                    if (protocol == PROTOCOL_TCP) {
                        throw new LineSenderException("password is not supported for TCP protocol");
                    }
                    password = sink.toString();
                } else if (Chars.equals("tls_verify", sink)) {
                    pos = getValue(configurationString, pos, sink, "tls_verify");
                    if (tlsValidationMode != null) {
                        throw new LineSenderException("tls_verify was already configured");
                    }
                    if (Chars.equals("on", sink)) {
                        tlsValidationMode = TlsValidationMode.DEFAULT;
                    } else if (Chars.equals("unsafe_off", sink)) {
                        tlsValidationMode = TlsValidationMode.INSECURE;
                    } else {
                        throw new LineSenderException("invalid tls_verify [value=").put(sink).put(", allowed-values=[on, unsafe_off]]");
                    }
                } else if (Chars.equals("tls_roots", sink)) {
                    pos = getValue(configurationString, pos, sink, "tls_roots");
                    if (trustStorePath != null) {
                        throw new LineSenderException("tls_roots was already configured");
                    }
                    trustStorePath = sink.toString();
                } else if (Chars.equals("tls_roots_password", sink)) {
                    pos = getValue(configurationString, pos, sink, "tls_roots_password");
                    if (trustStorePassword != null) {
                        throw new LineSenderException("tls_roots_password was already configured");
                    }
                    trustStorePassword = new char[sink.length()];
                    for (int i = 0, n = sink.length(); i < n; i++) {
                        trustStorePassword[i] = sink.charAt(i);
                    }
                } else if (Chars.equals("token", sink)) {
                    pos = getValue(configurationString, pos, sink, "token");
                    if (protocol == PROTOCOL_TCP) {
                        tcpToken = sink.toString();
                        // will configure later, we need to know a keyId first
                    } else if (protocol == PROTOCOL_HTTP) {
                        httpToken(sink.toString());
                    } else {
                        throw new AssertionError();
                    }
                } else if (Chars.equals("retry_timeout", sink)) {
                    pos = getValue(configurationString, pos, sink, "retry_timeout");
                    int timeout = parseIntValue(sink, "retry_timeout");
                    retryTimeoutMillis(timeout);
                } else if (Chars.equals("max_buf_size", sink)) {
                    pos = getValue(configurationString, pos, sink, "max_buf_size");
                    int maxBufferSize = parseIntValue(sink, "max_buf_size");
                    maxBufferCapacity(maxBufferSize);
                } else if (Chars.equals("init_buf_size", sink)) {
                    pos = getValue(configurationString, pos, sink, "init_buf_size");
                    int initBufferSize = parseIntValue(sink, "init_buf_size");
                    bufferCapacity(initBufferSize);
                } else if (Chars.equals("auto_flush_rows", sink)) {
                    pos = getValue(configurationString, pos, sink, "auto_flush_rows");
                    int autoFlushRows = parseIntValue(sink, "auto_flush_rows");
                    if (autoFlushRows < 1) {
                        throw new LineSenderException("invalid auto_flush_rows [value=").put(autoFlushRows).put("]");
                    }
                    autoFlushRows(autoFlushRows);
                } else if (Chars.equals("auto_flush", sink)) {
                    pos = getValue(configurationString, pos, sink, "auto_flush");
                    if (Chars.equalsIgnoreCase("off", sink)) {
                        disableAutoFlush();
                    } else if (!Chars.equalsIgnoreCase("on", sink)) {
                        throw new LineSenderException("invalid auto_flush [value=").put(sink).put(", allowed-values=[on, off]]");
                    }
                } else {
                    // ignore unknown keys, unless they are malformed
                    if ((pos = ConfStringParser.value(configurationString, pos, sink)) < 0) {
                        throw new LineSenderException("invalid parameter [error=").put(sink).put("]");
                    }
                }
            }
            if (host == null) {
                throw new LineSenderException("addr is missing");
            }
            if (trustStorePath != null) {
                if (trustStorePassword == null) {
                    throw new LineSenderException("tls_roots was configured, but tls_roots_password is missing");
                }
            } else if (trustStorePassword != null) {
                throw new LineSenderException("tls_roots_password was configured, but tls_roots is missing");
            }
            if (protocol == PROTOCOL_HTTP) {
                if (user != null) {
                    httpUsernamePassword(user, password);
                } else if (password != null) {
                    throw new LineSenderException("HTTP password is configured, but username is missing");
                }
            } else {
                if (user != null) {
                    enableAuth(user).authToken(tcpToken);
                } else if (tcpToken != null) {
                    throw new LineSenderException("TCP token is configured, but user is missing");
                }
            }
            return this;
        }

        /**
         * Use HTTP protocol as transport.
         * <br>
         * Configures the Sender to use the HTTP protocol.
         *
         * @return an instance of {@link LineSenderBuilder} for further configuration
         */
        public LineSenderBuilder http() {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("protocol was already configured ")
                        .put("[protocol=").put(protocol).put("]");
            }
            protocol = PROTOCOL_HTTP;
            return this;
        }

        /**
         * Set timeout is milliseconds for HTTP requests.
         * <br>
         * This is only used when communicating over HTTP transport and it's illegal to call this method when
         * communicating over TCP transport.
         *
         * @param httpTimeoutMillis timeout is milliseconds for HTTP requests.
         * @return this instance for method chaining
         */
        public LineSenderBuilder httpTimeoutMillis(int httpTimeoutMillis) {
            if (this.httpTimeout != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("HTTP timeout was already configured ")
                        .put("[timeout=").put(this.httpTimeout).put("]");
            }
            if (httpTimeoutMillis < 1) {
                throw new LineSenderException("HTTP timeout must be positive ")
                        .put("[timeout=").put(httpTimeoutMillis).put("]");

            }
            this.httpTimeout = httpTimeoutMillis;
            return this;
        }

        /**
         * Use HTTP Authentication token.
         * <br>
         * This is only used when communicating over HTTP transport, and it's illegal to
         * call this method when communicating over TCP transport.
         *
         * @param token HTTP authentication token
         * @return this instance for method chaining
         */
        public LineSenderBuilder httpToken(String token) {
            if (this.username != null) {
                throw new LineSenderException("authentication username was already configured ")
                        .put("[username=").put(this.username).put("]");
            }
            if (this.httpToken != null) {
                throw new LineSenderException("token was already configured");
            }
            if (Chars.isBlank(token)) {
                throw new LineSenderException("token cannot be empty nor null");
            }
            this.httpToken = token;
            return this;
        }

        /**
         * Use username and password for authentication when communicating over HTTP protocol.
         * <br>
         * This is only used when communicating over HTTP transport, and it's illegal to call this method when
         * communicating over TCP transport.
         *
         * @param username username
         * @param password password
         * @return this instance for method chaining
         * @see #httpToken(String)
         */
        public LineSenderBuilder httpUsernamePassword(String username, String password) {
            if (this.username != null) {
                throw new LineSenderException("authentication username was already configured ")
                        .put("[username=").put(this.username).put("]");
            }
            if (Chars.isBlank(username)) {
                throw new LineSenderException("username cannot be empty nor null");
            }
            if (Chars.isBlank(password)) {
                throw new LineSenderException("password cannot be empty nor null");
            }
            if (httpToken != null) {
                throw new LineSenderException("token authentication is already configured");
            }
            this.username = username;
            this.password = password;
            return this;
        }

        /**
         * Set the maximum local buffer capacity in bytes.
         * <br>
         * This is a hard limit on the maximum buffer capacity. The buffer cannot grow beyond this limit and Sender
         * will throw an exception if you try to accommodate more data in the buffer. To prevent this from happening
         * you should call {@link #flush()} periodically or set {@link #autoFlushRows(int)} to make sure that
         * Sender will flush the buffer automatically before it reaches the maximum capacity.
         * <br>
         * This is only used when communicating over HTTP transport since TCP transport uses a fixed buffer size.
         * <br>
         * Default value: 100 MB
         *
         * @param maximumBufferCapacity maximum buffer capacity in bytes.
         * @return this instance for method chaining
         */
        public LineSenderBuilder maxBufferCapacity(int maximumBufferCapacity) {
            if (maximumBufferCapacity < DEFAULT_BUFFER_CAPACITY) {
                throw new LineSenderException("maximum buffer capacity cannot be less than initial buffer capacity ")
                        .put("[maximumBufferCapacity=").put(maximumBufferCapacity)
                        .put(", initialBufferCapacity=").put(DEFAULT_BUFFER_CAPACITY)
                        .put("]");
            }
            this.maximumBufferCapacity = maximumBufferCapacity;
            return this;
        }

        /**
         * Set port where a QuestDB server is listening on.
         *
         * @param port port where a QuestDB server is listening on.
         * @return this instance for method chaining
         */
        public LineSenderBuilder port(int port) {
            if (this.port != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("post is already configured ")
                        .put("[port=").put(port).put("]");
            }
            if (port < 1 || port > 65535) {
                throw new LineSenderException("invalid port [port=").put(port).put("]");
            }
            this.port = port;
            return this;
        }

        /**
         * Configures the maximum time the Sender will spend retrying upon receiving a recoverable error from the server.
         * <br>
         * This setting is applicable only when communicating over the HTTP transport, and it is illegal to invoke this
         * method when communicating over the TCP transport.
         * <p>
         * Recoverable errors are those not caused by the client sending invalid data to the server. For instance,
         * connection issues or server outages are considered recoverable errors, whereas attempts to send a row
         * with an incorrect data type are not.
         * <p>
         * Setting this value to zero disables retries entirely. In such cases, the Sender will throw an exception
         * immediately. It's important to note that the Sender does not retry operations that fail
         * during {@link #close()}. Therefore, it is recommended to explicitly call {@link #flush()} before closing
         * the Sender.
         * <p>
         * <b>Warning:</b> Retrying may lead to data duplication. It is advisable to use
         * <a href="https://questdb.io/docs/concept/deduplication/">QuestDB deduplication</a> to mitigate this risk.
         * <p>
         * Default value: 10,000 milliseconds.
         *
         * @param retryTimeoutMillis the maximum retry duration in milliseconds.
         * @return this instance, enabling method chaining.
         */
        public LineSenderBuilder retryTimeoutMillis(int retryTimeoutMillis) {
            if (this.retryTimeoutMillis != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("retry timeout was already configured ")
                        .put("[retryTimeoutMillis=").put(this.retryTimeoutMillis).put("]");
            }
            if (retryTimeoutMillis < 0) {
                throw new LineSenderException("retry timeout cannot be negative ")
                        .put("[retryTimeoutMillis=").put(retryTimeoutMillis).put("]");
            }
            if (protocol == PROTOCOL_TCP) {
                throw new LineSenderException("retrying is not supported for TCP protocol");
            }
            this.retryTimeoutMillis = retryTimeoutMillis;
            return this;
        }

        /**
         * Use TCP protocol as transport.
         * <br>
         * Configures the Sender to use the TCP protocol. This is the default transport.
         *
         * @return an instance of {@link LineSenderBuilder} for further configuration
         */
        public LineSenderBuilder tcp() {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("protocol was already configured ")
                        .put("[protocol=").put(protocol).put("]");
            }
            protocol = PROTOCOL_TCP;
            return this;
        }

        private static int getValue(CharSequence configurationString, int pos, StringSink sink, String name) {
            if ((pos = ConfStringParser.value(configurationString, pos, sink)) < 0) {
                throw new LineSenderException("invalid ").put(name).put(" [error=").put(sink).put("]");
            }
            return pos;
        }

        private static int parseIntValue(@NotNull StringSink value, @NotNull String name) {
            if (Chars.isBlank(value)) {
                throw new LineSenderException(name).put(" cannot be empty");
            }
            try {
                return Numbers.parseInt(value);
            } catch (NumericException e) {
                throw new LineSenderException("invalid ").put(name).put(" [value=").put(value).put("]");
            }
        }

        private static RuntimeException rethrow(Throwable t) {
            if (t instanceof LineSenderException) {
                throw (LineSenderException) t;
            }
            throw new LineSenderException(t);
        }

        private void configureDefaults() {
            if (protocol == PARAMETER_NOT_SET_EXPLICITLY) {
                protocol = PROTOCOL_TCP;
            }
            if (bufferCapacity == PARAMETER_NOT_SET_EXPLICITLY) {
                bufferCapacity = DEFAULT_BUFFER_CAPACITY;
            }
            if (maximumBufferCapacity == PARAMETER_NOT_SET_EXPLICITLY) {
                maximumBufferCapacity = protocol == PROTOCOL_HTTP ? DEFAULT_MAXIMUM_BUFFER_CAPACITY : bufferCapacity;
            }
            if (port == PARAMETER_NOT_SET_EXPLICITLY) {
                port = protocol == PROTOCOL_HTTP ? DEFAULT_HTTP_PORT : DEFAULT_TCP_PORT;
            }
            if (tlsValidationMode == null) {
                tlsValidationMode = TlsValidationMode.DEFAULT;
            }
        }

        private void validateParameters() {
            if (host == null) {
                throw new LineSenderException("questdb server address not set");
            }
            if (!tlsEnabled && trustStorePath != null) {
                throw new LineSenderException("custom trust store configured, but TLS was not enabled ")
                        .put("[path=").put(LineSenderBuilder.this.trustStorePath).put("]");
            }
            if (!tlsEnabled && tlsValidationMode != TlsValidationMode.DEFAULT) {
                throw new LineSenderException("TSL validation disabled, but TLS was not enabled");
            }
            if (keyId != null && bufferCapacity < MIN_BUFFER_SIZE) {
                throw new LineSenderException("Requested buffer too small ")
                        .put("[minimalCapacity=").put(MIN_BUFFER_SIZE)
                        .put(", requestedCapacity=").put(bufferCapacity)
                        .put("]");
            }
            if (protocol == PROTOCOL_HTTP) {
                if (httpClientConfiguration.getMaximumRequestBufferSize() < httpClientConfiguration.getInitialRequestBufferSize()) {
                    throw new LineSenderException("maximum buffer capacity cannot be less than initial buffer capacity ")
                            .put("[maximumBufferCapacity=").put(httpClientConfiguration.getMaximumRequestBufferSize())
                            .put(", initialBufferCapacity=").put(httpClientConfiguration.getInitialRequestBufferSize())
                            .put("]");
                }
                if (privateKey != null) {
                    throw new LineSenderException("plain old token authentication is not supported for HTTP protocol. Did you mean to use HTTP token authentication?");
                }
            } else if (protocol == PROTOCOL_TCP) {
                if (username != null || password != null) {
                    throw new LineSenderException("username/password authentication is not supported for TCP protocol");
                }
                if (autoFlushRows == AUTO_FLUSH_DISABLED) {
                    throw new LineSenderException("disabling auto-flush is not supported for TCP protocol");
                } else if (autoFlushRows != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("auto flush rows is not supported for TCP protocol");
                }
                if (httpToken != null) {
                    throw new LineSenderException("HTTP token authentication is not supported for TCP protocol");
                }
                if (retryTimeoutMillis != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("retrying is not supported for TCP protocol");
                }
                if (httpTimeout != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("HTTP timeout is not supported for TCP protocol");
                }
                if (maximumBufferCapacity != bufferCapacity) {
                    throw new LineSenderException("maximum buffer capacity must be the same as initial buffer capacity for TCP protocol")
                            .put("[maximumBufferCapacity=").put(maximumBufferCapacity)
                            .put(", initialBufferCapacity=").put(bufferCapacity)
                            .put("]");
                }
            } else {
                throw new LineSenderException("unsupported protocol ")
                        .put("[protocol=").put(protocol).put("]");
            }
        }

        public class AdvancedTlsSettings {
            /**
             * Configure a custom truststore. This is only needed when using {@link #enableTls()} when your default
             * truststore does not contain certificate chain used by a server. Most users should not need it.
             * <br>
             * The path can be either a path on a local filesystem. Or you can prefix it with "classpath:" to instruct
             * the Sender to load a trust store from a classpath.
             *
             * @param trustStorePath     a path to a trust store.
             * @param trustStorePassword a password to for the truststore
             * @return an instance of LineSenderBuilder for further configuration
             */
            public LineSenderBuilder customTrustStore(String trustStorePath, char[] trustStorePassword) {
                if (LineSenderBuilder.this.trustStorePath != null) {
                    throw new LineSenderException("custom trust store was already configured ")
                            .put("[path=").put(LineSenderBuilder.this.trustStorePath).put("]");
                }
                if (Chars.isBlank(trustStorePath)) {
                    throw new LineSenderException("trust store path cannot be empty nor null");
                }
                if (trustStorePassword == null) {
                    throw new LineSenderException("trust store password cannot be null");
                }

                LineSenderBuilder.this.trustStorePath = trustStorePath;
                LineSenderBuilder.this.trustStorePassword = trustStorePassword;
                return LineSenderBuilder.this;
            }

            /**
             * This server certification validation altogether.
             * This is suitable when testing self-signed certificate. It's inherently insecure and should
             * never be used in a production.
             * <br>
             * If you cannot use trusted certificate then you should prefer {@link  #customTrustStore(String, char[])}
             * over disabling validation.
             *
             * @return an instance of LineSenderBuilder for further configuration
             */
            public LineSenderBuilder disableCertificateValidation() {
                LineSenderBuilder.this.tlsValidationMode = TlsValidationMode.INSECURE;
                return LineSenderBuilder.this;
            }

        }

        /**
         * Auxiliary class to configure client authentication.
         * If you have an instance of {@link PrivateKey} then you can pass it directly.
         * Alternative a private key encoded as a string token can be used too.
         */
        public class AuthBuilder {

            /**
             * Authenticate by using a token.
             *
             * @param token authentication token
             * @return an instance of LineSenderBuilder for further configuration
             */
            public LineSenderBuilder authToken(String token) {
                if (Chars.isBlank(token)) {
                    throw new LineSenderException("token cannot be empty nor null");
                }
                try {
                    LineSenderBuilder.this.privateKey = AuthUtils.toPrivateKey(token);
                } catch (IllegalArgumentException e) {
                    throw new LineSenderException("could not import token", e);
                }
                LineSenderBuilder.this.shouldDestroyPrivKey = true;
                return LineSenderBuilder.this;
            }

            /**
             * Configures a private key for authentication.
             *
             * @param privateKey privateKey to use for authentication
             * @return an instance of LineSenderBuilder for further configuration
             */
            public LineSenderBuilder privateKey(PrivateKey privateKey) {
                LineSenderBuilder.this.privateKey = privateKey;
                return LineSenderBuilder.this;
            }
        }
    }
}
