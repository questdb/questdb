/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.line.http;

import io.questdb.ClientTlsConfiguration;
import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.HttpClientConfiguration;
import io.questdb.client.Sender;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientException;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.array.DoubleArray;
import io.questdb.cutlass.line.array.LongArray;
import io.questdb.cutlass.http.ilpv4.*;
import io.questdb.cairo.TableUtils;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.bytes.DirectByteSlice;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import static io.questdb.cutlass.http.ilpv4.IlpV4Constants.*;

/**
 * ILP v4 HTTP client sender for sending binary data to QuestDB over HTTP.
 * <p>
 * This class provides a fluent API identical to IlpV4Sender but uses HTTP
 * transport instead of TCP. The binary protocol is the same. It implements
 * the standard {@link Sender} interface.
 * <p>
 * Example usage:
 * <pre>
 * try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
 *     sender.table("weather")
 *           .symbol("city", "London")
 *           .doubleColumn("temperature", 23.5)
 *           .longColumn("humidity", 65)
 *           .at(timestamp, ChronoUnit.MICROS);
 *     sender.flush();
 * }
 * </pre>
 */
public class IlpV4HttpSender implements Sender {

    private static final String WRITE_PATH = "/write/v4";
    private static final String CONTENT_TYPE = "application/x-ilp-v4";
    private static final int DEFAULT_TIMEOUT_MS = 30000;

    // Retry constants
    private static final int RETRY_BACKOFF_MULTIPLIER = 2;
    private static final int RETRY_INITIAL_BACKOFF_MS = 10;
    private static final int RETRY_MAX_JITTER_MS = 10;

    // Multi-host support
    private final ObjList<String> hosts;
    private final IntList ports;
    private int currentAddressIndex;

    private final IlpV4MessageEncoder encoder;
    private final Map<String, IlpV4TableBuffer> tableBuffers;
    private final ObjList<String> tableOrder;
    private final HttpClient client;
    private final StringSink errorSink;

    private IlpV4TableBuffer currentTable;
    private boolean gorillaEnabled;
    private boolean useSchemaRef;
    private int autoFlushRows;
    private int pendingRows;
    private int timeoutMs;

    // Authentication fields
    private String authToken;
    private String username;
    private String password;

    // Retry fields
    private long maxRetriesNanos;
    private int maxBackoffMillis;
    private final Rnd rnd;

    // Time-based auto-flush fields
    private long flushIntervalNanos;
    private long flushAfterNanos = Long.MAX_VALUE;

    // Name validation
    private int maxNameLength;

    private IlpV4HttpSender(ObjList<String> hosts, IntList ports, HttpClientConfiguration clientConfiguration, ClientTlsConfiguration tlsConfig) {
        this.hosts = hosts;
        this.ports = ports;
        this.currentAddressIndex = 0;
        // Create encoder without its own buffer - we'll set the sink before each flush
        this.encoder = new IlpV4MessageEncoder(null);
        this.tableBuffers = new HashMap<>();
        this.tableOrder = new ObjList<>();
        this.currentTable = null;
        this.gorillaEnabled = true;
        this.useSchemaRef = false;
        this.autoFlushRows = 0;
        this.pendingRows = 0;
        this.timeoutMs = DEFAULT_TIMEOUT_MS;
        this.flushIntervalNanos = Long.MAX_VALUE; // Disable time-based auto-flush by default
        // Create TLS or plain text HTTP client based on configuration
        this.client = tlsConfig != null
                ? HttpClientFactory.newTlsInstance(clientConfiguration, tlsConfig)
                : HttpClientFactory.newPlainTextInstance(clientConfiguration);
        this.errorSink = new StringSink();
        this.rnd = new Rnd(System.nanoTime(), System.nanoTime());
    }

    /**
     * Creates a new sender and connects to the specified host and port.
     *
     * @param host server host
     * @param port server HTTP port
     * @return connected sender
     */
    public static IlpV4HttpSender connect(String host, int port) {
        ObjList<String> hosts = new ObjList<>();
        hosts.add(host);
        IntList ports = new IntList();
        ports.add(port);
        return new IlpV4HttpSender(hosts, ports, DefaultHttpClientConfiguration.INSTANCE, null);
    }

    /**
     * Factory method for SenderBuilder integration.
     * Creates an IlpV4HttpSender with configuration from the builder.
     *
     * @param hosts               list of server hosts
     * @param ports               list of server ports
     * @param clientConfiguration HTTP client configuration
     * @param tlsConfig           TLS configuration (null for plain HTTP)
     * @param autoFlushRows       auto-flush threshold (0 = disabled)
     * @param authToken           Bearer token for authentication (null if not used)
     * @param username            Username for basic auth (null if not used)
     * @param password            Password for basic auth (null if not used)
     * @param maxRetriesNanos     Maximum time to retry (in nanoseconds)
     * @param maxBackoffMillis    Maximum backoff between retries (in milliseconds)
     * @param flushIntervalNanos  Time-based auto-flush interval (in nanoseconds, Long.MAX_VALUE to disable)
     * @param maxNameLength       Maximum length for table and column names
     * @return configured sender
     */
    public static IlpV4HttpSender create(
            ObjList<String> hosts,
            IntList ports,
            HttpClientConfiguration clientConfiguration,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            String authToken,
            String username,
            String password,
            long maxRetriesNanos,
            int maxBackoffMillis,
            long flushIntervalNanos,
            int maxNameLength
    ) {
        assert authToken == null || (username == null && password == null);
        IlpV4HttpSender sender = new IlpV4HttpSender(hosts, ports, clientConfiguration, tlsConfig);
        sender.autoFlushRows = autoFlushRows;
        sender.authToken = authToken;
        sender.username = username;
        sender.password = password;
        sender.maxRetriesNanos = maxRetriesNanos;
        sender.maxBackoffMillis = maxBackoffMillis;
        sender.flushIntervalNanos = flushIntervalNanos;
        sender.maxNameLength = maxNameLength;
        return sender;
    }

    /**
     * Returns the current host being used for connections.
     */
    private String currentHost() {
        return hosts.get(currentAddressIndex);
    }

    /**
     * Returns the current port being used for connections.
     */
    private int currentPort() {
        return ports.get(currentAddressIndex);
    }

    /**
     * Rotate to the next address in the list (for failover).
     */
    private void rotateAddress() {
        currentAddressIndex = (currentAddressIndex + 1) % hosts.size();
    }

    /**
     * Returns whether Gorilla encoding is enabled.
     */
    public boolean isGorillaEnabled() {
        return gorillaEnabled;
    }

    /**
     * Sets whether to use Gorilla timestamp encoding.
     */
    public IlpV4HttpSender setGorillaEnabled(boolean enabled) {
        this.gorillaEnabled = enabled;
        return this;
    }

    /**
     * Sets whether to use schema reference mode.
     */
    public IlpV4HttpSender useSchemaReference(boolean use) {
        this.useSchemaRef = use;
        return this;
    }

    /**
     * Sets auto-flush threshold (0 = disabled).
     */
    public IlpV4HttpSender autoFlushRows(int rows) {
        this.autoFlushRows = rows;
        return this;
    }

    /**
     * Sets HTTP request timeout in milliseconds.
     */
    public IlpV4HttpSender setTimeout(int timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    // ==================== Sender interface implementation ====================
    // Note: Methods return IlpV4HttpSender (covariant return) to enable chaining with ILPv4-specific methods

    @Override
    public IlpV4HttpSender table(CharSequence tableName) {
        validateTableName(tableName);
        String name = tableName.toString();
        currentTable = tableBuffers.get(name);
        if (currentTable == null) {
            currentTable = new IlpV4TableBuffer(name);
            tableBuffers.put(name, currentTable);
            tableOrder.add(name);
        }
        return this;
    }

    @Override
    public IlpV4HttpSender symbol(CharSequence columnName, CharSequence value) {
        checkCurrentTable();
        validateColumnName(columnName);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_SYMBOL, true);
        col.addSymbol(value.toString());
        return this;
    }

    @Override
    public IlpV4HttpSender boolColumn(CharSequence columnName, boolean value) {
        checkCurrentTable();
        validateColumnName(columnName);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_BOOLEAN, false);
        col.addBoolean(value);
        return this;
    }

    @Override
    public IlpV4HttpSender longColumn(CharSequence columnName, long value) {
        checkCurrentTable();
        validateColumnName(columnName);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_LONG, false);
        col.addLong(value);
        return this;
    }

    /**
     * Adds a byte column value.
     */
    public IlpV4HttpSender byteColumn(CharSequence columnName, byte value) {
        checkCurrentTable();
        validateColumnName(columnName);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_BYTE, false);
        col.addByte(value);
        return this;
    }

    /**
     * Adds a short column value.
     */
    public IlpV4HttpSender shortColumn(CharSequence columnName, short value) {
        checkCurrentTable();
        validateColumnName(columnName);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_SHORT, false);
        col.addShort(value);
        return this;
    }

    /**
     * Adds an int column value.
     */
    public IlpV4HttpSender intColumn(CharSequence columnName, int value) {
        checkCurrentTable();
        validateColumnName(columnName);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_INT, false);
        col.addInt(value);
        return this;
    }

    @Override
    public IlpV4HttpSender doubleColumn(CharSequence columnName, double value) {
        checkCurrentTable();
        validateColumnName(columnName);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_DOUBLE, false);
        col.addDouble(value);
        return this;
    }

    /**
     * Adds a float column value.
     */
    public IlpV4HttpSender floatColumn(CharSequence columnName, float value) {
        checkCurrentTable();
        validateColumnName(columnName);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_FLOAT, false);
        col.addFloat(value);
        return this;
    }

    @Override
    public IlpV4HttpSender stringColumn(CharSequence columnName, CharSequence value) {
        checkCurrentTable();
        validateColumnName(columnName);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_STRING, true);
        col.addString(value.toString());
        return this;
    }

    @Override
    public IlpV4HttpSender timestampColumn(CharSequence columnName, long value, ChronoUnit unit) {
        checkCurrentTable();
        validateColumnName(columnName);
        if (unit == ChronoUnit.NANOS) {
            // Send nanoseconds with full precision using TYPE_TIMESTAMP_NANOS
            IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_TIMESTAMP_NANOS, true);
            col.addLong(value);
        } else {
            // Convert to microseconds for TYPE_TIMESTAMP
            long micros = toMicros(value, unit);
            IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_TIMESTAMP, true);
            col.addLong(micros);
        }
        return this;
    }

    @Override
    public IlpV4HttpSender timestampColumn(CharSequence columnName, Instant value) {
        long micros = value.getEpochSecond() * 1_000_000L + value.getNano() / 1000L;
        checkCurrentTable();
        validateColumnName(columnName);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_TIMESTAMP, true);
        col.addLong(micros);
        return this;
    }

    /**
     * Adds a timestamp column value (microseconds since epoch).
     */
    public IlpV4HttpSender timestampColumn(CharSequence columnName, long valueMicros) {
        checkCurrentTable();
        validateColumnName(columnName);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_TIMESTAMP, true);
        col.addLong(valueMicros);
        return this;
    }

    /**
     * Adds a UUID column value.
     */
    public IlpV4HttpSender uuidColumn(CharSequence columnName, long high, long low) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_UUID, true);
        col.addUuid(high, low);
        return this;
    }

    @Override
    public void at(long timestamp, ChronoUnit unit) {
        if (unit == ChronoUnit.NANOS) {
            // Send nanoseconds with full precision using TYPE_TIMESTAMP_NANOS
            atNanos(timestamp);
        } else {
            // Convert to microseconds for TYPE_TIMESTAMP
            long micros = toMicros(timestamp, unit);
            atMicros(micros);
        }
    }

    @Override
    public void at(Instant timestamp) {
        long micros = timestamp.getEpochSecond() * 1_000_000L + timestamp.getNano() / 1000L;
        atMicros(micros);
    }

    /**
     * Sets the designated timestamp and completes the row.
     *
     * @param timestampMicros timestamp in microseconds since epoch
     */
    public void at(long timestampMicros) {
        atMicros(timestampMicros);
    }

    private void atMicros(long timestampMicros) {
        checkCurrentTable();
        // Use empty column name to indicate this is the designated timestamp.
        // Empty string is invalid for user columns, so it uniquely identifies the
        // designated timestamp. The server maps this value to the table's designated
        // timestamp column, regardless of its actual name.
        // Must be nullable to support atNow() with server-assigned timestamps.
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn("", TYPE_TIMESTAMP, true);
        col.addLong(timestampMicros);
        finishRow();
    }

    private void atNanos(long timestampNanos) {
        checkCurrentTable();
        // Use empty column name with TYPE_TIMESTAMP_NANOS to indicate designated timestamp
        // with full nanosecond precision.
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn("", TYPE_TIMESTAMP_NANOS, true);
        col.addLong(timestampNanos);
        finishRow();
    }

    @Override
    public void atNow() {
        // Server-assigned timestamp: don't send any timestamp column.
        // The server will detect that no designated timestamp was provided
        // and use its own clock. This matches the old ILP protocol behavior.
        //
        // We cannot simply create a column named "timestamp" because:
        // 1. The table's designated timestamp column might have a different name
        // 2. Creating "timestamp" would add a spurious column to the table
        finishRow();
    }

    private void finishRow() {
        checkCurrentTable();
        currentTable.nextRow();
        pendingRows++;

        if (shouldAutoFlush()) {
            flush();
        }
    }

    private boolean shouldAutoFlush() {
        // Check row-based auto-flush
        if (autoFlushRows > 0 && pendingRows >= autoFlushRows) {
            return true;
        }

        // Check time-based auto-flush
        if (flushIntervalNanos != Long.MAX_VALUE) {
            long nowNanos = System.nanoTime();
            if (flushAfterNanos == Long.MAX_VALUE) {
                flushAfterNanos = nowNanos + flushIntervalNanos;
            } else if (flushAfterNanos - nowNanos < 0) {
                return true;
            }
        }

        return false;
    }

    @Override
    public void flush() {
        if (tableOrder.size() == 0) {
            return;
        }

        long retryingDeadlineNanos = Long.MIN_VALUE;
        int retryBackoff = Math.min(maxBackoffMillis, RETRY_INITIAL_BACKOFF_MS);
        LineSenderException lastException = null;

        while (true) {
            try {
                doFlushAttempt();
                return; // Success
            } catch (LineSenderException e) {
                // Non-retryable error
                if (!e.isRetryable()) {
                    throw e;
                }
                lastException = e;
            } catch (HttpClientException e) {
                // Network error, retryable
                lastException = new LineSenderException("Failed to flush: " + e.getMessage(), true);
            }

            // Check retry deadline
            long nowNanos = Os.currentTimeNanos();
            retryingDeadlineNanos = (retryingDeadlineNanos == Long.MIN_VALUE)
                    ? nowNanos + maxRetriesNanos
                    : retryingDeadlineNanos;
            if (nowNanos >= retryingDeadlineNanos) {
                throw lastException;
            }

            // Rotate address for multi-host support
            if (hosts.size() > 1) {
                rotateAddress();
            }

            // Sleep with backoff
            Os.sleep(retryBackoff);
            retryBackoff = backoff(retryBackoff);
        }
    }

    private void doFlushAttempt() {
        // Create HTTP request and prepare for content
        HttpClient.Request request = client.newRequest(currentHost(), currentPort())
                .POST()
                .url(WRITE_PATH)
                .header("Content-Type", CONTENT_TYPE);

        // Add authentication headers if configured
        if (username != null) {
            request.authBasic(username, password);
        } else if (authToken != null) {
            request.authToken(null, authToken);
        }

        request.withContent();

        // Create sink that writes directly to the HTTP request buffer
        IlpV4HttpRequestSink sink = new IlpV4HttpRequestSink(request);
        encoder.setSink(sink);
        encoder.setGorillaEnabled(gorillaEnabled);

        // Write placeholder header (will be patched later)
        for (int i = 0; i < HEADER_SIZE; i++) {
            sink.putByte((byte) 0);
        }

        // Encode each table directly to the HTTP request buffer
        for (int i = 0, n = tableOrder.size(); i < n; i++) {
            String tableName = tableOrder.get(i);
            IlpV4TableBuffer buffer = tableBuffers.get(tableName);
            if (buffer.getRowCount() > 0) {
                buffer.encode(encoder, useSchemaRef, gorillaEnabled);
            }
        }

        // Calculate payload length and table count
        int payloadLength = request.getContentLength() - HEADER_SIZE;
        int tableCount = 0;
        for (int i = 0, n = tableOrder.size(); i < n; i++) {
            String tableName = tableOrder.get(i);
            IlpV4TableBuffer buffer = tableBuffers.get(tableName);
            if (buffer.getRowCount() > 0) {
                tableCount++;
            }
        }

        // Get header address AFTER all encoding (in case buffer relocated)
        long headerAddress = request.getContentStart();

        // Patch header directly in the buffer
        io.questdb.std.Unsafe.getUnsafe().putByte(headerAddress, (byte) 'I');
        io.questdb.std.Unsafe.getUnsafe().putByte(headerAddress + 1, (byte) 'L');
        io.questdb.std.Unsafe.getUnsafe().putByte(headerAddress + 2, (byte) 'P');
        io.questdb.std.Unsafe.getUnsafe().putByte(headerAddress + 3, (byte) '4');
        io.questdb.std.Unsafe.getUnsafe().putByte(headerAddress + 4, VERSION_1);
        io.questdb.std.Unsafe.getUnsafe().putByte(headerAddress + 5, gorillaEnabled ? FLAG_GORILLA : 0);
        io.questdb.std.Unsafe.getUnsafe().putShort(headerAddress + 6, (short) tableCount);
        io.questdb.std.Unsafe.getUnsafe().putInt(headerAddress + 8, payloadLength);

        // Send the request
        HttpClient.ResponseHeaders response = request.send(timeoutMs);
        response.await(timeoutMs);

        DirectUtf8Sequence statusCode = response.getStatusCode();
        if (statusCode == null || !Utf8s.equalsNcAscii("200", statusCode) && !Utf8s.equalsNcAscii("204", statusCode)) {
            boolean retryable = isRetryableHttpStatus(statusCode);
            errorSink.clear();
            errorSink.put("HTTP error: ");
            if (statusCode != null) {
                errorSink.put(statusCode);
            } else {
                errorSink.put("no status");
            }

            Response resp = response.getResponse();
            if (resp != null) {
                Fragment fragment = resp.recv();
                if (fragment != null && fragment.lo() < fragment.hi()) {
                    errorSink.put(" - ");
                    for (long ptr = fragment.lo(); ptr < fragment.hi() && ptr < fragment.lo() + 200; ptr++) {
                        errorSink.put((char) io.questdb.std.Unsafe.getUnsafe().getByte(ptr));
                    }
                }
            }
            throw new LineSenderException(errorSink.toString(), retryable);
        }

        // Clear buffers
        for (int i = 0, n = tableOrder.size(); i < n; i++) {
            tableBuffers.get(tableOrder.get(i)).reset();
        }
        pendingRows = 0;

        // Reset time-based flush timer
        if (flushIntervalNanos != Long.MAX_VALUE) {
            flushAfterNanos = System.nanoTime() + flushIntervalNanos;
        }
    }

    private int backoff(int currentBackoff) {
        int jitter = rnd.nextInt(RETRY_MAX_JITTER_MS);
        int backoff = currentBackoff + jitter;
        return Math.min(maxBackoffMillis, backoff * RETRY_BACKOFF_MULTIPLIER);
    }

    private static boolean isRetryableHttpStatus(DirectUtf8Sequence statusCode) {
        if (statusCode == null) {
            return true;
        }
        // 5xx status codes are retryable (server errors)
        // 503 Service Unavailable, 500 Internal Server Error, etc.
        return statusCode.size() >= 1 && statusCode.byteAt(0) == '5';
    }

    private void validateTableName(CharSequence name) {
        if (maxNameLength > 0 && !TableUtils.isValidTableName(name, maxNameLength)) {
            if (name.length() > maxNameLength) {
                throw new LineSenderException("table name is too long: [name = ")
                        .putAsPrintable(name)
                        .put(", maxNameLength=")
                        .put(maxNameLength)
                        .put(']');
            }
            throw new LineSenderException("table name contains an illegal char: '\\n', '\\r', '?', ',', ''', " +
                    "'\"', '\\', '/', ':', ')', '(', '+', '*' '%%', '~', or a non-printable char: ")
                    .putAsPrintable(name);
        }
    }

    private void validateColumnName(CharSequence name) {
        if (maxNameLength > 0 && !TableUtils.isValidColumnName(name, maxNameLength)) {
            if (name.length() > maxNameLength) {
                throw new LineSenderException("column name is too long: [name = ")
                        .putAsPrintable(name)
                        .put(", maxNameLength=")
                        .put(maxNameLength)
                        .put(']');
            }
            throw new LineSenderException("column name contains an illegal char: '\\n', '\\r', '?', '.', ','" +
                    ", ''', '\"', '\\', '/', ':', ')', '(', '+', '-', '*' '%%', '~', or a non-printable char: ")
                    .putAsPrintable(name);
        }
    }

    @Override
    public DirectByteSlice bufferView() {
        throw new LineSenderException("bufferView() is not supported for ILPv4 HTTP sender");
    }

    @Override
    public void cancelRow() {
        // HTTP supports cancelRow - just reset current table's last row
        if (currentTable != null) {
            currentTable.cancelCurrentRow();
        }
    }

    @Override
    public void reset() {
        for (int i = 0, n = tableOrder.size(); i < n; i++) {
            tableBuffers.get(tableOrder.get(i)).reset();
        }
        pendingRows = 0;
        currentTable = null;
    }

    // ==================== Array methods ====================

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[] values) {
        if (values == null) {
            return this;
        }
        checkCurrentTable();
        validateColumnName(name);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(name.toString(), TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(values);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][] values) {
        if (values == null) {
            return this;
        }
        checkCurrentTable();
        validateColumnName(name);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(name.toString(), TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(values);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][][] values) {
        if (values == null) {
            return this;
        }
        checkCurrentTable();
        validateColumnName(name);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(name.toString(), TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(values);
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, DoubleArray array) {
        if (array == null) {
            return this;
        }
        checkCurrentTable();
        validateColumnName(name);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(name.toString(), TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(array);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[] values) {
        if (values == null) {
            return this;
        }
        checkCurrentTable();
        validateColumnName(name);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(name.toString(), TYPE_LONG_ARRAY, true);
        col.addLongArray(values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][] values) {
        if (values == null) {
            return this;
        }
        checkCurrentTable();
        validateColumnName(name);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(name.toString(), TYPE_LONG_ARRAY, true);
        col.addLongArray(values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][][] values) {
        if (values == null) {
            return this;
        }
        checkCurrentTable();
        validateColumnName(name);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(name.toString(), TYPE_LONG_ARRAY, true);
        col.addLongArray(values);
        return this;
    }

    @Override
    public Sender longArray(CharSequence name, LongArray array) {
        if (array == null) {
            return this;
        }
        checkCurrentTable();
        validateColumnName(name);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(name.toString(), TYPE_LONG_ARRAY, true);
        col.addLongArray(array);
        return this;
    }

    // ==================== Decimal methods ====================

    @Override
    public Sender decimalColumn(CharSequence name, Decimal64 value) {
        if (value == null || value.isNull()) {
            return this;
        }
        checkCurrentTable();
        validateColumnName(name);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(name.toString(), TYPE_DECIMAL64, true);
        col.addDecimal64(value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal128 value) {
        if (value == null || value.isNull()) {
            return this;
        }
        checkCurrentTable();
        validateColumnName(name);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(name.toString(), TYPE_DECIMAL128, true);
        col.addDecimal128(value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal256 value) {
        if (value == null || value.isNull()) {
            return this;
        }
        checkCurrentTable();
        validateColumnName(name);
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(name.toString(), TYPE_DECIMAL256, true);
        col.addDecimal256(value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, CharSequence value) {
        if (value == null || value.length() == 0) {
            return this;
        }
        // Parse as Decimal256 (highest precision to preserve all digits)
        checkCurrentTable();
        validateColumnName(name);
        try {
            java.math.BigDecimal bd = new java.math.BigDecimal(value.toString());
            Decimal256 decimal = Decimal256.fromBigDecimal(bd);
            IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(name.toString(), TYPE_DECIMAL256, true);
            col.addDecimal256(decimal);
        } catch (Exception e) {
            throw new LineSenderException("Failed to parse decimal value: " + value, e);
        }
        return this;
    }

    // ==================== Helper methods ====================

    private long toMicros(long value, ChronoUnit unit) {
        switch (unit) {
            case NANOS:
                return value / 1000L;
            case MICROS:
                return value;
            case MILLIS:
                return value * 1000L;
            case SECONDS:
                return value * 1_000_000L;
            case MINUTES:
                return value * 60_000_000L;
            case HOURS:
                return value * 3_600_000_000L;
            case DAYS:
                return value * 86_400_000_000L;
            default:
                throw new LineSenderException("Unsupported time unit: " + unit);
        }
    }


    private void checkCurrentTable() {
        if (currentTable == null) {
            throw new LineSenderException("No table selected. Call table() first.");
        }
    }

    /**
     * Returns the number of pending rows.
     */
    public int getPendingRows() {
        return pendingRows;
    }

    /**
     * Returns the number of tables in the current batch.
     */
    public int getTableCount() {
        return tableOrder.size();
    }

    @Override
    public void close() {
        encoder.close();
        Misc.free(client);
    }
}
