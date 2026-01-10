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
import io.questdb.cutlass.line.tcp.v4.*;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
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

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

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

    private final String host;
    private final int port;
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

    private IlpV4HttpSender(String host, int port, HttpClientConfiguration clientConfiguration) {
        this.host = host;
        this.port = port;
        this.encoder = new IlpV4MessageEncoder();
        this.tableBuffers = new HashMap<>();
        this.tableOrder = new ObjList<>();
        this.currentTable = null;
        this.gorillaEnabled = true;
        this.useSchemaRef = false;
        this.autoFlushRows = 0;
        this.pendingRows = 0;
        this.timeoutMs = DEFAULT_TIMEOUT_MS;
        this.client = HttpClientFactory.newPlainTextInstance(clientConfiguration);
        this.errorSink = new StringSink();
    }

    /**
     * Creates a new sender and connects to the specified host and port.
     *
     * @param host server host
     * @param port server HTTP port
     * @return connected sender
     */
    public static IlpV4HttpSender connect(String host, int port) {
        return new IlpV4HttpSender(host, port, DefaultHttpClientConfiguration.INSTANCE);
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
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_SYMBOL, true);
        col.addSymbol(value.toString());
        return this;
    }

    @Override
    public IlpV4HttpSender boolColumn(CharSequence columnName, boolean value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_BOOLEAN, false);
        col.addBoolean(value);
        return this;
    }

    @Override
    public IlpV4HttpSender longColumn(CharSequence columnName, long value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_LONG, false);
        col.addLong(value);
        return this;
    }

    /**
     * Adds a byte column value.
     */
    public IlpV4HttpSender byteColumn(CharSequence columnName, byte value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_BYTE, false);
        col.addByte(value);
        return this;
    }

    /**
     * Adds a short column value.
     */
    public IlpV4HttpSender shortColumn(CharSequence columnName, short value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_SHORT, false);
        col.addShort(value);
        return this;
    }

    /**
     * Adds an int column value.
     */
    public IlpV4HttpSender intColumn(CharSequence columnName, int value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_INT, false);
        col.addInt(value);
        return this;
    }

    @Override
    public IlpV4HttpSender doubleColumn(CharSequence columnName, double value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_DOUBLE, false);
        col.addDouble(value);
        return this;
    }

    /**
     * Adds a float column value.
     */
    public IlpV4HttpSender floatColumn(CharSequence columnName, float value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_FLOAT, false);
        col.addFloat(value);
        return this;
    }

    @Override
    public IlpV4HttpSender stringColumn(CharSequence columnName, CharSequence value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_STRING, true);
        col.addString(value.toString());
        return this;
    }

    @Override
    public IlpV4HttpSender timestampColumn(CharSequence columnName, long value, ChronoUnit unit) {
        long micros = toMicros(value, unit);
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_TIMESTAMP, true);
        col.addLong(micros);
        return this;
    }

    @Override
    public IlpV4HttpSender timestampColumn(CharSequence columnName, Instant value) {
        long micros = value.getEpochSecond() * 1_000_000L + value.getNano() / 1000L;
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_TIMESTAMP, true);
        col.addLong(micros);
        return this;
    }

    /**
     * Adds a timestamp column value (microseconds since epoch).
     */
    public IlpV4HttpSender timestampColumn(CharSequence columnName, long valueMicros) {
        checkCurrentTable();
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
        long micros = toMicros(timestamp, unit);
        atMicros(micros);
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
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn("timestamp", TYPE_TIMESTAMP, false);
        col.addLong(timestampMicros);
        finishRow();
    }

    @Override
    public void atNow() {
        atMicros(System.currentTimeMillis() * 1000L);
    }

    private void finishRow() {
        checkCurrentTable();
        currentTable.nextRow();
        pendingRows++;

        if (autoFlushRows > 0 && pendingRows >= autoFlushRows) {
            flush();
        }
    }

    @Override
    public void flush() {
        if (tableOrder.size() == 0) {
            return;
        }

        try {
            // Encode all tables
            encoder.reset();
            encoder.setGorillaEnabled(gorillaEnabled);

            // Reserve space for header
            int headerPos = encoder.getPosition();
            encoder.setPosition(headerPos + HEADER_SIZE);

            // Encode each table
            for (int i = 0, n = tableOrder.size(); i < n; i++) {
                String tableName = tableOrder.get(i);
                IlpV4TableBuffer buffer = tableBuffers.get(tableName);
                if (buffer.getRowCount() > 0) {
                    buffer.encode(encoder, useSchemaRef, gorillaEnabled);
                }
            }

            // Calculate payload length
            int payloadLength = encoder.getPosition() - headerPos - HEADER_SIZE;

            // Write header at the beginning
            int endPos = encoder.getPosition();
            encoder.setPosition(headerPos);

            int tableCount = 0;
            for (int i = 0, n = tableOrder.size(); i < n; i++) {
                String tableName = tableOrder.get(i);
                IlpV4TableBuffer buffer = tableBuffers.get(tableName);
                if (buffer.getRowCount() > 0) {
                    tableCount++;
                }
            }
            encoder.writeHeader(tableCount, payloadLength);
            encoder.setPosition(endPos);

            // Send the message via HTTP - directly from encoder's native buffer
            sendHttp(encoder.getBufferAddress(), encoder.getPosition());

            // Clear buffers
            for (int i = 0, n = tableOrder.size(); i < n; i++) {
                tableBuffers.get(tableOrder.get(i)).reset();
            }
            pendingRows = 0;
        } catch (IOException e) {
            throw new LineSenderException("Failed to flush: " + e.getMessage(), e);
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

    // ==================== Array methods (not supported in ILPv4) ====================

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[] values) {
        throw new LineSenderException("Array columns are not supported in ILPv4");
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][] values) {
        throw new LineSenderException("Array columns are not supported in ILPv4");
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][][] values) {
        throw new LineSenderException("Array columns are not supported in ILPv4");
    }

    @Override
    public Sender doubleArray(CharSequence name, DoubleArray array) {
        throw new LineSenderException("Array columns are not supported in ILPv4");
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[] values) {
        throw new LineSenderException("Array columns are not supported in ILPv4");
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][] values) {
        throw new LineSenderException("Array columns are not supported in ILPv4");
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][][] values) {
        throw new LineSenderException("Array columns are not supported in ILPv4");
    }

    @Override
    public Sender longArray(CharSequence name, LongArray array) {
        throw new LineSenderException("Array columns are not supported in ILPv4");
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

    private void sendHttp(long bufferAddress, int length) throws IOException {
        try {
            HttpClient.Request request = client.newRequest(host, port)
                    .POST()
                    .url(WRITE_PATH)
                    .header("Content-Type", CONTENT_TYPE);

            request.withContent();

            // Write the binary data directly from native memory to the request buffer
            request.putNonAscii(bufferAddress, bufferAddress + length);

            HttpClient.ResponseHeaders response = request.send(timeoutMs);
            response.await(timeoutMs);

            DirectUtf8Sequence statusCode = response.getStatusCode();

            // Check for 204 No Content (success)
            if (statusCode != null && statusCode.size() == 3
                    && statusCode.byteAt(0) == '2' && statusCode.byteAt(1) == '0' && statusCode.byteAt(2) == '4') {
                return;
            }

            // Check for 200 OK (success with content)
            if (statusCode != null && statusCode.size() == 3
                    && statusCode.byteAt(0) == '2' && statusCode.byteAt(1) == '0' && statusCode.byteAt(2) == '0') {
                // Read and parse response if chunked
                if (response.isChunked()) {
                    Response chunkedRsp = response.getResponse();
                    Fragment fragment;
                    int totalSize = 0;
                    byte[] responseData = new byte[4096];
                    while ((fragment = chunkedRsp.recv()) != null) {
                        int fragmentSize = (int) (fragment.hi() - fragment.lo());
                        if (totalSize + fragmentSize > responseData.length) {
                            byte[] newData = new byte[responseData.length * 2];
                            System.arraycopy(responseData, 0, newData, 0, totalSize);
                            responseData = newData;
                        }
                        for (long addr = fragment.lo(); addr < fragment.hi(); addr++) {
                            responseData[totalSize++] = io.questdb.std.Unsafe.getUnsafe().getByte(addr);
                        }
                    }
                    if (totalSize > 0) {
                        byte[] actualData = new byte[totalSize];
                        System.arraycopy(responseData, 0, actualData, 0, totalSize);
                        IlpV4Response ilpResponse = parseResponse(actualData);
                        if (ilpResponse.getStatusCode() != IlpV4StatusCode.OK) {
                            throw new IOException("Server error: " + IlpV4StatusCode.name(ilpResponse.getStatusCode()) +
                                    " - " + ilpResponse.getErrorMessage());
                        }
                    }
                }
                return;
            }

            // Error response
            errorSink.clear();
            if (response.isChunked()) {
                Response chunkedRsp = response.getResponse();
                Fragment fragment;
                while ((fragment = chunkedRsp.recv()) != null) {
                    Utf8s.utf8ToUtf16(fragment.lo(), fragment.hi(), errorSink);
                }
            }

            String errorMessage = errorSink.length() > 0 ? errorSink.toString() :
                    "HTTP " + (statusCode != null ? statusCode.toString() : "unknown");
            throw new IOException("HTTP request failed: " +
                    (statusCode != null ? statusCode.toString() : "unknown") + " - " + errorMessage);

        } catch (HttpClientException e) {
            throw new IOException("HTTP request failed: " + e.getMessage(), e);
        }
    }

    private IlpV4Response parseResponse(byte[] data) {
        if (data.length == 0) {
            return IlpV4Response.ok();
        }

        byte statusCode = data[0];
        if (statusCode == IlpV4StatusCode.OK) {
            return IlpV4Response.ok();
        }

        // Parse error response
        try {
            return IlpV4ResponseEncoder.decode(data, 0, data.length);
        } catch (IlpV4ParseException e) {
            return IlpV4Response.error(statusCode, "Failed to parse response");
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
