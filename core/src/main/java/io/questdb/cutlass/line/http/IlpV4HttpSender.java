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

import io.questdb.cutlass.line.tcp.v4.*;
import io.questdb.std.ObjList;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

/**
 * ILP v4 HTTP client sender for sending binary data to QuestDB over HTTP.
 * <p>
 * This class provides a fluent API identical to IlpV4Sender but uses HTTP
 * transport instead of TCP. The binary protocol is the same.
 * <p>
 * Example usage:
 * <pre>
 * try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", 9000)) {
 *     sender.table("weather")
 *           .symbol("city", "London")
 *           .doubleColumn("temperature", 23.5)
 *           .longColumn("humidity", 65)
 *           .at(timestamp);
 *     sender.flush();
 * }
 * </pre>
 */
public class IlpV4HttpSender implements Closeable {

    private static final String WRITE_PATH = "/write/v4";
    private static final String CONTENT_TYPE = "application/x-ilp-v4";
    private static final int DEFAULT_TIMEOUT_MS = 30000;

    private final String host;
    private final int port;
    private final IlpV4MessageEncoder encoder;
    private final Map<String, IlpV4TableBuffer> tableBuffers;
    private final ObjList<String> tableOrder;

    private IlpV4TableBuffer currentTable;
    private boolean gorillaEnabled;
    private boolean useSchemaRef;
    private int autoFlushRows;
    private int pendingRows;
    private int timeoutMs;

    private IlpV4HttpSender(String host, int port) {
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
    }

    /**
     * Creates a new sender and connects to the specified host and port.
     *
     * @param host server host
     * @param port server HTTP port
     * @return connected sender
     */
    public static IlpV4HttpSender connect(String host, int port) {
        return new IlpV4HttpSender(host, port);
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

    /**
     * Starts a new row for the specified table.
     *
     * @param tableName table name
     * @return this sender for chaining
     */
    public IlpV4HttpSender table(String tableName) {
        currentTable = tableBuffers.get(tableName);
        if (currentTable == null) {
            currentTable = new IlpV4TableBuffer(tableName);
            tableBuffers.put(tableName, currentTable);
            tableOrder.add(tableName);
        }
        return this;
    }

    /**
     * Adds a symbol column value.
     */
    public IlpV4HttpSender symbol(String columnName, String value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName, TYPE_SYMBOL, true);
        col.addSymbol(value);
        return this;
    }

    /**
     * Adds a boolean column value.
     */
    public IlpV4HttpSender boolColumn(String columnName, boolean value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName, TYPE_BOOLEAN, false);
        col.addBoolean(value);
        return this;
    }

    /**
     * Adds a long column value.
     */
    public IlpV4HttpSender longColumn(String columnName, long value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName, TYPE_LONG, false);
        col.addLong(value);
        return this;
    }

    /**
     * Adds a byte column value.
     */
    public IlpV4HttpSender byteColumn(String columnName, byte value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName, TYPE_BYTE, false);
        col.addByte(value);
        return this;
    }

    /**
     * Adds a short column value.
     */
    public IlpV4HttpSender shortColumn(String columnName, short value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName, TYPE_SHORT, false);
        col.addShort(value);
        return this;
    }

    /**
     * Adds an int column value.
     */
    public IlpV4HttpSender intColumn(String columnName, int value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName, TYPE_INT, false);
        col.addInt(value);
        return this;
    }

    /**
     * Adds a double column value.
     */
    public IlpV4HttpSender doubleColumn(String columnName, double value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName, TYPE_DOUBLE, false);
        col.addDouble(value);
        return this;
    }

    /**
     * Adds a float column value.
     */
    public IlpV4HttpSender floatColumn(String columnName, float value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName, TYPE_FLOAT, false);
        col.addFloat(value);
        return this;
    }

    /**
     * Adds a string column value.
     */
    public IlpV4HttpSender stringColumn(String columnName, String value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName, TYPE_STRING, true);
        col.addString(value);
        return this;
    }

    /**
     * Adds a timestamp column value (microseconds since epoch).
     */
    public IlpV4HttpSender timestampColumn(String columnName, long value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName, TYPE_TIMESTAMP, true);
        col.addLong(value);
        return this;
    }

    /**
     * Adds a UUID column value.
     */
    public IlpV4HttpSender uuidColumn(String columnName, long high, long low) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName, TYPE_UUID, true);
        col.addUuid(high, low);
        return this;
    }

    /**
     * Sets the designated timestamp and completes the row.
     *
     * @param timestampMicros timestamp in microseconds since epoch
     * @return this sender for chaining
     * @throws IOException if auto-flush fails
     */
    public IlpV4HttpSender at(long timestampMicros) throws IOException {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn("timestamp", TYPE_TIMESTAMP, false);
        col.addLong(timestampMicros);
        return finishRow();
    }

    /**
     * Uses server timestamp and completes the row.
     *
     * @return this sender for chaining
     * @throws IOException if auto-flush fails
     */
    public IlpV4HttpSender atNow() throws IOException {
        return at(System.currentTimeMillis() * 1000L);
    }

    private IlpV4HttpSender finishRow() throws IOException {
        checkCurrentTable();
        currentTable.nextRow();
        pendingRows++;

        if (autoFlushRows > 0 && pendingRows >= autoFlushRows) {
            flush();
        }
        return this;
    }

    /**
     * Flushes all pending data to the server via HTTP POST.
     *
     * @throws IOException if sending fails
     */
    public void flush() throws IOException {
        if (tableOrder.size() == 0) {
            return;
        }

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

        // Send the message via HTTP
        byte[] data = encoder.toByteArray();
        sendHttp(data);

        // Clear buffers
        for (int i = 0, n = tableOrder.size(); i < n; i++) {
            tableBuffers.get(tableOrder.get(i)).reset();
        }
        pendingRows = 0;
    }

    private void sendHttp(byte[] data) throws IOException {
        URL url = new URL("http", host, port, WRITE_PATH);
        System.out.println("[IlpV4HttpSender] Sending " + data.length + " bytes to " + url);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        try {
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setConnectTimeout(timeoutMs);
            conn.setReadTimeout(timeoutMs);
            conn.setRequestProperty("Content-Type", CONTENT_TYPE);
            conn.setRequestProperty("Content-Length", String.valueOf(data.length));

            // Send data
            try (OutputStream out = conn.getOutputStream()) {
                out.write(data);
                out.flush();
            }

            // Check response
            int responseCode = conn.getResponseCode();
            if (responseCode == 204) {
                // Success - no content
                return;
            }

            if (responseCode == 200) {
                // Success with content - read response
                try (InputStream in = conn.getInputStream()) {
                    byte[] responseData = in.readAllBytes();
                    if (responseData.length > 0) {
                        IlpV4Response response = parseResponse(responseData);
                        if (response.getStatusCode() != IlpV4StatusCode.OK) {
                            throw new IOException("Server error: " + IlpV4StatusCode.name(response.getStatusCode()) +
                                    " - " + response.getErrorMessage());
                        }
                    }
                }
                return;
            }

            // Error response
            String errorMessage;
            try (InputStream err = conn.getErrorStream()) {
                if (err != null) {
                    byte[] errorData = err.readAllBytes();
                    errorMessage = new String(errorData);
                } else {
                    errorMessage = "HTTP " + responseCode;
                }
            }
            throw new IOException("HTTP request failed: " + responseCode + " - " + errorMessage);
        } finally {
            conn.disconnect();
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
            throw new IllegalStateException("No table selected. Call table() first.");
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
    }
}
