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

package io.questdb.cutlass.line.tcp.v4;

import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.array.DoubleArray;
import io.questdb.cutlass.line.array.LongArray;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.bytes.DirectByteSlice;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

/**
 * ILP v4 client sender for sending data to QuestDB.
 * <p>
 * This class provides a fluent API for constructing and sending ILP v4 messages
 * and implements the standard {@link Sender} interface.
 * <p>
 * Example usage:
 * <pre>
 * try (IlpV4Sender sender = IlpV4Sender.connect("localhost", 9009)) {
 *     sender.table("weather")
 *           .symbol("city", "London")
 *           .doubleColumn("temperature", 23.5)
 *           .longColumn("humidity", 65)
 *           .atNow();
 *     sender.flush();
 * }
 * </pre>
 */
public class IlpV4Sender implements Sender {

    private final Socket socket;
    private final OutputStream out;
    private final InputStream in;
    private final IlpV4MessageEncoder encoder;
    private final Map<String, IlpV4TableBuffer> tableBuffers;
    private final ObjList<String> tableOrder;
    private final byte[] responseBuffer;

    private IlpV4TableBuffer currentTable;
    private boolean gorillaEnabled;
    private boolean useSchemaRef;
    private int autoFlushRows;
    private int pendingRows;

    // Negotiated capabilities
    private byte negotiatedVersion;
    private short negotiatedCaps;

    private IlpV4Sender(Socket socket, OutputStream out, InputStream in) {
        this.socket = socket;
        this.out = out;
        this.in = in;
        this.encoder = new IlpV4MessageEncoder();
        this.tableBuffers = new HashMap<>();
        this.tableOrder = new ObjList<>();
        this.responseBuffer = new byte[1024];
        this.currentTable = null;
        this.gorillaEnabled = true;
        this.useSchemaRef = false;
        this.autoFlushRows = 0;
        this.pendingRows = 0;
    }

    /**
     * Creates a new sender and connects to the specified host and port.
     *
     * @param host server host
     * @param port server port
     * @return connected sender
     * @throws IOException if connection fails
     */
    public static IlpV4Sender connect(String host, int port) throws IOException {
        Socket socket = new Socket(host, port);
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(30000);

        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();

        IlpV4Sender sender = new IlpV4Sender(socket, out, in);
        sender.performHandshake();
        return sender;
    }

    /**
     * Creates a sender without performing handshake (for testing).
     */
    public static IlpV4Sender createWithoutHandshake(Socket socket, OutputStream out, InputStream in) {
        return new IlpV4Sender(socket, out, in);
    }

    /**
     * Performs the capability negotiation handshake.
     */
    private void performHandshake() throws IOException {
        // Send capability request
        byte[] request = new byte[CAPABILITY_REQUEST_SIZE];
        request[0] = 'I';
        request[1] = 'L';
        request[2] = 'P';
        request[3] = '?';
        request[4] = VERSION_1; // min version
        request[5] = VERSION_1; // max version
        short flags = FLAG_GORILLA; // Request Gorilla support
        request[6] = (byte) (flags & 0xFF);
        request[7] = (byte) ((flags >> 8) & 0xFF);

        out.write(request);
        out.flush();

        // Read capability response
        byte[] response = new byte[CAPABILITY_RESPONSE_SIZE];
        int bytesRead = 0;
        while (bytesRead < CAPABILITY_RESPONSE_SIZE) {
            int n = in.read(response, bytesRead, CAPABILITY_RESPONSE_SIZE - bytesRead);
            if (n < 0) {
                throw new IOException("Connection closed during handshake");
            }
            bytesRead += n;
        }

        // Parse response
        int magic = (response[0] & 0xFF) |
                ((response[1] & 0xFF) << 8) |
                ((response[2] & 0xFF) << 16) |
                ((response[3] & 0xFF) << 24);

        if (magic == MAGIC_FALLBACK) {
            throw new IOException("Server does not support ILP v4");
        }

        if (magic != MAGIC_CAPABILITY_RESPONSE) {
            throw new IOException("Invalid handshake response magic: " + Integer.toHexString(magic));
        }

        negotiatedVersion = response[4];
        // response[5] is reserved
        negotiatedCaps = (short) ((response[6] & 0xFF) | ((response[7] & 0xFF) << 8));

        gorillaEnabled = (negotiatedCaps & FLAG_GORILLA) != 0;
        encoder.setGorillaEnabled(gorillaEnabled);
    }

    /**
     * Returns the negotiated protocol version.
     */
    public byte getNegotiatedVersion() {
        return negotiatedVersion;
    }

    /**
     * Returns the negotiated capabilities.
     */
    public short getNegotiatedCapabilities() {
        return negotiatedCaps;
    }

    /**
     * Returns whether Gorilla encoding is enabled.
     */
    public boolean isGorillaEnabled() {
        return gorillaEnabled;
    }

    /**
     * Sets whether to use schema reference mode.
     */
    public IlpV4Sender useSchemaReference(boolean use) {
        this.useSchemaRef = use;
        return this;
    }

    /**
     * Sets auto-flush threshold (0 = disabled).
     */
    public IlpV4Sender autoFlushRows(int rows) {
        this.autoFlushRows = rows;
        return this;
    }

    // ==================== Sender interface implementation ====================
    // Note: Methods return IlpV4Sender (covariant return) to enable chaining with ILPv4-specific methods

    @Override
    public IlpV4Sender table(CharSequence tableName) {
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
    public IlpV4Sender symbol(CharSequence columnName, CharSequence value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_SYMBOL, true);
        col.addSymbol(value.toString());
        return this;
    }

    @Override
    public IlpV4Sender boolColumn(CharSequence columnName, boolean value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_BOOLEAN, false);
        col.addBoolean(value);
        return this;
    }

    @Override
    public IlpV4Sender longColumn(CharSequence columnName, long value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_LONG, false);
        col.addLong(value);
        return this;
    }

    /**
     * Adds an int column value.
     */
    public IlpV4Sender intColumn(CharSequence columnName, int value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_INT, false);
        col.addInt(value);
        return this;
    }

    @Override
    public IlpV4Sender doubleColumn(CharSequence columnName, double value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_DOUBLE, false);
        col.addDouble(value);
        return this;
    }

    /**
     * Adds a float column value.
     */
    public IlpV4Sender floatColumn(CharSequence columnName, float value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_FLOAT, false);
        col.addFloat(value);
        return this;
    }

    @Override
    public IlpV4Sender stringColumn(CharSequence columnName, CharSequence value) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_STRING, true);
        col.addString(value.toString());
        return this;
    }

    @Override
    public IlpV4Sender timestampColumn(CharSequence columnName, long value, ChronoUnit unit) {
        long micros = toMicros(value, unit);
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_TIMESTAMP, true);
        col.addLong(micros);
        return this;
    }

    @Override
    public IlpV4Sender timestampColumn(CharSequence columnName, Instant value) {
        long micros = value.getEpochSecond() * 1_000_000L + value.getNano() / 1000L;
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_TIMESTAMP, true);
        col.addLong(micros);
        return this;
    }

    /**
     * Adds a timestamp column value (microseconds since epoch).
     */
    public IlpV4Sender timestampColumn(CharSequence columnName, long valueMicros) {
        checkCurrentTable();
        IlpV4TableBuffer.ColumnBuffer col = currentTable.getOrCreateColumn(columnName.toString(), TYPE_TIMESTAMP, true);
        col.addLong(valueMicros);
        return this;
    }

    /**
     * Adds a UUID column value.
     */
    public IlpV4Sender uuidColumn(CharSequence columnName, long high, long low) {
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
            encoder.setGorillaEnabled(gorillaEnabled); // Restore Gorilla flag after reset

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

            // Send the message
            byte[] data = encoder.toByteArray();
            out.write(data);
            out.flush();

            // Read response
            IlpV4Response response = readResponse();

            if (response.getStatusCode() != IlpV4StatusCode.OK) {
                if (response.getStatusCode() == IlpV4StatusCode.PARTIAL) {
                    throw new LineSenderException("Partial failure: " + formatPartialErrors(response));
                } else {
                    throw new LineSenderException("Server error: " + IlpV4StatusCode.name(response.getStatusCode()) +
                            " - " + response.getErrorMessage());
                }
            }

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
        throw new LineSenderException("bufferView() is not supported for ILPv4 TCP sender");
    }

    @Override
    public void cancelRow() {
        throw new LineSenderException("cancelRow() is not supported for TCP transport");
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

    private IlpV4Response readResponse() throws IOException {
        // Read at least 1 byte (status code)
        int bytesRead = in.read(responseBuffer, 0, 1);
        if (bytesRead < 0) {
            throw new IOException("Connection closed while reading response");
        }

        byte statusCode = responseBuffer[0];

        if (statusCode == IlpV4StatusCode.OK) {
            return IlpV4Response.ok();
        }

        // Read more data for error responses
        int totalRead = 1;
        while (totalRead < responseBuffer.length) {
            int available = in.available();
            if (available == 0) {
                break;
            }
            int n = in.read(responseBuffer, totalRead, Math.min(available, responseBuffer.length - totalRead));
            if (n <= 0) {
                break;
            }
            totalRead += n;
        }

        try {
            return IlpV4ResponseEncoder.decode(responseBuffer, 0, totalRead);
        } catch (IlpV4ParseException e) {
            throw new IOException("Failed to parse response: " + e.getMessage(), e);
        }
    }

    private String formatPartialErrors(IlpV4Response response) {
        StringBuilder sb = new StringBuilder();
        ObjList<IlpV4Response.TableError> errors = response.getTableErrors();
        if (errors != null) {
            for (int i = 0; i < errors.size(); i++) {
                IlpV4Response.TableError err = errors.get(i);
                if (i > 0) {
                    sb.append("; ");
                }
                sb.append("table[").append(err.getTableIndex()).append("]: ");
                sb.append(IlpV4StatusCode.name(err.getErrorCode()));
                if (err.getErrorMessage() != null) {
                    sb.append(" - ").append(err.getErrorMessage());
                }
            }
        }
        return sb.toString();
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
        Misc.free(socket);
    }
}
