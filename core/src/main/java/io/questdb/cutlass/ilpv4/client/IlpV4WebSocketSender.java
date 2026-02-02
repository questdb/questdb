/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.cutlass.ilpv4.client;

import io.questdb.cutlass.ilpv4.protocol.*;

import io.questdb.client.Sender;
import io.questdb.cutlass.http.client.WebSocketClient;
import io.questdb.cutlass.http.client.WebSocketClientFactory;
import io.questdb.cutlass.http.client.WebSocketFrameHandler;

import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.array.DoubleArray;
import io.questdb.cutlass.line.array.LongArray;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.LongHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.bytes.DirectByteSlice;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static io.questdb.cutlass.ilpv4.protocol.IlpV4Constants.*;

/**
 * ILP v4 WebSocket client sender for streaming data to QuestDB.
 * <p>
 * This sender uses a double-buffering scheme with asynchronous I/O for high throughput:
 * <ul>
 *   <li>User thread writes rows to the active microbatch buffer</li>
 *   <li>When buffer is full (row count, byte size, or age), it's sealed and enqueued</li>
 *   <li>A dedicated I/O thread sends batches asynchronously</li>
 *   <li>Double-buffering ensures one buffer is always available for writing</li>
 * </ul>
 * <p>
 * Configuration options:
 * <ul>
 *   <li>{@code autoFlushRows} - Maximum rows per batch (default: 500)</li>
 *   <li>{@code autoFlushBytes} - Maximum bytes per batch (default: 1MB)</li>
 *   <li>{@code autoFlushIntervalNanos} - Maximum age before auto-flush (default: 100ms)</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>
 * try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connect("localhost", 9000)) {
 *     for (int i = 0; i &lt; 100_000; i++) {
 *         sender.table("metrics")
 *               .symbol("host", "server-" + (i % 10))
 *               .doubleColumn("cpu", Math.random() * 100)
 *               .atNow();
 *         // Rows are batched and sent asynchronously!
 *     }
 *     // flush() waits for all pending batches to be sent
 *     sender.flush();
 * }
 * </pre>
 */
public class IlpV4WebSocketSender implements Sender {

    private static final Log LOG = LogFactory.getLog(IlpV4WebSocketSender.class);

    private static final int DEFAULT_BUFFER_SIZE = 8192;
    private static final int DEFAULT_MICROBATCH_BUFFER_SIZE = 1024 * 1024; // 1MB
    public static final int DEFAULT_AUTO_FLUSH_ROWS = 500;
    public static final int DEFAULT_AUTO_FLUSH_BYTES = 1024 * 1024; // 1MB
    public static final long DEFAULT_AUTO_FLUSH_INTERVAL_NANOS = 100_000_000L; // 100ms
    public static final int DEFAULT_IN_FLIGHT_WINDOW_SIZE = InFlightWindow.DEFAULT_WINDOW_SIZE; // 8
    public static final int DEFAULT_SEND_QUEUE_CAPACITY = WebSocketSendQueue.DEFAULT_QUEUE_CAPACITY; // 16
    private static final String WRITE_PATH = "/write/v4";

    private final String host;
    private final int port;
    private final boolean tlsEnabled;
    private final CharSequenceObjHashMap<IlpV4TableBuffer> tableBuffers;
    private IlpV4TableBuffer currentTableBuffer;
    private String currentTableName;
    // Cached column references to avoid repeated hashmap lookups
    private IlpV4TableBuffer.ColumnBuffer cachedTimestampColumn;
    private IlpV4TableBuffer.ColumnBuffer cachedTimestampNanosColumn;

    // Encoder for ILP v4 messages
    private final IlpV4WebSocketEncoder encoder;

    // WebSocket client (zero-GC native implementation)
    private WebSocketClient client;
    private boolean connected;
    private boolean closed;

    // Double-buffering for async I/O
    private MicrobatchBuffer buffer0;
    private MicrobatchBuffer buffer1;
    private MicrobatchBuffer activeBuffer;
    private WebSocketSendQueue sendQueue;

    // Flow control
    private InFlightWindow inFlightWindow;

    // Auto-flush configuration
    private final int autoFlushRows;
    private final int autoFlushBytes;
    private final long autoFlushIntervalNanos;

    // Flow control configuration
    private final int inFlightWindowSize;
    private final int sendQueueCapacity;

    // Configuration
    private boolean gorillaEnabled = true;

    // Async mode: pending row tracking
    private int pendingRowCount;
    private long firstPendingRowTimeNanos;

    // Batch sequence counter (must match server's messageSequence)
    private long nextBatchSequence = 0;

    // Global symbol dictionary for delta encoding
    private final GlobalSymbolDictionary globalSymbolDictionary;

    // Track max global symbol ID used in current batch (for delta calculation)
    private int currentBatchMaxSymbolId = -1;

    // Track highest symbol ID sent to server (for delta encoding)
    // Once sent over TCP, server is guaranteed to receive it (or connection dies)
    private volatile int maxSentSymbolId = -1;

    // Track schema hashes that have been sent to the server (for schema reference mode)
    // First time we send a schema: full schema. Subsequent times: 8-byte hash reference.
    // Combined key = schemaHash XOR (tableNameHash << 32) to include table name in lookup.
    private final LongHashSet sentSchemaHashes = new LongHashSet();

    private IlpV4WebSocketSender(String host, int port, boolean tlsEnabled, int bufferSize,
                                 int autoFlushRows, int autoFlushBytes, long autoFlushIntervalNanos,
                                 int inFlightWindowSize, int sendQueueCapacity) {
        this.host = host;
        this.port = port;
        this.tlsEnabled = tlsEnabled;
        this.encoder = new IlpV4WebSocketEncoder(bufferSize);
        this.tableBuffers = new CharSequenceObjHashMap<>();
        this.currentTableBuffer = null;
        this.currentTableName = null;
        this.connected = false;
        this.closed = false;
        this.autoFlushRows = autoFlushRows;
        this.autoFlushBytes = autoFlushBytes;
        this.autoFlushIntervalNanos = autoFlushIntervalNanos;
        this.inFlightWindowSize = inFlightWindowSize;
        this.sendQueueCapacity = sendQueueCapacity;

        // Initialize global symbol dictionary for delta encoding
        this.globalSymbolDictionary = new GlobalSymbolDictionary();

        // Initialize double-buffering if async mode (window > 1)
        if (inFlightWindowSize > 1) {
            int microbatchBufferSize = Math.max(DEFAULT_MICROBATCH_BUFFER_SIZE, autoFlushBytes * 2);
            this.buffer0 = new MicrobatchBuffer(microbatchBufferSize, autoFlushRows, autoFlushBytes, autoFlushIntervalNanos);
            this.buffer1 = new MicrobatchBuffer(microbatchBufferSize, autoFlushRows, autoFlushBytes, autoFlushIntervalNanos);
            this.activeBuffer = buffer0;
        }
    }

    /**
     * Creates a new sender and connects to the specified host and port.
     * Uses synchronous mode for backward compatibility.
     *
     * @param host server host
     * @param port server HTTP port (WebSocket upgrade happens on same port)
     * @return connected sender
     */
    public static IlpV4WebSocketSender connect(String host, int port) {
        return connect(host, port, false);
    }

    /**
     * Creates a new sender with TLS and connects to the specified host and port.
     * Uses synchronous mode for backward compatibility.
     *
     * @param host       server host
     * @param port       server HTTP port
     * @param tlsEnabled whether to use TLS
     * @return connected sender
     */
    public static IlpV4WebSocketSender connect(String host, int port, boolean tlsEnabled) {
        IlpV4WebSocketSender sender = new IlpV4WebSocketSender(
                host, port, tlsEnabled, DEFAULT_BUFFER_SIZE,
                0, 0, 0, // No auto-flush in sync mode
                1, 1    // window=1 for sync behavior, queue=1 (not used)
        );
        sender.ensureConnected();
        return sender;
    }

    /**
     * Creates a new sender with async mode and custom configuration.
     *
     * @param host                    server host
     * @param port                    server HTTP port
     * @param tlsEnabled              whether to use TLS
     * @param autoFlushRows           rows per batch (0 = no limit)
     * @param autoFlushBytes          bytes per batch (0 = no limit)
     * @param autoFlushIntervalNanos  age before flush in nanos (0 = no limit)
     * @return connected sender
     */
    public static IlpV4WebSocketSender connectAsync(String host, int port, boolean tlsEnabled,
                                                    int autoFlushRows, int autoFlushBytes,
                                                    long autoFlushIntervalNanos) {
        return connectAsync(host, port, tlsEnabled, autoFlushRows, autoFlushBytes, autoFlushIntervalNanos,
                DEFAULT_IN_FLIGHT_WINDOW_SIZE, DEFAULT_SEND_QUEUE_CAPACITY);
    }

    /**
     * Creates a new sender with async mode and full configuration including flow control.
     *
     * @param host                    server host
     * @param port                    server HTTP port
     * @param tlsEnabled              whether to use TLS
     * @param autoFlushRows           rows per batch (0 = no limit)
     * @param autoFlushBytes          bytes per batch (0 = no limit)
     * @param autoFlushIntervalNanos  age before flush in nanos (0 = no limit)
     * @param inFlightWindowSize      max batches awaiting server ACK (default: 8)
     * @param sendQueueCapacity       max batches waiting to send (default: 16)
     * @return connected sender
     */
    public static IlpV4WebSocketSender connectAsync(String host, int port, boolean tlsEnabled,
                                                    int autoFlushRows, int autoFlushBytes,
                                                    long autoFlushIntervalNanos,
                                                    int inFlightWindowSize, int sendQueueCapacity) {
        IlpV4WebSocketSender sender = new IlpV4WebSocketSender(
                host, port, tlsEnabled, DEFAULT_BUFFER_SIZE,
                autoFlushRows, autoFlushBytes, autoFlushIntervalNanos,
                inFlightWindowSize, sendQueueCapacity
        );
        sender.ensureConnected();
        return sender;
    }

    /**
     * Creates a new sender with async mode and default configuration.
     *
     * @param host       server host
     * @param port       server HTTP port
     * @param tlsEnabled whether to use TLS
     * @return connected sender
     */
    public static IlpV4WebSocketSender connectAsync(String host, int port, boolean tlsEnabled) {
        return connectAsync(host, port, tlsEnabled,
                DEFAULT_AUTO_FLUSH_ROWS, DEFAULT_AUTO_FLUSH_BYTES, DEFAULT_AUTO_FLUSH_INTERVAL_NANOS);
    }

    /**
     * Factory method for SenderBuilder integration.
     */
    public static IlpV4WebSocketSender create(
            String host,
            int port,
            boolean tlsEnabled,
            int bufferSize,
            String authToken,
            String username,
            String password
    ) {
        IlpV4WebSocketSender sender = new IlpV4WebSocketSender(
                host, port, tlsEnabled, bufferSize,
                0, 0, 0,
                1, 1    // window=1 for sync behavior
        );
        // TODO: Store auth credentials for connection
        sender.ensureConnected();
        return sender;
    }

    /**
     * Creates a sender without connecting. For testing only.
     * <p>
     * This allows unit tests to test sender logic without requiring a real server.
     *
     * @param host              server host (not connected)
     * @param port              server port (not connected)
     * @param inFlightWindowSize window size: 1 for sync behavior, >1 for async
     * @return unconnected sender
     */
    public static IlpV4WebSocketSender createForTesting(String host, int port, int inFlightWindowSize) {
        return new IlpV4WebSocketSender(
                host, port, false, DEFAULT_BUFFER_SIZE,
                0, 0, 0,
                inFlightWindowSize, DEFAULT_SEND_QUEUE_CAPACITY
        );
        // Note: does NOT call ensureConnected()
    }

    /**
     * Creates a sender with custom flow control settings without connecting. For testing only.
     *
     * @param host                   server host (not connected)
     * @param port                   server port (not connected)
     * @param autoFlushRows          rows per batch (0 = no limit)
     * @param autoFlushBytes         bytes per batch (0 = no limit)
     * @param autoFlushIntervalNanos age before flush in nanos (0 = no limit)
     * @param inFlightWindowSize     window size: 1 for sync behavior, >1 for async
     * @param sendQueueCapacity      max batches waiting to send
     * @return unconnected sender
     */
    public static IlpV4WebSocketSender createForTesting(
            String host, int port,
            int autoFlushRows, int autoFlushBytes, long autoFlushIntervalNanos,
            int inFlightWindowSize, int sendQueueCapacity) {
        return new IlpV4WebSocketSender(
                host, port, false, DEFAULT_BUFFER_SIZE,
                autoFlushRows, autoFlushBytes, autoFlushIntervalNanos,
                inFlightWindowSize, sendQueueCapacity
        );
        // Note: does NOT call ensureConnected()
    }

    private void ensureConnected() {
        if (closed) {
            throw new LineSenderException("Sender is closed");
        }
        if (!connected) {
            // Create WebSocket client using factory (zero-GC native implementation)
            if (tlsEnabled) {
                client = WebSocketClientFactory.newInsecureTlsInstance();
            } else {
                client = WebSocketClientFactory.newPlainTextInstance();
            }

            // Connect and upgrade to WebSocket
            try {
                client.connect(host, port);
                client.upgrade(WRITE_PATH);
            } catch (Exception e) {
                client.close();
                client = null;
                throw new LineSenderException("Failed to connect to " + host + ":" + port, e);
            }

            // a window for tracking batches awaiting ACK (both modes)
            inFlightWindow = new InFlightWindow(inFlightWindowSize, InFlightWindow.DEFAULT_TIMEOUT_MS);

            // Initialize send queue for async mode (window > 1)
            // The send queue handles both sending AND receiving (single I/O thread)
            if (inFlightWindowSize > 1) {
                sendQueue = new WebSocketSendQueue(client, inFlightWindow,
                        sendQueueCapacity,
                        WebSocketSendQueue.DEFAULT_ENQUEUE_TIMEOUT_MS,
                        WebSocketSendQueue.DEFAULT_SHUTDOWN_TIMEOUT_MS);
            }
            // Sync mode (window=1): no send queue - we send and read ACKs synchronously

            // Clear sent schema hashes - server starts fresh on each connection
            sentSchemaHashes.clear();

            connected = true;
            LOG.info().$("Connected to WebSocket [host=").$(host)
                    .$(", port=").$(port)
                    .$(", windowSize=").$(inFlightWindowSize).I$();
        }
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
    public IlpV4WebSocketSender setGorillaEnabled(boolean enabled) {
        this.gorillaEnabled = enabled;
        this.encoder.setGorillaEnabled(enabled);
        return this;
    }

    /**
     * Returns whether async mode is enabled (window size > 1).
     */
    public boolean isAsyncMode() {
        return inFlightWindowSize > 1;
    }

    /**
     * Returns the in-flight window size.
     * Window=1 means sync mode, window>1 means async mode.
     */
    public int getInFlightWindowSize() {
        return inFlightWindowSize;
    }

    /**
     * Returns the send queue capacity.
     */
    public int getSendQueueCapacity() {
        return sendQueueCapacity;
    }

    /**
     * Returns the auto-flush row threshold.
     */
    public int getAutoFlushRows() {
        return autoFlushRows;
    }

    /**
     * Returns the auto-flush byte threshold.
     */
    public int getAutoFlushBytes() {
        return autoFlushBytes;
    }

    /**
     * Returns the auto-flush interval in nanoseconds.
     */
    public long getAutoFlushIntervalNanos() {
        return autoFlushIntervalNanos;
    }

    /**
     * Returns the global symbol dictionary.
     * For testing and encoder integration.
     */
    public GlobalSymbolDictionary getGlobalSymbolDictionary() {
        return globalSymbolDictionary;
    }

    /**
     * Returns the max symbol ID sent to the server.
     * Once sent over TCP, server is guaranteed to receive it (or connection dies).
     */
    public int getMaxSentSymbolId() {
        return maxSentSymbolId;
    }

    // ==================== Fast-path API for high-throughput generators ====================
    //
    // These methods bypass the normal fluent API to avoid per-row overhead:
    // - No hashmap lookups for column names
    // - No checkNotClosed()/checkTableSelected() per column
    // - Direct access to column buffers
    //
    // Usage:
    //   // Setup (once)
    //   IlpV4TableBuffer tableBuffer = sender.getTableBuffer("q");
    //   IlpV4TableBuffer.ColumnBuffer colSymbol = tableBuffer.getOrCreateColumn("s", TYPE_SYMBOL, true);
    //   IlpV4TableBuffer.ColumnBuffer colBid = tableBuffer.getOrCreateColumn("b", TYPE_DOUBLE, false);
    //
    //   // Hot path (per row)
    //   colSymbol.addSymbolWithGlobalId(symbol, sender.getOrAddGlobalSymbol(symbol));
    //   colBid.addDouble(bid);
    //   tableBuffer.nextRow();
    //   sender.incrementPendingRowCount();

    /**
     * Gets or creates a table buffer for direct access.
     * For high-throughput generators that want to bypass fluent API overhead.
     */
    public IlpV4TableBuffer getTableBuffer(String tableName) {
        IlpV4TableBuffer buffer = tableBuffers.get(tableName);
        if (buffer == null) {
            buffer = new IlpV4TableBuffer(tableName);
            tableBuffers.put(tableName, buffer);
        }
        currentTableBuffer = buffer;
        currentTableName = tableName;
        return buffer;
    }

    /**
     * Registers a symbol in the global dictionary and returns its ID.
     * For use with fast-path column buffer access.
     */
    public int getOrAddGlobalSymbol(String value) {
        int globalId = globalSymbolDictionary.getOrAddSymbol(value);
        if (globalId > currentBatchMaxSymbolId) {
            currentBatchMaxSymbolId = globalId;
        }
        return globalId;
    }

    /**
     * Increments the pending row count for auto-flush tracking.
     * Call this after adding a complete row via fast-path API.
     * Triggers auto-flush if any threshold is exceeded.
     */
    public void incrementPendingRowCount() {
        if (pendingRowCount == 0) {
            firstPendingRowTimeNanos = System.nanoTime();
        }
        pendingRowCount++;

        // Check if any flush threshold is exceeded (same as sendRow())
        if (shouldAutoFlush()) {
            if (inFlightWindowSize > 1) {
                flushPendingRows();
            } else {
                // Sync mode (window=1): flush directly with ACK wait
                flushSync();
            }
        }
    }

    // ==================== Sender interface implementation ====================

    @Override
    public IlpV4WebSocketSender table(CharSequence tableName) {
        checkNotClosed();
        // Fast path: if table name matches current, skip hashmap lookup
        if (currentTableName != null && currentTableBuffer != null && Chars.equals(tableName, currentTableName)) {
            return this;
        }
        // Table changed - invalidate cached column references
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        currentTableName = tableName.toString();
        currentTableBuffer = tableBuffers.get(currentTableName);
        if (currentTableBuffer == null) {
            currentTableBuffer = new IlpV4TableBuffer(currentTableName);
            tableBuffers.put(currentTableName, currentTableBuffer);
        }
        // Both modes accumulate rows until flush
        return this;
    }

    @Override
    public IlpV4WebSocketSender symbol(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_SYMBOL, true);

        if (value != null) {
            // Register symbol in global dictionary and track max ID for delta calculation
            String symbolValue = value.toString();
            int globalId = globalSymbolDictionary.getOrAddSymbol(symbolValue);
            if (globalId > currentBatchMaxSymbolId) {
                currentBatchMaxSymbolId = globalId;
            }
            // Store global ID in the column buffer
            col.addSymbolWithGlobalId(symbolValue, globalId);
        } else {
            col.addSymbol(null);
        }
        return this;
    }

    @Override
    public IlpV4WebSocketSender boolColumn(CharSequence columnName, boolean value) {
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_BOOLEAN, false);
        col.addBoolean(value);
        return this;
    }

    @Override
    public IlpV4WebSocketSender longColumn(CharSequence columnName, long value) {
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_LONG, false);
        col.addLong(value);
        return this;
    }

    @Override
    public IlpV4WebSocketSender doubleColumn(CharSequence columnName, double value) {
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_DOUBLE, false);
        col.addDouble(value);
        return this;
    }

    @Override
    public IlpV4WebSocketSender stringColumn(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_STRING, true);
        col.addString(value != null ? value.toString() : null);
        return this;
    }

    /**
     * Adds a CHAR column value to the current row.
     * <p>
     * CHAR is stored as a 2-byte SHORT (UTF-16 code unit) in QuestDB.
     *
     * @param columnName the column name
     * @param value      the character value
     * @return this sender for method chaining
     */
    public IlpV4WebSocketSender charColumn(CharSequence columnName, char value) {
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_SHORT, false);
        col.addShort((short) value);
        return this;
    }

    /**
     * Adds a UUID column value to the current row.
     *
     * @param columnName the column name
     * @param lo         the low 64 bits of the UUID
     * @param hi         the high 64 bits of the UUID
     * @return this sender for method chaining
     */
    public IlpV4WebSocketSender uuidColumn(CharSequence columnName, long lo, long hi) {
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_UUID, true);
        col.addUuid(hi, lo);
        return this;
    }

    /**
     * Adds a LONG256 column value to the current row.
     *
     * @param columnName the column name
     * @param l0         the least significant 64 bits
     * @param l1         the second 64 bits
     * @param l2         the third 64 bits
     * @param l3         the most significant 64 bits
     * @return this sender for method chaining
     */
    public IlpV4WebSocketSender long256Column(CharSequence columnName, long l0, long l1, long l2, long l3) {
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_LONG256, true);
        col.addLong256(l0, l1, l2, l3);
        return this;
    }

    @Override
    public IlpV4WebSocketSender timestampColumn(CharSequence columnName, long value, ChronoUnit unit) {
        checkNotClosed();
        checkTableSelected();
        if (unit == ChronoUnit.NANOS) {
            IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_TIMESTAMP_NANOS, true);
            col.addLong(value);
        } else {
            long micros = toMicros(value, unit);
            IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_TIMESTAMP, true);
            col.addLong(micros);
        }
        return this;
    }

    @Override
    public IlpV4WebSocketSender timestampColumn(CharSequence columnName, Instant value) {
        checkNotClosed();
        checkTableSelected();
        long micros = value.getEpochSecond() * 1_000_000L + value.getNano() / 1000L;
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_TIMESTAMP, true);
        col.addLong(micros);
        return this;
    }

    @Override
    public void at(long timestamp, ChronoUnit unit) {
        checkNotClosed();
        checkTableSelected();
        if (unit == ChronoUnit.NANOS) {
            atNanos(timestamp);
        } else {
            long micros = toMicros(timestamp, unit);
            atMicros(micros);
        }
    }

    @Override
    public void at(Instant timestamp) {
        checkNotClosed();
        checkTableSelected();
        long micros = timestamp.getEpochSecond() * 1_000_000L + timestamp.getNano() / 1000L;
        atMicros(micros);
    }

    private void atMicros(long timestampMicros) {
        // Add designated timestamp column (empty name for designated timestamp)
        // Use cached reference to avoid hashmap lookup per row
        if (cachedTimestampColumn == null) {
            cachedTimestampColumn = currentTableBuffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);
        }
        cachedTimestampColumn.addLong(timestampMicros);
        sendRow();
    }

    private void atNanos(long timestampNanos) {
        // Add designated timestamp column (empty name for designated timestamp)
        // Use cached reference to avoid hashmap lookup per row
        if (cachedTimestampNanosColumn == null) {
            cachedTimestampNanosColumn = currentTableBuffer.getOrCreateColumn("", TYPE_TIMESTAMP_NANOS, true);
        }
        cachedTimestampNanosColumn.addLong(timestampNanos);
        sendRow();
    }

    @Override
    public void atNow() {
        checkNotClosed();
        checkTableSelected();
        // Server-assigned timestamp - just send the row without designated timestamp
        sendRow();
    }

    /**
     * Accumulates the current row.
     * Both sync and async modes buffer rows until flush (explicit or auto-flush).
     * The difference is that sync mode flush() blocks until server ACKs.
     */
    private void sendRow() {
        ensureConnected();
        currentTableBuffer.nextRow();

        // Both modes: accumulate rows, don't encode yet
        if (pendingRowCount == 0) {
            firstPendingRowTimeNanos = System.nanoTime();
        }
        pendingRowCount++;

        // Check if any flush threshold is exceeded
        if (shouldAutoFlush()) {
            if (inFlightWindowSize > 1) {
                flushPendingRows();
            } else {
                // Sync mode (window=1): flush directly with ACK wait
                flushSync();
            }
        }
    }

    /**
     * Checks if any auto-flush threshold is exceeded.
     */
    private boolean shouldAutoFlush() {
        if (pendingRowCount <= 0) {
            return false;
        }
        // Row limit
        if (autoFlushRows > 0 && pendingRowCount >= autoFlushRows) {
            return true;
        }
        // Time limit
        if (autoFlushIntervalNanos > 0) {
            long ageNanos = System.nanoTime() - firstPendingRowTimeNanos;
            if (ageNanos >= autoFlushIntervalNanos) {
                return true;
            }
        }
        // Byte limit is harder to estimate without encoding, skip for now
        return false;
    }

    /**
     * Flushes pending rows by encoding and sending them.
     * Each table's rows are encoded into a separate ILP v4 message and sent as one WebSocket frame.
     */
    private void flushPendingRows() {
        if (pendingRowCount <= 0) {
            return;
        }

        LOG.debug().$("Flushing pending rows [count=").$(pendingRowCount)
                .$(", tables=").$(tableBuffers.size()).I$();

        // Ensure activeBuffer is ready for writing
        // It might be in RECYCLED state if previous batch was sent but we didn't swap yet
        ensureActiveBufferReady();

        // Encode all table buffers that have data
        // Iterate over the keys list directly
        ObjList<CharSequence> keys = tableBuffers.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue; // Skip null entries (shouldn't happen but be safe)
            }
            IlpV4TableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer == null) {
                continue;
            }
            int rowCount = tableBuffer.getRowCount();
            if (rowCount > 0) {
                // Check if this schema has been sent before (use schema reference mode if so)
                // Combined key includes table name since server caches by (tableName, schemaHash)
                long schemaHash = tableBuffer.getSchemaHash();
                long schemaKey = schemaHash ^ ((long) tableBuffer.getTableName().hashCode() << 32);
                boolean useSchemaRef = sentSchemaHashes.contains(schemaKey);

                LOG.debug().$("Encoding table [name=").$(tableName)
                        .$(", rows=").$(rowCount)
                        .$(", maxSentSymbolId=").$(maxSentSymbolId)
                        .$(", batchMaxId=").$(currentBatchMaxSymbolId)
                        .$(", useSchemaRef=").$(useSchemaRef).I$();

                // Encode this table's rows with delta symbol dictionary
                int messageSize = encoder.encodeWithDeltaDict(
                        tableBuffer,
                        globalSymbolDictionary,
                        maxSentSymbolId,
                        currentBatchMaxSymbolId,
                        useSchemaRef
                );

                // Track schema key if this was the first time sending this schema
                if (!useSchemaRef) {
                    sentSchemaHashes.add(schemaKey);
                }
                IlpBufferWriter buffer = encoder.getBuffer();

                // Copy to microbatch buffer and seal immediately
                // Each ILP v4 message must be in its own WebSocket frame
                activeBuffer.ensureCapacity(messageSize);
                activeBuffer.write(buffer.getBufferPtr(), messageSize);
                activeBuffer.incrementRowCount();
                activeBuffer.setMaxSymbolId(currentBatchMaxSymbolId);

                // Update maxSentSymbolId - once sent over TCP, server will receive it
                maxSentSymbolId = currentBatchMaxSymbolId;

                // Seal and enqueue for sending
                sealAndSwapBuffer();

                // Reset table buffer and batch-level symbol tracking
                tableBuffer.reset();
                currentBatchMaxSymbolId = -1;
            }
        }

        // Reset pending count
        pendingRowCount = 0;
        firstPendingRowTimeNanos = 0;
    }

    /**
     * Ensures the active buffer is ready for writing (in FILLING state).
     * If the buffer is in RECYCLED state, resets it. If it's in use, waits for it.
     */
    private void ensureActiveBufferReady() {
        if (activeBuffer.isFilling()) {
            return; // Already ready
        }

        if (activeBuffer.isRecycled()) {
            // Buffer was recycled but not reset - reset it now
            activeBuffer.reset();
            return;
        }

        // Buffer is in use (SEALED or SENDING) - wait for it
        // Use a while loop to handle spurious wakeups and race conditions with the latch
        while (activeBuffer.isInUse()) {
            LOG.debug().$("Waiting for active buffer [id=").$(activeBuffer.getBatchId())
                    .$(", state=").$(MicrobatchBuffer.stateName(activeBuffer.getState())).I$();
            boolean recycled = activeBuffer.awaitRecycled(30, TimeUnit.SECONDS);
            if (!recycled) {
                throw new LineSenderException("Timeout waiting for active buffer to be recycled");
            }
        }

        // Buffer should now be RECYCLED - reset it
        if (activeBuffer.isRecycled()) {
            activeBuffer.reset();
        }
    }

    /**
     * Adds encoded data to the active microbatch buffer.
     * Triggers seal and swap if buffer is full.
     */
    private void addToMicrobatch(long dataPtr, int length) {
        // Ensure activeBuffer is ready for writing
        ensureActiveBufferReady();

        // If current buffer can't hold the data, seal and swap
        if (activeBuffer.hasData() &&
            activeBuffer.getBufferPos() + length > activeBuffer.getBufferCapacity()) {
            sealAndSwapBuffer();
        }

        // Ensure buffer can hold the data
        activeBuffer.ensureCapacity(activeBuffer.getBufferPos() + length);

        // Copy data to buffer
        activeBuffer.write(dataPtr, length);
        activeBuffer.incrementRowCount();
    }

    /**
     * Seals the current buffer and swaps to the other buffer.
     * Enqueues the sealed buffer for async sending.
     */
    private void sealAndSwapBuffer() {
        if (!activeBuffer.hasData()) {
            return; // Nothing to send
        }

        MicrobatchBuffer toSend = activeBuffer;
        toSend.seal();

        LOG.debug().$("Sealing buffer [id=").$(toSend.getBatchId())
                .$(", rows=").$(toSend.getRowCount())
                .$(", bytes=").$(toSend.getBufferPos()).I$();

        // Swap to the other buffer
        activeBuffer = (activeBuffer == buffer0) ? buffer1 : buffer0;

        // If the other buffer is still being sent, wait for it
        // Use a while loop to handle spurious wakeups and race conditions with the latch
        while (activeBuffer.isInUse()) {
            LOG.debug().$("Waiting for buffer recycle [id=").$(activeBuffer.getBatchId())
                    .$(", state=").$(MicrobatchBuffer.stateName(activeBuffer.getState())).I$();
            boolean recycled = activeBuffer.awaitRecycled(30, TimeUnit.SECONDS);
            if (!recycled) {
                throw new LineSenderException("Timeout waiting for buffer to be recycled");
            }
            LOG.debug().$("Buffer recycled [id=").$(activeBuffer.getBatchId())
                    .$(", state=").$(MicrobatchBuffer.stateName(activeBuffer.getState())).I$();
        }

        // Reset the new active buffer
        int stateBeforeReset = activeBuffer.getState();
        LOG.debug().$("Resetting buffer [id=").$(activeBuffer.getBatchId())
                .$(", state=").$(MicrobatchBuffer.stateName(stateBeforeReset)).I$();
        activeBuffer.reset();

        // Enqueue the sealed buffer for sending
        if (!sendQueue.enqueue(toSend)) {
            throw new LineSenderException("Failed to enqueue buffer for sending");
        }
    }

    @Override
    public void flush() {
        checkNotClosed();
        ensureConnected();

        if (inFlightWindowSize > 1) {
            // Async mode (window > 1): flush pending rows and wait for ACKs
            flushPendingRows();

            // Flush any remaining data in the active microbatch buffer
            if (activeBuffer.hasData()) {
                sealAndSwapBuffer();
            }

            // Wait for all pending batches to be sent to the server
            sendQueue.flush();

            // Wait for all in-flight batches to be acknowledged by the server
            inFlightWindow.awaitEmpty();

            LOG.debug().$("Flush complete [totalBatches=").$(sendQueue.getTotalBatchesSent())
                    .$(", totalBytes=").$(sendQueue.getTotalBytesSent())
                    .$(", totalAcked=").$(inFlightWindow.getTotalAcked()).I$();
        } else {
            // Sync mode (window=1): flush pending rows and wait for ACKs synchronously
            flushSync();
        }
    }

    /**
     * Flushes pending rows synchronously, blocking until server ACKs.
     * Used in sync mode for simpler, blocking operation.
     */
    private void flushSync() {
        if (pendingRowCount <= 0) {
            return;
        }

        LOG.debug().$("Sync flush [pendingRows=").$(pendingRowCount)
                .$(", tables=").$(tableBuffers.size()).I$();

        // Encode all table buffers that have data into a single message
        ObjList<CharSequence> keys = tableBuffers.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue;
            }
            IlpV4TableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer == null || tableBuffer.getRowCount() == 0) {
                continue;
            }

            // Check if this schema has been sent before (use schema reference mode if so)
            // Combined key includes table name since server caches by (tableName, schemaHash)
            long schemaHash = tableBuffer.getSchemaHash();
            long schemaKey = schemaHash ^ ((long) tableBuffer.getTableName().hashCode() << 32);
            boolean useSchemaRef = sentSchemaHashes.contains(schemaKey);

            // Encode this table's rows with delta symbol dictionary
            int messageSize = encoder.encodeWithDeltaDict(
                    tableBuffer,
                    globalSymbolDictionary,
                    maxSentSymbolId,
                    currentBatchMaxSymbolId,
                    useSchemaRef
            );

            // Track schema key if this was the first time sending this schema
            if (!useSchemaRef) {
                sentSchemaHashes.add(schemaKey);
            }

            if (messageSize > 0) {
                IlpBufferWriter buffer = encoder.getBuffer();

                // Track batch in InFlightWindow before sending
                long batchSequence = nextBatchSequence++;
                inFlightWindow.addInFlight(batchSequence);

                // Update maxSentSymbolId - once sent over TCP, server will receive it
                maxSentSymbolId = currentBatchMaxSymbolId;

                LOG.debug().$("Sending sync batch [seq=").$(batchSequence)
                        .$(", bytes=").$(messageSize)
                        .$(", rows=").$(tableBuffer.getRowCount())
                        .$(", maxSentSymbolId=").$(maxSentSymbolId)
                        .$(", useSchemaRef=").$(useSchemaRef).I$();

                // Send over WebSocket
                client.sendBinary(buffer.getBufferPtr(), messageSize);

                // Wait for ACK synchronously
                waitForAck(batchSequence);
            }

            // Reset table buffer after sending
            tableBuffer.reset();

            // Reset batch-level symbol tracking
            currentBatchMaxSymbolId = -1;
        }

        // Reset pending row tracking
        pendingRowCount = 0;
        firstPendingRowTimeNanos = 0;

        LOG.debug().$("Sync flush complete [totalAcked=").$(inFlightWindow.getTotalAcked()).I$();
    }

    /**
     * Waits synchronously for an ACK from the server for the specified batch.
     */
    private void waitForAck(long expectedSequence) {
        WebSocketResponse response = new WebSocketResponse();
        long deadline = System.currentTimeMillis() + InFlightWindow.DEFAULT_TIMEOUT_MS;

        while (System.currentTimeMillis() < deadline) {
            try {
                boolean received = client.receiveFrame(new WebSocketFrameHandler() {
                    @Override
                    public void onBinaryMessage(long payloadPtr, int payloadLen) {
                        if (payloadLen >= WebSocketResponse.MIN_RESPONSE_SIZE) {
                            response.readFrom(payloadPtr, payloadLen);
                        }
                    }

                    @Override
                    public void onClose(int code, String reason) {
                        throw new LineSenderException("WebSocket closed while waiting for ACK: " + reason);
                    }
                }, 1000); // 1 second timeout per read attempt

                if (received) {
                    long sequence = response.getSequence();
                    if (response.isSuccess()) {
                        // Cumulative ACK - acknowledge all batches up to this sequence
                        inFlightWindow.acknowledgeUpTo(sequence);
                        if (sequence >= expectedSequence) {
                            return; // Our batch was acknowledged (cumulative)
                        }
                        // Got ACK for lower sequence - continue waiting
                    } else {
                        String errorMessage = response.getErrorMessage();
                        LineSenderException error = new LineSenderException(
                                "Server error for batch " + sequence + ": " +
                                        response.getStatusName() + " - " + errorMessage);
                        inFlightWindow.fail(sequence, error);
                        if (sequence == expectedSequence) {
                            throw error;
                        }
                    }
                }
            } catch (LineSenderException e) {
                throw e;
            } catch (Exception e) {
                throw new LineSenderException("Error waiting for ACK: " + e.getMessage(), e);
            }
        }

        throw new LineSenderException("Timeout waiting for ACK for batch " + expectedSequence);
    }

    @Override
    public DirectByteSlice bufferView() {
        throw new LineSenderException("bufferView() is not supported for WebSocket sender");
    }

    @Override
    public void cancelRow() {
        checkNotClosed();
        if (currentTableBuffer != null) {
            currentTableBuffer.cancelCurrentRow();
        }
    }

    @Override
    public void reset() {
        checkNotClosed();
        if (currentTableBuffer != null) {
            currentTableBuffer.reset();
        }
    }

    // ==================== Array methods ====================

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(values);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(values);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(values);
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, DoubleArray array) {
        if (array == null) return this;
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(array);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_LONG_ARRAY, true);
        col.addLongArray(values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_LONG_ARRAY, true);
        col.addLongArray(values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_LONG_ARRAY, true);
        col.addLongArray(values);
        return this;
    }

    @Override
    public Sender longArray(CharSequence name, LongArray array) {
        if (array == null) return this;
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_LONG_ARRAY, true);
        col.addLongArray(array);
        return this;
    }

    // ==================== Decimal methods ====================

    @Override
    public Sender decimalColumn(CharSequence name, Decimal64 value) {
        if (value == null || value.isNull()) return this;
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_DECIMAL64, true);
        col.addDecimal64(value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal128 value) {
        if (value == null || value.isNull()) return this;
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_DECIMAL128, true);
        col.addDecimal128(value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal256 value) {
        if (value == null || value.isNull()) return this;
        checkNotClosed();
        checkTableSelected();
        IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_DECIMAL256, true);
        col.addDecimal256(value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, CharSequence value) {
        if (value == null || value.length() == 0) return this;
        checkNotClosed();
        checkTableSelected();
        try {
            java.math.BigDecimal bd = new java.math.BigDecimal(value.toString());
            Decimal256 decimal = Decimal256.fromBigDecimal(bd);
            IlpV4TableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_DECIMAL256, true);
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

    private void checkNotClosed() {
        if (closed) {
            throw new LineSenderException("Sender is closed");
        }
    }

    private void checkTableSelected() {
        if (currentTableBuffer == null) {
            throw new LineSenderException("table() must be called before adding columns");
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;

            // Flush any remaining data
            try {
                if (inFlightWindowSize > 1) {
                    // Async mode (window > 1): flush accumulated rows in table buffers first
                    flushPendingRows();

                    if (activeBuffer != null && activeBuffer.hasData()) {
                        sealAndSwapBuffer();
                    }
                    if (sendQueue != null) {
                        sendQueue.close();
                    }
                } else {
                    // Sync mode (window=1): flush pending rows synchronously
                    if (pendingRowCount > 0 && client != null && client.isConnected()) {
                        flushSync();
                    }
                }
            } catch (Exception e) {
                LOG.error().$("Error during close: ").$(e).I$();
            }

            // Close buffers (async mode only, window > 1)
            if (buffer0 != null) {
                buffer0.close();
            }
            if (buffer1 != null) {
                buffer1.close();
            }

            if (client != null) {
                client.close();
                client = null;
            }
            encoder.close();
            tableBuffers.clear();

            LOG.info().$("IlpV4WebSocketSender closed").I$();
        }
    }
}
