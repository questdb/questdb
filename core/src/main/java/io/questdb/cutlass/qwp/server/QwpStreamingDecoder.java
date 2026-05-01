/*+*****************************************************************************
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

package io.questdb.cutlass.qwp.server;

import io.questdb.cutlass.qwp.protocol.QwpColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpSchemaRegistry;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

/**
 * Zero-allocation streaming decoder for QWP v1 messages.
 * <p>
 * This decoder provides streaming access to QWP v1 message content without
 * materializing intermediate Java objects. It uses flyweight cursors that
 * read directly from wire-format memory.
 * <p>
 * <b>Key Features:</b>
 * <ul>
 *   <li>Zero-allocation on the hot decode path after warmup</li>
 *   <li>Reusable cursors for tables, rows, and columns</li>
 *   <li>Flyweight string access via {@link io.questdb.std.str.DirectUtf8Sequence}</li>
 *   <li>Support for all QWP v1 column types including Gorilla timestamps</li>
 * </ul>
 * <p>
 * <b>Usage:</b>
 * <pre>{@code
 * QwpStreamingDecoder decoder = new QwpStreamingDecoder(schemaRegistry);
 * try {
 *     QwpMessageCursor message = decoder.decode(address, length);
 *     while (message.hasNextTable()) {
 *         QwpTableBlockCursor table = message.nextTable();
 *         while (table.hasNextRow()) {
 *             table.nextRow();
 *             for (int col = 0; col < table.getColumnCount(); col++) {
 *                 if (table.isColumnNull(col)) {
 *                     continue;  // use isColumnNull() to avoid megamorphic calls
 *                 }
 *                 QwpColumnCursor column = table.getColumn(col);
 *                 // Read value based on column type
 *             }
 *         }
 *     }
 * } finally {
 *     decoder.reset(); // Prepare for next message
 * }
 * }</pre>
 * <p>
 * <b>Thread Safety:</b> Not thread-safe. Create one decoder per thread/connection.
 *
 * @see QwpMessageCursor
 * @see QwpTableBlockCursor
 * @see QwpColumnCursor
 */
public class QwpStreamingDecoder implements QuietCloseable {

    private final QwpMessageCursor messageCursor;
    private final QwpSchemaRegistry schemaRegistry;

    /**
     * Creates a streaming decoder without a schema registry.
     */
    public QwpStreamingDecoder() {
        this(null, QwpConstants.DEFAULT_MAX_ROWS_PER_TABLE);
    }

    public QwpStreamingDecoder(QwpSchemaRegistry schemaRegistry, int maxRowsPerTable) {
        this.schemaRegistry = schemaRegistry;
        this.messageCursor = new QwpMessageCursor(maxRowsPerTable);
    }

    @Override
    public void close() {
        reset();
    }

    /**
     * Decodes a QWP v1 message from direct memory with delta symbol dictionary support.
     * <p>
     * If the message has FLAG_DELTA_SYMBOL_DICT set, the delta symbols are accumulated
     * to the provided connection dictionary. Symbol columns then reference this dictionary
     * using global IDs.
     *
     * @param messageAddress       address of the complete QWP v1 message
     * @param messageLength        total message length in bytes
     * @param connectionSymbolDict connection-level symbol dictionary for delta mode (may be null)
     * @return message cursor for streaming access
     * @throws QwpParseException if the message is malformed
     */
    public QwpMessageCursor decode(long messageAddress, int messageLength, ObjList<String> connectionSymbolDict)
            throws QwpParseException {
        messageCursor.clear();
        messageCursor.of(messageAddress, messageLength, schemaRegistry, connectionSymbolDict);
        return messageCursor;
    }

    /**
     * Decodes a QWP v1 message from direct memory.
     * <p>
     * The returned cursor is valid until the next call to {@link #decode} or {@link #reset()}.
     * <p>
     * <b>Zero-allocation:</b> After warmup, this method does not allocate
     * any new objects. Cursors are reused across calls.
     *
     * @param messageAddress address of the complete QWP v1 message (header + payload)
     * @param messageLength  total message length in bytes
     * @return message cursor for streaming access
     * @throws QwpParseException if the message is malformed
     */
    public QwpMessageCursor decode(long messageAddress, int messageLength) throws QwpParseException {
        return decode(messageAddress, messageLength, null);
    }

    /**
     * Resets the decoder for reuse.
     * <p>
     * Call this after processing a message to prepare for the next one.
     * All cursors are invalidated.
     */
    public void reset() {
        messageCursor.clear();
    }

    public void resetConnectionState() {
        reset();
        if (schemaRegistry != null) {
            schemaRegistry.clear();
        }
    }
}
