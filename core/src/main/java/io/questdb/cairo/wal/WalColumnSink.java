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

package io.questdb.cairo.wal;

import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.api.MemoryMA;

/**
 * Column-level write access to the current segment of a WAL writer, for clients that
 * append whole column blocks rather than rows.
 * <p>
 * A client brackets each block with {@link #beginColumnarWrite()} and then either
 * {@link #finishColumnarWrite(int, long, long, boolean)} or {@link #cancelColumnarWrite(long)}.
 * Between the two it appends the block's values to the memories {@link #getDataColumn(int)}
 * and {@link #getAuxColumn(int)} return, and after writing a column it calls
 * {@link #setRowValueNotNullColumnar(int, long)} with the block's last row. The client keeps
 * these rules:
 * <ul>
 *   <li>a column it writes receives a value for every row of the block;</li>
 *   <li>the designated timestamp column holds 128-bit (timestamp, row) pairs, and the row
 *   index starts at the {@link #getSegmentRowCount()} value read after
 *   {@link #beginColumnarWrite()};</li>
 *   <li>a STRING or BINARY aux column continues from the entry already present at its
 *   append offset; VARCHAR and ARRAY aux columns have no shared entry;</li>
 *   <li>a symbol value is written as the key {@link #resolveSymbol(int, CharSequence, SymbolMapReader)}
 *   returns, and a client that writes a NULL symbol key without that method calls
 *   {@link #markSymbolMapNull(int)}.</li>
 * </ul>
 * {@link #finishColumnarWrite(int, long, long, boolean)} NULL-fills every column the client
 * did not write and refuses a block that lacks the designated timestamp column.
 */
public interface WalColumnSink {

    /**
     * Opens a block. Rolls the segment first when a commit or a structure change left a
     * roll pending. A failed roll latches the writer distressed.
     */
    void beginColumnarWrite();

    /**
     * Discards the open block by moving every column's append position back to the given row.
     *
     * @param startRowId the {@link #getSegmentRowCount()} value read after {@link #beginColumnarWrite()}
     */
    void cancelColumnarWrite(long startRowId);

    /**
     * Throws a critical {@link io.questdb.cairo.CairoException} when the writer is distressed.
     */
    void checkDistressed();

    /**
     * Closes the open block: NULL-fills the columns the client did not write, then adds the
     * block's rows to the segment and its timestamps to the transaction.
     */
    void finishColumnarWrite(int rowCount, long minTimestamp, long maxTimestamp, boolean outOfOrder);

    MemoryMA getAuxColumn(int columnIndex);

    MemoryMA getDataColumn(int columnIndex);

    TableRecordMetadata getMetadata();

    long getSegmentRowCount();

    SymbolMapReader getSymbolMapReader(int columnIndex);

    void markSymbolMapNull(int columnIndex);

    /**
     * Returns the key for a symbol value: the table's key when the table dictionary holds the
     * value, otherwise a key from the writer's transaction-local dictionary. A null value marks
     * the column's null flag and returns {@link io.questdb.cairo.sql.SymbolTable#VALUE_IS_NULL}.
     */
    int resolveSymbol(int columnIndex, CharSequence symbolValue, SymbolMapReader symbolMapReader);

    void setRowValueNotNullColumnar(int columnIndex, long lastWrittenRow);

    /**
     * Throws when a designated timestamp value lies outside the bounds the table accepts.
     */
    void validateDesignatedTimestampBounds(long timestamp);
}
