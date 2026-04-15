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

package io.questdb.cutlass.qwp.protocol;

import io.questdb.std.Mutable;

/**
 * Base interface for streaming column cursors in QWP v1.
 * <p>
 * Column cursors provide zero-allocation access to column values by reading
 * directly from wire-format memory. Values are accessed one row at a time
 * via {@link #advanceRow()}.
 * <p>
 * <b>Lifecycle:</b>
 * <ol>
 *   <li>Cursor is initialized via {@code of(...)} method</li>
 *   <li>Call {@link #advanceRow()} before reading each row</li>
 *   <li>Read value using type-specific getter</li>
 *   <li>Call {@link #clear()} when done or reusing cursor</li>
 * </ol>
 * <p>
 * <b>Thread Safety:</b> Not thread-safe. Designed for single-threaded use
 * with object pooling.
 */
public interface QwpColumnCursor extends Mutable {

    /**
     * Advances to the next row.
     * <p>
     * Must be called before reading each row's value. The first call
     * advances to row 0.
     *
     * @return true if the current row's value is NULL
     * @throws QwpParseException if parsing fails during row advance
     */
    boolean advanceRow() throws QwpParseException;

    /**
     * Clears all state. Implements {@link Mutable#clear()}.
     */
    @Override
    void clear();

    /**
     * Returns the QWP v1 type code for this column.
     *
     * @return type code (without nullable flag)
     * @see QwpConstants
     */
    byte getTypeCode();

    /**
     * Returns whether the current row's value is NULL.
     * <p>
     * Must be called after {@link #advanceRow()}.
     *
     * @return true if current row is NULL
     */
    boolean isNull();

    /**
     * Resets the cursor to before the first row.
     * <p>
     * After calling this method, {@link #advanceRow()} must be called
     * to position on row 0.
     */
    void resetRowPosition();
}
