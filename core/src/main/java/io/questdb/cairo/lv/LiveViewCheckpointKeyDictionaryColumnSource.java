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

package io.questdb.cairo.lv;

/**
 * What {@link LiveViewCheckpointKeyDictionaryWriter} needs from a live view's LV-private
 * symbol dictionaries to seal them: one entry per distinct base SYMBOL column a view's
 * partition terms key by, in ascending {@code (baseTableId, baseWriterColumnIndex)} order.
 * {@link LiveViewSymbolIdRegistry#newDictionaryColumnSource()} is the production
 * implementation.
 * <p>
 * Column order is a precondition the writer validates rather than establishes: it is what
 * lets {@link LiveViewCheckpointKeyDictionaryReader#findColumn} binary-search the persisted
 * directory, so a source that hands columns out of order fails the seal immediately rather
 * than building a directory later reads cannot search correctly.
 */
public interface LiveViewCheckpointKeyDictionaryColumnSource {

    /**
     * @return the base table's id, which changes when the id space is replaced. Part of the
     * column identity together with {@link #getBaseWriterColumnIndex}.
     */
    int getBaseTableId(int columnIndex);

    /**
     * @return the column's base-table writer index. Part of the column identity together with
     * {@link #getBaseTableId}.
     */
    int getBaseWriterColumnIndex(int columnIndex);

    /**
     * @return the number of distinct base SYMBOL columns this source carries a dictionary for
     */
    int getColumnCount();

    /**
     * @return the base column's canonical name, for diagnostics. Never a projected name.
     */
    CharSequence getColumnName(int columnIndex);

    /**
     * @return the base column's type. Every column reaching this source is SYMBOL today,
     * but the persisted format carries the type so a future column shape needs no format
     * change to be told apart.
     */
    int getColumnType(int columnIndex);

    /**
     * @return how many ids {@code columnIndex}'s dictionary has handed out, i.e. one past the
     * highest live id
     */
    int getEntryCount(int columnIndex);

    /**
     * @return the string {@code columnIndex}'s dictionary assigned {@code lvId}. Must be
     * consumed before the next call for the same column: the production source returns a
     * reusable view that a later call on the same dictionary invalidates.
     */
    CharSequence getEntryValue(int columnIndex, int lvId);
}
