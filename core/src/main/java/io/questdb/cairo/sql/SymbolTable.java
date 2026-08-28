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

package io.questdb.cairo.sql;

import io.questdb.std.Numbers;

/**
 * A table to store symbols (repetitive strings) as integers
 * and corresponding string values
 */
public interface SymbolTable {
    int VALUE_IS_NULL = Numbers.INT_NULL;
    int VALUE_NOT_FOUND = -2;

    /**
     * Returns true when a consumer that wants a row's symbol should read the row's integer key
     * and resolve it through this table, rather than read the row's text directly. A static
     * dictionary answers true: resolving a key is a lookup that never touches the text. A dynamic
     * symbol function answers false by default, because it would have to hash the row's text to
     * produce a key at all, so the key buys the consumer nothing.
     * <p>
     * This is a hint, and a consumer fixes it once per cursor rather than per row. A table that
     * translates other dictionaries answers for the shape it expects to serve as a whole: an
     * all-SYMBOL UNION answers true because it serves table dictionaries by key without touching
     * their text, even though a leg that has to intern its own text pays for the key it hands
     * back. Read it as "prefer the key path here", not as a per-row guarantee.
     * <p>
     * The record paired with a table that returns true must represent a null symbol as
     * {@link #VALUE_IS_NULL} from {@link Record#getInt(int)}, and every non-null symbol as a
     * non-negative key. The table must resolve every such non-negative key through
     * {@link #valueOf(int)} and {@link #valueBOf(int)} without reading the record. Consumers may
     * reject any other negative key as a contract violation.
     */
    default boolean supportsKeyValueAccess() {
        return false;
    }

    /**
     * Look up "B" instance of CharSequence for symbol key. "B" instance allows
     * calling code to have two simultaneous symbol CharSequence instances in case
     * they have to be compared by their text value.
     *
     * @param key numeric key of the symbol
     * @return string value of the symbol
     */
    CharSequence valueBOf(int key);

    /**
     * Look up CharSequence by symbol key. The returned value is mutable and
     * must not be stored anywhere.
     *
     * @param key numeric key of the symbol
     * @return mutable CharSequence value of the symbol
     */
    CharSequence valueOf(int key);
}
