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
