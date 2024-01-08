/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;

public interface StatefulAtom extends QuietCloseable, Mutable {

    @Override
    default void clear() {
    }

    @Override
    default void close() {
    }

    /**
     * Initializes state required for filtering, such as child atoms, symbol table sources,
     * bind variable values, circuit breakers, etc.
     *
     * @param symbolTableSource symbol table source
     * @param executionContext  execution context
     * @throws SqlException when bind variable validation or any other kind of validation fails
     */
    default void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
    }

    /**
     * Initializes cursor-based data structures used by cursor functions, like 'symbol_col IN (SELECT ...)'.
     * Must be called after {@link #init(SymbolTableSource, SqlExecutionContext)}.
     *
     * @throws io.questdb.cairo.DataUnavailableException when the queried partition is in cold storage
     */
    default void initCursor() {
    }
}
