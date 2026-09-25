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

package io.questdb.cairo;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;

/**
 * The root of the per-type driver hierarchy: one driver instance per column type tag, holding
 * what the engine must know about that type. A driver is fetched once per column, batch or
 * query with {@link ColumnType#getTypeDriver(int)}, never per value.
 * <p>
 * Three driver concepts exist, and they are different things:
 * <ul>
 * <li>{@code TypeDriver}: per tag, this hierarchy. {@link FixedSizeTypeDriver} is the base of
 * the fixed-size leaves.</li>
 * <li>{@link ColumnTypeDriver}: the var-size storage API (aux and data vectors), which
 * extends this interface; STRING, BINARY, VARCHAR and ARRAY.</li>
 * <li>{@link TimestampDriver}: per timestamp <em>precision</em>, keyed by the encoded type,
 * not by tag. It is a separate facet, not a {@code TypeDriver}; the TIMESTAMP and DATE
 * drivers fetch it where a method needs temporal arithmetic.</li>
 * </ul>
 * Methods are added here only when a caller needs them. Pseudo tags (UNDEFINED, CURSOR,
 * VAR_ARG, RECORD, GEOHASH, DECIMAL, REGCLASS, REGPROCEDURE, ARRAY_STRING, PARAMETER, NULL)
 * have no driver.
 */
public interface TypeDriver {

    /**
     * This type's NULL as a widening fixed-width read returns it: the storage NULL of a type
     * up to 8 bytes wide, sign-extended to a long; 0 for wider types and for var-size types,
     * whose NULL is not a single word. Query-engine buffers that park one value per column in
     * a long slot use it for a column that has no data.
     */
    long getNullAsLong();

    /**
     * The constant function that yields this type's NULL, typed as {@code columnType}; the
     * query engine uses it for {@code cast(null as T)}, a CASE without ELSE, an outer join's
     * missing side and any other place that needs a NULL of a known type. Encoded types
     * (timestamp precision, geohash bits, decimal precision and scale, array dimensions) read
     * their parameters from {@code columnType}. Called once per query, never per row.
     */
    ConstantFunction getNullConstant(int columnType);

    /**
     * The value of the n-th long of this type's NULL, for a value up to 32 bytes wide; the
     * n-th long of the aux entry for a var-size type. Callers that fill a fixed-width NULL
     * pattern read longs 0..3.
     */
    long getNullLong(int longIndex);

    /**
     * The tag this driver serves. Exactly one driver instance exists per non-pseudo tag.
     */
    ColumnTypeTag getTag();

    /**
     * Whether the data vector can hold a NULL: false only for the types where every bit
     * pattern is a value (BOOLEAN, BYTE, SHORT, CHAR), whose column tops read as leading
     * default values rather than NULLs.
     */
    boolean hasNullSentinel();

    /**
     * Creates the appender that writes one NULL of this type at the current append position,
     * for a writer's per-column null setters. Called once per column when the writer opens it.
     */
    /**
     * The function that reads column {@code columnIndex} of this type from a record, typed as
     * {@code columnType}. SYMBOL is the exception: its column function needs the symbol table,
     * so callers build it themselves and the SYMBOL driver throws.
     */
    Function newColumnFunction(int columnIndex, int columnType);

    Runnable newNullAppender(MemoryA dataMem, MemoryA auxMem);

    /**
     * Fills {@code count} values of this type at {@code addr} with NULL in one native call.
     * A no-op for var-size types, whose NULLs live in the aux vector.
     */
    void setNull(long addr, long count);

    /**
     * The tag's constant name, e.g. {@code GEOBYTE}; unlike {@link ColumnType#nameOf(int)}
     * this is defined for every tag.
     */
    default String getTypeName() {
        return getTag().name();
    }
}
