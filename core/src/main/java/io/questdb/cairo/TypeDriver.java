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
import io.questdb.griffin.TypeConstant;
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
     * How this type represents NULL: {@link NullPolicy#NONE} only for the types where every bit
     * pattern is a value (BOOLEAN, BYTE, SHORT, CHAR), whose column tops read as leading
     * default values rather than NULLs; {@link NullPolicy#SENTINEL} for every other type.
     * <p>
     * Code never reads this to decide NULL for a column: it reads the column's policy through a
     * per-column accessor such as {@link io.questdb.cairo.sql.RecordMetadata#getColumnNullPolicy(int)},
     * whose body derives from this answer (FR-010).
     */
    NullPolicy getNullPolicy();

    /**
     * The accessor family (F27, F39): the record getter and the row, sink and map-key putters a
     * value of this type is read and written with. Per-row code that dispatches on the getter keys
     * on it at setup; see {@link PhysicalDescriptor.Accessor}.
     */
    PhysicalDescriptor.Accessor getAccessor();

    /**
     * The arithmetic tier (F27, F39): the width, representation and signedness that arithmetic,
     * comparison, sorting and minimum or maximum key on (FR-011); see
     * {@link PhysicalDescriptor.Arithmetic}.
     */
    PhysicalDescriptor.Arithmetic getArithmetic();

    /**
     * The data-movement tier (F39): how storage moves a value of this type. A fixed-size type
     * answers its width class, a var-size type {@link PhysicalDescriptor.Movement#VAR}. The width
     * and the fixed-size-ness of a type are this answer, declared once (FR-009); storage code that
     * only moves values keys on it, never on the tag (FR-011).
     */
    PhysicalDescriptor.Movement getMovement();

    /**
     * The name of {@code columnType} as SQL and metadata print it, for the full column type
     * (timestamp precision, geohash bits, decimal precision and scale, array dimensions), or
     * {@link ColumnType#UNKNOWN_NAME} for an encoding of this tag that has no name.
     */
    String getName(int columnType);

    /**
     * The tag this driver serves. Exactly one driver instance exists per non-pseudo tag.
     */
    ColumnTypeTag getTag();

    /**
     * The type constant a CAST names {@code columnType} with, as in {@code cast(x as T)}, or null
     * when no SQL type name resolves to exactly this encoding. The query engine resolves a type
     * name token through it once, at compile time. GEOHASH and DECIMAL casts name their pseudo
     * types, whose constants carry bits or precision and scale, so the bare geohash and decimal
     * tags answer null.
     */
    TypeConstant getTypeConstant(int columnType);

    /**
     * The validity operations of a column of this type under the type's NULL policy, for storage
     * code that holds only the column type (the out-of-order jobs, frame columns, fills of
     * decoded buffers). Code that holds the column takes {@link ValidityOps#of(NullPolicy)} of
     * the column's policy instead. Derived from {@link #getNullPolicy()}, so no type declares it
     * (FR-008).
     */
    default ValidityOps getValidityOps() {
        return ValidityOps.of(getNullPolicy());
    }

    /**
     * The function that reads column {@code columnIndex} of this type from a record, typed as
     * {@code columnType}. SYMBOL is the exception: its column function needs the symbol table,
     * so callers build it themselves and the SYMBOL driver throws.
     */
    Function newColumnFunction(int columnIndex, int columnType);

    /**
     * Creates the appender that writes one NULL of this type at the current append position,
     * for a writer's per-column null setters. Called once per column when the writer opens it.
     */
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
