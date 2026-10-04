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

import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.SqlException;
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
     * Defines bind variable {@code index} of {@code service} as {@code columnType}, holding NULL, and
     * answers the type the variable holds (a SYMBOL variable holds a STRING). A type no bind variable
     * can hold refuses with an error at {@code position}. The service pools its variables, so a
     * definition allocates nothing; PostgreSQL defines every parameter once per execution.
     */
    int defineBindVariable(BindVariableService service, int index, int columnType, int position) throws SqlException;

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
     * The one implicit-cast list this type declares (F34, PA-7): the types a value of it is passed as
     * to a function, best match first, where the position is the overload distance. It is the
     * overload row, and the relation rules ({@link RelationRules}) derive built-in widening, widening
     * cast and narrowing from it; the type itself comes first.
     */
    short[] getImplicitCasts();

    /**
     * The name of {@code columnType} as SQL and metadata print it, for the full column type
     * (timestamp precision, geohash bits, decimal precision and scale, array dimensions), or
     * {@link ColumnType#UNKNOWN_NAME} for an encoding of this tag that has no name.
     */
    String getName(int columnType);

    /**
     * The PostgreSQL type OID of an array whose elements are this type ({@link PgTypeOids}), or 0
     * when PostgreSQL wire describes no such array: pgwire sends only DOUBLE and VARCHAR arrays
     * (F41). Protocol data, asked once per column.
     */
    int getPgArrayOid();

    /**
     * The PostgreSQL type OID the wire describes a column of this type with ({@link PgTypeOids}),
     * or 0 when PostgreSQL wire has none: LONG128, which pgwire cannot send, and the bare ARRAY
     * tag, whose arrays take {@link #getPgArrayOid()} of their element type (F41). Protocol data,
     * asked once per column.
     */
    int getPgOid();

    /**
     * The value width in bits the relation rules read (F34): whether a small integer converts into a
     * temporal type, and which geohashes are narrower. 0 for a type without a fixed value width.
     */
    int getRelationBits();

    /**
     * The class of values the relation rules ({@link RelationRules}) group this type by. The type's
     * own relations to other types derive from this answer, its width and its implicit-cast list;
     * the relations of the existing types into it come from their lists and the rules' exception
     * cells, which name tags.
     */
    RelationKind getRelationKind();

    /**
     * The lower-case character that names this type in a function factory signature
     * ({@link io.questdb.griffin.FunctionFactory#getSignature()}); the upper-case form of the same
     * character is the constant-argument variant, so the character must differ from its upper-case
     * form in bit 5 only. {@link io.questdb.griffin.FunctionFactoryDescriptor#NO_SIGNATURE_CHAR} for
     * a type no signature names: the geohash and decimal widths are named by their pseudo tags and
     * an array by its element character followed by {@code []}. {@code FunctionFactoryDescriptorTest}
     * pins the table and the bit-5 rule.
     */
    char getSignatureChar();

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
     * How this type's values travel on the result protocols (F41): the byte form and NULL test the
     * protocol writers key on. Types that write the same bytes share a kind; see {@link WireKind}.
     * Per-row callers read {@link WireKind#of(int)}, which asks this once per tag.
     */
    WireKind getWireKind();

    /**
     * Whether the parser takes this type as the target of {@code cast(x as T)} and of the
     * {@code T 'literal'} form: from a value when {@code isFromNull} is false, from {@code null}
     * when it is true. GEOHASH and DECIMAL casts name their pseudo types, so the bare geohash and
     * decimal tags answer false.
     */
    boolean isCastTarget(boolean isFromNull);

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
