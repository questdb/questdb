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
 * extends this interface.</li>
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
     * Defines bind variable {@code index} of {@code service} as {@code columnType}, holding NULL,
     * and returns the type the variable holds (a SYMBOL variable holds a STRING). A type no bind
     * variable can hold throws {@link SqlException} at {@code position}.
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
     * their parameters from {@code columnType}.
     */
    ConstantFunction getNullConstant(int columnType);

    /**
     * Long {@code longIndex} (0 to 3) of this type's NULL, for a fixed-size value up to 32 bytes
     * wide. Callers that fill a fixed-width NULL pattern read longs 0 to 3. Var-size types return
     * NULL_LEN (-1), which is not their stored aux entry.
     */
    long getNullLong(int longIndex);

    /**
     * How this type represents NULL: {@link NullPolicy#NONE} for a type where every bit pattern is a
     * value, whose column tops read as default values rather than NULLs; {@link NullPolicy#SENTINEL}
     * for a type that reserves a value for NULL.
     * <p>
     * Code that holds a column reads the column's policy instead, through a per-column accessor
     * such as {@link io.questdb.cairo.sql.RecordMetadata#getColumnNullPolicy(int)}, which derives
     * from this value.
     */
    NullPolicy getNullPolicy();

    /**
     * The accessor family: the record getter and the row, sink and map-key putters a value of this
     * type is read and written with. Per-row code that dispatches on the getter switches on it at
     * setup; see {@link PhysicalDescriptor.Accessor}.
     */
    PhysicalDescriptor.Accessor getAccessor();

    /**
     * The arithmetic tier: the width, representation and signedness that arithmetic, comparison,
     * sorting and minimum or maximum switch on; see {@link PhysicalDescriptor.Arithmetic}.
     */
    PhysicalDescriptor.Arithmetic getArithmetic();

    /**
     * How storage moves a value of this type: a fixed-size type returns its width class, a var-size
     * type {@link PhysicalDescriptor.Movement#VAR}. This is the only place a type declares its
     * storage width; storage code that only moves values switches on it instead of the tag.
     */
    PhysicalDescriptor.Movement getMovement();

    /**
     * The types a value of this type can be passed as to a function, best match first, starting
     * with the type itself; the position is the overload distance. {@link RelationRules} also
     * derives widening, narrowing and the CASE result type from this list.
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
     * when PostgreSQL wire describes no such array.
     */
    int getPgArrayOid();

    /**
     * The PostgreSQL type OID the wire describes a column of this type with ({@link PgTypeOids}),
     * or 0 when PostgreSQL wire has none; an array takes {@link #getPgArrayOid()} of its element
     * type.
     */
    int getPgOid();

    /**
     * The value width in bits that {@link RelationRules} reads to decide whether a small integer
     * converts into a temporal type, which geohashes are narrower, and whether a type widens into a
     * type of its own kind that is no narrower. 0 for a type without a fixed value width.
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
     * a type no signature names by its own character: a signature names a type of several widths by
     * its pseudo tag, and an array by its element character followed by {@code []}.
     */
    char getSignatureChar();

    /**
     * The tag this driver serves. Exactly one driver instance exists per non-pseudo tag.
     */
    ColumnTypeTag getTag();

    /**
     * The type constant a CAST names {@code columnType} with, as in {@code cast(x as T)}, or null
     * when no SQL type name resolves to exactly this encoding: a CAST that names a pseudo type,
     * whose constant carries the type's parameters, leaves the bare tags of that family null.
     */
    TypeConstant getTypeConstant(int columnType);

    /**
     * How this type's values travel on the result protocols: the byte form and NULL test the
     * protocol writers switch on. Types that write the same bytes share a kind; see {@link
     * WireKind}. Per-row callers read {@link WireKind#of(int)}, which calls this once per tag.
     */
    WireKind getWireKind();

    /**
     * Whether the parser takes this type as the target of {@code cast(x as T)} and of the
     * {@code T 'literal'} form: from a value when {@code isFromNull} is false, from {@code null}
     * when it is true. A CAST that names a pseudo type leaves the bare tags of that family false.
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
     * for a writer's per-column null setters.
     */
    Runnable newNullAppender(MemoryA dataMem, MemoryA auxMem);

    /**
     * Fills {@code count} values of this type at {@code addr} with NULL in one native call. A no-op
     * for var-size types, which have no fixed-width NULL pattern.
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
