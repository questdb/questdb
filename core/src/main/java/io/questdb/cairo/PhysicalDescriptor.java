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
import org.jetbrains.annotations.Nullable;

/**
 * What storage and record access need to know about a type's physical form, as closed sets its
 * type definition answers (F27, F39). Code that only moves values keys on these sets, never on
 * the tag, so a type that stores like an existing one adds no arm there (FR-011); a new value of
 * a set makes javac list every switch over it. None of them says anything about NULL: code that
 * decides NULL reads the column's NULL policy (FR-010, FR-031).
 * <p>
 * Three sets: {@link Movement}, how a value moves; {@link Arithmetic}, how it computes, compares
 * and sorts; {@link Accessor}, the getter and putter family records read and write it with. The
 * accessor family sits next to the two tiers, not in them: today the getter decides NULL (the
 * column-top value of a read differs by getter, F42), so only a site that already dispatches on
 * the getter keys on it.
 */
public final class PhysicalDescriptor {

    private PhysicalDescriptor() {
    }

    /**
     * The accessor family of a stored column type, or null for a pseudo type and for
     * VARCHAR_SLICE, the transient read_parquet type that no record-access layout stores. Asked
     * once per column at setup.
     */
    public static @Nullable Accessor accessorOf(int columnType) {
        final TypeDriver driver = storedTypeDriverOf(columnType);
        return driver != null ? driver.getAccessor() : null;
    }

    /**
     * The opcode of a column type's accessor family ({@link Accessor#opcode()}), or -1 for a
     * pseudo type and for VARCHAR_SLICE. For the per-row switches that dispatch on the getter: it
     * reads a table filled from the type definitions on first use, so a per-row call asks no
     * definition (FR-024), as {@link ColumnType#sizeOf(int)} reads the movement tier.
     */
    public static short accessorOpcodeOf(int columnType) {
        final short tag = ColumnType.tagOf(columnType);
        return tag >= 0 && tag <= ColumnType.MAX_TAG ? Opcodes.ACCESSOR[tag] : -1;
    }

    /**
     * The per-row arm that compares or sort-encodes values of this type: the accessor family's
     * opcode when the type orders like the family's namesake, which every existing type does
     * because each is its own family's namesake. A type that reads through a family but orders
     * otherwise (an unsigned type on INT's accessor, for example) has no arm yet: it throws,
     * naming the type, the site and the decision, and the first such type adds its arm here.
     *
     * @param site the label of the asking site, as the refusal names it
     */
    public static short compareOpcode(TypeDriver driver, CharSequence site) {
        if (!isOrderedLikeFamily(driver)) {
            throw CairoException.critical(0).put("no compare arm for ").put(driver.getTypeName()).put(" at ").put(site)
                    .put(": add a compare arm or declare the type ordered like its family");
        }
        return driver.getAccessor().opcode();
    }

    /**
     * The accessor family a setup-time switch that computes or decides NULL may dispatch on: the
     * type's accessor when the type is like its family's namesake
     * ({@link #isLikeFamilyNamesake(TypeDriver)}), which every existing type is. Any other type
     * would take the namesake's range check and NULL sentinel in the family's arm, so this throws
     * instead, naming the type and the site. Asked once per column at setup, never per row.
     *
     * @param site the label of the asking site, as the refusal names it
     */
    public static Accessor familyArmOf(TypeDriver driver, CharSequence site) {
        if (!isLikeFamilyNamesake(driver)) {
            throw noFamilyArm(driver.getTypeName(), site);
        }
        return driver.getAccessor();
    }

    /**
     * The opcode form of {@link #familyArmOf(TypeDriver, CharSequence)} for the per-row switches
     * that dispatch on the getter: the accessor's opcode for a stored type like its family's
     * namesake, -1 for a pseudo type, for VARCHAR_SLICE and for a type with no family arm, so
     * those switches' default arms fire. It reads a table, as {@link #accessorOpcodeOf(int)}
     * does.
     */
    public static short familyArmOpcodeOf(int columnType) {
        final short tag = ColumnType.tagOf(columnType);
        return tag >= 0 && tag <= ColumnType.MAX_TAG ? Opcodes.FAMILY_ARM[tag] : -1;
    }

    /**
     * The value of an integer function at its type's arithmetic tier, as a long: getLong for a
     * 64-bit tier, getInt for a narrower one, and the unsigned value for an unsigned tier. Unlike
     * IntFunction's getLong, it keeps a 32-bit value equal to INT's sentinel as that value, so
     * that {@link #isNullAtTier(TypeDriver, long)} decides by the type's own NULL policy whether
     * the value is NULL. Reads a constant or runtime-constant function, at setup or once per
     * execution, never per row.
     */
    public static long getIntegerAtTier(Function function, TypeDriver driver) {
        final Arithmetic arithmetic = driver.getArithmetic();
        return switch (arithmetic) {
            case I64 -> function.getLong(null);
            case I8, I16, I32, U8, U16, U32 -> atTier(arithmetic, function.getInt(null));
            case F32, F64, WIDE, NONE -> throw CairoException.critical(0).put("not an integer tier [type=")
                    .put(driver.getTypeName()).put(", arithmetic=").put(arithmetic.name()).put(']');
        };
    }

    /**
     * Whether a stored column type reads through an accessor family but has no family arm. A
     * default arm of a switch keyed on {@link #familyArmOpcodeOf(int)} that an existing type can
     * also reach tests this first, so it raises {@link #noFamilyArm(CharSequence, CharSequence)}
     * only for such a type and keeps its own text for every other.
     */
    public static boolean isFamilyArmMissing(int columnType) {
        return accessorOpcodeOf(columnType) != -1 && familyArmOpcodeOf(columnType) == -1;
    }

    /**
     * Whether a value read at the type's arithmetic tier
     * ({@link #getIntegerAtTier(Function, TypeDriver)}) is the type's NULL. Only a type whose NULL
     * policy is a sentinel has a NULL value, and only its own sentinel is that value; a never-null
     * type reads every bit pattern, its minimum included, as a value.
     */
    public static boolean isNullAtTier(TypeDriver driver, long value) {
        return driver.getNullPolicy() == NullPolicy.SENTINEL
                && value == atTier(driver.getArithmetic(), driver.getNullAsLong());
    }

    /**
     * Whether a type computes and represents NULL as its accessor family's namesake does: the
     * same arithmetic tier and the same NULL policy. Every existing type is its own namesake.
     * An unsigned or a never-null type on INT's accessor is not.
     */
    public static boolean isLikeFamilyNamesake(TypeDriver driver) {
        final TypeDriver namesake = ColumnType.getTypeDriver(driver.getAccessor().opcode());
        return driver.getArithmetic() == namesake.getArithmetic()
                && driver.getNullPolicy() == namesake.getNullPolicy();
    }

    /**
     * Whether values of this type order as the namesake of its accessor family does: the same
     * arithmetic tier. Code that reads a value through the family's getter and then compares,
     * sorts or ranges over it may take the family's arm only then.
     */
    public static boolean isOrderedLikeFamily(TypeDriver driver) {
        return driver.getArithmetic() == ColumnType.getTypeDriver(driver.getAccessor().opcode()).getArithmetic();
    }

    /**
     * The refusal a site raises for a type that has no family arm there, naming the type, the
     * site and the decision its author has to make.
     */
    public static CairoException noFamilyArm(CharSequence typeName, CharSequence site) {
        return CairoException.critical(0).put("no family arm for ").put(typeName).put(" at ").put(site)
                .put(": add the arm or declare the type like its namesake");
    }

    // a sign-extended value of at most 32 bits as the tier reads it: unsigned tiers drop the sign
    private static long atTier(Arithmetic arithmetic, long value) {
        return switch (arithmetic) {
            case U8 -> value & 0xFFL;
            case U16 -> value & 0xFFFFL;
            case U32 -> value & 0xFFFF_FFFFL;
            case I8, I16, I32, I64, F32, F64, WIDE, NONE -> value;
        };
    }

    /**
     * The type definition of a stored column type, or null for a pseudo type and for
     * VARCHAR_SLICE (see {@link #accessorOf(int)}).
     */
    public static @Nullable TypeDriver storedTypeDriverOf(int columnType) {
        final short tag = ColumnType.tagOf(columnType);
        if (tag == ColumnType.VARCHAR_SLICE) {
            return null;
        }
        return TypeDrivers.find(columnType);
    }

    /**
     * The getter and putter family a type's values are read and written with (F27): the
     * record getters, the row and sink putters and the map-key putters. Each existing type is its
     * own family; a later type may answer an existing family and then takes that family's arm at
     * every per-row switch that dispatches on the getter. {@link #opcode()} is the tag the family
     * is named after, the value those per-row switches have always dispatched on.
     */
    public enum Accessor {
        BOOLEAN(ColumnType.BOOLEAN),
        BYTE(ColumnType.BYTE),
        SHORT(ColumnType.SHORT),
        CHAR(ColumnType.CHAR),
        INT(ColumnType.INT),
        LONG(ColumnType.LONG),
        DATE(ColumnType.DATE),
        TIMESTAMP(ColumnType.TIMESTAMP),
        FLOAT(ColumnType.FLOAT),
        DOUBLE(ColumnType.DOUBLE),
        STRING(ColumnType.STRING),
        SYMBOL(ColumnType.SYMBOL),
        LONG256(ColumnType.LONG256),
        GEOBYTE(ColumnType.GEOBYTE),
        GEOSHORT(ColumnType.GEOSHORT),
        GEOINT(ColumnType.GEOINT),
        GEOLONG(ColumnType.GEOLONG),
        BINARY(ColumnType.BINARY),
        UUID(ColumnType.UUID),
        LONG128(ColumnType.LONG128),
        IPv4(ColumnType.IPv4),
        VARCHAR(ColumnType.VARCHAR),
        ARRAY(ColumnType.ARRAY),
        DECIMAL8(ColumnType.DECIMAL8),
        DECIMAL16(ColumnType.DECIMAL16),
        DECIMAL32(ColumnType.DECIMAL32),
        DECIMAL64(ColumnType.DECIMAL64),
        DECIMAL128(ColumnType.DECIMAL128),
        DECIMAL256(ColumnType.DECIMAL256),
        INTERVAL(ColumnType.INTERVAL);

        private final short opcode;

        Accessor(short opcode) {
            this.opcode = opcode;
        }

        /**
         * The tag this family is named after: the arm value the per-row switches dispatch on.
         */
        public short opcode() {
            return opcode;
        }
    }

    /**
     * The arithmetic tier (PA-8): width, integer or floating-point representation, and
     * signedness. Code that computes, compares, sorts or takes a minimum or maximum keys on it,
     * never on the tag (FR-011). {@link #WIDE} is a 16- or 32-byte value with comparators of its
     * own; {@link #NONE} has no arithmetic order here (symbol keys, intervals, var-size values).
     */
    public enum Arithmetic {
        I8,
        I16,
        I32,
        I64,
        U8,
        U16,
        U32,
        F32,
        F64,
        WIDE,
        NONE
    }

    /**
     * The data-movement tier: the width class of a fixed-size value, or {@link #VAR} for a
     * var-size layout, whose values live in a data vector addressed through an aux vector. Code
     * on this tier copies, shuffles, sizes and fills values; it never compares or sorts them.
     */
    public enum Movement {
        W1(0),
        W2(1),
        W4(2),
        W8(3),
        W16(4),
        W32(5),
        VAR(-1);

        private final int pow2Size;

        Movement(int pow2Size) {
            this.pow2Size = pow2Size;
        }

        /**
         * log2 of the value width in bytes; -1 for {@link #VAR}.
         */
        public int pow2Size() {
            return pow2Size;
        }

        /**
         * The value width in bytes; 0 for {@link #VAR}, whose data vector has no fixed stride.
         */
        public int size() {
            return pow2Size < 0 ? 0 : 1 << pow2Size;
        }
    }

    /**
     * The accessor opcodes by tag, filled from the type definitions on first use and never from a
     * static initialiser the definitions reach (the class-init order of phase 1).
     * {@link #FAMILY_ARM} holds the same opcode for a type like its family's namesake and -1
     * otherwise, so it equals {@link #ACCESSOR} cell by cell while every type is its own
     * namesake.
     */
    private static final class Opcodes {
        static final short[] ACCESSOR = new short[ColumnType.MAX_TAG + 1];
        static final short[] FAMILY_ARM = new short[ColumnType.MAX_TAG + 1];

        static {
            for (short tag = 0; tag <= ColumnType.MAX_TAG; tag++) {
                final TypeDriver driver = storedTypeDriverOf(tag);
                ACCESSOR[tag] = driver != null ? driver.getAccessor().opcode() : -1;
                FAMILY_ARM[tag] = driver != null && isLikeFamilyNamesake(driver) ? driver.getAccessor().opcode() : -1;
            }
        }
    }
}
