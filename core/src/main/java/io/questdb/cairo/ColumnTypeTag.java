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

/**
 * The type tag as an enum: one constant per {@link ColumnType} tag constant, pseudo tags
 * included, plus {@link #UNKNOWN} for numbers that are no tag. Mirrors Rust
 * {@code qdb_core::ColumnTypeTag}.
 * <p>
 * Code that must decide something per type switches on this enum with a switch
 * <em>expression</em> and no {@code default} arm, so that javac rejects the switch when a tag
 * is added. Pseudo tags and {@code UNKNOWN} are listed explicitly in one arm that throws.
 * <p>
 * The codes are hand-numbered on purpose: this class must never touch {@link ColumnType} in its
 * static initialiser, so that either class can initialise first. {@code ColumnTypeTest} pins
 * that every constant here equals the {@code ColumnType} constant of the same name.
 */
public enum ColumnTypeTag {
    UNDEFINED(0),
    BOOLEAN(1),
    BYTE(2),
    SHORT(3),
    CHAR(4),
    INT(5),
    LONG(6),
    DATE(7),
    TIMESTAMP(8),
    FLOAT(9),
    DOUBLE(10),
    STRING(11),
    SYMBOL(12),
    LONG256(13),
    GEOBYTE(14),
    GEOSHORT(15),
    GEOINT(16),
    GEOLONG(17),
    BINARY(18),
    UUID(19),
    CURSOR(20),
    VAR_ARG(21),
    RECORD(22),
    GEOHASH(23),
    LONG128(24),
    IPv4(25),
    VARCHAR(26),
    ARRAY(27),
    DECIMAL8(28),
    DECIMAL16(29),
    DECIMAL32(30),
    DECIMAL64(31),
    DECIMAL128(32),
    DECIMAL256(33),
    DECIMAL(34),
    REGCLASS(35),
    REGPROCEDURE(36),
    ARRAY_STRING(37),
    PARAMETER(38),
    INTERVAL(39),
    VARCHAR_SLICE(40),
    NULL(41),
    /**
     * Not a tag. Returned by {@link #of(int)} for any number without a constant.
     */
    UNKNOWN(-1);

    private static final ColumnTypeTag[] BY_CODE = new ColumnTypeTag[256];
    private final short code;

    ColumnTypeTag(int code) {
        this.code = (short) code;
    }

    /**
     * Looks up the tag of an encoded column type. Uses the same low 8 bits that
     * {@link ColumnType#tagOf(int)} uses, so {@code of(type).code() == tagOf(type)} for
     * every encodable type; anything else, -1 included, is {@link #UNKNOWN}.
     */
    public static ColumnTypeTag of(int columnType) {
        return BY_CODE[columnType & 0xFF];
    }

    /**
     * The tag number, equal to the {@link ColumnType} constant of the same name;
     * -1 for {@link #UNKNOWN}.
     */
    public short code() {
        return code;
    }

    static {
        for (int i = 0; i < BY_CODE.length; i++) {
            BY_CODE[i] = UNKNOWN;
        }
        for (ColumnTypeTag tag : values()) {
            if (tag.code >= 0) {
                assert BY_CODE[tag.code] == UNKNOWN : "duplicate tag code " + tag.code;
                BY_CODE[tag.code] = tag;
            }
        }
    }
}
