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

import io.questdb.cairo.arr.ArrayTypeDriver;

/**
 * Maps a tag to its driver instance. The switch is exhaustive over {@link ColumnTypeTag} with
 * no default arm: adding a tag does not compile until its driver is listed here. Driver classes
 * initialise lazily, on the first lookup of their tag; {@link ColumnType}'s static initialiser
 * never reaches this class.
 */
final class TypeDrivers {

    private TypeDrivers() {
    }

    static TypeDriver get(int columnType) {
        final ColumnTypeTag tag = ColumnTypeTag.of(columnType);
        return switch (tag) {
            case BOOLEAN -> BooleanTypeDriver.INSTANCE;
            case BYTE -> ByteTypeDriver.INSTANCE;
            case SHORT -> ShortTypeDriver.INSTANCE;
            case CHAR -> CharTypeDriver.INSTANCE;
            case INT -> IntTypeDriver.INSTANCE;
            case LONG -> LongTypeDriver.INSTANCE;
            case DATE -> DateTypeDriver.INSTANCE;
            case TIMESTAMP -> TimestampTypeDriver.INSTANCE;
            case FLOAT -> FloatTypeDriver.INSTANCE;
            case DOUBLE -> DoubleTypeDriver.INSTANCE;
            case STRING -> StringTypeDriver.INSTANCE;
            case SYMBOL -> SymbolTypeDriver.INSTANCE;
            case LONG256 -> Long256TypeDriver.INSTANCE;
            case GEOBYTE -> GeoHashTypeDriver.GEOBYTE;
            case GEOSHORT -> GeoHashTypeDriver.GEOSHORT;
            case GEOINT -> GeoHashTypeDriver.GEOINT;
            case GEOLONG -> GeoHashTypeDriver.GEOLONG;
            case BINARY -> BinaryTypeDriver.INSTANCE;
            case UUID -> UuidTypeDriver.INSTANCE;
            case LONG128 -> Long128TypeDriver.INSTANCE;
            case IPv4 -> IPv4TypeDriver.INSTANCE;
            case VARCHAR, VARCHAR_SLICE -> VarcharTypeDriver.INSTANCE;
            case ARRAY -> ArrayTypeDriver.INSTANCE;
            case DECIMAL8 -> DecimalTypeDriver.DECIMAL8;
            case DECIMAL16 -> DecimalTypeDriver.DECIMAL16;
            case DECIMAL32 -> DecimalTypeDriver.DECIMAL32;
            case DECIMAL64 -> DecimalTypeDriver.DECIMAL64;
            case DECIMAL128 -> DecimalTypeDriver.DECIMAL128;
            case DECIMAL256 -> DecimalTypeDriver.DECIMAL256;
            case INTERVAL -> IntervalTypeDriver.INSTANCE;
            // pseudo tags resolve overloads or mark parser state; no value of theirs is ever stored or computed
            case UNDEFINED, CURSOR, VAR_ARG, RECORD, GEOHASH, DECIMAL, REGCLASS, REGPROCEDURE, ARRAY_STRING, PARAMETER,
                 NULL, UNKNOWN -> throw CairoException.critical(0).put("no type driver for type: ").put(columnType);
        };
    }
}
