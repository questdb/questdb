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

package io.questdb.cutlass.qwp.codec;

import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.qwp.protocol.QwpConstants;

/**
 * Maps QuestDB {@link ColumnType} to QWP wire type codes.
 */
public final class QwpColumnTypeMapper {

    private QwpColumnTypeMapper() {
    }

    /**
     * Returns the geohash precision in bits for a QuestDB GEOHASH/GEOBYTE/GEOSHORT/GEOINT/GEOLONG type,
     * or -1 if the column type is not a geohash.
     */
    public static int geohashPrecisionBits(int questdbColumnType) {
        if (!ColumnType.isGeoHash(questdbColumnType)) {
            return -1;
        }
        return ColumnType.getGeoHashBits(questdbColumnType);
    }

    /**
     * Returns true if the QuestDB type produces a QWP array wire type.
     */
    public static boolean isArrayType(int questdbColumnType) {
        return ColumnType.tagOf(questdbColumnType) == ColumnType.ARRAY;
    }

    /**
     * Returns true if the QuestDB type produces a QWP geohash wire type.
     */
    public static boolean isGeoHash(int questdbColumnType) {
        return ColumnType.isGeoHash(questdbColumnType);
    }

    /**
     * Maps a QuestDB column type to the corresponding QWP wire type code.
     *
     * @throws UnsupportedOperationException if the type is not exportable over QWP.
     */
    public static byte toWireType(int questdbColumnType) {
        short tag = ColumnType.tagOf(questdbColumnType);
        if (tag == ColumnType.TIMESTAMP) {
            return ColumnType.isTimestampNano(questdbColumnType)
                    ? QwpConstants.TYPE_TIMESTAMP_NANOS
                    : QwpConstants.TYPE_TIMESTAMP;
        }
        if (ColumnType.isGeoHash(questdbColumnType)) {
            return QwpConstants.TYPE_GEOHASH;
        }
        if (ColumnType.isArray(questdbColumnType)) {
            short elementTag = ColumnType.decodeArrayElementType(questdbColumnType);
            return switch (elementTag) {
                case ColumnType.DOUBLE -> QwpConstants.TYPE_DOUBLE_ARRAY;
                case ColumnType.LONG -> QwpConstants.TYPE_LONG_ARRAY;
                default -> throw new UnsupportedOperationException(
                        "QWP egress: unsupported array element type " + ColumnType.nameOf(elementTag));
            };
        }
        return switch (tag) {
            case ColumnType.BOOLEAN -> QwpConstants.TYPE_BOOLEAN;
            case ColumnType.BYTE -> QwpConstants.TYPE_BYTE;
            case ColumnType.SHORT -> QwpConstants.TYPE_SHORT;
            case ColumnType.CHAR -> QwpConstants.TYPE_CHAR;
            case ColumnType.INT, ColumnType.IPv4 -> QwpConstants.TYPE_INT;
            case ColumnType.LONG -> QwpConstants.TYPE_LONG;
            case ColumnType.DATE -> QwpConstants.TYPE_DATE;
            case ColumnType.FLOAT -> QwpConstants.TYPE_FLOAT;
            case ColumnType.DOUBLE -> QwpConstants.TYPE_DOUBLE;
            case ColumnType.STRING -> QwpConstants.TYPE_STRING;
            case ColumnType.SYMBOL -> QwpConstants.TYPE_SYMBOL;
            case ColumnType.LONG256 -> QwpConstants.TYPE_LONG256;
            case ColumnType.UUID -> QwpConstants.TYPE_UUID;
            case ColumnType.VARCHAR -> QwpConstants.TYPE_VARCHAR;
            case ColumnType.DECIMAL64 -> QwpConstants.TYPE_DECIMAL64;
            case ColumnType.DECIMAL128 -> QwpConstants.TYPE_DECIMAL128;
            case ColumnType.DECIMAL256 -> QwpConstants.TYPE_DECIMAL256;
            default -> throw new UnsupportedOperationException(
                    "QWP egress: unsupported column type " + ColumnType.nameOf(questdbColumnType));
        };
    }
}
