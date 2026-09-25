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
import io.questdb.cairo.ColumnTypeTag;
import io.questdb.cutlass.qwp.protocol.QwpConstants;

/**
 * Maps QuestDB {@link ColumnType} to QWP wire type codes.
 */
public final class QwpColumnTypeMapper {

    private QwpColumnTypeMapper() {
    }

    /**
     * Maps a QuestDB column type to the corresponding QWP wire type code.
     *
     * @throws UnsupportedOperationException if the type is not exportable over QWP.
     */
    public static byte toWireType(int questdbColumnType) {
        return switch (ColumnTypeTag.of(questdbColumnType)) {
            case BOOLEAN -> QwpConstants.TYPE_BOOLEAN;
            case BYTE -> QwpConstants.TYPE_BYTE;
            case SHORT -> QwpConstants.TYPE_SHORT;
            case CHAR -> QwpConstants.TYPE_CHAR;
            case INT -> QwpConstants.TYPE_INT;
            case IPv4 -> QwpConstants.TYPE_IPV4;
            case LONG -> QwpConstants.TYPE_LONG;
            case DATE -> QwpConstants.TYPE_DATE;
            // the precision travels in the encoded type, so one tag has two wire codes
            case TIMESTAMP -> ColumnType.isTimestampNano(questdbColumnType)
                    ? QwpConstants.TYPE_TIMESTAMP_NANOS
                    : QwpConstants.TYPE_TIMESTAMP;
            case FLOAT -> QwpConstants.TYPE_FLOAT;
            case DOUBLE -> QwpConstants.TYPE_DOUBLE;
            // QuestDB STRING and VARCHAR share the wire layout; egress always advertises
            // TYPE_VARCHAR so clients see a single string type regardless of source column.
            case STRING, VARCHAR -> QwpConstants.TYPE_VARCHAR;
            case SYMBOL -> QwpConstants.TYPE_SYMBOL;
            case LONG256 -> QwpConstants.TYPE_LONG256;
            // every width shares one wire code; the precision travels in the column header
            case GEOBYTE, GEOSHORT, GEOINT, GEOLONG -> QwpConstants.TYPE_GEOHASH;
            case UUID -> QwpConstants.TYPE_UUID;
            case BINARY -> QwpConstants.TYPE_BINARY;
            case DECIMAL64 -> QwpConstants.TYPE_DECIMAL64;
            case DECIMAL128 -> QwpConstants.TYPE_DECIMAL128;
            case DECIMAL256 -> QwpConstants.TYPE_DECIMAL256;
            case ARRAY -> {
                short elementTag = ColumnType.decodeArrayElementType(questdbColumnType);
                yield switch (elementTag) {
                    case ColumnType.DOUBLE -> QwpConstants.TYPE_DOUBLE_ARRAY;
                    case ColumnType.LONG -> QwpConstants.TYPE_LONG_ARRAY;
                    default -> throw new UnsupportedOperationException(
                            "QWP egress: unsupported array element type " + ColumnType.nameOf(elementTag));
                };
            }
            // no wire code: the narrow decimals (the wire starts at DECIMAL64), LONG128, INTERVAL,
            // a NULL-typed projection, and the pseudo tags
            case DECIMAL8, DECIMAL16, DECIMAL32, LONG128, INTERVAL, NULL, UNDEFINED, CURSOR, VAR_ARG, RECORD, GEOHASH,
                 DECIMAL, REGCLASS, REGPROCEDURE, ARRAY_STRING, PARAMETER, VARCHAR_SLICE, UNKNOWN ->
                    throw new UnsupportedOperationException(
                            "QWP egress: unsupported column type " + ColumnType.nameOf(questdbColumnType));
        };
    }
}
