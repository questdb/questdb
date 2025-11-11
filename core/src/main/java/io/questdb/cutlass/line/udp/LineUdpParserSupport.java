/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.line.udp;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlKeywords;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.Utf8StringSink;

public class LineUdpParserSupport {
    private final static Log LOG = LogFactory.getLog(LineUdpParserSupport.class);

    public static int getValueType(CharSequence value) {
        return getValueType(value, ColumnType.DOUBLE, ColumnType.LONG, true);
    }

    public static int getValueType(CharSequence value, boolean useLegacyStringDefault) {
        return getValueType(value, ColumnType.DOUBLE, ColumnType.LONG, useLegacyStringDefault);
    }

    public static int getValueType(CharSequence value, short defaultFloatColumnType, short defaultIntegerColumnType, boolean useLegacyStringDefault) {
        // method called for inbound ilp messages on each value.
        // returning UNDEFINED makes the whole line be skipped.
        // 0 len values, return null type.
        // the goal of this method is to guess the potential type,
        // and then it will be parsed accordingly by 'putValue'.
        int valueLen = value.length();
        if (valueLen > 0) {
            char first = value.charAt(0);
            char last = value.charAt(valueLen - 1); // see AbstractLineSender.field methods
            switch (last) {
                case 'i':
                    if (valueLen > 3 && value.charAt(0) == '0' && value.charAt(1) == 'x') {
                        return ColumnType.LONG256;
                    }
                    return valueLen == 1 ? ColumnType.SYMBOL : defaultIntegerColumnType;
                case 't':
                    if (valueLen > 1 && ((first >= '0' && first <= '9') || first == '-')) {
                        return ColumnType.TIMESTAMP_MICRO;
                    }
                    // fall through
                case 'T':
                    // t
                    // T
                case 'e':
                case 'E':
                    // tru(e)
                    // fals(e)
                case 'f':
                case 'F':
                    // f
                    // F
                    if (valueLen == 1) {
                        return last != 'e' ? ColumnType.BOOLEAN : ColumnType.SYMBOL;
                    }
                    return SqlKeywords.isTrueKeyword(value) || SqlKeywords.isFalseKeyword(value) ?
                            ColumnType.BOOLEAN : ColumnType.SYMBOL;
                case '"':
                    if (valueLen < 2 || value.charAt(0) != '\"') {
                        LOG.error().$("incorrectly quoted string: ").$(value).$();
                        return ColumnType.UNDEFINED;
                    }
                    return useLegacyStringDefault ? ColumnType.STRING : ColumnType.VARCHAR;
                default:
                    if (last >= '0' && last <= '9' && ((first >= '0' && first <= '9') || first == '-' || first == '.')) {
                        return defaultFloatColumnType;
                    }
                    if (SqlKeywords.isNanKeyword(value)) {
                        return defaultFloatColumnType;
                    }
                    if (value.charAt(0) == '"') {
                        return ColumnType.UNDEFINED;
                    }
                    return ColumnType.SYMBOL;
            }
        }
        return ColumnType.NULL;
    }

    /**
     * Writes column value to table row. CharSequence value is interpreted depending on
     * column type and written to column, identified by columnIndex. If value cannot be
     * cast to column type, #BadCastException is thrown.
     *
     * @param row            table row
     * @param columnType     column type value will be cast to
     * @param columnTypeMeta if columnType's tag is GeoHash it contains bits precision (low short)
     *                       and tag size (high short negative), otherwise -1
     * @param columnIndex    index of column to write value to
     * @param value          value characters
     */
    public static void putValue(
            TableWriter.Row row,
            int columnType,
            int columnTypeMeta,
            int columnIndex,
            CharSequence value
    ) {
        if (!value.isEmpty()) {
            try {
                switch (ColumnType.tagOf(columnType)) {
                    case ColumnType.LONG:
                        row.putLong(columnIndex, Numbers.parseLong(value, 0, value.length() - 1));
                        break;
                    case ColumnType.BOOLEAN:
                        row.putBool(columnIndex, isTrue(value));
                        break;
                    case ColumnType.STRING:
                        row.putStr(columnIndex, value, 1, value.length() - 2);
                        break;
                    case ColumnType.VARCHAR:
                        Utf8StringSink utf8Sink = Misc.getThreadLocalUtf8Sink();
                        utf8Sink.put(value, 1, value.length() - 1);
                        row.putVarchar(columnIndex, utf8Sink);
                        break;
                    case ColumnType.SYMBOL:
                        row.putSym(columnIndex, value);
                        break;
                    case ColumnType.DOUBLE:
                        row.putDouble(columnIndex, Numbers.parseDouble(value));
                        break;
                    case ColumnType.FLOAT:
                        row.putFloat(columnIndex, Numbers.parseFloat(value));
                        break;
                    case ColumnType.INT:
                        row.putInt(columnIndex, Numbers.parseInt(value, 0, value.length() - 1));
                        break;
                    case ColumnType.IPv4:
                        row.putIPv4(columnIndex, Numbers.parseIPv4UDP(value));
                        break;
                    case ColumnType.SHORT:
                        row.putShort(columnIndex, Numbers.parseShort(value, 0, value.length() - 1));
                        break;
                    case ColumnType.BYTE:
                        long v = Numbers.parseLong(value, 0, value.length() - 1);
                        if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                            throw CairoException.nonCritical()
                                    .put("line protocol integer is out of byte bounds [columnIndex=")
                                    .put(columnIndex)
                                    .put(", v=")
                                    .put(v)
                                    .put(']');
                        }
                        row.putByte(columnIndex, (byte) v);
                        break;
                    case ColumnType.DATE:
                        row.putDate(columnIndex, Numbers.parseLong(value, 0, value.length() - 1));
                        break;
                    case ColumnType.LONG256:
                        int limit = value.length() - 1;
                        if (value.charAt(limit) != 'i') {
                            limit++;
                        }
                        row.putLong256(columnIndex, value, 2, limit);
                        break;
                    case ColumnType.TIMESTAMP:
                        row.putTimestamp(columnIndex, Numbers.parseLong(value, 0, value.length() - 1));
                        break;
                    case ColumnType.CHAR:
                        row.putChar(columnIndex, value.length() == 2 ? (char) 0 : value.charAt(1)); // skip quotes
                        break;
                    case ColumnType.GEOBYTE:
                        row.putByte(
                                columnIndex,  // skip quotes
                                (byte) GeoHashes.fromStringTruncatingNl(
                                        value,
                                        1,
                                        value.length() - 1,
                                        columnTypeMeta
                                )
                        );
                        break;
                    case ColumnType.GEOSHORT:
                        row.putShort(
                                columnIndex,
                                (short) GeoHashes.fromStringTruncatingNl(
                                        value,
                                        1,
                                        value.length() - 1,
                                        columnTypeMeta
                                )
                        );
                        break;
                    case ColumnType.GEOINT:
                        row.putInt(
                                columnIndex,
                                (int) GeoHashes.fromStringTruncatingNl(
                                        value,
                                        1,
                                        value.length() - 1,
                                        columnTypeMeta
                                )
                        );
                        break;
                    case ColumnType.GEOLONG:
                        row.putLong(
                                columnIndex,
                                GeoHashes.fromStringTruncatingNl(
                                        value,
                                        1,
                                        value.length() - 1,
                                        columnTypeMeta
                                )
                        );
                        break;
                    case ColumnType.NULL:
                    default:
                        // unsupported types and null are ignored
                        break;
                }
            } catch (NumericException | ImplicitCastException e) {
                LOG.info()
                        .$("cast error [value=")
                        .$(value).$(", toType=")
                        .$(ColumnType.nameOf(columnType))
                        .$(']')
                        .$();
            }
        } else {
            putNullValue(row, columnIndex, columnType);
        }
    }

    private static boolean isTrue(CharSequence value) {
        return (value.charAt(0) | 32) == 't';
    }

    private static void putNullValue(TableWriter.Row row, int columnIndex, int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN:
                row.putBool(columnIndex, false);
                break;
            case ColumnType.STRING:
                row.putStr(columnIndex, null);
                break;
            case ColumnType.VARCHAR:
                row.putVarchar(columnIndex, null);
                break;
            case ColumnType.SYMBOL:
                row.putSym(columnIndex, null);
                break;
            case ColumnType.DOUBLE:
                row.putDouble(columnIndex, Double.NaN);
                break;
            case ColumnType.FLOAT:
                row.putFloat(columnIndex, Float.NaN);
                break;
            case ColumnType.LONG:
                row.putLong(columnIndex, Numbers.LONG_NULL);
                break;
            case ColumnType.INT:
                row.putInt(columnIndex, Numbers.INT_NULL);
                break;
            case ColumnType.IPv4:
                row.putIPv4(columnIndex, Numbers.IPv4_NULL);
            case ColumnType.SHORT:
                row.putShort(columnIndex, (short) 0);
                break;
            case ColumnType.BYTE:
                row.putByte(columnIndex, (byte) 0);
                break;
            case ColumnType.CHAR:
                row.putChar(columnIndex, (char) 0);
                break;
            case ColumnType.DATE:
                row.putDate(columnIndex, Numbers.LONG_NULL);
                break;
            case ColumnType.TIMESTAMP:
                row.putTimestamp(columnIndex, Numbers.LONG_NULL);
                break;
            case ColumnType.LONG256:
                row.putLong256(columnIndex, "");
                break;
            case ColumnType.GEOBYTE:
                row.putByte(columnIndex, GeoHashes.BYTE_NULL);
                break;
            case ColumnType.GEOSHORT:
                row.putShort(columnIndex, GeoHashes.SHORT_NULL);
                break;
            case ColumnType.GEOINT:
                row.putInt(columnIndex, GeoHashes.INT_NULL);
                break;
            case ColumnType.GEOLONG:
                row.putLong(columnIndex, GeoHashes.NULL);
                break;
            default:
                // unsupported types are ignored
                break;
        }
    }

    public static class BadCastException extends Exception {
        public static final BadCastException INSTANCE = new BadCastException();
    }
}
