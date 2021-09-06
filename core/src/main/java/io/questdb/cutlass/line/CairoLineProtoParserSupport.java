/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.line;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlKeywords;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;

public class CairoLineProtoParserSupport {
    private final static Log LOG = LogFactory.getLog(CairoLineProtoParserSupport.class);

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
     * @throws BadCastException when value cannot be cast to the give type
     */
    public static void putValue(TableWriter.Row row,
                                int columnType,
                                int columnTypeMeta,
                                int columnIndex,
                                CharSequence value) throws BadCastException {
        try {
            switch (ColumnType.storageTag(columnType)) {
                case ColumnType.LONG:
                    row.putLong(columnIndex, Numbers.parseLong(value, 0, value.length() - 1));
                    break;
                case ColumnType.BOOLEAN:
                    row.putBool(columnIndex, isTrue(value));
                    break;
                case ColumnType.STRING:
                    row.putStr(columnIndex, value, 1, value.length() - 2);
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
                case ColumnType.SHORT:
                    row.putShort(columnIndex, Numbers.parseShort(value, 0, value.length() - 1));
                    break;
                case ColumnType.BYTE:
                    long v = Numbers.parseLong(value, 0, value.length() - 1);
                    if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                        throw CairoException.instance(0)
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
                    row.putGeoHashByte(columnIndex,  // skip quotes
                            (byte) GeoHashes.fromStringTruncatingNl(value, 1, value.length() - 1, columnTypeMeta));
                    break;
                case ColumnType.GEOSHORT:
                    row.putGeoHashShort(columnIndex,
                            (short) GeoHashes.fromStringTruncatingNl(value, 1, value.length() - 1, columnTypeMeta));
                    break;
                case ColumnType.GEOINT:
                    row.putGeoHashInt(columnIndex,
                            (int) GeoHashes.fromStringTruncatingNl(value, 1, value.length() - 1, columnTypeMeta));
                    break;
                case ColumnType.GEOLONG:
                    row.putGeoHashLong(columnIndex,
                            GeoHashes.fromStringTruncatingNl(value, 1, value.length() - 1, columnTypeMeta));
                    break;
                default:
                    // unsupported types are ignored
                    break;
            }
        } catch (NumericException e) {
            LOG.info()
                    .$("cast error [value=")
                    .$(value).$(", toType=")
                    .$(ColumnType.nameOf(columnType))
                    .$(']')
                    .$();
        }
    }

    public static class BadCastException extends Exception {
        public static final BadCastException INSTANCE = new BadCastException();
    }

    public static int getValueType(CharSequence token) {
        // method called for inbound ilp messages on each value.
        // returning UNDEFINED makes the whole line be skipped.
        // the goal of this method is to guess the potential type
        // and then it will be parsed accordingly by 'putValue'.
        int len = token.length();
        if (len > 0) {
            char lastChar = token.charAt(len - 1); // see LineProtoSender.field methods
            switch (lastChar) {
                case 'i':
                    if (len > 3 && token.charAt(0) == '0' && token.charAt(1) == 'x') {
                        return ColumnType.LONG256;
                    }
                    return len == 1 ? ColumnType.SYMBOL : ColumnType.LONG;
                case 'e':
                case 'E':
                    // tru(e)
                    //  fals(e)
                case 't':
                case 'T':
                    // t
                    // T
                case 'f':
                case 'F':
                    // f
                    // F
                    if (len == 1) {
                        return lastChar != 'e' ? ColumnType.BOOLEAN : ColumnType.SYMBOL;
                    }
                    return SqlKeywords.isTrueKeyword(token) || SqlKeywords.isFalseKeyword(token) ?
                            ColumnType.BOOLEAN : ColumnType.SYMBOL;
                case '"':
                    if (len < 2 || token.charAt(0) != '\"') {
                        LOG.error().$("incorrectly quoted string: ").$(token).$();
                        return ColumnType.UNDEFINED;
                    }
                    return ColumnType.STRING;
                default:
                    if (lastChar >= '0' && lastChar <= '9') {
                        if (len > 2 && token.charAt(0) == '0' && token.charAt(1) == 'x') {
                            return ColumnType.LONG256;
                        }
                        return ColumnType.DOUBLE;
                    }
                    return ColumnType.SYMBOL;
            }
        }
        return ColumnType.UNDEFINED;
    }

    public static boolean isTrue(CharSequence value) {
        return (value.charAt(0) | 32) == 't';
    }
}
