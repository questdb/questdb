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

package io.questdb.cairo.wal;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.TableWriter;
import io.questdb.std.NumericException;
import io.questdb.std.str.Utf8Sequence;

public class WriterRowUtils {

    private WriterRowUtils() {
    }

    public static void putGeoHash(int index, long value, int columnType, TableWriter.Row row) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.GEOBYTE:
                row.putByte(index, (byte) value);
                break;
            case ColumnType.GEOSHORT:
                row.putShort(index, (short) value);
                break;
            case ColumnType.GEOINT:
                row.putInt(index, (int) value);
                break;
            default:
                row.putLong(index, value);
                break;
        }
    }

    public static void putGeoStr(int index, CharSequence hash, int type, TableWriter.Row row) {
        if (hash == null) {
            putGeoHash(index, GeoHashes.NULL, type, row);
            return;
        }

        final int hashLen = hash.length();
        final int typeBits = ColumnType.getGeoHashBits(type);
        final int charsRequired = (typeBits - 1) / 5 + 1;
        if (hashLen < charsRequired) {
            throw ImplicitCastException.inconvertibleValue(hash, ColumnType.STRING, type);
        }

        try {
            long val = GeoHashes.widen(
                    GeoHashes.fromString(hash, 0, charsRequired),
                    charsRequired * 5,
                    typeBits
            );
            putGeoHash(index, val, type, row);
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(hash, ColumnType.STRING, type);
        }
    }

    public static void putGeoVarchar(int index, Utf8Sequence hash, int type, TableWriter.Row row) {
        if (hash == null) {
            putGeoHash(index, GeoHashes.NULL, type, row);
            return;
        }

        final int hashLen = hash.size();
        final int typeBits = ColumnType.getGeoHashBits(type);
        final int bytesRequired = (typeBits - 1) / 5 + 1;
        if (hashLen < bytesRequired) {
            throw ImplicitCastException.inconvertibleValue(hash, ColumnType.VARCHAR, type);
        }

        try {
            long val = GeoHashes.widen(
                    GeoHashes.fromAscii(hash, 0, bytesRequired),
                    bytesRequired * 5,
                    typeBits
            );
            putGeoHash(index, val, type, row);
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(hash, ColumnType.VARCHAR, type);
        }
    }
}
