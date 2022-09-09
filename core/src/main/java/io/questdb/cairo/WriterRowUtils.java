/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.std.NumericException;

public class WriterRowUtils {

    private WriterRowUtils() {
    }

    public static void putGeoStr(int index, CharSequence hash, int type, TableWriter.Row row) {
        long val;
        if (hash != null) {
            final int hashLen = hash.length();
            final int typeBits = ColumnType.getGeoHashBits(type);
            final int charsRequired = (typeBits - 1) / 5 + 1;
            if (hashLen < charsRequired) {
                throw ImplicitCastException.inconvertibleValue(0, hash, ColumnType.STRING, type);
            } else {
                try {
                    val = ColumnType.truncateGeoHashBits(
                            GeoHashes.fromString(hash, 0, charsRequired),
                            charsRequired * 5,
                            typeBits
                    );
                } catch (NumericException e) {
                    throw ImplicitCastException.inconvertibleValue(0, hash, ColumnType.STRING, type);
                }
            }
        } else {
            val = GeoHashes.NULL;
        }
        putGeoHash(index, val, type, row);
    }

    public static void putGeoHash(int index, long value, int columnType, TableWriter.Row row) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.GEOBYTE:
                row.putByte(index, (byte) value);
                break;
            case ColumnType.GEOSHORT:
                row.putShort(index,(short) value);
                break;
            case ColumnType.GEOINT:
                row.putInt(index,(int) value);
                break;
            default:
                row.putLong(index, value);
                break;
        }
    }
}