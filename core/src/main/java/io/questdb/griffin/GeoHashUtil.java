/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;

public final class GeoHashUtil {

    private GeoHashUtil() {
    }

    public static int parseGeoHashBits(int position, int start, CharSequence sizeStr) throws SqlException {
        assert start >= 0;
        if (sizeStr.length() - start < 2) {
            throw SqlException.position(position)
                    .put("invalid GEOHASH size, must be number followed by 'C' or 'B' character");
        }
        int size;
        try {
            size = Numbers.parseInt(sizeStr, start, sizeStr.length() - 1);
        } catch (NumericException e) {
            throw SqlException.position(position)
                    .put("invalid GEOHASH size, must be number followed by 'C' or 'B' character");
        }
        switch (sizeStr.charAt(sizeStr.length() - 1)) {
            case 'C':
            case 'c':
                size *= 5;
                break;
            case 'B':
            case 'b':
                break;
            default:
                throw SqlException.position(position)
                        .put("invalid GEOHASH size units, must be 'c', 'C' for chars, or 'b', 'B' for bits");
        }
        if (size < 1 || size > ColumnType.GEOLONG_MAX_BITS) {
            throw SqlException.position(position)
                    .put("invalid GEOHASH type precision range, must be [1, 60] bits, provided=")
                    .put(size);
        }
        return size;
    }

    public static ConstantFunction parseGeoHashConstant(int position, final CharSequence tok, final int len) throws SqlException {
        try {
            if (tok.charAt(1) != '#') {
                // geohash from chars constant
                // optional '/dd', '/d' (max 3 chars, 1..60)
                final int sdd = ExpressionParser.extractGeoHashSuffix(position, tok);
                final int sddLen = Numbers.decodeLowShort(sdd);
                final int bits = Numbers.decodeHighShort(sdd);
                return Constants.getGeoHashConstant(
                        GeoHashes.fromStringTruncatingNl(tok, 1, len - sddLen, bits),
                        bits
                );
            } else {
                // geohash from binary constant
                // minus leading '##', truncates tail bits if over 60
                int bits = len - 2;
                if (bits <= ColumnType.GEOLONG_MAX_BITS) {
                    return Constants.getGeoHashConstant(
                            GeoHashes.fromBitStringNl(tok, 2),
                            bits
                    );
                }
            }
        } catch (NumericException ignored) {
        }
        return null;
    }
}
