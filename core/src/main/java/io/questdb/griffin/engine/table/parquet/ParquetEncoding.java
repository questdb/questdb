/*******************************************************************************
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

package io.questdb.griffin.engine.table.parquet;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlException;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.Os;
import io.questdb.std.str.StringSink;

public class ParquetEncoding {
    public static final int ENCODING_DEFAULT = 0;
    public static final int ENCODING_PLAIN = ENCODING_DEFAULT + 1; // 1
    public static final int ENCODING_RLE_DICTIONARY = ENCODING_PLAIN + 1; // 2
    public static final int ENCODING_DELTA_LENGTH_BYTE_ARRAY = ENCODING_RLE_DICTIONARY + 1; // 3
    public static final int ENCODING_DELTA_BINARY_PACKED = ENCODING_DELTA_LENGTH_BYTE_ARRAY + 1; // 4
    public static final int ENCODING_BYTE_STREAM_SPLIT = ENCODING_DELTA_BINARY_PACKED + 1; // 5
    public static final int MAX_ENUM_INT = ENCODING_BYTE_STREAM_SPLIT + 1;
    private static final StringSink ENCODING_NAMES = new StringSink(64);
    private static final IntObjHashMap<CharSequence> encodingToNameMap = new IntObjHashMap<>(16);
    private static final LowerCaseCharSequenceIntHashMap nameToEncodingMap = new LowerCaseCharSequenceIntHashMap(32);

    /**
     * Appends the comma-separated list of supported encoding names to the given exception.
     */
    public static void addEncodingNamesToException(SqlException e) {
        e.put(ENCODING_NAMES);
    }

    /**
     * Looks up an encoding id by its case-insensitive name (e.g. "plain", "rle_dictionary").
     *
     * @return the encoding constant, or -1 if the name is not recognised
     */
    public static int getEncoding(CharSequence name) {
        return nameToEncodingMap.get(name);
    }

    /**
     * Returns the human-readable name for the given encoding constant, or null if unknown.
     */
    public static CharSequence getEncodingName(int encoding) {
        return encodingToNameMap.get(encoding);
    }

    /**
     * Checks whether the given Parquet encoding is compatible with the column type.
     * The validation logic lives in Rust (the single source of truth for the
     * encoding-type compatibility matrix) and this method delegates via JNI.
     * {@link #ENCODING_DEFAULT} is valid for every column type.
     *
     * @param encoding   one of the {@code ENCODING_*} constants
     * @param columnType full QuestDB column type (tag + flags), see {@link ColumnType}
     * @return true if the encoding can be applied to columns of this type
     */
    public static boolean isValidForColumnType(int encoding, int columnType) {
        return isEncodingValid0(encoding, ColumnType.tagOf(columnType));
    }

    /**
     * Native bridge into the Rust {@code is_encoding_valid_for_column_tag()} function.
     */
    private static native boolean isEncodingValid0(int encodingId, int columnTypeTag);

    static {
        Os.init();
        nameToEncodingMap.put("plain", ENCODING_PLAIN);
        nameToEncodingMap.put("rle_dictionary", ENCODING_RLE_DICTIONARY);
        nameToEncodingMap.put("delta_length_byte_array", ENCODING_DELTA_LENGTH_BYTE_ARRAY);
        nameToEncodingMap.put("delta_binary_packed", ENCODING_DELTA_BINARY_PACKED);
        nameToEncodingMap.put("byte_stream_split", ENCODING_BYTE_STREAM_SPLIT);

        encodingToNameMap.put(ENCODING_PLAIN, "plain");
        encodingToNameMap.put(ENCODING_RLE_DICTIONARY, "rle_dictionary");
        encodingToNameMap.put(ENCODING_DELTA_LENGTH_BYTE_ARRAY, "delta_length_byte_array");
        encodingToNameMap.put(ENCODING_DELTA_BINARY_PACKED, "delta_binary_packed");
        encodingToNameMap.put(ENCODING_BYTE_STREAM_SPLIT, "byte_stream_split");

        for (int i = 1, n = MAX_ENUM_INT; i < n; i++) {
            ENCODING_NAMES.put(encodingToNameMap.get(i));
            if (i + 1 != n) {
                ENCODING_NAMES.put(", ");
            }
        }
    }
}
