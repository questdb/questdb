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

import io.questdb.std.Chars;
import io.questdb.std.str.CharSink;

/**
 * Defines the types of column indexes supported by QuestDB.
 * The index type is stored as a 2-bit value in bits 0-1 of the column
 * metadata flags (values 0-3).
 */
public final class IndexType {
    /**
     * Bitmap index (original BitmapIndex for SYMBOL columns).
     */
    public static final byte BITMAP = 1;
    /**
     * No index on this column.
     */
    public static final byte NONE = 0;
    /**
     * Posting index with adaptive row ID encoding.
     * Trial-encodes both Elias-Fano and delta-FoR per key and picks the smaller.
     */
    public static final byte POSTING = 2;
    /**
     * Posting index with delta-FoR row ID encoding only (no Elias-Fano).
     * Created via {@code INDEX TYPE POSTING DELTA}.
     */
    public static final byte POSTING_DELTA = 3;

    private IndexType() {
        // Utility class, no instances
    }

    /**
     * Returns true if the given index type indicates that the column is indexed.
     *
     * @param indexType the index type value
     * @return true if indexed, false otherwise
     */
    public static boolean isIndexed(byte indexType) {
        return indexType != NONE;
    }

    public static boolean isPosting(byte indexType) {
        return indexType == POSTING || indexType == POSTING_DELTA;
    }

    /**
     * Returns the name of the given index type. Always returns a string constant
     * (no allocation), so unknown values are reported as the plain "UNKNOWN" literal.
     * Callers that need the numeric value of an unknown type should use one of the
     * {@code putName} overloads, which render it into a sink without allocating.
     *
     * @param indexType the index type value
     * @return the name of the index type
     */
    public static String nameOf(byte indexType) {
        return switch (indexType) {
            case NONE -> "NONE";
            case BITMAP -> "BITMAP";
            case POSTING -> "POSTING";
            case POSTING_DELTA -> "POSTING DELTA";
            default -> "UNKNOWN";
        };
    }

    public static <T extends CharSink<?>> void putName(T sink, byte indexType) {
        switch (indexType) {
            case NONE -> sink.putAscii("NONE");
            case BITMAP -> sink.putAscii("BITMAP");
            case POSTING -> sink.putAscii("POSTING");
            case POSTING_DELTA -> sink.putAscii("POSTING DELTA");
            default -> sink.putAscii("UNKNOWN(").put(indexType).putAscii(')');
        }
    }

    /**
     * Returns the index type for the given name.
     *
     * @param name the name of the index type (case-insensitive)
     * @return the index type value, or NONE if not recognized
     */
    public static byte valueOf(CharSequence name) {
        if (name == null || name.isEmpty()) {
            return NONE;
        }
        if (Chars.equalsIgnoreCase(name, "BITMAP")) {
            return BITMAP;
        }
        if (Chars.equalsIgnoreCase(name, "POSTING")) {
            return POSTING;
        }
        if (Chars.equalsIgnoreCase(name, "POSTING DELTA") || Chars.equalsIgnoreCase(name, "POSTING_DELTA")) {
            return POSTING_DELTA;
        }
        return NONE;
    }
}
