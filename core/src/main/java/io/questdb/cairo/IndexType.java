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

package io.questdb.cairo;

import io.questdb.std.Chars;

/**
 * Defines the types of column indexes supported by QuestDB.
 * The index type is stored as a 2-bit value in bits 0-1 of the column
 * metadata flags (values 0-3).
 */
public final class IndexType {
    /**
     * Mask for extracting the 2-bit index type value.
     */
    public static final int INDEX_TYPE_MASK = 0x03;
    /**
     * No index on this column.
     */
    public static final byte NONE = 0;
    /**
     * Symbol index (original BitmapIndex for SYMBOL columns).
     */
    public static final byte SYMBOL = 1;
    /**
     * Posting index. Delta + FoR64 bitpacking with stride-indexed layout.
     */
    public static final byte POSTING = 2;
    /**
     * FSST-compressed bitmap index. Uses Finite State Symbol Table compression for postings.
     */
    public static final byte FSST = 3;

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

    /**
     * Returns the name of the given index type.
     *
     * @param indexType the index type value
     * @return the name of the index type
     */
    public static String nameOf(byte indexType) {
        return switch (indexType) {
            case NONE -> "NONE";
            case SYMBOL -> "SYMBOL";
            case POSTING -> "POSTING";
            case FSST -> "FSST";
            default -> "UNKNOWN(" + indexType + ")";
        };
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
        // Case-insensitive comparison
        if (Chars.equalsIgnoreCase(name, "SYMBOL") || Chars.equalsIgnoreCase(name, "LEGACY")) {
            return SYMBOL;
        }
        if (Chars.equalsIgnoreCase(name, "POSTING")) {
            return POSTING;
        }
        if (Chars.equalsIgnoreCase(name, "FSST")) {
            return FSST;
        }
        if (Chars.equalsIgnoreCase(name, "NONE")) {
            return NONE;
        }
        return NONE;
    }
}
