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

/**
 * Defines the types of column indexes supported by QuestDB.
 * The index type is stored as a 2-bit value in the column metadata flags.
 */
public final class IndexType {
    /**
     * Mask for extracting the index type from metadata flags (bits 0-2).
     */
    public static final int INDEX_TYPE_MASK = 0x07;
    /**
     * No index on this column.
     */
    public static final byte NONE = 0;
    /**
     * Symbol index (original BitmapIndex for SYMBOL columns).
     */
    public static final byte SYMBOL = 1;
    /**
     * Delta-encoded bitmap index. Achieves 2-4x compression for sequential row IDs.
     */
    public static final byte DELTA = 2;
    /**
     * Frame of Reference (FOR) bitmap index. Fixed-size blocks with SIMD-friendly decoding.
     */
    public static final byte FOR = 3;
    /**
     * Roaring bitmap index. Uses hybrid container types (array/bitmap) for optimal compression.
     */
    public static final byte ROARING = 4;

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
            case DELTA -> "DELTA";
            case FOR -> "FOR";
            case ROARING -> "ROARING";
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
        if (equalsIgnoreCase(name, "SYMBOL") || equalsIgnoreCase(name, "LEGACY")) {
            return SYMBOL;
        }
        if (equalsIgnoreCase(name, "DELTA")) {
            return DELTA;
        }
        if (equalsIgnoreCase(name, "FOR")) {
            return FOR;
        }
        if (equalsIgnoreCase(name, "ROARING")) {
            return ROARING;
        }
        if (equalsIgnoreCase(name, "NONE")) {
            return NONE;
        }
        return NONE;
    }

    private static boolean equalsIgnoreCase(CharSequence a, String b) {
        if (a.length() != b.length()) {
            return false;
        }
        for (int i = 0; i < a.length(); i++) {
            if (Character.toUpperCase(a.charAt(i)) != Character.toUpperCase(b.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
