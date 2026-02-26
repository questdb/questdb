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

package io.questdb.cairo.idx;

import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Constants for the FSST-compressed bitmap index.
 * <p>
 * One FSST symbol table per partition, trained once on first flush.
 * Each commit appends one generation to the value file.
 * <p>
 * Key File Layout (.lk):
 * <pre>
 * [Header 64B: sig(0xfc), seq, valMemSize, blockValues, keyCount, seqCheck, maxVal, genCount]
 * [Symbol table: FSST.SERIALIZED_MAX_SIZE bytes (padded)]
 * [Generation directory: genCount × 12B (offset(8), size(4))]
 * </pre>
 * <p>
 * Value File Layout (.lv):
 * <pre>
 * [Generation 0 data]
 * [Generation 1 data]
 * ...
 * </pre>
 * <p>
 * Generation Format (one per commit, covers all keys):
 * <pre>
 * [Per-key counts: keyCount × int]
 * [Per-key offsets: keyCount × int — byte offset into FSST data]
 * [Key 0 FSST-encoded values]
 * [Key 1 FSST-encoded values]
 * ...
 * </pre>
 */
public final class FSSTBitmapIndexUtils {

    // Key file header offsets (64 bytes)
    public static final int KEY_FILE_RESERVED = 64;
    public static final int KEY_RESERVED_OFFSET_SIGNATURE = 0;
    public static final int KEY_RESERVED_OFFSET_SEQUENCE = 8;
    public static final int KEY_RESERVED_OFFSET_VALUE_MEM_SIZE = 16;
    public static final int KEY_RESERVED_OFFSET_BLOCK_VALUES = 24;
    public static final int KEY_RESERVED_OFFSET_KEY_COUNT = 28;
    public static final int KEY_RESERVED_OFFSET_SEQUENCE_CHECK = 32;
    public static final int KEY_RESERVED_OFFSET_MAX_VALUE = 40;
    public static final int KEY_RESERVED_OFFSET_GEN_COUNT = 48;

    // Generation directory entry (12 bytes per generation)
    public static final int GEN_DIR_ENTRY_SIZE = 12;
    public static final int GEN_DIR_OFFSET_FILE_OFFSET = 0;
    public static final int GEN_DIR_OFFSET_SIZE = 8;

    public static final int DEFAULT_BLOCK_VALUES = 128;

    public static final byte SIGNATURE = (byte) 0xfc;

    // Symbol table stored immediately after header, padded to fixed size
    public static final int SYMBOL_TABLE_OFFSET = KEY_FILE_RESERVED;

    private FSSTBitmapIndexUtils() {
    }

    /**
     * Offset of generation directory entry in the key file.
     */
    public static long getGenDirOffset(int genIndex) {
        return SYMBOL_TABLE_OFFSET + FSST.SERIALIZED_MAX_SIZE + (long) genIndex * GEN_DIR_ENTRY_SIZE;
    }

    /**
     * Size of the per-generation header: counts + offsets for all keys.
     */
    public static int genHeaderSize(int keyCount) {
        return keyCount * Integer.BYTES * 2;
    }

    public static LPSZ keyFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".lk");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }

    public static LPSZ valueFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".lv");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }
}
