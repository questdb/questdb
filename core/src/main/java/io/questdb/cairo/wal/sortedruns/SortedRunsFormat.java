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

package io.questdb.cairo.wal.sortedruns;

/**
 * File format constants for the per-partition <code>_sortedruns</code>
 * sidecar that records per-WAL-commit metadata for partitions stored in
 * indexed-sorted-runs layout. See <code>docs/wal-ts-storage-plan-v2.md</code>.
 * <p>
 * Layout:
 * <pre>
 * File header (64 bytes, padded):
 *   [0..3]   magic     u32 = "QTCS" (little-endian = 0x53435451)
 *   [4..5]   version   u16
 *   [6..7]   flags     u16
 *   [8..63]  reserved  (zero-filled)
 *
 * Record (36-byte header + variable extension payload):
 *   [0..7]   physRowStart  i64
 *   [8..11]  rowCount      u32
 *   [12..13] flags         u16
 *   [14..15] reserved      u16
 *   [16..23] minTs         i64
 *   [24..31] maxTs         i64
 *   [32..35] extLength     u32
 *   [36..]   extensions    extLength bytes (TLV records)
 *
 * Extension TLV:
 *   [0..1]   type   u16
 *   [2..3]   length u16
 *   [4..]    payload length bytes
 * </pre>
 */
public final class SortedRunsFormat {
    public static final int EXT_BLOOM = 3;
    public static final int EXT_NUMERIC_RANGE = 2;
    public static final int EXT_RECORD_HEADER_BYTES = 4;
    public static final int EXT_SINGLE_SERIES = 4;
    public static final int EXT_SYMBOL_PRESENCE = 1;
    public static final String FILE_NAME = "_sortedruns";
    public static final int FLAG_HAS_EXTENSIONS = 1;
    public static final int FLAG_MERGED = 2;
    public static final int FLAG_SORTED_BY_TS = 0;
    public static final int HEADER_OFFSET_FLAGS = 6;
    public static final int HEADER_OFFSET_MAGIC = 0;
    public static final int HEADER_OFFSET_VERSION = 4;
    public static final int HEADER_SIZE_BYTES = 64;
    public static final int MAGIC = 0x53435451;
    public static final int RECORD_HEADER_SIZE_BYTES = 36;
    public static final int RECORD_OFFSET_EXT_LENGTH = 32;
    public static final int RECORD_OFFSET_FLAGS = 12;
    public static final int RECORD_OFFSET_MAX_TS = 24;
    public static final int RECORD_OFFSET_MIN_TS = 16;
    public static final int RECORD_OFFSET_PHYS_ROW_START = 0;
    public static final int RECORD_OFFSET_ROW_COUNT = 8;
    public static final int VERSION_1 = 1;

    private SortedRunsFormat() {
    }
}
