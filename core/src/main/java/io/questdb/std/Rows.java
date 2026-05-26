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

package io.questdb.std;

/**
 * Row id packing utilities.
 * <p>
 * Bit layout of a packed row id:
 * <pre>
 *   [63..44]  partitionIndex (20 bits)
 *   [43]      physical flag (1 = physical, 0 = logical)
 *   [42..0]   localRowId (43 bits, ~8.8 T rows per partition)
 * </pre>
 * The physical flag distinguishes row ids that already refer to a partition's
 * physical (on-disk) row position from row ids that need
 * logical-to-physical translation via the partition's
 * {@code _sortedruns.idx} mapping (see
 * {@link io.questdb.cairo.sql.PartitionFormat#INDEXED_SORTED_RUNS}).
 * <p>
 * For {@code NATIVE} and {@code PARQUET} partitions physical equals logical, so
 * the flag is informational; for {@code INDEXED_SORTED_RUNS} consumers must
 * inspect it before doing a setRowIndex round-trip.
 */
public final class Rows {
    public static final int MAX_SAFE_PARTITION_INDEX = (1 << 19) - 1;
    private static final long LOCAL_ROW_ID_MASK = 0x7FFFFFFFFFFL;
    private static final long PHYSICAL_FLAG = 0x80000000000L;

    private Rows() {
    }

    public static boolean isPhysical(long rowID) {
        return (rowID & PHYSICAL_FLAG) != 0;
    }

    public static long toLocalRowID(long rowID) {
        return rowID & LOCAL_ROW_ID_MASK;
    }

    public static int toPartitionIndex(long rowID) {
        return (int) (rowID >>> 44);
    }

    /**
     * Packs a row id with the physical flag UNSET. Use for row ids that refer
     * to a partition's logical (timestamp-ascending) position. For
     * {@code NATIVE}/{@code PARQUET} partitions logical equals physical.
     */
    public static long toRowID(int partitionIndex, long localRowID) {
        return (((long) partitionIndex) << 44) + (localRowID & LOCAL_ROW_ID_MASK);
    }

    /**
     * Packs a row id with the physical flag SET. Use when the localRowID is
     * already the partition-physical row offset (e.g., from the cursor of an
     * {@code INDEXED_SORTED_RUNS} partition, an index reader, or any source
     * that produces raw column offsets). Consumers that round-trip the row id
     * through {@code recordAt} can then skip the
     * {@code _sortedruns.idx} translation.
     */
    public static long toRowIDPhysical(int partitionIndex, long physRowID) {
        return ((((long) partitionIndex) << 44) | PHYSICAL_FLAG) + (physRowID & LOCAL_ROW_ID_MASK);
    }
}
