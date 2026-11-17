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

package io.questdb.test.griffin.fuzz;

import io.questdb.std.ObjList;

/**
 * The storage shape of a fuzz table that {@link FuzzTableFactory} gives a primary table, which its
 * shadow does not share: the shadow is always a partitioned WAL table that holds every column from
 * its first row, so the diff-shadow oracle compares each shape below with the plain one.
 *
 * @param isWal                      false for a {@code BYPASS WAL} table
 * @param isPartitioned              false for a {@code PARTITION BY NONE} table, which is never a WAL
 *                                   one
 * @param lateColumns                the columns that the table gains through
 *                                   {@code ALTER TABLE ADD COLUMN} after its first
 *                                   {@code firstChunkRows} rows, which are column tops of them;
 *                                   empty when it has every column from the start
 * @param firstChunkRows             the rows written before the late columns exist and the dropped
 *                                   column goes
 * @param droppedColumnPos           the position among the CREATE TABLE columns of a column that the
 *                                   table drops after its first rows, or -1 for none
 * @param isJoinKeyDictionaryPadded  true when the {@code sym} column's dictionary also holds the
 *                                   {@link #PADDING_TEXT_COUNT} texts of a day that the partitioned
 *                                   table dropped, far more than one per row of the table
 */
public record FuzzStorage(
        boolean isWal,
        boolean isPartitioned,
        ObjList<String> lateColumns,
        int firstChunkRows,
        int droppedColumnPos,
        boolean isJoinKeyDictionaryPadded
) {
    // The name of the column that a table with droppedColumnPos > -1 creates, then drops.
    public static final String DROPPED_COLUMN = "zdrop";
    // The texts that pad the sym dictionary of a table with isJoinKeyDictionaryPadded: a probe of
    // the table's rows, fewer than PADDING_TEXT_COUNT / 256, and so than PADDING_TEXT_COUNT / 16,
    // translates SYMBOL join keys through the join workers' own caches rather than a cache that the
    // dictionary sizes, under either operator.
    public static final int PADDING_TEXT_COUNT = 60_000;
    public static final FuzzStorage PLAIN = new FuzzStorage(true, true, new ObjList<>(), 0, -1, false);

    public String describe() {
        final StringBuilder sb = new StringBuilder();
        sb.append(isPartitioned ? (isWal ? "WAL" : "BYPASS WAL") : "PARTITION BY NONE BYPASS WAL");
        if (lateColumns.size() > 0) {
            sb.append(" late=[");
            for (int i = 0, n = lateColumns.size(); i < n; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(lateColumns.getQuick(i));
            }
            sb.append("] after ").append(firstChunkRows).append(" rows");
        }
        if (droppedColumnPos > -1) {
            sb.append(" dropped ").append(DROPPED_COLUMN).append('@').append(droppedColumnPos);
        }
        if (isJoinKeyDictionaryPadded) {
            sb.append(" padded sym dictionary");
        }
        return sb.toString();
    }

    public boolean hasColumnTops() {
        return lateColumns.size() > 0;
    }

    public boolean hasDroppedColumn() {
        return droppedColumnPos > -1;
    }

    public boolean isLate(String columnName) {
        for (int i = 0, n = lateColumns.size(); i < n; i++) {
            if (lateColumns.getQuick(i).equals(columnName)) {
                return true;
            }
        }
        return false;
    }
}
