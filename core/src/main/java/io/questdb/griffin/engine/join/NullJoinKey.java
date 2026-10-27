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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.Uuid;

/**
 * The key columns of one input of a hash outer join whose keys a later INNER join implies, see
 * {@code JoinInput.NullExtension#BY_ON_RESIDUAL}: tells whether every key column of a row is NULL, so that the join
 * decides the NULL extension of the row by its ON residual. A BOOLEAN, BYTE or SHORT key has no NULL.
 */
public final class NullJoinKey {
    private final IntList columnIndexes = new IntList();
    private final IntList columnTypes = new IntList();

    public NullJoinKey(IntList columnIndexes, IntList columnTypes) {
        this.columnIndexes.addAll(columnIndexes);
        this.columnTypes.addAll(columnTypes);
    }

    public boolean isNull(Record record) {
        for (int i = 0, n = columnIndexes.size(); i < n; i++) {
            if (!isNull(record, columnIndexes.getQuick(i), columnTypes.getQuick(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isNull(Record record, int columnIndex, int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.CHAR -> record.getChar(columnIndex) == 0;
            case ColumnType.INT -> record.getInt(columnIndex) == Numbers.INT_NULL;
            case ColumnType.LONG -> record.getLong(columnIndex) == Numbers.LONG_NULL;
            case ColumnType.DATE -> record.getDate(columnIndex) == Numbers.LONG_NULL;
            case ColumnType.TIMESTAMP -> record.getTimestamp(columnIndex) == Numbers.LONG_NULL;
            case ColumnType.IPv4 -> record.getIPv4(columnIndex) == Numbers.IPv4_NULL;
            case ColumnType.UUID -> Uuid.isNull(record.getLong128Lo(columnIndex), record.getLong128Hi(columnIndex));
            case ColumnType.SYMBOL -> record.getSymA(columnIndex) == null;
            case ColumnType.STRING -> record.getStrA(columnIndex) == null;
            case ColumnType.VARCHAR -> record.getVarcharA(columnIndex) == null;
            default -> false;
        };
    }
}
