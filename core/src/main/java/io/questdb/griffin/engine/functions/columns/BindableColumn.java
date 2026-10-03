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

package io.questdb.griffin.engine.functions.columns;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;

/**
 * A column function that remembers the column id it reads and takes its input position once the physical layout is final.
 */
public interface BindableColumn extends Function {

    static boolean isBindableType(int type) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.ARRAY, ColumnType.TIMESTAMP, ColumnType.STRING, ColumnType.SYMBOL, ColumnType.VARCHAR,
                 ColumnType.BYTE, ColumnType.SHORT, ColumnType.CHAR, ColumnType.DATE, ColumnType.IPv4, ColumnType.INT,
                 ColumnType.BOOLEAN, ColumnType.LONG, ColumnType.LONG256, ColumnType.UUID, ColumnType.GEOBYTE,
                 ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG, ColumnType.FLOAT, ColumnType.DOUBLE ->
                    true;
            default -> false;
        };
    }

    static BindableColumn newInstance(int columnId, int type, boolean isSymbolTableStatic) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.ARRAY -> new BindableArrayColumn(columnId, type);
            case ColumnType.TIMESTAMP -> new BindableTimestampColumn(columnId, type);
            case ColumnType.STRING -> new BindableStrColumn(columnId);
            case ColumnType.SYMBOL -> new BindableSymbolColumn(columnId, isSymbolTableStatic);
            case ColumnType.VARCHAR -> new BindableVarcharColumn(columnId);
            case ColumnType.BYTE -> new BindableByteColumn(columnId);
            case ColumnType.SHORT -> new BindableShortColumn(columnId);
            case ColumnType.CHAR -> new BindableCharColumn(columnId);
            case ColumnType.DATE -> new BindableDateColumn(columnId);
            case ColumnType.IPv4 -> new BindableIPv4Column(columnId);
            case ColumnType.INT -> new BindableIntColumn(columnId);
            case ColumnType.BOOLEAN -> new BindableBooleanColumn(columnId);
            case ColumnType.LONG -> new BindableLongColumn(columnId);
            case ColumnType.LONG256 -> new BindableLong256Column(columnId);
            case ColumnType.UUID -> new BindableUuidColumn(columnId);
            case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG ->
                    new BindableGeoHashColumn(columnId, type);
            case ColumnType.FLOAT -> new BindableFloatColumn(columnId);
            case ColumnType.DOUBLE -> new BindableDoubleColumn(columnId);
            default -> throw new IllegalStateException("column type is not bindable");
        };
    }

    int getColumnId();

    /**
     * False once an audited NULL fold closes this discarded operand; a closed leaf needs no input slot.
     */
    boolean isOpen();

    void setColumnId(int columnId);

    void setColumnIndex(int columnIndex);
}
