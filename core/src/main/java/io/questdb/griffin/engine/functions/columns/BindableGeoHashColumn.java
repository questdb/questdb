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
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.AbstractGeoHashFunction;

final class BindableGeoHashColumn extends AbstractGeoHashFunction implements BindableColumn {
    private int columnId;
    private int columnIndex = -1;
    private boolean isOpen = true;

    BindableGeoHashColumn(int columnId, int type) {
        super(type);
        this.columnId = columnId;
    }

    @Override
    public void close() {
        isOpen = false;
    }

    @Override
    public int getColumnId() {
        return columnId;
    }

    @Override
    public byte getGeoByte(Record record) {
        assert columnIndex >= 0 && ColumnType.tagOf(type) == ColumnType.GEOBYTE;
        return record.getGeoByte(columnIndex);
    }

    @Override
    public int getGeoInt(Record record) {
        assert columnIndex >= 0 && ColumnType.tagOf(type) == ColumnType.GEOINT;
        return record.getGeoInt(columnIndex);
    }

    @Override
    public long getGeoLong(Record record) {
        assert columnIndex >= 0 && ColumnType.tagOf(type) == ColumnType.GEOLONG;
        return record.getGeoLong(columnIndex);
    }

    @Override
    public short getGeoShort(Record record) {
        assert columnIndex >= 0 && ColumnType.tagOf(type) == ColumnType.GEOSHORT;
        return record.getGeoShort(columnIndex);
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    @Override
    public void setColumnId(int columnId) {
        assert columnIndex == -1 && isOpen;
        this.columnId = columnId;
    }

    @Override
    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.putColumnName(columnIndex);
    }
}
