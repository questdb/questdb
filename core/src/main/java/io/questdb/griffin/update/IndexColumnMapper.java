/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.update;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;

import java.io.IOException;

class IndexColumnMapper implements RecordColumnMapper, Mutable {
    private IntList selectChooseColumnMaps;

    @Override
    public void clear() {
        selectChooseColumnMaps = null;
    }

    @Override
    public void close() throws IOException {
        selectChooseColumnMaps = Misc.free(selectChooseColumnMaps);
    }

    @Override
    public byte getByte(Record record, int columnIndex) {
        return record.getByte(selectChooseColumnMaps.get(columnIndex));
    }

    @Override
    public char getChar(Record record, int columnIndex) {
        return record.getChar(selectChooseColumnMaps.get(columnIndex));
    }

    @Override
    public long getDate(Record record, int columnIndex) {
        return record.getDate(selectChooseColumnMaps.get(columnIndex));
    }

    @Override
    public double getDouble(Record record, int columnIndex) {
        return record.getDouble(selectChooseColumnMaps.get(columnIndex));
    }

    @Override
    public float getFloat(Record record, int columnIndex) {
        return record.getFloat(selectChooseColumnMaps.get(columnIndex));
    }

    @Override
    public int getInt(Record record, int columnIndex) {
        return record.getInt(selectChooseColumnMaps.get(columnIndex));
    }

    @Override
    public long getLong(Record record, int columnIndex) {
        return record.getLong(selectChooseColumnMaps.get(columnIndex));
    }

    @Override
    public short getShort(Record record, int columnIndex) {
        return record.getShort(selectChooseColumnMaps.get(columnIndex));
    }

    @Override
    public long getTimestamp(Record record, int columnIndex) {
        return record.getTimestamp(selectChooseColumnMaps.get(columnIndex));
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
    }

    IndexColumnMapper of(IntList selectChooseColumnMaps) {
        this.selectChooseColumnMaps = selectChooseColumnMaps;
        return this;
    }
}