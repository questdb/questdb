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

package io.questdb.griffin;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordColumnMapper;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

import java.io.IOException;

class FunctionsColumnMapper implements RecordColumnMapper, Mutable {
    private ObjList<Function> valuesFunctions = null;

    @Override
    public void clear() {
        valuesFunctions = null;
    }

    @Override
    public void close() throws IOException {
        Misc.freeObjList(valuesFunctions);
    }

    @Override
    public long getByte(Record record, int columnIndex) {
        return valuesFunctions.getQuick(columnIndex).getInt(record);
    }

    @Override
    public char getChar(Record record, int columnIndex) {
        return valuesFunctions.getQuick(columnIndex).getChar(record);
    }

    @Override
    public long getDate(Record record, int columnIndex) {
        return valuesFunctions.getQuick(columnIndex).getDate(record);
    }

    @Override
    public double getDouble(Record record, int columnIndex) {
        return valuesFunctions.getQuick(columnIndex).getDouble(record);
    }

    @Override
    public float getFloat(Record record, int columnIndex) {
        return valuesFunctions.getQuick(columnIndex).getFloat(record);
    }

    @Override
    public int getInt(Record record, int columnIndex) {
        return valuesFunctions.getQuick(columnIndex).getInt(record);
    }

    @Override
    public long getLong(Record record, int columnIndex) {
        return valuesFunctions.getQuick(columnIndex).getLong(record);
    }

    @Override
    public short getShort(Record record, int columnIndex) {
        return valuesFunctions.getQuick(columnIndex).getShort(record);
    }

    @Override
    public long getTimestamp(Record record, int columnIndex) {
        return valuesFunctions.getQuick(columnIndex).getTimestamp(record);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        for(int i = 0, n = valuesFunctions.size(); i < n; i++) {
            valuesFunctions.getQuick(i).init(symbolTableSource, executionContext);
        }
    }

    public FunctionsColumnMapper of(ObjList<Function> valuesFunctions) {
        this.valuesFunctions = valuesFunctions;
        return this;
    }
}