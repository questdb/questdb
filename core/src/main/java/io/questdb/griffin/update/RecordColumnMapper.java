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

import java.io.Closeable;

public interface RecordColumnMapper extends Closeable {
    byte getByte(Record record, int columnIndex);

    char getChar(Record record, int columnIndex);

    long getDate(Record record, int columnIndex);

    double getDouble(Record record, int columnIndex);

    float getFloat(Record record, int columnIndex);

    int getInt(Record record, int columnIndex);

    long getLong(Record record, int columnIndex);

    short getShort(Record record, int columnIndex);

    long getTimestamp(Record record, int columnIndex);

    void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException;
}