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

package io.questdb.cairo;

import io.questdb.cairo.sql.DataFrameCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.str.CharSink;

public abstract class AbstractDataFrameCursorFactory implements DataFrameCursorFactory {
    private final String tableName;
    private final int tableId;
    private final long tableVersion;

    public AbstractDataFrameCursorFactory(String tableName, int tableId, long tableVersion) {
        this.tableName = tableName;
        this.tableId = tableId;
        this.tableVersion = tableVersion;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put("{\"name\":\"").put(this.getClass().getSimpleName()).put("\", \"table\":\"").put(tableName).put("\"}");
    }

    protected TableReader getReader(SqlExecutionContext executionContext) throws SqlException {
        return executionContext.getCairoEngine()
                .getReader(
                        executionContext.getCairoSecurityContext(),
                        tableName,
                        tableId,
                        tableVersion
                );
    }

    @Override
    public void close() {
    }

    @Override
    public boolean supportTableRowId(CharSequence tableName) {
        return Chars.equalsIgnoreCaseNc(tableName, this.tableName);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.attr("tableName").val(tableName);
    }
}
