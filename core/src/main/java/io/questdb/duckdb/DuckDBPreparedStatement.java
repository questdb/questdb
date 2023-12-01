/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.duckdb;

import io.questdb.std.QuietCloseable;
import io.questdb.std.str.DirectUtf8StringZ;

public class DuckDBPreparedStatement implements QuietCloseable {
    private final long statement;
    private final DirectUtf8StringZ text = new DirectUtf8StringZ();

    public DuckDBPreparedStatement(long statement) {
        this.statement = statement;
    }

    public DirectUtf8StringZ getQueryText() {
        long text = DuckDB.preparedGetQueryText(statement);
        this.text.of(text);
        return this.text;
    }

    public DirectUtf8StringZ getErrorText() {
        long text = DuckDB.preparedGetError(statement);
        this.text.of(text);
        return this.text;
    }

    public int getStatementType() {
        return DuckDB.preparedGetStatementType(statement);
    }

    public int getStatementReturnType() {
        return DuckDB.preparedGetStatementReturnType(statement);
    }

    public boolean isAllowStreamingResults() {
        return DuckDB.preparedAllowStreamResult(statement);
    }

    public long getParameterCount() {
        return DuckDB.preparedParameterCount(statement);
    }

    public long getColumnCount() {
        return DuckDB.preparedGetColumnCount(statement);
    }

    public int getColumnLogicalType(long col) {
        return DuckDB.preparedGetColumnLogicalType(statement, col);
    }

    public int getColumnPhysicalType(long col) {
        return DuckDB.preparedGetColumnPhysicalType(statement, col);
    }

    public DirectUtf8StringZ getColumnName(long col) {
        long text = DuckDB.preparedGetColumnName(statement, col);
        this.text.of(text);
        return this.text;
    }

    public DuckDBResult execute() {
        DuckDBResult result = new DuckDBResult();
        result.of(DuckDB.preparedExecute(statement));
        return result;
    }

    @Override
    public void close() {
        DuckDB.preparedDestroy(statement);
    }
}
