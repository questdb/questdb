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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.sql.*;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class InsertStatementImpl implements InsertStatement {
    private final long structureVersion;
    private final String tableName;
    private final InsertMethodImpl insertMethod = new InsertMethodImpl();
    private final ObjList<InsertRowImpl> insertRows = new ObjList<>();
    private final CairoEngine engine;

    public InsertStatementImpl(
            CairoEngine engine,
            String tableName,
            long structureVersion
    ) {
        this.engine = engine;
        this.tableName = tableName;
        this.structureVersion = structureVersion;
    }

    @Override
    public InsertMethod createMethod(SqlExecutionContext executionContext) throws SqlException {
        return createMethod(executionContext, engine);
    }

    @Override
    public InsertMethod createMethod(SqlExecutionContext executionContext, WriterSource writerSource) throws SqlException {
        initContext(executionContext);
        if (insertMethod.writer == null) {
            final TableWriter writer = writerSource.getWriter(executionContext.getCairoSecurityContext(), tableName, "insert");
            if (writer.getStructureVersion() != structureVersion) {
                writer.close();
                throw WriterOutOfDateException.INSTANCE;
            }
            insertMethod.writer = writer;
        }
        return insertMethod;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public void addInsertRow(InsertRowImpl row) {
        insertRows.add(row);
    }

    private void initContext(SqlExecutionContext executionContext) throws SqlException {
        for (int i = 0, n = insertRows.size(); i < n; i++) {
            InsertRowImpl row = insertRows.get(i);
            row.initContext(executionContext);
        }
    }

    private class InsertMethodImpl implements InsertMethod {
        private TableWriter writer = null;

        @Override
        public long execute() {
            for (int i = 0, n = insertRows.size(); i < n; i++) {
                InsertRowImpl row = insertRows.get(i);
                row.append(writer);
            }
            return insertRows.size();
        }

        @Override
        public void commit() {
            writer.commit();
        }

        @Override
        public TableWriter popWriter() {
            TableWriter w = writer;
            this.writer = null;
            return w;
        }

        @Override
        public void close() {
            writer = Misc.free(writer);
        }
    }
}
