/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.sql.*;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class InsertStatementImpl implements InsertStatement {
    private final VirtualRecord virtualRecord;
    private final SqlCompiler.RecordToRowCopier copier;
    private final Function timestampFunction;
    private final RowFactory rowFactory;
    private final long structureVersion;
    private final String tableName;
    private final InsertMethodImpl insertMethod = new InsertMethodImpl();
    private final CairoEngine engine;
    private SqlExecutionContext lastUsedContext;

    // todo: recycle these
    public InsertStatementImpl(
            CairoEngine engine,
            String tableName,
            VirtualRecord virtualRecord,
            SqlCompiler.RecordToRowCopier copier,
            Function timestampFunction,
            long structureVersion
    ) {
        this.engine = engine;
        this.tableName = tableName;
        this.virtualRecord = virtualRecord;
        this.copier = copier;
        this.timestampFunction = timestampFunction;
        if (timestampFunction != null) {
            rowFactory = this::getRowWithTimestamp;
        } else {
            rowFactory = this::getRowWithoutTimestamp;
        }
        this.structureVersion = structureVersion;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public long getStructureVersion() {
        return structureVersion;
    }

    @Override
    public InsertMethod createMethod(SqlExecutionContext executionContext) {
        initContext(executionContext);

        final TableWriter writer = engine.getWriter(executionContext.getCairoSecurityContext(), tableName);
        if (writer.getStructureVersion() != getStructureVersion()) {
            writer.close();
            throw WriterOutOfDateException.INSTANCE;
        }
        insertMethod.writer = writer;
        return insertMethod;
    }

    private TableWriter.Row getRowWithTimestamp(TableWriter tableWriter) {
        return tableWriter.newRow(timestampFunction.getTimestamp(null));
    }

    private TableWriter.Row getRowWithoutTimestamp(TableWriter tableWriter) {
        return tableWriter.newRow();
    }

    private void initContext(SqlExecutionContext executionContext) {
        lastUsedContext = executionContext;
        final ObjList<? extends Function> functions = virtualRecord.getFunctions();
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).init(null, executionContext);
        }
        if (timestampFunction != null) {
            timestampFunction.init(null, executionContext);
        }
    }

    @FunctionalInterface
    private interface RowFactory {
        TableWriter.Row getRow(TableWriter tableWriter);
    }

    private class InsertMethodImpl implements InsertMethod {
        private TableWriter writer = null;

        @Override
        public void execute() {
            final TableWriter.Row row = rowFactory.getRow(writer);
            copier.copy(virtualRecord, row);
            row.append();
        }

        @Override
        public void commit() {
            writer.commit();
        }

        @Override
        public void close() {
            writer = Misc.free(writer);
        }
    }
}
