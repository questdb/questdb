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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
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
            if (timestampFunction.getType() != ColumnType.STRING) {
                rowFactory = this::getRowWithTimestamp;
            } else {
                rowFactory = this::getRowWithStringTimestamp;
            }
        } else {
            rowFactory = this::getRowWithoutTimestamp;
        }
        this.structureVersion = structureVersion;
    }
    @Override
    public void close() {
        detachWriter();
    }

    @Override
    public InsertMethod createMethod(SqlExecutionContext executionContext) {
        return createMethod(executionContext, engine);
    }

    @Override
    public InsertMethod createMethod(SqlExecutionContext executionContext, WriterSource writerSource) {
        initContext(executionContext);
        if (insertMethod.writer == null) {
            final TableWriter writer = writerSource.getWriter(executionContext.getCairoSecurityContext(), tableName, "insert");
            if (writer.getStructureVersion() != getStructureVersion()) {
                writer.close();
                throw WriterOutOfDateException.INSTANCE;
            }
            insertMethod.writer = writer;
        }
        return insertMethod;
    }

    @Override
    public long getStructureVersion() {
        return structureVersion;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public void detachWriter() {
        insertMethod.close();
    }

    private TableWriter.Row getRowWithTimestamp(TableWriter tableWriter) {
        long timestamp = timestampFunction.getTimestamp(null);
        return tableWriter.newRow(timestamp);
    }

    private TableWriter.Row getRowWithStringTimestamp(TableWriter tableWriter) {
        CharSequence tsStr = timestampFunction.getStr(null);
        try {
            long timestamp = IntervalUtils.parseFloorPartialDate(tsStr);
            return tableWriter.newRow(timestamp);
        } catch (NumericException e) {
            throw CairoException.instance(0).put("Invalid timestamp: ").put(tsStr);
        }
    }

    private TableWriter.Row getRowWithoutTimestamp(TableWriter tableWriter) {
        return tableWriter.newRow();
    }

    private void initContext(SqlExecutionContext executionContext) {
        final ObjList<? extends Function> functions = virtualRecord.getFunctions();
        Function.init(functions, null, executionContext);
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
        public long execute() {
            final TableWriter.Row row = rowFactory.getRow(writer);
            copier.copy(virtualRecord, row);
            row.append();
            return 1;
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
