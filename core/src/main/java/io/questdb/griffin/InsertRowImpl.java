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


import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.std.ObjList;

public class InsertRowImpl {
    private final VirtualRecord virtualRecord;
    private final RecordToRowCopier copier;
    private final Function timestampFunction;
    private final RowFactory rowFactory;
    private final int tupleIndex;

    public InsertRowImpl(
            VirtualRecord virtualRecord,
            RecordToRowCopier copier,
            Function timestampFunction,
            int tupleIndex
    ) {
        this.virtualRecord = virtualRecord;
        this.copier = copier;
        this.timestampFunction = timestampFunction;
        this.tupleIndex = tupleIndex;
        if (timestampFunction != null) {
            if (!ColumnType.isString(timestampFunction.getType())) {
                rowFactory = this::getRowWithTimestamp;
            } else {
                rowFactory = this::getRowWithStringTimestamp;
            }
        } else {
            rowFactory = this::getRowWithoutTimestamp;
        }
    }

    private TableWriter.Row getRowWithTimestamp(TableWriterAPI tableWriter) {
        return tableWriter.newRow(timestampFunction.getTimestamp(null));
    }

    private TableWriter.Row getRowWithStringTimestamp(TableWriterAPI tableWriter) {
        return tableWriter.newRow(
                SqlUtil.parseFloorPartialTimestamp(
                        timestampFunction.getStr(null),
                        tupleIndex,
                        ColumnType.TIMESTAMP
                )
        );
    }

    private TableWriter.Row getRowWithoutTimestamp(TableWriterAPI tableWriter) {
        return tableWriter.newRow();
    }

    public void initContext(SqlExecutionContext executionContext) throws SqlException {
        final ObjList<? extends Function> functions = virtualRecord.getFunctions();
        Function.init(functions, null, executionContext);
        if (timestampFunction != null) {
            timestampFunction.init(null, executionContext);
        }
    }

    public void append(TableWriterAPI writer) throws SqlException {
        final TableWriter.Row row = rowFactory.getRow(writer);
        copier.copy(virtualRecord, row);
        row.append();
    }

    @FunctionalInterface
    private interface RowFactory {
        TableWriter.Row getRow(TableWriterAPI tableWriter) throws SqlException;
    }
}
