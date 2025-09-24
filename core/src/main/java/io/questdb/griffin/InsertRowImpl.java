/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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


import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.datetime.CommonUtils;

public final class InsertRowImpl implements QuietCloseable {
    private final RecordToRowCopier copier;
    private final RowFactory rowFactory;
    private final TimestampDriver timestampDriver;
    private final Function timestampFunction;
    private final CommonUtils.TimestampUnitConverter timestampFunctionConverter;
    private final int timestampFunctionPosition;
    private final int timestampType;
    private final int tupleIndex;
    private final VirtualRecord virtualRecord;

    public InsertRowImpl(
            VirtualRecord virtualRecord,
            RecordToRowCopier copier,
            Function timestampFunction,
            int timestampFunctionPosition,
            int tupleIndex,
            int timestampType
    ) {
        this.virtualRecord = virtualRecord;
        this.copier = copier;
        this.timestampFunction = timestampFunction;
        this.timestampFunctionPosition = timestampFunctionPosition;
        this.tupleIndex = tupleIndex;
        this.timestampType = timestampType;
        this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
        if (timestampFunction != null) {
            int type = timestampFunction.getType();
            if (ColumnType.isString(type) || ColumnType.isVarchar(type)) {
                rowFactory = this::getRowWithStringTimestamp;
                this.timestampFunctionConverter = null;
            } else {
                timestampFunctionConverter = timestampDriver.getTimestampUnitConverter(type);
                if (timestampFunctionConverter == null) {
                    rowFactory = this::getRowWithTimestamp;
                } else {
                    rowFactory = this::getRowWithTimestampWithConverter;
                }
            }
        } else {
            this.timestampFunctionConverter = null;
            rowFactory = this::getRowWithoutTimestamp;
        }
    }

    public void append(TableWriterAPI writer) {
        final TableWriter.Row row = rowFactory.getRow(writer);
        try {
            copier.copy(virtualRecord, row);
            row.append();
        } catch (Throwable e) {
            row.cancel();
            throw e;
        }
    }

    @Override
    public void close() {
        Misc.free(timestampFunction);
        Misc.free(virtualRecord);
    }

    public void initContext(SqlExecutionContext executionContext) throws SqlException {
        final ObjList<? extends Function> functions = virtualRecord.getFunctions();
        Function.init(functions, null, executionContext, null);
        if (timestampFunction != null) {
            timestampFunction.init(null, executionContext);
        }
    }

    private TableWriter.Row getRowWithStringTimestamp(TableWriterAPI tableWriter) {
        CharSequence timestampValue = timestampFunction.getStrA(null);
        if (timestampValue != null) {
            try {
                return tableWriter.newRow(
                        timestampDriver.fromStr(
                                timestampFunction.getStrA(null),
                                tupleIndex,
                                timestampFunction.getType(),
                                timestampType
                        )
                );
            } catch (CairoException e) {
                if (!e.isCritical()) {
                    e.position(timestampFunctionPosition);
                }
                throw e;
            }
        }
        throw CairoException.nonCritical().put("designated timestamp column cannot be NULL");
    }

    private TableWriter.Row getRowWithTimestamp(TableWriterAPI tableWriter) {
        try {
            return tableWriter.newRow(timestampFunction.getTimestamp(null));
        } catch (CairoException e) {
            if (!e.isCritical()) {
                e.position(timestampFunctionPosition);
            }
            throw e;
        }
    }

    private TableWriter.Row getRowWithTimestampWithConverter(TableWriterAPI tableWriter) {
        try {
            return tableWriter.newRow(timestampFunctionConverter.convert(timestampFunction.getTimestamp(null)));
        } catch (CairoException e) {
            if (!e.isCritical()) {
                e.position(timestampFunctionPosition);
            }
            throw e;
        }
    }

    private TableWriter.Row getRowWithoutTimestamp(TableWriterAPI tableWriter) {
        return tableWriter.newRow();
    }

    @FunctionalInterface
    private interface RowFactory {
        TableWriter.Row getRow(TableWriterAPI tableWriter);
    }
}
