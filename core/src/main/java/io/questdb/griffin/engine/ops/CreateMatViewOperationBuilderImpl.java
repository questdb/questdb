/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.griffin.engine.ops;

import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.CreateTableColumnModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


public class CreateMatViewOperationBuilderImpl implements CreateMatViewOperationBuilder, Mutable {
    // where each named column of the outermost SELECT starts in the statement
    private final LowerCaseCharSequenceIntHashMap columnPositions = new LowerCaseCharSequenceIntHashMap();
    private final CreateTableOperationBuilderImpl createTableOperationBuilder = new CreateTableOperationBuilderImpl();
    private String baseTableName;
    private int baseTableNamePosition;
    private boolean deferred;
    // whether the statement lists the view's columns, see SqlParser.parseCreateMatView()
    private boolean hasColumnList;
    private int periodDelay;
    private char periodDelayUnit;
    private int periodLength;
    private char periodLengthUnit;
    private int refreshType = -1;
    private String timeZone;
    private String timeZoneOffset;
    private int timerInterval;
    private long timerStartUs = Numbers.LONG_NULL;
    private String timerTimeZone;
    private char timerUnit;

    @Override
    public CreateMatViewOperation build(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, CharSequence sqlText) throws SqlException {
        // The column list, as the operation keeps it: the parser's column models are pooled.
        GenericRecordMetadata columnListMetadata = null;
        int columnListPosition = 0;
        IntList columnListTypePositions = null;
        if (hasColumnList) {
            final int columnCount = createTableOperationBuilder.getColumnCount();
            columnListMetadata = new GenericRecordMetadata();
            columnListTypePositions = new IntList(columnCount);
            final LowerCaseCharSequenceIntHashMap columnIndexes = new LowerCaseCharSequenceIntHashMap();
            for (int i = 0; i < columnCount; i++) {
                columnIndexes.put(createTableOperationBuilder.getColumnName(i), i);
            }
            for (int i = 0; i < columnCount; i++) {
                final CharSequence columnName = createTableOperationBuilder.getColumnName(i);
                final CreateTableColumnModel model = createTableOperationBuilder.getColumnModel(columnName);
                assert model != null;
                // INCLUDE names a listed column, with the errors of CREATE TABLE
                CreateTableOperationImpl.resolveCoveringColumnIndices(i, model, columnIndexes);
                columnListMetadata.add(new TableColumnMetadata(
                        Chars.toString(columnName),
                        model.getColumnType(),
                        model.getIndexType(),
                        model.getIndexValueBlockSize(),
                        true,
                        null,
                        -1,
                        false,
                        -1,
                        model.getSymbolCacheFlag(),
                        model.getSymbolCapacity()
                ));
                columnListTypePositions.add(model.getColumnTypePos());
                if (i == 0) {
                    columnListPosition = model.getColumnNamePos();
                }
            }
        }
        final CreateTableOperationImpl createTableOperation = createTableOperationBuilder.build(compiler, sqlExecutionContext, sqlText);
        return new CreateMatViewOperationImpl(
                sqlExecutionContext.getCairoEngine().getConfiguration(),
                Chars.toString(sqlText),
                createTableOperation,
                refreshType,
                deferred,
                baseTableName,
                baseTableNamePosition,
                timeZone,
                timeZoneOffset,
                timerInterval,
                timerUnit,
                timerStartUs,
                timerTimeZone,
                periodLength,
                periodLengthUnit,
                periodDelay,
                periodDelayUnit,
                columnPositions,
                columnListMetadata,
                columnListPosition,
                columnListTypePositions
        );
    }

    @Override
    public void clear() {
        columnPositions.clear();
        createTableOperationBuilder.clear();
        refreshType = -1;
        deferred = false;
        hasColumnList = false;
        baseTableName = null;
        baseTableNamePosition = 0;
        timeZone = null;
        timeZoneOffset = null;
        timerInterval = 0;
        timerUnit = 0;
        timerStartUs = Numbers.LONG_NULL;
        timerTimeZone = null;
        periodLength = 0;
        periodLengthUnit = 0;
        periodDelay = 0;
        periodDelayUnit = 0;
    }

    public LowerCaseCharSequenceIntHashMap getColumnPositions() {
        return columnPositions;
    }

    public CreateTableOperationBuilderImpl getCreateTableOperationBuilder() {
        return createTableOperationBuilder;
    }

    @Override
    public IQueryModel getQueryModel() {
        return createTableOperationBuilder.getQueryModel();
    }

    @Override
    public CharSequence getTableName() {
        return createTableOperationBuilder.getTableName();
    }

    public boolean hasColumnList() {
        return hasColumnList;
    }

    @Override
    public ExpressionNode getTableNameExpr() {
        return createTableOperationBuilder.getTableNameExpr();
    }

    public void setBaseTableName(String baseTableName) {
        this.baseTableName = baseTableName;
    }

    public void setBaseTableNamePosition(int baseTableNamePosition) {
        this.baseTableNamePosition = baseTableNamePosition;
    }

    public void setDeferred(boolean deferred) {
        this.deferred = deferred;
    }

    public void setHasColumnList(boolean hasColumnList) {
        this.hasColumnList = hasColumnList;
    }

    public void setPeriodLength(int length, char lengthUnit, int delay, char delayUnit) {
        this.periodLength = length;
        this.periodLengthUnit = lengthUnit;
        this.periodDelay = delay;
        this.periodDelayUnit = delayUnit;
    }

    public void setRefreshType(int refreshType) {
        this.refreshType = refreshType;
    }

    @Override
    public void setSelectModel(IQueryModel selectModel) {
        createTableOperationBuilder.setSelectModel(selectModel);
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public void setTimeZoneOffset(String timeZoneOffset) {
        this.timeZoneOffset = timeZoneOffset;
    }

    public void setTimer(@Nullable String timeZone, long startUs, int interval, char unit) {
        this.timerTimeZone = timeZone;
        // timerStart always use microsecond precision.
        this.timerStartUs = startUs;
        this.timerInterval = interval;
        this.timerUnit = unit;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("create materialized view ");
        sink.put(createTableOperationBuilder.getTableName());
        if (hasColumnList) {
            sink.putAscii(" (");
            createTableOperationBuilder.columnsToSink(sink);
            sink.putAscii(')');
        }
        if (baseTableName != null) {
            sink.putAscii(" with base ");
            sink.put(baseTableName);
        }
        sink.putAscii(" refresh");
        if (refreshType == MatViewDefinition.REFRESH_TYPE_TIMER) {
            sink.putAscii(" every ");
            sink.put(timerInterval);
            sink.putAscii(timerUnit);
            if (deferred) {
                sink.putAscii(" deferred");
            }
            if (periodLength == 0) {
                sink.putAscii(" start '");
                sink.putISODate(MicrosTimestampDriver.INSTANCE, timerStartUs);
                if (timerTimeZone != null) {
                    sink.putAscii("' time zone '");
                    sink.put(timerTimeZone);
                }
                sink.putAscii('\'');
            }
        } else if (refreshType == MatViewDefinition.REFRESH_TYPE_IMMEDIATE) {
            sink.putAscii(" immediate");
            if (deferred) {
                sink.putAscii(" deferred");
            }
        } else if (refreshType == MatViewDefinition.REFRESH_TYPE_MANUAL) {
            sink.putAscii(" manual");
            if (deferred) {
                sink.putAscii(" deferred");
            }
        }
        if (periodLength > 0) {
            sink.putAscii(" period (length ");
            sink.put(periodLength);
            sink.putAscii(periodLengthUnit);
            if (timerTimeZone != null) {
                sink.putAscii(" time zone '");
                sink.put(timerTimeZone);
                sink.putAscii('\'');
            }
            if (periodDelay > 0) {
                sink.putAscii(" delay ");
                sink.put(periodDelay);
                sink.putAscii(periodDelayUnit);
            }
            sink.putAscii(')');
        } else if (periodLength == -1) {
            sink.putAscii(" period (sample by interval)");
        }
        sink.putAscii(" as (");
        if (createTableOperationBuilder.getQueryModel() != null) {
            createTableOperationBuilder.getQueryModel().toSink(sink);
        }
        sink.putAscii(')');
        // a column list prints its columns' indexes
        final int indexClauseColumnCount = hasColumnList ? 0 : createTableOperationBuilder.getColumnCount();
        for (int i = 0; i < indexClauseColumnCount; i++) {
            final CharSequence columnName = createTableOperationBuilder.getColumnName(i);
            final CreateTableColumnModel columnModel = createTableOperationBuilder.getColumnModel(columnName);
            if (columnModel != null && columnModel.isIndexed()) {
                sink.putAscii(", index(");
                sink.put(columnName);
                sink.putAscii(" capacity ");
                sink.put(columnModel.getIndexValueBlockSize());
                sink.putAscii(')');
            }
        }

        if (createTableOperationBuilder.getTimestampExpr() != null) {
            sink.putAscii(" timestamp(");
            createTableOperationBuilder.getTimestampExpr().toSink(sink);
            sink.putAscii(')');
        }

        if (createTableOperationBuilder.getPartitionByFromExpr() != PartitionBy.NONE) {
            sink.putAscii(" partition by ").put(PartitionBy.toString(createTableOperationBuilder.getPartitionByFromExpr()));
        }

        createTableOperationBuilder.ttlToSink(sink);

        final CharSequence volumeAlias = createTableOperationBuilder.getVolumeAlias();
        if (volumeAlias != null) {
            sink.putAscii(" in volume '").put(volumeAlias).putAscii('\'');
        }
    }
}
