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

package io.questdb.griffin.model;

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MaterializedViewDefinition;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

public class CreateMatViewModel implements Mutable, ExecutionModel, Sinkable {
    public static final ObjectFactory<CreateMatViewModel> FACTORY = CreateMatViewModel::new;
    private final CreateTableModel tableModel;
    private TableToken baseTableToken;
    private long fromMicros = -1;
    private long samplingInterval = -1;
    private char samplingIntervalUnit;
    private String timeZone;
    private String timeZoneOffset;
    private long toMicros = -1;
    private String viewSql;

    private CreateMatViewModel() {
        tableModel = CreateTableModel.FACTORY.newInstance();
    }

    @Override
    public void clear() {
        tableModel.clear();
        viewSql = null;
        baseTableToken = null;
        samplingInterval = -1;
        samplingIntervalUnit = '\0';
        fromMicros = -1;
        toMicros = -1;
        timeZone = null;
        timeZoneOffset = null;
    }

    public MaterializedViewDefinition generateDefinition(@NotNull TableToken matViewToken) {
        return new MaterializedViewDefinition(matViewToken, viewSql, baseTableToken, samplingInterval, samplingIntervalUnit,
                fromMicros, toMicros, timeZone, timeZoneOffset
        );
    }

    @Override
    public int getModelType() {
        return CREATE_MAT_VIEW;
    }

    public QueryModel getQueryModel() {
        return tableModel.getQueryModel();
    }

    public CreateTableModel getTableModel() {
        return tableModel;
    }

    @Override
    public CharSequence getTableName() {
        return tableModel.getTableName();
    }

    @Override
    public ExpressionNode getTableNameExpr() {
        return tableModel.getTableNameExpr();
    }

    public void setBaseTableToken(TableToken baseTableToken) {
        this.baseTableToken = baseTableToken;
    }

    public void setFromMicros(long fromMicros) {
        this.fromMicros = fromMicros;
    }

    public void setSamplingInterval(long samplingInterval) {
        this.samplingInterval = samplingInterval;
    }

    public void setSamplingIntervalUnit(char samplingIntervalUnit) {
        this.samplingIntervalUnit = samplingIntervalUnit;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public void setTimeZoneOffset(String timeZoneOffset) {
        this.timeZoneOffset = timeZoneOffset;
    }

    public void setToMicros(long toMicros) {
        this.toMicros = toMicros;
    }

    public void setViewSql(String viewSql) {
        this.viewSql = viewSql;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("create materialized view ");
        sink.put(tableModel.getName().token);
        sink.putAscii(" with base ");
        sink.put(baseTableToken.getTableName());
        sink.putAscii(" as (");
        sink.put(viewSql);
        sink.putAscii(')');
        for (int i = 0, n = tableModel.getColumnCount(); i < n; i++) {
            if (tableModel.isIndexed(i)) {
                sink.putAscii(", index(");
                sink.put(tableModel.getColumnName(i));
                sink.putAscii(" capacity ");
                sink.put(tableModel.getIndexBlockCapacity(i));
                sink.putAscii(')');
            }
        }

        sink.putAscii(" timestamp(");
        sink.put(tableModel.getColumnName(tableModel.getTimestampIndex()));
        sink.putAscii(')');

        sink.putAscii(" partition by ").put(PartitionBy.toString(tableModel.getPartitionBy()));

        final CharSequence volumeAlias = tableModel.getVolumeAlias();
        if (volumeAlias != null) {
            sink.putAscii(" in volume '").put(volumeAlias).putAscii('\'');
        }
    }
}
