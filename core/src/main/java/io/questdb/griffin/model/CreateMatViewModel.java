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

import io.questdb.cairo.ColumnType;
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
    private CharSequence baseTableName;
    private long intervalMicros = -1;
    private TableToken matViewToken;
    private CharSequence query;
    private long startEpochMicros = -1;

    private CreateMatViewModel() {
        tableModel = CreateTableModel.FACTORY.newInstance();
    }

    @Override
    public void clear() {
        tableModel.clear();
        baseTableName = null;
        query = null;
        intervalMicros = -1;
        startEpochMicros = -1;
        matViewToken = null;
    }

    public MaterializedViewDefinition generateDefinition() {
        return new MaterializedViewDefinition(baseTableName, startEpochMicros, intervalMicros, query, matViewToken);
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

    public void setBaseTableName(CharSequence baseTableName) {
        this.baseTableName = baseTableName;
    }

    public void setIntervalMicros(long intervalMicros) {
        this.intervalMicros = intervalMicros;
    }

    public void setQuery(CharSequence query) {
        this.query = query;
    }

    public void setStartEpochMicros(long startEpochMicros) {
        this.startEpochMicros = startEpochMicros;
    }

    public void setTableToken(TableToken matViewToken) {
        this.matViewToken = matViewToken;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("create materialized view ");
        sink.put(tableModel.getName().token);
        sink.putAscii(" with base ");
        sink.put(baseTableName);
        if (getQueryModel() != null) {
            sink.putAscii(" as (");
            getQueryModel().toSink(sink);
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
        } else {
            sink.putAscii(" (");
            int count = tableModel.getColumnCount();
            for (int i = 0; i < count; i++) {
                if (i > 0) {
                    sink.putAscii(", ");
                }
                sink.put(tableModel.getColumnName(i));
                sink.putAscii(' ');
                sink.put(ColumnType.nameOf(tableModel.getColumnType(i)));

                if (ColumnType.isSymbol(tableModel.getColumnType(i))) {
                    sink.putAscii(" capacity ");
                    sink.put(tableModel.getSymbolCapacity(i));
                    if (tableModel.getSymbolCacheFlag(i)) {
                        sink.putAscii(" cache");
                    } else {
                        sink.putAscii(" nocache");
                    }
                }

                if (tableModel.isIndexed(i)) {
                    sink.putAscii(" index capacity ");
                    sink.put(tableModel.getIndexBlockCapacity(i));
                }
            }
            sink.putAscii(')');
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
