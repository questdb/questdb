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

package io.questdb.cairo;

import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

public class CairoTable implements Sinkable {
    public LowerCaseCharSequenceIntHashMap columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
    public IntList columnOrderMap = new IntList();
    public ObjList<CairoColumn> columns = new ObjList<>();
    private boolean isDedup;
    private boolean isSoftLink;
    private int maxUncommittedRows;
    private long metadataVersion = -1;
    private long o3MaxLag;
    private int partitionBy;
    private int timestampIndex;
    private TableToken token;

    public CairoTable() {
    }

    public CairoTable(@NotNull TableToken token) {
        this.setTableToken(token);
    }

    public void clear() {
        for (int i = 0, n = columns.size(); i < n; i++) {
            columns.remove(i);
        }
    }

    public long getColumnCount() {
        return this.columns.size();
    }

    public ObjList<CharSequence> getColumnNames() {
        return this.columnNameIndexMap.keys();
    }

    public CairoColumn getColumnQuiet(@NotNull CharSequence columnName) {
        final int index = columnNameIndexMap.get(columnName);
        if (index != -1) {
            return columns.getQuiet(index);
        } else {
            return null;
        }
    }

    public CairoColumn getColumnQuiet(int position) {
        return columns.getQuiet(position);
    }

    public String getDirectoryName() {
        return getTableToken().getDirName();
    }

    public int getId() {
        return this.getTableToken().getTableId();
    }

    public boolean getIsDedup() {
        return isDedup;
    }

    public boolean getIsSoftLink() {
        return isSoftLink;
    }

    public int getMaxUncommittedRows() {
        return maxUncommittedRows;
    }

    public long getMetadataVersion() {
        return metadataVersion;
    }

    public long getO3MaxLag() {
        return o3MaxLag;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public String getPartitionByName() {
        return PartitionBy.toString(partitionBy);
    }

    public @NotNull String getTableName() {
        return this.getTableToken().getTableName();
    }

    public TableToken getTableToken() {
        return token;
    }

    public int getTimestampIndex() {
        return timestampIndex;
    }

    public CharSequence getTimestampName() {
        if (this.timestampIndex != -1) {
            final CairoColumn timestampColumn = getColumnQuiet(this.timestampIndex);
            if (timestampColumn != null) {
                return timestampColumn.getName();
            }
        }
        return null;
    }

    public boolean getWalEnabled() {
        return getTableToken().isWal();
    }

    public void setIsDedup(boolean isDedup) {
        this.isDedup = isDedup;
    }

    public void setIsSoftLink(boolean isSoftLink) {
        this.isSoftLink = isSoftLink;
    }

    public void setMaxUncommittedRows(int maxUncommittedRows) {
        this.maxUncommittedRows = maxUncommittedRows;
    }

    public void setMetadataVersion(long metadataVersion) {
        this.metadataVersion = metadataVersion;
    }

    public void setO3MaxLag(long o3MaxLag) {
        this.o3MaxLag = o3MaxLag;
    }

    public void setPartitionBy(int partitionBy) {
        this.partitionBy = partitionBy;
    }

    public void setTableToken(TableToken token) {
        this.token = token;
    }

    public void setTimestampIndex(int timestampIndex) {
        this.timestampIndex = timestampIndex;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.put("CairoTable [");
        sink.put("name=").put(getTableName()).put(", ");
        sink.put("id=").put(getId()).put(", ");
        sink.put("directoryName=").put(getDirectoryName()).put(", ");
        sink.put("isDedup=").put(getIsDedup()).put(", ");
        sink.put("isSoftLink=").put(getIsSoftLink()).put(", ");
        sink.put("metadataVersion=").put(getMetadataVersion()).put(", ");
        sink.put("maxUncommittedRows=").put(getMaxUncommittedRows()).put(", ");
        sink.put("o3MaxLag=").put(getO3MaxLag()).put(", ");
        sink.put("partitionBy=").put(getPartitionByName()).put(", ");
        sink.put("timestampIndex=").put(getTimestampIndex()).put(", ");
        sink.put("timestampName=").put(getTimestampName()).put(", ");
        sink.put("walEnabled=").put(getWalEnabled()).put(", ");
        sink.put("columnCount=").put(getColumnCount()).put("]");
        sink.put('\n');
        for (int i = 0, n = columns.size(); i < n; i++) {
            sink.put("\t\t");
            columns.getQuick(i).toSink(sink);
            if (i != columns.size() - 1) {
                sink.put('\n');
            }
        }
    }

    public void upsertColumn(@NotNull CairoColumn newColumn) throws CairoException {
        final CharSequence columnName = newColumn.getName();
        final CairoColumn existingColumn = getColumnQuiet(columnName);
        if (existingColumn != null) {
            int denseIndex = columnNameIndexMap.get(columnName);
            columns.getAndSetQuick(denseIndex, newColumn);
        } else {
            columns.add(newColumn);
            final int denseIndex = columns.size() - 1;
            columnNameIndexMap.put(columnName, denseIndex);
        }
    }
}
