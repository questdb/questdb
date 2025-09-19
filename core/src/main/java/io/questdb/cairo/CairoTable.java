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
    public final LowerCaseCharSequenceIntHashMap columnNameIndexMap;
    public final IntList columnOrderList;
    public final ObjList<CairoColumn> columns;
    private boolean dedup;
    private int matViewRefreshLimitHoursOrMonths;
    private int matViewTimerInterval;
    private long matViewTimerStart;
    private char matViewTimerUnit;
    private int maxUncommittedRows;
    private long metadataVersion = -1;
    private long o3MaxLag;
    private int partitionBy;
    private boolean softLink;
    private int timestampIndex;
    private int timestampType;
    private TableToken token;
    private int ttlHoursOrMonths;

    public CairoTable(@NotNull TableToken token) {
        this.token = token;

        columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
        columnOrderList = new IntList();
        columns = new ObjList<>();
    }

    public CairoTable(@NotNull TableToken token, CairoTable fromTab) {
        this.token = token;

        columnOrderList = fromTab.columnOrderList;
        columns = fromTab.columns;
        columnNameIndexMap = fromTab.columnNameIndexMap;

        metadataVersion = fromTab.getMetadataVersion();
        timestampType = fromTab.getTimestampType();
        partitionBy = fromTab.getPartitionBy();
        maxUncommittedRows = fromTab.getMaxUncommittedRows();
        o3MaxLag = fromTab.getO3MaxLag();
        timestampIndex = fromTab.getTimestampIndex();
        ttlHoursOrMonths = fromTab.getTtlHoursOrMonths();
        softLink = fromTab.isSoftLink();
        dedup = fromTab.hasDedup();
        matViewRefreshLimitHoursOrMonths = fromTab.getMatViewRefreshLimitHoursOrMonths();
        matViewTimerStart = fromTab.getMatViewTimerStart();
        matViewTimerInterval = fromTab.getMatViewTimerInterval();
        matViewTimerUnit = fromTab.getMatViewTimerUnit();
    }

    public int getColumnCount() {
        return columns.size();
    }

    public ObjList<CharSequence> getColumnNames() {
        return columnNameIndexMap.keys();
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
        return token.getDirName();
    }

    public int getId() {
        return token.getTableId();
    }

    public int getMatViewRefreshLimitHoursOrMonths() {
        return matViewRefreshLimitHoursOrMonths;
    }

    public int getMatViewTimerInterval() {
        return matViewTimerInterval;
    }

    public long getMatViewTimerStart() {
        return matViewTimerStart;
    }

    public char getMatViewTimerUnit() {
        return matViewTimerUnit;
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

    public @NotNull String getPartitionByName() {
        return PartitionBy.toString(partitionBy);
    }

    public @NotNull String getTableName() {
        return token.getTableName();
    }

    public TableToken getTableToken() {
        return token;
    }

    public int getTimestampIndex() {
        return timestampIndex;
    }

    public CharSequence getTimestampName() {
        if (timestampIndex != -1) {
            final CairoColumn timestampColumn = getColumnQuiet(timestampIndex);
            if (timestampColumn != null) {
                return timestampColumn.getName();
            }
        }
        return null;
    }

    public int getTimestampType() {
        return timestampType;
    }

    /**
     * Returns the time-to-live (TTL) of the data in this table: if positive,
     * it's in hours; if negative, it's in months (and the actual value is positive)
     */
    public int getTtlHoursOrMonths() {
        return ttlHoursOrMonths;
    }

    public boolean hasDedup() {
        return dedup;
    }

    public boolean isSoftLink() {
        return softLink;
    }

    public boolean isWalEnabled() {
        return token.isWal();
    }

    public void setDedupFlag(boolean dedup) {
        this.dedup = dedup;
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

    public void setSoftLinkFlag(boolean softLink) {
        this.softLink = softLink;
    }

    public void setTableToken(TableToken token) {
        this.token = token;
    }

    public void setTimestampIndex(int timestampIndex) {
        this.timestampIndex = timestampIndex;
    }

    public void setTimestampType(int timestampType) {
        this.timestampType = timestampType;
    }

    public void setTtlHoursOrMonths(int ttlHoursOrMonths) {
        this.ttlHoursOrMonths = ttlHoursOrMonths;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.put("CairoTable [");
        sink.put("name=").put(getTableName()).put(", ");
        sink.put("id=").put(getId()).put(", ");
        sink.put("directoryName=").put(getDirectoryName()).put(", ");
        sink.put("hasDedup=").put(hasDedup()).put(", ");
        sink.put("isSoftLink=").put(isSoftLink()).put(", ");
        sink.put("metadataVersion=").put(getMetadataVersion()).put(", ");
        sink.put("maxUncommittedRows=").put(getMaxUncommittedRows()).put(", ");
        sink.put("o3MaxLag=").put(getO3MaxLag()).put(", ");
        sink.put("partitionBy=").put(getPartitionByName()).put(", ");
        sink.put("timestampIndex=").put(getTimestampIndex()).put(", ");
        sink.put("timestampName=").put(getTimestampName()).put(", ");
        final int ttlHoursOrMonths = getTtlHoursOrMonths();
        if (ttlHoursOrMonths >= 0) {
            sink.put("ttlHours=").put(ttlHoursOrMonths).put(", ");
        } else {
            sink.put("ttlMonths=").put(-ttlHoursOrMonths).put(", ");
        }
        sink.put("walEnabled=").put(isWalEnabled()).put(", ");
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
