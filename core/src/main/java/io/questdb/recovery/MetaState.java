/*******************************************************************************
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

package io.questdb.recovery;

import io.questdb.cairo.ColumnType;
import io.questdb.std.ObjList;

/**
 * Parsed state from a {@code _meta} file. Contains the column list (types,
 * names, indexed flags), partition-by strategy, and designated timestamp index.
 *
 * <p>Populated by {@link BoundedMetaReader}; setters are package-private.
 */
public final class MetaState {
    private final ObjList<MetaColumnState> columns = new ObjList<>();
    private final ObjList<ReadIssue> issues = new ObjList<>();
    private int columnCount = TxnState.UNSET_INT;
    private String metaPath;
    private int partitionBy = TxnState.UNSET_INT;
    private int timestampIndex = TxnState.UNSET_INT;

    public void addIssue(RecoveryIssueSeverity severity, RecoveryIssueCode code, String message) {
        issues.add(new ReadIssue(severity, code, message));
    }

    public int getColumnCount() {
        return columnCount;
    }

    public ObjList<MetaColumnState> getColumns() {
        return columns;
    }

    public ObjList<ReadIssue> getIssues() {
        return issues;
    }

    public String getMetaPath() {
        return metaPath;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public int getTimestampColumnType() {
        if (timestampIndex >= 0 && timestampIndex < columns.size()) {
            return columns.getQuick(timestampIndex).type();
        }
        return ColumnType.TIMESTAMP;
    }

    public int getTimestampIndex() {
        return timestampIndex;
    }

    void setColumnCount(int columnCount) {
        this.columnCount = columnCount;
    }

    void setMetaPath(String metaPath) {
        this.metaPath = metaPath;
    }

    void setPartitionBy(int partitionBy) {
        this.partitionBy = partitionBy;
    }

    void setTimestampIndex(int timestampIndex) {
        this.timestampIndex = timestampIndex;
    }
}
