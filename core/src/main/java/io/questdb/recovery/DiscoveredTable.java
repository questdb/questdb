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

import io.questdb.cairo.TableUtils;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

public class DiscoveredTable {
    private final String dirName;
    private final ObjList<ReadIssue> issues = new ObjList<>();
    private RegistryEntry registryEntry;
    private final TableDiscoveryState state;
    private final String tableName;
    private int tableType;
    private final boolean walEnabled;
    private final boolean walEnabledKnown;

    public DiscoveredTable(
            String tableName,
            String dirName,
            TableDiscoveryState state,
            boolean walEnabledKnown,
            boolean walEnabled,
            int tableType
    ) {
        this.tableName = tableName;
        this.dirName = dirName;
        this.state = state;
        this.walEnabledKnown = walEnabledKnown;
        this.walEnabled = walEnabled;
        this.tableType = tableType;
    }

    public void addIssue(RecoveryIssueSeverity severity, RecoveryIssueCode code, String message) {
        issues.add(new ReadIssue(severity, code, message));
    }

    public String getDirName() {
        return dirName;
    }

    public ObjList<ReadIssue> getIssues() {
        return issues;
    }

    @Nullable
    public RegistryEntry getRegistryEntry() {
        return registryEntry;
    }

    public TableDiscoveryState getState() {
        return state;
    }

    public String getTableName() {
        return tableName;
    }

    public int getTableType() {
        return tableType;
    }

    public String getTableTypeName() {
        return switch (tableType) {
            case TableUtils.TABLE_TYPE_NON_WAL, TableUtils.TABLE_TYPE_WAL -> "table";
            case TableUtils.TABLE_TYPE_MAT -> "matview";
            case TableUtils.TABLE_TYPE_VIEW -> "view";
            default -> "";
        };
    }

    public void setRegistryEntry(RegistryEntry registryEntry) {
        this.registryEntry = registryEntry;
    }

    public void setTableType(int tableType) {
        this.tableType = tableType;
    }

    public boolean isWalEnabled() {
        return walEnabled;
    }

    public boolean isWalEnabledKnown() {
        return walEnabledKnown;
    }
}

