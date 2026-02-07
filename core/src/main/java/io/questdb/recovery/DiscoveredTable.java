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

import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

public class DiscoveredTable {
    private final String dirName;
    private final ObjList<ReadIssue> issues = new ObjList<>();
    private final TableDiscoveryState state;
    private final String tableName;
    private final boolean walEnabled;
    private final boolean walEnabledKnown;

    public DiscoveredTable(
            String tableName,
            String dirName,
            TableDiscoveryState state,
            boolean walEnabledKnown,
            boolean walEnabled
    ) {
        this.tableName = tableName;
        this.dirName = dirName;
        this.state = state;
        this.walEnabledKnown = walEnabledKnown;
        this.walEnabled = walEnabled;
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

    public TableDiscoveryState getState() {
        return state;
    }

    public String getTableName() {
        return tableName;
    }

    @Nullable
    public Boolean getWalEnabledOrNull() {
        return walEnabledKnown ? walEnabled : null;
    }

    public boolean isWalEnabled() {
        return walEnabled;
    }

    public boolean isWalEnabledKnown() {
        return walEnabledKnown;
    }
}

