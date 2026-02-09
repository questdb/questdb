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

public final class RegistryState {
    private final ObjList<RegistryEntry> entries = new ObjList<>();
    private final ObjList<ReadIssue> issues = new ObjList<>();
    private long appendOffset;
    private long fileSize;
    private int version;

    public void addIssue(RecoveryIssueSeverity severity, RecoveryIssueCode code, String message) {
        issues.add(new ReadIssue(severity, code, message));
    }

    public long getAppendOffset() {
        return appendOffset;
    }

    public ObjList<RegistryEntry> getEntries() {
        return entries;
    }

    public long getFileSize() {
        return fileSize;
    }

    public ObjList<ReadIssue> getIssues() {
        return issues;
    }

    public int getVersion() {
        return version;
    }

    void setAppendOffset(long appendOffset) {
        this.appendOffset = appendOffset;
    }

    void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    void setVersion(int version) {
        this.version = version;
    }
}
