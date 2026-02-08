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

public final class RegistryState {
    private final ObjList<RegistryEntry> entries = new ObjList<>();
    private final ObjList<ReadIssue> issues = new ObjList<>();
    private long appendOffset;
    private int entryCount;
    private long fileSize;
    private String registryPath;
    private int version;

    public void addIssue(RecoveryIssueSeverity severity, RecoveryIssueCode code, String message) {
        issues.add(new ReadIssue(severity, code, message));
    }

    @Nullable
    public RegistryEntry findByDirName(String dirName) {
        for (int i = 0, n = entries.size(); i < n; i++) {
            RegistryEntry entry = entries.getQuick(i);
            if (!entry.isRemoved() && dirName.equals(entry.getDirName())) {
                return entry;
            }
        }
        return null;
    }

    public long getAppendOffset() {
        return appendOffset;
    }

    public ObjList<RegistryEntry> getEntries() {
        return entries;
    }

    public int getEntryCount() {
        return entryCount;
    }

    public long getFileSize() {
        return fileSize;
    }

    public ObjList<ReadIssue> getIssues() {
        return issues;
    }

    public String getRegistryPath() {
        return registryPath;
    }

    public int getVersion() {
        return version;
    }

    void setAppendOffset(long appendOffset) {
        this.appendOffset = appendOffset;
    }

    void setEntryCount(int entryCount) {
        this.entryCount = entryCount;
    }

    void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    void setRegistryPath(String registryPath) {
        this.registryPath = registryPath;
    }

    void setVersion(int version) {
        this.version = version;
    }
}
