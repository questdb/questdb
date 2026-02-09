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

/** Column check results for one partition: partition directory name and per-column entries. */
public final class ColumnCheckResult {
    private final ObjList<ColumnCheckEntry> entries;
    private final String partitionDirName;

    public ColumnCheckResult(String partitionDirName, ObjList<ColumnCheckEntry> entries) {
        this.partitionDirName = partitionDirName;
        this.entries = entries;
    }

    public ObjList<ColumnCheckEntry> getEntries() {
        return entries;
    }

    public String getPartitionDirName() {
        return partitionDirName;
    }
}
