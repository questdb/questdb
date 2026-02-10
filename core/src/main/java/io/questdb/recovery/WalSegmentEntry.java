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

/** Immutable description of a WAL segment directory. */
public final class WalSegmentEntry {
    private final boolean hasEventFile;
    private final boolean hasEventIndexFile;
    private final boolean hasMetaFile;
    private final int segmentId;

    public WalSegmentEntry(int segmentId, boolean hasEventFile, boolean hasEventIndexFile, boolean hasMetaFile) {
        this.segmentId = segmentId;
        this.hasEventFile = hasEventFile;
        this.hasEventIndexFile = hasEventIndexFile;
        this.hasMetaFile = hasMetaFile;
    }

    public int getSegmentId() {
        return segmentId;
    }

    public boolean hasEventFile() {
        return hasEventFile;
    }

    public boolean hasEventIndexFile() {
        return hasEventIndexFile;
    }

    public boolean hasMetaFile() {
        return hasMetaFile;
    }

    public boolean isComplete() {
        return hasEventFile && hasEventIndexFile && hasMetaFile;
    }
}
