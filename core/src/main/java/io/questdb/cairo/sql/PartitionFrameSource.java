/*+*****************************************************************************
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

package io.questdb.cairo.sql;

import io.questdb.std.Mutable;

/**
 * Catalog fields identifying the physical base pinned by one partition snapshot.
 * Physical readers use this description to select their source. It contains only
 * scalar values and does not own native resources.
 */
public final class PartitionFrameSource implements Mutable {
    public static final int NATIVE = 1;
    public static final int PARQUET = 2;
    private long columnVersion = -1;
    private int kind;
    private long metadataCommittedBytes;
    private int metadataSlot = -1;
    private long partitionNameTxn = -1;

    @Override
    public void clear() {
        columnVersion = -1;
        kind = 0;
        metadataCommittedBytes = 0;
        metadataSlot = -1;
        partitionNameTxn = -1;
    }

    /** Native column-version identity, or -1 for Parquet. */
    public long getColumnVersion() {
        return columnVersion;
    }

    /** NATIVE or PARQUET; zero after clear or a failed source query. */
    public int getKind() {
        return kind;
    }

    /** Exact committed prefix of the selected Parquet metadata file; zero for native. */
    public long getMetadataCommittedBytes() {
        return metadataCommittedBytes;
    }

    /** Parquet metadata slot: 0 for _pm, 1 for _pm.b; -1 for native. */
    public int getMetadataSlot() {
        return metadataSlot;
    }

    public long getPartitionNameTxn() {
        return partitionNameTxn;
    }

    public void of(
            int kind,
            long partitionNameTxn,
            long columnVersion,
            long metadataCommittedBytes,
            int metadataSlot
    ) {
        this.kind = kind;
        this.partitionNameTxn = partitionNameTxn;
        this.columnVersion = columnVersion;
        this.metadataCommittedBytes = metadataCommittedBytes;
        this.metadataSlot = metadataSlot;
    }
}
