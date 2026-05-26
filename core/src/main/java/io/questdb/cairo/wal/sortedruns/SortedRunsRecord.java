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

package io.questdb.cairo.wal.sortedruns;

import io.questdb.std.Unsafe;

/**
 * Zero-allocation accessor over a single CommitStat record in the
 * <code>_sortedruns</code> file. The accessor binds to a base memory
 * address and a record offset; all field reads go through {@link Unsafe}
 * with constant offsets defined in {@link SortedRunsFormat}.
 * <p>
 * Instances are reusable: rebind via {@link #bind(long, long)} to point at
 * a different record.
 */
public final class SortedRunsRecord {
    private long memAddress;
    private long recordOffset;

    public SortedRunsRecord bind(long memAddress, long recordOffset) {
        this.memAddress = memAddress;
        this.recordOffset = recordOffset;
        return this;
    }

    public long getExtAddress() {
        return memAddress + recordOffset + SortedRunsFormat.RECORD_HEADER_SIZE_BYTES;
    }

    public int getExtLength() {
        return Unsafe.getUnsafe().getInt(memAddress + recordOffset + SortedRunsFormat.RECORD_OFFSET_EXT_LENGTH);
    }

    public int getFlags() {
        return Unsafe.getUnsafe().getShort(memAddress + recordOffset + SortedRunsFormat.RECORD_OFFSET_FLAGS) & 0xFFFF;
    }

    public long getMaxTs() {
        return Unsafe.getUnsafe().getLong(memAddress + recordOffset + SortedRunsFormat.RECORD_OFFSET_MAX_TS);
    }

    public long getMinTs() {
        return Unsafe.getUnsafe().getLong(memAddress + recordOffset + SortedRunsFormat.RECORD_OFFSET_MIN_TS);
    }

    public long getPhysRowStart() {
        return Unsafe.getUnsafe().getLong(memAddress + recordOffset + SortedRunsFormat.RECORD_OFFSET_PHYS_ROW_START);
    }

    public long getRecordOffset() {
        return recordOffset;
    }

    public int getRowCount() {
        return Unsafe.getUnsafe().getInt(memAddress + recordOffset + SortedRunsFormat.RECORD_OFFSET_ROW_COUNT);
    }

    public boolean hasFlag(int flagBit) {
        return (getFlags() & (1 << flagBit)) != 0;
    }

    public long recordByteLength() {
        return SortedRunsFormat.RECORD_HEADER_SIZE_BYTES + getExtLength();
    }
}
