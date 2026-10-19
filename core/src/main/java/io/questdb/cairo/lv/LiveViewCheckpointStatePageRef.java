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

package io.questdb.cairo.lv;

import io.questdb.cairo.vm.api.MemoryA;

/**
 * Flyweight reference to one encoded state page in an immutable checkpoint data
 * segment. The reference itself is serialized only inside checksummed metadata;
 * data segments deliberately have no page header or checksum.
 */
public final class LiveViewCheckpointStatePageRef {

    public static final int BYTES = 2 * Long.BYTES + 6 * Integer.BYTES; // 40
    public static final long NULL_SEGMENT_ID = -1;
    private int codec;
    private int decodedLength;
    private int flags;
    private long offset;
    private int pageKind;
    private int rowCount;
    private long segmentId = NULL_SEGMENT_ID;
    private int storedLength;

    public LiveViewCheckpointStatePageRef clear() {
        segmentId = NULL_SEGMENT_ID;
        offset = 0;
        storedLength = 0;
        decodedLength = 0;
        pageKind = 0;
        codec = 0;
        rowCount = 0;
        flags = 0;
        return this;
    }

    public int getCodec() {
        return codec;
    }

    public int getDecodedLength() {
        return decodedLength;
    }

    public int getFlags() {
        return flags;
    }

    public long getOffset() {
        return offset;
    }

    public int getPageKind() {
        return pageKind;
    }

    public int getRowCount() {
        return rowCount;
    }

    public long getSegmentId() {
        return segmentId;
    }

    public int getStoredLength() {
        return storedLength;
    }

    public boolean isNull() {
        return segmentId == NULL_SEGMENT_ID;
    }

    public LiveViewCheckpointStatePageRef of(
            long segmentId,
            long offset,
            int storedLength,
            int decodedLength,
            int pageKind,
            int codec,
            int rowCount,
            int flags
    ) {
        this.segmentId = segmentId;
        this.offset = offset;
        this.storedLength = storedLength;
        this.decodedLength = decodedLength;
        this.pageKind = pageKind;
        this.codec = codec;
        this.rowCount = rowCount;
        this.flags = flags;
        return this;
    }

    /**
     * Reads the reference from a checksummed metadata page already opened by
     * {@code reader}. Every field read is bounded by that metadata page.
     */
    public LiveViewCheckpointStatePageRef readFrom(LiveViewCheckpointMetaSegmentReader reader, long at) {
        segmentId = reader.getLong(at);
        offset = reader.getLong(at + Long.BYTES);
        storedLength = reader.getInt(at + 2L * Long.BYTES);
        decodedLength = reader.getInt(at + 2L * Long.BYTES + Integer.BYTES);
        pageKind = reader.getInt(at + 2L * Long.BYTES + 2L * Integer.BYTES);
        codec = reader.getInt(at + 2L * Long.BYTES + 3L * Integer.BYTES);
        rowCount = reader.getInt(at + 2L * Long.BYTES + 4L * Integer.BYTES);
        flags = reader.getInt(at + 2L * Long.BYTES + 5L * Integer.BYTES);
        return this;
    }

    /**
     * Appends the reference to a checksummed metadata page payload.
     */
    public void writeTo(MemoryA mem) {
        mem.putLong(segmentId);
        mem.putLong(offset);
        mem.putInt(storedLength);
        mem.putInt(decodedLength);
        mem.putInt(pageKind);
        mem.putInt(codec);
        mem.putInt(rowCount);
        mem.putInt(flags);
    }
}
