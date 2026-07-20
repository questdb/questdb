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

import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.cairo.vm.api.MemoryW;

/**
 * A reference to an immutable metadata page inside a metadata segment
 * ({@code m.<segmentId>}). This is the {@code META_PAGE_REF} the design
 * (sections 7-9) holds inside checksummed parent metadata - a superblock slot's
 * root references, a timeline tree node's child pointers, a checkpoint root's
 * function directory, and so on.
 * <p>
 * A page reference locates a page by {@code (segmentId, offset)} and carries the
 * page's total on-disk {@code length} (header + payload). The length lets a
 * reader bound-check the read before touching the mapping, and lets it
 * cross-check the length the page's own header self-describes; the page's own
 * per-page CRC32 then catches payload or framing corruption. Because a reference
 * always lives inside checksummed metadata, a corrupted reference is caught at
 * the parent, not here.
 * <p>
 * A null reference (a checkpoint that has no such page, e.g. an empty timeline
 * root) is encoded by {@link #NULL_SEGMENT_ID}. The type is a mutable flyweight:
 * callers reuse one instance across reads/writes to stay allocation-free on the
 * checkpoint path.
 */
public final class LiveViewCheckpointPageRef {

    /**
     * On-disk/in-metadata size of a serialized reference: {@code segmentId} LONG,
     * {@code offset} LONG, {@code length} INT.
     */
    public static final int BYTES = Long.BYTES + Long.BYTES + Integer.BYTES; // 20
    /**
     * Segment id sentinel marking a null reference (no page).
     */
    public static final long NULL_SEGMENT_ID = -1;
    private static final int LENGTH_FIELD_OFFSET = Long.BYTES + Long.BYTES; // 16
    private static final int OFFSET_FIELD_OFFSET = Long.BYTES; // 8

    private int length;
    private long offset;
    private long segmentId = NULL_SEGMENT_ID;

    /**
     * Resets this reference to null.
     */
    public LiveViewCheckpointPageRef clear() {
        segmentId = NULL_SEGMENT_ID;
        offset = 0;
        length = 0;
        return this;
    }

    /**
     * @return total on-disk page length (header + payload) this reference points at
     */
    public int getLength() {
        return length;
    }

    /**
     * @return byte offset of the page within its metadata segment
     */
    public long getOffset() {
        return offset;
    }

    /**
     * @return id of the metadata segment holding the page, or
     * {@link #NULL_SEGMENT_ID} when this reference is null
     */
    public long getSegmentId() {
        return segmentId;
    }

    /**
     * @return true when this reference points at no page
     */
    public boolean isNull() {
        return segmentId == NULL_SEGMENT_ID;
    }

    /**
     * Sets this reference to point at {@code (segmentId, offset)} with total page
     * {@code length}.
     */
    public LiveViewCheckpointPageRef of(long segmentId, long offset, int length) {
        this.segmentId = segmentId;
        this.offset = offset;
        this.length = length;
        return this;
    }

    /**
     * Reads {@link #BYTES} bytes at absolute {@code at} into this reference.
     */
    public LiveViewCheckpointPageRef readFrom(MemoryR mem, long at) {
        this.segmentId = mem.getLong(at);
        this.offset = mem.getLong(at + OFFSET_FIELD_OFFSET);
        this.length = mem.getInt(at + LENGTH_FIELD_OFFSET);
        return this;
    }

    /**
     * Writes {@link #BYTES} bytes at absolute {@code at}.
     */
    public void writeTo(MemoryW mem, long at) {
        mem.putLong(at, segmentId);
        mem.putLong(at + OFFSET_FIELD_OFFSET, offset);
        mem.putInt(at + LENGTH_FIELD_OFFSET, length);
    }
}
